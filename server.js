const WebSocket = require('ws');
const https = require('https');

let ytSearch = null;
try {
    ytSearch = require('yt-search');
} catch (e) {}

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });
const rooms = {};

// Fast YouTube InnerTube Search (Direct YouTube API - No Scraping Blocks)
function searchYouTubeInnerTube(query) {
    return new Promise((resolve, reject) => {
        const postData = JSON.stringify({
            context: {
                client: {
                    clientName: "WEB",
                    clientVersion: "2.20240101.00.00",
                    hl: "en",
                    gl: "US"
                }
            },
            query: query
        });

        const req = https.request({
            hostname: 'www.youtube.com',
            path: '/youtubei/v1/search?prettyPrint=false',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(postData),
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        }, (res) => {
            let body = '';
            res.on('data', chunk => body += chunk);
            res.on('end', () => {
                try {
                    const data = JSON.parse(body);
                    const sections = data.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents || [];
                    const videos = [];
                    for (const sec of sections) {
                        const items = sec.itemSectionRenderer?.contents || [];
                        for (const it of items) {
                            const vr = it.videoRenderer;
                            if (vr && vr.videoId) {
                                const title = vr.title?.runs?.[0]?.text || vr.title?.simpleText || 'Unknown';
                                const author = vr.ownerText?.runs?.[0]?.text || vr.shortBylineText?.runs?.[0]?.text || 'YouTube';
                                const timestamp = vr.lengthText?.simpleText || '';
                                const thumb = vr.thumbnail?.thumbnails?.slice(-1)[0]?.url || `https://i.ytimg.com/vi/${vr.videoId}/hqdefault.jpg`;
                                videos.push({
                                    title: title,
                                    videoId: vr.videoId,
                                    author: author,
                                    thumbnail: thumb,
                                    timestamp: timestamp
                                });
                                if (videos.length >= 8) break;
                            }
                        }
                        if (videos.length >= 8) break;
                    }
                    resolve(videos);
                } catch (e) {
                    reject(e);
                }
            });
        });

        req.on('error', reject);
        req.setTimeout(3500, () => { req.destroy(); reject(new Error('InnerTube timeout')); });
        req.write(postData);
        req.end();
    });
}

// Fallback search using yt-search with timeout
function searchYtSearch(query) {
    return new Promise((resolve, reject) => {
        if (!ytSearch) return resolve([]);
        const timer = setTimeout(() => reject(new Error('yt-search timeout')), 3500);
        ytSearch(query)
            .then(res => {
                clearTimeout(timer);
                if (res && res.videos) {
                    resolve(res.videos.slice(0, 8).map(v => ({
                        title: v.title,
                        videoId: v.videoId,
                        author: v.author ? v.author.name : 'YouTube',
                        thumbnail: v.thumbnail || `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
                        timestamp: v.timestamp || ''
                    })));
                } else {
                    resolve([]);
                }
            })
            .catch(err => {
                clearTimeout(timer);
                reject(err);
            });
    });
}

async function searchYouTube(query) {
    try {
        const results = await searchYouTubeInnerTube(query);
        if (results && results.length > 0) return results;
    } catch (e) {
        console.warn("InnerTube search failed, trying fallback:", e.message);
    }

    try {
        const fallbackResults = await searchYtSearch(query);
        if (fallbackResults && fallbackResults.length > 0) return fallbackResults;
    } catch (e) {
        console.warn("Fallback search failed:", e.message);
    }

    return [];
}

wss.on('connection', (ws) => {
    let boundRoom = null;

    ws.on('message', async (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch (e) { return; }

        if (msg.type === 'PING') {
            ws.send(JSON.stringify({ type: 'PONG' }));
            return;
        }

        if (msg.type === 'CREATE_ROOM' || msg.type === 'HOST_ROOM') {
            boundRoom = msg.code;
            ws.clientName = msg.name || 'Host';
            ws.accountId = msg.accountId;
            ws.role = 'HOST';

            if (!rooms[boundRoom]) rooms[boundRoom] = { host: ws, peers: [], currentMedia: null };
            else rooms[boundRoom].host = ws;

            ws.send(JSON.stringify({ type: 'ROOM_CREATED', code: boundRoom }));
            ws.send(JSON.stringify({ type: 'ROOM_HOSTED_SUCCESS', code: boundRoom }));
            broadcastPeers(boundRoom);
            return;
        }

        if (msg.type === 'JOIN_ROOM') {
            const roomCode = msg.code;
            if (!rooms[roomCode] || !rooms[roomCode].host) {
                ws.send(JSON.stringify({ type: 'ROOM_NOT_FOUND' }));
                return;
            }

            boundRoom = roomCode;
            ws.clientName = msg.name || 'Member';
            ws.accountId = msg.accountId;
            ws.role = 'MEMBER';

            rooms[roomCode].peers = rooms[roomCode].peers.filter(p => p.accountId !== ws.accountId);
            rooms[roomCode].peers.push(ws);

            ws.send(JSON.stringify({ type: 'ROOM_JOINED', code: roomCode }));
            ws.send(JSON.stringify({ type: 'ROOM_JOIN_SUCCESS', code: roomCode }));
            
            if (rooms[roomCode].currentMedia) {
                ws.send(JSON.stringify(rooms[roomCode].currentMedia));
            }
            broadcastPeers(roomCode);
            return;
        }

        if (msg.type === 'SEARCH_VIDEOS') {
            if (!msg.query) return;
            try {
                const videos = await searchYouTube(msg.query);
                ws.send(JSON.stringify({ type: 'SEARCH_RESULTS', query: msg.query, results: videos }));
            } catch (err) {
                ws.send(JSON.stringify({ type: 'SEARCH_RESULTS', query: msg.query, results: [] }));
            }
            return;
        }

        if (msg.type === 'SEARCH_AND_PLAY') {
            if (!boundRoom || !rooms[boundRoom] || rooms[boundRoom].host !== ws) return;
            if (!msg.query) return;
            try {
                const videos = await searchYouTube(msg.query);
                if (videos.length > 0) {
                    const topVideo = videos[0];
                    const mediaMsg = { type: 'PLAY_YOUTUBE', ytId: topVideo.videoId, currentTime: 0, title: topVideo.title };
                    rooms[boundRoom].currentMedia = mediaMsg;
                    broadcastToRoom(boundRoom, mediaMsg);
                    ws.send(JSON.stringify(mediaMsg));
                }
            } catch (err) {
                console.error("Search & Play Error:", err.message);
            }
            return;
        }

        if (['PLAY_YOUTUBE', 'LOAD_VIDEO', 'PLAY_PLAYLIST', 'MEDIA_SYNC', 'SYNC_TIME', 'CONTROL'].includes(msg.type)) {
            if (!boundRoom || !rooms[boundRoom] || rooms[boundRoom].host !== ws) return;
            if (['PLAY_YOUTUBE', 'LOAD_VIDEO', 'PLAY_PLAYLIST'].includes(msg.type)) {
                rooms[boundRoom].currentMedia = msg;
            }
            broadcastToRoom(boundRoom, msg, ws);
            return;
        }

        if (msg.type === 'CHAT' || msg.type === 'CHAT_MESSAGE') {
            if (!boundRoom || !rooms[boundRoom]) return;
            broadcastToRoom(boundRoom, { type: 'CHAT_MESSAGE', name: ws.clientName, message: msg.message }, ws);
            return;
        }

        if (msg.type === 'REACTION' || msg.type === 'ANIMATED_BLAST') {
            if (!boundRoom || !rooms[boundRoom]) return;
            broadcastToRoom(boundRoom, msg, ws);
            return;
        }

        if (msg.type === 'CLOSE_ROOM' || msg.type === 'ROOM_CLOSED') {
            if (boundRoom && rooms[boundRoom] && rooms[boundRoom].host === ws) {
                broadcastToRoom(boundRoom, { type: 'ROOM_CLOSED' });
                delete rooms[boundRoom];
                boundRoom = null;
            }
        }
    });

    ws.on('close', () => {
        if (!boundRoom || !rooms[boundRoom]) return;
        if (rooms[boundRoom].host === ws) {
            broadcastToRoom(boundRoom, { type: 'ROOM_CLOSED' });
            delete rooms[boundRoom];
        } else {
            rooms[boundRoom].peers = rooms[boundRoom].peers.filter(p => p !== ws);
            broadcastPeers(boundRoom);
        }
    });
});

function broadcastToRoom(roomCode, data, excludeWs = null) {
    const r = rooms[roomCode];
    if (!r) return;
    const all = [r.host, ...r.peers].filter(Boolean);
    all.forEach(client => {
        if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

function broadcastPeers(roomCode) {
    const r = rooms[roomCode];
    if (!r) return;
    const list = [];
    if (r.host) list.push({ name: r.host.clientName, role: 'HOST' });
    r.peers.forEach(p => list.push({ name: p.clientName, role: 'MEMBER' }));
    broadcastToRoom(roomCode, { type: 'PEER_LIST', peers: list });
}

console.log(`SyncBeat Server listening on port ${PORT}`);

