const WebSocket = require('ws');
const https = require('https');

let ytSearch = null;
try {
    ytSearch = require('yt-search');
} catch (e) {
    console.warn("yt-search not installed, fallback enabled.");
}

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });
const rooms = {};

// Helper: HTTP GET JSON
function fetchJSON(url, timeoutMs = 4000) {
    return new Promise((resolve, reject) => {
        const req = https.get(url, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
            });
        });
        req.on('error', reject);
        req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error('Timeout')); });
    });
}

// Fallback 1: Direct YouTube HTML Scraper
function scrapeYouTubeSearch(query) {
    return new Promise((resolve) => {
        const options = {
            hostname: 'www.youtube.com',
            path: `/results?search_query=${encodeURIComponent(query)}&hl=en`,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cookie': 'CONSENT=YES+cb.20210328-17-p0.en+FX+478; SOCS=CAESEwgDEgk0ODE3Nzk3MjQaAmVuIAEaBgiA_LyaBg'
            }
        };
        https.get(options, (res) => {
            let html = '';
            res.on('data', chunk => html += chunk);
            res.on('end', () => {
                try {
                    const match = html.match(/var ytInitialData = ({.*?});<\/script>/s) || html.match(/ytInitialData\s*=\s*({.+?});/);
                    if (!match) return resolve([]);
                    const json = JSON.parse(match[1]);
                    const contents = json.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents?.[0]?.itemSectionRenderer?.contents || [];
                    const videos = [];
                    for (const item of contents) {
                        const v = item.videoRenderer;
                        if (v && v.videoId && v.title?.runs?.[0]?.text) {
                            videos.push({
                                title: v.title.runs[0].text,
                                videoId: v.videoId,
                                author: v.ownerText?.runs?.[0]?.text || 'YouTube',
                                thumbnail: `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
                                timestamp: v.lengthText?.simpleText || ''
                            });
                            if (videos.length >= 8) break;
                        }
                    }
                    resolve(videos);
                } catch (e) { resolve([]); }
            });
        }).on('error', () => resolve([]));
    });
}

// Fallback 2: Public Invidious API
async function fetchInvidiousSearch(query) {
    const instances = [
        'https://invidious.nerdvpn.de',
        'https://yewtu.be',
        'https://inv.tux.pizza',
        'https://invidious.jing.rocks'
    ];
    for (const base of instances) {
        try {
            const data = await fetchJSON(`${base}/api/v1/search?q=${encodeURIComponent(query)}&type=video`, 3000);
            if (Array.isArray(data) && data.length > 0) {
                return data.slice(0, 8).map(v => ({
                    title: v.title,
                    videoId: v.videoId,
                    author: v.author || 'YouTube',
                    thumbnail: v.videoThumbnails?.[0]?.url || `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
                    timestamp: v.lengthSeconds ? `${Math.floor(v.lengthSeconds / 60)}:${('0' + (v.lengthSeconds % 60)).slice(-2)}` : ''
                }));
            }
        } catch (e) {}
    }
    return [];
}

// Multi-Tier Resilient YouTube Search Function
async function searchYouTube(query) {
    if (ytSearch) {
        try {
            const result = await ytSearch(query);
            if (result && result.videos && result.videos.length > 0) {
                return result.videos.slice(0, 8).map(v => ({
                    title: v.title,
                    videoId: v.videoId,
                    author: v.author ? v.author.name : 'YouTube',
                    thumbnail: v.thumbnail || `https://i.ytimg.com/vi/${v.videoId}/hqdefault.jpg`,
                    timestamp: v.timestamp || ''
                }));
            }
        } catch (err) {
            console.error("yt-search error:", err.message);
        }
    }

    const scraped = await scrapeYouTubeSearch(query);
    if (scraped.length > 0) return scraped;

    return await fetchInvidiousSearch(query);
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

console.log(`SyncBeat Server running on port ${PORT}`);

