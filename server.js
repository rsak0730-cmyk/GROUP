const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });
const rooms = {};

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
                const response = await fetch(`https://pipedapi.kavin.rocks/search?q=${encodeURIComponent(msg.query)}&filter=videos`);
                const data = await response.json();
                const videos = (data.items || []).slice(0, 6).map(v => ({
                    title: v.title,
                    videoId: v.url ? v.url.replace('/watch?v=', '') : '',
                    author: v.uploaderName || 'Unknown'
                })).filter(v => v.videoId);
                ws.send(JSON.stringify({ type: 'SEARCH_RESULTS', results: videos }));
            } catch (err) {
                ws.send(JSON.stringify({ type: 'SEARCH_RESULTS', results: [] }));
            }
            return;
        }

        if (msg.type === 'SEARCH_AND_PLAY') {
            if (!boundRoom || !rooms[boundRoom] || rooms[boundRoom].host !== ws) return;
            if (!msg.query) return;
            try {
                const response = await fetch(`https://pipedapi.kavin.rocks/search?q=${encodeURIComponent(msg.query)}&filter=videos`);
                const data = await response.json();
                const videos = data.items || [];
                if (videos.length > 0) {
                    const topVideo = videos[0];
                    const videoId = topVideo.url ? topVideo.url.replace('/watch?v=', '') : '';
                    if (videoId) {
                        const mediaMsg = { type: 'PLAY_YOUTUBE', ytId: videoId, currentTime: 0 };
                        rooms[boundRoom].currentMedia = mediaMsg;
                        broadcastToRoom(boundRoom, mediaMsg);
                        ws.send(JSON.stringify(mediaMsg));
                    }
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

