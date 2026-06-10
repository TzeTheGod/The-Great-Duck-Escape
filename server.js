/**
 * The Great Duck Escape — server
 * --------------------------------
 * Express + WebSocket. All state lives in memory (single room per server).
 *
 * Architecture note: this is a SCORE-SUBMISSION game, not a real-time sync game.
 * Phones count taps locally and POST a single final score. The server collects
 * scores during a grace window, then the host plays a scripted race replay.
 */

const path = require('path');
const http = require('http');
const os = require('os');
const express = require('express');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// ---- Timing constants ------------------------------------------------------
const COUNTDOWN_MS = 4000; // 3 -> 2 -> 1 -> QUACK
const TAP_MS = 10000;      // tapping window
const GRACE_MS = 5000;     // collect stragglers after tapping ends
const STATE_TTL_MS = 30 * 60 * 1000; // host can reconnect within 30 minutes

// ---- Game state ------------------------------------------------------------
const COLORS = ['#FFD700', '#4FC3F7', '#F48FB1', '#CE93D8', '#81C784', '#FFB74D'];

function makeRoomCode() {
  const letters = 'ABCDEFGHJKLMNPRSTUVWXYZ'; // omit easily-confused chars
  let code = '';
  for (let i = 0; i < 4; i++) code += letters[Math.floor(Math.random() * letters.length)];
  return code;
}

const game = {
  roomCode: makeRoomCode(),
  // lobby | countdown | tapping | submitting | race | reveal
  phase: 'lobby',
  players: new Map(), // playerId -> { id, name, color, tapCount, submitted, joinedAt }
  createdAt: Date.now(),
  timers: { phase: null, autoRace: null },
};

let nextId = 1;

function resetGame() {
  clearTimeout(game.timers.phase);
  clearTimeout(game.timers.autoRace);
  game.timers.phase = null;
  game.timers.autoRace = null;
  game.roomCode = makeRoomCode();
  game.phase = 'lobby';
  game.players.clear();
  game.createdAt = Date.now();
  broadcast({ type: 'reset', roomCode: game.roomCode });
  broadcast({ type: 'phase_change', phase: 'lobby' });
  log(`Game reset. New room code: ${game.roomCode}`);
}

function playerList() {
  return [...game.players.values()].map((p) => ({
    id: p.id,
    name: p.name,
    color: p.color,
    tapCount: p.tapCount,
    submitted: p.submitted,
  }));
}

function submittedCount() {
  return [...game.players.values()].filter((p) => p.submitted).length;
}

// ---- HTTP / Express --------------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/host', (req, res) => res.sendFile(path.join(__dirname, 'public', 'host.html')));

app.post('/api/join', (req, res) => {
  const { name, color } = req.body || {};
  if (game.phase !== 'lobby') {
    return res.status(409).json({ success: false, error: 'in_progress' });
  }
  const cleanName = String(name || '').trim().slice(0, 20) || 'Anon Duck';
  const cleanColor = COLORS.includes(color) ? color : COLORS[0];
  const id = 'p' + nextId++;
  game.players.set(id, {
    id,
    name: cleanName,
    color: cleanColor,
    tapCount: 0,
    submitted: false,
    joinedAt: Date.now(),
  });
  log(`Join: ${cleanName} (${cleanColor}) -> ${id}. Pond size: ${game.players.size}`);
  broadcast({
    type: 'player_joined',
    id,
    name: cleanName,
    color: cleanColor,
    count: game.players.size,
  });
  res.json({ success: true, playerId: id, roomCode: game.roomCode });
});

app.post('/api/submit', (req, res) => {
  const { playerId, tapCount } = req.body || {};
  const player = game.players.get(playerId);
  if (!player) return res.status(404).json({ success: false, error: 'unknown_player' });
  if (player.submitted) {
    // Duplicate submission — silently accept, keep first score.
    return res.json({ success: true });
  }
  player.tapCount = Math.max(0, Math.floor(Number(tapCount) || 0));
  player.submitted = true;
  log(`Submit: ${player.name} = ${player.tapCount} taps (${submittedCount()}/${game.players.size})`);
  broadcast({ type: 'submission_update', submitted: submittedCount(), total: game.players.size });

  // If everyone is in during the grace window, fast-track the race.
  if (game.phase === 'submitting' && submittedCount() >= game.players.size && game.players.size > 0) {
    clearTimeout(game.timers.autoRace);
    game.timers.autoRace = setTimeout(startRace, 1500);
  }
  res.json({ success: true });
});

app.get('/api/status', (req, res) => {
  res.json({
    phase: game.phase,
    roomCode: game.roomCode,
    playerCount: game.players.size,
    submissions: submittedCount(),
  });
});

// ---- WebSocket -------------------------------------------------------------
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(msg);
  }
}

wss.on('connection', (ws, req) => {
  const role = new URL(req.url, 'http://x').searchParams.get('role') || 'player';
  ws.role = role;
  // Welcome packet lets a freshly (re)connected screen rebuild its state.
  ws.send(
    JSON.stringify({
      type: 'welcome',
      roomCode: game.roomCode,
      phase: game.phase,
      players: playerList(),
      submitted: submittedCount(),
      total: game.players.size,
    })
  );

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (data.type === 'host_action') handleHostAction(data.action);
  });
});

function handleHostAction(action) {
  switch (action) {
    case 'start_countdown':
      if (game.phase !== 'lobby' || game.players.size === 0) return;
      startCountdown();
      break;
    case 'start_race':
      if (game.phase === 'submitting' || game.phase === 'tapping') startRace();
      break;
    case 'trigger_reveal':
      if (game.phase === 'race') triggerReveal();
      break;
    case 'reset':
      resetGame();
      break;
    default:
      break;
  }
}

// ---- Phase orchestration ---------------------------------------------------
function startCountdown() {
  game.phase = 'countdown';
  for (const p of game.players.values()) {
    p.tapCount = 0;
    p.submitted = false;
  }
  log('Phase -> countdown');
  broadcast({ type: 'phase_change', phase: 'countdown', startAt: Date.now() });
  clearTimeout(game.timers.phase);
  game.timers.phase = setTimeout(startTapping, COUNTDOWN_MS);
}

function startTapping() {
  game.phase = 'tapping';
  log('Phase -> tapping');
  broadcast({ type: 'phase_change', phase: 'tapping', startAt: Date.now(), duration: TAP_MS });
  clearTimeout(game.timers.phase);
  game.timers.phase = setTimeout(startSubmitting, TAP_MS);
}

function startSubmitting() {
  game.phase = 'submitting';
  log('Phase -> submitting (grace window)');
  broadcast({ type: 'phase_change', phase: 'submitting' });
  broadcast({ type: 'submission_update', submitted: submittedCount(), total: game.players.size });
  // Auto-start the race after the grace window if the host doesn't trigger it.
  clearTimeout(game.timers.autoRace);
  game.timers.autoRace = setTimeout(startRace, GRACE_MS);
}

function startRace() {
  if (game.phase === 'race' || game.phase === 'reveal') return;
  clearTimeout(game.timers.autoRace);
  game.phase = 'race';
  log('Phase -> race');
  broadcast({ type: 'scores', players: playerList() });
  broadcast({ type: 'phase_change', phase: 'race' });
}

function triggerReveal() {
  game.phase = 'reveal';
  log('Phase -> reveal');
  broadcast({ type: 'phase_change', phase: 'reveal' });
}

// ---- Housekeeping ----------------------------------------------------------
// Expire an abandoned game after the TTL so a forgotten server tab self-cleans.
setInterval(() => {
  if (game.phase === 'lobby' && game.players.size === 0) return;
  if (Date.now() - game.createdAt > STATE_TTL_MS) {
    log('State TTL reached — auto-resetting.');
    resetGame();
  }
}, 60 * 1000);

function log(msg) {
  console.log(`[${new Date().toLocaleTimeString()}] ${msg}`);
}

function lanAddresses() {
  const nets = os.networkInterfaces();
  const out = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name] || []) {
      if (net.family === 'IPv4' && !net.internal) out.push(net.address);
    }
  }
  return out;
}

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n🦆  THE GREAT DUCK ESCAPE is running!\n');
  console.log(`   Room code: ${game.roomCode}\n`);
  console.log('   Open these on the same Wi-Fi network:');
  console.log(`     Host screen   ->  http://localhost:${PORT}/host`);
  console.log(`     Player screen ->  http://localhost:${PORT}/`);
  const ips = lanAddresses();
  if (ips.length) {
    console.log('\n   Share with players on the LAN:');
    for (const ip of ips) {
      console.log(`     Host   ->  http://${ip}:${PORT}/host`);
      console.log(`     Player ->  http://${ip}:${PORT}/`);
    }
  }
  console.log('');
});
