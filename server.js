// Core Wars - Optimized Server
// npm install ws  →  node server.js
//
// Optimizations applied:
//  • 30 Hz tick loop, 15 Hz snapshot broadcast (every 2nd tick)
//  • Compact JSON keys throughout (t, i, x, y, ph, tm, …)
//  • Projectiles sent as spawn/destroy EVENTS only — client simulates motion
//  • Buildings sent as add/remove EVENTS only — not in every snapshot
//  • Event queue flushed once per snapshot (never mid-tick)
//  • Input-only from client (no position in client→server traffic)
//  • Input sent only when changed (client-side delta guard)
//  • 8-player hard cap per room
//  • Room deleted immediately when empty
//  • dt capped to avoid spiral-of-death on lag spike

const WebSocket = require('ws');
const crypto    = require('crypto');
const http      = require('http');

const server = http.createServer();
const wss    = new WebSocket.Server({ server });
const PORT   = process.env.PORT || 3000;

// ─── Constants ───────────────────────────────────────────────────────────────
const TICK_RATE   = 30;          // Hz — physics / logic
const SNAP_EVERY  = 2;           // broadcast every Nth tick → 15 Hz
const MAP_W       = 2000;
const MAP_H       = 1200;
const MAX_PLAYERS = 8;
const BUILD_TIME  = 30;          // seconds

// Phase IDs (keep numeric — cheaper JSON)
const PH = { LOBBY: 0, BUILD: 1, ATTACK: 2, END: 3 };

// Event type IDs (numeric = most compact in JSON)
const EV = {
    PHASE_CHANGE : 0,
    PLAYER_HIT   : 1,
    PLAYER_DIE   : 2,
    PLAYER_SPAWN : 3,
    PROJ_SPAWN   : 4,
    PROJ_DESTROY : 5,
    BUILD_ADD    : 6,
    BUILD_DESTROY: 7,
    BUILD_HIT    : 8,
    CORE_HIT     : 9,
    WIN          : 10,
};

// ─── Global state ────────────────────────────────────────────────────────────
const rooms   = new Map(); // roomId → Room
const clients = new Map(); // ws     → { id, roomId }

// ─── Helpers ─────────────────────────────────────────────────────────────────
const dist      = (x1,y1,x2,y2) => Math.hypot(x2-x1, y2-y1);
const clamp     = (v,lo,hi)      => Math.max(lo, Math.min(hi, v));
const shortId   = ()             => crypto.randomBytes(3).toString('hex'); // 6-char hex

function circleRect(cx, cy, cr, rx, ry, rw, rh) {
    const tx = clamp(cx, rx, rx + rw);
    const ty = clamp(cy, ry, ry + rh);
    return (cx-tx)**2 + (cy-ty)**2 <= cr*cr;
}

// Strip building to wire format
function wireBuild(b) {
    // Only fields the client needs; omit internals (ls, etc.)
    return { i: b.id, tp: b.type, tm: b.team, x: Math.round(b.x), y: Math.round(b.y),
             hp: b.hp, mhp: b.maxHp, r: b.r || 0 };
}

// ─── Room ────────────────────────────────────────────────────────────────────
class Room {
    constructor(id) {
        this.id        = id;
        this.players   = new Map();   // playerId → player object
        this.buildings = new Map();   // buildId  → building object
        this.projs     = new Map();   // projId   → projectile object

        this.phase     = PH.LOBBY;
        this.timer     = 0;
        this.winner    = -1;
        this.tickCount = 0;
        this.events    = [];          // flushed each snapshot

        this.tickInt   = null;
        this.timerInt  = null;
        this.lastTime  = Date.now();

        // Core positions are static after init — only HP changes travel over wire
        this.cores = [
            { id: 0, team: 0, x: 200,         y: MAP_H/2, hp: 2500, maxHp: 2500, r: 40 },
            { id: 1, team: 1, x: MAP_W - 200, y: MAP_H/2, hp: 2500, maxHp: 2500, r: 40 },
        ];
    }

    // ── Player management ──────────────────────────────────────────────────
    addPlayer(ws, id, name) {
        // Balance teams
        let red = 0, blue = 0;
        for (const p of this.players.values()) p.team === 0 ? red++ : blue++;
        const team   = red <= blue ? 0 : 1;
        const startX = team === 0 ? 300 : MAP_W - 300;

        this.players.set(id, {
            id, ws, name, team,
            x: startX, y: MAP_H / 2,
            r: 15, spd: 250,
            hp: 100, maxHp: 100,
            a: 0,                           // aim angle
            inp: { dx: 0, dy: 0, sh: false },
            lastShot: 0,
            rt: 0,                          // respawn timer
            res: 100,                       // resources / scrap
        });

        // Send init to THIS player only:
        //  • their id, team, start pos
        //  • static map dimensions
        //  • full building list (for late-joiners)
        //  • static core geometry (positions never change)
        ws.send(JSON.stringify({
            t    : 'init',
            id,
            r    : this.id,
            mw   : MAP_W, mh: MAP_H,
            team,
            x    : startX, y: MAP_H / 2,
            blds : Array.from(this.buildings.values()).map(wireBuild),
            cores: this.cores.map(c => ({ id: c.id, tm: c.team, x: c.x, y: c.y, r: c.r, mhp: c.maxHp })),
        }));

        // Auto-start when ≥ 2 players
        if (this.phase === PH.LOBBY && this.players.size >= 2) {
            this.startGame();
        } else {
            this.broadcastSnapshot(); // Sends current phase/players to everyone
        }
    }

    removePlayer(id) {
        this.players.delete(id);
        if (this.players.size === 0) this.cleanup();
    }

    cleanup() {
        clearInterval(this.tickInt);
        clearInterval(this.timerInt);
        rooms.delete(this.id);
    }

    // ── Game flow ──────────────────────────────────────────────────────────
    startGame() {
        this.phase   = PH.BUILD;
        this.timer   = BUILD_TIME;
        this.winner  = -1;
        this.events  = [];

        this.cores.forEach(c => { c.hp = c.maxHp; });
        this.buildings.clear();
        this.projs.clear();

        for (const p of this.players.values()) {
            p.res = 150;
            p.hp  = p.maxHp;
            p.rt  = 0;
            p.x   = p.team === 0 ? 300 : MAP_W - 300;
            p.y   = MAP_H / 2;
        }

        // Broadcast a lightweight "start" burst so clients reset immediately
        this.broadcastRaw(JSON.stringify({
            t    : 'start',
            ph   : this.phase,
            tm   : this.timer,
            cHps : [this.cores[0].hp, this.cores[1].hp],
        }));

        // 1-second countdown
        this.timerInt = setInterval(() => {
            if (this.players.size === 0) return;
            if (this.phase === PH.BUILD) {
                this.timer--;
                if (this.timer <= 0) {
                    this.phase = PH.ATTACK;
                    this.timer = 0;
                    this.events.push({ e: EV.PHASE_CHANGE, ph: PH.ATTACK });
                }
            }
        }, 1000);

        this.tickInt = setInterval(() => this.tick(), 1000 / TICK_RATE);
    }

    // ── Main tick (30 Hz) ──────────────────────────────────────────────────
    tick() {
        const now = Date.now();
        const dt  = Math.min((now - this.lastTime) / 1000, 0.1); // cap at 100 ms
        this.lastTime = now;
        this.tickCount++;

        if (this.phase === PH.END) return;

        // ── Players ────────────────────────────────────────────────────────
        for (const player of this.players.values()) {
            if (player.rt > 0) {
                player.rt -= dt;
                if (player.rt <= 0) {
                    player.rt = 0;
                    player.hp = player.maxHp;
                    player.x  = player.team === 0 ? 300 : MAP_W - 300;
                    player.y  = MAP_H / 2;
                    this.events.push({
                        e: EV.PLAYER_SPAWN,
                        i: player.id,
                        x: Math.round(player.x),
                        y: Math.round(player.y),
                        hp: player.hp,
                    });
                }
                continue; // dead players skip movement & shooting
            }

            // Movement
            let nx = player.x + player.inp.dx * player.spd * dt;
            let ny = player.y + player.inp.dy * player.spd * dt;

            const mid  = MAP_W / 2;
            let minX   = player.r;
            let maxX   = MAP_W - player.r;
            if (this.phase === PH.BUILD) {
                if (player.team === 0) maxX = mid - player.r;
                else                   minX = mid + player.r;
            }

            nx = clamp(nx, minX, maxX);
            ny = clamp(ny, player.r, MAP_H - player.r);

            // Wall collision (AABB vs circle, slide)
            for (const b of this.buildings.values()) {
                if (b.type === 'w') {
                    const rx = b.x - 25, ry = b.y - 25;
                    if (circleRect(nx, player.y, player.r, rx, ry, 50, 50)) nx = player.x;
                    if (circleRect(player.x, ny, player.r, rx, ry, 50, 50)) ny = player.y;
                }
            }

            player.x = nx;
            player.y = ny;

            // Shooting
            if (this.phase === PH.ATTACK && player.inp.sh && now - player.lastShot > 200) {
                player.lastShot = now;
                this.spawnProjectile(player.x, player.y, player.a, player.team, player.id, false);
            }
        }

        // ── Turrets (attack phase only) ────────────────────────────────────
        if (this.phase === PH.ATTACK) {
            for (const b of this.buildings.values()) {
                if (b.type === 't' && now - (b.ls || 0) > 800) {
                    const target = this.findClosestEnemy(b.x, b.y, b.team, 400);
                    if (target) {
                        b.a  = Math.atan2(target.y - b.y, target.x - b.x);
                        b.ls = now;
                        this.spawnProjectile(b.x, b.y, b.a, b.team, b.id, true);
                    }
                }
            }
        }

        // ── Projectiles ───────────────────────────────────────────────────
        for (const [pid, p] of this.projs) {
            p.x += Math.cos(p.a) * p.spd * dt;
            p.y += Math.sin(p.a) * p.spd * dt;
            p.life -= dt;

            let dead = p.life <= 0 || p.x < 0 || p.x > MAP_W || p.y < 0 || p.y > MAP_H;

            if (!dead && this.phase === PH.ATTACK) {
                // vs players
                for (const target of this.players.values()) {
                    if (dead) break;
                    if (target.rt > 0 || target.team === p.team) continue;
                    if (dist(p.x, p.y, target.x, target.y) < target.r + p.r) {
                        target.hp -= p.dmg;
                        dead = true;
                        if (target.hp <= 0) {
                            target.hp = 0;
                            target.rt = 5;
                            this.events.push({ e: EV.PLAYER_DIE, i: target.id });
                        } else {
                            this.events.push({ e: EV.PLAYER_HIT, i: target.id, hp: target.hp });
                        }
                    }
                }

                // vs buildings
                if (!dead) {
                    for (const [bid, b] of this.buildings) {
                        if (b.team === p.team) continue;
                        const hit =
                            (b.type === 'w' && circleRect(p.x, p.y, p.r, b.x-25, b.y-25, 50, 50)) ||
                            (b.type === 't' && dist(p.x, p.y, b.x, b.y) < b.r + p.r);
                        if (hit) {
                            b.hp -= p.dmg;
                            dead   = true;
                            if (b.hp <= 0) {
                                this.buildings.delete(bid);
                                this.events.push({ e: EV.BUILD_DESTROY, i: bid });
                            } else {
                                this.events.push({ e: EV.BUILD_HIT, i: bid, hp: b.hp });
                            }
                            break;
                        }
                    }
                }

                // vs cores
                if (!dead) {
                    for (const c of this.cores) {
                        if (c.team === p.team || dist(p.x, p.y, c.x, c.y) >= c.r + p.r) continue;
                        c.hp  -= p.dmg;
                        dead   = true;
                        if (c.hp <= 0) {
                            c.hp         = 0;
                            this.phase   = PH.END;
                            this.winner  = p.team;
                            this.events.push({ e: EV.WIN, w: this.winner });
                        } else {
                            this.events.push({ e: EV.CORE_HIT, id: c.id, hp: c.hp });
                        }
                        break;
                    }
                }
            }

            if (dead) {
                this.projs.delete(pid);
                this.events.push({ e: EV.PROJ_DESTROY, i: pid });
            }
        }

        // ── Broadcast snapshot every 2nd tick (15 Hz) ─────────────────────
        if (this.tickCount % SNAP_EVERY === 0) this.broadcastSnapshot();
    }

    // ── Spawn projectile + emit event ──────────────────────────────────────
    spawnProjectile(x, y, a, team, ownerId, isTurret) {
        const id  = shortId();
        const spd = isTurret ? 400 : 700;
        const dmg = isTurret ? 10  : 15;
        this.projs.set(id, { x, y, a, team, ownerId, spd, r: 5, dmg, life: 2.0 });

        // Client simulates motion from this seed — no per-tick position needed
        this.events.push({
            e  : EV.PROJ_SPAWN,
            i  : id,
            x  : Math.round(x),
            y  : Math.round(y),
            a  : +a.toFixed(4),
            tm : team,
            spd,
            r  : 5,
        });
    }

    findClosestEnemy(x, y, team, maxRange) {
        let closest = null, minD = maxRange;
        for (const p of this.players.values()) {
            if (p.team === team || p.rt > 0) continue;
            const d = dist(x, y, p.x, p.y);
            if (d < minD) { minD = d; closest = p; }
        }
        return closest;
    }

    // ── Build handler ──────────────────────────────────────────────────────
    handleBuild(player, req) {
        if (this.phase !== PH.BUILD) return;

        // req.bt: 'w' (wall) or 't' (turret)
        const cost = req.bt === 'w' ? 10 : 30;
        if (player.res < cost) return;

        const mid  = MAP_W / 2;
        const onRed  = req.x < mid - 50;
        const onBlue = req.x > mid + 50;
        if ((player.team === 0 && !onRed) || (player.team === 1 && !onBlue)) return;

        const core = this.cores[player.team];
        if (dist(req.x, req.y, core.x, core.y) < 150) return;

        const id = shortId();
        let b;
        if (req.bt === 'w') {
            b = { id, type: 'w', team: player.team, x: req.x, y: req.y, hp: 200, maxHp: 200 };
        } else if (req.bt === 't') {
            b = { id, type: 't', team: player.team, x: req.x, y: req.y, r: 20, hp: 150, maxHp: 150, a: 0, ls: 0 };
        } else return;

        this.buildings.set(id, b);
        player.res -= cost;

        // Only ONE event needed — all clients update their local building map
        this.events.push({ e: EV.BUILD_ADD, b: wireBuild(b) });
    }

    // ── Snapshot broadcast (15 Hz) ─────────────────────────────────────────
    // Contains: phase, timer, core HPs, all player positions + states, event queue
    // Does NOT contain: buildings (event-driven), projectiles (event-driven)
    broadcastSnapshot() {
        const snap = {
            t  : 's',
            ph : this.phase,
            tm : this.timer,
            c  : [this.cores[0].hp, this.cores[1].hp], // just the two HPs
            p  : Array.from(this.players.values()).map(pl => ({
                i  : pl.id,
                nm : pl.name,
                tm : pl.team,
                x  : Math.round(pl.x),
                y  : Math.round(pl.y),
                hp : pl.hp,
                rt : +pl.rt.toFixed(1),
                a  : +pl.a.toFixed(3),
                res: pl.res,
            })),
            ev : this.events.splice(0),  // drain queue
        };

        this.broadcastRaw(JSON.stringify(snap));
    }

    broadcastRaw(payload) {
        for (const p of this.players.values()) {
            if (p.ws.readyState === WebSocket.OPEN) p.ws.send(payload);
        }
    }
}

// ─── WebSocket server ─────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
    const id = crypto.randomUUID();
    clients.set(ws, { id, roomId: null });

    ws.on('message', (raw) => {
        try {
            const data = JSON.parse(raw);
            const info = clients.get(ws);
            const room = info.roomId ? rooms.get(info.roomId) : null;

            // ── join ──────────────────────────────────────────────────────
            if (data.t === 'join') {
                const roomId = data.r || findRoom();
                if (!rooms.has(roomId)) rooms.set(roomId, new Room(roomId));
                const r = rooms.get(roomId);
                if (r.players.size >= MAX_PLAYERS) {
                    ws.send(JSON.stringify({ t: 'err', msg: 'Room full' }));
                    return;
                }
                info.roomId = roomId;
                r.addPlayer(ws, id, data.n || 'Pilot');

            // ── input (compact: t:"i", dx, dy, a, sh) ────────────────────
            } else if (data.t === 'i' && room) {
                const player = room.players.get(id);
                if (player && player.rt <= 0) {
                    player.inp.dx = clamp(+(data.dx) || 0, -1, 1);
                    player.inp.dy = clamp(+(data.dy) || 0, -1, 1);
                    player.a      = +(data.a)  || 0;
                    player.inp.sh = !!data.sh;
                }

            // ── build (compact: t:"b", bt:"w"|"t", x, y) ─────────────────
            } else if (data.t === 'b' && room) {
                const player = room.players.get(id);
                if (player) room.handleBuild(player, data);
            }
        } catch (e) {
            console.error('WS message error:', e.message);
        }
    });

    ws.on('close', () => {
        const info = clients.get(ws);
        if (info?.roomId) {
            const room = rooms.get(info.roomId);
            if (room) room.removePlayer(info.id);
        }
        clients.delete(ws);
    });
});

// ─── Matchmaking — FIFO, no ranking ──────────────────────────────────────────
function findRoom() {
    for (const [id, r] of rooms) {
        if (r.phase === PH.LOBBY && r.players.size < MAX_PLAYERS) return id;
    }
    return crypto.randomBytes(4).toString('hex');
}

server.listen(PORT, '0.0.0.0', () => console.log(`Core Wars server on :${PORT}`));
