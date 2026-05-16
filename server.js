// Core Wars - Optimized Server
// npm install ws  →  node server.js
// https://core-wars-backend.onrender.com
//
// Optimizations applied:
//  • 30 Hz tick loop, 10 Hz snapshot broadcast (every 3rd tick)
//  • Compact JSON keys throughout (t, i, x, y, ph, tm, …)
//  • Projectiles sent as spawn/destroy EVENTS only — client simulates motion
//  • PROJ_DESTROY only emitted on actual hits; natural expiry handled client-side
//  • Buildings sent as add/remove EVENTS only — not in every snapshot
//  • Event queue flushed once per snapshot (never mid-tick)
//  • Input-only from client (no position in client→server traffic)
//  • Input sent only when changed (client-side delta guard, 0.12 rad threshold)
//  • 8-player hard cap per room
//  • Room deleted immediately when empty
//  • dt capped to avoid spiral-of-death on lag spike
//  • DELTA snapshots: only moved players included (position/angle change threshold)
//  • Angle byte-compressed: float → uint8 (0–255)
//  • Names sent via 'names' broadcast (not repeated each snapshot)
//  • HP sent via events only (PLAYER_HIT, PLAYER_SPAWN, PLAYER_DIE)
//  • Resources sent via RES_CHANGE event only (not each snapshot)
//  • Respawn timer removed from snapshots — client predicts locally
//  • Phase / timer / core HPs sent only when they change
//  • Snapshot skipped entirely when nothing has changed
//  • Empty ev array omitted from payload

const WebSocket = require('ws');
const crypto    = require('crypto');
const http      = require('http');

const server = http.createServer();
const wss    = new WebSocket.Server({ server });
const PORT   = process.env.PORT || 3000;

// ─── Constants ───────────────────────────────────────────────────────────────
const TICK_RATE   = 30;
const SNAP_EVERY  = 3;           // 10 Hz snapshots
const MAP_W       = 2000;
const MAP_H       = 1200;
const MAX_PLAYERS = 8;
const BUILD_TIME  = 30;

const PH = { LOBBY: 0, BUILD: 1, ATTACK: 2, END: 3 };

const EV = {
    PHASE_CHANGE   : 0,
    PLAYER_HIT     : 1,
    PLAYER_DIE     : 2,
    PLAYER_SPAWN   : 3,
    PROJ_SPAWN     : 4,
    PROJ_DESTROY   : 5,
    BUILD_ADD      : 6,
    BUILD_DESTROY  : 7,
    BUILD_HIT      : 8,
    CORE_HIT       : 9,
    WIN            : 10,
    NAMES          : 11,   // name/team list — sent on join/leave, not per-snapshot
    RES_CHANGE     : 12,   // resource update
    PLAYER_LEAVE   : 13,   // player disconnected
    TURRET_UPGRADE : 14,   // turret upgraded to new subtype
};

// ─── Turret upgrade tree ──────────────────────────────────────────────────────
// fireRate: ms between shots | dmg: damage per shot | range: targeting radius
// projSpd: projectile speed  | projR: projectile radius
// slow: applies movement slow on hit | dual: fires two projectiles
// splash: AoE radius on impact (0 = none) | bonusVsBldg: 1.5× vs buildings
const TURRET_DEFS = {
    't':       { fireRate: 800,  dmg: 10, range: 400, hp: 150, projSpd: 400, projR: 5  },
    't_mk2':   { fireRate: 650,  dmg: 12, range: 500, hp: 175, projSpd: 420, projR: 5,  upgFrom: 't',      cost: 40 },
    't_mk3':   { fireRate: 550,  dmg: 15, range: 520, hp: 280, projSpd: 440, projR: 5,  upgFrom: 't_mk2',  cost: 60 },
    't_supp':  { fireRate: 300,  dmg: 5,  range: 350, hp: 120, projSpd: 480, projR: 4,  upgFrom: 't',      cost: 40, slow: true },
    't_storm': { fireRate: 240,  dmg: 5,  range: 360, hp: 145, projSpd: 500, projR: 4,  upgFrom: 't_supp', cost: 60, slow: true, dual: true },
    't_break': { fireRate: 1600, dmg: 40, range: 450, hp: 180, projSpd: 300, projR: 9,  upgFrom: 't',      cost: 50, splash: 60 },
    't_siege': { fireRate: 1400, dmg: 50, range: 460, hp: 210, projSpd: 320, projR: 11, upgFrom: 't_break', cost: 70, splash: 85, bonusVsBldg: true },
};
const UPGRADE_PATHS = {
    't':       ['t_mk2', 't_supp', 't_break'],
    't_mk2':   ['t_mk3'],
    't_supp':  ['t_storm'],
    't_break': ['t_siege'],
};

// ─── Global state ────────────────────────────────────────────────────────────
const rooms   = new Map();
const clients = new Map();

// ─── Helpers ─────────────────────────────────────────────────────────────────
const dist    = (x1,y1,x2,y2) => Math.hypot(x2-x1, y2-y1);
const clamp   = (v,lo,hi)     => Math.max(lo, Math.min(hi, v));
const shortId = ()            => crypto.randomBytes(3).toString('hex');

function circleRect(cx, cy, cr, rx, ry, rw, rh) {
    const tx = clamp(cx, rx, rx + rw);
    const ty = clamp(cy, ry, ry + rh);
    return (cx-tx)**2 + (cy-ty)**2 <= cr*cr;
}

// Encode angle (-π..π) as uint8 (0–255)
function encodeAngle(a) {
    return Math.floor(((a + Math.PI) / (Math.PI * 2)) * 256) & 0xFF;
}

function wireBuild(b) {
    return { i: b.id, tp: b.type, st: b.subtype || 't', tm: b.team,
             x: Math.round(b.x), y: Math.round(b.y),
             hp: b.hp, mhp: b.maxHp, r: b.r || 0 };
}

// ─── Room ────────────────────────────────────────────────────────────────────
class Room {
    constructor(id) {
        this.id        = id;
        this.players   = new Map();
        this.buildings = new Map();
        this.projs     = new Map();

        this.phase     = PH.LOBBY;
        this.timer     = 0;
        this.winner    = -1;
        this.tickCount = 0;
        this.events    = [];

        this.tickInt   = null;
        this.timerInt  = null;
        this.lastTime  = Date.now();

        // Delta-snapshot state trackers
        this._prevPhase   = -1;
        this._prevTimer   = -1;
        this._prevCoreHPs = [-1, -1];

        this.cores = [
            { id: 0, team: 0, x: 200,         y: MAP_H/2, hp: 2500, maxHp: 2500, r: 40 },
            { id: 1, team: 1, x: MAP_W - 200, y: MAP_H/2, hp: 2500, maxHp: 2500, r: 40 },
        ];
    }

    addPlayer(ws, id, name) {
        let red = 0, blue = 0;
        for (const p of this.players.values()) p.team === 0 ? red++ : blue++;
        const team   = red <= blue ? 0 : 1;
        const startX = team === 0 ? 300 : MAP_W - 300;

        this.players.set(id, {
            id, ws, name, team,
            x: startX, y: MAP_H / 2,
            r: 15, spd: 250,
            hp: 100, maxHp: 100,
            a: 0,
            inp: { dx: 0, dy: 0, sh: false },
            lastShot: 0,
            rt: 0,
            res: 100,
            slowUntil: 0,
            _px: -1, _py: -1, _ab: -1,  // delta sentinels — force first inclusion
        });

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

        // Broadcast name list once — not repeated in snapshots
        this.broadcastNames();

        if (this.phase === PH.LOBBY && this.players.size >= 2) {
            this.startGame();
        } else {
            this.broadcastSnapshot();
        }
    }

    removePlayer(id) {
        if (this.players.size > 1) {
            this.events.push({ e: EV.PLAYER_LEAVE, i: id });
        }
        this.players.delete(id);
        if (this.players.size === 0) {
            this.cleanup();
        } else {
            this.broadcastNames();
        }
    }

    cleanup() {
        clearInterval(this.tickInt);
        clearInterval(this.timerInt);
        rooms.delete(this.id);
    }

    broadcastNames() {
        const n = Array.from(this.players.values()).map(p => ({ i: p.id, nm: p.name, tm: p.team }));
        this.broadcastRaw(JSON.stringify({ t: 'names', n }));
    }

    startGame() {
        this.phase   = PH.BUILD;
        this.timer   = BUILD_TIME;
        this.winner  = -1;
        this.events  = [];

        this.cores.forEach(c => { c.hp = c.maxHp; });
        this.buildings.clear();
        this.projs.clear();

        this._prevPhase   = -1;
        this._prevTimer   = -1;
        this._prevCoreHPs = [-1, -1];

        for (const p of this.players.values()) {
            p.res = 150;
            p.hp  = p.maxHp;
            p.rt  = 0;
            p.x   = p.team === 0 ? 300 : MAP_W - 300;
            p.y   = MAP_H / 2;
            p._px = -1; p._py = -1; p._ab = -1;
        }

        this.broadcastRaw(JSON.stringify({
            t    : 'start',
            ph   : this.phase,
            tm   : this.timer,
            cHps : [this.cores[0].hp, this.cores[1].hp],
        }));

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

    tick() {
        const now = Date.now();
        const dt  = Math.min((now - this.lastTime) / 1000, 0.1);
        this.lastTime = now;
        this.tickCount++;

        if (this.phase === PH.END) return;

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
                continue;
            }

            let nx = player.x + player.inp.dx * player.spd * (player.slowUntil > now ? 0.4 : 1.0) * dt;
            let ny = player.y + player.inp.dy * player.spd * (player.slowUntil > now ? 0.4 : 1.0) * dt;

            const mid = MAP_W / 2;
            let minX  = player.r;
            let maxX  = MAP_W - player.r;
            if (this.phase === PH.BUILD) {
                if (player.team === 0) maxX = mid - player.r;
                else                   minX = mid + player.r;
            }

            nx = clamp(nx, minX, maxX);
            ny = clamp(ny, player.r, MAP_H - player.r);

            for (const b of this.buildings.values()) {
                if (b.type === 'w') {
                    const rx = b.x - 25, ry = b.y - 25;
                    if (circleRect(nx, player.y, player.r, rx, ry, 50, 50)) nx = player.x;
                    if (circleRect(player.x, ny, player.r, rx, ry, 50, 50)) ny = player.y;
                }
            }

            player.x = nx;
            player.y = ny;

            if (this.phase === PH.ATTACK && player.inp.sh && now - player.lastShot > 200) {
                player.lastShot = now;
                this.spawnProjectile(player.x, player.y, player.a, player.team, player.id,
                    { spd: 700, dmg: 15, r: 5, life: 2.0, pt: 'pl' });
            }
        }

        if (this.phase === PH.ATTACK) {
            for (const b of this.buildings.values()) {
                if (b.type !== 't') continue;
                const fireRate = b.fireRate || 800;
                if (now - (b.ls || 0) <= fireRate) continue;
                const range  = b.range  || 400;
                const target = this.findClosestEnemy(b.x, b.y, b.team, range);
                if (!target) continue;
                b.a  = Math.atan2(target.y - b.y, target.x - b.x);
                b.ls = now;
                const opts = {
                    spd: b.projSpd || 400, dmg: b.dmgVal || 10, r: b.projR || 5, life: 2.0,
                    slow: b.slow || false, splash: b.splash || 0,
                    bonusVsBldg: b.bonusVsBldg || false, pt: b.subtype || 't',
                };
                this.spawnProjectile(b.x, b.y, b.a, b.team, b.id, opts);
                if (b.dual) {
                    this.spawnProjectile(b.x, b.y, b.a + 0.16, b.team, b.id, opts);
                }
            }
        }

        for (const [pid, p] of this.projs) {
            p.x += Math.cos(p.a) * p.spd * dt;
            p.y += Math.sin(p.a) * p.spd * dt;
            p.life -= dt;

            let dead         = p.life <= 0 || p.x < 0 || p.x > MAP_W || p.y < 0 || p.y > MAP_H;
            let hitSomething = false;

            if (!dead && this.phase === PH.ATTACK) {
                for (const target of this.players.values()) {
                    if (dead) break;
                    if (target.rt > 0 || target.team === p.team) continue;
                    if (dist(p.x, p.y, target.x, target.y) < target.r + p.r) {
                        target.hp -= p.dmg;
                        dead = true; hitSomething = true;
                        if (p.slow) target.slowUntil = now + 650;
                        if (target.hp <= 0) {
                            target.hp = 0;
                            target.rt = 5;
                            this.events.push({ e: EV.PLAYER_DIE, i: target.id });
                        } else {
                            this.events.push({ e: EV.PLAYER_HIT, i: target.id, hp: target.hp });
                        }
                    }
                }

                if (!dead) {
                    for (const [bid, b] of this.buildings) {
                        if (b.team === p.team) continue;
                        const hit =
                            (b.type === 'w' && circleRect(p.x, p.y, p.r, b.x-25, b.y-25, 50, 50)) ||
                            (b.type === 't' && dist(p.x, p.y, b.x, b.y) < b.r + p.r);
                        if (hit) {
                            const dmg = p.bonusVsBldg ? Math.round(p.dmg * 1.5) : p.dmg;
                            b.hp -= dmg;
                            dead = true; hitSomething = true;
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

                if (!dead) {
                    for (const c of this.cores) {
                        if (c.team === p.team || dist(p.x, p.y, c.x, c.y) >= c.r + p.r) continue;
                        c.hp  -= p.dmg;
                        dead   = true; hitSomething = true;
                        if (c.hp <= 0) {
                            c.hp        = 0;
                            this.phase  = PH.END;
                            this.winner = p.team;
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
                // Apply splash on impact
                if (hitSomething && p.splash > 0) {
                    this.applySplash(p.x, p.y, p.splash, p.team, Math.round(p.dmg * 0.5));
                }
                // Only emit PROJ_DESTROY on hits — natural timeout self-deleted by client
                if (hitSomething) this.events.push({ e: EV.PROJ_DESTROY, i: pid });
            }
        }

        if (this.tickCount % SNAP_EVERY === 0) this.broadcastSnapshot();
    }

    spawnProjectile(x, y, a, team, ownerId, opts = {}) {
        const id  = shortId();
        const spd = opts.spd !== undefined ? opts.spd : 700;
        const dmg = opts.dmg !== undefined ? opts.dmg : 15;
        const r   = opts.r   !== undefined ? opts.r   : 5;
        const life = opts.life || 2.0;
        this.projs.set(id, {
            x, y, a, team, ownerId, spd, r, dmg, life,
            slow: opts.slow || false,
            splash: opts.splash || 0,
            bonusVsBldg: opts.bonusVsBldg || false,
            pt: opts.pt || 't',
        });
        this.events.push({
            e  : EV.PROJ_SPAWN,
            i  : id,
            x  : Math.round(x),
            y  : Math.round(y),
            a  : +a.toFixed(4),
            tm : team,
            spd, r,
            pt : opts.pt || 't',
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

    handleBuild(player, req) {
        if (this.phase !== PH.BUILD) return;

        const cost = req.bt === 'w' ? 10 : 30;
        if (player.res < cost) return;

        const mid    = MAP_W / 2;
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
            const def = TURRET_DEFS['t'];
            b = {
                id, type: 't', subtype: 't', team: player.team,
                x: req.x, y: req.y, r: 20,
                hp: def.hp, maxHp: def.hp, a: 0, ls: 0,
                fireRate: def.fireRate, dmgVal: def.dmg, range: def.range,
                projSpd: def.projSpd, projR: def.projR,
                slow: false, dual: false, splash: 0, bonusVsBldg: false,
            };
        } else return;

        this.buildings.set(id, b);
        player.res -= cost;

        this.events.push({ e: EV.BUILD_ADD, b: wireBuild(b) });
        this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
    }

    handleUpgrade(player, req) {
        if (this.phase !== PH.BUILD && this.phase !== PH.ATTACK) return;
        const b = this.buildings.get(req.id);
        if (!b || b.type !== 't' || b.team !== player.team) return;

        const def = TURRET_DEFS[req.to];
        if (!def || def.upgFrom !== b.subtype) return;
        if (player.res < def.cost) return;

        player.res -= def.cost;
        const hpRatio  = b.hp / b.maxHp;
        b.subtype      = req.to;
        b.maxHp        = def.hp;
        b.hp           = Math.max(1, Math.round(def.hp * hpRatio));
        b.fireRate     = def.fireRate;
        b.dmgVal       = def.dmg;
        b.range        = def.range;
        b.projSpd      = def.projSpd;
        b.projR        = def.projR;
        b.slow         = def.slow        || false;
        b.dual         = def.dual        || false;
        b.splash       = def.splash      || 0;
        b.bonusVsBldg  = def.bonusVsBldg || false;

        this.events.push({ e: EV.TURRET_UPGRADE, i: b.id, st: b.subtype, hp: b.hp, mhp: b.maxHp });
        this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
    }

    applySplash(cx, cy, radius, attackingTeam, dmg) {
        for (const target of this.players.values()) {
            if (target.rt > 0 || target.team === attackingTeam) continue;
            if (dist(cx, cy, target.x, target.y) >= radius + target.r) continue;
            target.hp -= dmg;
            if (target.hp <= 0) {
                target.hp = 0; target.rt = 5;
                this.events.push({ e: EV.PLAYER_DIE, i: target.id });
            } else {
                this.events.push({ e: EV.PLAYER_HIT, i: target.id, hp: target.hp });
            }
        }
        const toDelete = [];
        for (const [bid, b] of this.buildings) {
            if (b.team === attackingTeam) continue;
            const hit = b.type === 'w'
                ? circleRect(cx, cy, radius, b.x - 25, b.y - 25, 50, 50)
                : dist(cx, cy, b.x, b.y) < radius + (b.r || 18);
            if (!hit) continue;
            b.hp -= dmg;
            if (b.hp <= 0) {
                toDelete.push(bid);
                this.events.push({ e: EV.BUILD_DESTROY, i: bid });
            } else {
                this.events.push({ e: EV.BUILD_HIT, i: bid, hp: b.hp });
            }
        }
        for (const bid of toDelete) this.buildings.delete(bid);
        for (const c of this.cores) {
            if (c.team === attackingTeam) continue;
            if (dist(cx, cy, c.x, c.y) >= radius + c.r) continue;
            c.hp -= dmg;
            if (c.hp <= 0) {
                c.hp = 0; this.phase = PH.END; this.winner = attackingTeam;
                this.events.push({ e: EV.WIN, w: this.winner });
            } else {
                this.events.push({ e: EV.CORE_HIT, id: c.id, hp: c.hp });
            }
        }
    }

    // Delta snapshot — only what changed since last broadcast
    broadcastSnapshot() {
        const evs = this.events.splice(0);

        // Delta: only alive players whose position/angle byte changed
        const changed = [];
        for (const pl of this.players.values()) {
            if (pl.rt > 0) continue;
            const rx = Math.round(pl.x);
            const ry = Math.round(pl.y);
            const ab = encodeAngle(pl.a);
            if (rx !== pl._px || ry !== pl._py || ab !== pl._ab) {
                pl._px = rx; pl._py = ry; pl._ab = ab;
                changed.push({ i: pl.id, x: rx, y: ry, a: ab });
            }
        }

        // Skip broadcast entirely if nothing changed
        if (changed.length === 0 && evs.length === 0) return;

        const snap = { t: 's' };

        if (this.phase !== this._prevPhase) {
            snap.ph = this.phase;
            this._prevPhase = this.phase;
        }
        if (this.timer !== this._prevTimer) {
            snap.tm = this.timer;
            this._prevTimer = this.timer;
        }
        if (this.cores[0].hp !== this._prevCoreHPs[0] || this.cores[1].hp !== this._prevCoreHPs[1]) {
            snap.c = [this.cores[0].hp, this.cores[1].hp];
            this._prevCoreHPs[0] = this.cores[0].hp;
            this._prevCoreHPs[1] = this.cores[1].hp;
        }

        if (changed.length) snap.p = changed;
        if (evs.length)     snap.ev = evs;

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

            } else if (data.t === 'i' && room) {
                const player = room.players.get(id);
                if (player && player.rt <= 0) {
                    player.inp.dx = clamp(+(data.dx) || 0, -1, 1);
                    player.inp.dy = clamp(+(data.dy) || 0, -1, 1);
                    player.a      = +(data.a)  || 0;
                    player.inp.sh = !!data.sh;
                }

            } else if (data.t === 'b' && room) {
                const player = room.players.get(id);
                if (player) room.handleBuild(player, data);

            } else if (data.t === 'upg' && room) {
                const player = room.players.get(id);
                if (player) room.handleUpgrade(player, data);
            }
        } catch (e) {
            console.error('WS message error:', e.message);
        }
    });

    ws.on('close', () => {
        const info = clients.get(ws);
        if (info?.roomId) {
            const room = rooms.get(info.roomId);
            if (room) room.removePlayer(id);
        }
        clients.delete(ws);
    });
});

function findRoom() {
    for (const [id, r] of rooms) {
        if (r.phase === PH.LOBBY && r.players.size < MAX_PLAYERS) return id;
    }
    return crypto.randomBytes(4).toString('hex');
}

server.listen(PORT, '0.0.0.0', () => console.log(`Core Wars server on :${PORT}`));
