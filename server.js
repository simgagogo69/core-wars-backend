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
const MAP_W       = 3000;
const MAP_H       = 1400;
const MAX_PLAYERS = 8;
const BUILD_TIME  = 30;
const LOBBY_TIME  = 120;  // 2-min lobby countdown (when 2+ players, nobody voted yet)
const VOTE_TIME   = 25;   // seconds for the dedicated voting phase

// ─── Map definitions ─────────────────────────────────────────────────────────
// waterZones: array of {x,y,w,h} rectangles that are impassable water
// cores:  [{x,y},{x,y}] — red then blue
// spawns: [{x,y},{x,y}] — red then blue
const MAP_DEFS = [
    {
        id  : 0,
        name: 'WARZONE',
        waterZones: [],
        cores : [{ x: 200,         y: MAP_H / 2 }, { x: MAP_W - 200,  y: MAP_H / 2 }],
        spawns: [{ x: 380,         y: MAP_H / 2 }, { x: MAP_W - 380,  y: MAP_H / 2 }],
    },
    {
        id  : 1,
        name: 'ARCHIPELAGO',
        // Red island  x:0-800, y:200-1200
        // Blue island x:2200-3000, y:200-1200
        // Bridge      x:800-2200,  y:540-860
        waterZones: [
            { x: 0,    y: 0,    w: 800,  h: 200  },   // red island top corner
            { x: 0,    y: 1200, w: 800,  h: 200  },   // red island bottom corner
            { x: 2200, y: 0,    w: 800,  h: 200  },   // blue island top corner
            { x: 2200, y: 1200, w: 800,  h: 200  },   // blue island bottom corner
            { x: 800,  y: 0,    w: 1400, h: 540  },   // open sea top (above bridge)
            { x: 800,  y: 860,  w: 1400, h: 540  },   // open sea bottom (below bridge)
        ],
        cores : [{ x: 250,  y: MAP_H / 2 }, { x: MAP_W - 250,  y: MAP_H / 2 }],
        spawns: [{ x: 430,  y: MAP_H / 2 }, { x: MAP_W - 430,  y: MAP_H / 2 }],
    },
    {
        id  : 2,
        name: 'CHOKEPOINT',
        // Red base  x:0-950,   full height
        // Blue base x:2050-3000, full height
        // Middle corridor x:950-2050, y:380-1020
        waterZones: [
            { x: 950, y: 0,    w: 1100, h: 380  },   // sea top-centre
            { x: 950, y: 1020, w: 1100, h: 380  },   // sea bottom-centre
        ],
        cores : [{ x: 200,         y: MAP_H / 2 }, { x: MAP_W - 200,  y: MAP_H / 2 }],
        spawns: [{ x: 380,         y: MAP_H / 2 }, { x: MAP_W - 380,  y: MAP_H / 2 }],
    },
];

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
    WALL_UPGRADE   : 15,   // wall upgraded to new subtype
};

// ─── Turret upgrade tree ──────────────────────────────────────────────────────
// fireRate: ms between shots | dmg: damage per shot | range: targeting radius
// projSpd: projectile speed  | projR: projectile radius
// slow: applies movement slow on hit | dual: fires two projectiles
// splash: AoE radius on impact (0 = none) | bonusVsBldg: 1.5× vs buildings
const TURRET_DEFS = {
    // ── ROE tree ──────────────────────────────────────────────────────────────
    't':        { fireRate: 800,  dmg: 10, range: 400, hp: 150, projSpd: 400, projR: 5  },
    't_mk2':    { fireRate: 650,  dmg: 12, range: 500, hp: 175, projSpd: 420, projR: 5,  upgFrom: 't',        cost: 40 },
    't_mk3':    { fireRate: 550,  dmg: 15, range: 520, hp: 280, projSpd: 440, projR: 5,  upgFrom: 't_mk2',    cost: 60 },
    't_supp':   { fireRate: 300,  dmg: 5,  range: 350, hp: 120, projSpd: 480, projR: 4,  upgFrom: 't',        cost: 40, slow: true },
    't_storm':  { fireRate: 240,  dmg: 5,  range: 360, hp: 145, projSpd: 500, projR: 4,  upgFrom: 't_supp',   cost: 60, slow: true, dual: true },
    't_break':  { fireRate: 1600, dmg: 40, range: 450, hp: 180, projSpd: 300, projR: 9,  upgFrom: 't',        cost: 50, splash: 60 },
    't_siege':  { fireRate: 1400, dmg: 50, range: 460, hp: 210, projSpd: 320, projR: 11, upgFrom: 't_break',  cost: 70, splash: 85, bonusVsBldg: true },
    // ── BGM Corp tree ─────────────────────────────────────────────────────────
    // Base BGM turret — Excavator Node → Heavy Crude Turret
    'bgm_t':    { fireRate: 1100, dmg: 22, range: 360, hp: 380, projSpd: 260, projR: 8  },  // Excavator Node — base BGM turret
    // Tier-1 branches (all directly from bgm_t)
    'bgm_drill':{ fireRate: 180,  dmg: 8,  range: 300, hp: 480, projSpd: 0,   projR: 6,  upgFrom: 'bgm_t', cost: 75, drill: true },
    'bgm_rail': { fireRate: 3200, dmg: 90, range: 750, hp: 400, projSpd: 900, projR: 12, upgFrom: 'bgm_t', cost: 80, bonusVsBldg: true },
    'bgm_molt': { fireRate: 1200, dmg: 15, range: 340, hp: 380, projSpd: 220, projR: 14, upgFrom: 'bgm_t', cost: 70, splash: 80, burn: true },
    'bgm_qsn':  { fireRate: 2000, dmg: 0,  range: 280, hp: 500, projSpd: 0,   projR: 0,  upgFrom: 'bgm_t', cost: 60, shield: true },
    // ── EPA tree ──────────────────────────────────────────────────────────────
    // Accurate, durable, expensive. Higher base stats, premium cost.
    'epa_t':        { fireRate: 600,  dmg: 14, range: 480, hp: 220, projSpd: 520, projR: 5  },  // Aegis Platform — base
    // Option 1: linear precision chain
    'epa_mk2':      { fireRate: 480,  dmg: 17, range: 530, hp: 300, projSpd: 560, projR: 5,  upgFrom: 'epa_t',        cost: 55 },
    'epa_mk3':      { fireRate: 380,  dmg: 21, range: 580, hp: 400, projSpd: 600, projR: 5,  upgFrom: 'epa_mk2',      cost: 75 },
    // Option 2: fortress/intercept chain
    'epa_fortress': { fireRate: 550,  dmg: 16, range: 460, hp: 380, projSpd: 540, projR: 6,  upgFrom: 'epa_t',        cost: 60, dual: true, intercept: 0.25 },
    'epa_knox':     { fireRate: 450,  dmg: 20, range: 480, hp: 520, projSpd: 560, projR: 6,  upgFrom: 'epa_fortress',  cost: 80, dual: true, intercept: 0.45 },
    // Option 3: dominion aura (buffs nearby EPA turrets)
    'epa_dominion': { fireRate: 500,  dmg: 18, range: 500, hp: 280, projSpd: 540, projR: 5,  upgFrom: 'epa_t',        cost: 65, dominion: true },
    // Option 4: citadel — very expensive, devastating
    'epa_citadel':  { fireRate: 280,  dmg: 28, range: 560, hp: 460, projSpd: 580, projR: 6,  upgFrom: 'epa_t',        cost: 120, dual: true, intercept: 0.30, citadel: true },
};
const UPGRADE_PATHS = {
    // ROE
    't':           ['t_mk2', 't_supp', 't_break'],
    't_mk2':       ['t_mk3'],
    't_supp':      ['t_storm'],
    't_break':     ['t_siege'],
    // BGM
    'bgm_t':       ['bgm_drill', 'bgm_rail', 'bgm_molt', 'bgm_qsn'],
    // EPA
    'epa_t':       ['epa_mk2', 'epa_fortress', 'epa_dominion', 'epa_citadel'],
    'epa_mk2':     ['epa_mk3'],
    'epa_fortress':['epa_knox'],
};

// ─── Wall upgrade tree ────────────────────────────────────────────────────────
// exploResist: multiplier applied to explosive (splash) damage received (< 1 = resistant)
// thermal: reflects partial energy dmg back, damages nearby enemies on hit
// conduit: buffs nearby BGM structures (HP regen pulse)
// anchor: massive HP, no special mechanics
const WALL_DEFS = {
    // ── ROE walls ─────────────────────────────────────────────────────────────
    'w':                 { hp: 200, repairCost: 0 },
    'w_reinforced':      { hp: 350, repairCost: 5,  upgFrom: 'w',                  cost: 20, exploResist: 0.75 },
    // ── BGM walls ─────────────────────────────────────────────────────────────
    'bgm_w':             { hp: 280, repairCost: 0 },
    'bgm_w_blast':       { hp: 420, repairCost: 5,  upgFrom: 'bgm_w',              cost: 25, exploResist: 0.40, shockAbsorb: true },
    'bgm_w_thermal':     { hp: 320, repairCost: 5,  upgFrom: 'bgm_w',              cost: 25, exploResist: 0.85, thermal: true },
    'bgm_w_anchor':      { hp: 800, repairCost: 8,  upgFrom: 'bgm_w',              cost: 35, exploResist: 0.60 },
    'bgm_w_conduit':     { hp: 300, repairCost: 5,  upgFrom: 'bgm_w',              cost: 30, exploResist: 0.90, conduit: true },
    // ── EPA walls — single linear upgrade chain ───────────────────────────────
    'epa_w':             { hp: 240, repairCost: 0 },
    'epa_w_fort':        { hp: 380, repairCost: 5,  upgFrom: 'epa_w',              cost: 22, exploResist: 0.80 },
    'epa_w_bulwark':     { hp: 460, repairCost: 5,  upgFrom: 'epa_w_fort',         cost: 35, exploResist: 0.75, regen: true },
    'epa_w_guardian':    { hp: 520, repairCost: 5,  upgFrom: 'epa_w_bulwark',      cost: 45, exploResist: 0.55, intercept: 0.20 },
    'epa_w_citadel':     { hp: 600, repairCost: 6,  upgFrom: 'epa_w_guardian',     cost: 55, exploResist: 0.40, intercept: 0.30, citadelAura: true },
    'epa_w_bastion':     { hp: 900, repairCost: 8,  upgFrom: 'epa_w_citadel',      cost: 70, exploResist: 0.25, intercept: 0.40, citadelAura: true, damageShare: true },
};
const WALL_UPGRADE_PATHS = {
    // ROE
    'w':                ['w_reinforced'],
    'w_reinforced':     [],
    // BGM
    'bgm_w':            ['bgm_w_blast', 'bgm_w_thermal', 'bgm_w_anchor', 'bgm_w_conduit'],
    'bgm_w_blast':      [],
    'bgm_w_thermal':    [],
    'bgm_w_anchor':     [],
    'bgm_w_conduit':    [],
    // EPA
    'epa_w':            ['epa_w_fort'],
    'epa_w_fort':       ['epa_w_bulwark'],
    'epa_w_bulwark':    ['epa_w_guardian'],
    'epa_w_guardian':   ['epa_w_citadel'],
    'epa_w_citadel':    ['epa_w_bastion'],
    'epa_w_bastion':    [],
};

// ─── Faction definitions ──────────────────────────────────────────────────────
// hasUpgrades: only ROE gets the turret/wall upgrade tree
// wallCost / turretCost: initial build price for this faction
const FACTIONS = {
    'roe': { wallCost: 8,  turretCost: 25, hasUpgrades: true,  baseTurret: 't',     baseWall: 'w'     },
    'bgm': { wallCost: 15, turretCost: 35, hasUpgrades: true,  baseTurret: 'bgm_t', baseWall: 'bgm_w' },
    'epa': { wallCost: 12, turretCost: 28, hasUpgrades: true,  baseTurret: 'epa_t',   baseWall: 'epa_w'  },
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
    return { i: b.id, tp: b.type, st: b.subtype || b.type, tm: b.team,
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

        // Lobby ready / countdown state
        this.readyStates    = new Map();  // playerId → true(ready) | false(not-ready)
        this.lobbyCountdown = -1;         // -1 = infinite (paused)
        this.lobbyRunning   = false;
        this.lobbyTimerInt  = null;
        this.inVotePhase    = false;

        // Faction voting (per team) + map voting (global)
        this.factionVotes = { 0: new Map(), 1: new Map() };
        this.teamFactions = ['roe', 'roe'];
        this.mapVotes     = new Map();  // playerId → mapId (number)

        // Pick a random map now as placeholder — will be resolved by vote at game start
        this.mapDef = MAP_DEFS[Math.floor(Math.random() * MAP_DEFS.length)];

        this.cores = [
            { id: 0, team: 0, x: this.mapDef.cores[0].x, y: this.mapDef.cores[0].y, hp: 2500, maxHp: 2500, r: 40 },
            { id: 1, team: 1, x: this.mapDef.cores[1].x, y: this.mapDef.cores[1].y, hp: 2500, maxHp: 2500, r: 40 },
        ];
    }

    // ── Lobby countdown management ────────────────────────────────────────────
    startLobbyCountdown(seconds) {
        if (this.lobbyTimerInt) clearInterval(this.lobbyTimerInt);
        this.lobbyCountdown = seconds;
        this.lobbyRunning   = true;
        this.lobbyTimerInt  = setInterval(() => {
            if (this.players.size === 0) { clearInterval(this.lobbyTimerInt); return; }
            this.lobbyCountdown = Math.max(0, this.lobbyCountdown - 1);
            this.broadcastLobbyState();
            if (this.lobbyCountdown <= 0) {
                clearInterval(this.lobbyTimerInt);
                this.lobbyTimerInt = null;
                this.startVotePhase();
            }
        }, 1000);
    }

    pauseLobbyCountdown() {
        if (this.lobbyTimerInt) { clearInterval(this.lobbyTimerInt); this.lobbyTimerInt = null; }
        this.lobbyRunning   = false;
        this.lobbyCountdown = -1;
    }

    // Re-evaluate lobby timer state based on ready votes
    evaluateLobby() {
        if (this.inVotePhase || this.phase !== PH.LOBBY) return;
        const n = this.players.size;
        if (n < 2) {
            this.pauseLobbyCountdown();
            this.broadcastLobbyState();
            return;
        }
        const readyCount  = [...this.readyStates.values()].filter(v => v === true).length;
        const notReadyAny = [...this.readyStates.values()].some(v => v === false);
        const anyReady    = readyCount > 0;
        const allReady    = readyCount === n;

        if (allReady) {
            if (this.lobbyTimerInt) clearInterval(this.lobbyTimerInt);
            this.broadcastLobbyState();
            this.startVotePhase();
            return;
        }

        if (notReadyAny) {
            // At least one explicit "not ready" → pause
            this.pauseLobbyCountdown();
        } else if (anyReady) {
            // Someone ready, nobody explicitly not-ready → ≤ 30s
            if (!this.lobbyRunning || this.lobbyCountdown > 30) {
                this.startLobbyCountdown(Math.min(this.lobbyCountdown > 0 ? this.lobbyCountdown : 30, 30));
            }
        } else {
            // Nobody has voted yet → start 2-min countdown if not already running
            if (!this.lobbyRunning) {
                this.startLobbyCountdown(LOBBY_TIME);
            }
        }
        this.broadcastLobbyState();
    }

    broadcastLobbyState() {
        const rd = [];
        for (const [pid, r] of this.readyStates) rd.push({ i: pid, r });
        this.broadcastRaw(JSON.stringify({
            t  : 'lobbystate',
            tm : this.lobbyRunning ? this.lobbyCountdown : -1,
            rd,
            n  : this.players.size,
        }));
    }

    // ── Faction voting ────────────────────────────────────────────────────────
    resolveFaction(team) {
        const votes = {};
        for (const f of this.factionVotes[team].values()) votes[f] = (votes[f] || 0) + 1;
        const entries = Object.entries(votes);
        if (entries.length === 0) return 'roe';
        const maxVotes = Math.max(...entries.map(e => e[1]));
        const tied     = entries.filter(e => e[1] === maxVotes).map(e => e[0]);
        return tied[Math.floor(Math.random() * tied.length)];
    }

    // Each player only sees their own team's faction tally
    broadcastFactionVotes() {
        for (const p of this.players.values()) {
            const tally = {};
            for (const f of this.factionVotes[p.team].values()) tally[f] = (tally[f] || 0) + 1;
            if (p.ws.readyState === WebSocket.OPEN)
                p.ws.send(JSON.stringify({ t: 'fvotes', v: tally }));
        }
    }

    // ── Map voting ────────────────────────────────────────────────────────────
    resolveMap() {
        const v = {};
        for (const mapId of this.mapVotes.values()) v[mapId] = (v[mapId] || 0) + 1;
        const entries = Object.entries(v);
        if (entries.length === 0) return MAP_DEFS[Math.floor(Math.random() * MAP_DEFS.length)];
        const maxV = Math.max(...entries.map(e => +e[1]));
        const tied = entries.filter(e => +e[1] === maxV).map(e => +e[0]);
        const winId = tied[Math.floor(Math.random() * tied.length)];
        return MAP_DEFS.find(m => m.id === winId) || MAP_DEFS[0];
    }

    broadcastMapVotes() {
        const v = {};
        for (const mapId of this.mapVotes.values()) v[mapId] = (v[mapId] || 0) + 1;
        this.broadcastRaw(JSON.stringify({ t: 'mvotes', v }));
    }

    startVotePhase() {
        if (this.inVotePhase) return;
        this.inVotePhase = true;
        this.factionVotes = { 0: new Map(), 1: new Map() };
        this.mapVotes     = new Map();
        let countdown = VOTE_TIME;

        // Send votestart individually so map list is included
        for (const p of this.players.values()) {
            if (p.ws.readyState === WebSocket.OPEN) {
                p.ws.send(JSON.stringify({
                    t    : 'votestart',
                    tm   : VOTE_TIME,
                    maps : MAP_DEFS.map(m => ({ id: m.id, name: m.name })),
                    team : p.team,
                }));
            }
        }

        const vi = setInterval(() => {
            if (this.players.size === 0) { clearInterval(vi); return; }
            countdown--;
            this.broadcastRaw(JSON.stringify({ t: 'votetick', tm: countdown }));
            if (countdown <= 0) { clearInterval(vi); this.startGame(); }
        }, 1000);
    }

    addPlayer(ws, id, name) {
        let red = 0, blue = 0;
        for (const p of this.players.values()) p.team === 0 ? red++ : blue++;
        const team   = red <= blue ? 0 : 1;
        const spawn  = this.mapDef.spawns[team];

        this.players.set(id, {
            id, ws, name, team,
            x: spawn.x, y: spawn.y,
            r: 15, spd: 250,
            hp: 100, maxHp: 100,
            a: 0,
            inp: { dx: 0, dy: 0, sh: false },
            lastShot: 0,
            rt: 0,
            res: 100,
            slowUntil: 0,
            burnUntil: 0,
            _px: -1, _py: -1, _ab: -1,  // delta sentinels — force first inclusion
        });

        ws.send(JSON.stringify({
            t    : 'init',
            id,
            r    : this.id,
            mw   : MAP_W, mh: MAP_H,
            team,
            x    : spawn.x, y: spawn.y,
            mapId  : this.mapDef.id,
            mapName: this.mapDef.name,
            wz     : this.mapDef.waterZones,
            spawns : this.mapDef.spawns,
            blds : Array.from(this.buildings.values()).map(wireBuild),
            cores: this.cores.map(c => ({ id: c.id, tm: c.team, x: c.x, y: c.y, r: c.r, mhp: c.maxHp })),
        }));

        // Broadcast name list once — not repeated in snapshots
        this.broadcastNames();

        // Evaluate lobby countdown state with the new player count
        this.evaluateLobby();
    }

    removePlayer(id) {
        if (this.players.size > 1) {
            this.events.push({ e: EV.PLAYER_LEAVE, i: id });
        }
        this.players.delete(id);
        this.readyStates.delete(id);
        this.mapVotes.delete(id);
        if (this.players.size === 0) {
            this.cleanup();
        } else {
            this.broadcastNames();
            this.evaluateLobby();
        }
    }

    cleanup() {
        clearInterval(this.tickInt);
        clearInterval(this.timerInt);
        if (this.lobbyTimerInt) clearInterval(this.lobbyTimerInt);
        rooms.delete(this.id);
    }

    broadcastNames() {
        const n = Array.from(this.players.values()).map(p => ({
            i  : p.id,
            nm : p.name,
            tm : p.team,
            rd : this.readyStates.get(p.id),   // true | false | undefined
        }));
        this.broadcastRaw(JSON.stringify({ t: 'names', n }));
    }

    // Returns true if a circle at (x,y) with radius r overlaps any water zone
    isOnWater(x, y, r = 0) {
        for (const wz of this.mapDef.waterZones) {
            if (x + r > wz.x && x - r < wz.x + wz.w &&
                y + r > wz.y && y - r < wz.y + wz.h) return true;
        }
        return false;
    }

    startGame() {
        // Resolve factions from votes
        this.teamFactions[0] = this.resolveFaction(0);
        this.teamFactions[1] = this.resolveFaction(1);

        // Resolve map from votes — updates mapDef and cores
        this.mapDef = this.resolveMap();
        this.cores  = [
            { id: 0, team: 0, x: this.mapDef.cores[0].x, y: this.mapDef.cores[0].y, hp: 2500, maxHp: 2500, r: 40 },
            { id: 1, team: 1, x: this.mapDef.cores[1].x, y: this.mapDef.cores[1].y, hp: 2500, maxHp: 2500, r: 40 },
        ];

        // Reset vote/lobby state
        this.factionVotes  = { 0: new Map(), 1: new Map() };
        this.mapVotes      = new Map();
        this.readyStates   = new Map();
        this.inVotePhase   = false;

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
            p.burnUntil = 0;
            p._lastBurnTick = 0;
            const spawn = this.mapDef.spawns[p.team];
            p.x   = spawn.x;
            p.y   = spawn.y;
            p._px = -1; p._py = -1; p._ab = -1;
        }

        this.broadcastRaw(JSON.stringify({
            t    : 'start',
            ph   : this.phase,
            tm   : this.timer,
            cHps : [this.cores[0].hp, this.cores[1].hp],
            fcts : this.teamFactions,   // resolved factions — ['roe','bgm'] etc.
            mapId  : this.mapDef.id,
            mapName: this.mapDef.name,
            wz     : this.mapDef.waterZones,
            spawns : this.mapDef.spawns,
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
                    player.burnUntil = 0;
                    player._lastBurnTick = 0;
                    const spawn = this.mapDef.spawns[player.team];
                    player.x  = spawn.x;
                    player.y  = spawn.y;
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
                } else if (b.type === 't') {
                    const tr = 18 + player.r;
                    if (Math.hypot(nx - b.x, player.y - b.y) < tr) nx = player.x;
                    if (Math.hypot(player.x - b.x, ny - b.y) < tr) ny = player.y;
                }
            }

            // Water zone collision — push back if entering water
            if (this.isOnWater(nx, player.y, player.r)) nx = player.x;
            if (this.isOnWater(player.x, ny, player.r)) ny = player.y;

            player.x = nx;
            player.y = ny;

            // ── Burn DoT (Molten Projector) ────────────────────────────────────
            if (player.burnUntil > now && this.phase === PH.ATTACK) {
                if (!player._lastBurnTick || now - player._lastBurnTick >= 500) {
                    player._lastBurnTick = now;
                    player.hp -= 5;
                    if (player.hp <= 0) {
                        player.hp = 0; player.rt = 5; player.burnUntil = 0;
                        this.events.push({ e: EV.PLAYER_DIE, i: player.id });
                    } else {
                        this.events.push({ e: EV.PLAYER_HIT, i: player.id, hp: player.hp });
                    }
                }
            }

            if (this.phase === PH.ATTACK && player.inp.sh && now - player.lastShot > 200) {
                player.lastShot = now;
                this.spawnProjectile(player.x, player.y, player.a, player.team, player.id,
                    { spd: 700, dmg: 15, r: 5, life: 2.0, pt: 'pl' });
            }
        }

        if (this.phase === PH.ATTACK) {
            for (const b of this.buildings.values()) {
                if (b.type !== 't') continue;

                // ── Quarry Shield Node: buff nearby friendly structures ────────
                if (b.shield) {
                    const shieldRadius = b.range || 280;
                    if (now - (b.ls || 0) > 2000) {
                        b.ls = now;
                        for (const nb of this.buildings.values()) {
                            if (nb.team !== b.team || nb.id === b.id) continue;
                            if (dist(b.x, b.y, nb.x, nb.y) < shieldRadius) {
                                const heal = Math.min(nb.maxHp - nb.hp, 5);
                                if (heal > 0) {
                                    nb.hp += heal;
                                    this.events.push({ e: EV.BUILD_HIT, i: nb.id, hp: nb.hp });
                                }
                            }
                        }
                    }
                    continue;
                }

                // ── Dominion Battery: buff nearby EPA turrets (fire rate + dmg) ─
                if (b.dominion) {
                    const domRadius = b.range || 500;
                    const epaSubtypes = new Set(['epa_t','epa_mk2','epa_mk3','epa_fortress','epa_knox','epa_dominion','epa_citadel']);
                    if (now - (b.ls || 0) > 4000) {
                        b.ls = now;
                        for (const nb of this.buildings.values()) {
                            if (nb.id === b.id || nb.team !== b.team) continue;
                            if (!epaSubtypes.has(nb.subtype)) continue;
                            if (dist(b.x, b.y, nb.x, nb.y) >= domRadius) continue;
                            // Apply temporary buff: reduce fireRate by 15%, +2 dmg for 3s
                            nb._domBuff = now + 3000;
                        }
                    }
                }

                // ── Intercept: chance to destroy incoming enemy projectiles ──────
                if ((b.intercept || 0) > 0) {
                    const intR = 140;
                    for (const [pid, p] of this.projs) {
                        if (p.team === b.team) continue;
                        if (dist(b.x, b.y, p.x, p.y) < intR) {
                            if (Math.random() < (b.intercept || 0)) {
                                this.projs.delete(pid);
                                this.events.push({ e: EV.PROJ_DESTROY, i: pid });
                            }
                        }
                    }
                }

                const fireRate = b.fireRate || 800;
                const buffedFireRate = (b._domBuff && b._domBuff > now) ? Math.round(fireRate * 0.85) : fireRate;
                if (now - (b.ls || 0) <= buffedFireRate) continue;
                const range  = b.range  || 400;
                const target = this.findClosestEnemy(b.x, b.y, b.team, range);
                if (!target) continue;
                b.a  = Math.atan2(target.y - b.y, target.x - b.x);
                b.ls = now;

                // ── Drill Turret: direct continuous-hit beam (no projectile) ──
                if (b.drill) {
                    const dmg = b.dmgVal || 8;
                    // Armour shred: apply bonus dmg vs buildings in beam path
                    target.hp -= dmg;
                    if (target.hp <= 0) {
                        target.hp = 0; target.rt = 5;
                        this.events.push({ e: EV.PLAYER_DIE, i: target.id });
                    } else {
                        this.events.push({ e: EV.PLAYER_HIT, i: target.id, hp: target.hp });
                    }
                    // Also shred first building in path (bonus dmg)
                    for (const [bid, bld] of this.buildings) {
                        if (bld.team === b.team) continue;
                        const d = dist(b.x, b.y, bld.x, bld.y);
                        if (d < range) {
                            const shredDmg = Math.round(dmg * 1.8);
                            bld.hp -= shredDmg;
                            if (bld.hp <= 0) {
                                this.buildings.delete(bid);
                                this.events.push({ e: EV.BUILD_DESTROY, i: bid });
                            } else {
                                this.events.push({ e: EV.BUILD_HIT, i: bid, hp: bld.hp });
                            }
                            break;
                        }
                    }
                    // Emit a special drill beam event so client can draw it
                    this.events.push({ e: EV.PROJ_SPAWN, i: shortId(),
                        x: Math.round(b.x), y: Math.round(b.y),
                        a: +b.a.toFixed(4), tm: b.team, spd: 9999, r: 4,
                        pt: 'bgm_drill' });
                    continue;
                }

                const opts = {
                    spd: b.projSpd || 400, dmg: b.dmgVal || 10, r: b.projR || 5, life: 2.0,
                    slow: b.slow || false, splash: b.splash || 0,
                    bonusVsBldg: b.bonusVsBldg || false,
                    burn: b.burn || false,
                    pt: b.subtype || 't',
                };

                // Citadel: shield pulse on self every 5s + fire at up to 3 targets
                if (b.citadel) {
                    if (!b._citadelPulse || now - b._citadelPulse > 5000) {
                        b._citadelPulse = now;
                        const healAmt = Math.min(b.maxHp - b.hp, 12);
                        if (healAmt > 0) {
                            b.hp += healAmt;
                            this.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
                        }
                    }
                    // Find up to 3 enemy targets
                    const targets = this.findMultipleEnemies(b.x, b.y, b.team, b.range || 560, 3);
                    for (const t of targets) {
                        const ta = Math.atan2(t.y - b.y, t.x - b.x);
                        this.spawnProjectile(b.x, b.y, ta, b.team, b.id, opts);
                    }
                    continue;
                }

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
                        // Burn: apply DoT for 3 seconds (6 ticks of 5dmg at 500ms)
                        if (p.burn) target.burnUntil = now + 3000;
                        if (target.hp <= 0) {
                            target.hp = 0;
                            target.rt = 5;
                            this.events.push({ e: EV.PLAYER_DIE, i: target.id });
                        } else {
                            this.events.push({ e: EV.PLAYER_HIT, i: target.id, hp: target.hp, pt: p.pt });
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
                            // Citadel buff: temporarily lower incoming damage by 15%
                            const citBuff  = (b.citadelBuff && b._citadelBuff > now) ? 0.85 : 1.0;
                            const finalDmg = Math.round(dmg * citBuff);
                            b.hp -= finalDmg;
                            dead = true; hitSomething = true;
                            // Thermal wall: reflect partial damage back toward attacker
                            if (b.thermal && b.hp > 0) {
                                const reflected = Math.round(finalDmg * 0.25);
                                const backAngle = Math.atan2(p.y - b.y, p.x - b.x);
                                this.spawnProjectile(b.x, b.y, backAngle, b.team, b.id,
                                    { spd: 320, dmg: reflected, r: 5, life: 1.2, pt: 'bgm_w_thermal' });
                            }
                            // Bastion damage share: distribute 20% of damage to nearby EPA walls
                            if (b.damageShare && finalDmg > 0) {
                                const shareR   = 200;
                                const shareDmg = Math.round(finalDmg * 0.20);
                                if (shareDmg > 0) {
                                    for (const [nbid, nb] of this.buildings) {
                                        if (nbid === bid || nb.team !== b.team || nb.type !== 'w') continue;
                                        if (dist(b.x, b.y, nb.x, nb.y) >= shareR) continue;
                                        nb.hp -= shareDmg;
                                        if (nb.hp <= 0) {
                                            this.buildings.delete(nbid);
                                            this.events.push({ e: EV.BUILD_DESTROY, i: nbid });
                                        } else {
                                            this.events.push({ e: EV.BUILD_HIT, i: nbid, hp: nb.hp });
                                        }
                                    }
                                }
                            }
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

        // ── BGM wall special tick (every 3s) ──────────────────────────────────
        if (this.phase === PH.ATTACK && this.tickCount % 90 === 0) {
            for (const b of this.buildings.values()) {
                if (b.type !== 'w') continue;

                // Thermal Wall — damages nearby enemies slightly each tick
                if (b.thermal) {
                    const thermalR = 80;
                    for (const target of this.players.values()) {
                        if (target.rt > 0 || target.team === b.team) continue;
                        if (dist(b.x, b.y, target.x, target.y) < thermalR + target.r) {
                            const reflected = 4;
                            target.hp -= reflected;
                            if (target.hp <= 0) {
                                target.hp = 0; target.rt = 5;
                                this.events.push({ e: EV.PLAYER_DIE, i: target.id });
                            } else {
                                this.events.push({ e: EV.PLAYER_HIT, i: target.id, hp: target.hp });
                            }
                        }
                    }
                }

                // Conduit Wall — pulses HP regen to nearby BGM structures
                if (b.conduit) {
                    const conduitR = 200;
                    const bgmSubtypes = new Set(['bgm_t','bgm_drill','bgm_rail','bgm_molt','bgm_qsn',
                                                 'bgm_w','bgm_w_blast','bgm_w_thermal','bgm_w_anchor','bgm_w_conduit']);
                    for (const nb of this.buildings.values()) {
                        if (nb.id === b.id || nb.team !== b.team) continue;
                        if (!bgmSubtypes.has(nb.subtype)) continue;
                        if (dist(b.x, b.y, nb.x, nb.y) >= conduitR) continue;
                        const heal = Math.min(nb.maxHp - nb.hp, 6);
                        if (heal > 0) {
                            nb.hp += heal;
                            this.events.push({ e: EV.BUILD_HIT, i: nb.id, hp: nb.hp });
                        }
                    }
                }

                // ── EPA wall mechanics ────────────────────────────────────────────
                // Bulwark+ regen: slow self-repair
                if (b.regen) {
                    const heal = Math.min(b.maxHp - b.hp, 4);
                    if (heal > 0) {
                        b.hp += heal;
                        this.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
                    }
                }

                // Citadel/Bastion aura: buff nearby EPA walls' exploResist
                if (b.citadelAura) {
                    const auraR = 180;
                    const epaWallSubs = new Set(['epa_w','epa_w_fort','epa_w_bulwark','epa_w_guardian','epa_w_citadel','epa_w_bastion']);
                    for (const nb of this.buildings.values()) {
                        if (nb.id === b.id || nb.team !== b.team) continue;
                        if (!epaWallSubs.has(nb.subtype)) continue;
                        if (dist(b.x, b.y, nb.x, nb.y) >= auraR) continue;
                        // Mark as citadel-buffed for 4s (reduces effective exploResist)
                        nb._citadelBuff = now + 4000;
                    }
                }
            }
        }

        // ── EPA wall intercept: chance to destroy nearby enemy projectiles ──────
        if (this.phase === PH.ATTACK) {
            for (const b of this.buildings.values()) {
                if (b.type !== 'w' || !b.intercept || b.team === undefined) continue;
                const intR = 100;
                for (const [pid, p] of this.projs) {
                    if (p.team === b.team) continue;
                    if (dist(b.x, b.y, p.x, p.y) < intR) {
                        if (Math.random() < b.intercept) {
                            this.projs.delete(pid);
                            this.events.push({ e: EV.PROJ_DESTROY, i: pid });
                        }
                    }
                }
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
            burn: opts.burn || false,
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
        if (this.phase !== PH.BUILD && this.phase !== PH.ATTACK) return;

        const faction = FACTIONS[this.teamFactions[player.team]] || FACTIONS['roe'];
        const cost = req.bt === 'w' ? faction.wallCost : faction.turretCost;
        if (player.res < cost) return;

        const mid    = MAP_W / 2;
        const onRed  = req.x < mid - 50;
        const onBlue = req.x > mid + 50;
        if ((player.team === 0 && !onRed) || (player.team === 1 && !onBlue)) return;

        const core = this.cores[player.team];
        if (dist(req.x, req.y, core.x, core.y) < 150) return;

        // Can't build on water
        if (this.isOnWater(req.x, req.y, 25)) return;

        const id = shortId();
        let b;
        if (req.bt === 'w') {
            const baseSt = faction.baseWall || 'w';
            const wdef   = WALL_DEFS[baseSt];
            b = { id, type: 'w', subtype: baseSt, team: player.team, x: req.x, y: req.y,
                  hp: wdef.hp, maxHp: wdef.hp, exploResist: wdef.exploResist || 1.0,
                  thermal: wdef.thermal || false,
                  conduit: wdef.conduit || false,
                  regen: wdef.regen || false,
                  intercept: wdef.intercept || 0,
                  citadelAura: wdef.citadelAura || false,
                  damageShare: wdef.damageShare || false };
        } else if (req.bt === 't') {
            const baseSt = faction.baseTurret || 't';
            const def = TURRET_DEFS[baseSt];
            b = {
                id, type: 't', subtype: baseSt, team: player.team,
                x: req.x, y: req.y, r: 20,
                hp: def.hp, maxHp: def.hp, a: 0, ls: 0,
                fireRate: def.fireRate, dmgVal: def.dmg, range: def.range,
                projSpd: def.projSpd, projR: def.projR,
                slow: false, dual: false, splash: 0, bonusVsBldg: false,
                drill: def.drill || false, burn: def.burn || false, shield: def.shield || false,
                intercept: def.intercept || 0, dominion: def.dominion || false, citadel: def.citadel || false,
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

        // Only factions with upgrades can upgrade
        const faction = FACTIONS[this.teamFactions[player.team]] || FACTIONS['roe'];
        if (!faction.hasUpgrades) return;

        const def = TURRET_DEFS[req.to];
        if (!def || def.upgFrom !== b.subtype) return;

        // Cross-faction guard: each faction's turrets stay in their own tree
        const bgmTypes = new Set(['bgm_t','bgm_drill','bgm_rail','bgm_molt','bgm_qsn']);
        const epaTypes = new Set(['epa_t','epa_mk2','epa_mk3','epa_fortress','epa_knox','epa_dominion','epa_citadel']);
        const targetIsBgm = bgmTypes.has(req.to);
        const currentIsBgm = bgmTypes.has(b.subtype);
        const targetIsEpa = epaTypes.has(req.to);
        const currentIsEpa = epaTypes.has(b.subtype);
        if (targetIsBgm !== currentIsBgm) return;
        if (targetIsEpa !== currentIsEpa) return;

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
        b.drill        = def.drill       || false;
        b.burn         = def.burn        || false;
        b.shield       = def.shield      || false;
        b.intercept    = def.intercept   || 0;
        b.dominion     = def.dominion    || false;
        b.citadel      = def.citadel     || false;

        this.events.push({ e: EV.TURRET_UPGRADE, i: b.id, st: b.subtype, hp: b.hp, mhp: b.maxHp });
        this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
    }

    handleWallUpgrade(player, req) {
        if (this.phase !== PH.BUILD && this.phase !== PH.ATTACK) return;
        const b = this.buildings.get(req.id);
        if (!b || b.type !== 'w' || b.team !== player.team) return;

        const faction = FACTIONS[this.teamFactions[player.team]] || FACTIONS['roe'];
        if (!faction.hasUpgrades) return;

        const def = WALL_DEFS[req.to];
        if (!def || def.upgFrom !== b.subtype) return;

        // Cross-faction guard: BGM walls stay BGM, EPA walls stay EPA
        const bgmWalls = new Set(['bgm_w','bgm_w_blast','bgm_w_thermal','bgm_w_anchor','bgm_w_conduit']);
        const epaWalls = new Set(['epa_w','epa_w_fort','epa_w_bulwark','epa_w_guardian','epa_w_citadel','epa_w_bastion']);
        if (bgmWalls.has(req.to) !== bgmWalls.has(b.subtype)) return;
        if (epaWalls.has(req.to) !== epaWalls.has(b.subtype)) return;

        if (player.res < def.cost) return;

        player.res -= def.cost;
        const hpRatio    = b.hp / b.maxHp;
        b.subtype        = req.to;
        b.maxHp          = def.hp;
        b.hp             = Math.max(1, Math.round(def.hp * hpRatio));
        b.exploResist    = def.exploResist    !== undefined ? def.exploResist : 1.0;
        b.thermal        = def.thermal        || false;
        b.conduit        = def.conduit        || false;
        b.regen          = def.regen          || false;
        b.intercept      = def.intercept      || 0;
        b.citadelAura    = def.citadelAura    || false;
        b.damageShare    = def.damageShare    || false;

        this.events.push({ e: EV.WALL_UPGRADE, i: b.id, st: b.subtype, hp: b.hp, mhp: b.maxHp });
        this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
    }

    findMultipleEnemies(x, y, team, maxRange, count = 3) {
        const results = [];
        for (const p of this.players.values()) {
            if (p.rt > 0 || p.team === team) continue;
            if (dist(x, y, p.x, p.y) <= maxRange) results.push(p);
        }
        results.sort((a, b) => dist(x, y, a.x, a.y) - dist(x, y, b.x, b.y));
        return results.slice(0, count);
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
            const resist = (b.type === 'w' && b.exploResist !== undefined) ? b.exploResist : 1.0;
            b.hp -= Math.round(dmg * resist);
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

            } else if (data.t === 'wupg' && room) {
                const player = room.players.get(id);
                if (player) room.handleWallUpgrade(player, data);

            } else if (data.t === 'vote' && room && room.inVotePhase) {
                const player = room.players.get(id);
                if (player && ['roe', 'bgm', 'epa'].includes(data.f)) {
                    room.factionVotes[player.team].set(id, data.f);
                    room.broadcastFactionVotes();
                }

            } else if (data.t === 'mvote' && room && room.inVotePhase) {
                const player = room.players.get(id);
                const mapId  = +data.m;
                if (player && MAP_DEFS.some(m => m.id === mapId)) {
                    room.mapVotes.set(id, mapId);
                    room.broadcastMapVotes();
                }

            } else if (data.t === 'ready' && room) {
                const player = room.players.get(id);
                if (player && !room.inVotePhase && room.phase === PH.LOBBY) {
                    room.readyStates.set(id, !!data.r);
                    room.broadcastNames();   // re-send names so ready state propagates
                    room.evaluateLobby();
                }

            } else if (data.t === 'chat' && room) {
                const player = room.players.get(id);
                if (player && typeof data.msg === 'string') {
                    const safe = data.msg.slice(0, 120);
                    room.broadcastRaw(JSON.stringify({
                        t: 'chat', id, nm: player.name, team: player.team, msg: safe,
                    }));
                }
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
        if (r.phase === PH.LOBBY && !r.inVotePhase && r.players.size < MAX_PLAYERS) return id;
    }
    return crypto.randomBytes(4).toString('hex');
}

server.listen(PORT, '0.0.0.0', () => console.log(`Core Wars server on :${PORT}`));
