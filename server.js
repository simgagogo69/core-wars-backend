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
    const MAP_W       = 4800;
    const MAP_H       = 2000;
    const MAX_PLAYERS = 8;
    const BUILD_TIME  = 30;
    const LOBBY_TIME  = 120;  // 2-min lobby countdown (when 2+ players, nobody voted yet)
    const VOTE_TIME   = 25;   // seconds for the dedicated voting phase
    const WALL_W      = 16;   // wall collision box size — must match client GRID_SIZE
    const WALL_HALF   = 8;    // WALL_W / 2
    const VOTE_MAP_COUNT = 3; // how many maps to randomly offer in the vote

    // ─── Map definitions ─────────────────────────────────────────────────────────
    // waterZones: array of {x,y,w,h} rectangles that are impassable water
    // cores:  [{x,y},{x,y}] — red then blue
    // spawns: [{x,y},{x,y}] — red then blue
    const MAP_DEFS = [
        {
            id  : 0,
            name: 'WARZONE',
            // Open flat battlefield — no water, full strategic freedom
            waterZones: [],
            cores : [{ x: 260,        y: MAP_H / 2 }, { x: MAP_W - 260,  y: MAP_H / 2 }],
            spawns: [{ x: 500,        y: MAP_H / 2 }, { x: MAP_W - 500,  y: MAP_H / 2 }],
        },
        {
            id  : 1,
            name: 'ARCHIPELAGO',
            // Two home islands connected by THREE bridges — top, center, bottom.
            // Teams must contest multiple crossing points simultaneously.
            // Red island:  x:0-1300
            // Blue island: x:3500-4800
            // Bridge 1 (top):    y:100-550
            // Bridge 2 (center): y:800-1300
            // Bridge 3 (bottom): y:1560-1950
            waterZones: [
                // Open sea (top above bridge 1)
                { x: 1300, y: 0,    w: 2200, h: 100  },
                // Between bridge 1 and bridge 2
                { x: 1300, y: 550,  w: 2200, h: 250  },
                // Between bridge 2 and bridge 3
                { x: 1300, y: 1300, w: 2200, h: 260  },
                // Open sea (bottom below bridge 3)
                { x: 1300, y: 1950, w: 2200, h: 50   },
                // Red island corner coves (natural coastline)
                { x: 0,    y: 0,    w: 380,  h: 100  },
                { x: 0,    y: 1950, w: 380,  h: 50   },
                // Blue island corner coves
                { x: 4420, y: 0,    w: 380,  h: 100  },
                { x: 4420, y: 1950, w: 380,  h: 50   },
                // Small center lagoon on bridge 2 (creates mid-bridge chokepoint island)
                { x: 2050, y: 880,  w: 700,  h: 160  },
            ],
            cores : [{ x: 300,        y: MAP_H / 2 }, { x: MAP_W - 300,  y: MAP_H / 2 }],
            spawns: [{ x: 580,        y: MAP_H / 2 }, { x: MAP_W - 580,  y: MAP_H / 2 }],
        },
        {
            id  : 2,
            name: 'CHOKEPOINT',
            // Wide home bases. The center opens into two corridors (top and bottom lanes)
            // plus a narrow center land-bridge with a mid-map island platform.
            // Top lane:      y:180-700
            // Center narrow: y:900-1150  (plus island platform at center x)
            // Bottom lane:   y:1380-1900
            waterZones: [
                { x: 1500, y: 0,    w: 1800, h: 180  },   // top sea cap
                { x: 1500, y: 700,  w: 1800, h: 200  },   // between top and center
                // Center narrow flanked by water — island platform hole left open at x:2200-2600
                { x: 1500, y: 900,  w: 700,  h: 250  },   // center-left water
                { x: 2600, y: 900,  w: 700,  h: 250  },   // center-right water
                { x: 1500, y: 1150, w: 1800, h: 230  },   // between center and bottom
                { x: 1500, y: 1900, w: 1800, h: 100  },   // bottom sea cap
            ],
            cores : [{ x: 260,        y: MAP_H / 2 }, { x: MAP_W - 260,  y: MAP_H / 2 }],
            spawns: [{ x: 500,        y: MAP_H / 2 }, { x: MAP_W - 500,  y: MAP_H / 2 }],
        },
        {
            id  : 3,
            name: 'DELTA',
            // River delta — three parallel water channels cut through the center,
            // creating four land corridors of varying widths. Team bases are wide open.
            // Corridor 1 (top strip):    y:0-380
            // Channel 1:                 y:380-640
            // Corridor 2 (upper island): y:640-980
            // Channel 2 (center):        y:980-1260
            // Corridor 3 (lower island): y:1260-1600
            // Channel 3:                 y:1600-1860
            // Corridor 4 (bottom strip): y:1860-2000
            waterZones: [
                // Three channels in the central zone x:1250-3550
                { x: 1250, y: 380,  w: 2300, h: 260  },   // channel 1
                { x: 1250, y: 980,  w: 2300, h: 280  },   // channel 2 (widest)
                { x: 1250, y: 1600, w: 2300, h: 260  },   // channel 3
                // Side inlets from home islands into channels (natural estuary feel)
                { x: 980,  y: 0,    w: 270,  h: 380  },   // red top inlet
                { x: 980,  y: 1600, w: 270,  h: 400  },   // red bottom inlet
                { x: 3550, y: 0,    w: 270,  h: 380  },   // blue top inlet
                { x: 3550, y: 1600, w: 270,  h: 400  },   // blue bottom inlet
                // Small mid-channel rocks/islet that breaks channel 2 slightly
                { x: 2050, y: 980,  w: 50,   h: 280  },   // stub — splits ch2 to hint at islands
            ],
            cores : [{ x: 260,        y: MAP_H / 2 }, { x: MAP_W - 260,  y: MAP_H / 2 }],
            spawns: [{ x: 500,        y: MAP_H / 2 }, { x: MAP_W - 500,  y: MAP_H / 2 }],
        },
        {
            id  : 4,
            name: 'FRACTURE',
            // Two asymmetric deep-water lakes force diagonal routing.
            // Lake 1 (upper-left of center):  blocks direct top approach.
            // Lake 2 (lower-right of center): blocks direct bottom approach.
            // Three viable paths emerge:
            //   Top route:    sneak above lake 1 via upper-right corridor
            //   Center route: thread the gap between lakes (y:880-1120)
            //   Bottom route: sweep below lake 2 via lower-left corridor
            waterZones: [
                // Lake 1: upper-left center
                { x: 1380, y: 0,    w: 1480, h: 880  },
                // Lake 2: lower-right center
                { x: 1940, y: 1120, w: 1480, h: 880  },
                // Coastal inlets (give bases a rocky, eroded feel)
                { x: 0,    y: 0,    w: 220,  h: 360  },   // red top-left bay
                { x: 0,    y: 1700, w: 300,  h: 300  },   // red bottom bay
                { x: 4580, y: 0,    w: 220,  h: 300  },   // blue top bay
                { x: 4500, y: 1640, w: 300,  h: 360  },   // blue bottom-right bay
            ],
            cores : [{ x: 260,        y: MAP_H / 2 }, { x: MAP_W - 260,  y: MAP_H / 2 }],
            spawns: [{ x: 500,        y: MAP_H / 2 }, { x: MAP_W - 500,  y: MAP_H / 2 }],
        },
    ];

    const PH = { LOBBY: 0, BUILD: 1, ATTACK: 2, END: 3, OPERATOR_SELECT: 4 };

    // ─── Operator Select phase duration ──────────────────────────────────────────
    const OPERATOR_SELECT_TIME = 30;  // seconds players have to pick operator + weapon

    // Rotate a local-space muzzle offset (mx, my) by angle and add to world position (cx, cy)
    function muzzleWorld(cx, cy, angle, mx, my) {
        const c = Math.cos(angle), s = Math.sin(angle);
        return { x: cx + c * mx - s * my, y: cy + s * mx + c * my };
    }

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
        VEHICLE_SPAWN  : 16,   // vehicle placed/spawned
        VEHICLE_DESTROY: 17,   // vehicle destroyed
        VEH_BRACE      : 18,   // Knox Guardian brace activated
        // ── Operator system events ──────────────────────────────────────────────
        LOADOUT_SYNC   : 19,   // broadcast all players' locked loadouts when phase begins
        ABILITY_USED   : 20,   // a player activated their active ability
        ABILITY_READY  : 21,   // player's ability cooldown expired (server tells client)
        SUPP_DEVICE    : 22,   // Duster deploys a suppression device
        REPAIR_DRONE   : 23,   // Orion deploys a repair drone
        SCOUT_DRONE    : 24,   // Daemon launches a scout drone
        SHIELD_EMITTER : 25,   // Konig projects a stationary energy shield
        PINCER_DASH    : 26,   // Blackjack dash — tells clients to render trail
        DRONE_HIT      : 27,   // Scout/repair drone took damage
        DRONE_DESTROY  : 28,   // Scout/repair drone destroyed
        // ── EPA Operator events ─────────────────────────────────────────────────
        PLASMA_DASH        : 29,   // Hellhound dash trail VFX
        REPAIR_BAY         : 30,   // Velarus deploys repair bay (wall + repair device)
        RESTORATION_FIELD  : 31,   // Caesarium creates a healing restoration field
        // ── Infantry system events ──────────────────────────────────────────────
        INF_SPAWN          : 32,   // infantry unit spawned
        INF_DIE            : 33,   // infantry unit died
        SHOUT              : 34,   // player shout — attach nearby infantry
        BK_MODE            : 35,   // barracks infantry-mode changed
        INF_HIT            : 36,   // infantry took damage and survived (HP update only)
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

    // ─── Vehicle definitions ──────────────────────────────────────────────────────
    // r: collision radius | spd: movement speed (px/s)
    // driverFireRate: ms between driver (hull) shots
    // passengerFireRate: ms between passenger (main gun) shots
    // passengerSplash: AoE radius on passenger shell impact (0 = none)
    // passengerSlow: applies movement slow on passenger hit
    const VEHICLE_DEFS = {
        // ── ROE ───────────────────────────────────────────────────────────────────
        'roe_breaker': {
            name: 'Breaker SPG', hp: 600, maxHp: 600, spd: 110, r: 54,
            turnRate: 1.8,   // radians per second — heavy SPG turns slowly
            spawnCost: 70,
            // Hull MG (driver) — fixed forward on hull
            driverFireRate : 450, driverDmg: 10, driverProjSpd: 620, driverProjR: 4,
            driverMuzzle: { x: 55, y: 0 },     // hull MG tip (forward of hull)
            // Siege cannon (passenger) — rotates on turret
            passengerFireRate: 2800, passengerDmg: 90, passengerProjSpd: 380,
            passengerProjR: 13, passengerSplash: 130,
            passengerMuzzle: { x: 98, y: 0 },  // end of muzzle brake
        },
        'roe_suppressor': {
            name: 'Suppressor Carrier', hp: 420, maxHp: 420, spd: 155, r: 48,
            turnRate: 2.5,   // lighter APC turns faster
            spawnCost: 50,
            // Single forward MG (driver)
            driverFireRate : 220, driverDmg: 7, driverProjSpd: 660, driverProjR: 4,
            driverMuzzle: { x: 50, y: 0 },
            // Twin rotary cannons (passenger) — high fire rate, slows enemies
            passengerFireRate: 140, passengerDmg: 5, passengerProjSpd: 540,
            passengerProjR: 5, passengerSlow: true,
            passengerMuzzle: { x: 73, y: 0 },  // flash suppressor tip (avg of twin barrels)
        },
        // ── BGM Corp ──────────────────────────────────────────────────────────────
        'bgm_prospector': {
            name: 'BGM-4 Prospector', hp: 380, maxHp: 380, spd: 145, r: 34,
            turnRate: 0, spawnCost: 55,
            isMech: true, singlePilot: true,
            // Main weapon: Thermal Rivet Gun (driver shoots on aim)
            driverFireRate: 420, driverDmg: 18, driverProjSpd: 520, driverProjR: 5,
            driverBurnChance: 0.25,   // 25% chance to apply burn on hit
            driverMuzzle: { x: 56, y: 0 },   // rivet gun tip in aim-space
            // MK-A Utility Drone — light auto MG
            drone: 'mk_a',
            droneFireRate: 600, droneDmg: 5, droneProjSpd: 580, droneProjR: 3,
            droneRange: 280, droneOrbitR: 52, droneOrbitSpd: 1.6,
        },
        'bgm_tunnelrat': {
            name: 'BGM-7 Tunnelrat', hp: 320, maxHp: 320, spd: 175, r: 32,
            turnRate: 0, spawnCost: 48,
            isMech: true, singlePilot: true,
            // Main weapon: Rotary Cutter — melee arc, spawns a very short-range slash projectile
            driverFireRate: 110, driverDmg: 9, driverProjSpd: 380, driverProjR: 6,
            driverMaxRange: 200,      // effective range; life capped to reach this
            driverBonusVsWall: 2.2,   // massive bonus vs walls
            driverMuzzle: { x: 55, y: 0 },   // cutter disc centre in aim-space
            isMeleeDriver: true,              // client renders as slash arc, not bullet
            // MK-A Utility Drone
            drone: 'mk_a',
            droneFireRate: 600, droneDmg: 5, droneProjSpd: 580, droneProjR: 3,
            droneRange: 280, droneOrbitR: 48, droneOrbitSpd: 2.0,
        },
        'bgm_hauler': {
            name: 'BGM-12 Hauler Frame', hp: 680, maxHp: 680, spd: 100, r: 52,
            turnRate: 0, spawnCost: 80,
            isMech: true, singlePilot: false,
            // Driver: hull MG — arm at (16,-24), twin barrels tip at (54,-30)
            driverFireRate: 300, driverDmg: 8, driverProjSpd: 640, driverProjR: 4,
            driverMuzzle: { x: 54, y: -30 },
            // Passenger: Mag Loader Cannon — arm at (16,24), barrel tip at (82,32)
            passengerFireRate: 2200, passengerDmg: 72, passengerProjSpd: 400,
            passengerProjR: 11, passengerSplash: 90,
            passengerMuzzle: { x: 82, y: 32 },
            // MK-B Escort Drone — larger, better tracking
            drone: 'mk_b',
            droneFireRate: 380, droneDmg: 8, droneProjSpd: 620, droneProjR: 4,
            droneRange: 340, droneOrbitR: 68, droneOrbitSpd: 1.3,
        },
        'epa_citadel_interceptor': {
            name: 'Citadel Interceptor', hp: 480, maxHp: 480, spd: 130, r: 50,
            turnRate: 2.2, spawnCost: 65,
            // Driver: precision tracking burst — twin burst guns, emitter tip at (52,0)
            driverFireRate: 300, driverDmg: 6, driverProjSpd: 700, driverProjR: 4,
            driverIntercept: true,
            driverMuzzle: { x: 52, y: 0 },   // avg of twin barrel emitter tips
            // Passenger: Citadel Array — array emitter barrel tip at (54,0)
            passengerFireRate: 220, passengerDmg: 8, passengerProjSpd: 680, passengerProjR: 5,
            passengerIntercept: 0.55,
            passengerInterceptR: 160,
            passengerMuzzle: { x: 54, y: 0 },
        },
        'epa_knox_guardian': {
            name: 'Knox Guardian', hp: 900, maxHp: 900, spd: 72, r: 60,
            turnRate: 0.9, spawnCost: 90,
            // Driver: brace — no projectile, no muzzle needed
            driverFireRate: 2200, driverDmg: 0, driverProjSpd: 0, driverProjR: 0,
            driverBrace: true,
            braceRadius: 240, braceDmgReduce: 0.45, braceBuildHeal: 4, braceDuration: 2500,
            // Passenger: heavy barrier cannon — wide muzzle emitter tip at (94,0)
            passengerFireRate: 1800, passengerDmg: 65, passengerProjSpd: 320,
            passengerProjR: 14, passengerSplash: 80,
            passengerPierce: true,
            passengerMuzzle: { x: 94, y: 0 },
        },
    };

    // Map faction → available vehicle types (empty = no vehicles yet)
    const FACTION_VEHICLES = {
        'roe': ['roe_breaker', 'roe_suppressor', 'roe_apc'],
        'bgm': ['bgm_prospector', 'bgm_tunnelrat', 'bgm_hauler', 'bgm_apc'],
        'epa': ['epa_citadel_interceptor', 'epa_knox_guardian', 'epa_apc'],
    };

    // ─── Faction APC definitions ──────────────────────────────────────────────────
    // ROE: Mammoth Transport Truck — fast, wheeled, light armour
    VEHICLE_DEFS['roe_apc'] = {
        name: 'Mammoth Transport', faction: 'roe',
        hp: 300, maxHp: 300, spd: 165, r: 34,
        turnRate: 2.7, spawnCost: 50,
        driverFireRate: 380, driverDmg: 7, driverProjSpd: 580, driverProjR: 4,
        driverMuzzle: { x: 38, y: 0 },
        isAPC: true, infantryCapacity: 4, singlePilot: true,
    };
    // BGM: Yukon Walker Carrier — slow, heavy, best armour
    VEHICLE_DEFS['bgm_apc'] = {
        name: 'Yukon Carrier', faction: 'bgm',
        hp: 420, maxHp: 420, spd: 140, r: 42,
        turnRate: 1.9, spawnCost: 50,
        driverFireRate: 520, driverDmg: 10, driverProjSpd: 510, driverProjR: 5,
        driverMuzzle: { x: 48, y: 0 },
        isAPC: true, infantryCapacity: 4, singlePilot: true,
        isMech: true,   // biped walker — uses WASD strafe, not tank steering
    };
    // EPA: Knox Hovercarrier — fastest, lightest, energy weapons
    VEHICLE_DEFS['epa_apc'] = {
        name: 'Knox Hovercarrier', faction: 'epa',
        hp: 250, maxHp: 250, spd: 195, r: 40,
        turnRate: 3.2, spawnCost: 50,
        driverFireRate: 300, driverDmg: 6, driverProjSpd: 640, driverProjR: 4,
        driverMuzzle: { x: 44, y: 0 },
        isAPC: true, infantryCapacity: 4, singlePilot: true,
    };

    // Vehicle depot build cost
    const VEHICLE_DEPOT_COST = 80;

    // ─── Infantry System Constants ────────────────────────────────────────────────
    const INF_MAX_SUPPLY        = 20;     // supply pool cap per team
    const INF_PROD_AMOUNT       = 2;      // units auto-produced per barracks cycle
    const INF_PROD_INTERVAL     = 4000;   // ms between barracks production cycles
    const INF_AI_HZ             = 5;      // AI update rate (times per second)
    const INF_AI_TICKS          = Math.round(TICK_RATE / INF_AI_HZ); // = 6

    // ─── Infantry Networking Thresholds ──────────────────────────────────────────
    // Tune these to balance visual fidelity vs. bandwidth. Raising them reduces
    // network traffic; lowering them increases smoothness.
    const INF_POS_THRESHOLD        = 4;    // px  — min position Δ per axis to transmit a move update
    const INF_ANGLE_BYTE_THRESHOLD = 4;    // enc — min encoded-angle Δ to transmit (out of 256; ≈5°)
    const INF_INTEREST_RADIUS      = 1400; // px  — clients only receive infantry within this radius
    const FOLLOW_RANGE          = 340;    // px; infantry detach beyond this
    const SHOUT_RADIUS          = 160;    // px; shout effect radius
    const APC_BOARD_RANGE       = 110;    // px; infantry auto-board within this distance
    const BARRACKS_COST         = 35;     // scrap cost to build barracks
    const APC_FROM_BARRACKS_COST= 50;     // scrap cost to call APC from barracks
    const BARRACKS_APC_COOLDOWN = 4000;   // ms between APC productions from barracks

    // ─── Infantry Type Definitions ────────────────────────────────────────────────
    // spd is in px/s.  Players run at 250 so all infantry are intentionally slower.
    const INFANTRY_DEFS = {
        'grunt': {
            name  : 'Grunt',
            hp    : 60,  maxHp: 60,  r: 9,
            spd   : 72,                         // ~29% of player speed
            dmg   : 8,   fireRate: 750, range: 270,
            projSpd: 480, projR: 4,
            cost  : 1,                           // 1 supply unit
        },
        'heavy': {
            name  : 'Heavy Gunner',
            hp    : 120, maxHp: 120, r: 11,
            spd   : 55,                         // ~22% of player speed
            dmg   : 14,  fireRate: 950, range: 220,
            projSpd: 460, projR: 5,
            cost  : 1,
        },

        // ── ROE infantry ───────────────────────────────────────────────────────
        // Mass-produced wartime automatons. Cheap, practical, intentionally weak.

        'roe_rifle': {
            name  : 'R-K9 Rifle Unit',
            hp    : 55,  maxHp: 55,  r: 9,
            spd   : 70,                         // frontline pusher, practical gait
            dmg   : 7,   fireRate: 580, range: 300, // fast suppressive bursts
            projSpd: 490, projR: 3,             // lightweight rifle round
            cost  : 1,
        },
        'roe_breach': {
            name  : 'R-K12 Breach Unit',
            hp    : 70,  maxHp: 70,  r: 10,
            spd   : 63,                         // heavier chest plate slows it
            dmg   : 18,  fireRate: 860, range: 145, // hits hard up close, useless at range
            projSpd: 360, projR: 5,             // wide slow scatter — shotgun feel
            cost  : 1,
        },

        // ── BGM infantry ───────────────────────────────────────────────────────
        // Armed industrial machinery. Mining equipment retrofitted for combat.

        'bgm_bur3': {
            name  : 'BUR-3 Excavation Unit',
            hp    : 65,  maxHp: 65,  r: 10,
            spd   : 66,                         // quadruped — steady but not agile
            dmg   : 9,   fireRate: 700, range: 280, // reliable automatic fire
            projSpd: 470, projR: 4,             // standard rifle round
            cost  : 1,
        },
        'bgm_bur8': {
            name  : 'BUR-8 Tunnel Hound',
            hp    : 52,  maxHp: 52,  r: 9,
            spd   : 82,                         // faster — built for aggressive forward pushes
            dmg   : 6,   fireRate: 420, range: 210, // rapid SMG: low dmg, very fast fire rate
            projSpd: 520, projR: 3,             // fast light rounds, wide spread
            cost  : 1,
        },

        // ── EPA infantry ───────────────────────────────────────────────────────
        // Precision-manufactured security automatons. Disciplined and coordinated.

        'epa_guardian': {
            name  : 'EPA Guardian Unit',
            hp    : 60,  maxHp: 60,  r: 9,
            spd   : 65,                         // deliberate positioning, not a sprinter
            dmg   : 11,  fireRate: 950, range: 320, // disciplined burst — fewer shots, accurate
            projSpd: 520, projR: 3,             // fast precise round
            cost  : 1,
        },
        'epa_intercept': {
            name  : 'EPA Intercept Unit',
            hp    : 50,  maxHp: 50,  r: 8,
            spd   : 70,                         // repositions to cover threatened areas
            dmg   : 6,   fireRate: 680, range: 230, // lightweight carbine — secondary role
            projSpd: 490, projR: 3,
            cost  : 1,
            // ── Interception parameters ────────────────────────────────────────
            interceptRadius  : 80,              // px — modest area, not a full denial zone
            interceptChance  : 0.15,            // 15% per qualifying projectile in radius
            interceptCooldown: 4000,            // ms — 4s between successful intercepts
        },
    };

    // ── Per-type barrel tip offsets (local space: +X = forward, +Y = right) ────
    // Bullets spawn from the visual barrel tip, not the unit's movement centre.
    // bx/by are in aim-angle local coordinates — the same frame the client draws in —
    // so the visual barrel tip and the bullet spawn point always match.
    const INFANTRY_BARREL = {
        'grunt'         : { bx: 20,    by:  0    },
        'heavy'         : { bx: 21,    by:  0    },
        'roe_rifle'     : { bx: 21,    by: -10   }, // side-mounted upper edge
        'roe_breach'    : { bx: 20,    by:  0    },
        'bgm_bur3'      : { bx: 19,    by: -12.8 }, // side-mounted rifle upper edge
        'bgm_bur8'      : { bx: 21,    by:  0    },
        'epa_guardian'  : { bx: 23,    by: -2    }, // precision barrel, slightly above centre
        'epa_intercept' : { bx: 18,    by:  2    }, // carbine, below centre
    };


    // hasUpgrades: only ROE gets the turret/wall upgrade tree
    // wallCost / turretCost: initial build price for this faction
const FACTIONS = {
    'roe': {
        name: 'Revolutionaries of Earth', short: 'ROE',
        wallCost: 8, turretCost: 25, hasUpgrades: true, baseTurret: 't', baseWall: 'w',
        color: '#f59e0b',
        desc: 'Jack of all trades. Cheap defenses, full upgrade tree. No clear weakness, no clear strength.',
    },
    'bgm': {
        name: 'BlackenedGods Mining Corp', short: 'B.G.M. Corp',
        wallCost: 15, turretCost: 35, hasUpgrades: true, baseTurret: 'bgm_t', baseWall: 'bgm_w',
        color: '#f97316',
        desc: 'Industrial powerhouse. Heavy costs, unique turret upgrade tree. Built like a fortress.',
    },
    'epa': {
        name: 'Earthen Protection Agency', short: 'EPA',
        wallCost: 12, turretCost: 28, hasUpgrades: true, baseTurret: 'epa_t', baseWall: 'epa_w',
        color: '#22d3ee',
        desc: 'Precision agency. Accurate, durable, expensive. Unique upgrade tree built for sustained defence.',
    },
};


    // ═══════════════════════════════════════════════════════════════════════════
    // WEAPON SYSTEM
    //
    // All weapons are defined here in a flat table. Operators reference weapon
    // IDs from this table — no duplicated stats per-operator.
    //
    // Weapon categories:
    //   assault_rifle, battle_rifle, submachine_gun, light_machine_gun,
    //   heavy_machine_gun, sniper_rifle, designated_marksman_rifle,
    //   pistol, revolver, shotgun
    //
    // How weapons plug into gameplay:
    //   • player.weaponId → key into WEAPON_DEFS
    //   • player.lastShot is compared against weapon.fireRate (ms)
    //   • damage, projSpd, spread etc. feed into spawnProjectile()
    //   • client reads weapon from LOADOUT_SYNC event and renders accordingly
    //
    // To add a new weapon: add an entry here. Zero other code changes needed.
    // ═══════════════════════════════════════════════════════════════════════════
    const WEAPON_DEFS = {
        // ── Assault Rifles ────────────────────────────────────────────────────
        'ar_standard': {
            id: 'ar_standard', name: 'M7 Carbine', category: 'assault_rifle',
            dmg: 15, fireRate: 110, reloadTime: 2200,
            projSpd: 700, projR: 4, spread: 0.04,
            magSize: 30, range: 600, falloffStart: 400,
            moveSpeedMult: 1.00,  // baseline
        },

        // ── Battle Rifles ─────────────────────────────────────────────────────
        'br_precision': {
            id: 'br_precision', name: 'Ravager BR', category: 'battle_rifle',
            dmg: 28, fireRate: 280, reloadTime: 2600,
            projSpd: 800, projR: 4, spread: 0.02,
            magSize: 20, range: 750, falloffStart: 500,
            moveSpeedMult: 0.93,  // heavier, longer
        },
        // ── Submachine Guns ───────────────────────────────────────────────────
        // NOTE: SMGs are shared between Engineer and Support operators
        'smg_compact': {
            id: 'smg_compact', name: 'Phantom SMG', category: 'submachine_gun',
            dmg: 10, fireRate: 75, reloadTime: 1800,
            projSpd: 640, projR: 3, spread: 0.09,
            magSize: 35, range: 400, falloffStart: 250,
            moveSpeedMult: 1.10,  // compact and light
        },

        // ── Light Machine Guns ────────────────────────────────────────────────
        'lmg_standard': {
            id: 'lmg_standard', name: 'Torrent LMG', category: 'light_machine_gun',
            dmg: 13, fireRate: 90, reloadTime: 3800,
            projSpd: 660, projR: 4, spread: 0.10,
            magSize: 75, range: 500, falloffStart: 350,
            moveSpeedMult: 0.85,  // heavy weapon
        },
        // ── Heavy Machine Guns ────────────────────────────────────────────────
        'hmg_suppression': {
            id: 'hmg_suppression', name: 'Wrecker HMG', category: 'heavy_machine_gun',
            dmg: 18, fireRate: 95, reloadTime: 5000,
            projSpd: 640, projR: 5, spread: 0.13,
            magSize: 100, range: 480, falloffStart: 320,
            moveSpeedMult: 0.75,  // very heavy
        },
        // ── Sniper Rifles ─────────────────────────────────────────────────────
        'sniper_long': {
            id: 'sniper_long', name: 'Eclipse SR', category: 'sniper_rifle',
            dmg: 85, fireRate: 1400, reloadTime: 3200,
            projSpd: 1200, projR: 4, spread: 0.0,
            magSize: 5, range: 1800, falloffStart: 800,
            moveSpeedMult: 0.80,  // long and heavy
        },
        // ── Designated Marksman Rifles ────────────────────────────────────────
        'dmr_standard': {
            id: 'dmr_standard', name: 'Watchman DMR', category: 'designated_marksman_rifle',
            dmg: 42, fireRate: 550, reloadTime: 2600,
            projSpd: 950, projR: 4, spread: 0.01,
            magSize: 10, range: 1100, falloffStart: 600,
            moveSpeedMult: 0.90,  // precision rifle
        },
        // ── Pistols ───────────────────────────────────────────────────────────
        'pistol_standard': {
            id: 'pistol_standard', name: 'Sidearm M3', category: 'pistol',
            dmg: 20, fireRate: 350, reloadTime: 1400,
            projSpd: 680, projR: 3, spread: 0.03,
            magSize: 12, range: 380, falloffStart: 220,
            moveSpeedMult: 1.15,  // lightest, fastest
        },
        // ── Revolvers ─────────────────────────────────────────────────────────
        'revolver_heavy': {
            id: 'revolver_heavy', name: 'Ironclad .50', category: 'revolver',
            dmg: 55, fireRate: 700, reloadTime: 2800,
            projSpd: 720, projR: 5, spread: 0.02,
            magSize: 6, range: 500, falloffStart: 300,
            moveSpeedMult: 1.05,  // compact sidearm
        },
        // ── Shotguns ──────────────────────────────────────────────────────────
        'shotgun_pump': {
            id: 'shotgun_pump', name: 'Rampart Pump', category: 'shotgun',
            dmg: 18, fireRate: 800, reloadTime: 2600,
            projSpd: 520, projR: 6, spread: 0.22,
            magSize: 6, pellets: 7, range: 250, falloffStart: 120,
            moveSpeedMult: 1.00,  // balanced trade-off
        },
    };

    // ═══════════════════════════════════════════════════════════════════════════
    // ABILITY SYSTEM
    //
    // Ability definitions: each operator has an abilityId referencing ABILITY_DEFS.
    // Abilities are activated by the player pressing the ability key (server-side
    // validation via 'ability' WS message).
    //
    // Fields:
    //   cooldown:  seconds before ability can be reused
    //   duration:  seconds the effect lasts (0 = instant)
    //   handler:   function(room, player) called on activation — add future logic here
    //
    // To add a new ability:
    //   1. Add an entry to ABILITY_DEFS
    //   2. Add the abilityId to an operator in OPERATOR_DEFS
    //   3. Implement handler() — everything else (cooldown tracking, WS events) is automatic
    // ═══════════════════════════════════════════════════════════════════════════
    const ABILITY_DEFS = {
        // ── Legacy test ability ───────────────────────────────────────────────
        'speed_boost': {
            id: 'speed_boost', name: 'Sprint Protocol',
            desc: 'Temporarily doubles movement speed for 3 seconds.',
            cooldown: 12, duration: 3,
            handler(room, player) {
                player.speedBoostUntil = Date.now() + 3000;
                return { abilityId: 'speed_boost', duration: 3 };
            },
        },

        // ── ROE OPERATOR ABILITIES ────────────────────────────────────────────

        // TITAN (Breacher) — magnetic explosive charge
        'breach_charge': {
            id: 'breach_charge', name: 'Breach Charge',
            desc: 'Throws a magnetic explosive that sticks to surfaces and detonates with a heavy splash.',
            cooldown: 14, duration: 0,
            handler(room, player) {
                room.spawnProjectile(player.x, player.y, player.a, player.team, player.id, {
                    spd: 240, dmg: 65, r: 9, life: 2.4,
                    splash: 80, bonusVsBldg: true,
                    pt: 'breach_charge',
                });
                return { abilityId: 'breach_charge', duration: 0 };
            },
        },

        // PEGASUS (Engineer) — repair nearby vehicles and structures
        'field_repair': {
            id: 'field_repair', name: 'Field Repair',
            desc: 'Repairs nearby friendly vehicles and structures.',
            cooldown: 18, duration: 3,
            handler(room, player) {
                const REPAIR_RADIUS = 130;
                const REPAIR_AMOUNT = 35;
                for (const b of room.buildings.values()) {
                    if (b.team !== player.team) continue;
                    if (Math.hypot(b.x - player.x, b.y - player.y) <= REPAIR_RADIUS) {
                        b.hp = Math.min(b.maxHp, b.hp + REPAIR_AMOUNT);
                        room.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
                    }
                }
                for (const v of room.vehicles.values()) {
                    if (v.team !== player.team) continue;
                    if (Math.hypot(v.x - player.x, v.y - player.y) <= REPAIR_RADIUS) {
                        v.hp = Math.min(v.maxHp, v.hp + REPAIR_AMOUNT);
                    }
                }
                return { abilityId: 'field_repair', duration: 3 };
            },
        },

        // PHANTOM (Recon) — near-invisible speed dash
        'phantom_rush': {
            id: 'phantom_rush', name: 'Phantom Rush',
            desc: 'Become nearly invisible and move faster for 3 seconds.',
            cooldown: 16, duration: 3,
            handler(room, player) {
                player.speedBoostUntil = Date.now() + 3000;
                player.invisibleUntil  = Date.now() + 3000;
                return { abilityId: 'phantom_rush', duration: 3 };
            },
        },

        // LEVIATHAN (Anti-Vehicle) — shoulder-launched rocket burst
        'rocket_barrage': {
            id: 'rocket_barrage', name: 'Rocket Barrage',
            desc: 'Fires a burst of 5 unguided rockets alternating from shoulder launchers.',
            cooldown: 20, duration: 0,
            handler(room, player) {
                const aRef    = player.a;
                const rockets = 5;
                // Perpendicular direction (right of aim = +90°)
                // Shoulder launchers are ~13px out to each side in world space
                const SHOULDER_DIST = 13;
                const perpA = aRef + Math.PI / 2;
                const perpX = Math.cos(perpA);
                const perpY = Math.sin(perpA);

                for (let i = 0; i < rockets; i++) {
                    // Strictly alternate: even = right shoulder, odd = left shoulder
                    const side = (i % 2 === 0) ? 1 : -1;
                    // Slight spread: rockets 0-4 fan out gently
                    const spread = (i - 2) * 0.07;
                    const ox = player.x + perpX * side * SHOULDER_DIST;
                    const oy = player.y + perpY * side * SHOULDER_DIST;
                    const delay = i * 100;
                    setTimeout(() => {
                        if (!room.players.has(player.id)) return;
                        room.spawnProjectile(ox, oy, aRef + spread, player.team, player.id, {
                            spd: 480, dmg: 48, r: 7, life: 2.0,
                            splash: 55, bonusVsBldg: true,
                            pt: 'rocket',
                        });
                    }, delay);
                }
                return { abilityId: 'rocket_barrage', duration: 0 };
            },
        },

        // DUSTER (Suppression) — deploys a device that fires in a cone
        'suppression_field': {
            id: 'suppression_field', name: 'Suppression Field',
            desc: 'Drops a suppression device that fires continuous slowing bursts in a forward cone for 5 seconds.',
            cooldown: 22, duration: 5,
            handler(room, player) {
                const devId  = shortId();
                const dur    = 5; // seconds
                const expiresAt = Date.now() + dur * 1000;
                // Place device at player's current position, facing their aim direction
                if (!room.suppDevices) room.suppDevices = new Map();
                room.suppDevices.set(devId, {
                    id: devId, x: player.x, y: player.y, a: player.a,
                    team: player.team, expiresAt,
                    lastShot: Date.now(),
                });
                // Tell clients about the device (they render it on canvas)
                room.events.push({
                    e: EV.SUPP_DEVICE, id: devId,
                    x: Math.round(player.x), y: Math.round(player.y),
                    a: +player.a.toFixed(4), tm: player.team, dur,
                });
                return { abilityId: 'suppression_field', duration: dur };
            },
        },

        // ZEPHYR (Support) — speed stim to nearby allies
        'combat_stim': {
            id: 'combat_stim', name: 'Combat Stim',
            desc: 'Nearby allies gain increased movement speed for 4 seconds.',
            cooldown: 18, duration: 4,
            handler(room, player) {
                const STIM_RADIUS = 190;
                const now = Date.now();
                const stimmed = [];
                for (const p of room.players.values()) {
                    if (p.team !== player.team) continue;
                    if (Math.hypot(p.x - player.x, p.y - player.y) <= STIM_RADIUS) {
                        p.speedBoostUntil = now + 4000;
                        stimmed.push(p.id);
                    }
                }
                return { abilityId: 'combat_stim', duration: 4, stimmed };
            },
        },

        // ── BGM CORP OPERATOR ABILITIES ───────────────────────────────────────────

        // BLACKJACK (BGM Breacher) — pincer dash (tick-loop driven, smooth)
        'pincer_rush': {
            id: 'pincer_rush', name: 'Pincer Rush',
            desc: 'Charges in the aimed direction, A/D to steer mid-dash. Damages enemies and structures.',
            cooldown: 14, duration: 1.1,
            handler(room, player) {
                const DASH_DIST = 280;
                const DASH_DUR  = 1100; // ms
                const DASH_SPD  = (DASH_DIST / DASH_DUR) * 1000; // px/s ≈ 254

                // Latch initial direction from player aim (= mouse direction)
                player.dashUntil      = Date.now() + DASH_DUR;
                player.dashAngle      = player.a;
                player.dashSpd        = DASH_SPD;
                player._dashHitPlayers = new Set();
                player._dashHitBuilds  = new Set();

                // Broadcast immediately so client can start its smooth prediction
                room.events.push({
                    e: EV.PINCER_DASH, i: player.id,
                    sx: Math.round(player.x), sy: Math.round(player.y),
                    // Preview end: straight ahead, will curve if player steers
                    ex: Math.round(Math.max(player.r, Math.min(MAP_W - player.r, player.x + Math.cos(player.a) * DASH_DIST))),
                    ey: Math.round(Math.max(player.r, Math.min(MAP_H - player.r, player.y + Math.sin(player.a) * DASH_DIST))),
                    a: +player.a.toFixed(4),
                    dur: DASH_DUR,
                });
                return { abilityId: 'pincer_rush', duration: 1.1 };
            },
        },

        // ORION (BGM Engineer) — hovering repair drone
        'repair_drone': {
            id: 'repair_drone', name: 'Repair Drone',
            desc: 'Deploys a hovering drone that repairs nearby allies and structures.',
            cooldown: 22, duration: 8,
            handler(room, player) {
                const droneId = shortId();
                const dur     = 8;
                if (!room.repairDrones) room.repairDrones = new Map();
                room.repairDrones.set(droneId, {
                    id: droneId, x: player.x, y: player.y,
                    team: player.team, expiresAt: Date.now() + dur * 1000,
                    lastRepair: Date.now(), orbitAngle: 0,
                });
                room.events.push({
                    e: EV.REPAIR_DRONE, id: droneId,
                    x: Math.round(player.x), y: Math.round(player.y),
                    tm: player.team, dur,
                });
                return { abilityId: 'repair_drone', duration: dur };
            },
        },

        // DAEMON (BGM Recon) — scout drone with light MG
        'scout_drone': {
            id: 'scout_drone', name: 'Scout Drone',
            desc: 'Launches a scout drone that flies forward, spots enemies, and fires a light MG.',
            cooldown: 20, duration: 0,
            handler(room, player) {
                const droneId = shortId();
                if (!room.scoutDrones) room.scoutDrones = new Map();
                room.scoutDrones.set(droneId, {
                    id: droneId, x: player.x, y: player.y, a: player.a,
                    team: player.team, ownerId: player.id,
                    hp: 80, maxHp: 80,      // HP-based — no timer
                    lastShot: Date.now(), spd: 90,
                });
                room.events.push({
                    e: EV.SCOUT_DRONE, id: droneId, ownerId: player.id,
                    x: Math.round(player.x), y: Math.round(player.y),
                    a: +player.a.toFixed(4), tm: player.team,
                    hp: 80, maxHp: 80,
                });
                return { abilityId: 'scout_drone', duration: 0 };
            },
        },

        // KASHTAN (BGM Anti-Vehicle) — CIWS auto-attack
        'overridden_ciws': {
            id: 'overridden_ciws', name: 'Overridden CIWS',
            desc: 'Shoulder CIWS and back micromissiles auto-attack the nearest vehicle or enemy for 5 seconds.',
            cooldown: 22, duration: 5,
            handler(room, player) {
                const dur = 5;
                player.ciwsUntil = Date.now() + dur * 1000;
                return { abilityId: 'overridden_ciws', duration: dur };
            },
        },

        // ODOGARON (BGM Suppression) — flame burst cone
        'flame_burst': {
            id: 'flame_burst', name: 'Flame Burst',
            desc: 'Rapidly spits fire in a wide cone, burning enemies.',
            cooldown: 16, duration: 1.5,
            handler(room, player) {
                const CONE_HALF = 0.52;   // ~30° each side
                const rounds    = 24;
                for (let i = 0; i < rounds; i++) {
                    // Randomise spread per round, not sequential — avoids two fireballs on same line
                    const spread = (Math.random() - 0.5) * CONE_HALF * 2;
                    const delay  = i * 60; // 60ms between rounds → 1.44s burst
                    setTimeout(() => {
                        if (!room.players.has(player.id)) return;
                        room.spawnProjectile(player.x, player.y, player.a + spread, player.team, player.id, {
                            spd: 360, dmg: 14, r: 8, life: 1.1,
                            burn: true, pt: 'bgm_flame',
                        });
                    }, delay);
                }
                return { abilityId: 'flame_burst', duration: 1.5 };
            },
        },

        // KONIG (BGM Support) — stationary energy shield
        'shield_emitter': {
            id: 'shield_emitter', name: 'Shield Emitter',
            desc: 'Projects a stationary energy shield that blocks incoming fire.',
            cooldown: 20, duration: 5,
            handler(room, player) {
                const shieldId = shortId();
                const dur      = 5;
                if (!room.shieldEmitters) room.shieldEmitters = new Map();
                room.shieldEmitters.set(shieldId, {
                    id: shieldId, x: player.x, y: player.y, a: player.a,
                    team: player.team, expiresAt: Date.now() + dur * 1000,
                    hp: 200, maxHp: 200,
                    // Shield arc: blocks projectiles within ±60° of facing angle, within 80px
                });
                room.events.push({
                    e: EV.SHIELD_EMITTER, id: shieldId,
                    x: Math.round(player.x), y: Math.round(player.y),
                    a: +player.a.toFixed(4), tm: player.team, dur,
                });
                return { abilityId: 'shield_emitter', duration: dur };
            },
        },

        // ── EPA OPERATOR ABILITIES ─────────────────────────────────────────────────

        // HELLHOUND (EPA Breacher) — instant plasma teleport, damages all enemies in path
        'plasma_dash': {
            id: 'plasma_dash', name: 'Plasma Dash',
            desc: 'Instantly teleports forward, damaging every enemy and structure passed through.',
            cooldown: 14, duration: 0,
            handler(room, player) {
                const DASH_DIST = 260;
                const DASH_DMG  = 65;
                const DASH_R    = 22;
                const now       = Date.now();

                const sx = player.x, sy = player.y;
                const ex = clamp(player.x + Math.cos(player.a) * DASH_DIST, player.r, MAP_W - player.r);
                const ey = clamp(player.y + Math.sin(player.a) * DASH_DIST, player.r, MAP_H - player.r);

                // Sweep along path in 10 steps to hit everything in the corridor
                const hitPlayers = new Set(), hitBuilds = new Set();
                const STEPS = 10;
                for (let s = 0; s <= STEPS; s++) {
                    const t = s / STEPS;
                    const cx = sx + (ex - sx) * t, cy = sy + (ey - sy) * t;
                    for (const p of room.players.values()) {
                        if (p.id === player.id || p.team === player.team || p.rt > 0 || hitPlayers.has(p.id)) continue;
                        if (dist(cx, cy, p.x, p.y) <= DASH_R + p.r) {
                            hitPlayers.add(p.id);
                            p.hp -= DASH_DMG;
                            p.lastDamaged = now;
                            if (p.hp <= 0) { p.hp = 0; p.rt = 3; room.events.push({ e: EV.PLAYER_DIE, i: p.id }); }
                            else room.events.push({ e: EV.PLAYER_HIT, i: p.id, hp: p.hp });
                        }
                    }
                    for (const [bid, b] of room.buildings) {
                        if (b.team === player.team || hitBuilds.has(bid)) continue;
                        if (dist(cx, cy, b.x, b.y) <= DASH_R + 20) {
                            hitBuilds.add(bid);
                            b.hp -= DASH_DMG;
                            if (b.hp <= 0) { b.hp = 0; room.buildings.delete(bid); room.events.push({ e: EV.BUILD_DESTROY, i: bid }); }
                            else room.events.push({ e: EV.BUILD_HIT, i: bid, hp: b.hp });
                        }
                    }
                }

                // Instant positional teleport
                player.x = ex;
                player.y = ey;

                room.events.push({
                    e: EV.PLASMA_DASH, i: player.id,
                    sx: Math.round(sx), sy: Math.round(sy),
                    ex: Math.round(ex), ey: Math.round(ey),
                    tm: player.team,
                });
                return { abilityId: 'plasma_dash', duration: 0 };
            },
        },

        // VELARUS (EPA Engineer) — deploys a temporary repair wall + repair device
        'repair_bay': {
            id: 'repair_bay', name: 'Repair Bay',
            desc: 'Creates a temporary wall that blocks projectiles, with a device behind it that repairs nearby vehicles and structures.',
            cooldown: 24, duration: 8,
            handler(room, player) {
                const dur   = 8;
                const bayId = shortId();
                // Place repair bay in front of player
                const WALL_DIST = 55;
                const bx = player.x + Math.cos(player.a) * WALL_DIST;
                const by = player.y + Math.sin(player.a) * WALL_DIST;
                if (!room.repairBays) room.repairBays = new Map();
                room.repairBays.set(bayId, {
                    id: bayId, x: bx, y: by, a: player.a,
                    team: player.team, expiresAt: Date.now() + dur * 1000,
                    lastRepair: Date.now(),
                });
                room.events.push({
                    e: EV.REPAIR_BAY, id: bayId,
                    x: Math.round(bx), y: Math.round(by),
                    a: +player.a.toFixed(4), tm: player.team, dur,
                });
                return { abilityId: 'repair_bay', duration: dur };
            },
        },

        // EXCELSIOR (EPA Recon) — volley of micro homing missiles vs recently-damaged enemies
        'micro_missile_swarm': {
            id: 'micro_missile_swarm', name: 'Micro Missile Swarm',
            desc: 'Launches a volley of small homing missiles that prioritize recently damaged enemies.',
            cooldown: 18, duration: 0,
            handler(room, player) {
                const missiles = 6;
                const aRef = player.a;
                for (let i = 0; i < missiles; i++) {
                    const spread = (i - (missiles - 1) / 2) * 0.13;
                    const delay  = i * 90;
                    setTimeout(() => {
                        if (!room.players.has(player.id)) return;
                        room.spawnProjectile(player.x, player.y, aRef + spread, player.team, player.id, {
                            spd: 480, dmg: 32, r: 5, life: 3.5,
                            splash: 38, bonusVsBldg: true,
                            homing: true,
                            pt: 'epa_micromissile',
                        });
                    }, delay);
                }
                return { abilityId: 'micro_missile_swarm', duration: 0 };
            },
        },

        // MORNING STAR (EPA Anti-Vehicle) — high-speed piercing rail round from shoulder cannon
        'rail_shot': {
            id: 'rail_shot', name: 'Rail Shot',
            desc: 'Charges and fires a high-speed piercing projectile effective against vehicles and structures.',
            cooldown: 16, duration: 0,
            handler(room, player) {
                // Rail cannon is mounted on the RIGHT shoulder — positive perpendicular to aim
                const perpA  = player.a + Math.PI / 2;
                const SHOULDER_OFFSET = 14;
                const ox = player.x + Math.cos(perpA) * SHOULDER_OFFSET;
                const oy = player.y + Math.sin(perpA) * SHOULDER_OFFSET;

                room.spawnProjectile(ox, oy, player.a, player.team, player.id, {
                    spd: 1600, dmg: 130, r: 8, life: 2.8,
                    pierce: true, bonusVsBldg: true,
                    pt: 'epa_rail',
                });
                return { abilityId: 'rail_shot', duration: 0 };
            },
        },

        // DRAKE (EPA Suppression) — sustained dual machine gun arm spray
        'machine_gun_arms': {
            id: 'machine_gun_arms', name: 'Machine Gun Arms',
            desc: 'Fires a suppressive machine gun spray with dual machine gun arms, wherever the player is aiming.',
            cooldown: 14, duration: 3,
            handler(room, player) {
                const dur = 3;
                player.machineGunArmsUntil = Date.now() + dur * 1000;
                player._mgaLastShot = 0;
                return { abilityId: 'machine_gun_arms', duration: dur };
            },
        },

        // CAESARIUM (EPA Support) — area restoration field that heals nearby allies
        'restoration_field': {
            id: 'restoration_field', name: 'Restoration Field',
            desc: 'Creates an area that slowly heals nearby allies.',
            cooldown: 24, duration: 6,
            handler(room, player) {
                const dur     = 6;
                const fieldId = shortId();
                if (!room.restorationFields) room.restorationFields = new Map();
                room.restorationFields.set(fieldId, {
                    id: fieldId, x: player.x, y: player.y,
                    team: player.team, expiresAt: Date.now() + dur * 1000,
                    lastHeal: Date.now(),
                });
                room.events.push({
                    e: EV.RESTORATION_FIELD, id: fieldId,
                    x: Math.round(player.x), y: Math.round(player.y),
                    tm: player.team, dur,
                });
                return { abilityId: 'restoration_field', duration: dur };
            },
        },
    };  // end ABILITY_DEFS

    // ═══════════════════════════════════════════════════════════════════════════
    // OPERATOR DEFINITIONS
    //
    // Each faction has 6 operator roles:
    //   breacher, engineer, recon, anti_vehicle, suppression, support
    //
    // Each operator:
    //   id           – unique key
    //   displayName  – shown in UI
    //   faction      – 'roe' | 'bgm' | 'epa'
    //   role         – one of the 6 roles
    //   allowedWeapons – array of WEAPON_DEFS keys this operator can equip
    //   abilityId    – key into ABILITY_DEFS (null = no ability yet)
    //   baseSpd      – movement speed override (uses room default if omitted)
    //
    // Shared weapons: 'smg_compact' appears in both Engineer and Support —
    // the same WEAPON_DEFS entry is reused with no duplication.
    //
    // To add a new operator: add one entry here. The selection UI,
    // loadout sync, and spawn system pick it up automatically.
    //
    // Only a representative sample is implemented here; the structure
    // cleanly supports dozens more.
    // ═══════════════════════════════════════════════════════════════════════════

    // ── Role weapon pools ────────────────────────────────────────────────────
    // Single source of truth for which 3 weapons each role can equip.
    // Every operator of the same role — regardless of faction — gets exactly
    // this list in exactly this order.  Change it here once; all 18 operators
    // update automatically.  No per-faction or per-operator overrides.
    const ROLE_WEAPON_POOLS = {
        breacher    : ['ar_standard',      'br_precision',  'shotgun_pump'    ],
        engineer    : ['smg_compact',      'lmg_standard',  'pistol_standard' ],
        recon       : ['sniper_long',      'dmr_standard',  'revolver_heavy'  ],
        anti_vehicle: ['hmg_suppression',  'lmg_standard',  'shotgun_pump'    ],
        suppression : ['lmg_standard',     'shotgun_pump',  'smg_compact'     ],
        support     : ['pistol_standard',  'revolver_heavy','smg_compact'     ],
    };

    const OPERATOR_DEFS = {
        // ── ROE Operators ─────────────────────────────────────────────────────
        'roe_breacher': {
            id: 'roe_breacher', displayName: 'Titan', faction: 'roe', role: 'breacher',
            allowedWeapons: ROLE_WEAPON_POOLS.breacher,
            abilityId: 'breach_charge',
            desc: 'Frontline defense killer. Throws magnetic Breach Charges that stick to walls, vehicles, and turrets before detonating.',
        },
        'roe_engineer': {
            id: 'roe_engineer', displayName: 'Pegasus', faction: 'roe', role: 'engineer',
            allowedWeapons: ROLE_WEAPON_POOLS.engineer,
            abilityId: 'field_repair',
            desc: 'Vehicle and turret repair specialist. Field Repair pulses wrist emitters to restore nearby friendly structures and vehicles.',
        },
        'roe_recon': {
            id: 'roe_recon', displayName: 'Phantom', faction: 'roe', role: 'recon',
            allowedWeapons: ROLE_WEAPON_POOLS.recon,
            abilityId: 'phantom_rush',
            desc: 'Vision intel and flank specialist. Phantom Rush renders nearly invisible for 3 seconds at boosted speed.',
        },
        'roe_anti_vehicle': {
            id: 'roe_anti_vehicle', displayName: 'Leviathan', faction: 'roe', role: 'anti_vehicle',
            allowedWeapons: ROLE_WEAPON_POOLS.anti_vehicle,
            abilityId: 'rocket_barrage',
            desc: 'Anti-tank and mech destroyer. Oversized shoulder launchers fire a burst of 5 unguided rockets.',
        },
        'roe_suppression': {
            id: 'roe_suppression', displayName: 'Duster', faction: 'roe', role: 'suppression',
            allowedWeapons: ROLE_WEAPON_POOLS.suppression,
            abilityId: 'suppression_field',
            desc: 'Area denial and lane pressure. Suppression Field unleashes a sustained cone of slowing fire.',
        },
        'roe_support': {
            id: 'roe_support', displayName: 'Zephyr', faction: 'roe', role: 'support',
            allowedWeapons: ROLE_WEAPON_POOLS.support,
            abilityId: 'combat_stim',
            desc: 'Heal utility and team buffing. Combat Stim supercharges nearby allies with a speed burst.',
        },
        // ── BGM Operators ─────────────────────────────────────────────────────
        'bgm_breacher': {
            id: 'bgm_breacher', displayName: 'Blackjack', faction: 'bgm', role: 'breacher',
            allowedWeapons: ROLE_WEAPON_POOLS.breacher,
            abilityId: 'pincer_rush',
            desc: 'Frontline defense killer. Charges through enemies and structures with powerful stag beetle pincers.',
        },
        'bgm_engineer': {
            id: 'bgm_engineer', displayName: 'Orion', faction: 'bgm', role: 'engineer',
            allowedWeapons: ROLE_WEAPON_POOLS.engineer,
            abilityId: 'repair_drone',
            desc: 'Industrial support specialist. Deploys a hovering repair drone to automatically restore allies and structures.',
        },
        'bgm_recon': {
            id: 'bgm_recon', displayName: 'Daemon', faction: 'bgm', role: 'recon',
            allowedWeapons: ROLE_WEAPON_POOLS.recon,
            abilityId: 'scout_drone',
            desc: 'Sharp angular scout. Launches a flying drone that spots enemies and harasses with a light MG.',
        },
        'bgm_anti_vehicle': {
            id: 'bgm_anti_vehicle', displayName: 'Kashtan', faction: 'bgm', role: 'anti_vehicle',
            allowedWeapons: ROLE_WEAPON_POOLS.anti_vehicle,
            abilityId: 'overridden_ciws',
            desc: 'Anti-armor destroyer. Overrides shoulder CIWS and back micromissiles to shred the nearest vehicle or enemy.',
        },
        'bgm_suppression': {
            id: 'bgm_suppression', displayName: 'Odogaron', faction: 'bgm', role: 'suppression',
            allowedWeapons: ROLE_WEAPON_POOLS.suppression,
            abilityId: 'flame_burst',
            desc: 'Predator-like area denial. Spits fire in a wide burning cone.',
        },
        'bgm_support': {
            id: 'bgm_support', displayName: 'Konig', faction: 'bgm', role: 'support',
            allowedWeapons: ROLE_WEAPON_POOLS.support,
            abilityId: 'shield_emitter',
            desc: 'Command robot. Projects a stationary energy shield that blocks incoming fire.',
        },
        // ── EPA Operators ─────────────────────────────────────────────────────
        'epa_breacher': {
            id: 'epa_breacher', displayName: 'Hellhound', faction: 'epa', role: 'breacher',
            allowedWeapons: ROLE_WEAPON_POOLS.breacher,
            abilityId: 'plasma_dash',
            desc: 'Frontline defense killer. Plasma Dash propels the sleek assault frame through enemy lines at plasma speed, scorching everything it passes through.',
        },
        'epa_engineer': {
            id: 'epa_engineer', displayName: 'Velarus', faction: 'epa', role: 'engineer',
            allowedWeapons: ROLE_WEAPON_POOLS.engineer,
            abilityId: 'repair_bay',
            desc: 'Vehicle and turret repair specialist. Repair Bay deploys a protective wall with a construction emitter that restores nearby friendly vehicles and structures.',
        },
        'epa_recon': {
            id: 'epa_recon', displayName: 'Excelsior', faction: 'epa', role: 'recon',
            allowedWeapons: ROLE_WEAPON_POOLS.recon,
            abilityId: 'micro_missile_swarm',
            desc: 'Vision intel and flank specialist. Micro Missile Swarm deploys orbiting sensor prisms to guide a volley of homing missiles onto recently damaged targets.',
        },
        'epa_anti_vehicle': {
            id: 'epa_anti_vehicle', displayName: 'Morning Star', faction: 'epa', role: 'anti_vehicle',
            allowedWeapons: ROLE_WEAPON_POOLS.anti_vehicle,
            abilityId: 'rail_shot',
            desc: 'Anti-tank and mech destroyer. Rail Shot charges the shoulder-mounted rail cannon for a single devastating hypersonic piercing round.',
        },
        'epa_suppression': {
            id: 'epa_suppression', displayName: 'Drake', faction: 'epa', role: 'suppression',
            allowedWeapons: ROLE_WEAPON_POOLS.suppression,
            abilityId: 'machine_gun_arms',
            desc: 'Area denial and pressure. Machine Gun Arms opens fire with both integrated arm-mounted machine guns in a sustained, wide suppressive spray.',
        },
        'epa_support': {
            id: 'epa_support', displayName: 'Caesarium', faction: 'epa', role: 'support',
            allowedWeapons: ROLE_WEAPON_POOLS.support,
            abilityId: 'restoration_field',
            desc: 'Heal utility and buffing. Restoration Field projects a ring emitter array that slowly restores the health of all nearby allies.',
        },
    };

    // Helper: get all operators belonging to a faction (for sending to client)
    function getOperatorsForFaction(factionId) {
        return Object.values(OPERATOR_DEFS).filter(op => op.faction === factionId);
    }

    // Helper: resolve a fallback loadout for a player (used when timer expires)
    // Returns { operatorId, weaponId }
    function getRandomLoadout(factionId) {
        const ops = getOperatorsForFaction(factionId);
        if (ops.length === 0) return { operatorId: null, weaponId: null };
        const op = ops[Math.floor(Math.random() * ops.length)];
        const weaponId = op.allowedWeapons[0] || null;
        return { operatorId: op.id, weaponId };
    }

    // ─── Global state ────────────────────────────────────────────────────────────
    const rooms   = new Map();
    const clients = new Map();

    // Broadcast to ALL connected WebSocket clients
    function broadcastAll(payload) {
        const str = typeof payload === 'string' ? payload : JSON.stringify(payload);
        for (const [ws] of clients) {
            if (ws.readyState === WebSocket.OPEN) ws.send(str);
        }
    }

    // Periodically push online count to everyone
    setInterval(() => {
        broadcastAll({ t: 'gchat_online', n: clients.size });
    }, 8000);

    // ─── Helpers ─────────────────────────────────────────────────────────────────
    const dist    = (x1,y1,x2,y2) => Math.hypot(x2-x1, y2-y1);
    const clamp   = (v,lo,hi)     => Math.max(lo, Math.min(hi, v));
    const shortId = ()            => crypto.randomBytes(3).toString('hex');

    // Segment-circle intersection: returns true if line segment (ax,ay)→(bx,by)
    // passes within radius r of point (cx,cy). Used for accurate bullet raycasting.
    function segCircle(ax, ay, bx, by, cx, cy, r) {
        const dx = bx - ax, dy = by - ay;
        const fx = ax - cx, fy = ay - cy;
        const a = dx*dx + dy*dy;
        if (a === 0) return fx*fx + fy*fy <= r*r;  // degenerate: point test
        const b = 2*(fx*dx + fy*dy);
        const c = fx*fx + fy*fy - r*r;
        let disc = b*b - 4*a*c;
        if (disc < 0) return false;
        disc = Math.sqrt(disc);
        const t1 = (-b - disc) / (2*a);
        const t2 = (-b + disc) / (2*a);
        return (t1 >= 0 && t1 <= 1) || (t2 >= 0 && t2 <= 1) || (t1 < 0 && t2 > 1);
    }

    // Segment-expanded-rect intersection (Minkowski sum: inflate rect by bullet radius).
    // Replaces circleRect point-check for wall collision so fast bullets never skip through.
    function segRect(ax, ay, bx, by, pr, rx, ry, rw, rh) {
        // Expand rect by bullet radius on all sides
        const ex = rx - pr, ey = ry - pr, ew = rw + 2*pr, eh = rh + 2*pr;
        // Fast reject: bounding box of segment vs expanded rect
        if (Math.min(ax,bx) > ex+ew || Math.max(ax,bx) < ex) return false;
        if (Math.min(ay,by) > ey+eh || Math.max(ay,by) < ey) return false;
        // Check if either endpoint is inside
        if (ax >= ex && ax <= ex+ew && ay >= ey && ay <= ey+eh) return true;
        if (bx >= ex && bx <= ex+ew && by >= ey && by <= ey+eh) return true;
        // Check segment against all four expanded rect edges
        function segSeg(p1x,p1y,p2x,p2y,p3x,p3y,p4x,p4y) {
            const d1x=p2x-p1x,d1y=p2y-p1y,d2x=p4x-p3x,d2y=p4y-p3y;
            const denom=d1x*d2y-d1y*d2x;
            if(Math.abs(denom)<1e-9) return false;
            const t=((p3x-p1x)*d2y-(p3y-p1y)*d2x)/denom;
            const u=((p3x-p1x)*d1y-(p3y-p1y)*d1x)/denom;
            return t>=0&&t<=1&&u>=0&&u<=1;
        }
        return segSeg(ax,ay,bx,by,ex,ey,ex+ew,ey) ||
               segSeg(ax,ay,bx,by,ex+ew,ey,ex+ew,ey+eh) ||
               segSeg(ax,ay,bx,by,ex,ey+eh,ex+ew,ey+eh) ||
               segSeg(ax,ay,bx,by,ex,ey,ex,ey+eh);
    }

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

    function wireVehicle(v) {
        return {
            id: v.id, tp: v.type, tm: v.team,
            x: Math.round(v.x), y: Math.round(v.y),
            a: encodeAngle(v.a),
            pa: encodeAngle(v.pa || 0),
            hp: Math.round(v.hp), mhp: v.maxHp,
            drv: v.driverId  || null,
            pax: v.passengerId || null,
            // APC infantry count
            ic: v.infantryIds ? v.infantryIds.length : undefined,
            // Drone fields (mechs only)
            dx: v.droneX !== undefined ? Math.round(v.droneX) : undefined,
            dy: v.droneY !== undefined ? Math.round(v.droneY) : undefined,
            da: v.droneA !== undefined ? encodeAngle(v.droneA) : undefined,
            // Drone orbit angle — 0..255 maps 0..2π, used by client for smooth extrapolation
            doa: v.droneOrbitAngle !== undefined
                ? Math.round(((v.droneOrbitAngle % (Math.PI * 2) + Math.PI * 2) % (Math.PI * 2)) / (Math.PI * 2) * 255) & 0xFF
                : undefined,
            // Movement angle for mech legs (separate from v.a which is driver aim)
            ma: v.moveA !== undefined ? encodeAngle(v.moveA) : undefined,
        };
    }


    // ─── Spatial Hash Grid ───────────────────────────────────────────────────────
    // Replaces O(N×M) entity loops with O(1) cell lookup.
    // Cell size 80px: fits infantry separation (r=20) in 1-4 cells,
    // fits turret range queries in ≤10×10 cells.
    class SpatialHash {
        constructor(cellSize = 80) {
            this._cs  = cellSize;
            this._map = new Map();
        }
        _key(cx, cy) { return (cx & 0x7FFF) | ((cy & 0x7FFF) << 15); }
        _cx(x) { return Math.floor(x / this._cs); }
        _cy(y) { return Math.floor(y / this._cs); }
        insert(x, y, obj) {
            const k = this._key(this._cx(x), this._cy(y));
            let c = this._map.get(k);
            if (!c) { c = []; this._map.set(k, c); }
            c.push(obj);
        }
        // Returns all objects in cells overlapping the square [x±r, y±r].
        // Callers do precise distance filtering after the query.
        query(x, y, r) {
            const x0 = this._cx(x - r), x1 = this._cx(x + r);
            const y0 = this._cy(y - r), y1 = this._cy(y + r);
            const out = [];
            for (let cx = x0; cx <= x1; cx++)
                for (let cy = y0; cy <= y1; cy++) {
                    const c = this._map.get(this._key(cx, cy));
                    if (c) for (const o of c) out.push(o);
                }
            return out;
        }
        clear() { this._map.clear(); }
    }

    // ─── Room ────────────────────────────────────────────────────────────────────
    class Room {
        constructor(id, mode = 'casual') {
            this.id        = id;
            this.mode      = mode;
            this.maxPlayers = mode === 'ranked1v1' ? 2
                            : mode === 'ranked2v2' ? 4
                            : 8;   // casual / private
            this.players   = new Map();
            this.buildings = new Map();
            this.projs     = new Map();
            this.vehicles  = new Map();   // vehicleId → vehicle object

            // ── Infantry system state ──────────────────────────────────────────
            this.infantry    = new Map();  // infantryId → infantry object
            this.teamSupply  = [
                { current: INF_MAX_SUPPLY, max: INF_MAX_SUPPLY },  // team 0
                { current: INF_MAX_SUPPLY, max: INF_MAX_SUPPLY },  // team 1
            ];
            this._infAiTick  = 0;   // counts ticks for 5 Hz AI throttle
            this._infAiDidUpdate = false; // set true when AI runs; consumed by broadcastSnapshot

            // ── Spatial hash grids — rebuilt each tick, used for all proximity queries ──
            this._playerHash  = new SpatialHash(80);
            this._buildHash   = new SpatialHash(80);
            this._infHash     = new SpatialHash(80);
            this._vehicleHash = new SpatialHash(80);

            this.rankSum   = 0;   // sum of all player ranks in this room
            this.rankCount = 0;   // number of players (for avg rank calculation)

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

            // ── Bandwidth profiling ───────────────────────────────────────────
            this._bwStats = {
                bytesSent  : 0,   // cumulative bytes sent this window
                infUpdates : 0,   // infantry update entries sent
                vehUpdates : 0,   // vehicle update entries sent
                snapshots  : 0,   // snapshot broadcasts
                lastLog    : Date.now(),
            };

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

            // ── Operator select state ──────────────────────────────────────────
            // Populated after vote phase resolves; cleared when game starts.
            // operatorSelections: playerId → { operatorId, weaponId, lockedIn }
            this.inOperatorSelect  = false;
            this.operatorSelTimer  = null;   // interval handle

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
            const options = this._voteMapOptionIds || MAP_DEFS.map(m => m.id);
            const entries = Object.entries(v).filter(e => options.includes(+e[0]));
            if (entries.length === 0) {
                const fallbackId = options[Math.floor(Math.random() * options.length)];
                return MAP_DEFS.find(m => m.id === fallbackId) || MAP_DEFS[0];
            }
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
            this._voteCountdown = VOTE_TIME;

            // Pick VOTE_MAP_COUNT random maps from all MAP_DEFS for this vote
            const shuffled = [...MAP_DEFS].sort(() => Math.random() - 0.5);
            const voteOptions = shuffled.slice(0, VOTE_MAP_COUNT);
            this._voteMapOptionIds = voteOptions.map(m => m.id);

            // Send votestart individually so map list is included
            for (const p of this.players.values()) {
                if (p.ws.readyState === WebSocket.OPEN) {
                    p.ws.send(JSON.stringify({
                        t    : 'votestart',
                        tm   : VOTE_TIME,
                        maps : voteOptions.map(m => ({ id: m.id, name: m.name })),
                        team : p.team,
                    }));
                }
            }

            const vi = setInterval(() => {
                if (this.players.size === 0) { clearInterval(vi); return; }
                this._voteCountdown--;
                this.broadcastRaw(JSON.stringify({ t: 'votetick', tm: this._voteCountdown }));
                if (this._voteCountdown <= 0) { clearInterval(vi); this.startOperatorSelectPhase(); }
            }, 1000);
        }

        // If every player has cast both votes and >3 s remain, snap to 3 s.
        checkAllVotedEarlyEnd() {
            if (!this.inVotePhase || this._voteCountdown <= 3) return;
            const n = this.players.size;
            if (n === 0) return;
            for (const p of this.players.values()) {
                if (!this.factionVotes[p.team].has(p.id)) return;
                if (!this.mapVotes.has(p.id)) return;
            }
            // Everyone voted — snap countdown to 3
            this._voteCountdown = 3;
            this.broadcastRaw(JSON.stringify({ t: 'votetick', tm: 3 }));
        }

        // ═══════════════════════════════════════════════════════════════════
        // OPERATOR SELECT PHASE
        //
        // Flow:
        //   1. startVotePhase() ends → startOperatorSelectPhase() is called
        //   2. Factions are resolved; each player receives 'opselect_start' with:
        //      - their faction's operator list
        //      - WEAPON_DEFS for those operators' allowed weapons
        //      - the timer duration
        //   3. Players send 'op_select' and 'wp_select' messages to choose
        //   4. Players send 'op_lock' to confirm their loadout
        //   5. Each player picks operator+weapon and drops in independently via handleLockIn()
        //      No timer, no waiting — the game world runs from vote resolution onward
        // ═══════════════════════════════════════════════════════════════════
        startOperatorSelectPhase() {
            if (this.inOperatorSelect) return;

            // Resolve factions/map from votes first
            this.teamFactions[0] = this.resolveFaction(0);
            this.teamFactions[1] = this.resolveFaction(1);
            this.mapDef = this.resolveMap();
            this.cores  = [
                { id: 0, team: 0, x: this.mapDef.cores[0].x, y: this.mapDef.cores[0].y, hp: 2500, maxHp: 2500, r: 40 },
                { id: 1, team: 1, x: this.mapDef.cores[1].x, y: this.mapDef.cores[1].y, hp: 2500, maxHp: 2500, r: 40 },
            ];

            this.inOperatorSelect = true;
            this.inVotePhase      = false;

            // Reset vote/lobby state
            this.factionVotes  = { 0: new Map(), 1: new Map() };
            this.mapVotes      = new Map();
            this.readyStates   = new Map();

            this.phase   = PH.BUILD;
            this.timer   = BUILD_TIME;
            this.winner  = -1;
            this.events  = [];

            this.cores.forEach(c => { c.hp = c.maxHp; });
            this.buildings.clear();
            this.projs.clear();
            this.vehicles.clear();
            if (this.suppDevices)    this.suppDevices.clear();
            if (this.repairDrones)   this.repairDrones.clear();
            if (this.scoutDrones)    this.scoutDrones.clear();
            if (this.shieldEmitters) this.shieldEmitters.clear();

            this._prevPhase    = -1;
            this._prevTimer    = -1;
            this._prevCoreHPs  = [-1, -1];
            this._lastScrapTick = null;
            this._infAiDidUpdate = false;

            // Initialise all players — rt=9999 keeps them out of the world until
            // they individually lock their loadout via handleLockIn
            for (const p of this.players.values()) {
                p.operatorId     = null;
                p.weaponId       = null;
                p.lockedIn       = false;
                p.res            = 150;
                p.hp             = p.maxHp;
                p.rt             = 9999;
                p.burnUntil      = 0;
                p._lastBurnTick  = 0;
                p.vehicleId      = null;
                p.vehicleRole    = null;
                p._weaponFireRate = 200;
                p._weaponProjSpd  = 700;
                p._weaponDmg      = 15;
                p._weaponProjR    = 5;
                p._weaponSpread   = 0.04;
                p._weaponPellets  = 1;
                p.activeAbility   = null;
                p.abilityCooldown = 0;
                p.speedBoostUntil = 0;
                const spawn = this.mapDef.spawns[p.team];
                p.x   = spawn.x;
                p.y   = spawn.y;
                p._px = -1; p._py = -1; p._ab = -1;
            }

            // Start the game world immediately (BUILD timer ticks while players pick)
            this.broadcastRaw(JSON.stringify({
                t      : 'start',
                ph     : this.phase,
                tm     : this.timer,
                cHps   : [this.cores[0].hp, this.cores[1].hp],
                fcts   : this.teamFactions,
                mapId  : this.mapDef.id,
                mapName: this.mapDef.name,
                wz     : this.mapDef.waterZones,
                spawns : this.mapDef.spawns,
                loadouts   : [],
                droppingIn : true,   // client shows op-select overlay, not the game canvas
            }));

            // Per-player opselect_start (each sees only their faction's operators)
            for (const p of this.players.values()) {
                if (p.ws.readyState !== WebSocket.OPEN) continue;
                const faction   = this.teamFactions[p.team];
                const operators = getOperatorsForFaction(faction);
                const weaponIds = new Set(operators.flatMap(op => op.allowedWeapons));
                const weapons   = {};
                for (const wid of weaponIds) {
                    const w = WEAPON_DEFS[wid];
                    if (w) weapons[wid] = { id: w.id, name: w.name, category: w.category,
                        dmg: w.dmg, fireRate: w.fireRate, magSize: w.magSize, range: w.range };
                }
                p.ws.send(JSON.stringify({
                    t        : 'opselect_start',
                    tm       : 0,
                    faction,
                    operators: operators.map(op => ({
                        id: op.id, displayName: op.displayName, role: op.role,
                        desc: op.desc, allowedWeapons: op.allowedWeapons,
                        abilityId: op.abilityId,
                    })),
                    weapons,
                }));
            }

            // Build-phase timer — runs independently of any player's lock-in
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

        // ── Operator selection from client ───────────────────────────────────
        // Called when player sends { t: 'op_select', opId: '...' }
        // Server validates the operator belongs to the player's faction before accepting
        handleOperatorSelect(player, opId) {
            if (!this.inOperatorSelect || player.lockedIn) return;
            const op = OPERATOR_DEFS[opId];
            if (!op) return;
            const faction = this.teamFactions[player.team];
            if (op.faction !== faction) return;   // cross-faction guard

            player.operatorId = opId;
            // Clear weapon if it's no longer valid for the newly selected operator.
            // Do NOT auto-select — the player must choose explicitly.
            // A fallback is applied at game-start (startGame) if they never pick one.
            if (player.weaponId !== null && !op.allowedWeapons.includes(player.weaponId)) {
                player.weaponId = null;
            }
            // Echo operator confirmation without a pre-selected weapon
            if (player.ws.readyState === WebSocket.OPEN) {
                player.ws.send(JSON.stringify({ t: 'op_confirmed', opId }));
            }
        }

        // ── Weapon selection from client ─────────────────────────────────────
        // Called when player sends { t: 'wp_select', weaponId: '...' }
        handleWeaponSelect(player, weaponId) {
            if (!this.inOperatorSelect || player.lockedIn) return;
            const op = OPERATOR_DEFS[player.operatorId];
            if (!op) return;  // must have an operator selected first
            if (!op.allowedWeapons.includes(weaponId)) return;  // weapon not in role pool
            if (!WEAPON_DEFS[weaponId]) return;

            player.weaponId = weaponId;
            if (player.ws.readyState === WebSocket.OPEN) {
                player.ws.send(JSON.stringify({ t: 'wp_confirmed', weaponId }));
            }
        }

        // ── Lock-in: individual drop-in, no dependency on other players ──────────
        // Called when a player sends { t: 'op_lock' } (triggered client-side as soon
        // as they pick an operator + weapon).  Each player drops into the live world
        // independently — others are unaffected.
        handleLockIn(player) {
            if (player.lockedIn) return;
            if (!player.operatorId || !player.weaponId) return;

            player.lockedIn = true;

            // Apply this player's weapon stats
            const weapDef = WEAPON_DEFS[player.weaponId];
            player._weaponFireRate = weapDef ? weapDef.fireRate  : 200;
            player._weaponProjSpd  = weapDef ? weapDef.projSpd   : 700;
            player._weaponDmg      = weapDef ? weapDef.dmg       : 15;
            player._weaponProjR    = weapDef ? weapDef.projR      : 5;
            player._weaponSpread   = weapDef ? weapDef.spread     : 0.04;
            player._weaponPellets  = weapDef ? (weapDef.pellets   || 1) : 1;
            player.activeAbility   = null;
            player.abilityCooldown = 0;
            player.speedBoostUntil = 0;

            // Spawn into the live world right now
            const spawn = this.mapDef ? this.mapDef.spawns[player.team] : { x: 400, y: 1000 };
            player.x  = spawn.x;
            player.y  = spawn.y;
            player.hp = player.maxHp;
            player.rt = 0;
            player.burnUntil = 0;

            this.events.push({
                e : EV.PLAYER_SPAWN,
                i : player.id,
                x : Math.round(player.x),
                y : Math.round(player.y),
                hp: player.hp,
            });

            // Tell all clients this player locked in (for status pips on the overlay)
            this.broadcastRaw(JSON.stringify({ t: 'op_locked', id: player.id }));

            // Tell THIS player to close the overlay and enter the game
            if (player.ws.readyState === WebSocket.OPEN) {
                player.ws.send(JSON.stringify({
                    t         : 'drop_in',
                    operatorId: player.operatorId,
                    weaponId  : player.weaponId,
                    role      : OPERATOR_DEFS[player.operatorId]?.role || null,
                }));
            }
        }

        // ═══════════════════════════════════════════════════════════════════════
        // ABILITY SYSTEM
        //
        // handleAbility() is called when a player presses the ability key.
        // Server validates: player is alive, operator has an ability, cooldown expired.
        // The ABILITY_DEFS handler() runs and returns event data to broadcast.
        //
        // Future ability types can do anything inside handler():
        //   - Spawn deployables (add to room.buildings or a new deployables Map)
        //   - Buff allies (iterate room.players, apply status)
        //   - Spawn projectiles (call room.spawnProjectile)
        //   - Trigger cooldown-gated map events
        // ═══════════════════════════════════════════════════════════════════════
        handleAbility(player) {
            if (this.phase !== PH.ATTACK) return;
            if (player.rt > 0) return;  // dead
            if (!player.operatorId) return;

            const op = OPERATOR_DEFS[player.operatorId];
            if (!op || !op.abilityId) return;

            const abilityDef = ABILITY_DEFS[op.abilityId];
            if (!abilityDef) return;

            const now = Date.now();
            if (now < player.abilityCooldown) return;  // still on cooldown

            // Run the ability handler — returns data to broadcast to clients
            const evData = abilityDef.handler(this, player);

            // Set cooldown
            player.abilityCooldown = now + abilityDef.cooldown * 1000;
            player.activeAbility   = op.abilityId;

            // Broadcast ABILITY_USED event so clients play VFX / update cooldown UI
            this.events.push({
                e        : EV.ABILITY_USED,
                i        : player.id,
                abilityId: op.abilityId,
                cooldown : abilityDef.cooldown,
                ...(evData || {}),
            });

            // Schedule ABILITY_READY event when cooldown expires
            setTimeout(() => {
                if (!this.players.has(player.id)) return;
                player.activeAbility = null;
                this.events.push({ e: EV.ABILITY_READY, i: player.id, abilityId: op.abilityId });
            }, abilityDef.cooldown * 1000);
        }

        addPlayer(ws, id, name, rank = 0) {
            let red = 0, blue = 0;
            for (const p of this.players.values()) p.team === 0 ? red++ : blue++;
            const team   = red <= blue ? 0 : 1;
            const spawn  = this.mapDef.spawns[team];

            this.rankSum   += rank;
            this.rankCount += 1;

            this.players.set(id, {
                id, ws, name, team, rank,
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
                vehicleId: null,    // id of vehicle this player is in
                vehicleRole: null,  // 'driver' | 'passenger'
                _px: -1, _py: -1, _ab: -1,  // delta sentinels — force first inclusion
                // ── Operator / loadout fields ─────────────────────────────────
                // Set during OPERATOR_SELECT phase; used at spawn time.
                // Future abilities hook into activeAbility and abilityCooldown.
                operatorId:    null,   // key into OPERATOR_DEFS
                weaponId:      null,   // key into WEAPON_DEFS
                lockedIn:      false,  // true once player confirms their selection
                // Ability state — updated by handleAbility()
                activeAbility:    null,    // currently running ability id (null = none active)
                abilityCooldown:  0,       // server timestamp when cooldown expires
                speedBoostUntil:  0,       // speed_boost / phantom_rush / combat_stim
                invisibleUntil:   0,       // phantom_rush: client renders transparent
                ciwsUntil:        0,       // overridden_ciws: auto-attack end time
                dashUntil:        0,       // pincer_rush / plasma_dash: dash end timestamp
                dashAngle:        0,       // pincer_rush: current dash direction (steerable)
                dashSpd:          0,       // pincer_rush: dash speed px/s
                machineGunArmsUntil: 0,    // machine_gun_arms: spray end time
                lastDamaged:      0,       // timestamp (ms) of last time this player took damage
                _dashHitPlayers:  null,    // pincer_rush: hit-dedup set (lives on player)
                _dashHitBuilds:   null,
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

            // Send current infantry world state to the joining player.
            // Infantry inside APCs are excluded: their position is the APC's position
            // and the APC's 'ic' field already communicates the cargo count.
            const visibleInfantry = Array.from(this.infantry.values()).filter(u => u.state !== 'in_apc');
            if (visibleInfantry.length > 0) {
                ws.send(JSON.stringify({
                    t    : 'inf_sync',
                    units: visibleInfantry.map(u => ({
                        i: u.id, x: Math.round(u.x), y: Math.round(u.y),
                        a: encodeAngle(u.a), tm: u.team, tp: u.type,
                        hp: u.hp, mhp: u.maxHp,
                    })),
                }));
            }
            // Send this player's team supply
            ws.send(JSON.stringify({
                t: 'supply', tm: team,
                cur: this.teamSupply[team].current,
                max: this.teamSupply[team].max,
            }));

            // Evaluate lobby countdown state with the new player count
            this.evaluateLobby();
        }

        removePlayer(id) {
            const p = this.players.get(id);
            if (p) {
                // Eject from vehicle if occupying one
                if (p.vehicleId) {
                    const v = this.vehicles.get(p.vehicleId);
                    if (v) {
                        if (v.driverId   === id) v.driverId   = null;
                        if (v.passengerId === id) v.passengerId = null;
                    }
                    p.vehicleId = null; p.vehicleRole = null;
                }
                this.rankSum = Math.max(0, this.rankSum - (p.rank || 0));
                this.rankCount = Math.max(0, this.rankCount - 1);
            }
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
            if (this.lobbyTimerInt)   clearInterval(this.lobbyTimerInt);
            if (this.operatorSelTimer) clearInterval(this.operatorSelTimer);
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
            this.vehicles.clear();
            if (this.suppDevices)    this.suppDevices.clear();
            if (this.repairDrones)   this.repairDrones.clear();
            if (this.scoutDrones)    this.scoutDrones.clear();
            if (this.shieldEmitters) this.shieldEmitters.clear();
            // Reset infantry system for fresh game
            this.infantry.clear();
            this.teamSupply = [
                { current: INF_MAX_SUPPLY, max: INF_MAX_SUPPLY },
                { current: INF_MAX_SUPPLY, max: INF_MAX_SUPPLY },
            ];
            this._infAiTick      = 0;
            this._infAiDidUpdate = false;

            for (const p of this.players.values()) {
                p.res = 150;
                p.hp  = p.maxHp;
                p.rt  = 0;
                p.burnUntil = 0;
                p._lastBurnTick = 0;
                p.vehicleId   = null;
                p.vehicleRole = null;
                // ── Apply weapon loadout ──────────────────────────────────────
                // If the player never explicitly chose a weapon (e.g. timer expired),
                // fall back to the first weapon allowed by their operator.
                if (!p.weaponId && p.operatorId) {
                    const _op = OPERATOR_DEFS[p.operatorId];
                    p.weaponId = _op?.allowedWeapons[0] || null;
                }
                const weapDef = WEAPON_DEFS[p.weaponId];
                p._weaponFireRate = weapDef ? weapDef.fireRate  : 200;   // ms between shots
                p._weaponProjSpd  = weapDef ? weapDef.projSpd   : 700;
                p._weaponDmg      = weapDef ? weapDef.dmg       : 15;
                p._weaponProjR    = weapDef ? weapDef.projR      : 5;
                p._weaponSpread   = weapDef ? weapDef.spread     : 0.04;
                p._weaponPellets  = weapDef ? (weapDef.pellets   || 1) : 1;  // shotgun pellet count
                // Apply weapon movement speed penalty/bonus
                p.spd = Math.round(250 * (weapDef ? (weapDef.moveSpeedMult || 1.0) : 1.0));
                // ── Reset ability state ───────────────────────────────────────
                p.activeAbility   = null;
                p.abilityCooldown = 0;
                p.speedBoostUntil = 0;
                // ── Spawn position ────────────────────────────────────────────
                const spawn = this.mapDef.spawns[p.team];
                p.x   = spawn.x;
                p.y   = spawn.y;
                p._px = -1; p._py = -1; p._ab = -1;
            }

            // Build loadout map for the start packet so clients know everyone's operator
            const startLoadouts = [];
            for (const p of this.players.values()) {
                startLoadouts.push({
                    id: p.id,
                    operatorId: p.operatorId,
                    weaponId  : p.weaponId,
                    role      : OPERATOR_DEFS[p.operatorId]?.role || null,
                });
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
                // ── Loadout data — clients use this to render operator/weapon ──
                loadouts: startLoadouts,
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

            // ── Rebuild spatial hash grids for this tick ───────────────────────────
            this._playerHash.clear();
            for (const p of this.players.values())
                if (p.rt <= 0) this._playerHash.insert(p.x, p.y, p);
            this._buildHash.clear();
            for (const b of this.buildings.values())
                this._buildHash.insert(b.x, b.y, b);
            this._vehicleHash.clear();
            for (const v of this.vehicles.values())
                this._vehicleHash.insert(v.x, v.y, v);

            for (const player of this.players.values()) {
                if (player.rt > 0) {
                    player.rt -= dt;
                    if (player.rt <= 0) {
                        player.rt = 0;
                        player.hp = player.maxHp;
                        player.burnUntil = 0;
                        player._lastBurnTick = 0;
                        // Clear any active dash
                        player.dashUntil = 0; player.dashSpd = 0;
                        player._dashHitPlayers = null; player._dashHitBuilds = null;
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

                // ── Vehicle occupant: movement handled by vehicle tick below ──────────
                if (player.vehicleId) {
                    // Burn DoT still applies inside a vehicle
                    if (player.burnUntil > now && this.phase === PH.ATTACK) {
                        if (!player._lastBurnTick || now - player._lastBurnTick >= 500) {
                            player._lastBurnTick = now;
                            player.hp -= 5;
                            if (player.hp <= 0) {
                                player.hp = 0; player.rt = 5; player.burnUntil = 0;
                                // Eject from vehicle on death
                                const dv = this.vehicles.get(player.vehicleId);
                                if (dv) {
                                    if (dv.driverId   === player.id) dv.driverId   = null;
                                    if (dv.passengerId === player.id) dv.passengerId = null;
                                }
                                player.vehicleId = null; player.vehicleRole = null;
                                this.events.push({ e: EV.PLAYER_DIE, i: player.id });
                            } else {
                                this.events.push({ e: EV.PLAYER_HIT, i: player.id, hp: player.hp });
                            }
                        }
                    }
                    continue;
                }

                let nx = player.x + player.inp.dx * player.spd * (player.slowUntil > now ? 0.4 : (player.speedBoostUntil > now ? 2.0 : 1.0)) * dt;
                let ny = player.y + player.inp.dy * player.spd * (player.slowUntil > now ? 0.4 : (player.speedBoostUntil > now ? 2.0 : 1.0)) * dt;

                // ── Pincer Rush dash override ──────────────────────────────────────────────
                if (player.dashUntil > now) {
                    const STEER_RATE = 1.8; // rad/s max steering — must match client constant
                    // Use Math.sign so steering is discrete (-1/0/1), matching the client's key reading
                    player.dashAngle += Math.sign(player.inp.dx || 0) * STEER_RATE * dt;
                    nx = player.x + Math.cos(player.dashAngle) * player.dashSpd * dt;
                    ny = player.y + Math.sin(player.dashAngle) * player.dashSpd * dt;
                    nx = clamp(nx, player.r, MAP_W - player.r);
                    ny = clamp(ny, player.r, MAP_H - player.r);

                    // Damage sweep each tick while dashing
                    const _hp = player._dashHitPlayers || (player._dashHitPlayers = new Set());
                    const _hb = player._dashHitBuilds  || (player._dashHitBuilds  = new Set());
                    const DASH_DMG = 55, DASH_R = 22;
                    for (const p of this.players.values()) {
                        if (p.id === player.id || p.team === player.team || p.rt > 0 || _hp.has(p.id)) continue;
                        if (Math.hypot(p.x - nx, p.y - ny) <= DASH_R + 14) {
                            _hp.add(p.id);
                            p.hp -= DASH_DMG;
                            if (p.hp <= 0) { p.hp = 0; p.rt = 3; this.events.push({ e: EV.PLAYER_DIE, i: p.id }); }
                            else this.events.push({ e: EV.PLAYER_HIT, i: p.id, hp: p.hp });
                        }
                    }
                    for (const [bid, b] of this.buildings) {
                        if (b.team === player.team || _hb.has(bid)) continue;
                        if (Math.hypot(b.x - nx, b.y - ny) <= DASH_R + 20) {
                            _hb.add(bid);
                            b.hp -= DASH_DMG;
                            if (b.hp <= 0) { b.hp = 0; this.buildings.delete(bid); this.events.push({ e: EV.BUILD_DESTROY, i: bid }); }
                            else this.events.push({ e: EV.BUILD_HIT, i: bid, hp: b.hp });
                        }
                    }
                    player.x = nx;
                    player.y = ny;
                    continue; // skip normal movement, wall collision, etc. while dashing
                }
                if (player.dashUntil && player.dashUntil <= now) {
                    // Dash just ended — clean up
                    player.dashUntil = 0; player.dashSpd = 0;
                    player._dashHitPlayers = null; player._dashHitBuilds = null;
                }

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
                        const rx = b.x - WALL_HALF, ry = b.y - WALL_HALF;
                        if (circleRect(nx, player.y, player.r, rx, ry, WALL_W, WALL_W)) nx = player.x;
                        if (circleRect(player.x, ny, player.r, rx, ry, WALL_W, WALL_W)) ny = player.y;
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

                if (this.phase === PH.ATTACK && player.inp.sh && now - player.lastShot > (player._weaponFireRate || 200)) {
                    player.lastShot = now;
                    const pellets = player._weaponPellets || 1;
                    const spread  = player._weaponSpread  || 0.04;
                    const projOpts = {
                        spd : player._weaponProjSpd || 700,
                        dmg : player._weaponDmg     || 15,
                        r   : player._weaponProjR   || 5,
                        life: 2.0,
                        pt  : player.weaponId || 'pl',
                    };
                    // For multi-pellet weapons (shotguns): spawn one projectile per
                    // pellet, each with an independent random spread offset.
                    // Single-pellet weapons take the same path with pellets === 1.
                    for (let i = 0; i < pellets; i++) {
                        const angle = player.a + (Math.random() - 0.5) * spread;
                        this.spawnProjectile(player.x, player.y, angle, player.team, player.id, projOpts);
                    }
                }
            }

            // ── Passive scrap income during attack phase (4 scrap/sec, synced to client) ──
            if (this.phase === PH.ATTACK) {
                if (!this._lastScrapTick) this._lastScrapTick = now;
                const scrapElapsed = now - this._lastScrapTick;
                if (scrapElapsed >= 1000) {
                    const gain = Math.round(4 * (scrapElapsed / 1000));
                    this._lastScrapTick = now;
                    for (const p of this.players.values()) {
                        if (p.rt > 0) continue;
                        p.res += gain;
                        this.events.push({ e: EV.RES_CHANGE, i: p.id, r: p.res });
                    }
                }
            }

            // ── Vehicle tick: movement, weapons, occupant positioning ─────────────────
            for (const [vid, veh] of this.vehicles) {
                const vDef = VEHICLE_DEFS[veh.type];
                if (!vDef) continue;

                const driver    = veh.driverId    ? this.players.get(veh.driverId)    : null;
                const passenger = veh.passengerId ? this.players.get(veh.passengerId) : null;

                // ── Move vehicle (driver controls) ────────────────────────────────
                if (driver) {
                    const spd      = vDef.spd;

                    let nvx, nvy;

                    if (vDef.isMech) {
                        // ── Biped movement: world-space WASD strafe, heading = driver aim ──
                        veh.a = driver.a;   // mech body faces where player aims
                        // Track movement direction separately for leg rendering
                        const mdx = driver.inp.dx, mdy = driver.inp.dy;
                        if (Math.hypot(mdx, mdy) > 0.05) veh.moveA = Math.atan2(mdy, mdx);
                        // (if not moving, keep previous moveA so legs hold their last direction)
                        if (driver.inp.sh) {
                            // Stop-to-shoot: freeze position while trigger is held
                            nvx = veh.x; nvy = veh.y;
                        } else {
                            nvx = veh.x + driver.inp.dx * spd * dt;
                            nvy = veh.y + driver.inp.dy * spd * dt;
                        }
                    } else {
                        // ── Tank-style steering ────────────────────────────────────
                        const turnRate = vDef.turnRate || 2.2;
                        const fwdX  = Math.cos(veh.a), fwdY  = Math.sin(veh.a);
                        const sideX = Math.cos(veh.a + Math.PI / 2), sideY = Math.sin(veh.a + Math.PI / 2);
                        const throttle = driver.inp.dx * fwdX  + driver.inp.dy * fwdY;
                        const steer    = driver.inp.dx * sideX + driver.inp.dy * sideY;
                        veh.a += steer * turnRate * dt;
                        if (driver.inp.sh && !vDef.isAPC) {
                            // Stop-to-shoot: non-APC tank-style vehicles freeze while firing
                            nvx = veh.x; nvy = veh.y;
                        } else {
                            nvx = veh.x + Math.cos(veh.a) * throttle * spd * dt;
                            nvy = veh.y + Math.sin(veh.a) * throttle * spd * dt;
                        }
                    }

                    // Map bounds
                    nvx = clamp(nvx, veh.r, MAP_W - veh.r);
                    nvy = clamp(nvy, veh.r, MAP_H - veh.r);

                    // Wall collision
                    for (const b of this.buildings.values()) {
                        if (b.type === 'w') {
                            if (circleRect(nvx, veh.y, veh.r, b.x - WALL_HALF, b.y - WALL_HALF, WALL_W, WALL_W)) nvx = veh.x;
                            if (circleRect(veh.x, nvy, veh.r, b.x - WALL_HALF, b.y - WALL_HALF, WALL_W, WALL_W)) nvy = veh.y;
                        }
                    }

                    // Water collision
                    if (this.isOnWater(nvx, veh.y, veh.r)) nvx = veh.x;
                    if (this.isOnWater(veh.x, nvy, veh.r)) nvy = veh.y;

                    // Build-phase side restriction
                    if (this.phase === PH.BUILD) {
                        const mid = MAP_W / 2;
                        if (veh.team === 0) nvx = Math.min(nvx, mid - veh.r);
                        else               nvx = Math.max(nvx, mid + veh.r);
                    }

                    veh.x = nvx; veh.y = nvy;

                    // ── Driver weapon (hull MG / special) ────────────────────────
                    if (this.phase === PH.ATTACK && driver.inp.sh && now - veh.lastDriverShot > vDef.driverFireRate) {
                        veh.lastDriverShot = now;

                        // Knox Guardian — driver braces instead of shooting
                        if (vDef.driverBrace) {
                            veh.braceUntil = now + vDef.braceDuration;
                            this.events.push({ e: EV.VEH_BRACE, i: veh.id, until: veh.braceUntil });
                            // Heal nearby friendly buildings
                            for (const b of this.buildings.values()) {
                                if (b.team !== veh.team) continue;
                                if (dist(veh.x, veh.y, b.x, b.y) <= vDef.braceRadius) {
                                    const heal = Math.min(b.maxHp - b.hp, vDef.braceBuildHeal);
                                    if (heal > 0) {
                                        b.hp += heal;
                                        this.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
                                    }
                                }
                            }
                        } else {
                            // Normal driver shot (includes mech main weapon)
                            const projLife = vDef.driverMaxRange
                                ? (vDef.driverMaxRange / (vDef.driverProjSpd || 500))
                                : 2.0;
                            const dm = vDef.driverMuzzle;
                            const dp = dm ? muzzleWorld(veh.x, veh.y, driver.a, dm.x, dm.y)
                                        : { x: veh.x, y: veh.y };
                            this.spawnProjectile(dp.x, dp.y, driver.a, veh.team, driver.id, {
                                spd: vDef.driverProjSpd, dmg: vDef.driverDmg,
                                r: vDef.driverProjR || 4, life: projLife,
                                slow: vDef.driverSlow || false,
                                intercept: vDef.driverIntercept || false,
                                burn: vDef.driverBurnChance && Math.random() < vDef.driverBurnChance,
                                bonusVsWall: vDef.driverBonusVsWall || 0,
                                pt: veh.type + '_drv',
                            });
                        }
                    }
                }

                // ── Automatic drone tick ───────────────────────────────────────────
                if (vDef.drone && this.phase === PH.ATTACK) {
                    const orbitR   = vDef.droneOrbitR   || 52;
                    const orbitSpd = vDef.droneOrbitSpd || 1.6;
                    const dRange   = vDef.droneRange     || 280;

                    // Orbit angle advances continuously
                    veh.droneOrbitAngle = ((veh.droneOrbitAngle || 0) + orbitSpd * dt) % (Math.PI * 2);
                    veh.droneX = veh.x + Math.cos(veh.droneOrbitAngle) * orbitR;
                    veh.droneY = veh.y + Math.sin(veh.droneOrbitAngle) * orbitR;

                    // Auto-find closest enemy and shoot
                    if (now - veh.droneLastShot > vDef.droneFireRate) {
                        const target = this.findClosestEnemy(veh.droneX, veh.droneY, veh.team, dRange);
                        if (target) {
                            veh.droneLastShot = now;
                            const da = Math.atan2(target.y - veh.droneY, target.x - veh.droneX);
                            veh.droneA = da;
                            this.spawnProjectile(veh.droneX, veh.droneY, da, veh.team, veh.id, {
                                spd: vDef.droneProjSpd, dmg: vDef.droneDmg,
                                r: vDef.droneProjR || 3, life: 1.6,
                                pt: veh.type + '_drone',
                            });
                        }
                    }
                }

                // Track passenger aim angle (for client cannon rendering)
                if (passenger) {
                    veh.pa = passenger.a;

                    // ── Citadel Interceptor: passive intercept dome — first entry, one roll ──
                    if (vDef.passengerIntercept && this.phase === PH.ATTACK) {
                        const intR   = vDef.passengerInterceptR || 160;
                        const chance = vDef.passengerIntercept;
                        const srcId  = 'veh_' + veh.id;   // unique source key for this vehicle
                        for (const [pid, p] of this.projs) {
                            if (p.team === veh.team) continue;
                            if (dist(veh.x, veh.y, p.x, p.y) >= intR) continue;
                            if (p.interceptSeen.has(srcId)) continue;
                            p.interceptSeen.add(srcId);
                            if (Math.random() < chance) {
                                this.projs.delete(pid);
                                this.events.push({ e: EV.PROJ_DESTROY, i: pid,
                                                sx: Math.round(veh.x), sy: Math.round(veh.y),
                                                px: Math.round(p.x),   py: Math.round(p.y) });
                            }
                        }
                    }

                    // ── Passenger weapon (main gun) ────────────────────────────────
                    if (this.phase === PH.ATTACK && passenger.inp.sh && now - veh.lastPassengerShot > vDef.passengerFireRate) {
                        veh.lastPassengerShot = now;
                        const pm = vDef.passengerMuzzle;
                        const pp = pm ? muzzleWorld(veh.x, veh.y, passenger.a, pm.x, pm.y)
                                    : { x: veh.x, y: veh.y };
                        this.spawnProjectile(pp.x, pp.y, passenger.a, veh.team, passenger.id, {
                            spd: vDef.passengerProjSpd, dmg: vDef.passengerDmg,
                            r: vDef.passengerProjR || 8, life: 2.5,
                            splash: vDef.passengerSplash || 0,
                            slow: vDef.passengerSlow || false,
                            pierce: vDef.passengerPierce || false,
                            pt: veh.type + '_pax',
                        });
                    }
                }

                // ── Carry all occupants to vehicle position ────────────────────────
                if (driver)    { driver.x    = veh.x; driver.y    = veh.y; }
                if (passenger) { passenger.x = veh.x; passenger.y = veh.y; }
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

                    // ── Intercept: first entry into field = one roll, never again ─────
                    if ((b.intercept || 0) > 0) {
                        const intR = 140;
                        for (const [pid, p] of this.projs) {
                            if (p.team === b.team) continue;
                            if (dist(b.x, b.y, p.x, p.y) >= intR) continue;
                            if (p.interceptSeen.has(b.id)) continue;   // already rolled for this source
                            p.interceptSeen.add(b.id);
                            if (Math.random() < (b.intercept || 0)) {
                                this.projs.delete(pid);
                                // sx/sy lets client draw the intercept laser
                                this.events.push({ e: EV.PROJ_DESTROY, i: pid,
                                                sx: Math.round(b.x), sy: Math.round(b.y),
                                                px: Math.round(p.x),  py: Math.round(p.y) });
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
                // Record position before movement for raycast collision this tick
                const prevX = p.x;
                const prevY = p.y;
                p.px = prevX;   // store on projectile so infantry collision can use it
                p.py = prevY;
                p.x += Math.cos(p.a) * p.spd * dt;
                p.y += Math.sin(p.a) * p.spd * dt;
                p.life -= dt;

                // ── Homing steering (micro_missile_swarm) ───────────────────────────────
                if (p.homing && p.life > 0) {
                    const HOMING_R   = 380;
                    const TURN_RATE  = 5.5;    // rad/s — very aggressive tracking
                    const RECENT_WIN = 6000;   // ms window for "recently damaged"
                    let bestTarget = null, bestScore = 0;
                    for (const t of this.players.values()) {
                        if (t.team === p.team || t.rt > 0) continue;
                        const d = dist(p.x, p.y, t.x, t.y);
                        if (d > HOMING_R) continue;
                        // Heavily prioritise recently-damaged targets
                        const recentBonus = (now - (t.lastDamaged || 0)) < RECENT_WIN ? 3.0 : 1.0;
                        const score = recentBonus / (d + 1);
                        if (score > bestScore) { bestScore = score; bestTarget = t; }
                    }
                    if (bestTarget) {
                        const targetA = Math.atan2(bestTarget.y - p.y, bestTarget.x - p.x);
                        let da = targetA - p.a;
                        while (da > Math.PI) da -= Math.PI * 2;
                        while (da < -Math.PI) da += Math.PI * 2;
                        p.a += Math.sign(da) * Math.min(Math.abs(da), TURN_RATE * dt);
                    }
                }

                let dead         = p.life <= 0 || p.x < 0 || p.x > MAP_W || p.y < 0 || p.y > MAP_H;
                let hitSomething = false;

                // ── Repair Bay wall collision (Velarus) ─────────────────────────────────
                if (!dead && this.repairBays && this.repairBays.size > 0 && p.team !== undefined) {
                    for (const bay of this.repairBays.values()) {
                        if (bay.expiresAt <= now || p.team === bay.team) continue;
                        // Wall is perpendicular to bay.a; its tangent direction is bay.a + π/2
                        const WALL_HALF_W = 65;
                        const wallNx = Math.cos(bay.a), wallNy = Math.sin(bay.a);
                        const wallTx = -wallNy,          wallTy =  wallNx;
                        // Current projectile distance from wall along its normal
                        const dx   = p.x - bay.x, dy = p.y - bay.y;
                        const nNow = dx * wallNx + dy * wallNy;
                        const tNow = dx * wallTx + dy * wallTy;
                        // Use stored previous position (already recorded above)
                        const nPrev = (prevX - bay.x) * wallNx + (prevY - bay.y) * wallNy;
                        if (nNow * nPrev <= 0 && Math.abs(tNow) < WALL_HALF_W) {
                            dead = true;
                            hitSomething = true;  // ensures PROJ_DESTROY is sent so client removes bullet visually
                            break;
                        }
                    }
                }

                if (!dead && this.phase === PH.ATTACK) {
                    for (const target of this._playerHash.query(p.x, p.y, 80)) {
                        if (dead) break;
                        if (target.rt > 0 || target.team === p.team) continue;
                        if (segCircle(prevX, prevY, p.x, p.y, target.x, target.y, target.r + p.r)) {
                            target.hp -= p.dmg;
                            target.lastDamaged = now;
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
                            // Intercept shots also destroy nearby enemy projectiles on impact
                            if (p.intercept) {
                                for (const [epid, ep] of this.projs) {
                                    if (ep.team === p.team) continue;
                                    if (dist(p.x, p.y, ep.x, ep.y) < 80) {
                                        this.projs.delete(epid);
                                        this.events.push({ e: EV.PROJ_DESTROY, i: epid });
                                    }
                                }
                            }
                        }
                    }

                    if (!dead) {
                        for (const b of this._buildHash.query(p.x, p.y, 90)) {
                            if (b.team === p.team) continue;
                            const hit =
                                (b.type === 'w'  && segRect(prevX, prevY, p.x, p.y, p.r, b.x-WALL_HALF, b.y-WALL_HALF, WALL_W, WALL_W)) ||
                                (b.type === 't'  && segCircle(prevX, prevY, p.x, p.y, b.x, b.y, b.r + p.r)) ||
                                (b.type === 'vd' && segCircle(prevX, prevY, p.x, p.y, b.x, b.y, b.r + p.r)) ||
                                (b.type === 'bk' && segCircle(prevX, prevY, p.x, p.y, b.x, b.y, (b.r || 20) + p.r));
                            if (hit) {
                                const dmg = p.bonusVsBldg ? Math.round(p.dmg * 1.5)
                                        : (p.bonusVsWall && b.type === 'w') ? Math.round(p.dmg * p.bonusVsWall)
                                        : p.dmg;
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
                                    this.buildings.delete(b.id);
                                    this.events.push({ e: EV.BUILD_DESTROY, i: b.id });
                                } else {
                                    this.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
                                }
                                break;
                            }
                        }
                    }

                    if (!dead) {
                        for (const v of this._vehicleHash.query(p.x, p.y, 120)) {
                            if (v.team === p.team) continue;
                            if (segCircle(prevX, prevY, p.x, p.y, v.x, v.y, v.r + p.r)) {
                                const vDef2 = VEHICLE_DEFS[v.type] || {};
                                const bracing = vDef2.driverBrace && v.braceUntil && now < v.braceUntil;
                                const actualDmg = bracing ? Math.round(p.dmg * (1 - (vDef2.braceDmgReduce || 0))) : p.dmg;
                                v.hp -= actualDmg;
                                if (!p.pierce) { dead = true; }
                                hitSomething = true;
                                if (v.hp <= 0) {
                                    // Eject occupants at vehicle position
                                    for (const oid of [v.driverId, v.passengerId]) {
                                        if (!oid) continue;
                                        const op = this.players.get(oid);
                                        if (op) { op.vehicleId = null; op.vehicleRole = null; }
                                    }
                                    // APC: bail out infantry — they scatter and fight on
                                    if (v.infantryIds && v.infantryIds.length > 0) {
                                        let bailSpread = 0;
                                        for (const iid of v.infantryIds) {
                                            const inf = this.infantry.get(iid);
                                            if (!inf) continue;
                                            const bailA = (bailSpread / v.infantryIds.length) * Math.PI * 2;
                                            inf.x      = clamp(v.x + Math.cos(bailA) * 40, inf.r, MAP_W - inf.r);
                                            inf.y      = clamp(v.y + Math.sin(bailA) * 40, inf.r, MAP_H - inf.r);
                                            inf.state  = 'default';
                                            inf.followPlayerId = null;
                                            bailSpread++;
                                        }
                                        v.infantryIds = [];
                                    }
                                    this.vehicles.delete(v.id);
                                    this.events.push({ e: EV.VEHICLE_DESTROY, id: v.id });
                                }
                                break;
                            }
                        }
                    }

                    if (!dead) {
                        for (const c of this.cores) {
                            if (c.team === p.team || !segCircle(prevX, prevY, p.x, p.y, c.x, c.y, c.r + p.r)) continue;
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

            // ── EPA wall intercept: first entry into field = one roll, never again ─────
            if (this.phase === PH.ATTACK) {
                for (const b of this.buildings.values()) {
                    if (b.type !== 'w' || !b.intercept || b.team === undefined) continue;
                    const intR = 100;
                    for (const [pid, p] of this.projs) {
                        if (p.team === b.team) continue;
                        if (dist(b.x, b.y, p.x, p.y) >= intR) continue;
                        if (p.interceptSeen.has(b.id)) continue;
                        p.interceptSeen.add(b.id);
                        if (Math.random() < b.intercept) {
                            this.projs.delete(pid);
                            this.events.push({ e: EV.PROJ_DESTROY, i: pid,
                                            sx: Math.round(b.x), sy: Math.round(b.y),
                                            px: Math.round(p.x),  py: Math.round(p.y) });
                        }
                    }
                }
            }

            // ── Repair Drones (Orion ability) ─────────────────────────────────────────
            if (this.repairDrones && this.repairDrones.size > 0) {
                const DRONE_REPAIR_RATE = 600;
                const DRONE_REPAIR_AMT  = 18;
                const DRONE_RADIUS      = 110;
                for (const [did, drone] of this.repairDrones) {
                    if (now >= drone.expiresAt) { this.repairDrones.delete(did); continue; }
                    if (now - drone.lastRepair < DRONE_REPAIR_RATE) continue;
                    drone.lastRepair = now;
                    for (const p of this.players.values()) {
                        if (p.team !== drone.team || p.rt > 0 || p.hp >= p.maxHp) continue;
                        if (Math.hypot(p.x - drone.x, p.y - drone.y) <= DRONE_RADIUS) {
                            p.hp = Math.min(p.maxHp, p.hp + DRONE_REPAIR_AMT);
                            this.events.push({ e: EV.PLAYER_HIT, i: p.id, hp: p.hp });
                        }
                    }
                    for (const b of this.buildings.values()) {
                        if (b.team !== drone.team || b.hp >= b.maxHp) continue;
                        if (Math.hypot(b.x - drone.x, b.y - drone.y) <= DRONE_RADIUS) {
                            b.hp = Math.min(b.maxHp, b.hp + DRONE_REPAIR_AMT);
                            this.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
                        }
                    }
                }
            }

            // ── Scout Drones (Daemon ability) ─────────────────────────────────────────
            if (this.scoutDrones && this.scoutDrones.size > 0) {
                const DRONE_HITBOX = 14;
                for (const [did, drone] of this.scoutDrones) {
                    if (drone.hp <= 0) {
                        this.scoutDrones.delete(did);
                        this.events.push({ e: EV.DRONE_DESTROY, id: did, tp: 'scout' });
                        continue;
                    }
                    // Check if any enemy projectile hits the drone
                    for (const [pid, p] of this.projs) {
                        if (p.team === drone.team) continue;
                        if (Math.hypot(p.x - drone.x, p.y - drone.y) <= DRONE_HITBOX) {
                            drone.hp -= p.dmg;
                            this.projs.delete(pid);
                            this.events.push({ e: EV.DRONE_HIT, id: did, hp: drone.hp, tp: 'scout' });
                            if (drone.hp <= 0) break;
                        }
                    }
                    if (drone.hp <= 0) {
                        this.scoutDrones.delete(did);
                        this.events.push({ e: EV.DRONE_DESTROY, id: did, tp: 'scout' });
                    }
                    // No auto-targeting or auto-fire — drone fires only via drone_fire messages
                }
            }

            // ── Overridden CIWS (Kashtan ability) ─────────────────────────────────────
            if (this.phase === PH.ATTACK) {
                for (const player of this.players.values()) {
                    if (!player.ciwsUntil || now >= player.ciwsUntil || player.rt > 0) continue;
                    if (!player._ciwsLastShot) player._ciwsLastShot = 0;
                    if (!player._ciwsMissile)  player._ciwsMissile  = 0;
                    // Rapid bullet spray — 80ms per burst of 3
                    if (now - player._ciwsLastShot >= 80) {
                        player._ciwsLastShot = now;
                        // Find closest enemy vehicle or player
                        let targetX = null, targetY = null, minD = 600;
                        for (const v of this.vehicles.values()) {
                            if (v.team === player.team) continue;
                            const d = Math.hypot(v.x - player.x, v.y - player.y);
                            if (d < minD) { minD = d; targetX = v.x; targetY = v.y; }
                        }
                        for (const p of this.players.values()) {
                            if (p.team === player.team || p.rt > 0) continue;
                            const d = Math.hypot(p.x - player.x, p.y - player.y);
                            if (d < minD) { minD = d; targetX = p.x; targetY = p.y; }
                        }
                        if (targetX !== null) {
                            const baseA = Math.atan2(targetY - player.y, targetX - player.x);
                            // 3-round CIWS spray with spread
                            for (let _c = 0; _c < 3; _c++) {
                                const spread = (Math.random() - 0.5) * 0.3;
                                this.spawnProjectile(player.x, player.y, baseA + spread, player.team, player.id, {
                                    spd: 700, dmg: 8, r: 3, life: 0.7, pt: 'bgm_ciws',
                                });
                            }
                        }
                    }
                    // Micromissile every 1200ms vs vehicles
                    if (now - player._ciwsMissile >= 1200) {
                        player._ciwsMissile = now;
                        let vTarget = null, minVD = 500;
                        for (const v of this.vehicles.values()) {
                            if (v.team === player.team) continue;
                            const d = Math.hypot(v.x - player.x, v.y - player.y);
                            if (d < minVD) { minVD = d; vTarget = v; }
                        }
                        if (vTarget) {
                            const mA = Math.atan2(vTarget.y - player.y, vTarget.x - player.x);
                            this.spawnProjectile(player.x, player.y, mA, player.team, player.id, {
                                spd: 460, dmg: 38, r: 6, life: 1.2,
                                splash: 40, bonusVsBldg: true, pt: 'bgm_micromissile',
                            });
                        }
                    }
                }
            }

            // ── Shield Emitters (Konig ability) ───────────────────────────────────────
            if (this.shieldEmitters && this.shieldEmitters.size > 0 && this.phase === PH.ATTACK) {
                const SHIELD_HALF_ARC = Math.PI / 3;  // ±60°
                const SHIELD_RADIUS   = 90;
                for (const [sid, shield] of this.shieldEmitters) {
                    if (now >= shield.expiresAt || shield.hp <= 0) {
                        this.shieldEmitters.delete(sid); continue;
                    }
                    // Silently absorb enemy projectiles that enter the shield arc
                    for (const [pid, p] of this.projs) {
                        if (p.team === shield.team) continue;
                        const d = Math.hypot(p.x - shield.x, p.y - shield.y);
                        if (d > SHIELD_RADIUS) continue;
                        const angToProj = Math.atan2(p.y - shield.y, p.x - shield.x);
                        let diff = angToProj - shield.a;
                        while (diff >  Math.PI) diff -= Math.PI * 2;
                        while (diff < -Math.PI) diff += Math.PI * 2;
                        if (Math.abs(diff) > SHIELD_HALF_ARC) continue;
                        // Absorb: delete projectile, drain shield HP, notify clients
                        shield.hp -= p.dmg * 0.5;
                        this.projs.delete(pid);
                        this.events.push({ e: EV.PROJ_DESTROY, i: pid });
                    }
                }
            }

            // ── Suppression Devices (Duster ability) ─────────────────────────────────
            if (this.suppDevices && this.suppDevices.size > 0 && this.phase === PH.ATTACK) {
                const SUPP_FIRE_RATE = 180;  // ms between shots per device
                const SUPP_CONE     = 0.45;  // half-angle of fire cone (radians, ~26°)
                for (const [did, dev] of this.suppDevices) {
                    if (now >= dev.expiresAt) { this.suppDevices.delete(did); continue; }
                    if (now - dev.lastShot < SUPP_FIRE_RATE) continue;
                    dev.lastShot = now;
                    // Fire 2 rounds per burst in a cone
                    for (let _si = 0; _si < 2; _si++) {
                        const spread = (Math.random() - 0.5) * SUPP_CONE * 2;
                        this.spawnProjectile(dev.x, dev.y, dev.a + spread, dev.team, null, {
                            spd: 420, dmg: 7, r: 4, life: 1.3,
                            slow: true, pt: 'supp_field',
                        });
                    }
                }
            }

            // ── Machine Gun Arms (Drake EPA Suppression) ─────────────────────────────
            if (this.phase === PH.ATTACK) {
                for (const player of this.players.values()) {
                    if (!player.machineGunArmsUntil || now >= player.machineGunArmsUntil || player.rt > 0) continue;
                    if (!player._mgaLastShot) player._mgaLastShot = 0;
                    // Fire both arms alternately at ~65ms intervals — bullets origin from each arm muzzle
                    if (now - player._mgaLastShot >= 65) {
                        player._mgaLastShot = now;
                        const CONE = 0.28;
                        const ARM_OUT   = 22;  // lateral distance from body centre to arm muzzle
                        const ARM_FWD   = 28;  // forward reach to muzzle — matches VFX drawn tip
                        const perpA = player.a + Math.PI / 2;
                        for (let _arm = 0; _arm < 2; _arm++) {
                            const spread  = (Math.random() - 0.5) * CONE * 2;
                            const sideSign = _arm === 0 ? -1 : 1;
                            // Lateral offset (perpendicular to aim)
                            const lx = player.x + Math.cos(perpA) * sideSign * ARM_OUT;
                            const ly = player.y + Math.sin(perpA) * sideSign * ARM_OUT;
                            // Forward offset along aim
                            const ox = lx + Math.cos(player.a) * ARM_FWD;
                            const oy = ly + Math.sin(player.a) * ARM_FWD;
                            this.spawnProjectile(ox, oy, player.a + spread, player.team, player.id, {
                                spd: 660, dmg: 11, r: 3, life: 0.9, pt: 'epa_mga',
                            });
                        }
                    }
                }
            }

            // ── Restoration Fields (Caesarium EPA Support) ───────────────────────────
            if (this.restorationFields && this.restorationFields.size > 0 && this.phase === PH.ATTACK) {
                const HEAL_RADIUS   = 160;
                const HEAL_INTERVAL = 1000;  // ms between heal pulses
                const HEAL_AMOUNT   = 12;
                for (const [fid, field] of this.restorationFields) {
                    if (now >= field.expiresAt) { this.restorationFields.delete(fid); continue; }
                    if (now - field.lastHeal < HEAL_INTERVAL) continue;
                    field.lastHeal = now;
                    for (const p of this.players.values()) {
                        if (p.team !== field.team || p.rt > 0) continue;
                        if (Math.hypot(p.x - field.x, p.y - field.y) <= HEAL_RADIUS) {
                            p.hp = Math.min(p.maxHp, p.hp + HEAL_AMOUNT);
                            this.events.push({ e: EV.PLAYER_HIT, i: p.id, hp: p.hp });
                        }
                    }
                }
            }

            // ── Repair Bays (Velarus EPA Engineer) ───────────────────────────────────
            if (this.repairBays && this.repairBays.size > 0 && this.phase === PH.ATTACK) {
                const BAY_REPAIR_RADIUS   = 110;
                const BAY_REPAIR_INTERVAL = 1200;
                const BAY_REPAIR_AMOUNT   = 28;
                for (const [bid, bay] of this.repairBays) {
                    if (now >= bay.expiresAt) { this.repairBays.delete(bid); continue; }
                    if (now - bay.lastRepair < BAY_REPAIR_INTERVAL) continue;
                    bay.lastRepair = now;
                    for (const b of this.buildings.values()) {
                        if (b.team !== bay.team) continue;
                        if (Math.hypot(b.x - bay.x, b.y - bay.y) <= BAY_REPAIR_RADIUS) {
                            b.hp = Math.min(b.maxHp, b.hp + BAY_REPAIR_AMOUNT);
                            this.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
                        }
                    }
                    for (const v of this.vehicles.values()) {
                        if (v.team !== bay.team) continue;
                        if (Math.hypot(v.x - bay.x, v.y - bay.y) <= BAY_REPAIR_RADIUS) {
                            v.hp = Math.min(v.maxHp, v.hp + BAY_REPAIR_AMOUNT);
                        }
                    }
                }
            }

            // Force-broadcast immediately when the game just ended so the WIN event
            // is never stranded in this.events by the early-return at the top of tick().
            const justEnded = this.phase === PH.END && this._prevPhase !== PH.END;

            // ── Barracks auto-production ────────────────────────────────────────────
            if (this.phase === PH.ATTACK) {
                for (const b of this.buildings.values()) {
                    if (b.type !== 'bk') continue;
                    if (now - (b.lastProduction || 0) < INF_PROD_INTERVAL) continue;
                    const supply = this.teamSupply[b.team];
                    if (supply.current < INF_PROD_AMOUNT) continue;
                    b.lastProduction = now;
                    for (let _pi = 0; _pi < INF_PROD_AMOUNT; _pi++) {
                        this.spawnInfantry(b.team, b.x, b.y, b.infantryMode || 'grunt');
                    }
                }
            }

            // ── Infantry AI (5 Hz) ──────────────────────────────────────────────────
            this._infAiTick = (this._infAiTick || 0) + 1;
            if (this._infAiTick >= INF_AI_TICKS) {
                this._infAiTick = 0;
                const dtAi = INF_AI_TICKS / TICK_RATE;
                this.tickInfantry(dtAi, now);
            }

            // ── Projectile vs Infantry collision ────────────────────────────────────
            if (this.phase === PH.ATTACK && this.infantry.size > 0) {
                for (const [pid, p] of this.projs) {
                    if (p.life <= 0) continue;
                    for (const [iid, inf] of this.infantry) {
                        if (inf.team === p.team || inf.state === 'in_apc') continue;
                        // Use stored prevX/prevY for accurate segment-circle raycast
                        const _ipx = p.px !== undefined ? p.px : p.x;
                        const _ipy = p.py !== undefined ? p.py : p.y;
                        if (segCircle(_ipx, _ipy, p.x, p.y, inf.x, inf.y, inf.r + p.r)) {
                            inf.hp -= p.dmg;
                            this.projs.delete(pid);
                            if (inf.hp <= 0) {
                                this.killInfantry(iid);
                            } else {
                                // HP-only event — keeps HP out of movement packets
                                this.events.push({ e: EV.INF_HIT, i: iid, hp: inf.hp });
                            }
                            break;
                        }
                    }
                }
            }

            // ── APC carries its infantry cargo ──────────────────────────────────────
            for (const veh of this.vehicles.values()) {
                if (!veh.infantryIds || veh.infantryIds.length === 0) continue;
                for (let _ii = veh.infantryIds.length - 1; _ii >= 0; _ii--) {
                    const inf = this.infantry.get(veh.infantryIds[_ii]);
                    if (!inf) { veh.infantryIds.splice(_ii, 1); continue; }
                    inf.x = veh.x; inf.y = veh.y;
                }
            }

            this._prevPhase = this.phase;
            if (justEnded || this.tickCount % SNAP_EVERY === 0) this.broadcastSnapshot();

            // Log bandwidth stats every ~10 s (300 ticks at 30 Hz)
            if (this.tickCount % 300 === 0) this.logBandwidthStats();
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
                bonusVsWall: opts.bonusVsWall || 0,
                burn: opts.burn || false,
                intercept: opts.intercept || false,
                pierce: opts.pierce || false,
                homing: opts.homing || false,
                pt: opts.pt || 't',
                // Tracks which intercept sources have already processed this projectile.
                // Each source gets exactly one roll — no repeated chances per tick.
                interceptSeen: new Set(),
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
            for (const p of this._playerHash.query(x, y, maxRange)) {
                if (p.team === team || p.rt > 0) continue;
                const d = dist(x, y, p.x, p.y);
                if (d < minD) { minD = d; closest = p; }
            }
            // Also consider enemy infantry as valid turret targets
            for (const inf of this._infHash.query(x, y, maxRange)) {
                if (inf.team === team || inf.state === 'in_apc') continue;
                const d = dist(x, y, inf.x, inf.y);
                if (d < minD) { minD = d; closest = { x: inf.x, y: inf.y, hp: inf.hp, id: inf.id, r: inf.r, _isInfantry: true }; }
            }
            return closest;
        }

        // ── Infantry AI target finder — players, turrets, vehicles, core ───────────
        // Uses spatial hash grids for O(k) lookup instead of O(N) full scans.
        findClosestEnemyForInfantry(x, y, team, maxRange) {
            let target = null, minD = maxRange;
            // Enemy players
            for (const p of this._playerHash.query(x, y, maxRange)) {
                if (p.team === team || p.rt > 0) continue;
                const d = dist(x, y, p.x, p.y);
                if (d < minD) { minD = d; target = { x: p.x, y: p.y }; }
            }
            // Enemy buildings (turrets, depots, barracks)
            for (const b of this._buildHash.query(x, y, maxRange)) {
                if (b.team === team) continue;
                if (b.type !== 't' && b.type !== 'vd' && b.type !== 'bk') continue;
                const d = dist(x, y, b.x, b.y);
                if (d < minD) { minD = d; target = { x: b.x, y: b.y }; }
            }
            // Enemy vehicles
            for (const v of this._vehicleHash.query(x, y, maxRange)) {
                if (v.team === team) continue;
                const d = dist(x, y, v.x, v.y);
                if (d < minD) { minD = d; target = { x: v.x, y: v.y }; }
            }
            // Enemy core — infantry stop and shoot when in range
            const enemyCore = this.cores[1 - team];
            if (enemyCore) {
                const d = dist(x, y, enemyCore.x, enemyCore.y);
                if (d < minD) { minD = d; target = { x: enemyCore.x, y: enemyCore.y }; }
            }
            return target;
        }

        // ── Supply helpers ────────────────────────────────────────────────────────
        broadcastTeamSupply(team) {
            const s = this.teamSupply[team];
            for (const p of this.players.values()) {
                if (p.team !== team) continue;
                if (p.ws.readyState === WebSocket.OPEN) {
                    p.ws.send(JSON.stringify({ t: 'supply', tm: team, cur: s.current, max: s.max }));
                }
            }
        }

        // ── Spawn a single infantry unit ──────────────────────────────────────────
        spawnInfantry(team, x, y, type = 'grunt') {
            const supply = this.teamSupply[team];
            if (supply.current < 1) return null;
            const def = INFANTRY_DEFS[type];
            if (!def) return null;

            const id     = shortId();
            const jx     = (Math.random() - 0.5) * 36;
            const jy     = (Math.random() - 0.5) * 36;
            const spawnX = clamp(x + jx, def.r, MAP_W - def.r);
            const spawnY = clamp(y + jy, def.r, MAP_H - def.r);

            const inf = {
                id, type, team,
                x: spawnX, y: spawnY,
                r: def.r, hp: def.hp, maxHp: def.maxHp,
                a        : team === 0 ? 0 : Math.PI,
                state    : 'default',   // 'default' | 'follow' | 'boarding_apc' | 'in_apc'
                followPlayerId: null,
                boardingApcId : null,   // set while walking toward an APC to board it
                lastShot : 0,
                _px: -9999, _py: -9999, _ab: -1, // delta sentinels
            };

            supply.current -= 1;
            this.infantry.set(id, inf);

            this.events.push({
                e: EV.INF_SPAWN, i: id,
                x: Math.round(inf.x), y: Math.round(inf.y),
                tm: team, tp: type,
                hp: inf.hp, mhp: inf.maxHp,
            });
            this.broadcastTeamSupply(team);
            return inf;
        }

        // ── Remove an infantry unit and return supply ─────────────────────────────
        killInfantry(id) {
            const inf = this.infantry.get(id);
            if (!inf) return;

            // Bail out of any APC
            for (const veh of this.vehicles.values()) {
                if (!veh.infantryIds) continue;
                const idx = veh.infantryIds.indexOf(id);
                if (idx !== -1) veh.infantryIds.splice(idx, 1);
            }

            // Return supply
            const s = this.teamSupply[inf.team];
            s.current = Math.min(s.max, s.current + 1);

            this.infantry.delete(id);
            this.events.push({ e: EV.INF_DIE, i: id });
            this.broadcastTeamSupply(inf.team);
        }

        // ── Shout: attach nearby friendly infantry to this player ─────────────────
        handleShout(player) {
            if (this.phase !== PH.ATTACK || player.rt > 0) return;
            let cnt = 0;
            for (const inf of this.infantry.values()) {
                if (inf.team !== player.team || inf.state === 'in_apc') continue;
                if (dist(player.x, player.y, inf.x, inf.y) <= SHOUT_RADIUS) {
                    inf.state          = 'follow';
                    inf.followPlayerId = player.id;
                    cnt++;
                }
            }
            this.events.push({
                e: EV.SHOUT,
                i: player.id,
                x: Math.round(player.x), y: Math.round(player.y),
                cnt,
            });
        }

        // ── Produce an APC from a barracks ───────────────────────────────────────
        handleBarracksAPC(player, barracksId) {
            if (this.phase !== PH.ATTACK && this.phase !== PH.BUILD) return;
            const b = this.buildings.get(barracksId);
            if (!b || b.type !== 'bk' || b.team !== player.team) return;
            if (player.res < APC_FROM_BARRACKS_COST) return;

            const now = Date.now();
            if (now - (b.apcCooldown || 0) < BARRACKS_APC_COOLDOWN) return;
            b.apcCooldown = now;

            // Pick this team's faction-specific APC
            const faction = this.teamFactions[player.team] || 'roe';
            const apcType = faction + '_apc';
            const vDef    = VEHICLE_DEFS[apcType];
            if (!vDef) return;

            const sOff   = b.r + vDef.r + 10;
            const spawnX = clamp(b.x + (player.team === 0 ? -sOff : sOff), vDef.r, MAP_W - vDef.r);
            const spawnY = clamp(b.y, vDef.r, MAP_H - vDef.r);
            const vid    = shortId();

            const veh = {
                id: vid, type: apcType, team: player.team,
                x: spawnX, y: spawnY,
                a: player.team === 0 ? 0 : Math.PI, pa: 0,
                hp: vDef.maxHp, maxHp: vDef.maxHp, r: vDef.r,
                driverId: null, passengerId: null,
                infantryIds: [],
                lastDriverShot: 0, lastPassengerShot: 0,
            };
            this.vehicles.set(vid, veh);
            player.res -= APC_FROM_BARRACKS_COST;

            this.events.push({ e: EV.VEHICLE_SPAWN, veh: wireVehicle(veh) });
            this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
        }

        // ── 5 Hz infantry AI tick ─────────────────────────────────────────────────
        // Called from the main tick loop; dt is the AI step in seconds.
        tickInfantry(dtAi, now) {
            if (this.phase !== PH.ATTACK) return;
            // Mark that AI ran this cycle — broadcastSnapshot uses this to gate
            // infantry updates to 5 Hz instead of 10 Hz.
            this._infAiDidUpdate = true;

            // Rebuild infantry hash for O(N) separation (replaces O(N²) full scan)
            this._infHash.clear();
            for (const inf of this.infantry.values())
                if (inf.state !== 'in_apc') this._infHash.insert(inf.x, inf.y, inf);

            for (const [iid, inf] of this.infantry) {
                if (inf.state === 'in_apc') continue;

                const def       = INFANTRY_DEFS[inf.type];
                const enemyCore = this.cores[1 - inf.team];

                // ── boarding_apc: walk directly toward APC, skip all other AI ─────
                if (inf.state === 'boarding_apc') {
                    const boardVeh = this.vehicles.get(inf.boardingApcId);
                    if (!boardVeh || boardVeh.team !== inf.team) {
                        // APC gone or captured — resume default
                        inf.state = 'default'; inf.boardingApcId = null;
                    } else {
                        // Cancel if APC is now full (another unit beat us to it)
                        const cap = (VEHICLE_DEFS[boardVeh.type] || {}).infantryCapacity || 4;
                        if ((boardVeh.infantryIds || []).length >= cap) {
                            inf.state = 'default'; inf.boardingApcId = null;
                        } else {
                            const dx = boardVeh.x - inf.x, dy = boardVeh.y - inf.y;
                            const dd = Math.hypot(dx, dy);
                            if (dd > 0) {
                                inf.a = Math.atan2(dy, dx);
                                inf.x = clamp(inf.x + (dx / dd) * def.spd * dtAi, inf.r, MAP_W - inf.r);
                                inf.y = clamp(inf.y + (dy / dd) * def.spd * dtAi, inf.r, MAP_H - inf.r);
                            }
                        }
                    }
                    continue; // skip normal combat AI
                }

                // ── Determine movement goal ──────────────────────────────────────
                let goalX = enemyCore.x, goalY = enemyCore.y;

                if (inf.state === 'follow') {
                    const leader = this.players.get(inf.followPlayerId);
                    if (!leader || leader.rt > 0) {
                        inf.state = 'default'; inf.followPlayerId = null;
                    } else {
                        const d = dist(inf.x, inf.y, leader.x, leader.y);
                        if (d > FOLLOW_RANGE) {
                            inf.state = 'default'; inf.followPlayerId = null;
                        } else {
                            goalX = leader.x; goalY = leader.y;
                        }
                    }
                }

                // ── Single target lookup — used for both attack and move suppression ──
                const _tgt = this.findClosestEnemyForInfantry(inf.x, inf.y, inf.team, def.range);

                // ── Attack if enemy in range and fire cooldown expired ────────────
                if (_tgt && now - inf.lastShot > def.fireRate) {
                    inf.lastShot = now;
                    const angle  = Math.atan2(_tgt.y - inf.y, _tgt.x - inf.x);
                    inf.a        = angle;
                    const _b     = INFANTRY_BARREL[inf.type] || { bx: 15, by: 0 };
                    const _bx    = inf.x + Math.cos(angle) * _b.bx - Math.sin(angle) * _b.by;
                    const _by    = inf.y + Math.sin(angle) * _b.bx + Math.cos(angle) * _b.by;
                    this.spawnProjectile(_bx, _by, angle, inf.team, iid, {
                        spd : def.projSpd, dmg: def.dmg,
                        r   : def.projR,   life: 1.6,
                        pt  : 'inf_' + inf.type,
                    });
                }

                // ── EPA Intercept Unit: passive projectile interception ───────────────
                if (inf.type === 'epa_intercept') {
                    const iDef = INFANTRY_DEFS['epa_intercept'];
                    if (!inf.lastIntercept || now - inf.lastIntercept > iDef.interceptCooldown) {
                        for (const [pid, p] of this.projs) {
                            if (p.team === inf.team) continue;
                            if (!p.splash || p.splash <= 0) continue;
                            if (dist(inf.x, inf.y, p.x, p.y) >= iDef.interceptRadius) continue;
                            if (p.interceptSeen.has(iid)) continue;
                            p.interceptSeen.add(iid);
                            if (Math.random() < iDef.interceptChance) {
                                inf.lastIntercept = now;
                                this.projs.delete(pid);
                                this.events.push({ e: EV.PROJ_DESTROY, i: pid,
                                    sx: Math.round(inf.x), sy: Math.round(inf.y),
                                    px: Math.round(p.x),   py: Math.round(p.y) });
                                break;
                            }
                        }
                    }
                }

                // ── Stop to shoot — face target, skip movement while engaged ─────────
                if (_tgt) {
                    inf.a = Math.atan2(_tgt.y - inf.y, _tgt.x - inf.x);
                    continue; // no movement when a target is in range
                }
                const dx = goalX - inf.x, dy = goalY - inf.y;
                const dd = Math.hypot(dx, dy);

                let mvx = 0, mvy = 0;

                // Move toward goal
                if (dd > 12) {
                    mvx = (dx / dd) * def.spd * dtAi;
                    mvy = (dy / dd) * def.spd * dtAi;
                }

                // Avoid nearby infantry (same team) — mild separation
                // Uses spatial hash: O(k) per unit instead of O(N) scan → O(N) total
                const AVOID_R = 20;
                for (const other of this._infHash.query(inf.x, inf.y, AVOID_R)) {
                    if (other === inf || other.state === 'in_apc') continue;
                    const od = dist(inf.x, inf.y, other.x, other.y);
                    if (od < AVOID_R && od > 0) {
                        const push = (AVOID_R - od) * 0.35;
                        mvx -= ((other.x - inf.x) / od) * push;
                        mvy -= ((other.y - inf.y) / od) * push;
                    }
                }

                // Avoid walls — soft repulsion (hash query: O(k) per unit)
                for (const b of this._buildHash.query(inf.x, inf.y, 40)) {
                    if (b.type !== 'w') continue;
                    const bd = dist(inf.x, inf.y, b.x, b.y);
                    if (bd < 28 && bd > 0) {
                        mvx -= ((b.x - inf.x) / bd) * 4;
                        mvy -= ((b.y - inf.y) / bd) * 4;
                    }
                }

                // Slight randomness to prevent perfect locking
                mvx += (Math.random() - 0.5) * 3.5;
                mvy += (Math.random() - 0.5) * 3.5;

                // Apply movement with boundary + water + wall collision
                let nx = clamp(inf.x + mvx, inf.r, MAP_W - inf.r);
                let ny = clamp(inf.y + mvy, inf.r, MAP_H - inf.r);

                if (this.isOnWater(nx, inf.y, inf.r)) nx = inf.x;
                if (this.isOnWater(inf.x, ny, inf.r)) ny = inf.y;

                for (const b of this._buildHash.query(inf.x, inf.y, 40)) {
                    if (b.type === 'w') {
                        const rx = b.x - WALL_HALF, ry = b.y - WALL_HALF;
                        if (circleRect(nx, inf.y, inf.r, rx, ry, WALL_W, WALL_W)) nx = inf.x;
                        if (circleRect(inf.x, ny, inf.r, rx, ry, WALL_W, WALL_W)) ny = inf.y;
                    }
                }

                if (Math.hypot(mvx, mvy) > 0.5) inf.a = Math.atan2(mvy, mvx);
                inf.x = nx;
                inf.y = ny;
            }
        }

        handleBuild(player, req) {
            if (this.phase !== PH.BUILD && this.phase !== PH.ATTACK) return;

            const faction = FACTIONS[this.teamFactions[player.team]] || FACTIONS['roe'];

            // ── Barracks — auto-produces infantry ─────────────────────────────────
            if (req.bt === 'bk') {
                if (player.res < BARRACKS_COST) return;

                const now    = Date.now();
                const mid    = MAP_W / 2;
                const onRed  = req.x < mid - 50;
                const onBlue = req.x > mid + 50;
                if ((player.team === 0 && !onRed) || (player.team === 1 && !onBlue)) return;
                if (this.isOnWater(req.x, req.y, 32)) return;

                // Max 2 barracks per team
                let bkCount = 0;
                for (const eb of this.buildings.values()) {
                    if (eb.type === 'bk' && eb.team === player.team) bkCount++;
                }
                if (bkCount >= 2) return;

                const id = shortId();
                const b  = {
                    id, type: 'bk', subtype: 'bk', team: player.team,
                    x: req.x, y: req.y,
                    hp: 300, maxHp: 300, r: 26,
                    infantryMode  : this.teamFactions[player.team] === 'roe' ? 'roe_rifle'
                                  : this.teamFactions[player.team] === 'bgm' ? 'bgm_bur3'
                                  : this.teamFactions[player.team] === 'epa' ? 'epa_guardian'
                                  : 'grunt',
                    lastProduction: now - INF_PROD_INTERVAL, // ready immediately
                    apcCooldown   : 0,
                };
                this.buildings.set(id, b);
                player.res -= BARRACKS_COST;
                this.events.push({ e: EV.BUILD_ADD, b: wireBuild(b) });
                this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
                return;
            }

            // ── Vehicle depot — place the building itself ──────────────────────────
            if (req.bt === 'vd') {
                // Only factions with vehicles can build a depot
                const factionVehicles = FACTION_VEHICLES[this.teamFactions[player.team]] || [];
                if (factionVehicles.length === 0) return;
                if (player.res < VEHICLE_DEPOT_COST) return;

                const mid    = MAP_W / 2;
                const onRed  = req.x < mid - 50;
                const onBlue = req.x > mid + 50;
                if ((player.team === 0 && !onRed) || (player.team === 1 && !onBlue)) return;
                if (this.isOnWater(req.x, req.y, 40)) return;

                const core = this.cores[player.team];
                if (dist(req.x, req.y, core.x, core.y) < 150) return;

                // One depot per team — don't allow duplicates
                for (const eb of this.buildings.values()) {
                    if (eb.type === 'vd' && eb.team === player.team) return;
                }

                const id = shortId();
                const b  = {
                    id, type: 'vd', subtype: 'vd', team: player.team,
                    x: req.x, y: req.y,
                    hp: 500, maxHp: 500,
                    r: 36,
                };
                this.buildings.set(id, b);
                player.res -= VEHICLE_DEPOT_COST;
                this.events.push({ e: EV.BUILD_ADD, b: wireBuild(b) });
                this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
                return;
            }

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

        handleVehicleSpawn(player, vType) {
            if (!vType) return;
            if (player.rt > 0) return;

            // Validate faction can use this vehicle
            const factionVehicles = FACTION_VEHICLES[this.teamFactions[player.team]] || [];
            if (!factionVehicles.includes(vType)) return;

            const vDef = VEHICLE_DEFS[vType];
            if (!vDef) return;

            // Find a friendly depot the player is near
            let depot = null;
            for (const b of this.buildings.values()) {
                if (b.type === 'vd' && b.team === player.team) {
                    if (dist(player.x, player.y, b.x, b.y) <= 120) { depot = b; break; }
                }
            }
            if (!depot) return;

            const spawnCost = vDef.spawnCost || 60;
            if (player.res < spawnCost) return;

            // Spawn vehicle just outside the depot
            const spawnOff = depot.r + vDef.r + 12;
            const spawnX   = clamp(depot.x + (player.team === 0 ? -spawnOff : spawnOff), vDef.r, MAP_W - vDef.r);
            const spawnY   = clamp(depot.y, vDef.r, MAP_H - vDef.r);

            const vid = shortId();
            const veh = {
                id: vid, type: vType, team: player.team,
                x: spawnX, y: spawnY, a: 0, pa: 0,
                hp: vDef.maxHp, maxHp: vDef.maxHp, r: vDef.r,
                driverId: null, passengerId: null,
                lastDriverShot: 0, lastPassengerShot: 0,
            };
            // Initialise drone orbit state for mechs
            if (vDef.drone) {
                veh.droneX         = spawnX + (vDef.droneOrbitR || 52);
                veh.droneY         = spawnY;
                veh.droneA         = 0;
                veh.droneOrbitAngle = 0;
                veh.droneLastShot  = 0;
            }
            this.vehicles.set(vid, veh);
            player.res -= spawnCost;
            this.events.push({ e: EV.VEHICLE_SPAWN, veh: wireVehicle(veh) });
            this.events.push({ e: EV.RES_CHANGE, i: player.id, r: player.res });
        }

        handleVehicleEnter(player, vid) {
            if (!vid || player.vehicleId || player.rt > 0) return;
            const veh = this.vehicles.get(vid);
            if (!veh || veh.team !== player.team) return;
            if (dist(player.x, player.y, veh.x, veh.y) > 140) return;
            const vDef = VEHICLE_DEFS[veh.type] || {};

            if (!veh.driverId) {
                veh.driverId       = player.id;
                player.vehicleId   = vid;
                player.vehicleRole = 'driver';

                // APC: two-phase infantry boarding — units walk up then disappear inside
                if (vDef.isAPC) {
                    if (!veh.infantryIds) veh.infantryIds = [];
                    for (const [iid, inf] of this.infantry) {
                        if (inf.team !== veh.team || inf.state === 'in_apc') continue;

                        const d = dist(veh.x, veh.y, inf.x, inf.y);

                        if (inf.state === 'boarding_apc' && inf.boardingApcId === vid) {
                            // Phase 2: infantry has walked close enough — board it
                            if (d <= 30 && veh.infantryIds.length < (vDef.infantryCapacity || 4)) {
                                inf.state         = 'in_apc';
                                inf.boardingApcId = null;
                                veh.infantryIds.push(iid);
                            }
                        } else if (inf.state !== 'boarding_apc' &&
                                   d <= APC_BOARD_RANGE &&
                                   veh.infantryIds.length < (vDef.infantryCapacity || 4)) {
                            // Phase 1: infantry enters boarding radius — start walking toward APC
                            inf.state         = 'boarding_apc';
                            inf.boardingApcId = vid;
                            inf.followPlayerId = null;
                        }
                    }
                }
            } else if (!veh.passengerId && !vDef.singlePilot && !vDef.isAPC) {
                veh.passengerId    = player.id;
                player.vehicleId   = vid;
                player.vehicleRole = 'passenger';
            }
        }

        handleVehicleExit(player) {
            if (!player.vehicleId) return;
            const veh = this.vehicles.get(player.vehicleId);
            if (veh) {
                if (veh.driverId    === player.id) veh.driverId    = null;
                if (veh.passengerId === player.id) veh.passengerId = null;
                // Eject slightly to the side
                player.x = clamp(veh.x + (player.team === 0 ? -70 : 70), player.r, MAP_W - player.r);
                player.y = clamp(veh.y + 30, player.r, MAP_H - player.r);

                // APC: unload infantry — they resume default AI at APC position
                if (veh.infantryIds && veh.infantryIds.length > 0) {
                    const unloadIds = [...veh.infantryIds];
                    veh.infantryIds = [];
                    let spread = 0;
                    for (const iid of unloadIds) {
                        const inf = this.infantry.get(iid);
                        if (!inf) continue;
                        const angle = (spread / Math.max(1, unloadIds.length)) * Math.PI * 2;
                        inf.x      = clamp(veh.x + Math.cos(angle) * 28, inf.r, MAP_W - inf.r);
                        inf.y      = clamp(veh.y + Math.sin(angle) * 28, inf.r, MAP_H - inf.r);
                        inf.state  = 'default';
                        inf.followPlayerId = null;
                        inf.boardingApcId  = null;
                        spread++;
                    }
                }
            }
            player.vehicleId   = null;
            player.vehicleRole = null;
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
            for (const target of this._playerHash.query(cx, cy, radius + 20)) {
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
            for (const b of this._buildHash.query(cx, cy, radius + 30)) {
                if (b.team === attackingTeam) continue;
                const hit = b.type === 'w'
                    ? circleRect(cx, cy, radius, b.x - WALL_HALF, b.y - WALL_HALF, WALL_W, WALL_W)
                    : dist(cx, cy, b.x, b.y) < radius + (b.r || 18);
                if (!hit) continue;
                const resist = (b.type === 'w' && b.exploResist !== undefined) ? b.exploResist : 1.0;
                b.hp -= Math.round(dmg * resist);
                if (b.hp <= 0) {
                    toDelete.push(b.id);
                    this.events.push({ e: EV.BUILD_DESTROY, i: b.id });
                } else {
                    this.events.push({ e: EV.BUILD_HIT, i: b.id, hp: b.hp });
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

            // ── Build common snapshot header (no infantry yet) ────────────────
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

            // Always include full vehicle state (few vehicles, always relevant)
            if (this.vehicles.size > 0) {
                snap.vh = Array.from(this.vehicles.values()).map(wireVehicle);
            }

            // ── OPTIMIZATION 1 + 2 + 3 + 4: Infantry delta ───────────────────
            // • Only computed on AI-update cycles (5 Hz, not 10 Hz)
            // • Skips infantry inside APCs (position equals APC; APC sends 'ic' count)
            // • 4px / ~5° thresholds suppress noise and stopped-unit chatter
            // • HP removed from movement packets — use dedicated INF_HIT event instead
            const infChanged = [];
            if (this._infAiDidUpdate) {
                this._infAiDidUpdate = false;
                for (const inf of this.infantry.values()) {
                    if (inf.state === 'in_apc') continue;          // OPT 4: skip APC cargo
                    const rx = Math.round(inf.x);
                    const ry = Math.round(inf.y);
                    const ab = encodeAngle(inf.a);
                    const moved = Math.abs(rx - inf._px) >= INF_POS_THRESHOLD ||
                                  Math.abs(ry - inf._py) >= INF_POS_THRESHOLD;
                    const turned = Math.abs(ab - inf._ab) >= INF_ANGLE_BYTE_THRESHOLD;
                    if (moved || turned) {
                        inf._px = rx; inf._py = ry; inf._ab = ab;
                        // OPT 3: position-only packet — no HP field
                        infChanged.push({ i: inf.id, x: rx, y: ry, a: ab });
                    }
                }
            }

            // ── Determine if there is anything at all to send ─────────────────
            const hasCommon = snap.ph !== undefined || snap.tm !== undefined ||
                              snap.c  !== undefined || changed.length > 0    ||
                              evs.length > 0        || snap.vh !== undefined;

            if (!hasCommon && infChanged.length === 0) return;

            // ── OPTIMIZATION 5: Interest-management — per-player infantry send ─
            // Infantry updates are sent individually per client, filtered to units
            // within INF_INTEREST_RADIUS of that player's position.
            // Common (non-infantry) data is reused from a single serialisation.
            const bw = this._bwStats;
            bw.snapshots++;
            if (snap.vh) bw.vehUpdates += snap.vh.length;

            if (infChanged.length === 0) {
                // No infantry this cycle — single broadcast is cheapest
                const str = JSON.stringify(snap);
                this.broadcastRaw(str);
                bw.bytesSent += str.length * this.players.size;
            } else {
                bw.infUpdates += infChanged.length;
                for (const pl of this.players.values()) {
                    if (pl.ws.readyState !== WebSocket.OPEN) continue;

                    // Filter infantry to those within interest radius of this player
                    const visInf = infChanged.filter(u => {
                        const inf = this.infantry.get(u.i);
                        return inf && dist(pl.x, pl.y, inf.x, inf.y) <= INF_INTEREST_RADIUS;
                    });

                    if (!hasCommon && visInf.length === 0) continue; // nothing for this client

                    // Build per-player snap (share base object when no inf for this player)
                    const pSnap = visInf.length
                        ? Object.assign({}, snap, { inf: visInf })
                        : snap;
                    const str = JSON.stringify(pSnap);
                    pl.ws.send(str);
                    bw.bytesSent += str.length;
                }
            }
        }

        // ── OPTIMIZATION 6: Periodic bandwidth profiling ─────────────────────────
        logBandwidthStats() {
            const bw      = this._bwStats;
            const elapsed = (Date.now() - bw.lastLog) / 1000;
            if (elapsed < 1) return;
            const kbps = (bw.bytesSent / elapsed / 1024).toFixed(1);
            console.log(
                `[BW room=${this.id} players=${this.players.size}] ` +
                `${kbps} KB/s | ` +
                `inf=${(bw.infUpdates / elapsed).toFixed(0)}/s | ` +
                `veh=${(bw.vehUpdates / elapsed).toFixed(0)}/s | ` +
                `snaps=${(bw.snapshots / elapsed).toFixed(0)}/s`
            );
            bw.bytesSent  = 0;
            bw.infUpdates = 0;
            bw.vehUpdates = 0;
            bw.snapshots  = 0;
            bw.lastLog    = Date.now();
        }

        broadcastRaw(payload) {
            for (const p of this.players.values()) {
                if (p.ws.readyState === WebSocket.OPEN) p.ws.send(payload);
            }
        }
        // Broadcast only to players on a specific team
        broadcastTeam(payload, team) {
            for (const p of this.players.values()) {
                if (p.team === team && p.ws.readyState === WebSocket.OPEN) p.ws.send(payload);
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

                if (data.t === 'gchat_name') {
                    // Set display name for global chat (can be called before join)
                    const nm = (typeof data.nm === 'string' ? data.nm : 'Pilot').slice(0, 18);
                    info.gchatName = nm;
                    // Send current online count back immediately
                    ws.send(JSON.stringify({ t: 'gchat_online', n: clients.size }));

                } else if (data.t === 'gchat') {
                    // Global chat — broadcast to ALL connected clients
                    if (typeof data.msg === 'string' && data.msg.trim().length > 0) {
                        const nm   = (info.gchatName || info.name || 'Pilot').slice(0, 18);
                        const safe = data.msg.slice(0, 120).trim();
                        broadcastAll({ t: 'gchat', nm, msg: safe });
                    }

                } else if (data.t === 'join') {
                    const mode   = ['ranked1v1','ranked2v2','casual','private'].includes(data.mode) ? data.mode : 'casual';
                    const rank   = Math.max(0, parseInt(data.rank) || 0);
                    // Private mode: always use the supplied room code (create if absent)
                    const roomId = data.r ? data.r : (mode === 'private' ? crypto.randomBytes(4).toString('hex') : findRoom(mode, rank));
                    if (!rooms.has(roomId)) rooms.set(roomId, new Room(roomId, data.r ? mode : mode));
                    const r = rooms.get(roomId);
                    if (r.players.size >= r.maxPlayers) {
                        ws.send(JSON.stringify({ t: 'err', msg: 'Room full' }));
                        return;
                    }
                    info.roomId = roomId;
                    info.name   = (data.n || 'Pilot').slice(0, 18);
                    r.addPlayer(ws, id, data.n || 'Pilot', rank);

                } else if (data.t === 'i' && room) {
                    const player = room.players.get(id);
                    if (player && player.rt <= 0) {
                        player.inp.dx = clamp(+(data.dx) || 0, -1, 1);
                        player.inp.dy = clamp(+(data.dy) || 0, -1, 1);
                        player.a      = +(data.a)  || 0;
                        player.inp.sh = !!data.sh;
                    }

                } else if (data.t === 'shout' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleShout(player);

                } else if (data.t === 'bk_mode' && room) {
                    // Player changes barracks production mode
                    const player = room.players.get(id);
                    if (!player) return;
                    const b = room.buildings.get(data.id);
                    if (!b || b.type !== 'bk' || b.team !== player.team) return;
                    if (['grunt', 'heavy', 'roe_rifle', 'roe_breach', 'bgm_bur3', 'bgm_bur8', 'epa_guardian', 'epa_intercept'].includes(data.mode)) {
                        b.infantryMode = data.mode;
                        room.events.push({ e: EV.BK_MODE, i: b.id, mode: data.mode });
                    }

                } else if (data.t === 'bk_apc' && room) {
                    // Player calls an APC from a barracks
                    const player = room.players.get(id);
                    if (player) room.handleBarracksAPC(player, data.id);

                } else if (data.t === 'b' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleBuild(player, data);

                } else if (data.t === 'upg' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleUpgrade(player, data);

                } else if (data.t === 'wupg' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleWallUpgrade(player, data);

                } else if (data.t === 'vspawn' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleVehicleSpawn(player, data.vt);

                } else if (data.t === 'venter' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleVehicleEnter(player, data.vid);

                } else if (data.t === 'vexit' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleVehicleExit(player);

                } else if (data.t === 'vote' && room && room.inVotePhase) {
                    const player = room.players.get(id);
                    if (player && ['roe', 'bgm', 'epa'].includes(data.f)) {
                        room.factionVotes[player.team].set(id, data.f);
                        room.broadcastFactionVotes();
                        room.checkAllVotedEarlyEnd();
                    }

                } else if (data.t === 'mvote' && room && room.inVotePhase) {
                    const player = room.players.get(id);
                    const mapId  = +data.m;
                    const validIds = room._voteMapOptionIds || MAP_DEFS.map(m => m.id);
                    if (player && validIds.includes(mapId)) {
                        room.mapVotes.set(id, mapId);
                        room.broadcastMapVotes();
                        room.checkAllVotedEarlyEnd();
                    }

                // ── Operator selection (during OPERATOR_SELECT phase) ─────────
                // Client sends: { t: 'op_select', opId: 'roe_breacher' }
                } else if (data.t === 'op_select' && room && room.inOperatorSelect) {
                    const player = room.players.get(id);
                    if (player) room.handleOperatorSelect(player, data.opId);

                // ── Weapon selection (during OPERATOR_SELECT phase) ───────────
                // Client sends: { t: 'wp_select', weaponId: 'ar_standard' }
                } else if (data.t === 'wp_select' && room && room.inOperatorSelect) {
                    const player = room.players.get(id);
                    if (player) room.handleWeaponSelect(player, data.weaponId);

                // ── Lock-in (during OPERATOR_SELECT phase) ────────────────────
                // Client sends: { t: 'op_lock' }
                } else if (data.t === 'op_lock' && room && room.inOperatorSelect) {
                    const player = room.players.get(id);
                    if (player) room.handleLockIn(player);

                // ── Ability activation (during ATTACK phase) ─────────────────
                // Client sends: { t: 'ability' }
                // Server validates operator, cooldown, phase before executing
                } else if (data.t === 'ability' && room) {
                    const player = room.players.get(id);
                    if (player) room.handleAbility(player);

                // ── Scout drone fire (Daemon operator) ───────────────────────
                } else if (data.t === 'drone_fire' && room) {
                    const player = room.players.get(id);
                    if (!player || room.phase !== PH.ATTACK) return;
                    const drone = room.scoutDrones?.get(data.id);
                    if (drone && drone.ownerId === id && drone.team === player.team) {
                        const dx = clamp(+(data.x) || drone.x, 0, MAP_W);
                        const dy = clamp(+(data.y) || drone.y, 0, MAP_H);
                        const da = +(data.a);   // aim toward mouse — client sends angle
                        drone.x = dx; drone.y = dy; drone.a = da;
                        room.spawnProjectile(dx, dy, da, player.team, id, {
                            spd: 680, dmg: 8, r: 3, life: 0.75, pt: 'bgm_scout_mg',
                        });
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
                        const safe  = data.msg.slice(0, 120).trim();
                        const scope = data.scope === 'team' ? 'team' : 'lobby';
                        const payload = JSON.stringify({
                            t: 'chat', id, nm: player.name, team: player.team, msg: safe, scope,
                        });
                        // Team-scope: send only to same-team players
                        // Lobby-scope: send to everyone in the room
                        if (scope === 'team') {
                            room.broadcastTeam(payload, player.team);
                        } else {
                            room.broadcastRaw(payload);
                        }
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

    function findRoom(mode, rank) {
        const RANK_WINDOW = 8;  // max rank-point difference to be considered "similar"
        let bestId   = null;
        let bestDiff = Infinity;

        for (const [id, r] of rooms) {
            if (r.phase !== PH.LOBBY || r.inVotePhase || r.inOperatorSelect) continue;
            if (r.mode !== mode) continue;
            if (r.players.size >= r.maxPlayers) continue;

            if (mode === 'ranked1v1' || mode === 'ranked2v2') {
                // Prefer rooms with closest average rank; hard-skip if too far away
                const avgRank = r.rankCount > 0 ? r.rankSum / r.rankCount : rank;
                const diff    = Math.abs(avgRank - rank);
                if (diff <= RANK_WINDOW && diff < bestDiff) {
                    bestDiff = diff; bestId = id;
                }
            } else {
                // Casual: first available room with this mode
                return id;
            }
        }

        // If we found a rank-compatible room, use it; otherwise fall back to any
        // available room of this mode (so ranked players always eventually match).
        if (bestId) return bestId;

        if (mode === 'ranked1v1' || mode === 'ranked2v2') {
            // Fallback: any open ranked room of the right size (ignoring rank gap)
            for (const [id, r] of rooms) {
                if (r.phase === PH.LOBBY && !r.inVotePhase && !r.inOperatorSelect && r.mode === mode && r.players.size < r.maxPlayers)
                    return id;
            }
        }

        // Create a fresh room
        return crypto.randomBytes(4).toString('hex');
    }

    server.listen(PORT, '0.0.0.0', () => console.log(`Core Wars server on :${PORT}`));
