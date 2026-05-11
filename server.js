// Node.js Backend Server
// Run: npm install ws
// Start: node server.js

const WebSocket = require('ws');
const crypto = require('crypto');
const http = require('http');

// Setup standard HTTP server and attach WebSocket server
const server = http.createServer();
const wss = new WebSocket.Server({ server });
// Use the environment's port if available, otherwise default to 3000
const PORT = process.env.PORT || 3000;

// Game Constants
const TICK_RATE = 30; // 30 FPS server-side logic
const MAP_WIDTH = 2000;
const MAP_HEIGHT = 1200;
const PHASES = { LOBBY: 0, BUILD: 1, ATTACK: 2, END: 3 };
const BUILD_TIME = 30; // seconds

// Game State Storage
const rooms = new Map(); // roomId -> Room object
const clients = new Map(); // ws -> { id, roomId }

class Room {
    constructor(id) {
        this.id = id;
        this.players = new Map(); // id -> Player data
        this.state = PHASES.LOBBY;
        this.timer = 0; // Seconds left for current phase
        this.tickInterval = null;
        
        // Game Entities
        this.projectiles = [];
        this.buildings = []; // Walls & Turrets
        this.cores = [
            { id: 'core-0', team: 0, x: 200, y: MAP_HEIGHT / 2, hp: 2500, maxHp: 2500, radius: 40 }, // Red
            { id: 'core-1', team: 1, x: MAP_WIDTH - 200, y: MAP_HEIGHT / 2, hp: 2500, maxHp: 2500, radius: 40 } // Blue
        ];
        
        this.lastTime = Date.now();
    }

    addPlayer(ws, id, name) {
        // Assign to the team with fewer players, default to Red (0)
        let redCount = 0, blueCount = 0;
        for (let p of this.players.values()) p.team === 0 ? redCount++ : blueCount++;
        const team = redCount <= blueCount ? 0 : 1;

        const startX = team === 0 ? 300 : MAP_WIDTH - 300;
        
        this.players.set(id, {
            id, ws, name, team,
            x: startX, y: MAP_HEIGHT / 2,
            radius: 15, speed: 250, // px per second
            hp: 100, maxHp: 100,
            angle: 0,
            input: { dx: 0, dy: 0, shooting: false },
            lastShot: 0,
            respawnTimer: 0,
            resources: 100 // Starting build points
        });

        // Start game automatically if 2 players are present (1 per team for testing)
        if (this.state === PHASES.LOBBY && this.players.size >= 1) {
            // For testing alone, it starts immediately. In real prod, you might wait for 2.
            this.startGame();
        }
    }

    removePlayer(id) {
        this.players.delete(id);
        if (this.players.size === 0) {
            clearInterval(this.tickInterval);
            rooms.delete(this.id);
        }
    }

    startGame() {
        this.state = PHASES.BUILD;
        this.timer = BUILD_TIME;
        this.cores.forEach(c => c.hp = c.maxHp);
        this.buildings = [];
        this.projectiles = [];

        // Distribute starting resources
        for (let p of this.players.values()) {
            p.resources = 150; 
            p.hp = p.maxHp;
            p.x = p.team === 0 ? 300 : MAP_WIDTH - 300;
            p.y = MAP_HEIGHT / 2;
        }

        // Timer decrement interval
        const timerInt = setInterval(() => {
            if (this.players.size === 0) {
                clearInterval(timerInt);
                return;
            }
            if (this.state === PHASES.BUILD) {
                this.timer--;
                if (this.timer <= 0) {
                    this.state = PHASES.ATTACK;
                    this.timer = 0; // Infinite
                }
            }
        }, 1000);

        this.tickInterval = setInterval(() => this.tick(), 1000 / TICK_RATE);
    }

    tick() {
        const now = Date.now();
        const dt = (now - this.lastTime) / 1000;
        this.lastTime = now;

        if (this.state === PHASES.END) return;

        // Process Player Movement & Shooting
        for (let player of this.players.values()) {
            if (player.respawnTimer > 0) {
                player.respawnTimer -= dt;
                if (player.respawnTimer <= 0) {
                    player.hp = player.maxHp;
                    player.x = player.team === 0 ? 300 : MAP_WIDTH - 300;
                    player.y = MAP_HEIGHT / 2;
                }
                continue; // Dead players don't move or shoot
            }

            // Move
            let nx = player.x + (player.input.dx * player.speed * dt);
            let ny = player.y + (player.input.dy * player.speed * dt);
            
            // Map boundaries
            const midPoint = MAP_WIDTH / 2;
            let minX = player.radius;
            let maxX = MAP_WIDTH - player.radius;
            
            // Build phase restriction (can't cross middle)
            if (this.state === PHASES.BUILD) {
                if (player.team === 0) maxX = midPoint - player.radius;
                if (player.team === 1) minX = midPoint + player.radius;
            }

            nx = Math.max(minX, Math.min(maxX, nx));
            ny = Math.max(player.radius, Math.min(MAP_HEIGHT - player.radius, ny));

            // Basic Wall Collision (slide)
            for (let b of this.buildings) {
                if (b.type === 'wall') {
                    // Simple AABB vs Circle
                    if (this.circleRectCollide(nx, player.y, player.radius, b.x - b.w/2, b.y - b.h/2, b.w, b.h)) nx = player.x; // Block X
                    if (this.circleRectCollide(player.x, ny, player.radius, b.x - b.w/2, b.y - b.h/2, b.w, b.h)) ny = player.y; // Block Y
                }
            }

            player.x = nx;
            player.y = ny;

            // Shoot
            if (this.state === PHASES.ATTACK && player.input.shooting && now - player.lastShot > 200) { // 5 shots/sec
                this.spawnProjectile(player.x, player.y, player.angle, player.team, player.id, false);
                player.lastShot = now;
            }
        }

        // Process Turrets
        if (this.state === PHASES.ATTACK) {
            for (let b of this.buildings) {
                if (b.type === 'turret') {
                    if (now - (b.lastShot || 0) > 800) { // Turret fire rate
                        let target = this.findClosestEnemy(b.x, b.y, b.team, 400); // range 400
                        if (target) {
                            let angle = Math.atan2(target.y - b.y, target.x - b.x);
                            this.spawnProjectile(b.x, b.y, angle, b.team, 'turret', true);
                            b.lastShot = now;
                            b.angle = angle;
                        }
                    }
                }
            }
        }

        // Process Projectiles
        for (let i = this.projectiles.length - 1; i >= 0; i--) {
            let p = this.projectiles[i];
            p.x += Math.cos(p.angle) * p.speed * dt;
            p.y += Math.sin(p.angle) * p.speed * dt;
            p.life -= dt;

            let destroyed = false;

            // Bounds check
            if (p.life <= 0 || p.x < 0 || p.x > MAP_WIDTH || p.y < 0 || p.y > MAP_HEIGHT) {
                destroyed = true;
            }

            // Collisions only in Attack Phase (for gameplay) or just general
            if (!destroyed && this.state === PHASES.ATTACK) {
                // Players
                for (let target of this.players.values()) {
                    if (target.respawnTimer <= 0 && target.team !== p.team && this.dist(p.x, p.y, target.x, target.y) < target.radius + p.radius) {
                        target.hp -= p.damage;
                        if (target.hp <= 0) target.respawnTimer = 5; // 5 sec respawn
                        destroyed = true;
                        break;
                    }
                }

                // Buildings
                if (!destroyed) {
                    for (let j = this.buildings.length - 1; j >= 0; j--) {
                        let b = this.buildings[j];
                        if (b.team !== p.team) {
                            let hit = false;
                            if (b.type === 'wall' && this.circleRectCollide(p.x, p.y, p.radius, b.x - b.w/2, b.y - b.h/2, b.w, b.h)) hit = true;
                            if (b.type === 'turret' && this.dist(p.x, p.y, b.x, b.y) < b.radius + p.radius) hit = true;
                            
                            if (hit) {
                                b.hp -= p.damage;
                                if (b.hp <= 0) this.buildings.splice(j, 1);
                                destroyed = true;
                                break;
                            }
                        }
                    }
                }

                // Cores
                if (!destroyed) {
                    for (let c of this.cores) {
                        if (c.team !== p.team && this.dist(p.x, p.y, c.x, c.y) < c.radius + p.radius) {
                            c.hp -= p.damage;
                            if (c.hp <= 0) {
                                this.state = PHASES.END;
                                this.winner = p.team;
                            }
                            destroyed = true;
                            break;
                        }
                    }
                }
            }

            if (destroyed) {
                this.projectiles.splice(i, 1);
            }
        }

        this.broadcastState();
    }

    spawnProjectile(x, y, angle, team, ownerId, isTurret) {
        this.projectiles.push({
            x, y, angle, team, ownerId,
            speed: isTurret ? 400 : 700,
            radius: 5,
            damage: isTurret ? 10 : 15,
            life: 2.0 // seconds
        });
    }

    findClosestEnemy(x, y, team, maxRange) {
        let closest = null;
        let minDist = maxRange;
        for (let p of this.players.values()) {
            if (p.team !== team && p.respawnTimer <= 0) {
                let d = this.dist(x, y, p.x, p.y);
                if (d < minDist) {
                    minDist = d;
                    closest = p;
                }
            }
        }
        return closest;
    }

    handleBuild(player, buildReq) {
        if (this.state !== PHASES.BUILD) return;
        
        const cost = buildReq.type === 'wall' ? 10 : 30;
        if (player.resources < cost) return;

        // Ensure building on their own side
        const isRedSide = buildReq.x < MAP_WIDTH / 2 - 50;
        const isBlueSide = buildReq.x > MAP_WIDTH / 2 + 50;
        
        if ((player.team === 0 && !isRedSide) || (player.team === 1 && !isBlueSide)) return;

        // Distancing from core
        let core = this.cores[player.team];
        if (this.dist(buildReq.x, buildReq.y, core.x, core.y) < 150) return; // Too close to core

        if (buildReq.type === 'wall') {
            this.buildings.push({
                id: crypto.randomUUID(), type: 'wall', team: player.team,
                x: buildReq.x, y: buildReq.y, w: 50, h: 50, hp: 200, maxHp: 200
            });
        } else if (buildReq.type === 'turret') {
            this.buildings.push({
                id: crypto.randomUUID(), type: 'turret', team: player.team,
                x: buildReq.x, y: buildReq.y, radius: 20, hp: 150, maxHp: 150, angle: 0
            });
        }

        player.resources -= cost;
    }

    dist(x1, y1, x2, y2) {
        return Math.hypot(x2 - x1, y2 - y1);
    }

    circleRectCollide(cx, cy, cr, rx, ry, rw, rh) {
        let testX = cx;
        let testY = cy;
        if (cx < rx) testX = rx; else if (cx > rx + rw) testX = rx + rw;
        if (cy < ry) testY = ry; else if (cy > ry + rh) testY = ry + rh;
        let distX = cx - testX;
        let distY = cy - testY;
        return (distX * distX) + (distY * distY) <= (cr * cr);
    }

    broadcastState() {
        const state = {
            type: 'gameState',
            phase: this.state,
            timer: this.timer,
            winner: this.winner,
            cores: this.cores,
            players: Array.from(this.players.values()).map(p => ({
                id: p.id, name: p.name, team: p.team, x: Math.round(p.x), y: Math.round(p.y),
                hp: p.hp, maxHp: p.maxHp, angle: p.angle, respawnTimer: p.respawnTimer, resources: p.resources
            })),
            buildings: this.buildings.map(b => ({...b, x: Math.round(b.x), y: Math.round(b.y)})),
            projectiles: this.projectiles.map(p => ({
                x: Math.round(p.x), y: Math.round(p.y), team: p.team, radius: p.radius
            }))
        };
        
        const payload = JSON.stringify(state);
        for (let p of this.players.values()) {
            if (p.ws.readyState === WebSocket.OPEN) {
                p.ws.send(payload);
            }
        }
    }
}

wss.on('connection', (ws) => {
    const id = crypto.randomUUID();
    clients.set(ws, { id, roomId: null });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const clientInfo = clients.get(ws);
            let room = clientInfo.roomId ? rooms.get(clientInfo.roomId) : null;

            if (data.type === 'join') {
                const requestedRoomId = data.roomId || findMatchmakingRoom();
                if (!rooms.has(requestedRoomId)) {
                    rooms.set(requestedRoomId, new Room(requestedRoomId));
                }
                room = rooms.get(requestedRoomId);
                
                clientInfo.roomId = requestedRoomId;
                room.addPlayer(ws, id, data.name || 'Player');
                
                ws.send(JSON.stringify({ 
                    type: 'init', 
                    id, 
                    roomId: requestedRoomId, 
                    map: { w: MAP_WIDTH, h: MAP_HEIGHT } 
                }));

            } else if (data.type === 'input' && room) {
                const player = room.players.get(id);
                if (player) {
                    player.input.dx = data.dx;
                    player.input.dy = data.dy;
                    player.angle = data.angle;
                    player.input.shooting = data.shooting;
                }
            } else if (data.type === 'build' && room) {
                const player = room.players.get(id);
                if (player) room.handleBuild(player, data);
            }
        } catch (e) {
            console.error('Message error:', e);
        }
    });

    ws.on('close', () => {
        const clientInfo = clients.get(ws);
        if (clientInfo && clientInfo.roomId) {
            const room = rooms.get(clientInfo.roomId);
            if (room) room.removePlayer(clientInfo.id);
        }
        clients.delete(ws);
    });
});

function findMatchmakingRoom() {
    // Find an open room in LOBBY phase
    for (let [id, room] of rooms.entries()) {
        if (room.state === PHASES.LOBBY && room.players.size < 10) {
            return id;
        }
    }
    // Else return new ID
    return crypto.randomBytes(4).toString('hex');
}

// Bind to 0.0.0.0 so cloud environments can detect the open port
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server started on port ${PORT}`);
});
