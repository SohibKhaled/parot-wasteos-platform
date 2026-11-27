const http = require('http');
const net = require('net');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const TCP_PORT = Number(process.env.DF703_TCP_PORT) || 5000;
const RAW_PORT = Number(process.env.PORT) || 3005;
const PORT = RAW_PORT === TCP_PORT ? 8080 : RAW_PORT;
const HOST = '0.0.0.0';
const ROOT_DIR = __dirname;
// Fixed sensor distance calibration (1000 mm = 100 cm -> 0% full, 20 cm -> 80% full, 0 cm -> 100% full).
const defaultBinDepthMm = Number(process.env.BIN_DEPTH_MM) || 1000;
const BIN_DEPTH_MM = defaultBinDepthMm;
// The DF703 reports 0x270F (9999 mm) when the ultrasonic pulse gets no echo
// (sensor tilted, lid open, nothing in range). That is "no reading", not an empty bin.
const NO_ECHO_MIN_MM = 9000;
// Battery: the DF703 reports voltage only. Its lithium (Li-SOCl2, 3.6 V) cell stays near
// 3.5-3.6 V for most of its life and wobbles by tens of mV with load and temperature, so a
// voltage->percent formula jumps around. We report a smoothed voltage and a coarse state.
const BATT_LOW_V = Number(process.env.BATT_LOW_V) || 3.3;
const BATT_CRITICAL_V = Number(process.env.BATT_CRITICAL_V) || 3.1;
const BATT_SMOOTH_WINDOW = 5;

const MIME_TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.pdf': 'application/pdf',
    '.csv': 'text/csv; charset=utf-8',
    '.ico': 'image/x-icon'
};

// In-memory runtime stores for IoT ingestion, downlink queues, and driver GPS logs
const telemetryLog = [];
const df703Packets = [];
const downlinkQueue = new Map(); // key: imei -> array of queued commands
const driverGpsState = {
    driverId: 'd1',
    driverName: 'Ahmed Mostafa',
    plannedKm: 16.5,
    actualKm: 16.5,
    compliancePct: 100,
    violationActive: false,
    violationReason: null,
    costPerKmEgp: 14,
    savedEgp: 222,
    wastedEgp: 0,
    history: []
};

let tcpServerActive = false;

// ---------------------------------------------------------------------------
// DF703 live-device settings (all optional, via environment variables)
//   DF703_TCP_PORT      raw TCP port the sensor connects to (default 5000)
//   DF703_SOCKET_IDLE_S close idle sensor sockets after N seconds (default 120)
//   DATA_DIR            folder for persisted packets (default ./data)
//   INGEST_TOKEN        if set, POST /api/df703/ingest and /downlink require
//                       "Authorization: Bearer <INGEST_TOKEN>"
// ---------------------------------------------------------------------------
const SOCKET_IDLE_MS = (Number(process.env.DF703_SOCKET_IDLE_S) || 120) * 1000;
const DATA_DIR = process.env.DATA_DIR || path.join(ROOT_DIR, 'data');
const PACKETS_FILE = path.join(DATA_DIR, 'df703_packets.jsonl');
const INGEST_TOKEN = process.env.INGEST_TOKEN || '';
const DEVICE_SETTINGS_FILE = path.join(DATA_DIR, 'df703_device_settings.json');
const ADMIN_PREFS_FILE = path.join(DATA_DIR, 'admin_preferences.json');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const AUDIT_FILE = path.join(DATA_DIR, 'audit_logs.jsonl');
const BACKUPS_DIR = path.join(DATA_DIR, 'backups');
const BACKUP_CONFIG_FILE = path.join(DATA_DIR, 'backup_config.json');
const SESSION_SECRET_FILE = path.join(DATA_DIR, '.session_secret');
const TENANTS_FILE = path.join(DATA_DIR, 'tenants.json');
const TENANT_STATES_FILE = path.join(DATA_DIR, 'tenant_states.json');

const DEFAULT_CORRIDOR_TOLERANCE_M = 120;

const DEFAULT_TENANTS = [
    {
        id: 'tmg',
        title: 'Smart Waste Solution — TMG',
        shortName: 'TMG • Madinaty',
        sub: 'Madinaty Compound',
        city: 'New Cairo',
        hardwareMode: 'live',
        color: '#D97706',
        sec: '#EC4899',
        center: [30.0955, 31.6480],
        zoom: 14,
        anchors: {
            depot: [30.1045, 31.6225, 'Madinaty Truck Garage'],
            landfill: [30.0885, 31.6690, 'East Unloading & Recycling Hub'],
            streets: [
                [30.0985, 31.6352, 'Central Spine Road'], [30.0954, 31.6448, 'Madinaty East Blvd'],
                [30.0928, 31.6535, 'B10 Market Street'], [30.1018, 31.6295, 'South Park Way'],
                [30.0905, 31.6380, 'Gate 2 Avenue'], [30.0940, 31.6602, 'Craft Area Loop'],
                [30.1060, 31.6388, 'Open Air Mall'], [30.0972, 31.6400, 'B1 Neighborhood'],
                [30.0962, 31.6512, 'B8 East Ring'], [30.1012, 31.6470, 'Talaat Moustafa Axis']
            ]
        },
        drivers: [
            { id: 'd1', name: 'Ahmed Mostafa', truck: 'Truck #101 (Actros)', homeDepot: 'Madinaty Truck Garage' },
            { id: 'd2', name: 'Khaled Mansour', truck: 'Truck #102 (Isuzu)', homeDepot: 'Madinaty Truck Garage' }
        ],
        seedBins: [
            {
                id: 1, code: 'B-LIVE-01', name: 'LIVE DF703 • Central Spine', street: 'Central Spine Road',
                category: 'Smart Curb Bin 660L (Live Hardware)', lat: 30.0985, lng: 31.6352,
                fill: 0, heightCm: 120, temp: 24, angle: 0, battery: 100, voltageV: 3.6, rssi: -65,
                imei: '864317087587096', serial: '864317087587096', model: 'DF703 4G Live Sensor', isLive: true
            }
        ]
    },
    {
        id: 'palm_hills',
        title: 'Palm Hills — Smart Waste',
        shortName: 'Palm Hills • New Cairo',
        sub: '5th Settlement Compound',
        city: 'New Cairo',
        hardwareMode: 'demo',
        color: '#10B981',
        sec: '#06B6D4',
        center: [30.0255, 31.4910],
        zoom: 14,
        anchors: {
            depot: [30.0365, 31.4720, 'Palm Hills Fleet Yard'],
            landfill: [30.0110, 31.5180, 'New Cairo Eco Transfer Hub'],
            streets: [
                [30.0295, 31.4835, 'Palm Central Promenade'], [30.0245, 31.4915, 'Clubhouse Ring Rd'],
                [30.0210, 31.5005, 'Villa Sector East'], [30.0318, 31.4960, 'Commercial Gate 3'],
                [30.0185, 31.4840, 'Lake View Parkway'], [30.0272, 31.5055, 'Golf Villas Loop']
            ]
        },
        drivers: [
            { id: 'ph-d1', name: 'Hossam El-Din', truck: 'Truck #201 (Mercedes)', homeDepot: 'Palm Hills Fleet Yard' },
            { id: 'ph-d2', name: 'Tarek Nabil', truck: 'Truck #202 (Isuzu)', homeDepot: 'Palm Hills Fleet Yard' }
        ],
        seedBins: [
            { id: 101, code: 'PH-01', name: 'PH-01 • Palm Central Promenade', street: 'Palm Central Promenade', category: 'Underground Compactor 1100L', lat: 30.0295, lng: 31.4835, fill: 92, heightCm: 10, temp: 29, angle: 2, battery: 89, voltageV: 3.55, rssi: -68, imei: '860761079948201', serial: '860761079948201', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 102, code: 'PH-02', name: 'PH-02 • Clubhouse Ring Rd', street: 'Clubhouse Ring Rd', category: 'Smart Curb Bin 660L', lat: 30.0245, lng: 31.4915, fill: 84, heightCm: 19, temp: 31, angle: 3, battery: 94, voltageV: 3.58, rssi: -71, imei: '860761079948202', serial: '860761079948202', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 103, code: 'PH-03', name: 'PH-03 • Villa Sector East', street: 'Villa Sector East', category: 'Smart Curb Bin 660L', lat: 30.0210, lng: 31.5005, fill: 64, heightCm: 43, temp: 27, angle: 1, battery: 78, voltageV: 3.49, rssi: -74, imei: '860761079948203', serial: '860761079948203', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 104, code: 'PH-04', name: 'PH-04 • Commercial Gate 3', street: 'Commercial Gate 3', category: 'Commercial Bin 1100L', lat: 30.0318, lng: 31.4960, fill: 88, heightCm: 14, temp: 61, angle: 4, battery: 82, voltageV: 3.52, rssi: -69, imei: '860761079948204', serial: '860761079948204', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 105, code: 'PH-05', name: 'PH-05 • Lake View Parkway', street: 'Lake View Parkway', category: 'Smart Curb Bin 240L', lat: 30.0185, lng: 31.4840, fill: 34, heightCm: 79, temp: 25, angle: 0, battery: 96, voltageV: 3.60, rssi: -64, imei: '860761079948205', serial: '860761079948205', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 106, code: 'PH-06', name: 'PH-06 • Golf Villas Loop', street: 'Golf Villas Loop', category: 'Smart Curb Bin 660L', lat: 30.0272, lng: 31.5055, fill: 51, heightCm: 59, temp: 26, angle: 44, battery: 72, voltageV: 3.46, rssi: -79, imei: '860761079948206', serial: '860761079948206', model: 'DF703 Pilot (Demo)', isLive: false }
        ]
    },
    {
        id: 'kafd',
        title: 'KAFD — Smart District',
        shortName: 'KAFD • Riyadh',
        sub: 'Financial District Ops',
        city: 'Riyadh',
        hardwareMode: 'demo',
        color: '#3B82F6',
        sec: '#8B5CF6',
        center: [24.7640, 46.6420],
        zoom: 14,
        anchors: {
            depot: [24.7760, 46.6250, 'KAFD North Fleet Base'],
            landfill: [24.7380, 46.6690, 'Riyadh Eco Sorting Hub'],
            streets: [
                [24.7640, 46.6420, 'KAFD Ring Road'], [24.7565, 46.6485, 'Al Thumamah Plaza'],
                [24.7695, 46.6370, 'Metro Station North'], [24.7590, 46.6345, 'Financial Tower Row'],
                [24.7520, 46.6530, 'East Promenade'], [24.7715, 46.6495, 'Conference Center Way']
            ]
        },
        drivers: [
            { id: 'kf-d1', name: 'Tariq Al-Otaibi', truck: 'Truck #301 (MAN)', homeDepot: 'KAFD North Fleet Base' },
            { id: 'kf-d2', name: 'Fahad Al-Harbi', truck: 'Truck #302 (Volvo)', homeDepot: 'KAFD North Fleet Base' }
        ],
        seedBins: [
            { id: 201, code: 'KF-01', name: 'KF-01 • KAFD Ring Road', street: 'KAFD Ring Road', category: 'Smart Solar Compactor', lat: 24.7640, lng: 46.6420, fill: 89, heightCm: 13, temp: 33, angle: 1, battery: 95, voltageV: 3.59, rssi: -66, imei: '860761079948301', serial: '860761079948301', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 202, code: 'KF-02', name: 'KF-02 • Al Thumamah Plaza', street: 'Al Thumamah Plaza', category: 'Smart Solar Compactor', lat: 24.7565, lng: 46.6485, fill: 76, heightCm: 29, temp: 30, angle: 2, battery: 91, voltageV: 3.56, rssi: -70, imei: '860761079948302', serial: '860761079948302', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 203, code: 'KF-03', name: 'KF-03 • Metro Station North', street: 'Metro Station North', category: 'Smart Curb Bin 660L', lat: 24.7695, lng: 46.6370, fill: 94, heightCm: 7, temp: 34, angle: 0, battery: 88, voltageV: 3.54, rssi: -67, imei: '860761079948303', serial: '860761079948303', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 204, code: 'KF-04', name: 'KF-04 • Financial Tower Row', street: 'Financial Tower Row', category: 'Smart Curb Bin 660L', lat: 24.7590, lng: 46.6345, fill: 28, heightCm: 86, temp: 28, angle: 0, battery: 97, voltageV: 3.60, rssi: -63, imei: '860761079948304', serial: '860761079948304', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 205, code: 'KF-05', name: 'KF-05 • East Promenade', street: 'East Promenade', category: 'Commercial Bin 1100L', lat: 24.7520, lng: 46.6530, fill: 58, heightCm: 50, temp: 29, angle: 1, battery: 83, voltageV: 3.52, rssi: -73, imei: '860761079948305', serial: '860761079948305', model: 'DF703 Pilot (Demo)', isLive: false },
            { id: 206, code: 'KF-06', name: 'KF-06 • Conference Center Way', street: 'Conference Center Way', category: 'Smart Curb Bin 660L', lat: 24.7715, lng: 46.6495, fill: 82, heightCm: 22, temp: 31, angle: 41, battery: 69, voltageV: 3.44, rssi: -76, imei: '860761079948306', serial: '860761079948306', model: 'DF703 Pilot (Demo)', isLive: false }
        ]
    }
];

let tenantsStore = JSON.parse(JSON.stringify(DEFAULT_TENANTS));
const tenantStates = new Map();
const imeiToTenant = new Map([['864317087587096', 'tmg']]);

function haversineKm(a, b) {
    const R = 6371;
    const dLat = (b[0] - a[0]) * Math.PI / 180;
    const dLng = (b[1] - a[1]) * Math.PI / 180;
    const s = Math.sin(dLat / 2) ** 2 + Math.cos(a[0] * Math.PI / 180) * Math.cos(b[0] * Math.PI / 180) * Math.sin(dLng / 2) ** 2;
    return 2 * R * Math.atan2(Math.sqrt(s), Math.sqrt(1 - s));
}

function distancePointToSegmentM(lat, lng, a, b) {
    const refLatRad = ((a[0] + b[0] + lat) / 3) * Math.PI / 180;
    const mPerDegLat = 111132;
    const mPerDegLng = 111320 * Math.cos(refLatRad);
    const px = (lng - a[1]) * mPerDegLng;
    const py = (lat - a[0]) * mPerDegLat;
    const bx = (b[1] - a[1]) * mPerDegLng;
    const by = (b[0] - a[0]) * mPerDegLat;
    const lenSq = bx * bx + by * by;
    if (lenSq <= 0.0001) return Math.hypot(px, py);
    const t = Math.max(0, Math.min(1, (px * bx + py * by) / lenSq));
    return Math.hypot(px - t * bx, py - t * by);
}

function distanceToPolylineM(lat, lng, coords) {
    if (!Array.isArray(coords) || coords.length === 0) return null;
    if (coords.length === 1) return Math.round(haversineKm([lat, lng], coords[0]) * 1000);
    let minM = Infinity;
    for (let i = 0; i < coords.length - 1; i++) {
        const d = distancePointToSegmentM(lat, lng, coords[i], coords[i + 1]);
        if (d < minM) minM = d;
    }
    return Math.round(minM);
}

function buildDefaultDriverState(tenant, drv) {
    const depot = tenant.anchors?.depot || [30.1045, 31.6225, 'Depot'];
    return {
        driverId: drv.id,
        driverName: drv.name,
        truck: drv.truck,
        homeDepot: drv.homeDepot || depot[2],
        lat: depot[0],
        lng: depot[1],
        speedKmh: 0,
        headingDeg: 0,
        accuracyM: null,
        screenState: 'standby',
        mode: 'standby',
        activeShift: false,
        lastPingAt: null,
        plannedKm: 16.5,
        actualKm: 0,
        costPerKmEgp: 14,
        savedEgp: 222,
        wastedEgp: 0,
        onRoute: true,
        distanceFromRouteM: 0,
        compliancePct: 100,
        violationActive: false,
        violationReason: '',
        deviationsCount: 0,
        onRoutePings: 0,
        totalPings: 0,
        assignedRoute: null,
        actualTrail: [],
        stopsCompleted: [],
        stopsSkipped: [],
        tankLoadPct: 0,
        events: []
    };
}

function ensureTenantState(tenantId) {
    const t = tenantsStore.find(x => x.id === tenantId) || tenantsStore[0];
    if (tenantStates.has(t.id)) return tenantStates.get(t.id);
    const driverStates = {};
    (t.drivers || []).forEach(drv => {
        driverStates[drv.id] = buildDefaultDriverState(t, drv);
    });
    const state = {
        tenantId: t.id,
        preferences: {
            fireTemp: 55,
            fullThreshold: 80,
            tiltDeg: 35,
            lowBattThreshold: 35,
            defaultCostPerKm: 14,
            routeEndPolicy: 'round_trip',
            autoSchedulerEnabled: true,
            corridorToleranceM: DEFAULT_CORRIDOR_TOLERANCE_M
        },
        anchors: JSON.parse(JSON.stringify(t.anchors)),
        drivers: JSON.parse(JSON.stringify(t.drivers || [])),
        activeDriverId: t.drivers?.[0]?.id || 'd1',
        driverStates,
        bins: JSON.parse(JSON.stringify(t.seedBins || [])),
        schedules: [
            { title: `${t.sub} Morning Sweep`, zone: t.sub, rule: 'Every Day at 05:30 • Bins 75%+ full', driver: t.drivers?.[0]?.name || 'Driver #1', type: 'Auto AI', enabled: true },
            { title: `${t.sub} Evening Loop`, zone: t.sub, rule: 'Every Day at 23:00 • Bins 80%+ full', driver: t.drivers?.[1]?.name || t.drivers?.[0]?.name || 'Driver #2', type: 'Auto AI', enabled: true },
            { title: 'Instant Heat & Safety Check', zone: t.sub, rule: 'Runs immediately if Heat exceeds 55°C', driver: 'Nearest Active Truck', type: 'Auto AI', enabled: true }
        ],
        pendingDevices: t.hardwareMode === 'live'
            ? [{ serial: '860761079948102', signal: 'Strong', area: t.sub }]
            : [{ serial: `8607610799${t.id.slice(0, 2).toUpperCase()}01`, signal: 'Strong', area: t.sub }]
    };
    tenantStates.set(t.id, state);
    return state;
}

function saveTenantsAndStates() {
    try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.writeFileSync(TENANTS_FILE, JSON.stringify(tenantsStore, null, 2));
        const obj = {};
        for (const [k, v] of tenantStates.entries()) obj[k] = v;
        fs.writeFileSync(TENANT_STATES_FILE, JSON.stringify(obj, null, 2));
    } catch {}
}

function loadTenantsAndStates() {
    try {
        if (fs.existsSync(TENANTS_FILE)) {
            const loaded = JSON.parse(fs.readFileSync(TENANTS_FILE, 'utf8'));
            if (Array.isArray(loaded) && loaded.length) tenantsStore = loaded;
        }
    } catch {}
    try {
        if (fs.existsSync(TENANT_STATES_FILE)) {
            const loaded = JSON.parse(fs.readFileSync(TENANT_STATES_FILE, 'utf8'));
            if (loaded && typeof loaded === 'object') {
                Object.entries(loaded).forEach(([k, v]) => tenantStates.set(k, v));
            }
        }
    } catch {}
    tenantsStore.forEach(t => {
        ensureTenantState(t.id);
        (t.seedBins || []).forEach(b => { if (b.imei) imeiToTenant.set(b.imei, t.id); });
    });
}

function getTenantById(tenantId) {
    return tenantsStore.find(t => t.id === tenantId) || tenantsStore[0];
}

function resolvePacketTenant(imei) {
    return imeiToTenant.get(String(imei || '')) || 'tmg';
}

let deviceSettings = {};                 // imei -> { binDepthMm }
let adminPreferences = {
    fireTemp: 55,
    fullThreshold: 80,
    tiltDeg: 35,
    lowBattThreshold: 35,
    defaultCostPerKm: 14,
    routeEndPolicy: 'round_trip',
    autoSchedulerEnabled: true
};
const voltageHistory = new Map();        // imei -> last few battery voltages
const MAX_STREAM_BUFFER = 4096;

// OWASP Security State: Users, Approval Queue, Granular RBAC, Audit Logs, Rate Limiting, Local Auto-Backup
const ALL_PERMISSIONS = [
    'dashboard:read',
    'alerts:read',
    'bins:read',
    'routes:read',
    'routes:write',
    'driver:gps',
    'downlink:send',
    'devices:write',
    'prefs:write',
    'backup:read',
    'backup:manage',
    'migration:manage',
    'audit:read',
    'users:manage',
    'chat:use'
];

const ROLE_DEFAULT_PERMISSIONS = {
    Admin: [...ALL_PERMISSIONS],
    Supervisor: ['dashboard:read', 'alerts:read', 'bins:read', 'routes:read', 'routes:write', 'driver:gps', 'downlink:send', 'devices:write', 'prefs:write', 'backup:read', 'chat:use'],
    Operator: ['dashboard:read', 'alerts:read', 'bins:read', 'routes:read', 'routes:write', 'chat:use'],
    Driver: ['dashboard:read', 'routes:read', 'driver:gps', 'chat:use'],
    Auditor: ['dashboard:read', 'alerts:read', 'bins:read', 'routes:read', 'audit:read', 'backup:read', 'chat:use']
};

let usersStore = [];
const auditLogs = [];
let auditSeq = 0;
const rateLimitBuckets = new Map(); // key -> { count, resetAt }
const SESSION_TTL_MS = 12 * 60 * 60 * 1000;

let backupConfig = {
    schedule: 'daily', // 'hourly' | '6h' | 'daily' | 'weekly' | 'manual'
    retentionCount: 24,
    lastRunAt: null,
    nextRunAt: null,
    autoDownloadBrowser: false
};

function getOrCreateSessionSecret() {
    if (process.env.SESSION_SECRET && process.env.SESSION_SECRET.length >= 32) {
        return process.env.SESSION_SECRET;
    }
    try {
        if (fs.existsSync(SESSION_SECRET_FILE)) {
            const s = fs.readFileSync(SESSION_SECRET_FILE, 'utf8').trim();
            if (s.length >= 32) return s;
        }
        fs.mkdirSync(DATA_DIR, { recursive: true });
        const generated = crypto.randomBytes(48).toString('hex');
        fs.writeFileSync(SESSION_SECRET_FILE, generated, { mode: 0o600 });
        return generated;
    } catch {
        return crypto.randomBytes(48).toString('hex');
    }
}
const SESSION_SECRET = getOrCreateSessionSecret();

let packetSeq = 0;
const deviceState = new Map();   // imei -> latest decoded state
const liveSockets = new Map();   // imei -> currently connected net.Socket
const tcpStats = { connections: 0, activeConnections: 0, framesDecoded: 0, framesRejected: 0, lastFrameAt: null };

function logDf703(...args) {
    console.log(`[DF703 ${new Date().toISOString()}]`, ...args);
}

/**
 * Convert an 8-hex-character little-endian IEEE-754 float (e.g. CD03E942) to number.
 */
function parseLittleEndianFloat(hex8) {
    if (!/^[0-9A-Fa-f]{8}$/.test(hex8)) return null;
    const reversed = hex8.slice(6, 8) + hex8.slice(4, 6) + hex8.slice(2, 4) + hex8.slice(0, 2);
    const val = Buffer.from(reversed, 'hex').readFloatBE(0);
    return Number.isFinite(val) ? Number(val.toFixed(6)) : null;
}

/** Fill % from the measured distance; null when the sensor got no echo. */
function computeFillPct(heightMm, depthMm) {
    if (!(heightMm >= 0 && heightMm < NO_ECHO_MIN_MM) || !(depthMm > 0)) return null;
    return Math.max(0, Math.min(100, Math.round(((depthMm - heightMm) / depthMm) * 100)));
}

/**
 * Decode a Dingtek DF703 4G Protocol V2.8 ASCII uplink packet (0x01, 0x02, or 0x03).
 */
function parseDf703Packet(rawInput) {
    let cleaned = String(rawInput || '').trim().replace(/\s+/g, '');
    // Strip trailing FF padding that AIR780 TCP firmware appends after frame terminator 81
    cleaned = cleaned.replace(/81F+$/i, '81');
    if (cleaned.length < 20) {
        throw new Error('Packet too short for DF703 V2.8 frame');
    }
    if (!cleaned.startsWith('80') || !cleaned.endsWith('81')) {
        throw new Error('Invalid DF703 frame: must start with 80 and end with 81');
    }

    const head = cleaned.slice(0, 2);
    const forcedBit = cleaned.slice(2, 4);
    const deviceType = cleaned.slice(4, 6);
    const reportType = cleaned.slice(6, 8);
    const packetSizeBytes = parseInt(cleaned.slice(8, 10), 16);

    if (deviceType !== '01') {
        throw new Error(`Unsupported device type ${deviceType}; expected 01 (DF703)`);
    }

    if ((reportType === '01' || reportType === '02') && !/^[0-9A-Fa-f]+$/.test(cleaned)) {
        throw new Error(`DF703 ${reportType} packet contains non-hex characters`);
    }

    if (reportType === '01' || reportType === '02') {
        const heightMm = parseInt(cleaned.slice(10, 14), 16);
        const gpsFlag = cleaned.slice(14, 16);
        const hasGps = gpsFlag === '01';
        const longitude = hasGps ? parseLittleEndianFloat(cleaned.slice(16, 24)) : null;
        const latitude = hasGps ? parseLittleEndianFloat(cleaned.slice(24, 32)) : null;
        const off = hasGps ? 32 : 16;

        if (cleaned.length < off + 51) {
            throw new Error(`Incomplete DF703 ${reportType} packet length (${cleaned.length} chars)`);
        }

        const tempByte = parseInt(cleaned.slice(off, off + 2), 16);
        const tempC = tempByte <= 0x7f ? tempByte : -(tempByte - 0x7f);
        const reservedByte = cleaned.slice(off + 2, off + 4);
        const fallAngleDeg = parseInt(cleaned.slice(off + 4, off + 6), 16);
        const statusCode = cleaned.slice(off + 6, off + 10);

        const battRaw = parseInt(cleaned.slice(off + 10, off + 14), 16);
        const voltageV = battRaw > 1000 ? +(battRaw / 1000).toFixed(2) : +(battRaw / 100).toFixed(2);

        const rsrpRaw = parseLittleEndianFloat(cleaned.slice(off + 14, off + 22));
        const rsrp = rsrpRaw !== null ? Number(rsrpRaw.toFixed(1)) : null;
        // Protocol examples: -650 -> -65.0 dBm, -907 -> -90.7 dBm (value is dBm x 10)
        const rsrpDbm = rsrp === null ? null : Number((Math.abs(rsrp) > 200 ? rsrp / 10 : rsrp).toFixed(1));
        const frameCount = parseInt(cleaned.slice(off + 22, off + 26), 16);
        const timestampUnix = parseInt(cleaned.slice(off + 26, off + 34), 16);
        const deviceIdField = cleaned.slice(off + 34, cleaned.length - 2);
        const imei = (deviceIdField.length === 16 && deviceIdField.startsWith('1')) ? deviceIdField.slice(1) : deviceIdField;
        const heightValid = heightMm >= 0 && heightMm < NO_ECHO_MIN_MM;
        const fillPct = computeFillPct(heightMm, defaultBinDepthMm);

        return {
            protocol: 'Dingtek DF703 4G V2.8',
            head,
            forcedBit,
            deviceModel: 'DF703',
            reportType,
            reportTypeLabel: reportType === '01' ? 'Abnormal Alert (0x01)' : 'Heartbeat Telemetry (0x02)',
            packetSizeBytes,
            imei,
            heightMm,
            heightCm: heightValid ? +(heightMm / 10).toFixed(1) : null,
            heightValid,
            binDepthMm: BIN_DEPTH_MM,
            fillPct,
            hasGps,
            longitude,
            latitude,
            tempC,
            reservedByte,
            fallAngleDeg,
            alarms: {
                full: statusCode[0] === '1',
                fire: statusCode[1] === '1',
                fall: statusCode[2] === '1',
                lowBattery: statusCode[3] === '1',
                rawStatus: statusCode
            },
            voltageV,
            rsrp,
            rsrpDbm,
            frameCount,
            timestampUnix,
            timestampIso: new Date(timestampUnix * 1000).toISOString(),
            rawPacket: cleaned
        };
    }

    if (reportType === '03') {
        if (cleaned.length < 66) {
            throw new Error('Incomplete DF703 0x03 parameter confirmation packet');
        }
        const fwMajor = parseInt(cleaned.slice(10, 12), 16);
        const fwMinor = parseInt(cleaned.slice(12, 14), 16);
        const uploadIntervalHours = parseInt(cleaned.slice(14, 16), 16);
        const detectTimeMin = parseInt(cleaned.slice(16, 18), 16);
        const heightThresholdCm = parseInt(cleaned.slice(18, 20), 16);
        const tempThresholdC = parseInt(cleaned.slice(20, 22), 16);
        const angleThresholdDeg = parseInt(cleaned.slice(22, 24), 16);
        const imsiField = cleaned.slice(24, 40);
        const imsi = imsiField.startsWith('1') ? imsiField.slice(1) : imsiField;
        const fallMode = cleaned.slice(40, 42);
        const ultraRangeCode = cleaned.slice(42, 44);
        const workModeCode = cleaned.slice(44, 46);
        const serverModeCode = cleaned.slice(46, 48);
        const maybe16 = cleaned.slice(cleaned.length - 18, cleaned.length - 2);
        const use16 = maybe16.startsWith('1') && /^[0-9]{16}$/.test(maybe16);
        const devField = use16 ? maybe16 : cleaned.slice(cleaned.length - 17, cleaned.length - 2);
        const imei = use16 ? devField.slice(1) : devField;
        const serverAddrHex = cleaned.slice(48, use16 ? cleaned.length - 18 : cleaned.length - 17);
        let serverAddress = serverAddrHex;
        if (/^[0-9A-Fa-f]+$/.test(serverAddrHex) && serverAddrHex.length % 2 === 0) {
            const decoded = Buffer.from(serverAddrHex, 'hex').toString('utf8').replace(/[^\x20-\x7E]/g, '');
            if (decoded.includes('.') || decoded.includes(';')) serverAddress = decoded;
        }

        const rangeMap = { '00': '112K', '01': '75K', '02': '40K' };
        const workMap = { '00': 'Normal (Cycle Active)', '01': 'Detection Closed', '02': 'Demo Mode' };

        return {
            protocol: 'Dingtek DF703 4G V2.8',
            head,
            forcedBit,
            deviceModel: 'DF703',
            reportType: '03',
            reportTypeLabel: 'Downlink Config Confirm (0x03)',
            packetSizeBytes,
            imei,
            imsi,
            fwVersion: `V${fwMajor}.${String(fwMinor).padStart(2, '0')}`,
            uploadIntervalHours,
            detectTimeMin,
            heightThresholdCm,
            tempThresholdC,
            angleThresholdDeg,
            fallEnabled: fallMode === '01',
            ultraRange: rangeMap[ultraRangeCode] || ultraRangeCode,
            workMode: workMap[workModeCode] || workModeCode,
            serverMode: serverModeCode === '00' ? 'Server1 Only (00)' : serverModeCode,
            serverAddress,
            rawPacket: cleaned
        };
    }

    throw new Error(`Unsupported report data type ${reportType}`);
}

/**
 * Build an ASCII DF703 V2.8 Downlink Command frame (80029999 + CMD + CONTENT + 81).
 */
function buildDf703Downlink(cmdCode, rawValue) {
    const cmd = String(cmdCode || '').toUpperCase().padStart(2, '0');
    let content = '';

    const toHexByte = (n, min, max) => {
        const clamped = Math.max(min, Math.min(max, Number(n)));
        return Math.round(clamped).toString(16).toUpperCase().padStart(2, '0');
    };

    if (cmd === '01') content = toHexByte(rawValue || 24, 1, 168);
    else if (cmd === '02') content = toHexByte(rawValue || 30, 15, 255);
    else if (cmd === '03') content = toHexByte(rawValue || 75, 1, 255);
    else if (cmd === '04') content = toHexByte(rawValue || 30, 15, 90);
    else if (cmd === '05') content = toHexByte(rawValue || 30, 5, 99);
    else if (cmd === '08') content = toHexByte(rawValue || 10, 1, 60);
    else if (cmd === '09') content = String(rawValue || '0B').toUpperCase().padStart(2, '0').slice(0, 2);
    else if (cmd === '06') {
        const clean = String(rawValue || 'ingest.parotcorp.com;5000;').trim();
        const parts = clean.split(';').filter(Boolean);
        content = `${parts[0] || 'ingest.parotcorp.com'};${parts[1] || '5000'};`;
    } else if (cmd === '0D') {
        const clean = String(rawValue || 'internet;;;').trim();
        const parts = clean.split(';');
        content = `${parts[0] || 'internet'};${parts[1] || ''};${parts[2] || ''};`;
    } else {
        throw new Error(`Unsupported downlink command code ${cmd}`);
    }

    return `80029999${cmd}${content}81`;
}

/**
 * Generate 90-day (3-month) operational history CSV with UTF-8 BOM for Microsoft Excel.
 */
function buildQuarterlyHistoryCsv() {
    const header = [
        'Date',
        'Area / Zone',
        'Bins Serviced',
        'Planned Distance (km)',
        'Actual Distance (km)',
        'Route Compliance (%)',
        'Fuel Saved (%)',
        'Operating Cost Saved (EGP)',
        'Safety Alerts Resolved',
        'Backup Snapshot Verified'
    ].join(',');

    const zones = ['Madinaty Spine', 'New Cairo 90th', 'Riyadh KAFD'];
    const rows = [header];
    const now = Date.now();
    for (let d = 89; d >= 0; d--) {
        const dt = new Date(now - d * 86400000).toISOString().slice(0, 10);
        const zone = zones[d % zones.length];
        const bins = 18 + (d % 9);
        const plannedKm = +(14.2 + (d % 5) * 1.4).toFixed(1);
        const deviation = d % 11 === 0 ? 1.8 : 0.2;
        const actualKm = +(plannedKm + deviation).toFixed(1);
        const compliance = d % 11 === 0 ? 84 : 98;
        const fuelPct = d % 11 === 0 ? 28 : 37 + (d % 4);
        const savedEgp = Math.round(bins * 18 + (compliance > 90 ? 140 : 60));
        const alerts = d % 4 === 0 ? 2 : d % 7 === 0 ? 1 : 0;
        rows.push([dt, `"${zone}"`, bins, plannedKm, actualKm, compliance, fuelPct, savedEgp, alerts, 'SHA256-OK'].join(','));
    }
    return '\uFEFF' + rows.join('\r\n');
}

function cleanStr(val, maxLen = 160) {
    return String(val ?? '')
        .replace(/[\u0000-\u001F\u007F]/g, '')
        .replace(/[<>]/g, '')
        .trim()
        .slice(0, maxLen);
}

function getClientIp(req) {
    const fwd = req.headers['x-forwarded-for'];
    if (typeof fwd === 'string' && fwd.trim()) return fwd.split(',')[0].trim().slice(0, 64);
    return (req.socket?.remoteAddress || 'unknown').slice(0, 64);
}

function hashPassword(plain, existingSalt) {
    const salt = existingSalt || crypto.randomBytes(16).toString('hex');
    const derived = crypto.scryptSync(String(plain), salt, 64).toString('hex');
    return `${salt}:${derived}`;
}

function verifyPassword(plain, storedHash) {
    if (!storedHash || !storedHash.includes(':')) return false;
    const [salt, expected] = storedHash.split(':');
    const actual = crypto.scryptSync(String(plain), salt, 64).toString('hex');
    const a = Buffer.from(actual, 'hex');
    const b = Buffer.from(expected, 'hex');
    return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function signSessionToken(user) {
    const payloadObj = {
        uid: user.id,
        email: user.email,
        name: user.name,
        role: user.role,
        tenantId: user.tenantId || 'tmg',
        canSwitchTenants: Boolean(user.canSwitchTenants),
        exp: Date.now() + SESSION_TTL_MS
    };
    const payloadB64 = Buffer.from(JSON.stringify(payloadObj)).toString('base64url');
    const sig = crypto.createHmac('sha256', SESSION_SECRET).update(payloadB64).digest('base64url');
    return `${payloadB64}.${sig}`;
}

function verifySessionToken(token) {
    if (!token || typeof token !== 'string' || !token.includes('.')) return null;
    const [payloadB64, sig] = token.split('.');
    if (!payloadB64 || !sig) return null;
    const expectedSig = crypto.createHmac('sha256', SESSION_SECRET).update(payloadB64).digest('base64url');
    const a = Buffer.from(sig);
    const b = Buffer.from(expectedSig);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
    try {
        const parsed = JSON.parse(Buffer.from(payloadB64, 'base64url').toString('utf8'));
        if (!parsed.exp || Date.now() > parsed.exp) return null;
        return parsed;
    } catch {
        return null;
    }
}

function parseCookies(req) {
    const out = {};
    const header = req.headers.cookie;
    if (!header) return out;
    for (const part of header.split(';')) {
        const idx = part.indexOf('=');
        if (idx > 0) {
            const k = part.slice(0, idx).trim();
            const v = part.slice(idx + 1).trim();
            out[k] = decodeURIComponent(v);
        }
    }
    return out;
}

function getAuthenticatedUser(req) {
    const cookies = parseCookies(req);
    const hdrToken = req.headers['x-session-token'] ||
        ((req.headers.authorization || '').startsWith('Bearer ') ? req.headers.authorization.slice(7).trim() : '');
    const token = cookies.tmg_session || hdrToken;
    const claims = verifySessionToken(token);
    if (!claims) return null;
    const user = usersStore.find(u => u.id === claims.uid && u.status === 'approved');
    return user || null;
}

function userHasPermission(user, perm) {
    if (!user || user.status !== 'approved') return false;
    const perms = Array.isArray(user.permissions) ? user.permissions : (ROLE_DEFAULT_PERMISSIONS[user.role] || []);
    return perms.includes('*') || perms.includes(perm);
}

function sanitizeUserPublic(u) {
    const t = getTenantById(u.tenantId || 'tmg');
    return {
        id: u.id,
        name: u.name,
        email: u.email,
        role: u.role,
        tenantId: u.tenantId || 'tmg',
        tenantName: t?.shortName || t?.title || 'TMG • Madinaty',
        canSwitchTenants: Boolean(u.canSwitchTenants),
        assignedDriverId: u.assignedDriverId || null,
        status: u.status,
        permissions: u.permissions || ROLE_DEFAULT_PERMISSIONS[u.role] || [],
        requestedNote: u.requestedNote || '',
        createdAt: u.createdAt,
        approvedAt: u.approvedAt || null,
        approvedBy: u.approvedBy || null,
        lastLoginAt: u.lastLoginAt || null,
        lastLoginIp: u.lastLoginIp || null
    };
}

function logAudit(event) {
    const rec = {
        id: ++auditSeq,
        at: new Date().toISOString(),
        tenantId: cleanStr(event.tenantId || 'tmg', 40),
        actorId: event.actorId || null,
        actorEmail: cleanStr(event.actorEmail || 'anonymous', 120),
        actorRole: cleanStr(event.actorRole || 'Public', 40),
        action: cleanStr(event.action || 'EVENT', 64),
        target: cleanStr(event.target || '-', 120),
        status: cleanStr(event.status || 'INFO', 24),
        ip: cleanStr(event.ip || 'unknown', 64),
        details: cleanStr(event.details || '', 260)
    };
    auditLogs.unshift(rec);
    if (auditLogs.length > 500) auditLogs.pop();
    try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.appendFileSync(AUDIT_FILE, JSON.stringify(rec) + '\n');
    } catch {}
    return rec;
}

function loadAuditLogs() {
    try {
        if (!fs.existsSync(AUDIT_FILE)) return;
        const lines = fs.readFileSync(AUDIT_FILE, 'utf8').split('\n').filter(Boolean);
        for (const line of lines.slice(-400)) {
            try {
                const rec = JSON.parse(line);
                auditSeq = Math.max(auditSeq, Number(rec.id) || 0);
                auditLogs.unshift(rec);
            } catch {}
        }
        if (auditLogs.length > 500) auditLogs.length = 500;
    } catch {}
}

function saveUsersStore() {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(USERS_FILE, JSON.stringify(usersStore, null, 2));
}

function loadUsersStore() {
    const now = new Date().toISOString();
    const defaultUsers = [
        {
            id: 'u-admin-1',
            name: 'Platform Super Admin',
            email: 'admin@tmg-madinaty.com',
            passwordHash: hashPassword('TmgAdmin#2026'),
            role: 'Admin',
            tenantId: 'tmg',
            canSwitchTenants: true,
            status: 'approved',
            permissions: [...ALL_PERMISSIONS],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        },
        {
            id: 'u-ops-1',
            name: 'Madinaty Fleet Supervisor',
            email: 'ops@tmg-madinaty.com',
            passwordHash: hashPassword('TmgOps#2026'),
            role: 'Supervisor',
            tenantId: 'tmg',
            canSwitchTenants: false,
            status: 'approved',
            permissions: [...ROLE_DEFAULT_PERMISSIONS.Supervisor],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        },
        {
            id: 'u-drv-1',
            name: 'Ahmed Mostafa (#101)',
            email: 'driver101@tmg-madinaty.com',
            passwordHash: hashPassword('TmgDriver#2026'),
            role: 'Driver',
            tenantId: 'tmg',
            canSwitchTenants: false,
            assignedDriverId: 'd1',
            status: 'approved',
            permissions: [...ROLE_DEFAULT_PERMISSIONS.Driver],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        },
        {
            id: 'u-aud-1',
            name: 'TMG Security Auditor',
            email: 'audit@tmg-madinaty.com',
            passwordHash: hashPassword('TmgAudit#2026'),
            role: 'Auditor',
            tenantId: 'tmg',
            canSwitchTenants: false,
            status: 'approved',
            permissions: [...ROLE_DEFAULT_PERMISSIONS.Auditor],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        },
        {
            id: 'u-ph-admin',
            name: 'Palm Hills Admin',
            email: 'admin@palmhills.demo',
            passwordHash: hashPassword('PalmAdmin#2026'),
            role: 'Admin',
            tenantId: 'palm_hills',
            canSwitchTenants: false,
            status: 'approved',
            permissions: [...ALL_PERMISSIONS],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        },
        {
            id: 'u-ph-drv',
            name: 'Hossam El-Din (#201)',
            email: 'driver201@palmhills.demo',
            passwordHash: hashPassword('PalmDriver#2026'),
            role: 'Driver',
            tenantId: 'palm_hills',
            canSwitchTenants: false,
            assignedDriverId: 'ph-d1',
            status: 'approved',
            permissions: [...ROLE_DEFAULT_PERMISSIONS.Driver],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        },
        {
            id: 'u-kf-admin',
            name: 'KAFD Admin',
            email: 'admin@kafd.demo',
            passwordHash: hashPassword('KafdAdmin#2026'),
            role: 'Admin',
            tenantId: 'kafd',
            canSwitchTenants: false,
            status: 'approved',
            permissions: [...ALL_PERMISSIONS],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        }
    ];
    try {
        if (fs.existsSync(USERS_FILE)) {
            const loaded = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
            if (Array.isArray(loaded) && loaded.length) {
                usersStore = loaded.map(u => ({
                    ...u,
                    tenantId: u.tenantId || 'tmg',
                    canSwitchTenants: u.canSwitchTenants ?? (u.email === 'admin@tmg-madinaty.com'),
                    assignedDriverId: u.assignedDriverId || (u.role === 'Driver' ? 'd1' : null)
                }));
                defaultUsers.forEach(def => {
                    if (!usersStore.some(u => u.email.toLowerCase() === def.email.toLowerCase())) {
                        usersStore.push(def);
                    }
                });
                saveUsersStore();
                return;
            }
        }
    } catch {}
    usersStore = defaultUsers;
    saveUsersStore();
}

function checkRateLimit(key, limit, windowMs) {
    const now = Date.now();
    let bucket = rateLimitBuckets.get(key);
    if (!bucket || now > bucket.resetAt) {
        bucket = { count: 0, resetAt: now + windowMs };
        rateLimitBuckets.set(key, bucket);
    }
    bucket.count++;
    if (rateLimitBuckets.size > 2000) {
        for (const [k, v] of rateLimitBuckets.entries()) {
            if (now > v.resetAt) rateLimitBuckets.delete(k);
        }
    }
    return { allowed: bucket.count <= limit, remaining: Math.max(0, limit - bucket.count), resetAt: bucket.resetAt };
}

function computeNextBackupIso(schedule) {
    const msMap = {
        hourly: 60 * 60 * 1000,
        '6h': 6 * 60 * 60 * 1000,
        daily: 24 * 60 * 60 * 1000,
        weekly: 7 * 24 * 60 * 60 * 1000
    };
    const delta = msMap[schedule];
    return delta ? new Date(Date.now() + delta).toISOString() : null;
}

function loadBackupConfig() {
    try {
        if (fs.existsSync(BACKUP_CONFIG_FILE)) {
            Object.assign(backupConfig, JSON.parse(fs.readFileSync(BACKUP_CONFIG_FILE, 'utf8')) || {});
        }
        if (!backupConfig.nextRunAt && backupConfig.schedule !== 'manual') {
            backupConfig.nextRunAt = computeNextBackupIso(backupConfig.schedule);
        }
    } catch {}
}

function saveBackupConfig() {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(BACKUP_CONFIG_FILE, JSON.stringify(backupConfig, null, 2));
}

function listLocalBackups() {
    try {
        if (!fs.existsSync(BACKUPS_DIR)) return [];
        return fs.readdirSync(BACKUPS_DIR)
            .filter(f => f.endsWith('.json'))
            .sort((a, b) => b.localeCompare(a))
            .map(filename => {
                const full = path.join(BACKUPS_DIR, filename);
                const st = fs.statSync(full);
                return { filename, sizeBytes: st.size, createdAt: st.mtime.toISOString() };
            });
    } catch {
        return [];
    }
}

function createLocalBackup(trigger = 'manual', actor = 'system', tenantId = 'tmg') {
    fs.mkdirSync(BACKUPS_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const safeTid = cleanStr(tenantId || 'tmg', 32).replace(/[^a-zA-Z0-9_-]/g, '') || 'tmg';
    const filename = `${safeTid}-backup-${stamp}.json`;
    const ts = ensureTenantState(safeTid);
    const tMeta = getTenantById(safeTid);
    const tenantPackets = df703Packets.filter(p => (p.tenantId || resolvePacketTenant(p.imei)) === safeTid).slice(0, 200);
    const core = {
        format: 'TMG-LOCAL-BACKUP-V1',
        tenantId: safeTid,
        platform: tMeta?.title || 'Smart Waste Solution',
        createdAt: new Date().toISOString(),
        trigger,
        actor,
        adminPreferences: ts.preferences,
        anchors: ts.anchors,
        deviceSettings,
        df703Packets: tenantPackets,
        telemetryLog: telemetryLog.filter(x => (x.tenantId || 'tmg') === safeTid).slice(0, 200),
        driverStates: ts.driverStates,
        users: usersStore.filter(u => (u.tenantId || 'tmg') === safeTid).map(sanitizeUserPublic),
        auditLogsCount: auditLogs.filter(a => (a.tenantId || 'tmg') === safeTid).length
    };
    const sha256 = crypto.createHash('sha256').update(JSON.stringify(core)).digest('hex');
    const payload = { ...core, sha256 };
    fs.writeFileSync(path.join(BACKUPS_DIR, filename), JSON.stringify(payload, null, 2));

    // Retention cleanup
    const all = listLocalBackups();
    const keep = Math.max(3, Number(backupConfig.retentionCount) || 24);
    all.slice(keep).forEach(old => {
        try { fs.unlinkSync(path.join(BACKUPS_DIR, old.filename)); } catch {}
    });

    backupConfig.lastRunAt = core.createdAt;
    backupConfig.nextRunAt = computeNextBackupIso(backupConfig.schedule);
    saveBackupConfig();
    return { filename, sha256, tenantId: safeTid, createdAt: core.createdAt, packetsCount: core.df703Packets.length };
}

function requirePermission(req, res, perm) {
    const user = getAuthenticatedUser(req);
    const ip = getClientIp(req);
    if (!user) {
        logAudit({ action: 'AUTH_REQUIRED', target: req.url, status: 'DENIED', ip, details: `Missing/expired session for ${perm}` });
        sendJson(res, 401, { error: 'Authentication required. Please sign in.' });
        return null;
    }
    if (perm && !userHasPermission(user, perm)) {
        logAudit({
            actorId: user.id,
            actorEmail: user.email,
            actorRole: user.role,
            action: 'RBAC_DENIED',
            target: req.url,
            status: 'DENIED',
            ip,
            details: `Role ${user.role} lacks permission "${perm}"`
        });
        sendJson(res, 403, { error: `Forbidden: permission "${perm}" required for your role (${user.role}).` });
        return null;
    }
    return user;
}

function setSecurityAndCorsHeaders(res, contentType, req) {
    res.setHeader('Content-Type', contentType);
    const origin = req?.headers?.origin;
    const host = req?.headers?.host;
    if (origin && host && (origin === `http://${host}` || origin === `https://${host}`)) {
        res.setHeader('Access-Control-Allow-Origin', origin);
        res.setHeader('Vary', 'Origin');
        res.setHeader('Access-Control-Allow-Credentials', 'true');
    }
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Session-Token, X-PAROT-Signature, X-Tenant-Id');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('X-XSS-Protection', '0');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), payment=(), geolocation=(self)');
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    res.setHeader(
        'Content-Security-Policy',
        "default-src 'self'; " +
        "script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval' https://cdn.tailwindcss.com https://unpkg.com https://cdn.jsdelivr.net; " +
        "worker-src 'self' blob:; " +
        "child-src 'self' blob:; " +
        "style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://unpkg.com https://fonts.googleapis.com; " +
        "font-src 'self' data: https://cdnjs.cloudflare.com https://fonts.gstatic.com; " +
        "img-src 'self' data: blob: https://tile.openstreetmap.org https://*.tile.openstreetmap.org https://server.arcgisonline.com https://basemaps.cartocdn.com https://*.basemaps.cartocdn.com https://tiles.openfreemap.org; " +
        "connect-src 'self' blob: data: https://router.project-osrm.org https://basemaps.cartocdn.com https://*.basemaps.cartocdn.com https://tiles.openfreemap.org https://unpkg.com https://cdn.tailwindcss.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com; " +
        "frame-ancestors 'none'; object-src 'none'; base-uri 'self'"
    );
}

function sendJson(res, status, data, req) {
    setSecurityAndCorsHeaders(res, 'application/json; charset=utf-8', req);
    res.writeHead(status);
    res.end(JSON.stringify(data));
}

function readJsonBody(req, maxBytes = 131072) {
    return new Promise((resolve, reject) => {
        let raw = '';
        req.on('data', chunk => {
            raw += chunk;
            if (raw.length > maxBytes) {
                reject(new Error('Payload too large'));
                req.destroy();
            }
        });
        req.on('end', () => {
            if (!raw.trim()) return resolve({});
            try {
                resolve(JSON.parse(raw));
            } catch {
                reject(new Error('Invalid JSON payload'));
            }
        });
        req.on('error', reject);
    });
}

/**
 * Field names the dashboard (index.html -> pollLiveBackendSync) reads from `packet.parsed`.
 */
function buildDashboardView(parsed) {
    if (parsed.reportType === '03') {
        return { reportType: '03', fwVersion: parsed.fwVersion, uploadIntervalHours: parsed.uploadIntervalHours };
    }
    return {
        reportType: parsed.reportType,
        fillPct: parsed.fillPct,
        heightMm: parsed.heightMm,
        heightValid: parsed.heightValid,
        binDepthMm: parsed.binDepthMm,
        temperatureC: parsed.tempC,
        tiltAngleDeg: parsed.fallAngleDeg,
        voltageV: parsed.voltageV,
        batteryVoltageAvg: parsed.batteryVoltageAvg,
        batteryState: parsed.batteryState,
        rsrp: parsed.rsrp,
        rsrpDbm: parsed.rsrpDbm,
        latitude: parsed.latitude,
        longitude: parsed.longitude,
        alarms: parsed.alarms
    };
}

function updateDeviceState(entry) {
    const prev = deviceState.get(entry.imei) || { imei: entry.imei, firstSeenAt: entry.receivedAt, packets: 0 };
    const next = { ...prev, lastSeenAt: entry.receivedAt, lastChannel: entry.channel, packets: prev.packets + 1, lastReportType: entry.reportType };
    if (entry.reportType === '03') {
        next.config = {
            fwVersion: entry.fwVersion, imsi: entry.imsi, uploadIntervalHours: entry.uploadIntervalHours,
            detectTimeMin: entry.detectTimeMin, heightThresholdCm: entry.heightThresholdCm,
            tempThresholdC: entry.tempThresholdC, angleThresholdDeg: entry.angleThresholdDeg,
            fallEnabled: entry.fallEnabled, workMode: entry.workMode, serverAddress: entry.serverAddress,
            confirmedAt: entry.receivedAt
        };
    } else {
        next.telemetry = { ...entry.parsed, frameCount: entry.frameCount, deviceTimeIso: entry.timestampIso, at: entry.receivedAt };
    }
    deviceState.set(entry.imei, next);
}

function persistEntry(entry) {
    try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.appendFileSync(PACKETS_FILE, JSON.stringify(entry) + '\n');
    } catch (err) {
        logDf703('WARN could not persist packet:', err.message);
    }
}

function addEntryToMemory(entry) {
    if (!entry.tenantId) entry.tenantId = resolvePacketTenant(entry.imei);
    df703Packets.unshift(entry);
    if (df703Packets.length > 200) df703Packets.pop();

    telemetryLog.unshift({
        receivedAt: entry.receivedAt,
        tenantId: entry.tenantId,
        deviceId: entry.imei,
        fill: entry.fillPct ?? null,
        temp: entry.tempC ?? entry.tempThresholdC ?? null,
        angle: entry.fallAngleDeg ?? entry.angleThresholdDeg ?? null,
        battery: entry.batteryVoltageAvg ?? entry.voltageV ?? null
    });
    if (telemetryLog.length > 200) telemetryLog.pop();

    if (typeof entry.voltageV === 'number') {
        const hist = voltageHistory.get(entry.imei) || [];
        hist.push(entry.voltageV);
        if (hist.length > BATT_SMOOTH_WINDOW) hist.shift();
        voltageHistory.set(entry.imei, hist);
    }
    updateDeviceState(entry);
}

function median(values) {
    const v = [...values].sort((a, b) => a - b);
    const mid = Math.floor(v.length / 2);
    return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
}

/**
 * Add per-sensor context to a freshly decoded reading: the bin depth configured for this
 * IMEI (fill %), and a smoothed battery voltage + state over the last few readings.
 */
function enrichReading(parsed) {
    if (parsed.reportType !== '01' && parsed.reportType !== '02') return parsed;
    const depth = Number(deviceSettings[parsed.imei]?.binDepthMm) || defaultBinDepthMm;
    const recent = [...(voltageHistory.get(parsed.imei) || []), parsed.voltageV]
        .filter(v => typeof v === 'number').slice(-BATT_SMOOTH_WINDOW);
    const avg = recent.length ? Number(median(recent).toFixed(2)) : null;
    let batteryState = 'unknown';
    if (avg !== null) batteryState = avg < BATT_CRITICAL_V ? 'critical' : avg < BATT_LOW_V ? 'low' : 'good';
    if (parsed.alarms?.lowBattery && batteryState === 'good') batteryState = 'low';
    return {
        ...parsed,
        binDepthMm: depth,
        fillPct: computeFillPct(parsed.heightMm, depth),
        batteryVoltageAvg: avg,
        batteryState
    };
}

function loadDeviceSettings() {
    try {
        if (fs.existsSync(DEVICE_SETTINGS_FILE)) deviceSettings = JSON.parse(fs.readFileSync(DEVICE_SETTINGS_FILE, 'utf8')) || {};
    } catch (err) {
        logDf703('WARN could not load device settings:', err.message);
    }
}

function saveDeviceSettings() {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(DEVICE_SETTINGS_FILE, JSON.stringify(deviceSettings, null, 2));
}

function loadAdminPreferences() {
    try {
        if (fs.existsSync(ADMIN_PREFS_FILE)) {
            const loaded = JSON.parse(fs.readFileSync(ADMIN_PREFS_FILE, 'utf8'));
            if (loaded && typeof loaded === 'object') {
                delete loaded.binDepthCm;
                Object.assign(adminPreferences, loaded);
            }
        }
    } catch (err) {
        logDf703('WARN could not load admin preferences:', err.message);
    }
}

function saveAdminPreferences() {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(ADMIN_PREFS_FILE, JSON.stringify(adminPreferences, null, 2));
}

/**
 * Reload previously received packets so a restart does not wipe live sensor data.
 */
function loadPersistedPackets() {
    try {
        if (!fs.existsSync(PACKETS_FILE)) return;
        const lines = fs.readFileSync(PACKETS_FILE, 'utf8').split('\n').filter(Boolean);
        for (const line of lines.slice(-500)) {
            try {
                const entry = JSON.parse(line);
                packetSeq = Math.max(packetSeq, Number(entry.id) || 0);
                addEntryToMemory(entry);
            } catch { /* skip corrupt line */ }
        }
        logDf703(`Restored ${df703Packets.length} packets for ${deviceState.size} device(s) from ${PACKETS_FILE}`);
    } catch (err) {
        logDf703('WARN could not load persisted packets:', err.message);
    }
}

function recordParsedDf703(rawParsed, sourceChannel, meta = {}) {
    const parsed = enrichReading(rawParsed);
    const tenantId = meta.tenantId || resolvePacketTenant(parsed.imei);
    imeiToTenant.set(parsed.imei, tenantId);
    const expectedChars = (parsed.packetSizeBytes || 0) * 2;
    const entry = {
        id: ++packetSeq,
        tenantId,
        receivedAt: new Date().toISOString(),
        channel: sourceChannel,
        remoteAddress: meta.remoteAddress || null,
        packetSizeOk: parsed.reportType === '03' ? null : parsed.rawPacket.length === expectedChars,
        ...parsed,
        parsed: buildDashboardView(parsed)
    };
    addEntryToMemory(entry);
    persistEntry(entry);

    const queued = downlinkQueue.get(parsed.imei) || [];
    const pendingDownlink = queued.length ? queued.shift() : null;
    if (queued.length === 0) downlinkQueue.delete(parsed.imei);

    return { entry, pendingDownlink };
}

/**
 * TCP is a byte stream: one sensor packet can arrive split over several chunks,
 * or several packets in one chunk. This framer buffers the stream and cuts out
 * complete DF703 frames.
 *
 * ASCII mode (protocol V2.8 default): the frame is hex text, e.g. "8000010122...81".
 *   - type 01/02: fixed length, 68 chars (no GPS) or 84 chars (with GPS)
 *   - type 03:    variable length; ends with "IP;PORT;" + 16-digit device ID + "81"
 * Binary mode (fallback, if a firmware sends raw bytes): first byte is 0x80, frame length = size byte.
 */
const TYPE03_RE = /^80[0-9A-F]{2}0103[0-9A-F]{2}[0-9A-F]{38}[^;]{1,50};[^;]{1,10};[0-9A-F]{16}81/i;
const HEADER_RE = /80[0-9A-F]{2}010[123]/i;

function createDf703Framer() {
    let buffer = '';
    let binaryMode = null;

    return function push(chunk) {
        const frames = [];
        const junk = [];

        if (binaryMode === null) binaryMode = chunk.length > 0 && chunk[0] === 0x80;
        buffer += binaryMode ? chunk.toString('hex').toUpperCase() : chunk.toString('latin1').replace(/\s+/g, '');

        for (;;) {
            const m = buffer.match(HEADER_RE);
            if (!m) {
                // keep a possible partial header at the end, drop the rest
                const keep = buffer.slice(-9);
                if (buffer.length > keep.length) junk.push(buffer.slice(0, buffer.length - keep.length));
                buffer = keep;
                break;
            }
            if (m.index > 0) {
                junk.push(buffer.slice(0, m.index));
                buffer = buffer.slice(m.index);
            }
            if (buffer.length < 16) break;

            const type = buffer.slice(6, 8);
            let frameLen = 0;

            if (binaryMode) {
                frameLen = parseInt(buffer.slice(8, 10), 16) * 2;
            } else if (type === '01' || type === '02') {
                // 16-char device ID ("1"+IMEI, per the PDF) or a bare 15-digit IMEI (AIR780 firmware)
                // Decide only once the char after the shorter frame is visible, so a long frame
                // whose IMEI happens to end in "81" is not cut early.
                const full = buffer.slice(14, 16) === '01' ? 84 : 68;
                if (buffer.length < full) break; // wait for more data
                const up = buffer.slice(0, full).toUpperCase();
                frameLen = up.endsWith('81') ? full : up.slice(0, full - 1).endsWith('81') ? full - 1 : full;
            } else {
                const t3 = buffer.match(TYPE03_RE);
                if (t3) frameLen = t3[0].length;
                else if (buffer.length < 220) break; // wait for more data
            }

            if (frameLen && buffer.length < frameLen) break; // wait for more data
            const candidate = frameLen ? buffer.slice(0, frameLen) : '';
            if (!candidate || !candidate.toUpperCase().endsWith('81')) {
                junk.push(buffer.slice(0, 2));
                buffer = buffer.slice(2); // resync on next header
                continue;
            }
            frames.push(binaryMode && type === '03' ? binaryType03ToAscii(candidate) : candidate);
            buffer = buffer.slice(frameLen);
        }

        if (buffer.length > MAX_STREAM_BUFFER) {
            junk.push(buffer);
            buffer = '';
        }
        return { frames, junk: junk.filter(Boolean) };
    };
}

/** In binary mode the 0x03 server address is raw ASCII bytes; turn it back into text. */
function binaryType03ToAscii(hexFrame) {
    const fixed = hexFrame.slice(0, 48);
    const tail = hexFrame.slice(hexFrame.length - 18);
    const addrHex = hexFrame.slice(48, hexFrame.length - 18);
    return fixed + Buffer.from(addrHex, 'hex').toString('latin1') + tail;
}

function isAuthorized(req) {
    if (!INGEST_TOKEN) return true;
    return (req.headers.authorization || '') === `Bearer ${INGEST_TOKEN}`;
}

/** Try to push a queued downlink straight away if the sensor is connected right now. */
function tryDeliverNow(imei) {
    const sock = liveSockets.get(imei);
    const queued = downlinkQueue.get(imei);
    if (!sock || sock.destroyed || !queued || !queued.length) return null;
    const item = queued.shift();
    if (!queued.length) downlinkQueue.delete(imei);
    sock.write(item.frame);
    item.sentAt = new Date().toISOString();
    logDf703(`Downlink sent live to ${imei}: ${item.frame}`);
    return item;
}

function makeSessionCookie(token, maxAgeSec, req) {
    const isHttps = req.headers['x-forwarded-proto'] === 'https' || req.socket?.encrypted;
    return `tmg_session=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAgeSec}${isHttps ? '; Secure' : ''}`;
}

function getMergedTenantBins(tenantId) {
    const ts = ensureTenantState(tenantId);
    const tMeta = getTenantById(tenantId);
    const baseBins = JSON.parse(JSON.stringify(ts.bins || []));
    const pkts = df703Packets.filter(p => (p.tenantId || resolvePacketTenant(p.imei)) === tenantId);
    const seen = new Set();
    const streets = ts.anchors?.streets || [];
    pkts.forEach(p => {
        if (!p.imei || seen.has(p.imei)) return;
        seen.add(p.imei);
        let b = baseBins.find(x => x.imei === p.imei);
        if (!b) {
            const idx = baseBins.length;
            const anchor = streets[idx % Math.max(1, streets.length)] || tMeta.center;
            b = {
                id: idx + 1,
                code: `B-LIVE-0${idx + 1}`,
                name: `LIVE DF703 • ${anchor[2] || tMeta.sub}`,
                street: anchor[2] || tMeta.sub,
                category: 'Smart Curb Bin 660L (Live Hardware)',
                lat: (anchor[0] || tMeta.center[0]) + (idx * 0.0008),
                lng: (anchor[1] || tMeta.center[1]) + (idx * 0.0008),
                fill: 0, heightCm: 120, temp: 24, angle: 0, battery: 100, voltageV: 3.6, rssi: -65,
                imei: p.imei, serial: p.imei, model: 'DF703 4G Live Sensor', isLive: true
            };
            baseBins.push(b);
        }
        const latest = pkts.find(x => x.imei === p.imei && (x.reportType === '01' || x.reportType === '02')) || p;
        const noEcho = latest.heightValid === false || latest.heightMm >= NO_ECHO_MIN_MM;
        b.noEcho = (latest.reportType === '01' || latest.reportType === '02') && noEcho;
        const lastValid = pkts.find(x => x.imei === p.imei && (x.reportType === '01' || x.reportType === '02')
            && x.heightValid !== false && x.heightMm < NO_ECHO_MIN_MM && typeof x.fillPct === 'number');
        if (lastValid) {
            b.fill = lastValid.fillPct;
            if (typeof lastValid.heightCm === 'number') b.heightCm = lastValid.heightCm;
        }
        if (typeof latest.tempC === 'number') b.temp = latest.tempC;
        if (typeof latest.fallAngleDeg === 'number') b.angle = latest.fallAngleDeg;
        if (typeof (latest.batteryVoltageAvg ?? latest.voltageV) === 'number') b.voltageV = latest.batteryVoltageAvg ?? latest.voltageV;
        if (latest.batteryState) {
            b.batteryState = latest.batteryState;
            b.battery = { good: 100, low: 20, critical: 5 }[latest.batteryState] ?? b.battery;
        }
        const dbm = typeof latest.rsrpDbm === 'number' ? latest.rsrpDbm
            : typeof latest.rsrp === 'number' ? (Math.abs(latest.rsrp) > 200 ? latest.rsrp / 10 : latest.rsrp) : null;
        if (dbm !== null && dbm < 0) b.rssi = Math.round(dbm);
        if (latest.alarms) b.deviceAlarms = latest.alarms;
    });
    return { bins: baseBins, packets: pkts };
}

function buildOverviewReport(tenantId) {
    const tMeta = getTenantById(tenantId);
    const ts = ensureTenantState(tenantId);
    const prefs = ts.preferences;
    const { bins, packets } = getMergedTenantBins(tenantId);
    const fullThreshold = prefs.fullThreshold ?? 80;
    const fireTemp = prefs.fireTemp ?? 55;
    const tiltDeg = prefs.tiltDeg ?? 35;

    let low = 0, mid = 0, full = 0, noEcho = 0, fireCount = 0, tiltCount = 0, sumFill = 0;
    const streetMap = new Map();
    bins.forEach(b => {
        const f = Number(b.fill || 0);
        sumFill += f;
        if (b.noEcho) noEcho++;
        if ((Number(b.temp) >= fireTemp) || b.deviceAlarms?.fire) fireCount++;
        if ((Number(b.angle) >= tiltDeg) || b.deviceAlarms?.fall) tiltCount++;
        if (f >= fullThreshold) full++;
        else if (f >= 40) mid++;
        else low++;
        const st = b.street || tMeta.sub;
        const cur = streetMap.get(st) || { street: st, totalFill: 0, count: 0, maxFill: 0 };
        cur.totalFill += f;
        cur.count++;
        cur.maxFill = Math.max(cur.maxFill, f);
        streetMap.set(st, cur);
    });

    const areaBreakdown = [...streetMap.values()]
        .map(x => ({ street: x.street, area: x.street, avgFill: Math.round(x.totalFill / Math.max(1, x.count)), count: x.count, maxFill: x.maxFill }))
        .sort((a, b) => b.avgFill - a.avgFill);

    const recentReadings = packets
        .filter(p => (p.reportType === '01' || p.reportType === '02') && typeof p.fillPct === 'number')
        .slice(0, 12)
        .reverse();
    const hasLiveTrend = recentReadings.length >= 2;
    const avgNow = bins.length ? Math.round(sumFill / bins.length) : 0;
    const hourlyTrend = hasLiveTrend
        ? {
            isLive: true,
            labels: recentReadings.map(p => new Date(p.receivedAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })),
            values: recentReadings.map(p => p.fillPct)
        }
        : {
            isLive: tMeta.hardwareMode === 'live',
            labels: ['06:00', '09:00', '12:00', '15:00', '18:00', 'Now'],
            values: tMeta.hardwareMode === 'live'
                ? [avgNow, avgNow, avgNow, avgNow, avgNow, avgNow]
                : [Math.max(15, avgNow - 24), Math.max(20, avgNow - 14), Math.max(25, avgNow - 4), Math.max(18, avgNow - 18), Math.max(22, avgNow - 7), avgNow]
        };

    const fleetSummary = Object.values(ts.driverStates || {}).map(ds => ({
        driverId: ds.driverId,
        driverName: ds.driverName,
        truck: ds.truck,
        activeShift: Boolean(ds.activeShift),
        mode: ds.mode || 'standby',
        lat: ds.lat,
        lng: ds.lng,
        speedKmh: ds.speedKmh || 0,
        onRoute: ds.onRoute !== false,
        distanceFromRouteM: Number(ds.distanceFromRouteM || 0),
        compliancePct: Number(ds.compliancePct ?? 100),
        violationActive: Boolean(ds.violationActive),
        violationReason: ds.violationReason || '',
        plannedKm: Number(ds.plannedKm || 0),
        actualKm: Number(ds.actualKm || 0),
        wastedEgp: Number(ds.wastedEgp || 0),
        stopsCompletedCount: Array.isArray(ds.stopsCompleted) ? ds.stopsCompleted.length : 0,
        stopsTotalCount: Array.isArray(ds.assignedRoute?.stops) ? ds.assignedRoute.stops.length : 0,
        lastPingAt: ds.lastPingAt,
        hasAssignedRoute: Boolean(ds.assignedRoute?.coords?.length >= 2)
    }));

    return {
        tenantId,
        hardwareMode: tMeta.hardwareMode,
        generatedAt: new Date().toISOString(),
        summaryKpis: {
            totalBins: bins.length,
            liveSensors: bins.filter(b => b.isLive).length,
            fullBins: full,
            warmingBins: mid,
            fireCount,
            tiltCount,
            avgFillPct: avgNow,
            activeTrucks: fleetSummary.length,
            onRouteDrivers: fleetSummary.filter(f => f.onRoute).length,
            offRouteDrivers: fleetSummary.filter(f => f.activeShift && !f.onRoute).length
        },
        fillDistribution: { low, mid, full, noEcho, total: bins.length },
        hourlyTrend,
        areaBreakdown,
        fleetSummary
    };
}

function resolveTenantContext(req, res, reqUrl, user) {
    const requested = cleanStr(reqUrl.searchParams.get('tenantId') || req.headers['x-tenant-id'] || '', 40);
    if (user) {
        if (requested && requested !== user.tenantId && !user.canSwitchTenants) {
            logAudit({
                tenantId: user.tenantId,
                actorId: user.id,
                actorEmail: user.email,
                actorRole: user.role,
                action: 'TENANT_ISOLATION_BLOCKED',
                target: requested,
                status: 'DENIED',
                ip: getClientIp(req),
                details: `Tenant-locked user (${user.tenantId}) attempted cross-tenant access to ${requested}`
            });
            sendJson(res, 403, { error: `Tenant isolation enforced: your account is restricted to "${user.tenantId}".` }, req);
            return null;
        }
        const effective = (user.canSwitchTenants && requested && tenantsStore.some(t => t.id === requested))
            ? requested
            : (user.tenantId || 'tmg');
        return effective;
    }
    return (requested && tenantsStore.some(t => t.id === requested)) ? requested : 'tmg';
}

function updateDriverGpsAndAdherence(tenantId, body, actor, ip) {
    const ts = ensureTenantState(tenantId);
    const driverId = cleanStr(body.driverId || actor?.assignedDriverId || ts.activeDriverId || ts.drivers[0]?.id || 'd1', 32);
    if (!ts.driverStates[driverId]) {
        const drvMeta = ts.drivers.find(d => d.id === driverId) || { id: driverId, name: cleanStr(body.driverName || driverId, 80), truck: 'Assigned Truck' };
        ts.driverStates[driverId] = buildDefaultDriverState(getTenantById(tenantId), drvMeta);
    }
    const ds = ts.driverStates[driverId];
    ts.activeDriverId = driverId;
    if (typeof body.driverName === 'string' && body.driverName.trim()) ds.driverName = cleanStr(body.driverName, 80);
    if (typeof body.mode === 'string') ds.mode = cleanStr(body.mode, 24);
    else if (!ds.mode || ds.mode === 'standby') ds.mode = 'live_gps';
    if (typeof body.screenState === 'string') ds.screenState = cleanStr(body.screenState, 32);
    if (typeof body.accuracyM === 'number' && Number.isFinite(body.accuracyM)) ds.accuracyM = Math.round(body.accuracyM);
    if (typeof body.speedKmh === 'number' && Number.isFinite(body.speedKmh)) ds.speedKmh = Math.max(0, Math.round(body.speedKmh));
    if (typeof body.headingDeg === 'number' && Number.isFinite(body.headingDeg)) ds.headingDeg = Math.round(body.headingDeg);
    if (typeof body.activeShift === 'boolean') ds.activeShift = body.activeShift;
    else ds.activeShift = true;

    const prevLat = ds.lat;
    const prevLng = ds.lng;
    const hasCoords = typeof body.lat === 'number' && Number.isFinite(body.lat) && typeof body.lng === 'number' && Number.isFinite(body.lng);
    if (hasCoords) {
        ds.lat = Number(body.lat.toFixed(6));
        ds.lng = Number(body.lng.toFixed(6));
        if (typeof prevLat === 'number' && typeof prevLng === 'number' && ds.totalPings > 0) {
            const stepKm = haversineKm([prevLat, prevLng], [ds.lat, ds.lng]);
            if (stepKm > 0.01 && stepKm < 4) {
                ds.actualKm = Number(((ds.actualKm || 0) + stepKm).toFixed(2));
            }
        }
    }
    if (typeof body.plannedKm === 'number' && Number.isFinite(body.plannedKm)) ds.plannedKm = Number(body.plannedKm.toFixed(1));
    if (typeof body.actualKm === 'number' && Number.isFinite(body.actualKm)) ds.actualKm = Number(body.actualKm.toFixed(2));

    const toleranceM = Number(ts.preferences?.corridorToleranceM) || DEFAULT_CORRIDOR_TOLERANCE_M;
    if ((!Array.isArray(ds.assignedRoute?.coords) || ds.assignedRoute.coords.length < 2) && Array.isArray(body.routeCoords) && body.routeCoords.length >= 2) {
        ds.assignedRoute = {
            zone: tenantId,
            strategy: 'auto_sync',
            plannedKm: Number(body.plannedKm || ds.plannedKm || 16.5),
            stops: Array.isArray(ds.assignedRoute?.stops) ? ds.assignedRoute.stops : [],
            coords: body.routeCoords.filter(c => Array.isArray(c) && c.length >= 2 && Number.isFinite(c[0]) && Number.isFinite(c[1])).slice(0, 400),
            assignedAt: new Date().toISOString()
        };
    }
    const routeCoords = Array.isArray(ds.assignedRoute?.coords) ? ds.assignedRoute.coords : [];
    const wasViolation = Boolean(ds.violationActive);

    if (hasCoords && routeCoords.length >= 2) {
        const distM = distanceToPolylineM(ds.lat, ds.lng, routeCoords);
        ds.distanceFromRouteM = distM ?? 0;
        ds.onRoute = (distM ?? 0) <= toleranceM;
        ds.violationActive = !ds.onRoute;
        ds.totalPings = (ds.totalPings || 0) + 1;
        if (ds.onRoute) ds.onRoutePings = (ds.onRoutePings || 0) + 1;
        const pingRatio = ds.totalPings ? (ds.onRoutePings / ds.totalPings) * 100 : 100;
        const extraRatioPenalty = ds.plannedKm > 0 && ds.actualKm > ds.plannedKm ? Math.min(25, ((ds.actualKm - ds.plannedKm) / ds.plannedKm) * 35) : 0;
        ds.compliancePct = Math.max(0, Math.min(100, Math.round(pingRatio - extraRatioPenalty)));
        if (ds.violationActive) {
            ds.violationReason = body.violationReason ? cleanStr(body.violationReason, 160) : `Off planned corridor by ${ds.distanceFromRouteM} m (limit ${toleranceM} m)`;
            if (!wasViolation) {
                ds.deviationsCount = (ds.deviationsCount || 0) + 1;
                ds.events.unshift({ at: new Date().toISOString(), type: 'off_route', text: `${ds.driverName} moved ${ds.distanceFromRouteM}m off planned route` });
                logAudit({ tenantId, actorId: actor?.id, actorEmail: actor?.email || ds.driverName, actorRole: actor?.role || 'Driver', action: 'DRIVER_OFF_ROUTE', target: ds.driverId, status: 'DENIED', ip, details: ds.violationReason });
            }
        } else {
            ds.violationReason = '';
            if (wasViolation) {
                ds.events.unshift({ at: new Date().toISOString(), type: 'back_on_route', text: `${ds.driverName} returned onto planned corridor (${ds.distanceFromRouteM}m)` });
            }
        }
    } else {
        if (typeof body.compliancePct === 'number') ds.compliancePct = Math.max(0, Math.min(100, Math.round(body.compliancePct)));
        if (typeof body.violationActive === 'boolean') ds.violationActive = body.violationActive;
        if (typeof body.distanceFromRouteM === 'number') ds.distanceFromRouteM = Math.max(0, Math.round(body.distanceFromRouteM));
        ds.onRoute = !ds.violationActive;
        if (body.violationReason !== undefined) ds.violationReason = cleanStr(body.violationReason, 160);
    }

    ds.lastPingAt = new Date().toISOString();
    const extraKm = Math.max(0, +((ds.actualKm || 0) - (ds.plannedKm || 0)).toFixed(1));
    ds.costPerKmEgp = Number(ts.preferences?.defaultCostPerKm) || 14;
    ds.wastedEgp = ds.violationActive || extraKm > 0.3 ? Math.round(Math.max(extraKm, ds.distanceFromRouteM / 1000) * ds.costPerKmEgp) : 0;

    if (hasCoords) {
        const lastPt = ds.actualTrail[ds.actualTrail.length - 1];
        const movedEnough = !lastPt || haversineKm([lastPt.lat, lastPt.lng], [ds.lat, ds.lng]) >= 0.008;
        if (movedEnough) {
            ds.actualTrail.push({
                lat: ds.lat,
                lng: ds.lng,
                at: ds.lastPingAt,
                onRoute: ds.onRoute,
                distanceM: ds.distanceFromRouteM,
                speedKmh: ds.speedKmh
            });
            if (ds.actualTrail.length > 250) ds.actualTrail.shift();
        }
    }
    if (ds.events.length > 40) ds.events.length = 40;
    saveTenantsAndStates();
    return ds;
}

function buildLivePlatformContext(tenantId = 'tmg') {
    const ts = ensureTenantState(tenantId);
    const prefs = ts.preferences;
    const { bins, packets } = getMergedTenantBins(tenantId);
    const activeFires = bins.filter(b => (Number(b.temp) >= prefs.fireTemp) || b.deviceAlarms?.fire);
    const activeTilts = bins.filter(b => (Number(b.angle) >= prefs.tiltDeg) || b.deviceAlarms?.fall);
    const activeFulls = bins.filter(b => Number(b.fill || 0) >= prefs.fullThreshold);
    const pendingUsers = usersStore.filter(u => u.status === 'pending' && (u.tenantId || 'tmg') === tenantId).length;
    const activeDriver = ts.driverStates[ts.activeDriverId] || Object.values(ts.driverStates)[0];
    return {
        tenantId,
        tenant: getTenantById(tenantId),
        prefs,
        bins,
        packets,
        activeDriver,
        activeFires,
        activeTilts,
        activeFulls,
        pendingUsers,
        latestPkt: packets.find(p => p.reportType === '01' || p.reportType === '02') || packets[0] || null,
        backups: listLocalBackups()
    };
}

function buildAiChatReply(question, user, tenantId = 'tmg') {
    const qRaw = cleanStr(question, 500);
    const q = qRaw.toLowerCase();
    const isAr = /[\u0600-\u06FF]/.test(qRaw);
    const ctx = buildLivePlatformContext(tenantId);
    const d = ctx.latestPkt;
    const tName = ctx.tenant?.shortName || ctx.tenant?.title || tenantId;
    const drv = ctx.activeDriver;

    const actions = [];
    if (userHasPermission(user, 'backup:read')) {
        actions.push({ label: isAr ? 'تنزيل تقرير Excel CSV' : 'Download Live CSV Report', type: 'download', url: `/api/export/history.csv?tenantId=${encodeURIComponent(tenantId)}` });
    }
    if (userHasPermission(user, 'routes:read')) {
        actions.push({ label: isAr ? 'فتح مخطط المسارات' : 'Open AI Route Studio', type: 'tab', target: 'routes' });
    }

    if (/report|csv|export|تقرير|تقارير|ملف/.test(q)) {
        const msgAr = `تقرير التشغيل المباشر (${tName}):\n• إجمالي السلات: ${ctx.bins.length} (${ctx.tenant?.hardwareMode === 'live' ? 'هاردوير فعلي' : 'قطاع تجريبي'})\n• الباكتات المستلمة لهذا المشروع: ${ctx.packets.length}\n• حريق: ${ctx.activeFires.length} | مائلة: ${ctx.activeTilts.length} | ممتلئة (>=${ctx.prefs.fullThreshold}%): ${ctx.activeFulls.length}\n• السائق النشط: ${drv?.driverName || '--'} — التزام ${drv?.compliancePct ?? 100}% (${drv?.onRoute !== false ? 'على المسار المحدد' : `خارج المسار بـ ${drv?.distanceFromRouteM || 0}م`})\n• النسخ الاحتياطية: ${ctx.backups.length} نسخة.\n\nيمكنك تحميل التقرير مباشرة من الزر أدناه.`;
        const msgEn = `${tName} Live Operations Report:\n• Monitored Bins: ${ctx.bins.length} | Tenant Packets: ${ctx.packets.length}\n• Fire Alerts: ${ctx.activeFires.length} | Tilted: ${ctx.activeTilts.length} | Full (>=${ctx.prefs.fullThreshold}%): ${ctx.activeFulls.length}\n• Driver Status: ${drv?.driverName || '--'} (${drv?.compliancePct ?? 100}% compliance — ${drv?.onRoute !== false ? 'On Planned Route' : `Off-Route by ${drv?.distanceFromRouteM || 0}m`})\n• Local Backups: ${ctx.backups.length} stored.`;
        return { reply: isAr ? msgAr : msgEn, actions };
    }

    if (/route|best|priority|driver|fire|tilt|رووت|مسار|سواق|سائق|حريق|ميل|اولوي/.test(q)) {
        const orderNote = ctx.activeFires.length
            ? (isAr ? `الأولوية القصوى #1 الآن لإنذار الحريق (${ctx.activeFires.map(x => x.code).join(', ')})` : `URGENT Priority #1: Fire Alert on ${ctx.activeFires.map(x => x.code).join(', ')}`)
            : ctx.activeTilts.length
            ? (isAr ? `الأولوية #1 الآن لسلة مائلة (${ctx.activeTilts.map(x => x.code).join(', ')})` : `Priority #1: Tilted bin ${ctx.activeTilts.map(x => x.code).join(', ')}`)
            : (isAr ? `لا توجد طوارئ حالياً — الأولوية للسلات >= ${ctx.prefs.fullThreshold}% مع متابعة التزام السائق بالمسار المحدد.` : `No active fire or tilt emergencies — prioritizing bins >= ${ctx.prefs.fullThreshold}% with live route adherence tracking.`);
        const msgAr = `توصية المسار الذكي (${tName}):\n• ${orderNote}\n• حالة السائق الحالي (${drv?.driverName || '--'}): ${drv?.onRoute !== false ? 'يسير داخل المسار المحدد' : `انحراف ${drv?.distanceFromRouteM || 0} متر عن المسار`} (${drv?.compliancePct ?? 100}%).\n• حدود التنبيه: حرارة ${ctx.prefs.fireTemp}°C، امتلاء ${ctx.prefs.fullThreshold}%، ميلان ${ctx.prefs.tiltDeg}°.`;
        const msgEn = `Smart Route Recommendation (${tName}):\n• ${orderNote}\n• Active Driver (${drv?.driverName || '--'}): ${drv?.onRoute !== false ? 'On Planned Route' : `Off-Route by ${drv?.distanceFromRouteM || 0}m`} (${drv?.compliancePct ?? 100}%).\n• Thresholds: Fire >= ${ctx.prefs.fireTemp}°C, Full >= ${ctx.prefs.fullThreshold}%, Tilt >= ${ctx.prefs.tiltDeg}°.`;
        return { reply: isAr ? msgAr : msgEn, actions };
    }

    if (/security|owasp|audit|user|approve|backup|امان|سكيويرتي|صلاحيات|موافق|باك اب|نسخ|عزل/.test(q)) {
        if (userHasPermission(user, 'audit:read')) {
            actions.push({ label: isAr ? 'فتح مركز الأمان والنسخ' : 'Open Security & Audit Center', type: 'tab', target: 'security' });
        }
        const tenantAudits = auditLogs.filter(a => (a.tenantId || 'tmg') === tenantId).length;
        const msgAr = `حالة الأمان وعزل المشاريع (${tName}):\n• كل مشروع معزول بالكامل على السيرفر (السلات، السائقين، الباكتات، الإعدادات، وسجل التدقيق).\n• طلبات المستخدمين المعلقة: ${ctx.pendingUsers} | أحداث التدقيق للمشروع: ${tenantAudits}\n• جدول النسخ الاحتياطي المحلي: ${backupConfig.schedule} (${ctx.backups.length} نسخة).`;
        const msgEn = `OWASP Security & Tenant Isolation (${tName}):\n• Strict per-tenant data isolation active across bins, driver GPS, routes, preferences, users, and audit logs.\n• Pending Approvals: ${ctx.pendingUsers} | Tenant Audit Events: ${tenantAudits}\n• Auto-Backup: ${backupConfig.schedule} (${ctx.backups.length} snapshots).`;
        return { reply: isAr ? msgAr : msgEn, actions };
    }

    const liveSummaryAr = d
        ? `آخر قراءة (${d.imei}): الامتلاء ${d.fillPct ?? 'No echo'}%، الحرارة ${d.tempC ?? '--'}°C، الميلان ${d.fallAngleDeg ?? '--'}°، البطارية ${d.voltageV ?? '--'}V.`
        : `عدد السلات بالممشى الحالي (${tName}): ${ctx.bins.length} سلة، وحالة السائق: ${drv?.onRoute !== false ? 'على المسار' : 'خارج المسار'}.`;
    const liveSummaryEn = d
        ? `Latest Sensor (${d.imei}): Fill ${d.fillPct ?? 'No echo'}%, Temp ${d.tempC ?? '--'}°C, Tilt ${d.fallAngleDeg ?? '--'}°, Battery ${d.voltageV ?? '--'}V.`
        : `${tName} active bins: ${ctx.bins.length}, driver status: ${drv?.onRoute !== false ? 'On Planned Route' : 'Off-Route'}.`;

    return {
        reply: isAr
            ? `أهلاً ${user.name} (${tName}).\n• ${liveSummaryAr}\n• اسألني عن: "تقرير التشغيل"، "أفضل مسار وحالة السائق"، أو "الأمان وعزل البيانات".`
            : `Hello ${user.name} (${tName}).\n• ${liveSummaryEn}\n• Ask me about: "Live report", "Best route & driver compliance", or "Security & tenant isolation".`,
        actions
    };
}

const server = http.createServer(async (req, res) => {
    const reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = decodeURIComponent(reqUrl.pathname);
    const ip = getClientIp(req);

    if (req.method === 'OPTIONS') {
        setSecurityAndCorsHeaders(res, 'text/plain; charset=utf-8', req);
        res.writeHead(204);
        return res.end();
    }

    // Global & Auth Rate Limiting (OWASP API4)
    if (pathname.startsWith('/api/auth/')) {
        const rl = checkRateLimit(`auth:${ip}`, 25, 15 * 60 * 1000);
        if (!rl.allowed) {
            logAudit({ action: 'RATE_LIMIT_EXCEEDED', target: pathname, status: 'DENIED', ip, details: 'Auth brute-force protection triggered' });
            return sendJson(res, 429, { error: 'Too many authentication attempts. Please wait 15 minutes.' }, req);
        }
    } else if (pathname.startsWith('/api/')) {
        const rl = checkRateLimit(`api:${ip}`, 240, 60 * 1000);
        if (!rl.allowed) {
            return sendJson(res, 429, { error: 'Rate limit exceeded. Slow down requests.' }, req);
        }
    }

    // =========================================================================
    // OWASP AUTHENTICATION, USER APPROVAL WORKFLOW & RBAC SESSION ENDPOINTS
    // =========================================================================
    if (req.method === 'GET' && pathname === '/api/auth/session') {
        const user = getAuthenticatedUser(req);
        if (!user) return sendJson(res, 200, { authenticated: false, user: null }, req);
        return sendJson(res, 200, { authenticated: true, user: sanitizeUserPublic(user) }, req);
    }

    if (req.method === 'POST' && pathname === '/api/auth/login') {
        try {
            const body = await readJsonBody(req);
            const email = cleanStr(body.email || '', 120).toLowerCase();
            const password = String(body.password || '');
            if (!email || !password) return sendJson(res, 400, { error: 'Email and password are required' }, req);

            const user = usersStore.find(u => u.email.toLowerCase() === email);
            if (!user || !verifyPassword(password, user.passwordHash)) {
                logAudit({ actorEmail: email, action: 'LOGIN_FAILED', target: '/api/auth/login', status: 'DENIED', ip, details: 'Invalid credentials' });
                return sendJson(res, 401, { error: 'Invalid email or password' }, req);
            }
            if (user.status === 'pending') {
                logAudit({ tenantId: user.tenantId, actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'LOGIN_PENDING_BLOCKED', target: '/api/auth/login', status: 'DENIED', ip, details: 'Attempted login before admin approval' });
                return sendJson(res, 403, { error: 'Your account is pending Administrator approval.', code: 'PENDING_APPROVAL' }, req);
            }
            if (user.status !== 'approved') {
                logAudit({ tenantId: user.tenantId, actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'LOGIN_REJECTED_BLOCKED', target: '/api/auth/login', status: 'DENIED', ip, details: `Status is ${user.status}` });
                return sendJson(res, 403, { error: 'Your account access has been revoked or rejected.' }, req);
            }
            user.lastLoginAt = new Date().toISOString();
            user.lastLoginIp = ip;
            saveUsersStore();
            const token = signSessionToken(user);
            logAudit({ tenantId: user.tenantId, actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'LOGIN_SUCCESS', target: user.tenantId || 'tmg', status: 'ALLOWED', ip, details: `Signed in with role ${user.role}` });
            setSecurityAndCorsHeaders(res, 'application/json; charset=utf-8', req);
            res.setHeader('Set-Cookie', makeSessionCookie(token, Math.round(SESSION_TTL_MS / 1000), req));
            res.writeHead(200);
            return res.end(JSON.stringify({ ok: true, token, user: sanitizeUserPublic(user) }));
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/auth/demo-login') {
        try {
            const body = await readJsonBody(req);
            const roleReq = cleanStr(body.role || 'Admin', 40);
            const reqTenant = cleanStr(body.tenantId || 'tmg', 40);
            let targetEmail = null;
            if (reqTenant === 'palm_hills') {
                targetEmail = roleReq === 'Driver' ? 'driver201@palmhills.demo' : 'admin@palmhills.demo';
            } else if (reqTenant === 'kafd') {
                targetEmail = 'admin@kafd.demo';
            } else {
                const demoEmailMap = {
                    Admin: 'admin@tmg-madinaty.com',
                    Supervisor: 'ops@tmg-madinaty.com',
                    Driver: 'driver101@tmg-madinaty.com',
                    Auditor: 'audit@tmg-madinaty.com'
                };
                targetEmail = demoEmailMap[roleReq];
            }
            const user = targetEmail ? usersStore.find(u => u.email === targetEmail && u.status === 'approved') : null;
            if (!user) return sendJson(res, 403, { error: 'Demo profile unavailable or disabled by admin' }, req);
            user.lastLoginAt = new Date().toISOString();
            user.lastLoginIp = ip;
            saveUsersStore();
            const token = signSessionToken(user);
            logAudit({ tenantId: user.tenantId, actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'DEMO_LOGIN_SUCCESS', target: user.tenantId || 'tmg', status: 'ALLOWED', ip, details: `Quick-access login as ${user.role}` });
            setSecurityAndCorsHeaders(res, 'application/json; charset=utf-8', req);
            res.setHeader('Set-Cookie', makeSessionCookie(token, Math.round(SESSION_TTL_MS / 1000), req));
            res.writeHead(200);
            return res.end(JSON.stringify({ ok: true, token, user: sanitizeUserPublic(user) }));
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/auth/register') {
        try {
            const body = await readJsonBody(req);
            const name = cleanStr(body.name || '', 80);
            const email = cleanStr(body.email || '', 120).toLowerCase();
            const password = String(body.password || '');
            const requestedRole = ['Supervisor', 'Operator', 'Driver', 'Auditor'].includes(body.role) ? body.role : 'Operator';
            const tenantId = tenantsStore.some(t => t.id === body.tenantId) ? body.tenantId : 'tmg';
            const note = cleanStr(body.note || '', 160);

            if (!name || !email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
                return sendJson(res, 400, { error: 'Valid full name and email address are required' }, req);
            }
            if (password.length < 8) {
                return sendJson(res, 400, { error: 'OWASP Policy: Password must be at least 8 characters long' }, req);
            }
            if (usersStore.some(u => u.email.toLowerCase() === email)) {
                return sendJson(res, 409, { error: 'An account with this email already exists' }, req);
            }

            const ts = ensureTenantState(tenantId);
            const newUser = {
                id: 'u-' + crypto.randomBytes(6).toString('hex'),
                name,
                email,
                passwordHash: hashPassword(password),
                role: requestedRole,
                tenantId,
                canSwitchTenants: false,
                assignedDriverId: requestedRole === 'Driver' ? (ts.drivers?.[0]?.id || 'd1') : null,
                status: 'pending',
                permissions: [...(ROLE_DEFAULT_PERMISSIONS[requestedRole] || ROLE_DEFAULT_PERMISSIONS.Operator)],
                requestedNote: note,
                createdAt: new Date().toISOString()
            };
            usersStore.push(newUser);
            saveUsersStore();
            logAudit({ tenantId, actorId: newUser.id, actorEmail: newUser.email, actorRole: requestedRole, action: 'USER_REGISTERED', target: 'Approval Queue', status: 'INFO', ip, details: `New ${requestedRole} account awaiting admin approval (${tenantId})` });
            return sendJson(res, 201, {
                ok: true,
                status: 'pending',
                message: `Registration submitted for ${getTenantById(tenantId)?.shortName || tenantId}. Waiting for Administrator approval.`
            }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/auth/logout') {
        const user = getAuthenticatedUser(req);
        if (user) {
            logAudit({ actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'LOGOUT', target: 'Session', status: 'INFO', ip, details: 'Signed out cleanly' });
        }
        setSecurityAndCorsHeaders(res, 'application/json; charset=utf-8', req);
        res.setHeader('Set-Cookie', makeSessionCookie('', 0, req));
        res.writeHead(200);
        return res.end(JSON.stringify({ ok: true }));
    }

    // =========================================================================
    // MULTI-TENANT BOOTSTRAP, ISOLATION & OVERVIEW REPORTING
    // =========================================================================
    if (req.method === 'GET' && pathname === '/api/tenants') {
        const user = getAuthenticatedUser(req);
        const list = user && !user.canSwitchTenants
            ? tenantsStore.filter(t => t.id === user.tenantId)
            : tenantsStore;
        return sendJson(res, 200, {
            tenants: list.map(t => ({
                id: t.id, title: t.title, shortName: t.shortName, sub: t.sub, city: t.city,
                hardwareMode: t.hardwareMode, color: t.color, sec: t.sec, center: t.center, zoom: t.zoom
            })),
            canSwitchTenants: user ? Boolean(user.canSwitchTenants) : true
        }, req);
    }

    if (req.method === 'POST' && pathname === '/api/admin/tenants') {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        if (!actor.canSwitchTenants) {
            return sendJson(res, 403, { error: 'Only Platform Super Admin can provision new isolated tenants.' }, req);
        }
        try {
            const body = await readJsonBody(req);
            const shortName = cleanStr(body.shortName || body.name || '', 60);
            const sub = cleanStr(body.sub || body.sector || 'Smart Compound', 60);
            const city = cleanStr(body.city || 'Cairo', 40);
            const rawId = cleanStr(body.id || shortName.toLowerCase().replace(/[^a-z0-9]+/g, '_'), 24).replace(/^_+|_+$/g, '');
            const id = rawId || `tenant_${Date.now().toString(36)}`;
            if (!shortName) return sendJson(res, 400, { error: 'Compound / tenant name is required' }, req);
            if (tenantsStore.some(t => t.id === id)) return sendJson(res, 409, { error: `Tenant "${id}" already exists` }, req);

            const lat = Number.isFinite(Number(body.lat)) ? Number(body.lat) : 30.0444;
            const lng = Number.isFinite(Number(body.lng)) ? Number(body.lng) : 31.2357;
            const color = /^#[0-9a-fA-F]{6}$/.test(String(body.color || '')) ? body.color : '#06B6D4';
            const prefix = id.slice(0, 2).toUpperCase();
            const newTenant = {
                id,
                title: cleanStr(body.title || `${shortName} — Smart Waste`, 80),
                shortName,
                sub,
                city,
                hardwareMode: body.hardwareMode === 'live' ? 'live' : 'demo',
                color,
                sec: '#8B5CF6',
                center: [lat, lng],
                zoom: 14,
                anchors: {
                    depot: [+(lat + 0.008).toFixed(4), +(lng - 0.011).toFixed(4), `${shortName} Depot`],
                    landfill: [+(lat - 0.009).toFixed(4), +(lng + 0.013).toFixed(4), `${shortName} Recycling Hub`],
                    streets: [
                        [+(lat + 0.003).toFixed(4), +(lng - 0.004).toFixed(4), `${shortName} Main Ave`],
                        [+(lat - 0.002).toFixed(4), +(lng + 0.005).toFixed(4), `${shortName} Central Loop`],
                        [+(lat + 0.005).toFixed(4), +(lng + 0.003).toFixed(4), `${shortName} Gate Blvd`],
                        [+(lat - 0.004).toFixed(4), +(lng - 0.005).toFixed(4), `${shortName} Park Way`]
                    ]
                },
                drivers: [
                    { id: `${id}-d1`, name: cleanStr(body.driverName || `${shortName} Driver #1`, 50), truck: 'Truck #101', homeDepot: `${shortName} Depot` }
                ],
                seedBins: body.hardwareMode === 'live' ? [] : [
                    { id: Date.now() + 1, code: `${prefix}-01`, name: `${prefix}-01 • Main Ave`, street: `${shortName} Main Ave`, category: 'Smart Curb Bin 660L', lat: +(lat + 0.003).toFixed(4), lng: +(lng - 0.004).toFixed(4), fill: 84, heightCm: 19, temp: 27, angle: 1, battery: 95, voltageV: 3.58, rssi: -68, imei: `86099${String(Date.now()).slice(-10)}1`, serial: `86099${String(Date.now()).slice(-10)}1`, model: 'DF703 Pilot (Demo)', isLive: false },
                    { id: Date.now() + 2, code: `${prefix}-02`, name: `${prefix}-02 • Central Loop`, street: `${shortName} Central Loop`, category: 'Smart Curb Bin 660L', lat: +(lat - 0.002).toFixed(4), lng: +(lng + 0.005).toFixed(4), fill: 48, heightCm: 62, temp: 25, angle: 0, battery: 90, voltageV: 3.55, rssi: -71, imei: `86099${String(Date.now()).slice(-10)}2`, serial: `86099${String(Date.now()).slice(-10)}2`, model: 'DF703 Pilot (Demo)', isLive: false },
                    { id: Date.now() + 3, code: `${prefix}-03`, name: `${prefix}-03 • Gate Blvd`, street: `${shortName} Gate Blvd`, category: 'Commercial Bin 1100L', lat: +(lat + 0.005).toFixed(4), lng: +(lng + 0.003).toFixed(4), fill: 91, heightCm: 11, temp: 28, angle: 2, battery: 88, voltageV: 3.54, rssi: -69, imei: `86099${String(Date.now()).slice(-10)}3`, serial: `86099${String(Date.now()).slice(-10)}3`, model: 'DF703 Pilot (Demo)', isLive: false }
                ]
            };
            tenantsStore.push(newTenant);
            ensureTenantState(id);
            saveTenantsAndStates();
            logAudit({ tenantId: id, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'TENANT_CREATED', target: id, status: 'ALLOWED', ip, details: `Created isolated workspace ${shortName}` });
            return sendJson(res, 201, { ok: true, tenant: newTenant }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && pathname === '/api/tenant/bootstrap') {
        const user = getAuthenticatedUser(req);
        const tenantId = resolveTenantContext(req, res, reqUrl, user);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        const tMeta = getTenantById(tenantId);
        const { bins, packets } = getMergedTenantBins(tenantId);
        const accessibleTenants = user && !user.canSwitchTenants
            ? tenantsStore.filter(t => t.id === user.tenantId)
            : tenantsStore;
        return sendJson(res, 200, {
            ok: true,
            tenantId,
            tenant: {
                id: tMeta.id, title: tMeta.title, shortName: tMeta.shortName, sub: tMeta.sub,
                city: tMeta.city, hardwareMode: tMeta.hardwareMode, color: tMeta.color, sec: tMeta.sec,
                center: tMeta.center, zoom: tMeta.zoom
            },
            tenants: accessibleTenants.map(t => ({
                id: t.id, title: t.title, shortName: t.shortName, sub: t.sub,
                hardwareMode: t.hardwareMode, color: t.color, sec: t.sec, center: t.center
            })),
            canSwitchTenants: user ? Boolean(user.canSwitchTenants) : true,
            preferences: ts.preferences,
            anchors: ts.anchors,
            drivers: ts.drivers,
            activeDriverId: ts.activeDriverId,
            driverStates: ts.driverStates,
            bins,
            packets: packets.slice(0, 40),
            schedules: ts.schedules,
            pendingDevices: ts.pendingDevices,
            overviewReport: buildOverviewReport(tenantId)
        }, req);
    }

    if (req.method === 'GET' && pathname === '/api/overview/report') {
        const user = getAuthenticatedUser(req);
        const tenantId = resolveTenantContext(req, res, reqUrl, user);
        if (!tenantId) return;
        return sendJson(res, 200, buildOverviewReport(tenantId), req);
    }

    // =========================================================================
    // ADMIN USER APPROVAL, GRANULAR PRIVILEGES (RBAC) & AUDIT LOGS
    // =========================================================================
    if (req.method === 'GET' && pathname === '/api/admin/users') {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const scopedUsers = usersStore.filter(u => (u.tenantId || 'tmg') === tenantId);
        return sendJson(res, 200, {
            tenantId,
            users: scopedUsers.map(sanitizeUserPublic),
            allPermissions: ALL_PERMISSIONS,
            roleDefaults: ROLE_DEFAULT_PERMISSIONS
        }, req);
    }

    const approveMatch = req.method === 'POST' && pathname.match(/^\/api\/admin\/users\/([^/]+)\/approve$/);
    if (approveMatch) {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        try {
            const body = await readJsonBody(req);
            const target = usersStore.find(u => u.id === approveMatch[1]);
            if (!target) return sendJson(res, 404, { error: 'User not found' }, req);
            if (!actor.canSwitchTenants && (target.tenantId || 'tmg') !== tenantId) {
                return sendJson(res, 403, { error: 'Cannot approve users belonging to another tenant' }, req);
            }
            const assignedRole = ROLE_DEFAULT_PERMISSIONS[body.role] ? body.role : target.role;
            target.role = assignedRole;
            target.status = 'approved';
            target.approvedAt = new Date().toISOString();
            target.approvedBy = actor.email;
            if (Array.isArray(body.permissions)) {
                target.permissions = body.permissions.filter(p => ALL_PERMISSIONS.includes(p) || p === '*');
            } else {
                target.permissions = [...(ROLE_DEFAULT_PERMISSIONS[assignedRole] || [])];
            }
            saveUsersStore();
            logAudit({ tenantId: target.tenantId || tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'USER_APPROVED', target: target.email, status: 'ALLOWED', ip, details: `Approved as ${target.role} with ${target.permissions.length} privileges` });
            return sendJson(res, 200, { ok: true, user: sanitizeUserPublic(target) }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    const rejectMatch = req.method === 'POST' && pathname.match(/^\/api\/admin\/users\/([^/]+)\/reject$/);
    if (rejectMatch) {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const target = usersStore.find(u => u.id === rejectMatch[1]);
        if (!target) return sendJson(res, 404, { error: 'User not found' }, req);
        if (!actor.canSwitchTenants && (target.tenantId || 'tmg') !== tenantId) {
            return sendJson(res, 403, { error: 'Cannot modify users belonging to another tenant' }, req);
        }
        if (target.id === actor.id) return sendJson(res, 400, { error: 'Cannot reject your own active admin session' }, req);
        target.status = 'rejected';
        saveUsersStore();
        logAudit({ tenantId: target.tenantId || tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'USER_REJECTED', target: target.email, status: 'ALLOWED', ip, details: 'Access denied / revoked' });
        return sendJson(res, 200, { ok: true, user: sanitizeUserPublic(target) }, req);
    }

    const permsMatch = req.method === 'POST' && pathname.match(/^\/api\/admin\/users\/([^/]+)\/permissions$/);
    if (permsMatch) {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        try {
            const body = await readJsonBody(req);
            const target = usersStore.find(u => u.id === permsMatch[1]);
            if (!target) return sendJson(res, 404, { error: 'User not found' }, req);
            if (!actor.canSwitchTenants && (target.tenantId || 'tmg') !== tenantId) {
                return sendJson(res, 403, { error: 'Cannot edit users belonging to another tenant' }, req);
            }
            if (body.role && ROLE_DEFAULT_PERMISSIONS[body.role]) target.role = body.role;
            if (Array.isArray(body.permissions)) {
                target.permissions = body.permissions.filter(p => ALL_PERMISSIONS.includes(p) || p === '*');
            }
            saveUsersStore();
            logAudit({ tenantId: target.tenantId || tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'PERMISSIONS_UPDATED', target: target.email, status: 'ALLOWED', ip, details: `Role=${target.role}, Perms=${target.permissions.length}` });
            return sendJson(res, 200, { ok: true, user: sanitizeUserPublic(target) }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && pathname === '/api/admin/audit-logs') {
        const actor = requirePermission(req, res, 'audit:read');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const limit = Math.min(300, Math.max(10, Number(reqUrl.searchParams.get('limit')) || 120));
        const scopedLogs = auditLogs.filter(l => (l.tenantId || 'tmg') === tenantId);
        return sendJson(res, 200, { tenantId, total: scopedLogs.length, logs: scopedLogs.slice(0, limit) }, req);
    }

    if (req.method === 'GET' && pathname === '/api/admin/security-status') {
        const actor = requirePermission(req, res, 'dashboard:read');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const pendingCount = usersStore.filter(u => u.status === 'pending' && (u.tenantId || 'tmg') === tenantId).length;
        const auditCount = auditLogs.filter(l => (l.tenantId || 'tmg') === tenantId).length;
        return sendJson(res, 200, {
            tenantId,
            standard: 'OWASP Top 10 + Multi-Tenant Isolation Controls',
            controls: [
                { id: 'A01', name: 'Broken Access Control (RBAC + Tenant Isolation)', active: true, detail: `Strict isolation for "${tenantId}" (${pendingCount} pending approvals)` },
                { id: 'A02', name: 'Cryptographic Failures (scrypt + HMAC-SHA256)', active: true, detail: 'Salted scrypt passwords, signed HttpOnly sessions, SHA-256 backup digests' },
                { id: 'A03', name: 'Injection & XSS Defense', active: true, detail: 'CSP headers + zero-SQL JSON store + strict HTML escaping & payload clamping' },
                { id: 'A05', name: 'Security Misconfiguration Hardening', active: true, detail: 'X-Frame-Options: DENY, nosniff, HSTS, same-origin CORS' },
                { id: 'A07', name: 'Auth Rate Limiting & Brute-Force Guard', active: true, detail: '25 auth attempts / 15m per IP, 240 API req / min' },
                { id: 'A09', name: 'Security Logging & Monitoring (Audit Trail)', active: true, detail: `${auditCount} tenant audit events recorded` }
            ]
        }, req);
    }

    // =========================================================================
    // LOCAL AUTO-BACKUP MANAGER & MULTI-METHOD EXPORT/RESTORE
    // =========================================================================
    if (req.method === 'GET' && pathname === '/api/admin/backups') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const files = listLocalBackups().filter(f => f.filename.startsWith(`${tenantId}-`) || (tenantId === 'tmg' && f.filename.startsWith('tmg-')));
        return sendJson(res, 200, { tenantId, config: backupConfig, files }, req);
    }

    if (req.method === 'POST' && pathname === '/api/admin/backups/config') {
        const actor = requirePermission(req, res, 'backup:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        try {
            const body = await readJsonBody(req);
            if (['hourly', '6h', 'daily', 'weekly', 'manual'].includes(body.schedule)) {
                backupConfig.schedule = body.schedule;
                backupConfig.nextRunAt = computeNextBackupIso(body.schedule);
            }
            if (body.retentionCount !== undefined) {
                backupConfig.retentionCount = Math.max(3, Math.min(100, Number(body.retentionCount) || 24));
            }
            if (body.autoDownloadBrowser !== undefined) {
                backupConfig.autoDownloadBrowser = Boolean(body.autoDownloadBrowser);
            }
            saveBackupConfig();
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_CONFIG_UPDATED', target: backupConfig.schedule, status: 'ALLOWED', ip, details: `Retention=${backupConfig.retentionCount}, AutoDownload=${backupConfig.autoDownloadBrowser}` });
            return sendJson(res, 200, { ok: true, config: backupConfig }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/admin/backups/run') {
        const actor = requirePermission(req, res, 'backup:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const created = createLocalBackup('manual', actor.email, tenantId);
        logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_CREATED', target: created.filename, status: 'ALLOWED', ip, details: `SHA256=${created.sha256.slice(0, 12)}... (${created.packetsCount} packets)` });
        const files = listLocalBackups().filter(f => f.filename.startsWith(`${tenantId}-`) || (tenantId === 'tmg' && f.filename.startsWith('tmg-')));
        return sendJson(res, 201, { ok: true, backup: created, config: backupConfig, files }, req);
    }

    if (req.method === 'GET' && pathname === '/api/admin/backups/download') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const fileParam = path.basename(reqUrl.searchParams.get('file') || '');
        const format = (reqUrl.searchParams.get('format') || 'json').toLowerCase();
        const tenantFiles = listLocalBackups().filter(f => f.filename.startsWith(`${tenantId}-`) || (tenantId === 'tmg' && f.filename.startsWith('tmg-')));
        let targetFile = fileParam && fileParam.endsWith('.json') ? fileParam : tenantFiles[0]?.filename;
        if (!targetFile) {
            targetFile = createLocalBackup('on-demand', actor.email, tenantId).filename;
        }
        const full = path.join(BACKUPS_DIR, targetFile);
        if (!full.startsWith(BACKUPS_DIR + path.sep) || !fs.existsSync(full)) {
            return sendJson(res, 404, { error: 'Backup file not found' }, req);
        }
        logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_DOWNLOADED', target: targetFile, status: 'ALLOWED', ip, details: `Format=${format}` });
        if (format === 'csv') {
            const parsed = JSON.parse(fs.readFileSync(full, 'utf8'));
            const rows = ['ID,ReceivedAt,ReportType,IMEI,FillPct,HeightCm,TempC,TiltDeg,VoltageV,BatteryState,SignalDbm'];
            (parsed.df703Packets || []).forEach(p => {
                rows.push([p.id, p.receivedAt, p.reportType, p.imei, p.fillPct ?? '', p.heightCm ?? '', p.tempC ?? '', p.fallAngleDeg ?? '', p.voltageV ?? '', p.batteryState ?? '', p.rsrpDbm ?? ''].join(','));
            });
            setSecurityAndCorsHeaders(res, 'text/csv; charset=utf-8', req);
            res.setHeader('Content-Disposition', `attachment; filename="${targetFile.replace('.json', '.csv')}"`);
            res.writeHead(200);
            return res.end('\uFEFF' + rows.join('\r\n'));
        }
        setSecurityAndCorsHeaders(res, 'application/json; charset=utf-8', req);
        res.setHeader('Content-Disposition', `attachment; filename="${targetFile}"`);
        res.writeHead(200);
        return fs.createReadStream(full).pipe(res);
    }

    if (req.method === 'POST' && pathname === '/api/admin/backups/restore') {
        const actor = requirePermission(req, res, 'backup:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        try {
            const body = await readJsonBody(req, 1048576);
            let backupObj = body.backupData;
            if (!backupObj && body.filename) {
                const safeName = path.basename(String(body.filename));
                const full = path.join(BACKUPS_DIR, safeName);
                if (!full.startsWith(BACKUPS_DIR + path.sep) || !fs.existsSync(full)) {
                    return sendJson(res, 404, { error: 'Backup file not found' }, req);
                }
                backupObj = JSON.parse(fs.readFileSync(full, 'utf8'));
            }
            if (!backupObj || backupObj.format !== 'TMG-LOCAL-BACKUP-V1') {
                return sendJson(res, 400, { error: 'Invalid backup format; expected TMG-LOCAL-BACKUP-V1' }, req);
            }
            const ts = ensureTenantState(tenantId);
            if (Array.isArray(backupObj.df703Packets)) {
                backupObj.df703Packets.forEach(p => { if (!df703Packets.some(x => x.id === p.id)) addEntryToMemory({ ...p, tenantId }); });
            }
            if (backupObj.adminPreferences) {
                Object.assign(ts.preferences, backupObj.adminPreferences);
                saveTenantsAndStates();
            }
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_RESTORED', target: body.filename || 'uploaded-json', status: 'ALLOWED', ip, details: `Restored tenant ${tenantId} preferences & packets` });
            return sendJson(res, 200, { ok: true, restoredPackets: (backupObj.df703Packets || []).length, restoredAt: new Date().toISOString() }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // =========================================================================
    // AI OPERATIONS CHATBOT & DYNAMIC EMERGENCY DRIVER RE-ROUTING
    // =========================================================================
    if (req.method === 'POST' && pathname === '/api/chat') {
        const actor = requirePermission(req, res, 'chat:use');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        try {
            const body = await readJsonBody(req);
            const message = cleanStr(body.message || '', 500);
            if (!message) return sendJson(res, 400, { error: 'Message cannot be empty' }, req);
            const out = buildAiChatReply(message, actor, tenantId);
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'AI_CHAT_QUERY', target: 'Chatbot', status: 'ALLOWED', ip, details: message.slice(0, 90) });
            return sendJson(res, 200, { ok: true, ...out }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/driver/reroute-check') {
        const actor = requirePermission(req, res, 'driver:gps');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        try {
            const body = await readJsonBody(req, 262144);
            const allBins = Array.isArray(body.bins) ? body.bins : [];
            const currentRoute = Array.isArray(body.currentRouteIds) ? body.currentRouteIds.map(String) : [];
            const remainingOriginal = allBins.filter(b => currentRoute.includes(String(b.id)));
            const fires = allBins.filter(b => (Number(b.temp) >= ts.preferences.fireTemp) || b.deviceAlarms?.fire);
            const tilts = allBins.filter(b => !fires.includes(b) && ((Number(b.angle) >= ts.preferences.tiltDeg) || b.deviceAlarms?.fall));

            const emergencyIds = new Set([...fires, ...tilts].map(b => String(b.id)));
            const resumedStops = remainingOriginal.filter(b => !emergencyIds.has(String(b.id)));
            const reroutedStops = [...fires, ...tilts, ...resumedStops];

            if (fires.length || tilts.length) {
                logAudit({
                    tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role,
                    action: 'EMERGENCY_REROUTE', target: 'Driver Route', status: 'ALLOWED', ip,
                    details: `Fire=${fires.length}, Tilted=${tilts.length}, Resuming=${resumedStops.length} remaining stops`
                });
            }
            return sendJson(res, 200, {
                ok: true,
                rerouted: fires.length > 0 || tilts.length > 0,
                prioritySummary: { fires: fires.length, tilts: tilts.length, resumed: resumedStops.length },
                orderedStopIds: reroutedStops.map(b => b.id)
            }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && (pathname === '/health' || pathname === '/api/health')) {
        return sendJson(res, 200, {
            status: 'ok',
            service: 'tmg-madinaty-smart-waste',
            tcpBridgePort: TCP_PORT,
            tcpBridgeActive: tcpServerActive,
            tcpStats,
            connectedDevices: [...liveSockets.keys()],
            knownDevices: deviceState.size,
            uptimeSeconds: Math.round(process.uptime()),
            timestamp: new Date().toISOString()
        }, req);
    }

    if (req.method === 'GET' && pathname === '/api/status') {
        return sendJson(res, 200, {
            platform: 'Smart Waste Solution - TMG (Madinaty)',
            version: '3.0.0-OWASP',
            protocolSupport: 'Dingtek DF703 4G V2.8 (TCP + HTTPS API Gateway)',
            ingestReady: true,
            recentTelemetryCount: telemetryLog.length,
            recentPacketsCount: df703Packets.length,
            recentTelemetry: telemetryLog.slice(0, 20)
        }, req);
    }

    // Legacy / generic JSON telemetry endpoint
    if (req.method === 'POST' && pathname === '/api/telemetry') {
        try {
            const payload = await readJsonBody(req);
            const record = {
                receivedAt: new Date().toISOString(),
                deviceId: cleanStr(payload.deviceId || payload.serial || 'unknown', 64),
                fill: payload.fill ?? null,
                temp: payload.temp ?? null,
                angle: payload.angle ?? null,
                battery: payload.battery ?? null
            };
            telemetryLog.unshift(record);
            if (telemetryLog.length > 200) telemetryLog.pop();
            return sendJson(res, 201, { accepted: true, record }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // Dingtek DF703 4G V2.8 Ingestion Endpoint
    if (req.method === 'POST' && pathname === '/api/df703/ingest') {
        if (!isAuthorized(req)) return sendJson(res, 401, { error: 'Missing or invalid Bearer token' }, req);
        try {
            const body = await readJsonBody(req);
            if (!body.rawHex) {
                return sendJson(res, 400, { error: 'rawHex field is required' }, req);
            }
            const parsed = parseDf703Packet(body.rawHex);
            const reqTenant = cleanStr(body.tenantId || reqUrl.searchParams.get('tenantId') || '', 40);
            const targetTenant = tenantsStore.some(t => t.id === reqTenant) ? reqTenant : resolvePacketTenant(parsed.imei);
            const { entry, pendingDownlink } = recordParsedDf703(parsed, cleanStr(body.channel || 'HTTPS API Gateway', 64), {
                remoteAddress: cleanStr(body.remoteAddress || req.socket.remoteAddress, 64),
                tenantId: targetTenant
            });
            logDf703(`HTTP ingest type ${parsed.reportType} from IMEI ${parsed.imei} (tenant=${targetTenant})`);
            return sendJson(res, 201, {
                accepted: true,
                tenantId: targetTenant,
                decoded: entry,
                queuedDownlinkToSend: pendingDownlink
            }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && pathname === '/api/df703/packets') {
        const user = getAuthenticatedUser(req);
        const tenantId = resolveTenantContext(req, res, reqUrl, user);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        const scopedPackets = df703Packets.filter(p => (p.tenantId || resolvePacketTenant(p.imei)) === tenantId);
        const queuedSummary = [];
        for (const [imei, cmds] of downlinkQueue.entries()) {
            if (resolvePacketTenant(imei) === tenantId) queuedSummary.push({ imei, commands: cmds });
        }
        return sendJson(res, 200, {
            protocol: 'Dingtek DF703 4G V2.8',
            tenantId,
            tcpBridgeActive: tcpServerActive,
            tcpBridgePort: TCP_PORT,
            tcpStats,
            connectedDevices: [...liveSockets.keys()].filter(imei => resolvePacketTenant(imei) === tenantId),
            packetsCount: scopedPackets.length,
            packets: scopedPackets.slice(0, 40),
            queuedDownlinks: queuedSummary,
            adminPreferences: ts.preferences,
            driverStates: ts.driverStates,
            activeDriverId: ts.activeDriverId,
            overviewReport: buildOverviewReport(tenantId)
        }, req);
    }

    // Queue Downlink Command (80029999...81) for a DF703 IMEI (Requires downlink:send privilege or INGEST_TOKEN)
    if (req.method === 'POST' && pathname === '/api/df703/downlink') {
        const hasBearer = INGEST_TOKEN && isAuthorized(req);
        const actor = hasBearer ? { id: 'token', email: 'api-token', role: 'Admin', canSwitchTenants: true, tenantId: 'tmg' } : requirePermission(req, res, 'downlink:send');
        if (!actor) return;
        const tenantId = hasBearer ? 'tmg' : resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        try {
            const body = await readJsonBody(req);
            const frame = buildDf703Downlink(body.cmdCode, body.value);
            const imei = cleanStr(body.imei || '864317087587096', 24);
            if (!downlinkQueue.has(imei)) downlinkQueue.set(imei, []);
            const item = { imei, tenantId, cmdCode: String(body.cmdCode).toUpperCase(), frame, queuedAt: new Date().toISOString() };
            downlinkQueue.get(imei).push(item);
            const sentNow = tryDeliverNow(imei);
            logDf703(`Downlink ${sentNow ? 'sent' : 'queued'} for ${imei}: ${frame}`);
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'DOWNLINK_QUEUED', target: imei, status: 'ALLOWED', ip, details: `CMD ${item.cmdCode} -> ${frame}` });
            return sendJson(res, 201, { queued: !sentNow, sentNow: Boolean(sentNow), command: item }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // Admin Preferences (persisted per tenant in DATA_DIR/tenant_states.json, requires prefs:write on POST)
    if (req.method === 'GET' && pathname === '/api/admin/preferences') {
        const user = getAuthenticatedUser(req);
        const tenantId = resolveTenantContext(req, res, reqUrl, user);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        return sendJson(res, 200, { ok: true, tenantId, preferences: ts.preferences, anchors: ts.anchors }, req);
    }
    if (req.method === 'POST' && pathname === '/api/admin/preferences') {
        const actor = requirePermission(req, res, 'prefs:write');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        try {
            const body = await readJsonBody(req);
            if (!body || typeof body !== 'object') {
                return sendJson(res, 400, { error: 'Invalid JSON body' }, req);
            }
            const next = { ...ts.preferences };
            if (body.fireTemp !== undefined) next.fireTemp = Math.max(0, Math.min(255, Number(body.fireTemp)));
            if (body.fullThreshold !== undefined) next.fullThreshold = Math.max(0, Math.min(100, Number(body.fullThreshold)));
            if (body.tiltDeg !== undefined) next.tiltDeg = Math.max(0, Math.min(90, Number(body.tiltDeg)));
            if (body.lowBattThreshold !== undefined) next.lowBattThreshold = Math.max(0, Math.min(100, Number(body.lowBattThreshold)));
            if (body.defaultCostPerKm !== undefined) next.defaultCostPerKm = Math.max(1, Number(body.defaultCostPerKm));
            if (body.corridorToleranceM !== undefined) next.corridorToleranceM = Math.max(25, Math.min(1000, Number(body.corridorToleranceM)));
            if (body.routeEndPolicy !== undefined) next.routeEndPolicy = cleanStr(body.routeEndPolicy, 32);
            if (body.autoSchedulerEnabled !== undefined) next.autoSchedulerEnabled = Boolean(body.autoSchedulerEnabled);
            ts.preferences = next;
            if (body.anchors && typeof body.anchors === 'object') {
                if (Array.isArray(body.anchors.depot) && body.anchors.depot.length >= 2) ts.anchors.depot = body.anchors.depot;
                if (Array.isArray(body.anchors.landfill) && body.anchors.landfill.length >= 2) ts.anchors.landfill = body.anchors.landfill;
            }
            if (typeof body.brandTitle === 'string' && body.brandTitle.trim()) {
                const tMeta = getTenantById(tenantId);
                if (tMeta) tMeta.title = cleanStr(body.brandTitle, 80);
            }
            if (typeof body.brandColor === 'string' && /^#[0-9a-fA-F]{6}$/.test(body.brandColor)) {
                const tMeta = getTenantById(tenantId);
                if (tMeta) tMeta.color = body.brandColor;
            }
            if (tenantId === 'tmg') {
                adminPreferences = { ...adminPreferences, ...next };
                saveAdminPreferences();
            }
            saveTenantsAndStates();
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'PREFERENCES_UPDATED', target: `Tenant ${tenantId}`, status: 'ALLOWED', ip, details: `Fire=${next.fireTemp}C Full=${next.fullThreshold}% Tilt=${next.tiltDeg}deg Corridor=${next.corridorToleranceM || DEFAULT_CORRIDOR_TOLERANCE_M}m` });
            return sendJson(res, 200, { ok: true, tenantId, preferences: ts.preferences, anchors: ts.anchors }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // Latest state of every DF703 that has reported (one row per IMEI)
    if (req.method === 'GET' && pathname === '/api/df703/devices') {
        const devices = [...deviceState.values()]
            .map(d => ({ ...d, settings: { binDepthMm: defaultBinDepthMm, ...deviceSettings[d.imei] }, connected: liveSockets.has(d.imei), pendingDownlinks: (downlinkQueue.get(d.imei) || []).length }))
            .sort((a, b) => String(b.lastSeenAt).localeCompare(String(a.lastSeenAt)));
        return sendJson(res, 200, { count: devices.length, devices });
    }
    // Per-sensor settings, e.g. the bin depth used for fill % (persisted in DATA_DIR)
    const settingsMatch = req.method === 'POST' && pathname.match(/^\/api\/df703\/devices\/(\d{6,20})\/settings$/);
    if (settingsMatch) {
        if (!isAuthorized(req)) return sendJson(res, 401, { error: 'Missing or invalid Bearer token' });
        try {
            const imei = settingsMatch[1];
            const body = await readJsonBody(req);
            const depth = Number(body.binDepthMm);
            if (!Number.isFinite(depth) || depth < 200 || depth > 5000) {
                return sendJson(res, 400, { error: 'binDepthMm must be a number between 200 and 5000' });
            }
            deviceSettings[imei] = { ...deviceSettings[imei], binDepthMm: Math.round(depth) };
            saveDeviceSettings();
            logDf703(`Settings for ${imei}: bin depth ${deviceSettings[imei].binDepthMm} mm`);
            return sendJson(res, 200, { imei, settings: deviceSettings[imei] });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
        }
    }
    const devMatch = req.method === 'GET' && pathname.match(/^\/api\/df703\/devices\/(\d{6,20})$/);
    if (devMatch) {
        const imei = devMatch[1];
        const state = deviceState.get(imei);
        if (!state) return sendJson(res, 404, { error: `No packets received from IMEI ${imei}` });
        return sendJson(res, 200, {
            ...state,
            connected: liveSockets.has(imei),
            pendingDownlinks: downlinkQueue.get(imei) || [],
            recentPackets: df703Packets.filter(p => p.imei === imei).slice(0, 20)
        });
    }

    // Live Driver Mobile GPS Compliance & Route Adherence Tracking + Assigned Route Sync
    if (req.method === 'GET' && pathname === '/api/driver/gps') {
        const user = getAuthenticatedUser(req);
        const tenantId = resolveTenantContext(req, res, reqUrl, user);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        const reqDrv = cleanStr(reqUrl.searchParams.get('driverId') || ts.activeDriverId || ts.drivers?.[0]?.id || 'd1', 32);
        const activeState = ts.driverStates[reqDrv] || Object.values(ts.driverStates)[0] || null;
        return sendJson(res, 200, {
            tenantId,
            activeDriverId: ts.activeDriverId,
            toleranceM: ts.preferences?.corridorToleranceM || DEFAULT_CORRIDOR_TOLERANCE_M,
            driver: activeState,
            drivers: Object.values(ts.driverStates || {})
        }, req);
    }
    if (req.method === 'POST' && pathname === '/api/driver/gps') {
        const actor = requirePermission(req, res, 'driver:gps');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        try {
            const body = await readJsonBody(req);
            const state = updateDriverGpsAndAdherence(tenantId, body, actor, ip);
            return sendJson(res, 200, { updated: true, tenantId, state }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && pathname === '/api/driver/route') {
        const user = getAuthenticatedUser(req);
        const tenantId = resolveTenantContext(req, res, reqUrl, user);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        const drvId = cleanStr(reqUrl.searchParams.get('driverId') || user?.assignedDriverId || ts.activeDriverId || ts.drivers?.[0]?.id || 'd1', 32);
        const ds = ts.driverStates[drvId] || Object.values(ts.driverStates)[0];
        return sendJson(res, 200, {
            tenantId,
            driverId: drvId,
            assignedRoute: ds?.assignedRoute || null,
            driverState: ds || null
        }, req);
    }
    if (req.method === 'POST' && pathname === '/api/driver/route') {
        const actor = requirePermission(req, res, 'routes:write');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        try {
            const body = await readJsonBody(req, 262144);
            const drvId = cleanStr(body.driverId || ts.activeDriverId || ts.drivers?.[0]?.id || 'd1', 32);
            if (!ts.driverStates[drvId]) {
                const drvMeta = ts.drivers.find(d => d.id === drvId) || { id: drvId, name: drvId, truck: 'Assigned Truck' };
                ts.driverStates[drvId] = buildDefaultDriverState(getTenantById(tenantId), drvMeta);
            }
            const ds = ts.driverStates[drvId];
            ts.activeDriverId = drvId;
            ds.assignedRoute = { ...body, assignedAt: new Date().toISOString(), assignedBy: actor.email };
            ds.plannedKm = Number(body.plannedKm) || ds.plannedKm || 16.5;
            ds.actualKm = 0;
            ds.compliancePct = 100;
            ds.onRoute = true;
            ds.distanceFromRouteM = 0;
            ds.violationActive = false;
            ds.violationReason = '';
            ds.actualTrail = [];
            ds.stopsCompleted = [];
            ds.stopsSkipped = [];
            ds.deviationsCount = 0;
            ds.onRoutePings = 0;
            ds.totalPings = 0;
            ds.events.unshift({ at: new Date().toISOString(), type: 'route_assigned', text: `Route assigned (${Array.isArray(body.stops) ? body.stops.length : 0} stops, ${ds.plannedKm} km)` });
            saveTenantsAndStates();
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'ROUTE_ASSIGNED', target: drvId, status: 'ALLOWED', ip, details: `${Array.isArray(body.stops) ? body.stops.length : 0} stops assigned (${ds.plannedKm} km)` });
            return sendJson(res, 200, { assigned: true, tenantId, driverId: drvId, route: ds.assignedRoute, driverState: ds }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/driver/stop-action') {
        const actor = requirePermission(req, res, 'driver:gps');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        try {
            const body = await readJsonBody(req);
            const drvId = cleanStr(body.driverId || actor.assignedDriverId || ts.activeDriverId || ts.drivers?.[0]?.id || 'd1', 32);
            const ds = ts.driverStates[drvId] || Object.values(ts.driverStates)[0];
            if (!ds) return sendJson(res, 404, { error: 'Driver not found' }, req);
            const action = cleanStr(body.action || 'collect', 24);
            const stopId = body.stopId !== undefined ? String(body.stopId) : null;
            const reason = cleanStr(body.reason || '', 160);
            if (action === 'collect' && stopId) {
                if (!ds.stopsCompleted.includes(stopId)) ds.stopsCompleted.push(stopId);
                ds.tankLoadPct = Math.min(100, (ds.tankLoadPct || 0) + 16);
                const bin = ts.bins.find(b => String(b.id) === stopId);
                if (bin) { bin.fill = 4; bin.angle = 0; }
                ds.events.unshift({ at: new Date().toISOString(), type: 'collected', text: `${ds.driverName} emptied stop #${stopId}` });
            } else if (action === 'skip' && stopId) {
                if (!ds.stopsSkipped.some(s => String(s.stopId) === stopId)) {
                    ds.stopsSkipped.push({ stopId, reason: reason || 'Access blocked', at: new Date().toISOString() });
                }
                ds.events.unshift({ at: new Date().toISOString(), type: 'skipped', text: `${ds.driverName} skipped stop #${stopId}: ${reason || 'Blocked'}` });
            } else if (action === 'unload') {
                ds.tankLoadPct = 0;
                ds.events.unshift({ at: new Date().toISOString(), type: 'unload', text: `${ds.driverName} unloaded truck tank at recycling hub` });
            }
            if (ds.events.length > 40) ds.events.length = 40;
            saveTenantsAndStates();
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: `DRIVER_${action.toUpperCase()}`, target: `${drvId}:${stopId || 'tank'}`, status: 'ALLOWED', ip, details: reason || action });
            return sendJson(res, 200, { ok: true, tenantId, driverState: ds }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // Full Platform Data Migration Export & Import (Tenant Scoped)
    if (req.method === 'GET' && pathname === '/api/migration/export') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        const bundle = {
            format: 'TMG-FULL-MIGRATION-V2',
            tenantId,
            exportedAt: new Date().toISOString(),
            platformVersion: '3.1.0-MULTITENANT',
            telemetryLog: telemetryLog.filter(t => (t.tenantId || 'tmg') === tenantId),
            df703Packets: df703Packets.filter(p => (p.tenantId || resolvePacketTenant(p.imei)) === tenantId),
            driverStates: ts.driverStates,
            adminPreferences: ts.preferences,
            anchors: ts.anchors,
            bins: ts.bins
        };
        bundle.sha256 = crypto.createHash('sha256').update(JSON.stringify(bundle)).digest('hex');
        logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'MIGRATION_EXPORT', target: tenantId, status: 'ALLOWED', ip, details: `SHA256=${bundle.sha256.slice(0, 12)}...` });
        return sendJson(res, 200, bundle, req);
    }

    if (req.method === 'POST' && pathname === '/api/migration/import') {
        const actor = requirePermission(req, res, 'migration:manage');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        try {
            const body = await readJsonBody(req, 1048576);
            if (!body || (!String(body.format || '').startsWith('PAROT-') && !String(body.format || '').startsWith('TMG-'))) {
                return sendJson(res, 400, { error: 'Invalid migration bundle format' }, req);
            }
            if (Array.isArray(body.df703Packets)) {
                body.df703Packets.slice(0, 200).forEach(p => { if (!df703Packets.some(x => x.id === p.id)) addEntryToMemory({ ...p, tenantId }); });
            }
            if (body.adminPreferences) Object.assign(ts.preferences, body.adminPreferences);
            if (body.driverStates && typeof body.driverStates === 'object') Object.assign(ts.driverStates, body.driverStates);
            saveTenantsAndStates();
            logAudit({ tenantId, actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'MIGRATION_IMPORT', target: tenantId, status: 'ALLOWED', ip, details: `Imported tenant ${tenantId} bundle` });
            return sendJson(res, 200, {
                migrated: true,
                tenantId,
                importedAt: new Date().toISOString(),
                packetsCount: df703Packets.filter(p => (p.tenantId || resolvePacketTenant(p.imei)) === tenantId).length,
                telemetryCount: telemetryLog.filter(t => (t.tenantId || 'tmg') === tenantId).length
            }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // 3-Month Excel-Compatible CSV History Export
    if (req.method === 'GET' && pathname === '/api/export/history.csv') {
        const user = getAuthenticatedUser(req);
        const tenantId = resolveTenantContext(req, res, reqUrl, user) || 'tmg';
        const csv = buildQuarterlyHistoryCsv();
        setSecurityAndCorsHeaders(res, 'text/csv; charset=utf-8', req);
        res.setHeader('Content-Disposition', `attachment; filename="${tenantId}_SmartWaste_History.csv"`);
        res.writeHead(200);
        return res.end(csv);
    }

    // <1-Hour Disaster Recovery Backup Snapshot & Integrity Verification
    if (req.method === 'GET' && pathname === '/api/backup/snapshot') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        const tenantId = resolveTenantContext(req, res, reqUrl, actor);
        if (!tenantId) return;
        const ts = ensureTenantState(tenantId);
        const tenantPkts = df703Packets.filter(p => (p.tenantId || resolvePacketTenant(p.imei)) === tenantId).slice(0, 100);
        const tenantTlm = telemetryLog.filter(t => (t.tenantId || 'tmg') === tenantId).slice(0, 100);
        const snapshotData = {
            format: 'TMG-DR-SNAPSHOT-V2',
            tenantId,
            createdAt: new Date().toISOString(),
            rtoTargetMinutes: 45,
            rpoTargetMinutes: 5,
            telemetryCount: tenantTlm.length,
            df703PacketsCount: tenantPkts.length,
            telemetryLog: tenantTlm,
            df703Packets: tenantPkts,
            driverStates: ts.driverStates
        };
        const digest = crypto.createHash('sha256').update(JSON.stringify(snapshotData)).digest('hex');
        return sendJson(res, 200, { ...snapshotData, sha256: digest }, req);
    }

    if (req.method === 'POST' && pathname === '/api/backup/restore') {
        const actor = requirePermission(req, res, 'backup:manage');
        if (!actor) return;
        try {
            const body = await readJsonBody(req, 524288);
            if (!body || (!String(body.format || '').startsWith('PAROT-DR-SNAPSHOT') && !String(body.format || '').startsWith('TMG-DR-SNAPSHOT'))) {
                return sendJson(res, 400, { error: 'Invalid snapshot format' }, req);
            }
            if (Array.isArray(body.df703Packets)) {
                df703Packets.splice(0, df703Packets.length, ...body.df703Packets.slice(0, 200));
            }
            if (Array.isArray(body.telemetryLog)) {
                telemetryLog.splice(0, telemetryLog.length, ...body.telemetryLog.slice(0, 200));
            }
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'DR_RESTORE', target: 'Snapshot', status: 'ALLOWED', ip, details: `Restored ${df703Packets.length} packets` });
            return sendJson(res, 200, {
                restored: true,
                restoredAt: new Date().toISOString(),
                restoredPackets: df703Packets.length,
                elapsedMinutes: 1
            }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // Static file serving with path traversal guard
    const safeRelPath = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
    const resolvedPath = path.resolve(ROOT_DIR, safeRelPath);

    if (!resolvedPath.startsWith(ROOT_DIR + path.sep) && resolvedPath !== path.join(ROOT_DIR, 'index.html')) {
        res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('Forbidden');
    }

    const baseName = path.basename(resolvedPath);
    if (resolvedPath.startsWith(path.resolve(DATA_DIR) + path.sep) || baseName === 'df703_gateway.py' ||
        baseName.startsWith('.') || baseName === 'server.js' || baseName === 'Dockerfile' || baseName === 'package.json') {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        return res.end('Not Found');
    }

    fs.stat(resolvedPath, (err, stat) => {
        if (err || !stat.isFile()) {
            res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
            return res.end('Not Found');
        }
        const ext = path.extname(resolvedPath).toLowerCase();
        setSecurityAndCorsHeaders(res, MIME_TYPES[ext] || 'application/octet-stream', req);
        const noCache = ext === '.html' || baseName === 'sw.js';
        res.setHeader('Cache-Control', noCache ? 'no-cache, no-store, must-revalidate' : 'public, max-age=3600');
        res.writeHead(200);
        fs.createReadStream(resolvedPath).pipe(res);
    });
});

// ---------------------------------------------------------------------------
// Raw TCP listener: the DF703 connects here directly over 4G (Server1 = IP;PORT;)
// ---------------------------------------------------------------------------
const tcpServer = net.createServer((socket) => {
    const peer = `${socket.remoteAddress}:${socket.remotePort}`;
    const push = createDf703Framer();
    let socketImei = null;

    tcpStats.connections++;
    tcpStats.activeConnections++;
    socket.setKeepAlive(true, 30000);
    socket.setTimeout(SOCKET_IDLE_MS);
    logDf703(`TCP connect ${peer}`);

    socket.on('data', (chunk) => {
        const { frames, junk } = push(chunk);
        if (junk.length) logDf703(`Ignored ${junk.join('').length} non-frame chars from ${peer}: ${junk.join('').slice(0, 80)}`);

        for (const frame of frames) {
            try {
                const parsed = parseDf703Packet(frame);
                const { entry, pendingDownlink } = recordParsedDf703(parsed, `Raw TCP:${TCP_PORT}`, { remoteAddress: peer });
                tcpStats.framesDecoded++;
                tcpStats.lastFrameAt = entry.receivedAt;

                if (socketImei !== parsed.imei) {
                    socketImei = parsed.imei;
                    liveSockets.set(socketImei, socket);
                }
                logDf703(`#${entry.id} ${entry.reportTypeLabel} IMEI ${parsed.imei}` +
                    (parsed.reportType === '03'
                        ? ` fw ${parsed.fwVersion} server ${parsed.serverAddress}`
                        : ` height ${parsed.heightMm}mm fill ${parsed.fillPct}% temp ${parsed.tempC}C angle ${parsed.fallAngleDeg} batt ${parsed.voltageV}V status ${parsed.alarms.rawStatus}`));

                if (pendingDownlink) {
                    socket.write(pendingDownlink.frame);
                    logDf703(`Downlink sent to ${parsed.imei}: ${pendingDownlink.frame}`);
                }
            } catch (err) {
                tcpStats.framesRejected++;
                logDf703(`Rejected frame from ${peer}: ${err.message} | ${frame.slice(0, 120)}`);
            }
        }
    });

    socket.on('timeout', () => {
        logDf703(`TCP idle timeout ${peer}`);
        socket.end();
    });
    socket.on('error', (err) => logDf703(`TCP error ${peer}: ${err.message}`));
    socket.on('close', () => {
        tcpStats.activeConnections--;
        if (socketImei && liveSockets.get(socketImei) === socket) liveSockets.delete(socketImei);
        logDf703(`TCP close ${peer}${socketImei ? ' (IMEI ' + socketImei + ')' : ''}`);
    });
});

tcpServer.on('error', (err) => {
    tcpServerActive = false;
    logDf703(`TCP listener error on port ${TCP_PORT}: ${err.message}`);
});
tcpServer.listen(TCP_PORT, HOST, () => {
    tcpServerActive = true;
    logDf703(`DF703 TCP listener on ${HOST}:${TCP_PORT} (set the sensor Server1 to YOUR_PUBLIC_IP;${TCP_PORT};)`);
});

loadTenantsAndStates();
loadUsersStore();
loadAuditLogs();
loadBackupConfig();
loadAdminPreferences();
if (tenantStates.has('tmg')) {
    Object.assign(tenantStates.get('tmg').preferences, adminPreferences);
}
loadDeviceSettings();
loadPersistedPackets();

// Automatic Local Backup Scheduler (checks every 60s)
setInterval(() => {
    if (backupConfig.schedule === 'manual') return;
    if (!backupConfig.nextRunAt || Date.now() >= Date.parse(backupConfig.nextRunAt)) {
        try {
            const created = createLocalBackup('auto-schedule', 'scheduler');
            logAudit({ actorEmail: 'system-scheduler', actorRole: 'System', action: 'AUTO_BACKUP_CREATED', target: created.filename, status: 'INFO', ip: '127.0.0.1', details: `Schedule=${backupConfig.schedule}` });
        } catch {}
    }
}, 60 * 1000);

server.on('error', (err) => {
    console.error(`HTTP server error on port ${PORT}:`, err.message);
});
server.listen(PORT, HOST, () => {
    console.log(`Smart Waste Solution - TMG server running on http://${HOST}:${PORT} (DF703 TCP on :${TCP_PORT})`);
});
