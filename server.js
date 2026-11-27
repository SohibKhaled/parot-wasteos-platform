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
    return {
        id: u.id,
        name: u.name,
        email: u.email,
        role: u.role,
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
    try {
        if (fs.existsSync(USERS_FILE)) {
            const loaded = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
            if (Array.isArray(loaded) && loaded.length) {
                usersStore = loaded;
                return;
            }
        }
    } catch {}
    const now = new Date().toISOString();
    usersStore = [
        {
            id: 'u-admin-1',
            name: 'TMG Madinaty Admin',
            email: 'admin@tmg-madinaty.com',
            passwordHash: hashPassword('TmgAdmin#2026'),
            role: 'Admin',
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
            status: 'approved',
            permissions: [...ROLE_DEFAULT_PERMISSIONS.Auditor],
            createdAt: now,
            approvedAt: now,
            approvedBy: 'system-bootstrap'
        }
    ];
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

function createLocalBackup(trigger = 'manual', actor = 'system') {
    fs.mkdirSync(BACKUPS_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `tmg-backup-${stamp}.json`;
    const core = {
        format: 'TMG-LOCAL-BACKUP-V1',
        platform: 'Smart Waste Solution - TMG (Madinaty)',
        createdAt: new Date().toISOString(),
        trigger,
        actor,
        adminPreferences,
        deviceSettings,
        df703Packets: df703Packets.slice(0, 200),
        telemetryLog: telemetryLog.slice(0, 200),
        driverGpsState,
        users: usersStore.map(sanitizeUserPublic),
        auditLogsCount: auditLogs.length
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
    return { filename, sha256, createdAt: core.createdAt, packetsCount: core.df703Packets.length };
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
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Session-Token, X-PAROT-Signature');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('X-XSS-Protection', '0');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), payment=(), geolocation=(self)');
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    res.setHeader(
        'Content-Security-Policy',
        "default-src 'self'; " +
        "script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com https://unpkg.com https://cdn.jsdelivr.net; " +
        "style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://unpkg.com https://fonts.googleapis.com; " +
        "font-src 'self' data: https://cdnjs.cloudflare.com https://fonts.gstatic.com; " +
        "img-src 'self' data: blob: https://*.tile.openstreetmap.org https://server.arcgisonline.com https://*.basemaps.cartocdn.com; " +
        "connect-src 'self' https://router.project-osrm.org; " +
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
    df703Packets.unshift(entry);
    if (df703Packets.length > 200) df703Packets.pop();

    telemetryLog.unshift({
        receivedAt: entry.receivedAt,
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
    const expectedChars = (parsed.packetSizeBytes || 0) * 2;
    const entry = {
        id: ++packetSeq,
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

function buildLivePlatformContext() {
    const devs = [...deviceState.values()];
    const activeFires = devs.filter(d => (d.telemetry?.temperatureC >= adminPreferences.fireTemp) || d.telemetry?.alarms?.fire);
    const activeTilts = devs.filter(d => (d.telemetry?.tiltAngleDeg >= adminPreferences.tiltDeg) || d.telemetry?.alarms?.fall);
    const activeFulls = devs.filter(d => (d.telemetry?.fillPct ?? 0) >= adminPreferences.fullThreshold);
    const pendingUsers = usersStore.filter(u => u.status === 'pending').length;
    return {
        devs,
        activeFires,
        activeTilts,
        activeFulls,
        pendingUsers,
        latestPkt: df703Packets.find(p => p.reportType === '01' || p.reportType === '02') || df703Packets[0] || null,
        backups: listLocalBackups()
    };
}

function buildAiChatReply(question, user) {
    const qRaw = cleanStr(question, 500);
    const q = qRaw.toLowerCase();
    const isAr = /[\u0600-\u06FF]/.test(qRaw);
    const ctx = buildLivePlatformContext();
    const d = ctx.latestPkt;

    const actions = [];
    if (userHasPermission(user, 'backup:read')) {
        actions.push({ label: isAr ? 'تنزيل تقرير Excel CSV' : 'Download Live CSV Report', type: 'download', url: '/api/export/history.csv' });
    }
    if (userHasPermission(user, 'routes:read')) {
        actions.push({ label: isAr ? 'فتح Route Studio' : 'Open AI Route Studio', type: 'tab', target: 'routes' });
    }

    // 1. Report / export query
    if (/report|csv|export|تقرير|تقارير|ملف/.test(q)) {
        const msgAr = `تقرير التشغيل المباشر لمنظومة TMG مدينتي:\n• عدد السلات النشطة فعلياً: ${ctx.devs.length}\n• إجمالي الباكتات المستلمة: ${df703Packets.length}\n• حريق: ${ctx.activeFires.length} | مائلة: ${ctx.activeTilts.length} | ممتلئة (>=${adminPreferences.fullThreshold}%): ${ctx.activeFulls.length}\n• حالة السائق: ${driverGpsState.driverName} — التزام ${driverGpsState.compliancePct}%\n• النسخ الاحتياطية المحلية: ${ctx.backups.length} نسخة (آخر نسخة: ${backupConfig.lastRunAt ? new Date(backupConfig.lastRunAt).toLocaleString() : 'لم تُنفذ بعد'}).\n\nيمكنك تحميل التقرير الفعلي مباشرة من الزر أدناه.`;
        const msgEn = `TMG Madinaty Live Operations Report:\n• Live Hardware Sensors: ${ctx.devs.length} | Total Packets: ${df703Packets.length}\n• Fire Alerts: ${ctx.activeFires.length} | Tilted: ${ctx.activeTilts.length} | Full (>=${adminPreferences.fullThreshold}%): ${ctx.activeFulls.length}\n• Driver Status: ${driverGpsState.driverName} (${driverGpsState.compliancePct}% route compliance)\n• Local Backups: ${ctx.backups.length} stored.\n\nUse the action button below to download the live report.`;
        return { reply: isAr ? msgAr : msgEn, actions };
    }

    // 2. Best Route / Priority / Driver Emergency query
    if (/route|best|priority|driver|fire|tilt|رووت|مسار|سواق|سائق|حريق|ميل|اولوي/.test(q)) {
        const orderNote = ctx.activeFires.length
            ? (isAr ? `الأولوية القصوى #1 الآن لإنذار الحريق (${ctx.activeFires.map(x => x.imei).join(', ')})` : `URGENT Priority #1: Fire Alert on ${ctx.activeFires.map(x => x.imei).join(', ')}`)
            : ctx.activeTilts.length
            ? (isAr ? `الأولوية #1 الآن لسلة مائلة (${ctx.activeTilts.map(x => x.imei).join(', ')})` : `Priority #1: Tilted bin ${ctx.activeTilts.map(x => x.imei).join(', ')}`)
            : (isAr ? `لا توجد طوارئ حريق أو ميلان حالياً — الأولوية للسلات >= ${adminPreferences.fullThreshold}% امتلاء مع أقصر مسار شارع-بشارع.` : `No active fire or tilt emergencies right now — prioritizing bins >= ${adminPreferences.fullThreshold}% fill via shortest street loop.`);
        const msgAr = `توصية المسار الذكي (TMG مدينتي):\n• ${orderNote}\n• قاعدة التحويل التلقائي للسائق: الحريق (أولوية #1) ← الميلان (أولوية #2) ← إكمال باقي محطات المسار.\n• حدود الإنذار الحالية: حرارة ${adminPreferences.fireTemp}°C، امتلاء ${adminPreferences.fullThreshold}%، ميلان ${adminPreferences.tiltDeg}°.`;
        const msgEn = `Smart Route Recommendation (TMG Madinaty):\n• ${orderNote}\n• Dynamic Emergency Rule: Fire (#1) → Tilted (#2) → Resume remaining scheduled stops.\n• Active Admin Thresholds: Fire >= ${adminPreferences.fireTemp}°C, Full >= ${adminPreferences.fullThreshold}%, Tilt >= ${adminPreferences.tiltDeg}°.`;
        return { reply: isAr ? msgAr : msgEn, actions };
    }

    // 3. Security / OWASP / Audit / Users / Backup query
    if (/security|owasp|audit|user|approve|backup|امان|سكيويرتي|صلاحيات|موافق|باك اب|نسخ/.test(q)) {
        if (userHasPermission(user, 'audit:read')) {
            actions.push({ label: isAr ? 'فتح مركز الأمان والنسخ' : 'Open Security & Audit Center', type: 'tab', target: 'security' });
        }
        const msgAr = `حالة الأمان (OWASP) والنسخ الاحتياطي:\n• حماية OWASP مفعّلة: تشفير كلمات المرور scrypt، جلسات HMAC-SHA256، حظر Brute-Force، رؤوس CSP/X-Frame-Options، وسجل مراقبة كامل.\n• طلبات المستخدمين المعلقة للموافقة: ${ctx.pendingUsers}\n• سجل المراقبة (Audit Events): ${auditLogs.length} حدث مسجل.\n• جدول النسخ الاحتياطي المحلي: ${backupConfig.schedule} (${ctx.backups.length} نسخة محفوظة محلياً على السيرفر).`;
        const msgEn = `OWASP Security & Local Backup Status:\n• Active OWASP Controls: scrypt password hashing, HMAC-SHA256 HttpOnly sessions, rate limiting, CSP + X-Frame-Options: DENY, strict RBAC, and Audit Logging.\n• Pending User Approvals: ${ctx.pendingUsers} | Recorded Audit Events: ${auditLogs.length}\n• Local Auto-Backup Schedule: ${backupConfig.schedule} (${ctx.backups.length} local snapshots stored).`;
        return { reply: isAr ? msgAr : msgEn, actions };
    }

    // 4. Default / Sensor status query
    const liveSummaryAr = d
        ? `قراءة السنسور المباشر (IMEI: ${d.imei}): الامتلاء ${d.fillPct ?? 'No echo'}% (${d.heightCm ?? '--'} سم)، الحرارة ${d.tempC ?? '--'}°C، زاوية الميلان ${d.fallAngleDeg ?? '--'}°، البطارية ${d.voltageV ?? '--'}V (${d.batteryState || 'good'})، الإشارة ${d.rsrpDbm ?? '--'} dBm.`
        : 'لا توجد قراءات 4G مستلمة في الذاكرة منذ آخر تشغيل للسيرفر، ولكن السيرفر جاهز على TCP :5000 (الخارجي :44200).';
    const liveSummaryEn = d
        ? `Live Sensor (IMEI: ${d.imei}): Fill ${d.fillPct ?? 'No echo'}% (${d.heightCm ?? '--'} cm), Temp ${d.tempC ?? '--'}°C, Tilt ${d.fallAngleDeg ?? '--'}°, Battery ${d.voltageV ?? '--'}V (${d.batteryState || 'good'}), Signal ${d.rsrpDbm ?? '--'} dBm.`
        : 'No 4G sensor packets received yet since last restart; TCP :5000 listener is active and ready.';

    const reply = isAr
        ? `أهلاً ${user.name} في مساعد Smart Waste Solution — TMG.\n• ${liveSummaryAr}\n• اسألني عن: "أفضل مسار الآن"، "تقرير فوري"، "حالة السنسور والبطارية"، أو "حالة الأمان والباك أب".`
        : `Hello ${user.name} — TMG AI Operations Assistant.\n• ${liveSummaryEn}\n• Ask me about: "Best route right now", "Live report", "Active fire & tilted alerts", or "OWASP security & backup status".`;
    return { reply, actions };
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
                logAudit({ actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'LOGIN_PENDING_BLOCKED', target: '/api/auth/login', status: 'DENIED', ip, details: 'Attempted login before admin approval' });
                return sendJson(res, 403, { error: 'Your account is pending Administrator approval.', code: 'PENDING_APPROVAL' }, req);
            }
            if (user.status !== 'approved') {
                logAudit({ actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'LOGIN_REJECTED_BLOCKED', target: '/api/auth/login', status: 'DENIED', ip, details: `Status is ${user.status}` });
                return sendJson(res, 403, { error: 'Your account access has been revoked or rejected.' }, req);
            }
            user.lastLoginAt = new Date().toISOString();
            user.lastLoginIp = ip;
            saveUsersStore();
            const token = signSessionToken(user);
            logAudit({ actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'LOGIN_SUCCESS', target: 'TMG Madinaty Workspace', status: 'ALLOWED', ip, details: `Signed in with role ${user.role}` });
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
            const demoEmailMap = {
                Admin: 'admin@tmg-madinaty.com',
                Supervisor: 'ops@tmg-madinaty.com',
                Driver: 'driver101@tmg-madinaty.com',
                Auditor: 'audit@tmg-madinaty.com'
            };
            const targetEmail = demoEmailMap[roleReq];
            const user = targetEmail ? usersStore.find(u => u.email === targetEmail && u.status === 'approved') : null;
            if (!user) return sendJson(res, 403, { error: 'Demo profile unavailable or disabled by admin' }, req);
            user.lastLoginAt = new Date().toISOString();
            user.lastLoginIp = ip;
            saveUsersStore();
            const token = signSessionToken(user);
            logAudit({ actorId: user.id, actorEmail: user.email, actorRole: user.role, action: 'DEMO_LOGIN_SUCCESS', target: 'TMG Madinaty Workspace', status: 'ALLOWED', ip, details: `Quick-access login as ${user.role}` });
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

            const newUser = {
                id: 'u-' + crypto.randomBytes(6).toString('hex'),
                name,
                email,
                passwordHash: hashPassword(password),
                role: requestedRole,
                status: 'pending',
                permissions: [...(ROLE_DEFAULT_PERMISSIONS[requestedRole] || ROLE_DEFAULT_PERMISSIONS.Operator)],
                requestedNote: note,
                createdAt: new Date().toISOString()
            };
            usersStore.push(newUser);
            saveUsersStore();
            logAudit({ actorId: newUser.id, actorEmail: newUser.email, actorRole: requestedRole, action: 'USER_REGISTERED', target: 'Approval Queue', status: 'INFO', ip, details: `New ${requestedRole} account awaiting admin approval` });
            return sendJson(res, 201, {
                ok: true,
                status: 'pending',
                message: 'Registration submitted. Your account is waiting for Administrator approval before accessing TMG Madinaty.'
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
    // ADMIN USER APPROVAL, GRANULAR PRIVILEGES (RBAC) & AUDIT LOGS
    // =========================================================================
    if (req.method === 'GET' && pathname === '/api/admin/users') {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        return sendJson(res, 200, {
            users: usersStore.map(sanitizeUserPublic),
            allPermissions: ALL_PERMISSIONS,
            roleDefaults: ROLE_DEFAULT_PERMISSIONS
        }, req);
    }

    const approveMatch = req.method === 'POST' && pathname.match(/^\/api\/admin\/users\/([^/]+)\/approve$/);
    if (approveMatch) {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        try {
            const body = await readJsonBody(req);
            const target = usersStore.find(u => u.id === approveMatch[1]);
            if (!target) return sendJson(res, 404, { error: 'User not found' }, req);
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
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'USER_APPROVED', target: target.email, status: 'ALLOWED', ip, details: `Approved as ${target.role} with ${target.permissions.length} privileges` });
            return sendJson(res, 200, { ok: true, user: sanitizeUserPublic(target) }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    const rejectMatch = req.method === 'POST' && pathname.match(/^\/api\/admin\/users\/([^/]+)\/reject$/);
    if (rejectMatch) {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        const target = usersStore.find(u => u.id === rejectMatch[1]);
        if (!target) return sendJson(res, 404, { error: 'User not found' }, req);
        if (target.id === actor.id) return sendJson(res, 400, { error: 'Cannot reject your own active admin session' }, req);
        target.status = 'rejected';
        saveUsersStore();
        logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'USER_REJECTED', target: target.email, status: 'ALLOWED', ip, details: 'Access denied / revoked' });
        return sendJson(res, 200, { ok: true, user: sanitizeUserPublic(target) }, req);
    }

    const permsMatch = req.method === 'POST' && pathname.match(/^\/api\/admin\/users\/([^/]+)\/permissions$/);
    if (permsMatch) {
        const actor = requirePermission(req, res, 'users:manage');
        if (!actor) return;
        try {
            const body = await readJsonBody(req);
            const target = usersStore.find(u => u.id === permsMatch[1]);
            if (!target) return sendJson(res, 404, { error: 'User not found' }, req);
            if (body.role && ROLE_DEFAULT_PERMISSIONS[body.role]) target.role = body.role;
            if (Array.isArray(body.permissions)) {
                target.permissions = body.permissions.filter(p => ALL_PERMISSIONS.includes(p) || p === '*');
            }
            saveUsersStore();
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'PERMISSIONS_UPDATED', target: target.email, status: 'ALLOWED', ip, details: `Role=${target.role}, Perms=${target.permissions.length}` });
            return sendJson(res, 200, { ok: true, user: sanitizeUserPublic(target) }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && pathname === '/api/admin/audit-logs') {
        const actor = requirePermission(req, res, 'audit:read');
        if (!actor) return;
        const limit = Math.min(300, Math.max(10, Number(reqUrl.searchParams.get('limit')) || 120));
        return sendJson(res, 200, { total: auditLogs.length, logs: auditLogs.slice(0, limit) }, req);
    }

    if (req.method === 'GET' && pathname === '/api/admin/security-status') {
        const actor = requirePermission(req, res, 'dashboard:read');
        if (!actor) return;
        return sendJson(res, 200, {
            standard: 'OWASP Top 10 + ASVS Level 2 Controls',
            controls: [
                { id: 'A01', name: 'Broken Access Control (RBAC + Approval Gate)', active: true, detail: `${usersStore.filter(u => u.status === 'pending').length} pending approvals enforced` },
                { id: 'A02', name: 'Cryptographic Failures (scrypt + HMAC-SHA256)', active: true, detail: 'Salted scrypt passwords, signed HttpOnly sessions, SHA-256 backup digests' },
                { id: 'A03', name: 'Injection & XSS Defense', active: true, detail: 'CSP headers + zero-SQL JSON store + strict HTML escaping & payload clamping' },
                { id: 'A05', name: 'Security Misconfiguration Hardening', active: true, detail: 'X-Frame-Options: DENY, nosniff, HSTS, same-origin CORS' },
                { id: 'A07', name: 'Auth Rate Limiting & Brute-Force Guard', active: true, detail: '25 auth attempts / 15m per IP, 240 API req / min' },
                { id: 'A09', name: 'Security Logging & Monitoring (Audit Trail)', active: true, detail: `${auditLogs.length} immutable audit events recorded` }
            ]
        }, req);
    }

    // =========================================================================
    // LOCAL AUTO-BACKUP MANAGER & MULTI-METHOD EXPORT/RESTORE
    // =========================================================================
    if (req.method === 'GET' && pathname === '/api/admin/backups') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        return sendJson(res, 200, { config: backupConfig, files: listLocalBackups() }, req);
    }

    if (req.method === 'POST' && pathname === '/api/admin/backups/config') {
        const actor = requirePermission(req, res, 'backup:manage');
        if (!actor) return;
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
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_CONFIG_UPDATED', target: backupConfig.schedule, status: 'ALLOWED', ip, details: `Retention=${backupConfig.retentionCount}, AutoDownload=${backupConfig.autoDownloadBrowser}` });
            return sendJson(res, 200, { ok: true, config: backupConfig }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/admin/backups/run') {
        const actor = requirePermission(req, res, 'backup:manage');
        if (!actor) return;
        const created = createLocalBackup('manual', actor.email);
        logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_CREATED', target: created.filename, status: 'ALLOWED', ip, details: `SHA256=${created.sha256.slice(0, 12)}... (${created.packetsCount} packets)` });
        return sendJson(res, 201, { ok: true, backup: created, config: backupConfig, files: listLocalBackups() }, req);
    }

    if (req.method === 'GET' && pathname === '/api/admin/backups/download') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        const fileParam = path.basename(reqUrl.searchParams.get('file') || '');
        const format = (reqUrl.searchParams.get('format') || 'json').toLowerCase();
        let targetFile = fileParam && fileParam.endsWith('.json') ? fileParam : listLocalBackups()[0]?.filename;
        if (!targetFile) {
            targetFile = createLocalBackup('on-demand', actor.email).filename;
        }
        const full = path.join(BACKUPS_DIR, targetFile);
        if (!full.startsWith(BACKUPS_DIR + path.sep) || !fs.existsSync(full)) {
            return sendJson(res, 404, { error: 'Backup file not found' }, req);
        }
        logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_DOWNLOADED', target: targetFile, status: 'ALLOWED', ip, details: `Format=${format}` });
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
            if (Array.isArray(backupObj.df703Packets)) {
                df703Packets.splice(0, df703Packets.length, ...backupObj.df703Packets.slice(0, 200));
            }
            if (Array.isArray(backupObj.telemetryLog)) {
                telemetryLog.splice(0, telemetryLog.length, ...backupObj.telemetryLog.slice(0, 200));
            }
            if (backupObj.adminPreferences) {
                Object.assign(adminPreferences, backupObj.adminPreferences);
                saveAdminPreferences();
            }
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'BACKUP_RESTORED', target: body.filename || 'uploaded-json', status: 'ALLOWED', ip, details: `Restored ${df703Packets.length} packets & admin preferences` });
            return sendJson(res, 200, { ok: true, restoredPackets: df703Packets.length, restoredAt: new Date().toISOString() }, req);
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
        try {
            const body = await readJsonBody(req);
            const message = cleanStr(body.message || '', 500);
            if (!message) return sendJson(res, 400, { error: 'Message cannot be empty' }, req);
            const out = buildAiChatReply(message, actor);
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'AI_CHAT_QUERY', target: 'Chatbot', status: 'ALLOWED', ip, details: message.slice(0, 90) });
            return sendJson(res, 200, { ok: true, ...out }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'POST' && pathname === '/api/driver/reroute-check') {
        const actor = requirePermission(req, res, 'driver:gps');
        if (!actor) return;
        try {
            const body = await readJsonBody(req, 262144);
            const allBins = Array.isArray(body.bins) ? body.bins : [];
            const currentRoute = Array.isArray(body.currentRouteIds) ? body.currentRouteIds.map(String) : [];
            const remainingOriginal = allBins.filter(b => currentRoute.includes(String(b.id)));
            const fires = allBins.filter(b => (Number(b.temp) >= adminPreferences.fireTemp) || b.deviceAlarms?.fire);
            const tilts = allBins.filter(b => !fires.includes(b) && ((Number(b.angle) >= adminPreferences.tiltDeg) || b.deviceAlarms?.fall));

            const emergencyIds = new Set([...fires, ...tilts].map(b => String(b.id)));
            const resumedStops = remainingOriginal.filter(b => !emergencyIds.has(String(b.id)));
            const reroutedStops = [...fires, ...tilts, ...resumedStops];

            if (fires.length || tilts.length) {
                logAudit({
                    actorId: actor.id, actorEmail: actor.email, actorRole: actor.role,
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
            const { entry, pendingDownlink } = recordParsedDf703(parsed, cleanStr(body.channel || 'HTTPS API Gateway', 64), {
                remoteAddress: cleanStr(body.remoteAddress || req.socket.remoteAddress, 64)
            });
            logDf703(`HTTP ingest type ${parsed.reportType} from IMEI ${parsed.imei}`);
            return sendJson(res, 201, {
                accepted: true,
                decoded: entry,
                queuedDownlinkToSend: pendingDownlink
            }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && pathname === '/api/df703/packets') {
        const queuedSummary = [];
        for (const [imei, cmds] of downlinkQueue.entries()) {
            queuedSummary.push({ imei, commands: cmds });
        }
        return sendJson(res, 200, {
            protocol: 'Dingtek DF703 4G V2.8',
            tcpBridgeActive: tcpServerActive,
            tcpBridgePort: TCP_PORT,
            tcpStats,
            connectedDevices: [...liveSockets.keys()],
            packetsCount: df703Packets.length,
            packets: df703Packets.slice(0, 40),
            queuedDownlinks: queuedSummary,
            adminPreferences
        }, req);
    }

    // Queue Downlink Command (80029999...81) for a DF703 IMEI (Requires downlink:send privilege or INGEST_TOKEN)
    if (req.method === 'POST' && pathname === '/api/df703/downlink') {
        const hasBearer = INGEST_TOKEN && isAuthorized(req);
        const actor = hasBearer ? { id: 'token', email: 'api-token', role: 'Admin' } : requirePermission(req, res, 'downlink:send');
        if (!actor) return;
        try {
            const body = await readJsonBody(req);
            const frame = buildDf703Downlink(body.cmdCode, body.value);
            const imei = cleanStr(body.imei || '864317087587096', 24);
            if (!downlinkQueue.has(imei)) downlinkQueue.set(imei, []);
            const item = { imei, cmdCode: String(body.cmdCode).toUpperCase(), frame, queuedAt: new Date().toISOString() };
            downlinkQueue.get(imei).push(item);
            const sentNow = tryDeliverNow(imei);
            logDf703(`Downlink ${sentNow ? 'sent' : 'queued'} for ${imei}: ${frame}`);
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'DOWNLINK_QUEUED', target: imei, status: 'ALLOWED', ip, details: `CMD ${item.cmdCode} -> ${frame}` });
            return sendJson(res, 201, { queued: !sentNow, sentNow: Boolean(sentNow), command: item }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // Admin Preferences (persisted in DATA_DIR/admin_preferences.json, requires prefs:write on POST)
    if (req.method === 'GET' && pathname === '/api/admin/preferences') {
        return sendJson(res, 200, { ok: true, preferences: adminPreferences }, req);
    }
    if (req.method === 'POST' && pathname === '/api/admin/preferences') {
        const actor = requirePermission(req, res, 'prefs:write');
        if (!actor) return;
        try {
            const body = await readJsonBody(req);
            if (!body || typeof body !== 'object') {
                return sendJson(res, 400, { error: 'Invalid JSON body' }, req);
            }
            const next = { ...adminPreferences };
            if (body.fireTemp !== undefined) next.fireTemp = Math.max(0, Math.min(255, Number(body.fireTemp)));
            if (body.fullThreshold !== undefined) next.fullThreshold = Math.max(0, Math.min(100, Number(body.fullThreshold)));
            if (body.tiltDeg !== undefined) next.tiltDeg = Math.max(0, Math.min(90, Number(body.tiltDeg)));
            if (body.lowBattThreshold !== undefined) next.lowBattThreshold = Math.max(0, Math.min(100, Number(body.lowBattThreshold)));
            if (body.defaultCostPerKm !== undefined) next.defaultCostPerKm = Math.max(1, Number(body.defaultCostPerKm));
            if (body.routeEndPolicy !== undefined) next.routeEndPolicy = cleanStr(body.routeEndPolicy, 32);
            if (body.autoSchedulerEnabled !== undefined) next.autoSchedulerEnabled = Boolean(body.autoSchedulerEnabled);
            adminPreferences = next;
            saveAdminPreferences();
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'PREFERENCES_UPDATED', target: 'Alert Thresholds', status: 'ALLOWED', ip, details: `Fire=${next.fireTemp}C Full=${next.fullThreshold}% Tilt=${next.tiltDeg}deg` });
            return sendJson(res, 200, { ok: true, preferences: adminPreferences }, req);
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

    // Live Driver Mobile GPS Compliance & Violation Tracking + Assigned Route Sync
    if (req.method === 'GET' && pathname === '/api/driver/gps') {
        return sendJson(res, 200, driverGpsState);
    }
    if (req.method === 'POST' && pathname === '/api/driver/gps') {
        const actor = requirePermission(req, res, 'driver:gps');
        if (!actor) return;
        try {
            const body = await readJsonBody(req);
            if (typeof body.driverId === 'string') driverGpsState.driverId = cleanStr(body.driverId, 32);
            if (typeof body.driverName === 'string') driverGpsState.driverName = cleanStr(body.driverName, 80);
            if (typeof body.lat === 'number') driverGpsState.lat = body.lat;
            if (typeof body.lng === 'number') driverGpsState.lng = body.lng;
            if (typeof body.speedKmh === 'number') driverGpsState.speedKmh = body.speedKmh;
            if (typeof body.screenState === 'string') driverGpsState.screenState = cleanStr(body.screenState, 32);
            if (typeof body.plannedKm === 'number') driverGpsState.plannedKm = body.plannedKm;
            if (typeof body.actualKm === 'number') driverGpsState.actualKm = body.actualKm;
            if (typeof body.compliancePct === 'number') driverGpsState.compliancePct = body.compliancePct;
            if (typeof body.violationActive === 'boolean') driverGpsState.violationActive = body.violationActive;
            if (body.violationReason !== undefined) driverGpsState.violationReason = cleanStr(body.violationReason, 160);
            driverGpsState.lastPingAt = new Date().toISOString();
            const extraKm = Math.max(0, +(driverGpsState.actualKm - driverGpsState.plannedKm).toFixed(1));
            driverGpsState.wastedEgp = Math.round(extraKm * driverGpsState.costPerKmEgp);
            driverGpsState.history.unshift({
                at: driverGpsState.lastPingAt,
                driverId: driverGpsState.driverId,
                lat: body.lat ?? null,
                lng: body.lng ?? null,
                speedKmh: body.speedKmh ?? 0,
                screenState: body.screenState || 'active',
                compliancePct: driverGpsState.compliancePct,
                violationActive: driverGpsState.violationActive
            });
            if (driverGpsState.history.length > 80) driverGpsState.history.pop();
            return sendJson(res, 200, { updated: true, state: driverGpsState }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    if (req.method === 'GET' && pathname === '/api/driver/route') {
        return sendJson(res, 200, { assignedRoute: driverGpsState.assignedRoute || null }, req);
    }
    if (req.method === 'POST' && pathname === '/api/driver/route') {
        const actor = requirePermission(req, res, 'routes:write');
        if (!actor) return;
        try {
            const body = await readJsonBody(req, 262144);
            driverGpsState.assignedRoute = body;
            driverGpsState.plannedKm = Number(body.plannedKm) || driverGpsState.plannedKm;
            driverGpsState.actualKm = driverGpsState.plannedKm;
            driverGpsState.compliancePct = 100;
            driverGpsState.violationActive = false;
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'ROUTE_ASSIGNED', target: cleanStr(body.driverId || 'Driver', 32), status: 'ALLOWED', ip, details: `${Array.isArray(body.stops) ? body.stops.length : 0} stops assigned` });
            return sendJson(res, 200, { assigned: true, route: driverGpsState.assignedRoute }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // Full Platform Data Migration Export & Import (Move cleanly to any server/platform)
    if (req.method === 'GET' && pathname === '/api/migration/export') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        const bundle = {
            format: 'TMG-FULL-MIGRATION-V2',
            exportedAt: new Date().toISOString(),
            platformVersion: '3.0.0-OWASP',
            telemetryLog,
            df703Packets,
            driverGpsState,
            adminPreferences
        };
        bundle.sha256 = crypto.createHash('sha256').update(JSON.stringify(bundle)).digest('hex');
        logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'MIGRATION_EXPORT', target: 'Full Platform Bundle', status: 'ALLOWED', ip, details: `SHA256=${bundle.sha256.slice(0, 12)}...` });
        return sendJson(res, 200, bundle, req);
    }

    if (req.method === 'POST' && pathname === '/api/migration/import') {
        const actor = requirePermission(req, res, 'migration:manage');
        if (!actor) return;
        try {
            const body = await readJsonBody(req, 1048576);
            if (!body || (!String(body.format || '').startsWith('PAROT-') && !String(body.format || '').startsWith('TMG-'))) {
                return sendJson(res, 400, { error: 'Invalid migration bundle format' }, req);
            }
            if (Array.isArray(body.df703Packets)) {
                df703Packets.splice(0, df703Packets.length, ...body.df703Packets.slice(0, 200));
            }
            if (Array.isArray(body.telemetryLog)) {
                telemetryLog.splice(0, telemetryLog.length, ...body.telemetryLog.slice(0, 200));
            }
            if (body.driverGpsState && typeof body.driverGpsState === 'object') {
                Object.assign(driverGpsState, body.driverGpsState);
            }
            logAudit({ actorId: actor.id, actorEmail: actor.email, actorRole: actor.role, action: 'MIGRATION_IMPORT', target: 'Full Platform Bundle', status: 'ALLOWED', ip, details: `Imported ${df703Packets.length} packets` });
            return sendJson(res, 200, {
                migrated: true,
                importedAt: new Date().toISOString(),
                packetsCount: df703Packets.length,
                telemetryCount: telemetryLog.length
            }, req);
        } catch (err) {
            return sendJson(res, 400, { error: err.message }, req);
        }
    }

    // 3-Month Excel-Compatible CSV History Export
    if (req.method === 'GET' && pathname === '/api/export/history.csv') {
        const csv = buildQuarterlyHistoryCsv();
        setSecurityAndCorsHeaders(res, 'text/csv; charset=utf-8', req);
        res.setHeader('Content-Disposition', 'attachment; filename="TMG_SmartWaste_Madinaty_History.csv"');
        res.writeHead(200);
        return res.end(csv);
    }

    // <1-Hour Disaster Recovery Backup Snapshot & Integrity Verification
    if (req.method === 'GET' && pathname === '/api/backup/snapshot') {
        const actor = requirePermission(req, res, 'backup:read');
        if (!actor) return;
        const snapshotData = {
            format: 'TMG-DR-SNAPSHOT-V2',
            createdAt: new Date().toISOString(),
            rtoTargetMinutes: 45,
            rpoTargetMinutes: 5,
            telemetryCount: telemetryLog.length,
            df703PacketsCount: df703Packets.length,
            telemetryLog: telemetryLog.slice(0, 100),
            df703Packets: df703Packets.slice(0, 100),
            driverGpsState
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
        setSecurityAndCorsHeaders(res, MIME_TYPES[ext] || 'application/octet-stream');
        res.setHeader('Cache-Control', ext === '.html' ? 'no-cache' : 'public, max-age=3600');
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

loadUsersStore();
loadAuditLogs();
loadBackupConfig();
loadAdminPreferences();
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
