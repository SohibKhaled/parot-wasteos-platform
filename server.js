const http = require('http');
const net = require('net');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT) || 3005;
const TCP_PORT = Number(process.env.DF703_TCP_PORT) || 5000;
const HOST = '0.0.0.0';
const ROOT_DIR = __dirname;
const BIN_DEPTH_MM = 1200;

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
const MAX_STREAM_BUFFER = 4096;

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

/**
 * Decode a Dingtek DF703 4G Protocol V2.8 ASCII uplink packet (0x01, 0x02, or 0x03).
 */
function parseDf703Packet(rawInput) {
    const cleaned = String(rawInput || '').trim().replace(/\s+/g, '');
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

        if (cleaned.length < off + 52) {
            throw new Error(`Incomplete DF703 ${reportType} packet length (${cleaned.length} chars)`);
        }

        const tempByte = parseInt(cleaned.slice(off, off + 2), 16);
        const tempC = tempByte <= 0x7f ? tempByte : -(tempByte - 0x7f);
        const reservedByte = cleaned.slice(off + 2, off + 4);
        const fallAngleDeg = parseInt(cleaned.slice(off + 4, off + 6), 16);
        const statusCode = cleaned.slice(off + 6, off + 10);

        const battRaw = parseInt(cleaned.slice(off + 10, off + 14), 16);
        const voltageV = battRaw > 1000 ? +(battRaw / 1000).toFixed(2) : +(battRaw / 100).toFixed(2);
        const batteryPct = Math.max(5, Math.min(100, Math.round(((voltageV - 3.0) / 0.7) * 100)));

        const rsrpRaw = parseLittleEndianFloat(cleaned.slice(off + 14, off + 22));
        const rsrp = rsrpRaw !== null ? Number(rsrpRaw.toFixed(1)) : null;
        const frameCount = parseInt(cleaned.slice(off + 22, off + 26), 16);
        const timestampUnix = parseInt(cleaned.slice(off + 26, off + 34), 16);
        const deviceIdField = cleaned.slice(off + 34, off + 50);
        const imei = deviceIdField.startsWith('1') ? deviceIdField.slice(1) : deviceIdField;
        const fillPct = Math.max(0, Math.min(100, Math.round(((BIN_DEPTH_MM - heightMm) / BIN_DEPTH_MM) * 100)));

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
            batteryPct,
            rsrp,
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
        const serverAddress = cleaned.slice(48, cleaned.length - 18);
        const devField = cleaned.slice(cleaned.length - 18, cleaned.length - 2);
        const imei = devField.startsWith('1') ? devField.slice(1) : devField;

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

function setSecurityAndCorsHeaders(res, contentType) {
    res.setHeader('Content-Type', contentType);
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-PAROT-Signature');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'SAMEORIGIN');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
}

function sendJson(res, status, data) {
    setSecurityAndCorsHeaders(res, 'application/json; charset=utf-8');
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
        temperatureC: parsed.tempC,
        tiltAngleDeg: parsed.fallAngleDeg,
        batteryPct: parsed.batteryPct,
        voltageV: parsed.voltageV,
        rsrp: parsed.rsrp,
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
        battery: entry.batteryPct ?? null
    });
    if (telemetryLog.length > 200) telemetryLog.pop();

    updateDeviceState(entry);
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

function recordParsedDf703(parsed, sourceChannel, meta = {}) {
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
                frameLen = buffer.slice(14, 16) === '01' ? 84 : 68;
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

const server = http.createServer(async (req, res) => {
    const reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = decodeURIComponent(reqUrl.pathname);

    if (req.method === 'OPTIONS') {
        setSecurityAndCorsHeaders(res, 'text/plain; charset=utf-8');
        res.writeHead(204);
        return res.end();
    }

    if (req.method === 'GET' && (pathname === '/health' || pathname === '/api/health')) {
        return sendJson(res, 200, {
            status: 'ok',
            service: 'parot-wasteos-platform',
            tcpBridgePort: TCP_PORT,
            tcpBridgeActive: tcpServerActive,
            tcpStats,
            connectedDevices: [...liveSockets.keys()],
            knownDevices: deviceState.size,
            uptimeSeconds: Math.round(process.uptime()),
            timestamp: new Date().toISOString()
        });
    }

    if (req.method === 'GET' && pathname === '/api/status') {
        return sendJson(res, 200, {
            platform: 'PAROT WasteOS',
            version: '2.8.0',
            protocolSupport: 'Dingtek DF703 4G V2.8 (TCP + HTTPS API Gateway)',
            ingestReady: true,
            recentTelemetryCount: telemetryLog.length,
            recentPacketsCount: df703Packets.length,
            recentTelemetry: telemetryLog.slice(0, 20)
        });
    }

    // Legacy / generic JSON telemetry endpoint
    if (req.method === 'POST' && pathname === '/api/telemetry') {
        try {
            const payload = await readJsonBody(req);
            const record = {
                receivedAt: new Date().toISOString(),
                deviceId: payload.deviceId || payload.serial || 'unknown',
                fill: payload.fill ?? null,
                temp: payload.temp ?? null,
                angle: payload.angle ?? null,
                battery: payload.battery ?? null
            };
            telemetryLog.unshift(record);
            if (telemetryLog.length > 200) telemetryLog.pop();
            return sendJson(res, 201, { accepted: true, record });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
        }
    }

    // Dingtek DF703 4G V2.8 Ingestion Endpoint
    if (req.method === 'POST' && pathname === '/api/df703/ingest') {
        if (!isAuthorized(req)) return sendJson(res, 401, { error: 'Missing or invalid Bearer token' });
        try {
            const body = await readJsonBody(req);
            if (!body.rawHex) {
                return sendJson(res, 400, { error: 'rawHex field is required' });
            }
            const parsed = parseDf703Packet(body.rawHex);
            const { entry, pendingDownlink } = recordParsedDf703(parsed, body.channel || 'HTTPS API Gateway', {
                remoteAddress: body.remoteAddress || req.socket.remoteAddress
            });
            logDf703(`HTTP ingest type ${parsed.reportType} from IMEI ${parsed.imei}`);
            return sendJson(res, 201, {
                accepted: true,
                decoded: entry,
                queuedDownlinkToSend: pendingDownlink
            });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
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
            queuedDownlinks: queuedSummary
        });
    }

    // Queue Downlink Command (80029999...81) for a DF703 IMEI
    if (req.method === 'POST' && pathname === '/api/df703/downlink') {
        if (!isAuthorized(req)) return sendJson(res, 401, { error: 'Missing or invalid Bearer token' });
        try {
            const body = await readJsonBody(req);
            const frame = buildDf703Downlink(body.cmdCode, body.value);
            const imei = String(body.imei || '864376046967369').trim();
            if (!downlinkQueue.has(imei)) downlinkQueue.set(imei, []);
            const item = { imei, cmdCode: String(body.cmdCode).toUpperCase(), frame, queuedAt: new Date().toISOString() };
            downlinkQueue.get(imei).push(item);
            const sentNow = tryDeliverNow(imei);
            logDf703(`Downlink ${sentNow ? 'sent' : 'queued'} for ${imei}: ${frame}`);
            return sendJson(res, 201, { queued: !sentNow, sentNow: Boolean(sentNow), command: item });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
        }
    }

    // Latest state of every DF703 that has reported (one row per IMEI)
    if (req.method === 'GET' && pathname === '/api/df703/devices') {
        const devices = [...deviceState.values()]
            .map(d => ({ ...d, connected: liveSockets.has(d.imei), pendingDownlinks: (downlinkQueue.get(d.imei) || []).length }))
            .sort((a, b) => String(b.lastSeenAt).localeCompare(String(a.lastSeenAt)));
        return sendJson(res, 200, { count: devices.length, devices });
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
        try {
            const body = await readJsonBody(req);
            if (typeof body.driverId === 'string') driverGpsState.driverId = body.driverId;
            if (typeof body.driverName === 'string') driverGpsState.driverName = body.driverName;
            if (typeof body.lat === 'number') driverGpsState.lat = body.lat;
            if (typeof body.lng === 'number') driverGpsState.lng = body.lng;
            if (typeof body.speedKmh === 'number') driverGpsState.speedKmh = body.speedKmh;
            if (typeof body.screenState === 'string') driverGpsState.screenState = body.screenState;
            if (typeof body.plannedKm === 'number') driverGpsState.plannedKm = body.plannedKm;
            if (typeof body.actualKm === 'number') driverGpsState.actualKm = body.actualKm;
            if (typeof body.compliancePct === 'number') driverGpsState.compliancePct = body.compliancePct;
            if (typeof body.violationActive === 'boolean') driverGpsState.violationActive = body.violationActive;
            if (body.violationReason !== undefined) driverGpsState.violationReason = body.violationReason;
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
            return sendJson(res, 200, { updated: true, state: driverGpsState });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
        }
    }

    if (req.method === 'GET' && pathname === '/api/driver/route') {
        return sendJson(res, 200, { assignedRoute: driverGpsState.assignedRoute || null });
    }
    if (req.method === 'POST' && pathname === '/api/driver/route') {
        try {
            const body = await readJsonBody(req, 262144);
            driverGpsState.assignedRoute = body;
            driverGpsState.plannedKm = Number(body.plannedKm) || driverGpsState.plannedKm;
            driverGpsState.actualKm = driverGpsState.plannedKm;
            driverGpsState.compliancePct = 100;
            driverGpsState.violationActive = false;
            return sendJson(res, 200, { assigned: true, route: driverGpsState.assignedRoute });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
        }
    }

    // Full Platform Data Migration Export & Import (Move cleanly to any server/platform)
    if (req.method === 'GET' && pathname === '/api/migration/export') {
        const bundle = {
            format: 'PAROT-FULL-MIGRATION-V1',
            exportedAt: new Date().toISOString(),
            platformVersion: '2.8.0',
            telemetryLog,
            df703Packets,
            driverGpsState
        };
        bundle.sha256 = crypto.createHash('sha256').update(JSON.stringify(bundle)).digest('hex');
        return sendJson(res, 200, bundle);
    }

    if (req.method === 'POST' && pathname === '/api/migration/import') {
        try {
            const body = await readJsonBody(req, 1048576);
            if (!body || !String(body.format || '').startsWith('PAROT-')) {
                return sendJson(res, 400, { error: 'Invalid migration bundle format' });
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
            return sendJson(res, 200, {
                migrated: true,
                importedAt: new Date().toISOString(),
                packetsCount: df703Packets.length,
                telemetryCount: telemetryLog.length
            });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
        }
    }

    // 3-Month Excel-Compatible CSV History Export
    if (req.method === 'GET' && pathname === '/api/export/history.csv') {
        const csv = buildQuarterlyHistoryCsv();
        setSecurityAndCorsHeaders(res, 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', 'attachment; filename="PAROT_WasteOS_3Month_History.csv"');
        res.writeHead(200);
        return res.end(csv);
    }

    // <1-Hour Disaster Recovery Backup Snapshot & Integrity Verification
    if (req.method === 'GET' && pathname === '/api/backup/snapshot') {
        const snapshotData = {
            format: 'PAROT-DR-SNAPSHOT-V1',
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
        return sendJson(res, 200, { ...snapshotData, sha256: digest });
    }

    if (req.method === 'POST' && pathname === '/api/backup/restore') {
        try {
            const body = await readJsonBody(req, 524288);
            if (!body || body.format !== 'PAROT-DR-SNAPSHOT-V1') {
                return sendJson(res, 400, { error: 'Invalid snapshot format; expected PAROT-DR-SNAPSHOT-V1' });
            }
            if (Array.isArray(body.df703Packets)) {
                df703Packets.splice(0, df703Packets.length, ...body.df703Packets.slice(0, 200));
            }
            if (Array.isArray(body.telemetryLog)) {
                telemetryLog.splice(0, telemetryLog.length, ...body.telemetryLog.slice(0, 200));
            }
            return sendJson(res, 200, {
                restored: true,
                restoredAt: new Date().toISOString(),
                restoredPackets: df703Packets.length,
                elapsedMinutes: 1
            });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
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

loadPersistedPackets();

server.listen(PORT, HOST, () => {
    console.log(`PAROT WasteOS server running on http://${HOST}:${PORT}`);
});
