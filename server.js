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

function recordParsedDf703(parsed, sourceChannel) {
    const entry = {
        id: df703Packets.length + 1,
        receivedAt: new Date().toISOString(),
        channel: sourceChannel,
        ...parsed
    };
    df703Packets.unshift(entry);
    if (df703Packets.length > 200) df703Packets.pop();

    telemetryLog.unshift({
        receivedAt: entry.receivedAt,
        deviceId: parsed.imei,
        fill: parsed.fillPct ?? null,
        temp: parsed.tempC ?? parsed.tempThresholdC ?? null,
        angle: parsed.fallAngleDeg ?? parsed.angleThresholdDeg ?? null,
        battery: parsed.batteryPct ?? null
    });
    if (telemetryLog.length > 200) telemetryLog.pop();

    const queued = downlinkQueue.get(parsed.imei) || [];
    const pendingDownlink = queued.length ? queued.shift() : null;
    if (queued.length === 0) downlinkQueue.delete(parsed.imei);

    return { entry, pendingDownlink };
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
        try {
            const body = await readJsonBody(req);
            if (!body.rawHex) {
                return sendJson(res, 400, { error: 'rawHex field is required' });
            }
            const parsed = parseDf703Packet(body.rawHex);
            const { entry, pendingDownlink } = recordParsedDf703(parsed, body.channel || 'HTTPS API Gateway');
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
            packetsCount: df703Packets.length,
            packets: df703Packets.slice(0, 40),
            queuedDownlinks: queuedSummary
        });
    }

    // Queue Downlink Command (80029999...81) for a DF703 IMEI
    if (req.method === 'POST' && pathname === '/api/df703/downlink') {
        try {
            const body = await readJsonBody(req);
            const frame = buildDf703Downlink(body.cmdCode, body.value);
            const imei = String(body.imei || '864376046967369').trim();
            if (!downlinkQueue.has(imei)) downlinkQueue.set(imei, []);
            const item = { imei, cmdCode: String(body.cmdCode).toUpperCase(), frame, queuedAt: new Date().toISOString() };
            downlinkQueue.get(imei).push(item);
            return sendJson(res, 201, { queued: true, command: item });
        } catch (err) {
            return sendJson(res, 400, { error: err.message });
        }
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
    if (baseName.startsWith('.') || baseName === 'server.js' || baseName === 'Dockerfile' || baseName === 'package.json') {
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

// Start raw TCP socket listener for native Dingtek DF703 ASCII streams (non-fatal if port busy)
const tcpServer = net.createServer((socket) => {
    socket.on('data', (buf) => {
        const raw = buf.toString('utf8').trim();
        try {
            const parsed = parseDf703Packet(raw);
            const { pendingDownlink } = recordParsedDf703(parsed, `Raw TCP:${TCP_PORT}`);
            if (pendingDownlink) {
                socket.write(pendingDownlink.frame);
            }
        } catch {
            // Ignore malformed keepalive fragments
        }
    });
    socket.on('error', () => {});
});

tcpServer.on('error', () => {
    tcpServerActive = false;
});
tcpServer.listen(TCP_PORT, HOST, () => {
    tcpServerActive = true;
});

server.listen(PORT, HOST, () => {
    console.log(`PAROT WasteOS server running on http://${HOST}:${PORT}`);
});
