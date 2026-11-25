# PAROT WasteOS™ — Smart Waste & IoT Fleet Management Platform

Enterprise IoT platform built with **Node.js backend** and **zero-pixelation Vector GL GIS maps** for smart municipal waste management, Dingtek DF703 4G telemetry ingestion, AI route optimization, driver GPS compliance monitoring, and enterprise cloud/security governance.

---

## Key Capabilities

- **Dingtek DF703 4G Protocol V2.8 Ready:** Bit-accurate ASCII uplink decoder (`0x01` Abnormal Alert, `0x02` Heartbeat with/without IEEE-754 GPS, `0x03` Config Confirmation) + Downlink Command Builder (`80029999` + `01..0D` + `81`) + Domain & API Gateway ingestion (`ingest.parotcorp.com;5000;` -> `POST /api/df703/ingest`) with zero single-IP lock-in.
- **Wheeled Container Markers & Clean Geospatial Spacing:** Interactive MapLibre GL Vector Maps (up to Z20+ zoom clarity) displaying well-spaced wheeled municipal bin pins with real-time fill %, fire heat, fall/tilt angle, and battery status.
- **Driver Mobile GPS & Route Compliance Monitor:** Compares Planned AI Route vs Actual Driver track (`GET/POST /api/driver/gps`), flags off-route detours with wasted fuel calculation, and streams live trip narration.
- **3-Month Operating Cost ROI & Excel CSV Export:** One-click 90-day CSV export (`GET /api/export/history.csv`) with UTF-8 BOM for Arabic/English Excel compatibility plus Labor, Fuel, and Fleet Maintenance ROI proof.
- **Enterprise Cloud Costing, Cybersecurity, <1-Hour DR & SLA:** Interactive `Pay-As-You-Go` vs `Dedicated VM` estimator, dual-layer security model, `SHA-256` Disaster Recovery Snapshot/Restore (`GET /api/backup/snapshot`, `POST /api/backup/restore`), and formal P1–P4 SLA matrix.
- **Bilingual UI (English / Arabic RTL) & Light/Dark Themes.**

---

## Backend API Endpoints (`server.js`)

| Method | Endpoint | Purpose |
| :--- | :--- | :--- |
| `GET` | `/health` | Render health & TCP bridge status |
| `GET` | `/api/status` | Platform telemetry counter & recent records |
| `POST` | `/api/df703/ingest` | Parse & ingest DF703 V2.8 ASCII frames (`01`, `02`, `03`) |
| `GET` | `/api/df703/packets` | List decoded DF703 frames & pending downlinks |
| `POST` | `/api/df703/downlink` | Queue ASCII downlink command (`80029999...81`) |
| `GET/POST` | `/api/driver/gps` | Track driver GPS compliance & route deviations |
| `GET` | `/api/export/history.csv` | Download 90-day Excel-ready history CSV |
| `GET/POST` | `/api/backup/snapshot`, `/restore` | Export or verify <1-hour Disaster Recovery snapshots |
| `GET` | `/api/df703/devices` | Latest state of every DF703 that reported |
| `POST` | `/api/df703/devices/:imei/settings` | Set a sensor's bin depth, e.g. `{"binDepthMm": 1100}` (used for fill %) |

### How DF703 readings are interpreted

- **Fill %** = `(binDepthMm - distance) / binDepthMm`. Default depth `BIN_DEPTH_MM` (1200 mm); set the real depth per sensor with the settings endpoint above.
- **No echo:** a distance of `9999 mm` (`0x270F`) means the ultrasonic pulse got no echo (sensor tilted, lid open, nothing in range). It is shown as "No echo", not 0%, and the bin keeps its last valid fill.
- **Battery:** the sensor reports voltage only. The platform shows the median of the last 5 readings plus a state: `good`, `low` (< `BATT_LOW_V`, default 3.3 V, or the sensor's own low-battery bit) or `critical` (< `BATT_CRITICAL_V`, default 3.1 V).
- **Signal:** RSRP arrives as dBm x 10 (`-650` = -65.0 dBm) and is exposed as `rsrpDbm`.
- **Alarms:** the sensor's own fire / fall bits also raise the dashboard alerts.

---

## Quick Start

```bash
git clone https://github.com/SohibKhaled/parot-wasteos-platform.git
cd parot-wasteos-platform

node server.js
# Open http://127.0.0.1:3005/
```

---

**PAROT Corp** — *Smart City & Fleet Operations*
Website: https://parotcorp.com
