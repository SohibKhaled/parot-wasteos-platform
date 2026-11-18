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
