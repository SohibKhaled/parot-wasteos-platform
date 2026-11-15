# PAROT WasteOS™ — Smart Waste & IoT Fleet Management Platform

An enterprise IoT platform designed for smart waste monitoring, zero-pixelation vector mapping, AI-driven collection route planning, animated live driver simulation, and multi-tenant operations across Egypt, Saudi Arabia, and regional markets.

---

## Key Features

- **Custom Brand Splash & Bilingual Auth Portal:** Animated self-drawing monoline smart-bin logo (`PAROT WasteOS`) with progress preloader, workspace switcher, Light & Dark modes, and instant English / Arabic (`RTL`) switching.
- **Zero-Pixelation Vector Maps (up to Z20+):** GPU-accelerated MapLibre GL vector tiles (`OpenFreeMap Positron` light / `Carto Dark Matter Vector` dark) + Esri World Imagery & boundaries labels in Satellite mode, with custom smart-bin markers showing live fill (`%`), high-heat alerts, and tipped-over orientation.
- **Plain-Language Explainers ("Why & What To Do"):** Every bin inspector card, safety notice, AI trip plan, and top Activity Story log clearly explains sensor triggers, route decisions, and recommended actions in simple non-technical language.
- **AI Smart Routes & Return Policies:** Dynamic street routing between your configured **Starting Warehouse / Garage**, prioritized bins (fire heat, tipped, or fill threshold), and **Recycling / Unloading Hub** supporting 3 End-of-Route policies (`Recycling + Return`, `Return to Warehouse`, `End at Recycling`) with fuel & cost estimation.
- **Live Animated Driver Simulation:** Real-time moving truck along the planned route with play/pause, `1x / 2x / 5x` speed controls, closed-street detour testing, and live movement narration (`Live Movement Story`).
- **Admin Preferences Control Center:** Interactive mini-map to relocate Warehouse and Recycling coordinates by clicking, live sliders for Fire/Heat (`°C`), Pickup Fill (`%`), Tipped Angle (`°`), Low Battery (`%`) limits, and `localStorage` persistence.
- **Dual Scheduling (AI Auto + Manual Custom):** Supports both an automated AI daily/safety scheduler with optimal time-slot suggestions and a custom manual schedule builder (name, time, frequency, target zone, assigned driver).
- **Multi-Protocol IoT Ingestion & API Docs:** Sensor onboarding workflow, MQTT v5.0 / TCP Hex telemetry previews, and multi-tenant white-label customization studio.

---

## Quick Start

No build step required. Serve locally with Python:

```bash
# Clone the repository
git clone https://github.com/SohibKhaled/parot-wasteos-platform.git
cd parot-wasteos-platform

# Start local server on port 3005
python3 -m http.server 3005

# Open in your browser
open http://localhost:3005
```

---

## Repository Structure

```text
├── index.html                                        # Complete PAROT WasteOS Single-Page Application
├── proposal.html                                     # Executive Technical & Commercial Proposal (HTML)
├── starlink_proposal.html                            # Star Link Quotation #QT-000013 Proposal (HTML)
├── PAROT_Commercial_Technical_Proposal_NEXORA.pdf    # 7-page Commercial & Technical Proposal PDF
├── PAROT_StarLink_Proposal_QT000013.pdf              # Approved Star Link 3-page Proposal PDF
└── README.md                                         # Project Documentation
```

---

**PAROT Corp** — *Smart City & Fleet Operations*
Website: https://parotcorp.com
