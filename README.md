# PAROT WasteOS™ — Smart Waste & IoT Fleet Management Platform

An enterprise IoT platform designed for smart waste management, live telemetry monitoring, AI-driven collection route optimization, and multi-tenant fleet dispatch across Egypt, Saudi Arabia, and regional markets.

![Platform](https://parotcorp.com/Images/Logo.png)

---

## 🚀 Key Features

- **Multi-Protocol IoT Ingestion:** Supports Dingtek DF703 4G (TCP Socket ASCII/Hex frames) + standard MQTT v5.0 (EMQX/AWS IoT) + HTTP Webhooks & LoRaWAN.
- **Interactive GIS Map (Esri Dark Canvas):** High-precision real-time geospatial asset tracking in Cairo (Madinaty, New Capital) and Riyadh (KAFD, Olaya) with zero watermarks.
- **AI Route Optimization:** Ant-Colony & TSP clustering for bins exceeding 80% fill capacity, minimizing travel distance, fuel consumption, and carbon footprint.
- **Driver Mobile Console (PWA):** Turn-by-turn navigation and step-by-step stop completion with live map updates.
- **Multi-Tenant & White-Label Studio:** Dynamic branding switcher allowing live customization of primary colors, logos, titles, and custom domains (`iot.nexora.com`, `waste.tmg.com.eg`, `iot.parotcorp.com`).
- **High-Capacity Scalability Simulator:** Live performance benchmarking from 200 up to 10,000 sensors with sub-second ingestion latency (&lt;20ms).
- **Two-Way Remote Configuration (Downlink):** Bi-directional FOTA and threshold configuration commands transmitted to Dingtek DF703 sensors.
- **Bilingual Interface:** Seamless one-click toggle between English and Arabic (`العربية`) with full RTL support.

---

## 🛠️ Quick Start

No complex build pipeline required. Run locally using Python:

```bash
# Clone the repository
git clone <REPO_URL>
cd "Sensors App"

# Start local server on port 3000
python3 -m http.server 3000

# Open in your browser
open http://localhost:3000
```

---

## 📁 Repository Structure

```
├── index.html                                        # Full Single-Page Application (HTML5 / Tailwind / Leaflet / Chart.js)
├── proposal.html                                     # Executive Technical & Commercial Proposal (HTML template)
├── starlink_proposal.html                            # Star Link Quotation #QT-000013 Proposal (HTML template)
├── PAROT_Commercial_Technical_Proposal_NEXORA.pdf   # Complete 7-page Commercial & Technical Proposal PDF
├── PAROT_StarLink_Proposal_QT000013.pdf             # Approved Star Link 3-page Proposal PDF
└── README.md                                         # Project Documentation
```

---

## 🏢 Built & Powered by
**PAROT Corp** — *Transforming Vision into Digital Reality*  
Website: [parotcorp.com](https://parotcorp.com)
