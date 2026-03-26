# QCTAQ PDF Scraper

Scrapes PDF decisions from the **Tribunal administratif du Qu&eacute;bec (QCTAQ)** on CanLII.

Coverage: **2000 to present** (~26 years of Tribunal administratif du Qu&eacute;bec decisions).

---

## Architecture

1. **Collector** (`collector.py`) — browses CanLII monthly listing pages using nodriver (undetected Chrome), clicks "Show More" to load all cases, stores URLs in SQLite.
2. **Downloader** (`downloader.py` / `downloader_fast.py`) — uses Patchright (stealth Chromium) with ProtonVPN IP rotation to fetch PDFs. Rotates IP every 18 downloads to stay under DataDome's detection threshold.
3. **Captcha solver** (`captcha_solver.py` + `gemini_ocr.py`) — auto-solves text CAPTCHAs via Gemini Vision (2.5 Flash).
4. **Dashboard** (`dashboard.py`) — Flask web UI on port 5056 with live stats, year/month grid, controls, and activity log.

### URL patterns

- Monthly listing: `https://www.canlii.org/qc/qctaq/nav/date/{year}_{month}`
- Case PDF: `https://www.canlii.org/fr/qc/qctaq/doc/2025/2025canlii80797/2025canlii80797.pdf`

---

## Prerequisites (install BEFORE cloning)

### 1. Python 3.10+ (3.11 or 3.12 recommended)

> **Warning:** The codebase uses modern type syntax (`int | None`) which requires **Python 3.10+**. Using Python 3.9 or earlier will fail with `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'`. On macOS, the system `python3` is often 3.9 — make sure to install and use a newer version.

```bash
# macOS
brew install python@3.12

# Ubuntu/Debian
sudo apt install python3.12 python3.12-venv
```

### 2. OpenVPN

The scraper manages VPN tunnels directly via OpenVPN. **This is required** — the scraper refuses to run without VPN to protect your real IP.

```bash
# macOS (Apple Silicon)
brew install openvpn
# Binary will be at /opt/homebrew/sbin/openvpn

# macOS (Intel)
brew install openvpn
# Binary will be at /usr/local/sbin/openvpn
# NOTE: Update OPENVPN_BIN in vpn.py if your path differs

# Ubuntu/Debian
sudo apt install openvpn
# Binary will be at /usr/sbin/openvpn
# NOTE: Update OPENVPN_BIN in vpn.py to "/usr/sbin/openvpn"
```

### 3. Sudo timeout (IMPORTANT)

OpenVPN requires `sudo`. The scraper has a keepalive thread that refreshes sudo every 60s, but you need to **enter your password once** before starting the scraper. If your sudo timeout is too short, extend it:

```bash
sudo -v

# To extend sudo timeout to 30 minutes (recommended):
sudo visudo
# Find the line: Defaults    env_reset
# Add below it:   Defaults    timestamp_timeout=30
```

### 4. ProtonVPN (or any OpenVPN-compatible VPN)

You need a ProtonVPN account (paid plan recommended for more servers).

**Download OpenVPN configs:**

1. Go to ProtonVPN > Downloads > OpenVPN configuration files
2. Select **UDP** configs (faster than TCP)
3. Download multiple server configs (10-20 recommended for rotation)
4. Place all `.ovpn` files in `../configs/` (one directory above this repo)

**Create credentials file:**

```bash
mkdir -p ../configs
cat > ../configs/proton-creds.txt << 'EOF'
your_openvpn_username
your_openvpn_password
EOF
chmod 600 ../configs/proton-creds.txt
```

### 5. Gemini API key (for auto captcha solving)

```bash
echo "GOOGLE_API_KEY=your_key_here" > .env
```

---

## Setup

```bash
git clone https://github.com/hollywood02/qctaq.git
cd qctaq

# IMPORTANT: use python3.11 or python3.12, NOT the system python3 (which may be 3.9)
python3.11 -m venv .venv_qctaq
.venv_qctaq/bin/pip install patchright nodriver python-dotenv Pillow flask
.venv_qctaq/bin/python -m patchright install chromium

echo "GOOGLE_API_KEY=your_key_here" > .env

sudo -v
ls ../configs/*.ovpn
```

---

## Usage

```bash
# Full pipeline (collect URLs + download PDFs)
.venv_qctaq/bin/python scraper.py run --fast

# Step by step
.venv_qctaq/bin/python scraper.py collect
.venv_qctaq/bin/python scraper.py download --fast

# Monitoring
.venv_qctaq/bin/python scraper.py status
.venv_qctaq/bin/python scraper.py dashboard

# Maintenance
.venv_qctaq/bin/python scraper.py retry
.venv_qctaq/bin/python scraper.py collect --start-year 2020 --end-year 2025
```

---

## Output

PDFs are saved to:

```
../cases_pdf/taq/{YEAR}/{MM}/Title, YYYY CanLII XXXXX.pdf
```

Database: `qctaq.db` (SQLite, WAL mode).
