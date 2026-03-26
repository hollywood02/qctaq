"""VPN management (OpenVPN / WireGuard) and shared PDF utilities."""
import atexit
import glob
import os
import signal
import subprocess
import threading
import time

CONFIGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
CREDS_FILE = os.path.join(CONFIGS_DIR, "proton-creds.txt")
OPENVPN_BIN = "/opt/homebrew/sbin/openvpn"
BATCH_SIZE_BEFORE_ROTATE = 18  # rotate IP before hitting DataDome's ~20 limit

# -- VPN state --

_active_vpn_proc = None   # OpenVPN subprocess
_active_vpn_conf = None   # config path (used for both ovpn and wg)
_vpn_mode = None          # "openvpn" or "wireguard"
_real_ip = None           # bare-metal IP recorded before any VPN connects
_sudo_keepalive_thread = None


def start_sudo_keepalive():
    """Start a background thread that refreshes sudo every 60s so it never expires."""
    global _sudo_keepalive_thread
    if _sudo_keepalive_thread and _sudo_keepalive_thread.is_alive():
        return

    def _keepalive():
        while True:
            try:
                subprocess.run(["sudo", "-v"], capture_output=True, timeout=5)
            except Exception:
                pass
            time.sleep(60)

    _sudo_keepalive_thread = threading.Thread(target=_keepalive, daemon=True)
    _sudo_keepalive_thread.start()


def set_vpn_mode(mode: str):
    """Set the VPN mode global."""
    global _vpn_mode
    _vpn_mode = mode


def get_vpn_mode() -> str | None:
    """Return the current VPN mode."""
    return _vpn_mode


def detect_vpn_mode(configs: list[str]) -> str:
    """Detect VPN mode based on config file extensions."""
    ovpn = [c for c in configs if c.endswith(".ovpn")]
    wg = [c for c in configs if c.endswith(".conf")]
    if ovpn and not wg:
        return "openvpn"
    if wg and not ovpn:
        return "wireguard"
    if ovpn and wg:
        print(f"Found both .ovpn ({len(ovpn)}) and .conf ({len(wg)}) files. Using OpenVPN.")
        return "openvpn"
    return "none"


def get_public_ip(timeout: int = 5) -> str | None:
    """Fetch the current public IP address. Returns None on failure."""
    for url in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
        try:
            result = subprocess.run(
                ["curl", "-s", "--max-time", str(timeout), url],
                capture_output=True, text=True, timeout=timeout + 3,
            )
            ip = result.stdout.strip()
            if ip and "." in ip:
                return ip
        except Exception:
            continue
    return None


def record_real_ip() -> str | None:
    """Record the bare-metal IP before any VPN connects. Call once at startup."""
    global _real_ip
    _real_ip = get_public_ip(timeout=5)
    return _real_ip


def get_real_ip() -> str | None:
    """Return the recorded real IP."""
    return _real_ip


def verify_vpn_ip(timeout: int = 8) -> str | None:
    """Verify that the current IP differs from the real IP.

    Returns the VPN IP on success, None if still on real IP or can't check.
    """
    ip = get_public_ip(timeout=timeout)
    if not ip:
        return None
    if _real_ip and ip == _real_ip:
        return None  # still on real IP -- VPN not working
    return ip


def check_internet(timeout: int = 5) -> bool:
    """Quick connectivity check -- ping a reliable IP (Cloudflare DNS)."""
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", str(timeout), "1.1.1.1"],
            capture_output=True, timeout=timeout + 2,
        )
        return result.returncode == 0
    except Exception:
        return False


def emergency_vpn_down():
    """Bring down VPN on any exit -- atexit + signal handler."""
    global _active_vpn_proc, _active_vpn_conf
    if _vpn_mode == "openvpn" and _active_vpn_proc:
        proc = _active_vpn_proc
        _active_vpn_proc = None
        _active_vpn_conf = None
        try:
            proc.terminate()
            proc.wait(timeout=5)
            print("\n[cleanup] OpenVPN disconnected.")
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            print("\n[cleanup] OpenVPN force-killed.")
    elif _vpn_mode == "wireguard" and _active_vpn_conf:
        conf = _active_vpn_conf
        _active_vpn_conf = None
        try:
            subprocess.run(["sudo", "wg-quick", "down", conf],
                           capture_output=True, timeout=10)
            print(f"\n[cleanup] WireGuard disconnected: {os.path.basename(conf)}")
        except Exception:
            print(f"\n[cleanup] Failed to disconnect WireGuard. Run: sudo wg-quick down {conf}")


def _signal_handler(signum, frame):
    """Handle SIGTERM/SIGINT -- clean up VPN then exit."""
    emergency_vpn_down()
    raise SystemExit(1)


atexit.register(emergency_vpn_down)
signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def get_vpn_configs():
    """Get list of VPN config files (.ovpn or .conf)."""
    configs = sorted(
        glob.glob(os.path.join(CONFIGS_DIR, "*.ovpn"))
        + glob.glob(os.path.join(CONFIGS_DIR, "*.conf"))
    )
    if not configs:
        print(f"WARNING: No .ovpn or .conf files found in {CONFIGS_DIR}")
    return configs


# -- OpenVPN --

def _refresh_sudo():
    """Refresh sudo timestamp so openvpn calls don't hang on password prompt."""
    try:
        subprocess.run(["sudo", "-vn"], capture_output=True, timeout=2)
    except Exception:
        pass


def _ovpn_up(conf: str) -> bool:
    """Start OpenVPN as a background process. Returns True if IP changed."""
    global _active_vpn_proc, _active_vpn_conf
    _refresh_sudo()
    try:
        proc = subprocess.Popen(
            ["sudo", OPENVPN_BIN, "--config", conf,
             "--auth-user-pass", CREDS_FILE,
             "--connect-retry-max", "3"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        _active_vpn_proc = proc
        _active_vpn_conf = conf

        # Wait for tunnel + verify IP actually changed
        deadline = time.time() + 25
        while time.time() < deadline:
            if proc.poll() is not None:
                _active_vpn_proc = None
                _active_vpn_conf = None
                return False
            time.sleep(2)
            if check_internet(timeout=3):
                # Internet works -- now verify IP actually changed
                vpn_ip = verify_vpn_ip(timeout=5)
                if vpn_ip:
                    print(f"  VPN IP: {vpn_ip}")
                    return True
                # Internet works but IP hasn't changed yet -- keep waiting
                continue

        # Timed out -- kill it
        print(f"  OpenVPN timeout: {os.path.basename(conf)}")
        _ovpn_down()
        return False
    except Exception as e:
        print(f"  OpenVPN start failed: {e}")
        _ovpn_down()
        return False


def _ovpn_down():
    """Stop the active OpenVPN process."""
    global _active_vpn_proc, _active_vpn_conf
    if _active_vpn_proc:
        try:
            _active_vpn_proc.terminate()
            _active_vpn_proc.wait(timeout=5)
        except Exception:
            try:
                _active_vpn_proc.kill()
            except Exception:
                pass
        _active_vpn_proc = None
    _active_vpn_conf = None


# -- WireGuard --

def _wg_up(conf: str) -> bool:
    """Bring up WireGuard interface. Returns True if internet works."""
    global _active_vpn_conf
    result = subprocess.run(["sudo", "wg-quick", "up", conf],
                            capture_output=True, timeout=15)
    if result.returncode != 0:
        print(f"  WG error: {result.stderr.decode()[:100]}")
        return False
    _active_vpn_conf = conf
    time.sleep(3)
    if not check_internet():
        print(f"  WG up but no connectivity -- rolling back: {os.path.basename(conf)}")
        _wg_down(conf)
        return False
    return True


def _wg_down(conf: str):
    """Bring down WireGuard interface."""
    global _active_vpn_conf
    try:
        subprocess.run(["sudo", "wg-quick", "down", conf],
                        capture_output=True, timeout=10)
    except Exception:
        pass
    _active_vpn_conf = None


# -- Unified VPN interface --

def vpn_switch(current_conf: str | None, next_conf: str) -> bool:
    """Switch VPN -- works for both OpenVPN and WireGuard."""
    # Bring down current
    if current_conf:
        if _vpn_mode == "openvpn":
            _ovpn_down()
        else:
            _wg_down(current_conf)
        time.sleep(1)

    # Bring up next
    if _vpn_mode == "openvpn":
        return _ovpn_up(next_conf)
    else:
        return _wg_up(next_conf)


def vpn_down(conf: str):
    """Disconnect current VPN."""
    if _vpn_mode == "openvpn":
        _ovpn_down()
    else:
        _wg_down(conf)


def kill_protonvpn_app():
    """Kill the ProtonVPN desktop app and its WireGuard system extension."""
    for proc_name in ["ProtonVPN", "ProtonVPNAgent", "ProtonVPN OpenVPN"]:
        subprocess.run(["killall", proc_name], capture_output=True, timeout=5)
    # Kill ProtonVPN's WireGuard system extension (runs as root, holds default route)
    try:
        subprocess.run(["sudo", "-n", "killall", "-9",
                         "ch.protonvpn.mac.WireGuard-Extension"],
                        capture_output=True, timeout=5)
    except (subprocess.TimeoutExpired, Exception):
        pass
    # Remove any leftover utun routes from the ProtonVPN WireGuard extension
    try:
        subprocess.run(["sudo", "-n", "route", "-n", "delete", "default",
                         "-ifscope", "utun8"],
                        capture_output=True, timeout=5)
    except (subprocess.TimeoutExpired, Exception):
        pass
    time.sleep(1)


def bring_down_all_vpns():
    """Clean up any stuck VPN from previous runs + kill ProtonVPN app."""
    global _active_vpn_proc, _active_vpn_conf
    # Kill ProtonVPN desktop app first -- it auto-connects and interferes
    kill_protonvpn_app()
    # Kill any lingering openvpn processes
    try:
        subprocess.run(["sudo", "-n", "killall", "openvpn"],
                        capture_output=True, timeout=5)
    except (subprocess.TimeoutExpired, Exception):
        pass
    # Bring down any WireGuard interfaces
    for conf in sorted(glob.glob(os.path.join(CONFIGS_DIR, "*.conf"))):
        try:
            subprocess.run(["sudo", "-n", "wg-quick", "down", conf],
                            capture_output=True, timeout=10)
        except (subprocess.TimeoutExpired, Exception):
            pass
    _active_vpn_proc = None
    _active_vpn_conf = None


# -- Shared PDF utilities --

def build_pdf_path(base_dir: str, tribunal: str, year: int, month: int, case_id: str,
                    title: str | None = None) -> str:
    if title:
        # Sanitize title for filesystem: remove/replace illegal chars
        safe_title = title.replace("/", "-").replace("\\", "-").replace(":", " -")
        safe_title = safe_title.replace('"', "").replace("*", "").replace("?", "")
        safe_title = safe_title.replace("<", "").replace(">", "").replace("|", "")
        safe_title = safe_title.strip(". ")
        # Format: "Title, 2025 QCTAQ 4689.pdf"
        ref = case_id.upper().replace("CANLII", "CanLII ")
        # Try to build a nice reference like "2025 QCTAQ 4689" or "2025 CanLII 80797"
        import re
        m = re.match(r"(\d{4})(qctaq|canlii)(\d+)", case_id, re.IGNORECASE)
        if m:
            label = m.group(2).upper()
            if label == "CANLII":
                label = "CanLII"
            ref = f"{m.group(1)} {label} {m.group(3)}"
        filename = f"{safe_title}, {ref}.pdf"
        # Truncate if too long (macOS limit ~255 bytes)
        if len(filename.encode("utf-8")) > 240:
            filename = filename[:200].rsplit(" ", 1)[0] + f"... {ref}.pdf"
        return os.path.join(base_dir, tribunal, str(year), f"{month:02d}", filename)
    return os.path.join(base_dir, tribunal, str(year), f"{month:02d}", f"{case_id}.pdf")


def is_valid_pdf(content: bytes) -> bool:
    return len(content) > 0 and content[:5] == b"%PDF-"


def is_block_response(status_code: int, content: bytes) -> bool:
    if status_code in (403, 429):
        return True
    if status_code == 200 and len(content) < 5000:
        text = content[:2000].decode("utf-8", errors="ignore").lower()
        if any(marker in text for marker in ["datadome", "captcha", "challenge", "blocked"]):
            return True
    return False
