import os
import sys
import re
import time
import subprocess
from dotenv import load_dotenv

class CaptchaSolver:
    def __init__(self, service=None, api_key=None):
        load_dotenv()
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        # Path to the standalone OCR worker script
        self.worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gemini_ocr.py")
        self.python = sys.executable

    def solve_visually(self, image_path):
        log_path = "solver_debug.log"

        def log(msg):
            with open(log_path, "a") as f:
                f.write(f"{time.ctime()}: {msg}\n")
            print(msg, flush=True)

        if not os.path.exists(image_path):
            log(f"ERROR: Image not found: {image_path}")
            return None

        # Run gemini_ocr.py in a subprocess with proxy vars stripped out
        proxy_keys = {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "FTP_PROXY",
                      "http_proxy", "https_proxy", "all_proxy", "ftp_proxy",
                      "NO_PROXY", "no_proxy", "SOCKS_PROXY", "socks_proxy"}
        clean_env = {k: v for k, v in os.environ.items() if k not in proxy_keys}
        clean_env["GOOGLE_API_KEY"] = self.api_key

        for attempt in range(5):
            try:
                log(f"Sending to Gemini (attempt {attempt + 1}/5)...")
                t0 = time.time()
                result = subprocess.run(
                    [self.python, self.worker, image_path],
                    capture_output=True, text=True,
                    timeout=180, env=clean_env
                )
                dt = time.time() - t0

                if result.returncode != 0:
                    stderr = result.stderr.strip()
                    log(f"Worker error (exit {result.returncode}): {stderr[:400]}")

                    # Parse rate-limit retry-after from Gemini 429 error
                    if "429" in stderr or "quota" in stderr.lower() or "rate" in stderr.lower():
                        # gemini_ocr.py emits "RETRY_AFTER_SECONDS: N" on stderr
                        wait_match = re.search(r"RETRY_AFTER_SECONDS:\s*(\d+\.?\d*)", stderr)
                        if not wait_match:
                            wait_match = re.search(r"retry[_\s-]*(?:in|after)[:\s]+(\d+\.?\d*)", stderr, re.IGNORECASE)
                        if not wait_match:
                            wait_match = re.search(r'"seconds":\s*(\d+)', stderr)
                        wait_secs = float(wait_match.group(1)) if wait_match else 65
                        wait_secs = max(wait_secs, 15)  # at least 15s
                        log(f"Rate limited (429). Waiting {wait_secs:.0f}s before retry...")
                        time.sleep(wait_secs + 2)
                    else:
                        if attempt < 4:
                            time.sleep(3)
                    continue

                clean = re.sub(r"[^A-Za-z0-9]", "", result.stdout.strip())
                log(f"Gemini replied in {dt:.1f}s -> '{clean}'")
                return clean

            except subprocess.TimeoutExpired:
                log(f"Attempt {attempt + 1} timed out after 180s")
            except Exception as e:
                log(f"Attempt {attempt + 1} error: {e}")

        log("All attempts failed.")
        return None
