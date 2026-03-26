#!/usr/bin/env python3
"""
Standalone Gemini OCR worker -- called as a subprocess by captcha_solver.py.
Runs in a clean environment with no Chrome/Selenium libraries loaded.
Prints the CAPTCHA characters to stdout.
"""
import sys
import os
import base64
import json
import ssl
import re
import io
from urllib import request as urllib_req


def preprocess(image_path):
    """Prepare image for Gemini. Upscale small CAPTCHAs; keep full-page screenshots as-is."""
    from PIL import Image
    img = Image.open(image_path).convert("RGB")
    w, h = img.size

    # Full-page screenshot: downscale if very large to keep payload small
    if w > 800 or h > 600:
        max_w, max_h = 1280, 900
        ratio = min(max_w / w, max_h / h, 1.0)
        if ratio < 1.0:
            img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return base64.b64encode(buf.getvalue()).decode(), "fullpage"

    # Small CAPTCHA element: upscale 3x for better OCR
    img = img.resize((w * 3, h * 3), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode(), "captcha"


def make_prompt(mode):
    if mode == "fullpage":
        return (
            "This is a screenshot of a web page that contains a CAPTCHA challenge. "
            "Find the CAPTCHA image on the page — it is a small distorted text image "
            "with 4-7 alphanumeric characters. "
            "Read only the characters inside the CAPTCHA image carefully from left to right. "
            "Reply with ONLY those characters (letters and digits), no spaces, no explanation. "
            "If you cannot find a CAPTCHA image reply with NOCAPTCHA."
        )
    return (
        "This is a CAPTCHA image. It contains 4-7 characters "
        "(a mix of lowercase letters a-z and digits 0-9). "
        "The text is slightly distorted and tilted. "
        "Read each character carefully from left to right. "
        "Reply with ONLY the characters, no spaces, no explanation."
    )


def main():
    if len(sys.argv) < 2:
        sys.exit(1)

    image_path = sys.argv[1]
    api_key = os.getenv("GOOGLE_API_KEY")
    model = "gemini-2.5-flash"

    try:
        image_b64, mode = preprocess(image_path)
    except Exception:
        # Fallback: send raw file
        with open(image_path, "rb") as f:
            image_b64 = base64.b64encode(f.read()).decode()
        mode = "captcha"

    prompt_text = make_prompt(mode)

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={api_key}"
    )
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt_text},
                {"inline_data": {"mime_type": "image/png", "data": image_b64}}
            ]
        }]
    }

    body = json.dumps(payload).encode("utf-8")
    ssl_ctx = ssl.create_default_context()
    opener = urllib_req.build_opener(
        urllib_req.HTTPSHandler(context=ssl_ctx),
        urllib_req.ProxyHandler({})
    )
    req = urllib_req.Request(
        url, data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        resp = opener.open(req, timeout=60)
    except Exception as e:
        body_err = getattr(e, 'read', lambda: b'')()
        err_body = body_err.decode('utf-8', errors='replace')
        code = getattr(e, 'code', '?')
        print(f"ERROR {code}: {err_body}", file=sys.stderr)
        # Surface retry-after for 429 so captcha_solver.py can parse it
        if str(code) == '429':
            import re as _re
            m = _re.search(r'retry[_\s-]*(?:in|after)[:\s]+(\d+\.?\d*)', err_body, _re.IGNORECASE)
            if not m:
                m = _re.search(r'"seconds":\s*(\d+)', err_body)
            if m:
                print(f"RETRY_AFTER_SECONDS: {m.group(1)}", file=sys.stderr)
        raise

    with resp:
        data = json.loads(resp.read().decode("utf-8"))

    raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    clean = re.sub(r"[^A-Za-z0-9]", "", raw)
    print(clean)


if __name__ == "__main__":
    main()
