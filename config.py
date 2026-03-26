import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "qctaq.db")
PDF_BASE_DIR = os.path.join(os.path.dirname(BASE_DIR), "cases_pdf")
DASHBOARD_PORT = 5056

TRIBUNALS = {
    "taq": {
        "name": "Tribunal administratif du Qu\u00e9bec",
        "url_pattern": "https://www.canlii.org/qc/qctaq/nav/date/{year}_{month}",
        "case_selector": 'a[href*="/doc/"]',
        "show_more_selector": "span.showMoreResults",
        "year_range": (2000, 2026),
    },
}


def url_to_pdf_url(url: str) -> str:
    """Convert a case HTML URL to its PDF URL."""
    if url.endswith(".html"):
        return url[:-5] + ".pdf"
    return url.rstrip("/") + ".pdf"


def url_to_case_id(url: str) -> str:
    """Extract case_id from URL. E.g. '2025canlii80797' from '.../2025canlii80797/2025canlii80797.html'"""
    parts = url.rstrip("/").split("/")
    if len(parts) >= 2:
        return parts[-2]
    return parts[-1].replace(".html", "").replace(".pdf", "")


def url_to_year_month(url: str) -> tuple[int, int]:
    """Extract (year, month) from a case URL by parsing the case_id."""
    case_id = url_to_case_id(url)
    year = int(case_id[:4])
    return year, 0
