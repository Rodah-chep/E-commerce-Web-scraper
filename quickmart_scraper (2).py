"""
╔══════════════════════════════════════════════════════════════════╗
║           Quickmart.co.ke — Interactive Product Scraper          ║
║                                                                  ║
║  Correctly handles:                                              ║
║   • Hyphen-separated URL params  (page-N, shop-N, etc.)         ║
║   • pagerecordcount token seeded from the initial GET request    ║
║   • POST-based pagination for subsequent pages                   ║
║   • Interactive CLI with sensible defaults                        ║
╚══════════════════════════════════════════════════════════════════╝

Install dependencies:
    pip install requests beautifulsoup4

Run:
    python quickmart_scraper.py                   # interactive mode
    python quickmart_scraper.py --help            # show all options
    python quickmart_scraper.py \\
        --category flour \\
        --shop-id 27 \\
        --location "Nanyuki, Kenya" \\
        --lat 0.0074 \\
        --lng 37.0722 \\
        --radius 7 \\
        --max-pages 10 \\
        --delay 1.5 \\
        --out-json products.json \\
        --out-csv  products.csv
"""

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://www.quickmart.co.ke"

# GA / fbp cookies are analytics-only; PHPSESSID is the critical session cookie.
# The scraper will obtain a fresh PHPSESSID automatically on startup.
STATIC_COOKIES: dict[str, str] = {
    "_fbp":           "fb.2.1777388275060.971950891",
    "_ga":            "GA1.1.1666159676.1777388276",
    "_ga_YGT3Y1H929": "GS2.1.s1777388275$o1$g1$t1777389157$j60$l0$h0",
    "_gcl_au":        "1.1.251512229.1777388276",
}

BASE_HEADERS: dict[str, str] = {
    "Accept":             "text/html, */*; q=0.01",
    "Accept-Encoding":    "gzip, deflate, br, zstd",
    "Accept-Language":    "en-US,en;q=0.9",
    "Connection":         "keep-alive",
    "Content-Type":       "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin":             BASE_URL,
    "Sec-Ch-Ua":          '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "Sec-Ch-Ua-Mobile":   "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "same-origin",
    "User-Agent":         (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "X-Requested-With":   "XMLHttpRequest",
}


# ─────────────────────────────────────────────────────────────────────────────
# Config dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScrapeConfig:
    category:   str                   # URL slug, e.g. "flour", "foods"
    shop_id:    int                   # branch ID, e.g. 27
    location:   str                   # human label, e.g. "Nanyuki, Kenya"
    lat:        float                 # geo latitude
    lng:        float                 # geo longitude
    radius:     int   = 7             # geo radius (km)
    currency:   int   = 7             # 7 = KES
    page_size:  int   = 30
    sort:       str   = "sort-discounted-desc"
    max_pages:  int   = 50
    delay:      float = 1.5           # seconds between requests
    out_json:   str   = "quickmart_products.json"
    out_csv:    str   = "quickmart_products.csv"
    phpsessid:  str   = ""            # filled automatically


# ─────────────────────────────────────────────────────────────────────────────
# Interactive prompt helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ask(prompt: str, default: str = "", cast=str):
    """Ask the user for input; press Enter to accept the default."""
    display_default = f"  [{default}]" if default != "" else ""
    while True:
        raw = input(f"{prompt}{display_default}: ").strip()
        value = raw if raw else str(default)
        try:
            return cast(value)
        except ValueError:
            print(f"  ⚠  Please enter a valid {cast.__name__}.")


def interactive_config() -> ScrapeConfig:
    """Walk the user through configuration interactively."""
    print("\n" + "═" * 60)
    print("  Quickmart Scraper — Setup")
    print("═" * 60)
    print("  Press Enter to accept the [default] value.\n")

    category  = _ask("Category slug (e.g. flour, foods, cooking-oil, sugar)", "flour")
    shop_id   = _ask("Shop / Branch ID", 27, int)
    location  = _ask("Location label (e.g. Nanyuki Kenya)", "Nanyuki Kenya")
    lat       = _ask("Latitude", 0.0074, float)
    lng       = _ask("Longitude", 37.0722, float)
    radius    = _ask("Geo radius (km)", 7, int)
    max_pages = _ask("Maximum pages to scrape", 50, int)
    delay     = _ask("Delay between requests (seconds)", 1.5, float)
    out_json  = _ask("Output JSON filename", "quickmart_products.json")
    out_csv   = _ask("Output CSV  filename", "quickmart_products.csv")

    print()
    return ScrapeConfig(
        category  = category,
        shop_id   = shop_id,
        location  = location,
        lat       = lat,
        lng       = lng,
        radius    = radius,
        max_pages = max_pages,
        delay     = delay,
        out_json  = out_json,
        out_csv   = out_csv,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Session bootstrap — GET page 1 to collect PHPSESSID + price range + token
# ─────────────────────────────────────────────────────────────────────────────

def build_geo_cookies(cfg: ScrapeConfig) -> dict[str, str]:
    """Build the geo-aware cookies that determine which branch's stock is shown."""
    encoded_location = quote(cfg.location, safe="")
    return {
        **STATIC_COOKIES,
        "_ygGeoAddress": encoded_location,
        "_ygGeoLat":     str(cfg.lat),
        "_ygGeoLng":     str(cfg.lng),
        "_ygGeoRadius":  str(cfg.radius),
        "_ygShopId":     str(cfg.shop_id),
    }


def bootstrap_session(session: requests.Session, cfg: ScrapeConfig) -> tuple[int, int, str]:
    """
    Perform an initial GET to the category page to:
      1. Let the server set a fresh PHPSESSID.
      2. Discover the actual price min/max for this category.
      3. Extract the first pagerecordcount token (needed for page-1 POST).

    Returns (price_min, price_max, pagerecordcount_token).
    """
    url = f"{BASE_URL}/{cfg.category}/"
    print(f"\n  🔍 Bootstrapping session via GET → {url}")

    headers = {**BASE_HEADERS}
    headers.pop("X-Requested-With", None)   # GET is not AJAX
    headers["Sec-Fetch-Dest"] = "document"
    headers["Sec-Fetch-Mode"] = "navigate"
    headers["Sec-Fetch-Site"] = "none"
    headers["Referer"]        = BASE_URL + "/"

    resp = session.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    html = resp.text

    # ── Price range ──────────────────────────────────────────────────────────
    # Embedded in a URL like: page-1&shop-27 inside pagination anchors OR in
    # the active filter breadcrumb / hidden inputs.
    price_min, price_max = _extract_price_range(html, cfg)

    # ── pagerecordcount token ────────────────────────────────────────────────
    # Present in pagination hrefs even on the very first page.
    token = _extract_token_from_html(html, current_page=0)  # page 0 → look for page-1 link
    if token is None:
        token = "0"   # safe fallback; some categories start without a token

    print(f"  ✓ Price range : KES {price_min} – {price_max}")
    print(f"  ✓ Initial token : {token[:30]}{'…' if len(token) > 30 else ''}")
    return price_min, price_max, token


def _extract_price_range(html: str, cfg: ScrapeConfig) -> tuple[int, int]:
    """
    Try several places in the page HTML to find the price min/max.
    Falls back to broad defaults (0, 99999) if nothing is found.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Method 1: from pagination/filter anchor hrefs
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        m = re.search(
            r"price-min-range-(\d+)&price-max-range-(\d+)", href
        )
        if m:
            return int(m.group(1)), int(m.group(2))

    # Method 2: from hidden form inputs
    for inp in soup.find_all("input", {"type": "hidden"}):
        name  = inp.get("name", "")
        value = inp.get("value", "")
        if "price" in name.lower() and re.match(r"\d+", value):
            pass   # extend if site uses named inputs

    # Method 3: from <script> tags (JSON config blobs)
    for script in soup.find_all("script"):
        text = script.get_text()
        m = re.search(
            r"price.{1,20}min.{1,10}[:\s]+(\d+).{1,30}max.{1,10}[:\s]+(\d+)",
            text, re.IGNORECASE
        )
        if m:
            return int(m.group(1)), int(m.group(2))

    # Default — broad enough to include everything
    print("  ⚠  Could not detect price range; using 0–99999.")
    return 0, 99999


def _extract_token_from_html(html: str, current_page: int) -> Optional[str]:
    """
    Scan all anchor hrefs for a pagerecordcount token.

    Priority:
      1. Href that contains 'page-{current_page + 1}' (exact next page).
      2. Any href with a base64-style token (len >= 8).
    """
    soup = BeautifulSoup(html, "html.parser")
    next_marker = f"page-{current_page + 1}"

    # Priority 1 — unambiguous next-page link
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if next_marker in href:
            m = re.search(r"pagerecordcount-([^&/\s]+)", href)
            if m and m.group(1) != "0":
                return m.group(1)

    # Priority 2 — generic rel/class next
    for anchor in (
        soup.select("a[rel='next']")
        + soup.select("a.next")
        + soup.select("li.next > a")
    ):
        href = anchor.get("href", "")
        m = re.search(r"pagerecordcount-([^&/\s]+)", href)
        if m and m.group(1) != "0":
            return m.group(1)

    # Priority 3 — any href with a base64-ish token
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        m = re.search(r"pagerecordcount-([A-Za-z0-9+/=]{8,})", href)
        if m:
            return m.group(1)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# URL builder
# ─────────────────────────────────────────────────────────────────────────────

def build_page_url(
    cfg:             ScrapeConfig,
    page:            int,
    price_min:       int,
    price_max:       int,
    pagerecordcount: str,
) -> str:
    """
    Builds the exact URL format used by the site:
      /{slug}?price-min-range-{min}&price-max-range-{max}&currency-{c}
              &page-{n}&shop-{s}&pagerecordcount-{token}
              &{sort}&pagesize-{ps}/
    """
    return (
        f"{BASE_URL}/{cfg.category}"
        f"?price-min-range-{price_min}"
        f"&price-max-range-{price_max}"
        f"&currency-{cfg.currency}"
        f"&page-{page}"
        f"&shop-{cfg.shop_id}"
        f"&pagerecordcount-{pagerecordcount}"
        f"&{cfg.sort}"
        f"&pagesize-{cfg.page_size}/"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Product parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_products(html: str, category: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    products = []

    for tile in soup.select("div.products.productInfoJs"):
        try:
            # ── Name + URL ─────────────────────────────────────────────────
            title_tag   = tile.select_one("a.products-title")
            name        = title_tag.get_text(strip=True) if title_tag else None
            rel_url     = title_tag.get("href")          if title_tag else None
            product_url = urljoin(BASE_URL, rel_url)     if rel_url  else None

            # ── Prices ─────────────────────────────────────────────────────
            price_tag = tile.select_one("span.products-price-new")
            price     = _parse_price(price_tag.get_text(strip=True) if price_tag else None)

            old_tag   = tile.select_one("span.products-price-old")
            old_price = _parse_price(old_tag.get_text(strip=True) if old_tag else None)

            discount_pct = tile.select_one("off.products-price-off")
            discount_pct= _parse_price(discount_pct.get_text(strip=True) if discount_pct else None)


            # discount_pct: Optional[float] = None
            # if price and old_price and old_price > price:
            #     discount_pct = round((old_price - price) / old_price * 100, 1)

            # ── Image ──────────────────────────────────────────────────────
            img_tag   = tile.select_one("div.products-img img")
            image_url = img_tag.get("src") if img_tag else None

            # ── Product ID from form class "frmBuyProd-NNNNNN" ─────────────
            form_tag   = tile.select_one("form.addToCartForm")
            product_id = None
            if form_tag:
                for cls in form_tag.get("class", []):
                    m = re.search(r"frmBuyProd-(\d+)", cls)
                    if m:
                        product_id = int(m.group(1))
                        break

            # ── Stock ──────────────────────────────────────────────────────
            qty_block = tile.select_one("div.quantityBlockJs")
            in_stock  = True
            if qty_block:
                in_stock = int(qty_block.get("data-stock", "1")) > 0

            products.append({
                "product_id":    product_id,
                "name":          name,
                "category":      category,
                "price_kes":     price,
                "old_price_kes": old_price,
                "discount_pct":  discount_pct,
                "in_stock":      in_stock,
                "product_url":   product_url,
                "image_url":     image_url,
            })

        except Exception as exc:
            print(f"    [warn] Skipped one tile: {exc}")

    return products


def _parse_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", text)
    return float(cleaned) if cleaned else None


# ─────────────────────────────────────────────────────────────────────────────
# Main scraper
# ─────────────────────────────────────────────────────────────────────────────

def scrape(cfg: ScrapeConfig) -> list[dict]:
    """
    Full scrape for one category.

    Flow:
      1. GET the category page → extract PHPSESSID, price range, token for page 1.
      2. POST page 1 using that token → parse products → extract token for page 2.
      3. POST page 2 …
      4. Stop when no token is found or no products returned.
    """
    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    session.cookies.update(build_geo_cookies(cfg))

    # ── Step 1: bootstrap ────────────────────────────────────────────────────
    try:
        price_min, price_max, pagerecordcount = bootstrap_session(session, cfg)
    except requests.RequestException as exc:
        print(f"  [error] Bootstrap failed: {exc}")
        return []

    all_products: list[dict] = []

    # ── Step 2+: paginate ────────────────────────────────────────────────────
    for page in range(1, cfg.max_pages + 1):
        url = build_page_url(cfg, page, price_min, price_max, pagerecordcount)

        # Update Referer to the previous page (mimics real browser behaviour)
        if page == 1:
            referer = f"{BASE_URL}/{cfg.category}/"
        else:
            prev_url = build_page_url(cfg, page - 1, price_min, price_max, pagerecordcount)
            referer  = prev_url
        session.headers["Referer"] = referer

        print(f"  📄 Page {page:>3}: {url}")

        try:
            resp = session.post(url, timeout=20)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  [error] Page {page} request failed: {exc}")
            break

        html = resp.text

        # Check for products
        products = parse_products(html, cfg.category)
        if not products:
            print(f"  ⛔ No products on page {page}. Stopping.")
            break

        all_products.extend(products)
        print(f"       → {len(products):>3} products  |  total so far: {len(all_products)}")

        # Extract token for NEXT page
        next_token = _extract_token_from_html(html, current_page=page)
        if next_token is None:
            print(f"  ✅ No further pages after page {page}.")
            break

        pagerecordcount = next_token
        time.sleep(cfg.delay)

    return all_products


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def save_json(products: list[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    print(f"\n  💾 JSON → {path}  ({len(products)} products)")


def save_csv(products: list[dict], path: str) -> None:
    if not products:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(products[0].keys()))
        writer.writeheader()
        writer.writerows(products)
    print(f"  💾 CSV  → {path}  ({len(products)} products)")


# ─────────────────────────────────────────────────────────────────────────────
# CLI argument parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> Optional[ScrapeConfig]:
    """
    If any CLI args are passed, parse them and return a ScrapeConfig.
    If no args are passed, return None → fall through to interactive mode.
    """
    parser = argparse.ArgumentParser(
        description="Quickmart.co.ke product scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--category",  default=None,             help="Category slug, e.g. flour")
    parser.add_argument("--shop-id",   type=int,   default=None, help="Branch/shop ID, e.g. 27")
    parser.add_argument("--location",  default=None,             help='Location label, e.g. "Nanyuki Kenya"')
    parser.add_argument("--lat",       type=float, default=None, help="Latitude")
    parser.add_argument("--lng",       type=float, default=None, help="Longitude")
    parser.add_argument("--radius",    type=int,   default=7,    help="Geo radius in km")
    parser.add_argument("--max-pages", type=int,   default=50,   help="Max pages per category")
    parser.add_argument("--delay",     type=float, default=1.5,  help="Delay between requests (s)")
    parser.add_argument("--out-json",  default="quickmart_products.json", help="JSON output path")
    parser.add_argument("--out-csv",   default="quickmart_products.csv",  help="CSV output path")

    args = parser.parse_args()

    # If no meaningful args provided, signal interactive mode
    if all(v is None for v in [args.category, args.shop_id, args.location, args.lat, args.lng]):
        return None

    # Validate required fields when using CLI mode
    missing = [
        f for f, v in [
            ("--category", args.category),
            ("--shop-id",  args.shop_id),
            ("--location", args.location),
            ("--lat",      args.lat),
            ("--lng",      args.lng),
        ] if v is None
    ]
    if missing:
        parser.error(f"The following arguments are required: {', '.join(missing)}")

    return ScrapeConfig(
        category  = args.category,
        shop_id   = args.shop_id,
        location  = args.location,
        lat       = args.lat,
        lng       = args.lng,
        radius    = args.radius,
        max_pages = args.max_pages,
        delay     = args.delay,
        out_json  = args.out_json,
        out_csv   = args.out_csv,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = parse_args()

    if cfg is None:
        # No CLI args → interactive mode
        cfg = interactive_config()

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  Scrape configuration")
    print("═" * 60)
    print(f"  Category  : {cfg.category}")
    print(f"  Shop ID   : {cfg.shop_id}")
    print(f"  Location  : {cfg.location}")
    print(f"  Lat / Lng : {cfg.lat}, {cfg.lng}  (radius {cfg.radius} km)")
    print(f"  Max pages : {cfg.max_pages}")
    print(f"  Delay     : {cfg.delay}s")
    print(f"  Output    : {cfg.out_json}  |  {cfg.out_csv}")
    print("═" * 60)

    confirm = input("\n  Start scraping? [Y/n]: ").strip().lower()
    if confirm not in ("", "y", "yes"):
        print("  Aborted.")
        sys.exit(0)

    # ── Run ───────────────────────────────────────────────────────────────────
    t0       = time.time()
    products = scrape(cfg)
    elapsed  = time.time() - t0

    if not products:
        print("\n  ⚠  No products were scraped. Check category slug, shop ID, and cookies.")
        sys.exit(1)

    # ── Save ──────────────────────────────────────────────────────────────────
    save_json(products, cfg.out_json)
    save_csv(products,  cfg.out_csv)

    print(f"\n  ✅ Done!  {len(products)} products in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
