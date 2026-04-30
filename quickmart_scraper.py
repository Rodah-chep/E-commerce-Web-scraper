import requests
from bs4 import BeautifulSoup
import json
import time
import csv
import re
from urllib.parse import urljoin

BASE_URL = "https://www.quickmart.co.ke"

# ── Cookies (from your browser session) ──────────────────────────────────────
COOKIES = {
    "PHPSESSID":     "ncfuvc6l45chh2ppa230ljdvlo",
    "_fbp":          "fb.2.1777388275060.971950891",
    "_ga":           "GA1.1.1666159676.1777388276",
    "_ga_YGT3Y1H929":"GS2.1.s1777388275$o1$g1$t1777389157$j60$l0$h0",
    "_gcl_au":       "1.1.251512229.1777388276",
    "_ygGeoAddress": "Nanyuki%2C%20Kenya",
    "_ygGeoLat":     "0.007441499999999999",
    "_ygGeoLng":     "37.0722303",
    "_ygGeoRadius":  "7",
    "_ygShopId":     "27",
}

# ── Request headers (mirroring browser) ──────────────────────────────────────
HEADERS = {
    "Accept":           "text/html, */*; q=0.01",
    "Accept-Encoding":  "gzip, deflate, br, zstd",
    "Accept-Language":  "en-US,en;q=0.9",
    "Connection":       "keep-alive",
    "Content-Type":     "application/x-www-form-urlencoded",
    "Host":             "www.quickmart.co.ke",
    "Origin":           "https://www.quickmart.co.ke",
    "Sec-Ch-Ua":        '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest":   "empty",
    "Sec-Fetch-Mode":   "cors",
    "Sec-Fetch-Site":   "same-origin",
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}


def build_url(category: str, page: int, shop_id: int = 27, page_size: int = 30) -> str:
    """
    Build the paginated product-listing URL.
    category = URL slug, e.g. 'flour', 'rice', 'cooking-oil'
    """
    return (
        f"{BASE_URL}/{category}"
        f"?page-{page}"
        f"&shop-{shop_id}"
        f"&sort-discounted-desc"
        f"&pagesize-{page_size}/"
    )


def parse_products(html: str) -> list[dict]:
    """Extract product details from a listing page."""
    soup = BeautifulSoup(html, "html.parser")
    products = []

    # Each product tile sits inside div.products.productInfoJs
    for tile in soup.select("div.products.productInfoJs"):
        try:
            # ── Name ──────────────────────────────────────────────────────────
            title_tag = tile.select_one("a.products-title")
            name = title_tag.get_text(strip=True) if title_tag else None
            relative_url = title_tag["href"] if title_tag and title_tag.get("href") else None
            product_url = urljoin(BASE_URL, relative_url) if relative_url else None

            # ── Price ─────────────────────────────────────────────────────────
            price_tag = tile.select_one("span.products-price-new")
            price_text = price_tag.get_text(strip=True) if price_tag else None
            # Strip "KES " and commas → float
            price = None
            if price_text:
                cleaned = re.sub(r"[^\d.]", "", price_text)
                price = float(cleaned) if cleaned else None

            # Old / original price (if on sale)
            old_price_tag = tile.select_one("span.products-price-old")
            old_price_text = old_price_tag.get_text(strip=True) if old_price_tag else None
            old_price = None
            if old_price_text:
                cleaned = re.sub(r"[^\d.]", "", old_price_text)
                old_price = float(cleaned) if cleaned else None

            # #offer
            # offer_tag=tile.select_one("span.products-price-off")


            # ── Image ─────────────────────────────────────────────────────────
            img_tag = tile.select_one("div.products-img img")
            image_url = img_tag["src"] if img_tag and img_tag.get("src") else None

            # ── Product ID (from the addToCart form class) ────────────────────
            form_tag = tile.select_one("form.addToCartForm")
            product_id = None
            if form_tag:
                # class is like "addToCartForm frmBuyProd-347267"
                for cls in form_tag.get("class", []):
                    match = re.search(r"frmBuyProd-(\d+)", cls)
                    if match:
                        product_id = int(match.group(1))
                        break

            # ── Stock ─────────────────────────────────────────────────────────
            qty_block = tile.select_one("div.quantityBlockJs")
            in_stock = True
            if qty_block:
                stock = qty_block.get("data-stock", "1")
                in_stock = int(stock) > 0

            products.append({
                "product_id":  product_id,
                "name":        name,
                "price_kes":   price,
                "old_price_kes": old_price,
                "in_stock":    in_stock,
                "product_url": product_url,
                "image_url":   image_url,
            })

        except Exception as e:
            print(f"  [warn] Skipped a tile due to error: {e}")

    return products


def has_next_page(html: str) -> bool:
    """Return True if a 'next page' link exists in the pagination."""
    soup = BeautifulSoup(html, "html.parser")
    return bool(soup.select_one("a.next, li.next a, a[rel='next']"))


def scrape_category(category: str, max_pages: int = 10, delay: float = 1.5) -> list[dict]:
    """
    Scrape all pages for a given category slug.
    category examples: 'flour', 'rice', 'cooking-oil', 'sugar'
    """
    all_products = []
    session = requests.Session()
    session.cookies.update(COOKIES)
    session.headers.update(HEADERS)

    for page in range(1, max_pages + 1):
        url = build_url(category, page)
        print(f"  Fetching page {page}: {url}")

        try:
            # The site uses POST for AJAX-powered navigation
            resp = session.post(url, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [error] Request failed on page {page}: {e}")
            break

        html = resp.text
        products = parse_products(html)

        if not products:
            print(f"  No products found on page {page}. Stopping.")
            break

        print(f"  Found {len(products)} products on page {page}.")
        all_products.extend(products)

        if not has_next_page(html):
            print("  No next page detected. Done.")
            break

        time.sleep(delay)   # polite crawl delay

    return all_products


def save_to_json(products: list[dict], filename: str = "products.json") -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(products)} products → {filename}")


def save_to_csv(products: list[dict], filename: str = "products.csv") -> None:
    if not products:
        print("No products to save.")
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)
    print(f"Saved {len(products)} products → {filename}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Categories to scrape — add/remove as needed
    CATEGORIES = [
        "flour",
        "rice",
        "cooking-oil",
        "sugar",
    ]

    all_results = []

    for cat in CATEGORIES:
        print(f"\n{'='*50}")
        print(f"Scraping category: {cat}")
        print(f"{'='*50}")
        products = scrape_category(cat, max_pages=10, delay=1.5)
        # Tag each product with its category
        for p in products:
            p["category"] = cat
        all_results.extend(products)

    # Save outputs
    save_to_json(all_results, "quickmart_products.json")
    save_to_csv(all_results,  "quickmart_products.csv")

    print(f"\nDone! Total products scraped: {len(all_results)}")
