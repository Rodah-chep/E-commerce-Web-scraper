"""
Carrefour Kenya Product Scraper
Uses curl_cffi to impersonate Chrome's TLS fingerprint and bypass Akamai bot detection.

Install:
    pip install curl_cffi
"""

import json
import time
import csv
from typing import Optional

from curl_cffi import requests  # drop-in replacement — same API, real Chrome TLS


# ── Session cookies ────────────────────────────────────────────────────────────
# These Akamai tokens (_abck, ak_bmsc, bm_sv, bm_sz) expire in 2–4 hrs.
# Refresh them by opening DevTools → Network → copy the Cookie header from any
# request to www.carrefour.ke and paste the values back here.
COOKIES = {
    "cart_api": "v2",
    "__privaci_cookie_consent_uuid": "b47e6e92-d365-4b00-a629-fe84f90cb875:2",
    "__privaci_cookie_consent_generated": "b47e6e92-d365-4b00-a629-fe84f90cb875:2",
    "__privaci_cookie_consents": (
        '{"consents":{"97":1,"98":1,"99":1,"100":1},"location":"30#KE",'
        '"lang":"en","gpcInBrowserOnConsent":false,"gpcStatusInPortalOnConsent":false,'
        '"status":"record-consent-success","implicit_consent":false,'
        '"gcm":{"1":1,"2":1,"3":1,"4":1,"5":1,"6":1,"7":1}}'
    ),
    "__privaci_latest_published_version": "4",
    "maf-session-id": "9066AE59-C91A-4E49-BDAA-7A86E36E2B36",
    "AKA_A2": "A",
    "mafken-preferred-delivery-area": "Westlands - Nairobi",
    "JSESSIONID": "00C952BE03ABD3007CED9D222679CD9E",
    "storeInfo": "mafken|en|KES",
    "posInfo": "food=684_Zone01,express=KE4_Zone01,nonfood=681_Zone01",
    "tabInfo": "SLOTTED|DEFAULT|MKP_GLOBAL#ANY,EXPRESS|QELEC|QMKP#ANY",
    # ── Akamai bot-detection tokens (short-lived — refresh these often) ─────────
    "_abck": "A3D4E19809BA676925549E87696CCD36~0~YAAQRqERAjweb2adAQAA98hWhg8a/G3u6jj/LikpuRdrurYtKxUlWxbo0W7YQiIpPOp1Up88/g5DGrNKKE8l5ggXqjXmbJ2cD5u7+kXI/s5rxMi9G8ZCs1+hq2D8izoCaba5smdHzIRvyxrBAwHj0VGNHLUoUdZf/8hzg5S5j+CDD6+Z9LLnfVAvCKkwSSDDPjbVxl/9+ay7r2N46uUHsUZxLxvOCFFcTOcT3FTgjyciY/GTOkmtWb943icSUii526dEit/pxc9i5jNMVJTsIQ4aqlSTHrXIEyBqRtZa0X+DgNui/ERl4zJCetEJ5FjC4VJ2i4IFBfw5tXXfuT1doOmni1zUlol77bEEJTyv625JOf5zALBTjAPYM5Hbdbwi48E13bbToNtYx0N91O2i7sQpfPtQGDJiuwVSspF7DEmfDx2dWZ9fxVjoE/IG+6NJ2jD7bdILivyDK4Ee6q1vo6habpn66tp3FfCiN15jk7xq55wEVQHJKqGRGTVzklTVh0ddtQ+vLzHtfr+zKbdfm7E82+NE0qisK5c/SFmOx5LhLALz/gEwqjVK/XGIKzhbRrvsE9e/MMrvrw9miPBkJ57mb2YQaC6UyYWwlcyRYfcUkoU7iSHw98zkK6FHTO4/GNhkyBTes9i+tA==~-1~-1~-1~AAQAAAAF%2f%2f%2f%2f%2f27wXG8brmuB6fmvuVzRfb3Bb1kSdG0qOqnGClLi31orcfvYPPmPpFna%2fnGsXJQR88z4c7duakMQNVaExFhltdUzKso9djYrgFNi~-1",
    "ak_bmsc": "7751C3A66107F6657F7940E63A37F85A~000000000000000000000000000000~YAAQRqERAj0eb2adAQAA98hWhh9yxvGKnc4dwDHjjCtQNRa60gYPerNtdtwepSova1/EzPDdIPFnqKIYO03M0yFkMNvL5RRpPggXekxuyNVUWB8n7eOsPlwieM1B9pnT4OJqgke7S/IE62bbAtssRl0lcyyUylBmTb2uxao4kCDDk9OimX64qw0KMpyMy0lYQmSBCjp9ujN/QOh7H6jt67O4VJRsUcA5oJbSlKUxeJ6Ywt4grKnw3g+C6vQopA8fYgZXTxedRDGGDXTDVWKU0yR9DE+iNkFyjnD4Z4t5NzXT8HwHsduD5Mvj3wJsQxrAEeuNlDd4JifUn6UoRV/4Z8/UvCKrwG09lv/RkbopqxzlFa4FRYae+FYsf4fZRK3bqeJLvl2gtLP+ie29TX2qVj9/DL1RIKbAhZ+pBhJh1vUpHTZUV6/AYPKruQbHInVOSTMsALiEiAJXkxCsHWmj",
    "bm_sv": "75126D67C6157D0B63C7B864FACB02E0~YAAQRqERAj4eb2adAQAA98hWhh8tOF9I90IvHpc1STFkZlmfWAZlxHKnV9bp6lFCMdSxPNMFRctF8xY8PC4T8avyVmjBuZso/CeZE3BM0Iot/MdYii2ZDeFqka1d2DS0akiUF4kauBOBwndPzwgIbfbA8f923xpy/fAVWLAKrCMfTRgGGLdy6B2rOlqmZGGSjoMAweMjMySuC3IpKd/4XGWyGf50ZEmIhNJxNWvvqm/EpMpNTrXAc6ahrQ2yI6ys30euMO5y~1",
    "bm_sz": "E391B2F8130D620E4603EA1E00DBC4E7~YAAQRqERAj8eb2adAQAA98hWhh/5ahSJIasiiTL2/9tgkcWO9Q792T4FCLdcAiKTMICOUmr4N6t3Zwqe8VNnoZDoKX0LN4llspqQZhgZbsvQnWsY+hleGODe83TNwtMePdIDPVHxlwC6Hike6W00fRSf64cPJ0wVP7YhTvNjtgNVFjvQVSIK+WykYW8H8mi751BRR7kVQmbqUNqV7BnwyFsypnguUkfDFG/Jbr0AybV4XyDnHIcGOrgUTmi1WVfGNPQpeOjySWKXIjTVxLwuV7q3oQKnAepPamAcgeH5HKG9l8cbO7eFot6YTnOdvVHdyvfrrLx7kSpEObtT5Y1idjXgJ6L+uo8xPID5orqJnfRdFlPDYKq7IgqT6+WsmFmqP0GdDB+21/DXRDowj/h17Xxp3sSp~3291449~4339769",
}

# ── Request headers ────────────────────────────────────────────────────────────
HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9,sw;q=0.8",
    "access-control-allow-credentials": "true",
    "appflavour": "carrefour",
    "appid": "Reactweb",
    "channel": "c4online",
    "content-type": "application/json; charset=utf-8",
    "currency": "KES",
    "env": "prod",
    "hashedemail": "anonymous",
    "lang": "en",
    "langcode": "en",
    "latitude": "-1.2672236834605626",
    "longitude": "36.810586556760555",
    "loadbeaconspixel": "false",
    "mw_sellerflex_enabled": "false",
    "posinfo": "food=684_Zone01,express=KE4_Zone01,nonfood=681_Zone01",
    "producttype": "ANY",
    "servicetypes": "SLOTTED|DEFAULT|MKP_GLOBAL",
    "storeid": "mafken",
    "userid": "anonymous",
    "x-maf-account": "carrefour",
    "x-maf-env": "prod",
    "x-maf-revamp": "true",
    "x-maf-tenant": "mafretail",
    "x-requested-with": "XMLHttpRequest",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "referer": "https://www.carrefour.ke/mafken/en/c/FKEN1700000",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_URL = "https://www.carrefour.ke/api/v8/categories/{category_id}"
DEFAULT_CATEGORY = "FKEN1700000"
PAGE_SIZE = 40
DELAY_BETWEEN_REQUESTS = 1.5   # seconds — be polite to the server

# curl_cffi impersonation target — must match sec-ch-ua header above
IMPERSONATE = "chrome146"   # options: chrome110, chrome119, chrome124, chrome146


# ── Shared session (reuses TCP connection + cookie jar) ────────────────────────
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
SESSION.cookies.update(COOKIES)


# ── Core fetcher ───────────────────────────────────────────────────────────────
def fetch_page(
    category_id: str = DEFAULT_CATEGORY,
    page: int = 1,
    page_size: int = PAGE_SIZE,
) -> Optional[dict]:
    """Fetch a single page of products for a given category."""
    url = BASE_URL.format(category_id=category_id)
    params = {
        "sortBy": "relevance",
        "categoryCode": "",
        "needFilter": "false",
        "pageSize": page_size,
        "requireSponsProducts": "true",
        "verticalCategory": "true",
        "needVariantsData": "true",
        "currentPage": page,
        "responseWithCatTree": "true",
        "depth": "3",
        "lang": "en",
        "categoryId": category_id,
        "latitude": "-1.2672236834605626",
        "longitude": "36.810586556760555",
    }

    try:
        response = SESSION.get(
            url,
            params=params,
            impersonate=IMPERSONATE,   # ← bypasses Akamai TLS fingerprinting
            timeout=30,
        )

        if response.status_code == 403:
            print(
                f"  [403 Forbidden] Page {page} — Akamai cookies have likely expired.\n"
                "  Open DevTools → Network → any carrefour.ke request → copy the full\n"
                "  Cookie header and update the COOKIES dict at the top of this file."
            )
            return None

        response.raise_for_status()
        return response.json()

    except Exception as e:
        print(f"  [Error] Page {page}: {e}")
        try:
            print(f"  Raw (first 300 chars): {response.text[:300]}")
        except Exception:
            pass
    return None


# ── Product parser ─────────────────────────────────────────────────────────────
def parse_products(data: dict) -> list[dict]:
    """Extract a flat list of product dicts from an API response."""
    products = []
    items = (
        data.get("products")
        or data.get("data", {}).get("products")
        or []
    )
    for item in items:
        # ── Price ──────────────────────────────────────────────────────────────
        price_info  = item.get("price", {})
        price       = price_info.get("price", "")
        currency    = price_info.get("currency", "KES")
        fmt_value   = price_info.get("formattedValue", "")
        min_buying  = price_info.get("minBuyingValue", "")

        # ── Brand ──────────────────────────────────────────────────────────────
        brand_obj   = item.get("brand", {})
        brand_name  = brand_obj.get("name", "") if isinstance(brand_obj, dict) else str(brand_obj)
        brand_id    = brand_obj.get("id", "")   if isinstance(brand_obj, dict) else ""

        # ── Category (first entry in the list) ────────────────────────────────
        categories      = item.get("category", [])
        top_category    = categories[0].get("name", "") if categories else ""
        top_category_id = categories[0].get("id", "")   if categories else ""
        top_category_level = categories[0].get("level", "") if categories else ""

        # ── Full category hierarchy (breadcrumb string) ────────────────────────
        category_path = item.get("productCategoriesHearchi", "")

        # ── Stock ──────────────────────────────────────────────────────────────
        stock_info   = item.get("stock", {})
        stock_status = stock_info.get("stockLevelStatus", "")
        stock_qty    = stock_info.get("value", "")

        # ── Availability ───────────────────────────────────────────────────────
        avail     = item.get("availability", {})
        available = avail.get("isAvailable", "")
        avail_max = avail.get("max", "")

        # ── Unit / sizing ──────────────────────────────────────────────────────
        unit_info       = item.get("unit", {})
        unit_of_measure = unit_info.get("unitOfMeasure", "")
        increment_by    = unit_info.get("incrementBy", "")
        min_order       = unit_info.get("min", "")
        max_order       = unit_info.get("maxToOrder", "")

        
        # ── Seller / offer (first offer entry) ────────────────────────────────
        offers      = item.get("offers", [])
        first_offer = offers[0] if offers else {}
        offer_id=   first_offer.get("id", ""),
        seller_name = first_offer.get("sellerName", "")
        seller_shop_id = first_offer.get("shopId", ""),
        seller_type = first_offer.get("type", "")
        shipping    = first_offer.get("shippingIndicator", "")

        products.append({
            # Identifiers
            "id":               item.get("id", ""),
            "ean":              item.get("ean", ""),
            "name":             item.get("name", ""),
            "type":             item.get("type", ""),
            "food_type":        item.get("foodType", ""),
            # Brand & origin
            "brand":            brand_name,
            "brand_id":         brand_id,
            "product_origin":   item.get("productOrigin", ""),
            "supplier":         item.get("supplier", ""),
            # Category
            "top_category":     top_category,
            "top_category_id":  top_category_id,
            "category_path":    category_path, 
            "top_category_level": top_category_level,
            # Size
            "size":             item.get("size", ""),
            "unit_of_measure":  unit_of_measure,
            "increment_by":     increment_by,
            "min_order":        min_order,
            "max_order":        max_order,
            # Pricing
            "price":            price,
            "currency":         currency,
            "formatted_price":  fmt_value,
            "min_buying_value": min_buying,
            # Stock & availability
            "stock_status":     stock_status,
            "stock_qty":        stock_qty,
            "is_available":     available,
            "availability_max": avail_max,
            # Seller / fulfilment
            "seller":           seller_name,
            "seller_type":      seller_type,
            "shipping":         shipping,
            "offer_id":         offer_id,
            "seller_shop_id":   seller_shop_id,

            # Flags
            "is_marketplace":   item.get("isMarketPlace", False),
            "is_express":       item.get("isExpress", False),
            "is_bulk":          item.get("isBulk", False),
            "is_scalable":      item.get("isScalable", False),
            "is_fbc":           item.get("isFBC", False),
            "preorder":         item.get("preorder", False),
            "has_promo":        bool(item.get("promoBadges")),
        })
    return products


# ── Pagination helper ──────────────────────────────────────────────────────────
def get_total_pages(data: dict, page_size: int = PAGE_SIZE) -> int:
    pagination = (
        data.get("pagination")
        or data.get("data", {}).get("pagination")
        or {}
    )
    total_results = pagination.get("totalResults", 0)
    if total_results:
        return -(-total_results // page_size)
    return 1


# ── Main scraper ───────────────────────────────────────────────────────────────
def scrape_category(
    category_id: str = DEFAULT_CATEGORY,
    max_pages: Optional[int] = None,
    output_json: str = "carrefour_products.json",
    output_csv: str = "carrefour_products.csv",
) -> list[dict]:
    all_products: list[dict] = []

    print(f"[→] Fetching page 1 for category: {category_id}")
    first_page = fetch_page(category_id, page=1)
    if not first_page:
        print("[✗] Could not retrieve the first page. Aborting.")
        return []

    total_pages = get_total_pages(first_page)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"[i] Total pages to scrape: {total_pages}")
    all_products.extend(parse_products(first_page))

    for page in range(2, total_pages + 1):
        print(f"[→] Fetching page {page}/{total_pages} ...")
        time.sleep(DELAY_BETWEEN_REQUESTS)
        data = fetch_page(category_id, page=page)
        if data:
            batch = parse_products(data)
            all_products.extend(batch)
            print(f"    +{len(batch)} products  (total: {len(all_products)})")
        else:
            print(f"    [!] Skipping page {page}.")

    if all_products:
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)
        print(f"\n[✓] {len(all_products)} products → {output_json}")

        fieldnames = list(all_products[0].keys())
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_products)
        print(f"[✓] {len(all_products)} products → {output_csv}")
    else:
        print("[!] No products collected.")

    return all_products


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    products = scrape_category(
        category_id="FKEN1700000",
        max_pages=None,   # set e.g. max_pages=3 to test first
    )

    if products:
        print("\n── Sample product ──")
        for k, v in products[0].items():
            print(f"  {k:20s}: {v}")
