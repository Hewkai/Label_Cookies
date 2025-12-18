import requests
from bs4 import BeautifulSoup
import re
import mysql.connector
from urllib.parse import quote_plus
import time
import random
from datetime import datetime
import unicodedata  

# DB helper

def get_db():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="cookies_db"
    )
    cursor = db.cursor(dictionary=True)
    return db, cursor


# Config / Constants

EASYLIST_URL = "https://easylist.to/easylist/easylist.txt"

CATEGORY_UNKNOWN          = "unknown"
CATEGORY_EASYLIST_DEFAULT = "Targeting or Advertising Cookies"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# RAM caches
COOKIESEARCH_CACHE = {}
COOKIEPEDIA_CACHE  = {}

# สำหรับ rate limit ของ cookiepedia
COOKIEPEDIA_LAST_CALL = 0.0   # timestamp ล่าสุดที่ยิง cookiepedia
COOKIEPEDIA_CALLS_IN_MIN = []


# log file สำหรับความคืบหน้า
LOG_FILE = "label_progress.log"


# Logging helper

def log_progress(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# Normalize to 4 core categories

def normalize_category(raw: str) -> str:
    if not raw:
        return CATEGORY_UNKNOWN

    # Normalize unicode
    cleaned = unicodedata.normalize("NFKC", str(raw))

    # ลบ NBSP และ zero-width characters
    cleaned = (
        cleaned
        .replace("\xa0", " ")      # NBSP
        .replace("\u200b", "")     # ZERO WIDTH SPACE
        .replace("\u200c", "")     # ZERO WIDTH NON-JOINER
        .replace("\u200d", "")     # ZERO WIDTH JOINER
    )

    r = cleaned.lower().strip()

    # Debug (ถ้าอยากดูค่า raw จริง ๆ ให้เปิดบรรทัดนี้)
    # log_progress(f"[normalize] raw={repr(raw)} cleaned={repr(r)}")

    # Strictly necessary
    if any(x in r for x in [
        "strictly necessary", "necessary", "essential", "required", "security"
    ]):
        return "Strictly Necessary Cookies"

    # Performance / analytics
    if any(x in r for x in [
        "performance", "analytics", "measurement", "statistics", "statistical"
    ]):
        return "Performance Cookies"

    # Functionality / preferences
    if any(x in r for x in [
        "functionality", "functional", "preferences", "preference",
        "customization", "personalization", "social", "chat", "support"
    ]):
        return "Functionality Cookies"

    # Advertising / tracking / marketing
    if any(x in r for x in [
        "advertising", "advertisement", "advertisements",
        "ads", "adverts", "adtech",
        "targeting", "behavioural", "behavioral",
        "marketing", "remarketing", "retargeting",
        "tracking", "profiling"
    ]):
        return "Targeting or Advertising Cookies"

    return CATEGORY_UNKNOWN


# STEP 1: EasyList

def load_filter_list_domains(url: str):
    log_progress(f"[*] Downloading filter list from {url} ...")
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, timeout=120, headers=headers)
    resp.raise_for_status()

    domains = set()
    for line in resp.text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("!") or line.startswith("["):
            continue
        if "##" in line or "#@" in line or "$" in line:
            continue

        for host in re.findall(r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}", line):
            domains.add(host.lower())

    log_progress(f"    -> extracted {len(domains)} domains")
    return domains


def build_ad_domain_set():
    easylist_domains = load_filter_list_domains(EASYLIST_URL)
    log_progress(f"[*] Total ad/tracking domains: {len(easylist_domains)}")
    return easylist_domains


def is_ad_domain(domain: str, ad_domain_set):
    if not domain:
        return False

    d = domain.lstrip(".").lower()
    parts = d.split(".")
    for i in range(len(parts) - 1):
        candidate = ".".join(parts[i:])
        if candidate in ad_domain_set:
            return True
    return False


# STEP 2: CookieSearch

def parse_cookiesearch_html(html: str):
    try:
        soup = BeautifulSoup(html, "html.parser")
        label_node = soup.find(string=re.compile(r"Category", re.I))

        if label_node:
            node = label_node
            for _ in range(10):
                node = node.next_element
                if isinstance(node, str):
                    txt = node.strip()
                    if txt and txt not in [":", "-"]:
                        return txt

        m = re.search(
            r"Category\s*[:：]\s*</[^>]+>\s*<[^>]*>\s*([A-Za-z ]+)",
            html,
            re.IGNORECASE
        )
        if m:
            return m.group(1).strip()

    except Exception:
        pass

    return None


def check_cookiesearch(cookie_name: str):
    key = (cookie_name or "").lower()
    if not key:
        return None

    # RAM cache ก่อน
    if key in COOKIESEARCH_CACHE:
        return COOKIESEARCH_CACHE[key]

    try:
        q = quote_plus(cookie_name)
        url = f"https://cookiesearch.org/cookies/?search-term={q}&filter-type=cookie-name&sort=asc&cookie-id={q}"

        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, timeout=20, headers=headers)

        if resp.status_code != 200:
            log_progress(f"[CookieSearch] {cookie_name} -> HTTP {resp.status_code}")
            COOKIESEARCH_CACHE[key] = None
            return None

        raw_category = parse_cookiesearch_html(resp.text)
        if raw_category:
            log_progress(f"[CookieSearch] {cookie_name} -> {repr(raw_category)}")
            result = {
                "label": raw_category,
                "source": "cookiesearch",
                "url": url
            }
        else:
            log_progress(f"[CookieSearch] {cookie_name} -> no category found")
            result = None

        COOKIESEARCH_CACHE[key] = result
        return result

    except Exception as e:
        log_progress(f"[CookieSearch] error on {cookie_name}: {e}")
        COOKIESEARCH_CACHE[key] = None
        return None


# STEP 3: Cookiepedia
#   - Random Delay + Backoff
#   - Limit rate 3 req/sec
#   - DB preload cache
#   - เพื่อลด limit rate

def cookiepedia_rate_limit():
    """
    Enforce:
      - minimum 1 second between requests
      - optional: max X requests per minute (here: 60)
    """
    global COOKIEPEDIA_LAST_CALL, COOKIEPEDIA_CALLS_IN_MIN
    now = time.time()

    # Keep only last 60 sec timestamps
    COOKIEPEDIA_CALLS_IN_MIN = [t for t in COOKIEPEDIA_CALLS_IN_MIN if now - t < 60]

    # 1 request per second → interval = 1 request 60 seconds
    MIN_INTERVAL = 60.0

    elapsed = now - COOKIEPEDIA_LAST_CALL
    if elapsed < MIN_INTERVAL:
        time.sleep(MIN_INTERVAL - elapsed)

    # Optional: max 60 req/min (not really needed but keeps list clean)
    COOKIEPEDIA_CALLS_IN_MIN.append(time.time())
    COOKIEPEDIA_LAST_CALL = time.time()



COOKIEPEDIA_LAST_CALL = time.time()
COOKIEPEDIA_CALLS_IN_MIN.append(COOKIEPEDIA_LAST_CALL)

# def _respect_cookiepedia_rate_limit():
#     """บังคับให้ไม่เกิน ~3 request/sec + random delay เล็กน้อย"""
#     global COOKIEPEDIA_LAST_CALL
#     now = time.time()
#     min_interval = 1.0 / 3.0  # 3 req/sec
#     elapsed = now - COOKIEPEDIA_LAST_CALL

#     if elapsed < min_interval:
#         time.sleep(min_interval - elapsed)

#     # random jitter เพื่อลด pattern ที่คงที่เกินไป
#     time.sleep(random.uniform(0.0, 0.3))

#     COOKIEPEDIA_LAST_CALL = time.time()


def check_cookiepedia(cookie_name: str):
    key = (cookie_name or "").lower()
    if not key:
        return None

    # RAM cache ก่อน
    if key in COOKIEPEDIA_CACHE:
        return COOKIEPEDIA_CACHE[key]

    url = f"https://cookiepedia.co.uk/cookies/{cookie_name}"
    max_retries = 3
    result = None

    for attempt in range(max_retries):
        try:
            # _respect_cookiepedia_rate_limit()
            cookiepedia_rate_limit()
            
            headers = {"User-Agent": USER_AGENT}
            resp = requests.get(url, timeout=20, headers=headers)

            # ถ้าโดน rate limit → backoff แล้วลองใหม่
            if resp.status_code == 429:
                wait = (attempt + 1) * random.uniform(10, 20)
                log_progress(f"[Cookiepedia] {cookie_name} -> HTTP 429, backoff {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = (attempt + 1) * random.uniform(5, 10)
                log_progress(f"[Cookiepedia] {cookie_name} -> HTTP {resp.status_code}, retry in {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                log_progress(f"[Cookiepedia] {cookie_name} -> HTTP {resp.status_code}, giving up")
                result = None
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            main_purpose = None
            for p in soup.find_all("p"):
                text = p.get_text(strip=True)
                if "The main purpose of this cookie is" in text:
                    parts = text.split(":", 1)
                    if len(parts) == 2:
                        main_purpose = parts[1].strip()
                    break

            if main_purpose:
                result = {
                    "label": main_purpose,
                    "source": "cookiepedia",
                    "url": url
                }
            else:
                result = {
                    "label": CATEGORY_UNKNOWN,
                    "source": "cookiepedia",
                    "url": url
                }

            break  # เสร็จแล้วออกจาก loop

        except Exception as e:
            log_progress(f"[Cookiepedia] error on {cookie_name}: {e}")
            if attempt < max_retries - 1:
                wait = (attempt + 1) * random.uniform(5, 10)
                log_progress(f"    -> retry in {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            else:
                result = None

    COOKIEPEDIA_CACHE[key] = result
    return result


# DB-level cache preload

def preload_cookiepedia_cache_from_db(db):
    cur = db.cursor(dictionary=True)
    cur.execute("""
        SELECT name, label, label_source, label_source_url
        FROM cookies
        WHERE label_source = 'cookiepedia'
          AND label IS NOT NULL
          AND label <> ''
    """)
    count = 0
    for row in cur:
        key = (row["name"] or "").lower()
        if not key:
            continue
        COOKIEPEDIA_CACHE[key] = {
            "label": row["label"],
            "source": row["label_source"],
            "url": row["label_source_url"],
        }
        count += 1
    cur.close()
    log_progress(f"[*] Preloaded {count} cookiepedia labels from DB into cache.")


def preload_cookiesearch_cache_from_db(db):
    cur = db.cursor(dictionary=True)
    cur.execute("""
        SELECT name, label, label_source, label_source_url
        FROM cookies
        WHERE label_source = 'cookiesearch'
          AND label IS NOT NULL
          AND label <> ''
    """)
    count = 0
    for row in cur:
        key = (row["name"] or "").lower()
        if not key:
            continue
        COOKIESEARCH_CACHE[key] = {
            "label": row["label"],
            "source": row["label_source"],
            "url": row["label_source_url"],
        }
        count += 1
    cur.close()
    log_progress(f"[*] Preloaded {count} cookiesearch labels from DB into cache.")


# Label cookie

def label_one_cookie(cookie_name, domain, website_url, ad_domain_set):
    name = (cookie_name or "").strip()
    dom  = (domain or "").strip()

    if not name:
        return {
            "label": CATEGORY_UNKNOWN,
            "source": "none",
            "url": None,
        }

    # STEP 1: EasyList
    if is_ad_domain(dom, ad_domain_set):
        return {
            "label": CATEGORY_EASYLIST_DEFAULT,
            "source": "filterlist",
            "url": EASYLIST_URL,
        }

    # STEP 2: CookieSearch
    cs = check_cookiesearch(name)
    if cs:
        return {
            "label": normalize_category(cs["label"]),
            "source": cs["source"],
            "url": cs["url"],
        }

    # STEP 3: Cookiepedia
    cp = check_cookiepedia(name)
    if cp:
        return {
            "label": normalize_category(cp["label"]),
            "source": cp["source"],
            "url": cp["url"],
        }

    # ไม่เจอ
    return {
        "label": CATEGORY_UNKNOWN,
        "source": "none",
        "url": None,
    }


# Main

def label_cookies_from_db(cookie_batch_size=200):
    db, cursor = get_db()

    # preload DB-level cache
    preload_cookiepedia_cache_from_db(db)
    preload_cookiesearch_cache_from_db(db)

    # ใช้ดู progress
    cursor.execute("""
        SELECT COUNT(*) AS total
        FROM cookies
        WHERE label IS NULL OR label = ''
    """)
    total_unlabeled = cursor.fetchone()["total"]
    log_progress(f"[*] START run: total_unlabeled={total_unlabeled}")

    # โหลด EasyList แค่ครั้งเดียว
    ad_domain_set = build_ad_domain_set()

    processed_in_run = 0

    while True:
        cursor.execute("""
            SELECT id, website, name, domain
            FROM cookies
            WHERE label IS NULL OR label = ''
            LIMIT %s
        """, (cookie_batch_size,))
        rows = cursor.fetchall()

        if not rows:
            log_progress("[*] No more unlabeled cookies.")
            break

        log_progress(f"[*] Labeling batch of {len(rows)} cookies ...")

        for row in rows:
            cid     = row["id"]
            website = row.get("website") or ""
            name    = row.get("name") or ""
            domain  = row.get("domain") or ""

            result = label_one_cookie(
                cookie_name=name,
                domain=domain,
                website_url=website,
                ad_domain_set=ad_domain_set
            )

            label      = result["label"]
            source     = result["source"]
            source_url = result["url"]

            log_progress(f"  - [id={cid}] name={name} ({domain}) -> {label} ({source})")

            cursor.execute("""
                UPDATE cookies
                SET label = %s,
                    label_source = %s,
                    label_source_url = %s
                WHERE id = %s
            """, (label, source, source_url, cid))

        db.commit()
        processed_in_run += len(rows)
        est_remaining = max(total_unlabeled - processed_in_run, 0)
        log_progress(f"[*] Batch committed. processed={processed_in_run}, est_remaining={est_remaining}\n")

    cursor.close()
    db.close()
    log_progress("[*] DONE run.")
    
def recheck_unknown_cookie(cookie_name, domain, website_url):
    name = (cookie_name or "").strip()
    dom  = (domain or "").strip()

    if not name:
        return None

#############################recheck_unknown_cookie#########################################

    # Try CookieSearch again
    cs = check_cookiesearch(name)
    if cs:
        normalized = normalize_category(cs["label"])
        if normalized != CATEGORY_UNKNOWN:
            return {
                "label": normalized,
                "source": cs["source"],
                "url": cs["url"],
            }

    # Try Cookiepedia again
    cp = check_cookiepedia(name)
    if cp:
        normalized = normalize_category(cp["label"])
        if normalized != CATEGORY_UNKNOWN:
            return {
                "label": normalized,
                "source": cp["source"],
                "url": cp["url"],
            }

    # Still truly unknown
    return None

def recheck_unknowns_from_db(batch_size=50):
    db, cursor = get_db()

    cursor.execute("""
        SELECT COUNT(*) AS total
        FROM cookies
        WHERE label = %s
    """, (CATEGORY_UNKNOWN,))
    total_unknown = cursor.fetchone()["total"]

    log_progress(f"[*] START recheck UNKNOWN pass: total_unknown={total_unknown}")

    processed = 0

    while True:
        cursor.execute("""
            SELECT id, website, name, domain
            FROM cookies
            WHERE label = %s
            LIMIT %s
        """, (CATEGORY_UNKNOWN, batch_size))
        rows = cursor.fetchall()

        if not rows:
            break

        for row in rows:
            cid     = row["id"]
            website = row.get("website") or ""
            name    = row.get("name") or ""
            domain  = row.get("domain") or ""

            result = recheck_unknown_cookie(
                cookie_name=name,
                domain=domain,
                website_url=website
            )

            if result:
                log_progress(
                    f"  [RECHECK] id={cid} name={name} -> {result['label']} ({result['source']})"
                )

                cursor.execute("""
                    UPDATE cookies
                    SET label = %s,
                        label_source = %s,
                        label_source_url = %s
                    WHERE id = %s
                """, (
                    result["label"],
                    result["source"],
                    result["url"],
                    cid
                ))

        db.commit()
        processed += len(rows)
        log_progress(f"[*] Recheck batch committed. processed={processed}")

    cursor.close()
    db.close()
    log_progress("[*] DONE recheck UNKNOWN pass.")


if __name__ == "__main__":
    label_cookies_from_db(cookie_batch_size=100)

    # Second pass to confirm real UNKNOWNs
    recheck_unknowns_from_db(batch_size=50)
