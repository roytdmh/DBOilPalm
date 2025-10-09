# ScraperScriptOilPalm.py
import requests
from bs4 import BeautifulSoup
import sqlite3
import hashlib
import re
from collections import deque, defaultdict
import time
import os
from urllib.parse import urljoin, urlparse
import langdetect
import argparse
# Optional: For PDF extraction (install pdfplumber)
import pdfplumber
import json

# -------------------- Your original config --------------------
CATEGORIES = {
    'Cultivation': ['cultivation', 'planting', 'growth', 'sowing', 'harvesting', 'agronomy'],
    'Processing': ['processing', 'extraction', 'refining', 'mill', 'oil extraction', 'fruit bunch'],
    'Environmental Impact': ['environment', 'biodiversity', 'deforestation', 'sustainability', 'climate change', 'impact'],
    'Market Trends': ['market', 'trends', 'price', 'trade', 'economy', 'production', 'export'],
    'Plantation Management': ['plantation', 'management', 'yield', 'pests', 'irrigation', 'soil', 'farm']
}

REPUTABLE_DOMAINS = {
    'eos.com', 'ourworldindata.org', 'iucn.org', 'farmonaut.com', 'cabiagbio.biomedcentral.com',
    'sciencedirect.com', 'ocl-journal.org', 'frontiersin.org', 'researchgate.net',
    'sustainablepalmoilchoice.eu', 'pmc.ncbi.nlm.nih.gov', 'intechopen.com', 'epthinktank.eu',
    'onlinelibrary.wiley.com', 'annualreviews.org', 'cambridge.org', 'nature.com',
    'fao.org', 'mdpi.com', 'aocs.org', 'mongabay.com', 'custommarketinsights.com',
    'iisd.org', 'en.wikipedia.org', 'earth.org','zsl.org','iopscience.iop.org','unu.edu','plos.org','worldwildlife.org','unl.edu','academicjournals.org','cifor.org','conservation.org','monash.edu','doi.org'
}

SEED_URLS = [
    # ... (kept as in your original list) ...
]

OIL_PALM_KEYWORDS = ['oil palm', 'palm oil', 'elaeis guineensis', 'plantation', 'cultivation', 'processing']

DB_PATH = r"C:\Users\Roy\Documents\DBOilPalmmiro\oilpalmdbmiro.db"
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# -------------------- New: category DB mirroring support --------------------
# Cache of open category DB connections: category_name -> {'conn': sqlite3.Connection, 'path': path}
_CATEGORY_DB_CACHE = {}
# Whether to mirror to per-category DBs; set by CLI flag
MIRROR_CATEGORY_DBS = False

def sanitize_filename(name: str) -> str:
    """Make a safe filename for category DBs."""
    if not name:
        return "Uncategorized"
    s = str(name).strip()
    if s == "":
        return "Uncategorized"
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[\\/:*?"<>|]+', '_', s)
    return s[:200]

def _open_category_db(category_name: str):
    """
    Open (or reuse) a per-category DB file and ensure it has the articles schema.
    Returns sqlite3.Connection.
    """
    global _CATEGORY_DB_CACHE
    key = str(category_name)
    if key in _CATEGORY_DB_CACHE:
        return _CATEGORY_DB_CACHE[key]['conn']
    fname = sanitize_filename(category_name) + ".db"
    folder = os.path.dirname(DB_PATH) or "."
    out_path = os.path.join(folder, fname)
    conn = sqlite3.connect(out_path)
    # performance pragmas (safe)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = OFF;")
    # Ensure articles table schema exists (match source)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            url TEXT PRIMARY KEY,
            title TEXT,
            content TEXT,
            category TEXT,
            scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            hash TEXT UNIQUE
        )
    ''')
    conn.commit()
    _CATEGORY_DB_CACHE[key] = {'conn': conn, 'path': out_path}
    return conn

def _close_all_category_dbs():
    global _CATEGORY_DB_CACHE
    for info in _CATEGORY_DB_CACHE.values():
        try:
            info['conn'].commit()
            info['conn'].close()
        except Exception:
            pass
    _CATEGORY_DB_CACHE.clear()

# -------------------- original DB setup --------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pending_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            depth INTEGER DEFAULT 0
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS visited_urls (
            url TEXT PRIMARY KEY
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            url TEXT PRIMARY KEY,
            title TEXT,
            content TEXT,
            category TEXT,
            scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            hash TEXT UNIQUE  -- For dedup
        )
    ''')
    conn.commit()
    conn.close()

# -------------------- helpers (unchanged) --------------------
def add_seeds():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for url in SEED_URLS:
        if not is_visited(url):
            cursor.execute("INSERT OR IGNORE INTO pending_urls (url, depth) VALUES (?, 0)", (url,))
    conn.commit()
    conn.close()

def is_visited(url):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM visited_urls WHERE url = ?", (url,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def add_to_pending(urls, current_depth, max_depth=float(3)):
    if current_depth >= max_depth:
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for url in urls:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain in REPUTABLE_DOMAINS or any(seed_domain in domain for seed_domain in REPUTABLE_DOMAINS):
            if not is_visited(url):
                cursor.execute("INSERT OR IGNORE INTO pending_urls (url, depth) VALUES (?, ?)", (url, current_depth + 1))
    conn.commit()
    conn.close()

def get_next_pending():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT url, depth FROM pending_urls ORDER BY id ASC LIMIT 1")
    result = cursor.fetchone()
    if result:
        url, depth = result
        cursor.execute("DELETE FROM pending_urls WHERE url = ?", (url,))
        conn.commit()
        conn.close()
        return url, depth
    else:
        conn.commit()
        conn.close()
        return None, None

def mark_visited(url):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO visited_urls (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()

# -------------------- Web fetch & preprocess (unchanged) --------------------
def fetch_raw_data(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    try:
        time.sleep(1)
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        if url.lower().endswith('.pdf'):
            return {'title': '', 'raw_text': ''}
        soup = BeautifulSoup(response.text, 'lxml')
        title = soup.title.string.strip() if soup.title else ''
        for script in soup(["script", "style"]):
            script.decompose()
        raw_text = soup.get_text()
        return {'title': title, 'raw_text': raw_text}
    except Exception as e:
        print(f"Fetch error for {url}: {e}")
        return None

def preprocess_data(raw_data):
    if not raw_data or not raw_data['raw_text']:
        return ''
    text = raw_data['raw_text']
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s\.\,\!\?]', '', text)
    text = text.strip().lower()
    return text

def classify_text(text):
    scores = {cat: sum(1 for kw in kws if kw in text) for cat, kws in CATEGORIES.items()}
    max_score = max(scores.values())
    if max_score == 0:
        return 'Uncategorized'
    return max(cat for cat, score in scores.items() if score == max_score)

def quality_assurance(url, title, content):
    try:
        lang = langdetect.detect(content)
        if lang != 'en':
            print(f"Flagged: Non-English ({lang}) for {url}")
            return False, 'Language'
    except:
        pass
    content_hash = hashlib.md5(content.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT hash FROM articles WHERE hash = ?", (content_hash,))
    if cursor.fetchone():
        print(f"Flagged: Duplicate for {url}")
        conn.close()
        return False, 'Duplicate'
    conn.close()
    domain = urlparse(url).netloc.lower()
    if domain not in REPUTABLE_DOMAINS:
        print(f"Flagged: Low credibility domain {domain} for {url}")
        return False, 'Source'
    if len(content) < 100:
        print(f"Flagged: Too short for {url}")
        return False, 'Short'
    return True, content_hash

# -------------------- Modified storage: mirror to category DBs --------------------
def store_article(url, title, content, category, content_hash):
    """
    Store into the main DB and (optionally) also into a per-category DB file.
    """
    # store in main DB (unchanged behavior)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO articles (url, title, content, category, hash)
        VALUES (?, ?, ?, ?, ?)
    ''', (url, title, content, category, content_hash))
    conn.commit()
    conn.close()
    print(f"Stored: {title[:50]}... in main DB as {category}")

    # Mirror to per-category DBs if enabled
    if not MIRROR_CATEGORY_DBS:
        return

    # Ensure category value exists
    if not category or str(category).strip() == '':
        cat_name = 'Uncategorized'
    else:
        cat_name = category

    try:
        cat_conn = _open_category_db(cat_name)
        # Insert into category DB's articles table
        cat_cursor = cat_conn.cursor()
        cat_cursor.execute('''
            INSERT OR REPLACE INTO articles (url, title, content, category, hash)
            VALUES (?, ?, ?, ?, ?)
        ''', (url, title, content, category, content_hash))
        cat_conn.commit()
        print(f"Mirrored: {title[:50]}... -> {sanitize_filename(cat_name)}.db")
    except Exception as e:
        print(f"Error mirroring to category DB '{cat_name}': {e}")

# -------------------- Main pipeline (slight change: close category DBs on exit) --------------------
def main(mirror_category_dbs=False):
    global MIRROR_CATEGORY_DBS
    MIRROR_CATEGORY_DBS = mirror_category_dbs

    init_db()
    add_seeds()
    processed_count = 0
    max_items = float('inf')

    while True:
        try:
            next_url, depth = get_next_pending()
            if next_url is None:
                print("Queue empty. Scraping complete.")
                break
            if is_visited(next_url):
                continue

            print(f"Processing: {next_url} (depth {depth})")
            mark_visited(next_url)

            raw_data = fetch_raw_data(next_url)
            if not raw_data:
                continue

            content = preprocess_data(raw_data)
            category = classify_text(content)

            meets_standards, result = quality_assurance(next_url, raw_data['title'], content)
            if not meets_standards:
                print(f"Deleted/Flagged: {result} for {next_url}")
                continue

            store_article(next_url, raw_data['title'], content, category, result)
            processed_count += 1

            # Continue crawl: extract links, add them to pending
            try:
                page = requests.get(next_url, timeout=10)
                soup = BeautifulSoup(page.text, 'lxml')
                raw_links = [a.get('href') for a in soup.find_all('a', href=True) if a.get('href')]
                clean_links = {urljoin(next_url, link).split('#')[0] for link in raw_links}
                add_to_pending(list(clean_links), depth)
            except Exception as e:
                # If fetching links fails, skip adding new links for this page
                pass

            if processed_count >= max_items:
                print(f"Reached max items ({max_items}). Stopping.")
                break

            time.sleep(2)

        except KeyboardInterrupt:
            print("\nInterrupted. Progress saved to DB. Rerun to resume.")
            break
        except Exception as e:
            print(f"Error: {e}")
            continue

    # close any open category DBs
    _close_all_category_dbs()
    print(f"Total processed: {processed_count}")

# -------------------- CLI entry --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Oil palm scraper with optional per-category DB mirroring.")
    parser.add_argument('--mirror-category-dbs', action='store_true', help='Also write each saved article into a per-category DB file (one DB per category).')
    args = parser.parse_args()
    main(mirror_category_dbs=args.mirror_category_dbs)
