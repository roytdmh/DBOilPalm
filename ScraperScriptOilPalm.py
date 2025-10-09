import requests
from bs4 import BeautifulSoup
import sqlite3
import hashlib
import re
from collections import deque
import time
import os
from urllib.parse import urljoin, urlparse
import langdetect
# Optional: For PDF extraction (install pdfplumber)
import pdfplumber

# Define agronomy categories (per Miro board: exact sequence starts here)
CATEGORIES = {
    'Cultivation': ['cultivation', 'planting', 'growth', 'sowing', 'harvesting', 'agronomy'],
    'Processing': ['processing', 'extraction', 'refining', 'mill', 'oil extraction', 'fruit bunch'],
    'Environmental Impact': ['environment', 'biodiversity', 'deforestation', 'sustainability', 'climate change', 'impact'],
    'Market Trends': ['market', 'trends', 'price', 'trade', 'economy', 'production', 'export'],
    'Plantation Management': ['plantation', 'management', 'yield', 'pests', 'irrigation', 'soil', 'farm']
}

# Reputable domains for source credibility (whitelist from researched seeds)
REPUTABLE_DOMAINS = {
    'eos.com', 'ourworldindata.org', 'iucn.org', 'farmonaut.com', 'cabiagbio.biomedcentral.com',
    'sciencedirect.com', 'ocl-journal.org', 'frontiersin.org', 'researchgate.net',
    'sustainablepalmoilchoice.eu', 'pmc.ncbi.nlm.nih.gov', 'intechopen.com', 'epthinktank.eu',
    'onlinelibrary.wiley.com', 'annualreviews.org', 'cambridge.org', 'nature.com',
    'fao.org', 'mdpi.com', 'aocs.org', 'mongabay.com', 'custommarketinsights.com',
    'iisd.org', 'en.wikipedia.org', 'earth.org','zsl.org','iopscience.iop.org','unu.edu','plos.org','worldwildlife.org','unl.edu','academicjournals.org','cifor.org','conservation.org','monash.edu','doi.org'  # Add more as needed
}

# Seed URLs (researched reputable sources for oil palm topics; maximizes coverage)
SEED_URLS = [
'https://eos.com/blog/oil-palm-plantation/',
'https://eos.com/blog/palm-oil-malaysia-indonesia-thailand/',
'https://eos.com/blog/palm-oil-deforestation/',
'https://eos.com/blog/a-story-behind-a-story-on-pahang-oil-palm-plantations/',
'https://eos.com/blog/eosda-enhanced-pysawit-model-to-improve-its-accuracy/',
'https://eos.com/events/agritech-tools-for-oil-palms-in-southeast-asia/',
'https://eos.com/blog-tags/crop-cultivation/',
'https://eos.com/blog-author/kateryna-sergieieva/',
'https://eos.com/blog/sustainable-forestry/',
'https://ourworldindata.org/grapher/palm-oil-production',
'https://ourworldindata.org/grapher/palm-oil-yields',
'https://ourworldindata.org/palm-oil',
'https://ourworldindata.org/grapher/land-use-palm-oil',
'https://ourworldindata.org/grapher/oil-yield-by-crop',
'https://ourworldindata.org/grapher/palm-oil-imports',
'https://ourworldindata.org/grapher/land-use-for-vegetable-oil-crops',
'https://ourworldindata.org/grapher/change-in-production-yield-and-land-palm',
'https://ourworldindata.org/grapher/area-per-tonne-oil',
'https://ourworldindata.org/grapher/vegetable-oil-production',
'https://iucn.org/resources/issues-brief/palm-oil-and-biodiversity',
'https://portals.iucn.org/library/sites/library/files/documents/2018-027-En.pdf',
'https://iucn.org/resources/publication/oil-palm-and-biodiversity',
'https://iucn.org/resources/infographic/palm-oil-and-biodiversity-infographic',
'https://iucn.org/news/secretariat/201806/saying-no-palm-oil-would-likely-displace-not-halt-biodiversity-loss---iucn-report',
'https://iucn.org/blog/202507/can-reading-iucn-report-palm-oil-shift-conservation-perspectives',
'https://iucn.org/sites/default/files/2023-11/2020-oil-palm-tf-report_publication.pdf',
'https://iucn.org/news/forests/201908/cracking-open-a-better-source-oil',
'https://iucn.org/sites/default/files/2023-11/2016-2017-oil-palm-tf-report.pdf',
'https://portals.iucn.org/library/taxonomy/term/71981',
'https://farmonaut.com/precision-farming/maximizing-oil-palm-yields-a-comprehensive-guide-to-planting-density-age-and-acre-productivity',
'https://farmonaut.com/precision-farming/high-quality-palm-oil-farm-7-tips-to-maximize-profits',
'https://farmonaut.com/precision-farming/palm-oil-yield-per-acre-7-ways-to-boost-productivity',
'https://farmonaut.com/blogs/oil-palm-plantation-valuation-7-key-insights',
'https://farmonaut.com/precision-farming/how-to-plant-oil-palm-unlock-explosive-growth-secrets',
'https://farmonaut.com/asia/palm-oil-farmers-malaysias-shocking-sustainability-secrets',
'https://farmonaut.com/blogs/deforestation-free-palm-oil-key-facts-statistics-2025',
'https://farmonaut.com/precision-farming/palm-tree-farming-7-tips-to-maximize-profits-fast',
'https://farmonaut.com/remote-sensing/oil-palm-plantation-tech-3-key-solutions-by-farmonaut',
'https://farmonaut.com/blogs/certified-sustainable-palm-oil-forest-certifications-2025',
'https://cabiagbio.biomedcentral.com/articles/10.1186/s43170-021-00058-3',
'https://cabiagbio.biomedcentral.com/articles/10.1186/s43170-021-00058-3/tables/2',
'https://cabiagbio.biomedcentral.com/articles/10.1186/s43170-022-00127-1',
'https://cabiagbio.biomedcentral.com/articles/10.1186/s43170-021-00058-3/metrics',
'https://cabiagbio.biomedcentral.com/articles/10.1186/s43170-022-00127-1/tables/2',
'https://cabiagbio.biomedcentral.com/articles/10.1186/s43170-022-00138-y',
'https://cabiagbio.biomedcentral.com/articles/10.1186/s43170-021-00058-3/tables/1',
'https://www.sciencedirect.com/topics/chemical-engineering/palm-oil',
'https://www.sciencedirect.com/topics/agricultural-and-biological-sciences/palm-oil',
'https://www.sciencedirect.com/topics/agricultural-and-biological-sciences/oil-palm-products',
'https://www.sciencedirect.com/science/article/abs/pii/B9780981893693500046',
'https://www.sciencedirect.com/topics/pharmacology-toxicology-and-pharmaceutical-science/palm-oil',
'https://www.sciencedirect.com/science/article/abs/pii/S030090842030225X',
'https://www.sciencedirect.com/science/article/pii/S1877343520300749',
'https://www.sciencedirect.com/science/article/pii/S235255092300132X',
'https://www.sciencedirect.com/science/article/abs/pii/S1462901124000054',
'https://www.sciencedirect.com/book/9780981893693/palm-oil',
'https://www.ocl-journal.org/articles/ocl/full_html/2023/01/ocl230042/ocl230042.html',
'https://www.ocl-journal.org/articles/ocl/full_html/2022/01/ocl210098/ocl210098.html',
'https://www.ocl-journal.org/articles/ocl/full_html/2022/01/ocl220003/ocl220003.html',
'https://www.ocl-journal.org/articles/ocl/abs/2005/02/ocl2005122p161/ocl2005122p161.html',
'https://www.ocl-journal.org/articles/ocl/full_html/2024/01/ocl230044/ocl230044.html',
'https://www.ocl-journal.org/articles/ocl/pdf/2006/01/ocl2006131p9.pdf',
'https://www.ocl-journal.org/articles/ocl/full_html/2022/01/ocl210102/ocl210102.html',
'https://www.ocl-journal.org/articles/ocl/full_html/2023/01/ocl220053/ocl220053.html',
'https://www.ocl-journal.org/articles/ocl/pdf/2009/04/ocl2009164p193.pdf',
'https://www.ocl-journal.org/articles/ocl/abs/2005/02/ocl2005122p141/ocl2005122p141.html',
'https://www.frontiersin.org/journals/sustainable-food-systems/articles/10.3389/fsufs.2024.1398877/full',
'https://www.frontiersin.org/journals/sustainable-food-systems/articles/10.3389/fsufs.2023.1083022/full',
'https://www.frontiersin.org/journals/sustainable-food-systems/articles/10.3389/fsufs.2025.1606323/full',
'https://www.frontiersin.org/journals/sustainable-food-systems/articles/10.3389/fsufs.2025.1621217/full',
'https://www.frontiersin.org/journals/sustainable-food-systems/articles/10.3389/fsufs.2025.1473991/full',
'https://www.frontiersin.org/journals/sustainable-food-systems/articles/10.3389/fsufs.2023.1217653/full',
'https://www.frontiersin.org/journals/nutrition/articles/10.3389/fnut.2024.1388259/full',
'https://kids.frontiersin.org/articles/10.3389/frym.2020.00086',
'https://www.frontiersin.org/journals/sustainable-food-systems/articles/10.3389/fsufs.2024.1418732/full',
'https://www.frontiersin.org/journals/forests-and-global-change/articles/10.3389/ffgc.2024.1441266/full',
'https://www.researchgate.net/publication/291181965_A_Brief_History_of_the_Oil_Palm',
'https://www.researchgate.net/publication/323706249_Oil_Palm',
'https://www.researchgate.net/figure/Fatty-acid-composition-of-palm-oil-and-palm-kernel-oil_tbl1_282129563',
'https://www.researchgate.net/publication/285328768_Food_Uses_of_Palm_Oil_and_Its_Components',
'https://www.researchgate.net/profile/Cristina-Larrea/publication/378140450_Global_Market_Report_Palm_Oil/links/65c92781790074549771e351/Global-Market-Report-Palm-Oil.pdf',
'https://www.researchgate.net/profile/Veronique-Gibon/publication/270843249_Future_prospects_for_palm_oil_refining_and_modifications/links/568b9bf508ae051f9afc50dc/Future-prospects-for-palm-oil-refining-and-modifications.pdf',
'https://www.researchgate.net/post/What-are-the-benefits-of-palm-oil',
'https://www.researchgate.net/publication/281001432_Traditional_oil_palm_Elaeis_guineensis_jacq_and_its_medicinal_uses_A_review',
'https://www.researchgate.net/figure/Summary-of-effects-of-blending-palm-oil-palm-olein-and-other-vegetable-oils_tbl2_272197406',
'https://www.researchgate.net/publication/235719534_Palm_oil_Features_and_applications',
'https://www.sustainablepalmoilchoice.eu/what-is-palm-oil/',
'https://www.sustainablepalmoilchoice.eu/facts-on-palm-oil/',
'https://www.sustainablepalmoilchoice.eu/how-is-palm-oil-produced/',
'https://www.sustainablepalmoilchoice.eu/deforestation-palm-oil/',
'https://www.sustainablepalmoilchoice.eu/more-facts/',
'https://www.sustainablepalmoilchoice.eu/why-sustainable-palm-oil/',
'https://www.sustainablepalmoilchoice.eu/take-action/what-can-consumers-do/',
'https://www.sustainablepalmoilchoice.eu/palm-oil-production/',
'https://www.sustainablepalmoilchoice.eu/the-palm-oil-debate/',
'https://www.sustainablepalmoilchoice.eu/biodiversity-palm-oil/',
'https://pmc.ncbi.nlm.nih.gov/articles/PMC10236033/',
'https://pmc.ncbi.nlm.nih.gov/articles/PMC11394976/',
'https://pmc.ncbi.nlm.nih.gov/articles/PMC6770503/',
'https://pmc.ncbi.nlm.nih.gov/articles/PMC9183044/'
]

# Oil palm keywords for link filtering (to maximize relevant scraping)
OIL_PALM_KEYWORDS = ['oil palm', 'palm oil', 'elaeis guineensis', 'plantation', 'cultivation', 'processing']

# DB setup
DB_PATH = r"C:\Users\Roy\Documents\DBOilPalmmiro\oilpalmdbmiro.db"
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Pending queue: id, url, depth (FIFO via id)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pending_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            depth INTEGER DEFAULT 0
        )
    ''')
    # Visited URLs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS visited_urls (
            url TEXT PRIMARY KEY
        )
    ''')
    # Articles (categorized storage)
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
        # Filter: Same/reputable domain + oil palm keywords
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
        # If no result, commit any potential changes (none in this case) and close
        conn.commit()
        conn.close()
        return None, None # Return a tuple that can be safely unpacked

def mark_visited(url):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO visited_urls (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()

# Web Crawl & Extract Raw Data (HTML/PDF)
def fetch_raw_data(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    try:
        time.sleep(1)  # Polite delay
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        if url.lower().endswith('.pdf'):
            # Optional PDF: Uncomment if pdfplumber installed
            # with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            #     text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
            # return {'title': '', 'raw_text': text}
            return {'title': '', 'raw_text': ''}  # Skip PDF for simple
        soup = BeautifulSoup(response.text, 'lxml')
        title = soup.title.string.strip() if soup.title else ''
        # Extract raw text (body, ignore scripts/styles)
        for script in soup(["script", "style"]):
            script.decompose()
        raw_text = soup.get_text()
        return {'title': title, 'raw_text': raw_text}
    except Exception as e:
        print(f"Fetch error for {url}: {e}")
        return None

# Preprocess Data: Clean & Normalize
def preprocess_data(raw_data):
    if not raw_data or not raw_data['raw_text']:
        return ''
    text = raw_data['raw_text']
    # Clean: Remove extra whitespace, non-alpha, normalize
    text = re.sub(r'\s+', ' ', text)  # Normalize spaces
    text = re.sub(r'[^\w\s\.\,\!\?]', '', text)  # Basic clean
    text = text.strip().lower()
    return text

# Automated Classification (NLP-based: keyword scoring)
def classify_text(text):
    scores = {cat: sum(1 for kw in kws if kw in text) for cat, kws in CATEGORIES.items()}
    max_score = max(scores.values())
    if max_score == 0:
        return 'Uncategorized'
    return max(cat for cat, score in scores.items() if score == max_score)

# Quality Assurance Layer
def quality_assurance(url, title, content):
    # Language check
    try:
        lang = langdetect.detect(content)
        if lang != 'en':
            print(f"Flagged: Non-English ({lang}) for {url}")
            return False, 'Language'
    except:
        pass  # Assume English if detect fails

    # Deduplication (hash content)
    content_hash = hashlib.md5(content.encode()).hexdigest()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT hash FROM articles WHERE hash = ?", (content_hash,))
    if cursor.fetchone():
        print(f"Flagged: Duplicate for {url}")
        conn.close()
        return False, 'Duplicate'
    conn.close()

    # Source credibility
    domain = urlparse(url).netloc.lower()
    if domain not in REPUTABLE_DOMAINS:
        print(f"Flagged: Low credibility domain {domain} for {url}")
        return False, 'Source'

    # Fact check: Skipped (complex; assume passes for simple)
    # Data meets standards? Yes if all pass

    # Length check (basic standard)
    if len(content) < 100:
        print(f"Flagged: Too short for {url}")
        return False, 'Short'

    return True, content_hash

# Store in Single DB & Categorize
def store_article(url, title, content, category, content_hash):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO articles (url, title, content, category, hash)
        VALUES (?, ?, ?, ?, ?)
    ''', (url, title, content, category, content_hash))
    conn.commit()
    conn.close()
    print(f"Stored: {title[:50]}... in {category}")

# Main Pipeline (exact Miro sequence)
def main():
    init_db()
    add_seeds()
    processed_count = 0
    max_items = float('inf')  # Optional cap; set to float('inf') for unlimited

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

            # Extract Raw Data
            raw_data = fetch_raw_data(next_url)
            if not raw_data:
                continue

            # Preprocess Data
            content = preprocess_data(raw_data)

            # Automated Classification
            category = classify_text(content)

            # Quality Assurance (Delete/Flag if fails)
            meets_standards, result = quality_assurance(next_url, raw_data['title'], content)
            if not meets_standards:
                print(f"Deleted/Flagged: {result} for {next_url}")
                continue

            # Data Meets Standards? Yes → Store
            store_article(next_url, raw_data['title'], content, category, result)
            processed_count += 1

           # Add new links to pending (crawl continuation)
            soup = BeautifulSoup(requests.get(next_url).text, 'lxml')
            raw_links = [a.get('href') for a in soup.find_all('a', href=True) if a.get('href')]
            # Normalize links by removing fragments (#) and remove duplicates from the page
            clean_links = {urljoin(next_url, link).split('#')[0] for link in raw_links}
            add_to_pending(list(clean_links), depth)

            if processed_count >= max_items:
                print(f"Reached max items ({max_items}). Stopping.")
                break

            time.sleep(2)  # Delay for politeness

        except KeyboardInterrupt:
            print("\nInterrupted. Progress saved to DB. Rerun to resume.")
            break
        except Exception as e:
            print(f"Error: {e}")
            continue

    print(f"Total processed: {processed_count}")

if __name__ == "__main__":
    main()