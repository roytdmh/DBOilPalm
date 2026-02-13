# 🌴 Modular Web Crawler System  
### Oil Palm Intelligence Data Engine

A production-ready, modular web crawler designed for structured oil palm data acquisition, classification, validation, and database partitioning.

---

## 🚀 Overview

This system:

- Crawls reputable research and agricultural domains  
- Extracts structured article data  
- Classifies content into agronomy categories  
- Performs quality validation  
- Stores data in a primary SQLite database  
- Optionally mirrors content into per-category database files  
- Supports post-processing database splitting  

Built for reliability, resume safety, and modular scalability.

---

## 🧠 Architecture

```
Seed URLs
   ↓
Crawl Engine
   ↓
HTML Parsing
   ↓
Text Normalization
   ↓
Keyword Classification
   ↓
Quality Assurance
   ↓
Main Database (SQLite)
   ↓
Optional Category DB Mirroring
```

---

## 📂 Project Structure

```text
OilPalmCrawler/
│
├── ScraperScriptOilPalm.py
├── ScraperScriptOilPalm_with_mirror.py
├── split_sqlite_by_category.py
├── requirements.txt
└── README.md
```

---

## 🗄 Database Design

### Main Database

```
oilpalmdbmiro.db
```

Tables:

```sql
pending_urls
visited_urls
articles
```

### Articles Schema

```sql
CREATE TABLE articles (
    url TEXT PRIMARY KEY,
    title TEXT,
    content TEXT,
    category TEXT,
    scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    hash TEXT UNIQUE
);
```

---

## 🗂 Classification Categories

- Cultivation  
- Processing  
- Environmental Impact  
- Market Trends  
- Plantation Management  
- Uncategorized  

Classification is keyword-score based.

---

# ⚙️ Installation

## 1️⃣ Create Virtual Environment (Recommended)

```bash
python -m venv venv
venv\Scripts\activate
```

## 2️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

Dependencies:

- requests  
- beautifulsoup4  
- lxml  
- langdetect  
- pdfplumber  

---

# ▶ Running the Scraper

---

## 🔹 Standard Mode (Single Database)

```bash
python ScraperScriptOilPalm.py
```

Stores everything in:

```
oilpalmdbmiro.db
```

---

## 🔹 Mirror Mode (Recommended)

```bash
python ScraperScriptOilPalm_with_mirror.py --mirror-category-dbs
```

This will:

- Store in main database  
- Also create one database per category:

```
Cultivation.db
Processing.db
Environmental_Impact.db
Market_Trends.db
Plantation_Management.db
Uncategorized.db
```

Each contains its own `articles` table.

---

# 🔁 Resume Safety

The crawler is fully resumable.

If interrupted:

```bash
Ctrl + C
```

Then simply rerun:

```bash
python ScraperScriptOilPalm_with_mirror.py --mirror-category-dbs
```

The system:

- Skips visited URLs  
- Prevents duplicates via content hash  
- Continues from queue  

No data loss.

---

# 🧪 Quality Assurance Rules

An article is rejected if:

- Not English  
- Duplicate content  
- Domain not in whitelist  
- Content too short  

This ensures high data purity.

---

# 🛠 Splitting an Existing Database

If you already have a populated database and want to split by category:

```bash
python split_sqlite_by_category.py
```

Or specify path:

```bash
python split_sqlite_by_category.py --db "C:\path\to\oilpalmdbmiro.db"
```

This creates one `.db` file per category.

Rows without categories go to:

```
Uncategorized.db
```

---

# 📂 Output Location

Default database path:

```
C:\Users\Roy\Documents\DBOilPalmmiro\oilpalmdbmiro.db
```

Category DBs are created in the same folder.

Modify `DB_PATH` in the script to change this.

---

# 🧩 Configuration Points

Inside the script you may modify:

- `SEED_URLS`
- `REPUTABLE_DOMAINS`
- `CATEGORIES`
- Crawl depth
- Delay timing
- DB path

---

# 🛡 Design Strengths

✔ Modular  
✔ Resume-safe  
✔ Deduplication via hash  
✔ Domain credibility filtering  
✔ Optional DB mirroring  
✔ Post-processing splitter  
✔ SQLite WAL optimization  

---

# 📊 Typical Workflow

### Initial Large Crawl

```bash
python ScraperScriptOilPalm_with_mirror.py --mirror-category-dbs
```

### Daily Update Run

Run the same command.  
Only new URLs will be processed.

### Post-Processing Split

```bash
python split_sqlite_by_category.py
```

---

# 🔮 Future Enhancements

- Scheduler integration (cron / Task Scheduler)
- PostgreSQL support
- Async crawling
- Dashboard analytics
- LLM-based semantic classification

---

# 👤 Maintainer

Roy Obiri-Yeboah  
Oil Palm Intelligence System  

---


---

