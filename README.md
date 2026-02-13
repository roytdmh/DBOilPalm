🌴 Oil Palm Intelligence Scraper (v1 – Category Mirroring Edition)

A modular oil palm web scraping system that:

Crawls reputable agricultural & research sources

Extracts and cleans structured article data

Automatically classifies content into agronomy categories

Stores data in a main SQLite database

Optionally mirrors articles into separate category-specific databases

Allows post-processing splitting of an existing database

📦 Project Files
ScraperScriptOilPalm.py                  # Core scraper (single DB)
ScraperScriptOilPalm_with_mirror.py      # Scraper with per-category DB mirroring
split_sqlite_by_category.py              # Post-processing DB splitter
requirements.txt                         # Python dependencies

🧠 System Overview

The scraper follows this pipeline:

Seed URLs
   ↓
Crawl (reputable domains only)
   ↓
Extract HTML
   ↓
Clean & Normalize Text
   ↓
Keyword-Based Classification
   ↓
Quality Assurance
   ↓
Store in Database

🗂 Categories

The scraper classifies content into:

Cultivation

Processing

Environmental Impact

Market Trends

Plantation Management

Uncategorized

Classification is keyword-score based.

🗄 Database Structure

Main database file:

oilpalmdbmiro.db


Tables created automatically:

pending_urls

visited_urls

articles

Articles table schema:

url TEXT PRIMARY KEY
title TEXT
content TEXT
category TEXT
scraped_date TIMESTAMP
hash TEXT UNIQUE

🚀 Installation
1️⃣ Create Virtual Environment (Recommended)
python -m venv venv
venv\Scripts\activate

2️⃣ Install Dependencies
pip install -r requirements.txt


Dependencies:

requests 

requirements

beautifulsoup4 

requirements

lxml 

requirements

langdetect 

requirements

pdfplumber 

requirements

▶ Running the Scraper
🔹 Option 1 — Standard Mode (Single DB Only)
python ScraperScriptOilPalm.py


Behavior:

Stores everything in oilpalmdbmiro.db

No per-category DB files created

🔹 Option 2 — Mirror Mode (Recommended)
python ScraperScriptOilPalm_with_mirror.py --mirror-category-dbs


Behavior:

Stores data in main DB

Also creates separate DB files:

Cultivation.db
Processing.db
Market_Trends.db
Environmental_Impact.db
Plantation_Management.db
Uncategorized.db


Each contains its own articles table.

🔁 Resuming After Interruption

The scraper is fully resumable.

If stopped:

Ctrl + C


Then simply rerun:

python ScraperScriptOilPalm_with_mirror.py --mirror-category-dbs


It will:

Continue from pending_urls

Skip already visited URLs

Prevent duplicate articles (via hash check)

No data loss occurs.

🛠 Splitting an Existing Database

If you already have a populated oilpalmdbmiro.db and want to split it into category-based DB files:

python split_sqlite_by_category.py


Or specify DB path:

python split_sqlite_by_category.py --db "C:\Users\Roy\Documents\DBOilPalmmiro\oilpalmdbmiro.db"


This creates:

CategoryName.db


Rows without a category go to:

Uncategorized.db

🧪 Quality Assurance Rules

An article is rejected if:

Not English (langdetect)

Duplicate content (hash match)

Domain not in whitelist

Content length < 100 characters

This ensures high data purity.

🧯 Safe Restart & Recovery

If system crashes:

Do NOT delete the DB.

Just rerun the script.

Queue resumes automatically.

If DB becomes corrupted:

Restore from backup

Or rerun scraper from scratch

📂 Output Location

Database location (hardcoded in script):

C:\Users\Roy\Documents\DBOilPalmmiro\oilpalmdbmiro.db


Category DBs are created in the same folder.

You may modify DB_PATH inside the script to change this.

⚙ Configuration Points You Can Modify

Inside script:

SEED_URLS → Expand sources

REPUTABLE_DOMAINS → Adjust whitelist

CATEGORIES → Modify classification logic

max_depth in add_to_pending

Politeness delay (time.sleep())

🧩 Architecture Strengths

✔ Modular
✔ Resume-safe
✔ Deduplication built-in
✔ Domain credibility filtering
✔ Optional DB mirroring
✔ Post-processing splitter

🧭 Typical Workflow
First Time Setup

Run mirror version:

python ScraperScriptOilPalm_with_mirror.py --mirror-category-dbs

Daily Data Expansion

Run again. It will only fetch new URLs.

After Large Crawl

Use:

split_sqlite_by_category.py


if you want physical separation by category.

👤 Maintainer

Roy Obiri-Yeboah
Oil Palm Data Intelligence System
