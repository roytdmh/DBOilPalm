#!/usr/bin/env python3
"""
split_sqlite_by_category.py

Splits rows from source SQLite tables that have a category-like column into one target DB per category.

Usage examples:
    python split_sqlite_by_category.py
    python split_sqlite_by_category.py --db "C:\\Users\\Roy\\Documents\\DBOilPalmmiro\\oilpalmdbmiro.db"
    python split_sqlite_by_category.py --db "C:\\path\\to\\oilpalmdbmiro.db" --outdir "." --include-noncategory

By default:
 - Source DB: C:\\Users\\Roy\\Documents\\DBOilPalmmiro\\oilpalmdbmiro.db
 - Only tables with a detected category column are processed (safer).
"""

import sqlite3
import os
import argparse
import re
import json
import sys
from collections import defaultdict

# ----------------- Config -----------------
DEFAULT_DB_PATH = r"C:/Users/Roy/Documents/DBOilPalmmiro/oilpalmdbmiro.db"

# Candidate names for category-like columns (case-insensitive)
CANDIDATE_CATEGORY_COLUMNS = [
    "category", "categories", "cat",
    "tag", "tags",
    "topic", "topics",
    "label", "labels",
    "type", "types", "genre"
]

# Regex used to split multi-value category strings
SPLIT_REGEX = re.compile(r'[,\|;/]')

# How many rows to fetch per batch
FETCH_BATCH = 1000

# ------------------------------------------

def sanitize_filename(name: str) -> str:
    if not name:
        return "Uncategorized"
    s = str(name).strip()
    if s == "":
        return "Uncategorized"
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[\\/:*?"<>|]+', '_', s)
    return s[:200]

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def find_category_column(cols):
    names = {c.lower(): c for c in cols}
    for cand in CANDIDATE_CATEGORY_COLUMNS:
        if cand.lower() in names:
            return names[cand.lower()]
    return None

def build_create_table_sql(table_name, create_sql, src_conn):
    if create_sql and create_sql.strip() != "":
        fixed = re.sub(r'CREATE\s+TABLE', 'CREATE TABLE IF NOT EXISTS', create_sql, flags=re.IGNORECASE, count=1)
        return fixed
    cols = src_conn.execute(f'PRAGMA table_info({quote_ident(table_name)})').fetchall()
    if not cols:
        return None
    parts = []
    for c in cols:
        name = c[1]
        typ = c[2] if c[2] else "TEXT"
        notnull = " NOT NULL" if c[3] else ""
        dflt = f" DEFAULT {c[4]}" if c[4] is not None else ""
        pk = " PRIMARY KEY" if c[5] else ""
        parts.append(f'{quote_ident(name)} {typ}{notnull}{dflt}{pk}')
    sql = f'CREATE TABLE IF NOT EXISTS {quote_ident(table_name)} (\n  ' + ",\n  ".join(parts) + "\n);"
    return sql

def row_to_tuple(row, cols):
    return tuple(row[c] for c in cols)

def extract_categories_from_value(raw):
    if raw is None:
        return []
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode('utf-8', errors='ignore')
        except Exception:
            raw = str(raw)
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw if x is not None and str(x).strip()]
    s = str(raw).strip()
    if s == "":
        return []
    if s.startswith('[') and s.endswith(']'):
        try:
            parsed = json.loads(s)
            if isinstance(parsed, (list, tuple)):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
    if SPLIT_REGEX.search(s):
        parts = [p.strip() for p in SPLIT_REGEX.split(s)]
        return [p for p in parts if p]
    return [s]

def open_target_db_for_category(out_dir, category_name, cache):
    if category_name in cache:
        return cache[category_name]['conn'], cache[category_name]['created_tables']
    fname = sanitize_filename(category_name) + ".db"
    out_path = os.path.join(out_dir, fname)
    conn = sqlite3.connect(out_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = OFF;")
    cache[category_name] = {'conn': conn, 'created_tables': set(), 'path': out_path}
    return conn, cache[category_name]['created_tables']

def main(source_db_path, out_dir, include_noncategory_tables):
    if not os.path.isfile(source_db_path):
        print("ERROR: source DB not found:", source_db_path)
        sys.exit(1)
    if not out_dir:
        out_dir = os.path.dirname(os.path.abspath(source_db_path)) or "."
    os.makedirs(out_dir, exist_ok=True)

    src_conn = sqlite3.connect(source_db_path)
    src_conn.row_factory = sqlite3.Row
    cur = src_conn.cursor()

    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    all_tables = cur.fetchall()
    if not all_tables:
        print("No user tables found in the DB.")
        return

    tables = []
    for name, create_sql in all_tables:
        cols_info = src_conn.execute(f'PRAGMA table_info({quote_ident(name)})').fetchall()
        cols = [c[1] for c in cols_info]
        cat_col = find_category_column(cols)
        if cat_col is None and not include_noncategory_tables:
            print(f'Skipping table "{name}" (no category column detected).')
            continue
        create_stmt = build_create_table_sql(name, create_sql, src_conn)
        tables.append({
            'name': name,
            'columns': cols,
            'category_column': cat_col,
            'create_sql': create_stmt
        })

    if not tables:
        print("No tables selected for processing. Exiting.")
        src_conn.close()
        return

    print(f"Processing {len(tables)} table(s)...")
    category_cache = {}
    row_counts_by_category = defaultdict(int)
    inserted_counts = defaultdict(lambda: defaultdict(int))

    for t in tables:
        tname = t['name']
        cols = t['columns']
        cat_col = t['category_column']
        create_sql = t['create_sql']

        print(f'Processing table "{tname}" (columns: {len(cols)}). Category column: {cat_col}')
        if not cols:
            print(f'  Skipping table "{tname}" (no columns).')
            continue

        col_list = ", ".join(quote_ident(c) for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        insert_stmt_template = f'INSERT OR REPLACE INTO {quote_ident(tname)} ({col_list}) VALUES ({placeholders})'

        sel_cur = src_conn.execute(f'SELECT * FROM {quote_ident(tname)};')
        while True:
            rows = sel_cur.fetchmany(FETCH_BATCH)
            if not rows:
                break
            for row in rows:
                categories = []
                if cat_col:
                    raw = row[cat_col]
                    cats = extract_categories_from_value(raw)
                    categories = cats if cats else ["Uncategorized"]
                else:
                    # table had no category column but include_noncategory_tables==True
                    categories = ["Uncategorized"]

                for cat in categories:
                    conn, created = open_target_db_for_category(out_dir, cat, category_cache)
                    if tname not in created:
                        if create_sql:
                            try:
                                conn.executescript(create_sql if "IF NOT EXISTS" in create_sql.upper() else re.sub(r'CREATE\s+TABLE', 'CREATE TABLE IF NOT EXISTS', create_sql, flags=re.IGNORECASE, count=1))
                            except Exception as e:
                                alt_sql = build_create_table_sql(tname, None, src_conn)
                                if alt_sql:
                                    conn.executescript(alt_sql)
                                else:
                                    print(f"WARNING: Could not create table {tname} in target DB for category '{cat}': {e}")
                                    continue
                        else:
                            alt_sql = build_create_table_sql(tname, None, src_conn)
                            if alt_sql:
                                conn.executescript(alt_sql)
                            else:
                                print(f"WARNING: No CREATE SQL available for table {tname}. Skipping.")
                                continue
                        created.add(tname)

                    vals = row_to_tuple(row, cols)
                    try:
                        conn.execute(insert_stmt_template, vals)
                        inserted_counts[cat][tname] += 1
                        row_counts_by_category[cat] += 1
                    except sqlite3.IntegrityError:
                        try:
                            conn.execute('INSERT OR IGNORE INTO ' + quote_ident(tname) + f' ({col_list}) VALUES ({placeholders})', vals)
                            inserted_counts[cat][tname] += 1
                            row_counts_by_category[cat] += 1
                        except Exception as e:
                            print(f"Error inserting into {cat}/{tname}: {e}")
            for info in category_cache.values():
                try:
                    info['conn'].commit()
                except Exception:
                    pass

    for cat, info in category_cache.items():
        try:
            info['conn'].commit()
            info['conn'].close()
        except Exception:
            pass

    src_conn.close()

    print("\nDone. Summary:")
    total_files = len(category_cache)
    print(f"  Created {total_files} category DB file(s) in: {out_dir}")
    for cat, info in category_cache.items():
        print(f"   - {cat}: {row_counts_by_category.get(cat,0)} rows -> {info.get('path')}")
        tbls = inserted_counts.get(cat, {})
        if tbls:
            s = ", ".join(f"{tbl}:{count}" for tbl, count in tbls.items())
            print(f"      tables: {s}")
    print("\nNotes:")
    print(" - Rows with no category were placed in 'Uncategorized.db'.")
    print(" - If your DB stores tags in separate tables (many-to-many), consider materializing categories first or ask for an adapted script.")
    print(" - Check a few produced DBs with DB Browser for SQLite to confirm everything looks right.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Split a sqlite DB into per-category DB files.")
    parser.add_argument('--db', help='Path to source sqlite DB', default=DEFAULT_DB_PATH)
    parser.add_argument('--outdir', help='Output directory for category DBs (default: same folder as source DB)', default=None)
    parser.add_argument('--include-noncategory', help='Also include tables that do NOT have a detected category column (they go to Uncategorized.db)', action='store_true')
    args = parser.parse_args()
    main(args.db, args.outdir, args.include_noncategory)
