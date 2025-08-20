import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import sqlite3

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
DB_FILE = "pubmed_rochester.db"

# Query parameters
one_year_ago = (datetime.utcnow() - timedelta(days=365)).strftime("%Y/%m/%d")
today = datetime.utcnow().strftime("%Y/%m/%d")

query = f'(("Rochester MN"[AD]) OR ("Rochester Minnesota"[AD])) AND ("{one_year_ago}"[PDAT] : "{today}"[PDAT])'


def get_pmids():
    """Fetch all PMIDs matching query from the last year."""
    pmids = []
    retmax = 200
    retstart = 0

    while True:
        params = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": retmax,
            "retstart": retstart
        }
        r = requests.get(BASE_URL + "esearch.fcgi", params=params)
        r.raise_for_status()
        data = r.json()

        ids = data.get("esearchresult", {}).get("idlist", [])
        if not ids:
            break
        pmids.extend(ids)

        retstart += retmax
        if retstart >= int(data["esearchresult"]["count"]):
            break

        time.sleep(0.34)  # ✅ respect NCBI rate limit

    return pmids


def fetch_details(pmids):
    """Fetch article details with EFetch (XML)."""
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml"
    }
    r = requests.get(BASE_URL + "efetch.fcgi", params=params)
    r.raise_for_status()
    time.sleep(0.34)  # ✅ rate limit
    return r.text


def create_table(conn):
    """Create SQLite table if not exists."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS pubmed_articles (
        pmid TEXT PRIMARY KEY,
        publication_year TEXT,
        publication_date TEXT,
        first_author TEXT,
        all_authors TEXT,
        title TEXT,
        issn TEXT,
        doi TEXT,
        pages TEXT,
        issue TEXT,
        volume TEXT,
        journal TEXT,
        journal_abbreviation TEXT,
        abstract TEXT
    );
    """)
    conn.commit()


def insert_article(conn, data):
    """Insert article (idempotent via INSERT OR IGNORE)."""
    conn.execute("""
    INSERT OR IGNORE INTO pubmed_articles
    (pmid, publication_year, publication_date, first_author, all_authors,
     title, issn, doi, pages, issue, volume, journal, journal_abbreviation, abstract)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()


def parse_articles(xml_data, conn):
    """Parse PubMed XML and insert into DB."""
    root = ET.fromstring(xml_data)

    for article in root.findall(".//PubmedArticle"):
        pmid = article.findtext(".//PMID")

        # Journal info
        journal = article.findtext(".//Journal/Title", "") or ""
        journal_abbr = article.findtext(".//Journal/ISOAbbreviation", "") or ""
        issn = article.findtext(".//Journal/ISSN", "") or ""

        # Publication details
        pub_year = article.findtext(".//JournalIssue/PubDate/Year") or ""
        pub_date = article.findtext(".//PubDate/MedlineDate") or ""
        volume = article.findtext(".//JournalIssue/Volume") or ""
        issue = article.findtext(".//JournalIssue/Issue") or ""
        pages = article.findtext(".//Pagination/MedlinePgn") or ""
        doi = ""
        for el in article.findall(".//ArticleId"):
            if el.attrib.get("IdType") == "doi":
                doi = el.text

        # Title & Abstract
        title = article.findtext(".//ArticleTitle", "") or ""
        abstract_text = " ".join([t.text for t in article.findall(".//Abstract/AbstractText") if t.text]) or ""

        # Authors
        authors = []
        first_author = ""
        for author in article.findall(".//Author"):
            lastname = author.findtext("LastName")
            forename = author.findtext("ForeName")
            name = " ".join(filter(None, [forename, lastname]))
            if name:
                authors.append(name)
        if authors:
            first_author = authors[0]
        all_authors = ", ".join(authors)

        # Insert into DB
        insert_article(conn, (
            pmid, pub_year, pub_date, first_author, all_authors,
            title, issn, doi, pages, issue, volume,
            journal, journal_abbr, abstract_text
        ))


if __name__ == "__main__":
    conn = sqlite3.connect(DB_FILE)
    create_table(conn)

    pmids = get_pmids()
    print(f"Found {len(pmids)} articles.")

    batch_size = 200
    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i+batch_size]
        xml_data = fetch_details(batch)
        parse_articles(xml_data, conn)

    conn.close()
    print(f"✅ Saved {len(pmids)} articles into {DB_FILE}")
