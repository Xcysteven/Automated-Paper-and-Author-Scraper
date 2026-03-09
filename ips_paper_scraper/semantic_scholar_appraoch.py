import sqlite3
import requests
import time
import urllib.parse

class SemanticScholarPipeline:
    def __init__(self, db_name="neurips_research.db"):
        self.db_name = db_name
        self.base_url = "https://api.semanticscholar.org/graph/v1/paper/search"

    def run_pipeline(self, limit=100):
        print(f"--- Starting Semantic Scholar API Pipeline (Processing {limit} papers) ---")
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute("SELECT paper_id, title FROM papers WHERE is_processed = 0 ORDER BY paper_id ASC LIMIT ?", (limit,))
        papers_to_process = cursor.fetchall()
        
        if not papers_to_process:
            print("🎉 All papers have been processed!")
            conn.close()
            return False

        # Use a requests session for faster connection pooling
        session = requests.Session()

        for paper_id, title in papers_to_process:
            print(f"\n🔍 Searching API for: '{title[:60]}...'")
            
            try:
                self._process_single_paper(session, cursor, paper_id, title)
                conn.commit()
                
                # Semantic Scholar's unauthenticated rate limit is ~1 request per second
                # We wait 1.5 seconds to be perfectly safe and polite.
                time.sleep(1.5)
                
            except Exception as e:
                print(f"   💥 API Error: {e}")
                if "429" in str(e):
                    print("   🛑 Hit API rate limit. Sleeping for 60 seconds...")
                    time.sleep(60)
                else:
                    print("   🛑 Unexpected error. Stopping batch to be safe.")
                    break

        conn.close()
        print("\n--- API Batch Complete ---")
        return True

    def _process_single_paper(self, session, cursor, paper_id, title):
        # We ask the API to give us exactly the fields we need, saving massive amounts of data
        params = {
            "query": title,
            "limit": 1,
            "fields": "title,abstract,authors.name,authors.url,authors.homepage,authors.citationCount"
        }
        
        response = session.get(self.base_url, params=params, timeout=10)
        
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text}")
            
        data = response.json()
        
        # Check if the API actually found a matching paper
        if "data" not in data or len(data["data"]) == 0:
            print("   ⚠️ No results found in Semantic Scholar for this title.")
            cursor.execute("UPDATE papers SET is_processed = 1 WHERE paper_id = ?", (paper_id,))
            return

        paper_data = data["data"][0]
        
        # 1. Extract Abstract
        abstract = paper_data.get("abstract", "No abstract snippet available")
        if not abstract: 
            abstract = "No abstract snippet available"
            
        cursor.execute("UPDATE papers SET abstract = ?, is_processed = 1 WHERE paper_id = ?", (abstract, paper_id))
        
        # 2. Extract Authors
        authors = paper_data.get("authors", [])
        if authors:
            print(f"   👥 Found {len(authors)} authors via API.")
            for author in authors:
                author_name = author.get("name")
                # We save the Semantic Scholar URL into our 'gs_url' column so we don't have to rebuild the DB
                s2_url = author.get("url") 
                homepage = author.get("homepage")
                citations = author.get("citationCount", 0)
                
                # Insert author into database
                cursor.execute("""
                    INSERT OR IGNORE INTO authors (name, gs_url, homepage_url, citations) 
                    VALUES (?, ?, ?, ?)
                """, (author_name, s2_url, homepage, citations))
                
                # Retrieve the author_id to link them to the paper
                cursor.execute("SELECT author_id FROM authors WHERE name = ?", (author_name,))
                author_row = cursor.fetchone()
                
                if author_row:
                    author_id = author_row[0]
                    cursor.execute("""
                        INSERT OR IGNORE INTO paper_authors (paper_id, author_id) 
                        VALUES (?, ?)
                    """, (paper_id, author_id))
        else:
            print("   ⚠️ No authors listed for this paper in the API.")

if __name__ == "__main__":
    pipeline = SemanticScholarPipeline()
    
    # Run autonomously! 1.5s delay * 100 papers = ~2.5 minutes per batch.
    batch = 1
    while True:
        print(f"\n🚀 --- INITIATING API BATCH {batch} ---")
        has_more = pipeline.run_pipeline(limit=5)
        
        if not has_more:
            break
            
        print("\n🛌 Batch complete. Taking a quick 10-second breather...")
        time.sleep(10)
        batch += 1