import sqlite3
import requests
import time
import random
import urllib.parse
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
from seleniumbase import Driver

class Phase3EnrichmentEngine:
    def __init__(self, db_path="neurips_research.db"):
        self.db_path = db_path
        self.base_scholar_url = "https://scholar.google.com"
        self._upgrade_database()

    def _upgrade_database(self):
        """Automatically adds the necessary Phase 3 columns to your database."""
        print("🛠️ Checking database schema...")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Helper function to add columns safely
        def add_column(table, column, data_type):
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {data_type}")
                print(f"   [+] Added column '{column}' to '{table}'")
            except sqlite3.OperationalError:
                pass # Column already exists
                
        add_column("authors", "email", "TEXT")
        add_column("authors", "affiliation", "TEXT")
        add_column("papers", "or_processed", "INTEGER DEFAULT 0") # Tracks OpenReview status
        
        conn.commit()
        conn.close()

    def run_openreview_sync(self, batch_size=50):
        """Fast API pass to find ghost authors and emails."""
        print(f"\n🚀 [STEP 1] Starting OpenReview API Sync (Batch limit: {batch_size})")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Grab papers that haven't been checked against OpenReview yet
        cursor.execute("SELECT paper_id, title FROM papers WHERE is_processed = 1 AND or_processed = 0 LIMIT ?", (batch_size,))
        papers = cursor.fetchall()

        if not papers:
            print("   ✅ All processed papers have already been cross-validated with OpenReview.")
            conn.close()
            return

        for paper_id, title in papers:
            encoded_title = urllib.parse.quote(title)
            url = f"https://api2.openreview.net/notes?content.title={encoded_title}"
            
            try:
                response = requests.get(url, timeout=10)
                data = response.json()
                
                if data.get('notes'):
                    best_match = None
                    best_ratio = 0
                    
                    for note in data['notes']:
                        if 'content' in note and 'title' in note['content']:
                            api_title = note['content']['title']['value']
                            ratio = SequenceMatcher(None, title.lower(), api_title.lower()).ratio()
                            if ratio >= 0.85 and ratio > best_ratio:
                                best_ratio = ratio
                                best_match = note
                                
                    if best_match:
                        content = best_match['content']
                        or_authors = content.get('authors', {}).get('value', [])
                        or_authorids = content.get('authorids', {}).get('value', [])
                        
                        print(f"   ✅ API Match: {title[:40]}... ({len(or_authors)} authors)")
                        
                        for i, author_name in enumerate(or_authors):
                            contact_id = or_authorids[i] if i < len(or_authorids) else None
                            email = contact_id if contact_id and '@' in contact_id else None
                            
                            cursor.execute("SELECT author_id, email FROM authors WHERE name = ?", (author_name,))
                            row = cursor.fetchone()
                            
                            if row:
                                author_id = row[0]
                                if email and not row[1]:
                                    cursor.execute("UPDATE authors SET email = ? WHERE author_id = ?", (email, author_id))
                            else:
                                cursor.execute("INSERT INTO authors (name, email) VALUES (?, ?)", (author_name, email))
                                author_id = cursor.lastrowid
                                print(f"      👻 Injected missing ghost author: {author_name}")
                                
                            cursor.execute("INSERT OR IGNORE INTO paper_authors (paper_id, author_id) VALUES (?, ?)", (paper_id, author_id))
                    else:
                        print(f"   ⚠️ Title mismatch on OpenReview: {title[:40]}...")
                else:
                    print(f"   ❌ Not found on OpenReview: {title[:40]}...")
                
                # Mark as processed so we don't query the API for this paper again
                cursor.execute("UPDATE papers SET or_processed = 1 WHERE paper_id = ?", (paper_id,))
                conn.commit()
                time.sleep(0.5) # Be polite to the API server
                
            except Exception as e:
                print(f"   🚨 Error processing {title[:30]}: {e}")

        conn.close()

    def run_scholar_rescue(self, batch_size=12):
        """Stealth pass to find URLs and Affiliations for missing authors."""
        print(f"\n🕵️‍♂️ [STEP 2] Starting Scholar Rescue Mission (Batch limit: {batch_size})")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Find authors who don't have a Google Scholar URL yet
        cursor.execute("SELECT author_id, name FROM authors WHERE gs_url IS NULL LIMIT ?", (batch_size,))
        missing_authors = cursor.fetchall()
        
        if not missing_authors:
            print("   🎉 No missing URLs found! Your database is fully enriched.")
            conn.close()
            return

        print(f"   🌐 Booting stealth browser for {len(missing_authors)} missing authors...")
        driver = Driver(uc=True, user_data_dir="chrome_profile", headless=False)
        time.sleep(random.uniform(2, 4))

        try:
            for i, (author_id, name) in enumerate(missing_authors):
                print(f"   [{i+1}/{len(missing_authors)}] 🔍 Searching Author: {name}")
                
                encoded_name = urllib.parse.quote(name)
                search_url = f"{self.base_scholar_url}/citations?view_op=search_authors&hl=en&mauthors={encoded_name}"
                
                driver.uc_open_with_reconnect(search_url, reconnect_time=3)
                time.sleep(random.uniform(4, 8)) 
                
                soup = BeautifulSoup(driver.get_page_source(), "html.parser")
                
                if "detected unusual traffic" in soup.text or 'id="gs_captcha_ccl"' in soup.text:
                    print("   🚨 CAPTCHA triggered! Google caught us. Stopping rescue mission.")
                    break
                
                first_profile = soup.select_one('.gsc_1usr')
                
                if first_profile:
                    name_link = first_profile.select_one('.gs_ai_name a')
                    if name_link and name_link.get('href'):
                        clean_url = self.base_scholar_url + name_link['href'].split('&')[0] 
                        
                        affil_el = first_profile.select_one('.gs_ai_aff')
                        affiliation = affil_el.text.strip() if affil_el else None
                        
                        print(f"      ✅ URL: {clean_url}")
                        if affiliation: print(f"      🏛️ Affiliation: {affiliation}")
                            
                        cursor.execute("UPDATE authors SET gs_url = ?, affiliation = ? WHERE author_id = ?", (clean_url, affiliation, author_id))
                    else:
                        print("      ⚠️ Profile card found, but no URL extracted.")
                else:
                    print("      ❌ No Scholar profile exists.")
                    cursor.execute("UPDATE authors SET gs_url = 'NO_PROFILE_FOUND' WHERE author_id = ?", (author_id,))
                
                conn.commit()

        except Exception as e:
            print(f"   🚨 Error during scraping: {e}")
            
        finally:
            print("\n🛑 Closing stealth browser...")
            driver.quit()
            conn.close()
            print("💤 Mission complete. Take a cooldown break before running again!")

if __name__ == "__main__":
    engine = Phase3EnrichmentEngine()
    
    # Step 1: Rapid-fire API sync (Safe to do 50 at a time)
    engine.run_openreview_sync(batch_size=50)
    
    # Step 2: Stealth Browser URL rescue (Strictly limit to 12 to avoid bans)
    engine.run_scholar_rescue(batch_size=12)