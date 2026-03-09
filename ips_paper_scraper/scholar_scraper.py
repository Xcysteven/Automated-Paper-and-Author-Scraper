import sqlite3
import time
import random
import urllib.parse
from seleniumbase import Driver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class GoogleScholarPipeline:
    def __init__(self, db_name="neurips_research.db"):
        self.db_name = db_name
        self.base_url = "https://scholar.google.com"
        self.driver = None

    def _start_browser(self):
        self._kill_browser()
        print("   🌐 Starting browser...")
        self.driver = Driver(uc=True, headless=False)
        self.driver.uc_open_with_reconnect(self.base_url, reconnect_time=4)
        time.sleep(2)

    def _kill_browser(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def _handle_captcha(self):
        """Google Scholar loves reCAPTCHA. We try to bypass it, or pause."""
        if "sorry" in self.driver.current_url.lower() or self.driver.is_element_present('div#recaptcha'):
            print("\n   🚨 GOOGLE SCHOLAR CAPTCHA DETECTED!")
            try:
                self.driver.uc_gui_click_captcha()
                time.sleep(5)
            except:
                pass
            
            # If still blocked, we must Hard Reset to drop the session
            if "sorry" in self.driver.current_url.lower():
                raise Exception("Unresolvable CAPTCHA block.")

    def run_pipeline(self, limit=50):
        """Fetches unprocessed papers and searches them on Scholar."""
        print(f"--- Starting Google Scholar Pipeline (Processing {limit} papers) ---")
        
        # 1. Connect to DB and get un-processed papers
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        cursor.execute("SELECT paper_id, title FROM papers WHERE is_processed = 0 LIMIT ?", (limit,))
        papers_to_process = cursor.fetchall()
        
        if not papers_to_process:
            print("🎉 All papers have been processed!")
            conn.close()
            return False

        self._start_browser()
        session_search_count = 0  # NEW: Track searches per browser window

        for paper_id, title in papers_to_process:
            print(f"\n🔍 Searching: '{title[:50]}...'")
            
            try:
                self._process_single_paper(cursor, paper_id, title)
                conn.commit()  # Save data immediately after each paper
                
                session_search_count += 1
                
                # --- PREEMPTIVE HARD RESET LOGIC ---
                if session_search_count >= 5:
                    print("   🔄 Preemptive Hard Reset to dodge Google's CAPTCHA...")
                    self._start_browser()
                    session_search_count = 0  # Reset the counter for the new window
                    time.sleep(3)  # Short pause to let the new browser breathe
                else:
                    # Normal pacing if we aren't resetting
                    delay = random.uniform(4.0, 7.0)
                    print(f"   ⏳ Pacing: Waiting {delay:.1f} seconds...")
                    time.sleep(delay)
                
            except Exception as e:
                print(f"   💥 Pipeline error or block: {e}")
                print("   🔄 Initiating Emergency Hard Reset...")
                self._start_browser()
                session_search_count = 0  # Reset counter on emergency restarts too
                time.sleep(3)

        conn.close()
        self._kill_browser()
        print("\n--- Pipeline Batch Complete ---")

    def _process_single_paper(self, cursor, paper_id, title):
        # Construct the exact search URL
        encoded_title = urllib.parse.quote(title)
        search_url = f"{self.base_url}/scholar?hl=en&q={encoded_title}"
        
        self.driver.uc_open_with_reconnect(search_url, reconnect_time=3)
        self._handle_captcha()
        
        # Check if we got results
        html = self.driver.get_page_source()
        soup = BeautifulSoup(html, "html.parser")
        
        # Find the first search result container
        first_result = soup.select_one('.gs_ri')
        
        if not first_result:
            print("   ⚠️ No results found on Google Scholar for this title.")
            cursor.execute("UPDATE papers SET is_processed = 1 WHERE paper_id = ?", (paper_id,))
            return

        # 1. Extract Abstract Snippet
        abstract_el = first_result.select_one('.gs_rs')
        abstract = abstract_el.text.strip().replace('\n', ' ') if abstract_el else "No abstract snippet available"
        
        # Update the paper with the abstract and mark as processed
        cursor.execute("UPDATE papers SET abstract = ?, is_processed = 1 WHERE paper_id = ?", (abstract, paper_id))
        
        # 2. Extract Authors with Google Scholar profiles
        # Google scholar links authors like: <a href="/citations?user=XXXX">Name</a>
        author_links = first_result.select('.gs_a a[href*="/citations?user="]')
        
        if author_links:
            print(f"   👥 Found {len(author_links)} linked authors.")
            for author_link in author_links:
                author_name = author_link.text.strip()
                gs_url = self.base_url + author_link['href']
                
                # Insert author (IGNORE if they already exist in the database from another paper)
                cursor.execute("""
                    INSERT OR IGNORE INTO authors (name, gs_url) 
                    VALUES (?, ?)
                """, (author_name, gs_url))
                
                # Get the author_id (whether we just inserted them, or they already existed)
                cursor.execute("SELECT author_id FROM authors WHERE gs_url = ?", (gs_url,))
                author_row = cursor.fetchone()
                
                if author_row:
                    author_id = author_row[0]
                    # Link this author to the paper
                    cursor.execute("""
                        INSERT OR IGNORE INTO paper_authors (paper_id, author_id) 
                        VALUES (?, ?)
                    """, (paper_id, author_id))
        else:
            print("   ⚠️ No linked Google Scholar profiles found for these authors.")

if __name__ == "__main__":
    pipeline = GoogleScholarPipeline()
    
    batch = 1
    while True:
        print(f"\n🚀 --- INITIATING BATCH {batch} ---")
        
        # Capture the True/False signal
        has_more_papers = pipeline.run_pipeline(limit=200) 
        
        if not has_more_papers:
            print("\n🛑 Database is empty. Shutting down the automation entirely.")
            break # This kills the while loop immediately!
            
        print("\n🛌 Batch complete. Sleeping for 1 hour to cool down the IP...")
        time.sleep(3600)
        batch += 1