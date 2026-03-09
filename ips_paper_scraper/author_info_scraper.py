import sqlite3
import time
import random
import re
from seleniumbase import Driver
from bs4 import BeautifulSoup

class AuthorProfileCrawler:
    def __init__(self, db_name="neurips_research.db"):
        self.db_name = db_name
        self.driver = None
        self._ensure_specializations_table()

    def _ensure_specializations_table(self):
        """Creates the specializations table just in case Phase 1 missed it."""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS specializations (
                author_id INTEGER,
                keyword TEXT,
                UNIQUE(author_id, keyword)
            )
        ''')
        conn.commit()
        conn.close()

    def _start_browser(self):
        self._kill_browser()
        print("   🌐 Starting fresh browser window...")
        self.driver = Driver(uc=True, headless=False) # Set to True later for background running
        time.sleep(2)

    def _kill_browser(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def _handle_captcha(self):
        if "sorry" in self.driver.current_url.lower() or self.driver.is_element_present('div#recaptcha'):
            print("\n   🚨 CAPTCHA DETECTED! Attempting bypass...")
            try:
                self.driver.uc_gui_click_captcha()
                time.sleep(5)
            except:
                pass
            if "sorry" in self.driver.current_url.lower():
                raise Exception("Unresolvable CAPTCHA block.")

    def run_crawler(self, limit=50):
        print(f"--- Starting Author Profile Crawler (Processing up to {limit} authors) ---")
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Grab authors who have a Google Scholar URL but haven't been crawled yet
        cursor.execute("SELECT author_id, name, gs_url FROM authors WHERE is_crawled = 0 AND gs_url IS NOT NULL LIMIT ?", (limit,))
        authors_to_process = cursor.fetchall()
        
        if not authors_to_process:
            print("🎉 All available authors have been crawled!")
            conn.close()
            return False # Signal that we are out of authors

        self._start_browser()
        session_profile_count = 0 # Track profiles per browser window

        for author_id, name, gs_url in authors_to_process:
            print(f"\n👤 Crawling Profile: {name}")
            
            try:
                self._process_single_author(cursor, author_id, gs_url)
                conn.commit()
                
                session_profile_count += 1
                
                # --- PREEMPTIVE HARD RESET LOGIC ---
                if session_profile_count >= 5:
                    print("   🔄 Preemptive Hard Reset to dodge Google's CAPTCHA...")
                    self._start_browser()
                    session_profile_count = 0  # Reset the counter
                    time.sleep(3) 
                else:
                    # Normal pacing
                    delay = random.uniform(3.0, 5.0)
                    print(f"   ⏳ Waiting {delay:.1f} seconds...")
                    time.sleep(delay)
                
            except Exception as e:
                print(f"   💥 Error or block: {e}")
                print("   🔄 Initiating Emergency Hard Reset...")
                self._start_browser()
                session_profile_count = 0 # Reset counter on emergency too
                time.sleep(3)

        conn.close()
        self._kill_browser()
        print("\n--- Crawler Batch Complete ---")
        return True # Signal that there might be more authors left

    def _process_single_author(self, cursor, author_id, gs_url):
        self.driver.uc_open_with_reconnect(gs_url, reconnect_time=3)
        self._handle_captcha()
        
        html = self.driver.get_page_source()
        soup = BeautifulSoup(html, "html.parser")
        
        # 1. Extract Headline
        headline_el = soup.find('div', class_='gsc_prf_il')
        headline = headline_el.text.strip() if headline_el else None
        
        # 2. Extract Homepage URL
        homepage_el = soup.find('a', string=re.compile('Homepage', re.I))
        homepage_url = homepage_el['href'] if homepage_el and 'href' in homepage_el.attrs else None
        
        # 3. Extract Citations
        cit_el = soup.select_one('#gsc_rsb_st td.gsc_rsb_std')
        citations = 0
        if cit_el:
            cit_text = cit_el.text.strip().replace(',', '')
            citations = int(cit_text) if cit_text.isdigit() else 0

        # Update the authors table
        cursor.execute("""
            UPDATE authors 
            SET headline = ?, homepage_url = ?, citations = ?, is_crawled = 1 
            WHERE author_id = ?
        """, (headline, homepage_url, citations, author_id))
        
        print(f"   ✓ Citations: {citations} | Homepage Found: {'Yes' if homepage_url else 'No'}")

        # 4. Extract Specializations/Tags
        tags = soup.select('.gsc_prf_inta')
        for tag in tags:
            keyword = tag.text.strip()
            if keyword:
                cursor.execute("""
                    INSERT OR IGNORE INTO specializations (author_id, keyword) 
                    VALUES (?, ?)
                """, (author_id, keyword))


if __name__ == "__main__":
    crawler = AuthorProfileCrawler()
    
    # Autonomous batching: Run in chunks of 100, then rest
    batch = 1
    while True:
        print(f"\n🚀 --- INITIATING CRAWLER BATCH {batch} ---")
        
        has_more_authors = crawler.run_crawler(limit=100)
        
        if not has_more_authors:
            print("\n🛑 No more authors to crawl. Shutting down automation.")
            break
            
        # Sleep for 30 minutes (1800 seconds) to cool down the IP address
        print("\n🛌 Batch complete. Sleeping for 30 minutes to protect IP...")
        time.sleep(1800) 
        batch += 1