import sqlite3
import time
import random
import urllib.parse
import os
from datetime import datetime
from seleniumbase import Driver
from bs4 import BeautifulSoup

class GoogleScholarScraper:
    def __init__(self, db_name="neurips_research.db", session_dir="sessions"):
        self.db_name = db_name
        self.base_url = "https://scholar.google.com"
        self.driver = None
        self.session_dir = session_dir
        self.consecutive_blocks = 0
        self.papers_in_session = 0
        self.max_papers_per_session = 15  # Restart browser after 15 papers
        
        os.makedirs(session_dir, exist_ok=True)
        
        # More realistic user agents with proper formats
        self.user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
        ]

    def _get_random_user_agent(self):
        """Get a random, realistic user agent"""
        return random.choice(self.user_agents)

    def _start_browser(self):
        """Start browser with persistent profile and native anti-detection"""
        self._kill_browser()
        print("   🌐 Starting browser with persistent profile...")
        
        # We create a persistent folder to store Google's trust cookies
        profile_path = os.path.join(os.getcwd(), self.session_dir, "chrome_profile")
        
        try:
            self.driver = Driver(
                uc=True,  # Undetected Chrome
                headless=False,
                user_data_dir=profile_path,  # <-- THIS IS THE MAGIC. It saves your cookies!
            )
            
            # NOTE: We deleted the execute_cdp_cmd User-Agent override entirely. 
            # We are letting Undetected Chromedriver use its mathematically perfect default.

            # Navigate to base URL
            self.driver.uc_open_with_reconnect(self.base_url, reconnect_time=4)
            time.sleep(random.uniform(3, 6))
            
            # Simulate some random scrolling and waiting on homepage
            self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
            time.sleep(random.uniform(1, 3))
            self.driver.execute_script("window.scrollBy(0, -window.innerHeight);")
            
            print("   ✅ Browser started and cookies loaded")
            self.papers_in_session = 0
            
        except Exception as e:
            print(f"   💥 Failed to start browser: {e}")
            raise

    def _kill_browser(self):
        """Kill browser safely"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def _human_like_delay(self):
        """
        Simulate human-like browsing delays
        Humans don't request pages every 8 seconds - they vary much more
        """
        # 70% of the time: normal delay (15-25 seconds)
        if random.random() < 0.7:
            delay = random.uniform(15, 25)
        # 20% of the time: shorter delay (10-15 seconds)
        elif random.random() < 0.86:  # 20/100
            delay = random.uniform(10, 15)
        # 10% of the time: longer delay (30-60 seconds) - reading the page
        else:
            delay = random.uniform(30, 60)
        
        # Add random jitter
        jitter = random.uniform(0.9, 1.1)
        return delay * jitter

    def _detect_block(self, html_content):
        """Detect if Google is blocking us"""
        page_text = html_content.lower()
        
        # Hard block indicators
        if any(phrase in page_text for phrase in [
            "unusual traffic from your computer network",
            "we have detected unusual traffic",
            "our systems have detected unusual traffic"
        ]):
            return "hard_block"
        
        # CAPTCHA indicators
        if any(phrase in page_text for phrase in [
            "recaptcha",
            'name="recaptcha',
            'class="g-recaptcha"',
            "not a robot",
            "verify you're human"
        ]):
            return "captcha"
        
        # Soft block indicators
        if any(phrase in page_text for phrase in [
            "unusual traffic",
            "too many requests",
            "try again in a few moments",
            "please slow down"
        ]):
            return "soft_block"
        
        return None

    def _handle_block(self, block_type):
        """Handle different types of blocks"""
        self.consecutive_blocks += 1
        
        if block_type == "hard_block":
            print("\n   🚨 HARD BLOCK DETECTED!")
            print("   Google has issued a hard IP ban.")
            print("   💤 Sleeping for 1 hour before retry...")
            time.sleep(3600)
            self.consecutive_blocks = 0
            return True
        
        elif block_type == "soft_block":
            print("\n   ⚠️ SOFT BLOCK DETECTED (Rate Limiting)")
            # Exponential backoff
            wait_time = min(60 * (2 ** self.consecutive_blocks), 600)  # Up to 10 min
            print(f"   😴 Backing off for {wait_time}s ({wait_time//60}m)...")
            time.sleep(wait_time)
            return True
        
        elif block_type == "captcha":
            print("\n   🚨 CAPTCHA DETECTED!")
            print("   ⏱️ IP is being throttled. Waiting 45 minutes...")
            time.sleep(2700)  # 45 minutes
            return True
        
        return False

    def _is_valid_result(self, first_result):
        """Check if result looks like actual search result"""
        if not first_result:
            return False
        if not first_result.select_one('.gs_rt a'):
            return False
        return True

    def _simulate_human_browsing(self):
        """Simulate human-like browsing behavior"""
        # Random chance to scroll on the page
        if random.random() < 0.3:
            scroll_amount = random.randint(100, 500)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(1, 3))
        
        # Random chance to hover over elements (pause)
        if random.random() < 0.2:
            time.sleep(random.uniform(2, 5))

    def run_pipeline(self, limit=30):
        """Main pipeline to process papers"""
        print(f"\n{'='*75}")
        print(f"Starting Google Scholar Pipeline (Processing max {limit} papers)")
        print(f"{'='*75}")
        print(f"Consecutive blocks: {self.consecutive_blocks}")
        print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Get unprocessed papers
        cursor.execute(
            "SELECT paper_id, title FROM papers WHERE is_processed = 0 ORDER BY paper_id ASC LIMIT ?",
            (limit,)
        )
        papers = cursor.fetchall()
        
        if not papers:
            print("\n✅ All papers have been processed!")
            conn.close()
            return False
        
        print(f"Found {len(papers)} unprocessed papers\n")
        
        self._start_browser()
        successful_count = 0
        
        for idx, (paper_id, title) in enumerate(papers, 1):
            print(f"[{idx}/{len(papers)}] 🔍 Processing: {title[:65]}...")
            
            try:
                # Process the paper
                result = self._process_paper(cursor, paper_id, title)
                
                if result:
                    successful_count += 1
                    self.consecutive_blocks = 0  # Reset on success
                
                conn.commit()
                
                # Human-like delay
                delay = self._human_like_delay()
                print(f"   ⏳ Waiting {delay:.1f}s before next request...\n")
                time.sleep(delay)
                
                # Restart browser every 15 papers to get fresh session
                self.papers_in_session += 1
                if self.papers_in_session >= self.max_papers_per_session:
                    print("   🔄 Restarting browser for fresh session...\n")
                    self._kill_browser()
                    time.sleep(random.uniform(5, 10))
                    self._start_browser()
                
            except Exception as e:
                error_msg = str(e).lower()
                print(f"   ❌ Error: {e}\n")
                
                if "hard block" in error_msg or "unresolvable" in error_msg:
                    print("   🛑 Stopping pipeline due to hard block.")
                    break
                
                # Don't mark as processed - let it retry in next batch
                print(f"   ⏳ Will retry this paper in next batch...")
        
        conn.close()
        self._kill_browser()
        
        print(f"\n{'='*75}")
        print(f"Pipeline complete: {successful_count} papers processed successfully")
        print(f"{'='*75}\n")
        
        return True

    def _process_paper(self, cursor, paper_id, title):
        """Process a single paper"""
        import re
        from difflib import SequenceMatcher
        from datetime import datetime
        
        # Build search URL
        import urllib.parse
        encoded_title = urllib.parse.quote(title)
        search_url = f"{self.base_url}/scholar?hl=en&q={encoded_title}"
        
        # Navigate to search with randomized timing
        import time, random
        time.sleep(random.uniform(1, 2))
        self.driver.uc_open_with_reconnect(search_url, reconnect_time=3)
        time.sleep(random.uniform(3, 5))
        
        # Simulate human browsing
        self._simulate_human_browsing()
        
        # Get page content
        html = self.driver.get_page_source()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        
        # Check for blocks
        block_type = self._detect_block(html)
        if block_type:
            if self._handle_block(block_type):
                # Restart browser after block to get clean session
                self._kill_browser()
                time.sleep(random.uniform(5, 15))
                self._start_browser()
                
                # Retry the paper
                print("   🔄 Retrying after block handling...")
                time.sleep(random.uniform(2, 4))
                self.driver.uc_open_with_reconnect(search_url, reconnect_time=3)
                time.sleep(random.uniform(3, 5))
                
                html = self.driver.get_page_source()
                soup = BeautifulSoup(html, "html.parser")
                
                # Check again
                block_type = self._detect_block(html)
                if block_type:
                    raise Exception(f"Block persists after wait: {block_type}")
        
        # Check if Google explicitly says there are no results
        if "did not match any articles" in soup.text or "did not match any articles" in html.lower():
            print("   ⚠️ Genuinely no results found on Google Scholar for this paper.")
            cursor.execute("""
                UPDATE papers SET is_processed = 1, notes = 'Zero results on Scholar' 
                WHERE paper_id = ?
            """, (paper_id,))
            return True

        # --- THE SMART RESULT SELECTOR ---
        results = soup.select('.gs_ri')
        best_result = None
        result_title = ""
        result_url = None
        
        for result in results[:3]: 
            title_el = result.select_one('.gs_rt a')
            if not title_el:
                title_el = result.select_one('.gs_rt') 
                
            raw_title = title_el.text.strip() if title_el else ""
            clean_title = re.sub(r'^\[.*?\]\s*', '', raw_title)
            
            similarity = SequenceMatcher(None, title.lower(), clean_title.lower()).ratio()
            
            url = title_el.get('href', '').lower() if title_el and title_el.name == 'a' else ""
            venue_info = result.select_one('.gs_a').text.lower() if result.select_one('.gs_a') else ""
            
            is_neurips = "neurips" in url or "nips" in url or "neural information" in venue_info
            
            if similarity >= 0.85:
                if is_neurips:
                    best_result = result
                    result_title = clean_title
                    result_url = title_el.get('href') if title_el and title_el.name == 'a' else None
                    break 
                elif best_result is None:
                    best_result = result
                    result_title = clean_title
                    result_url = title_el.get('href') if title_el and title_el.name == 'a' else None
                    
        if not best_result:
            print(f"   ⚠️ Mismatch Alert! Google returned unrelated papers or no valid cards.")
            cursor.execute("""
                UPDATE papers SET is_processed = 1, notes = 'Skipped: Result mismatch or no NeurIPS tag' 
                WHERE paper_id = ?
            """, (paper_id,))
            return True 
            
        first_result = best_result
        
        # Extract abstract
        abstract_el = first_result.select_one('.gs_rs')
        abstract = abstract_el.text.strip().replace('\n', ' ') if abstract_el else "No abstract"
        
        # Update paper record
        cursor.execute("""
            UPDATE papers 
            SET abstract = ?, scholar_title = ?, scholar_url = ?, is_processed = 1, processed_date = ?
            WHERE paper_id = ?
        """, (abstract, result_title, result_url, datetime.now().isoformat(), paper_id))
        
        # --- THE HYBRID AUTHOR EXTRACTOR (DOM-Optimized) ---
        author_div = first_result.select_one('.gs_a')
        
        if author_div:
            # 1. Grab ALL Scholar profiles from the block
            linked_authors = {}
            for link in first_result.select('.gs_a a[href*="/citations?user="]'):
                linked_name = link.text.strip()
                linked_url = self.base_url + link['href']
                linked_authors[linked_name] = linked_url
                
            # 2. Extract clean author list from hidden DOM or fallback to regex
            clean_author_div = first_result.select_one('.gs_fmaa')
            
            if clean_author_div:
                all_author_names = [name.strip() for name in clean_author_div.text.split(',') if name.strip()]
            else:
                full_text = author_div.text.replace('\xa0', ' ')
                parts = re.split(r'\s+[-–—]\s+', full_text)
                author_chunk = parts[0].replace('\u2026', '').replace('...', '').strip()
                all_author_names = [name.strip() for name in author_chunk.split(',') if name.strip()]
            
            # 3. Failsafe for weird HTML glitches
            for linked_name in linked_authors.keys():
                if linked_name not in all_author_names:
                    all_author_names.append(linked_name)
            
            print(f"   👥 Found {len(all_author_names)} total authors ({len(linked_authors)} have Scholar profiles)")
            
            # 4. Save to Database
            for author_name in all_author_names:
                gs_url = linked_authors.get(author_name, None)
                
                cursor.execute("""
                    INSERT OR IGNORE INTO authors (name, gs_url) 
                    VALUES (?, ?)
                """, (author_name, gs_url))
                
                if gs_url:
                    cursor.execute("SELECT author_id FROM authors WHERE gs_url = ?", (gs_url,))
                else:
                    cursor.execute("SELECT author_id FROM authors WHERE name = ? AND gs_url IS NULL", (author_name,))
                
                author_row = cursor.fetchone()
                
                if not author_row:
                    cursor.execute("SELECT author_id FROM authors WHERE name = ?", (author_name,))
                    author_row = cursor.fetchone()
                        
                if author_row:
                    author_id = author_row[0]
                    cursor.execute("""
                        INSERT OR IGNORE INTO paper_authors (paper_id, author_id) 
                        VALUES (?, ?)
                    """, (paper_id, author_id))
        else:
            print("   ⚠️ No author information found in the result")
        
        print("   ✅ Paper processed successfully")
        return True


def main():
    """Main entry point"""
    
    print("""
    ╔═══════════════════════════════════════════════════════════════╗
    ║     Google Scholar Scraper - Anti-Detection Optimized        ║
    ║            Focused on Avoiding CAPTCHAs                      ║
    ╚═══════════════════════════════════════════════════════════════╝
    """)
    
    scraper = GoogleScholarScraper(db_name="neurips_research.db")
    
    batch_num = 1
    
    while True:
        print(f"\n🚀 BATCH {batch_num}")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Process smaller batches with better spacing
        has_more = scraper.run_pipeline(limit=12)
        
        if not has_more:
            print("\n✅ All papers processed!")
            break
        
        batch_num += 1
        
        # Long cooldown between batches - let IP reputation recover
        cooldown_seconds = 720  # 12 minutes
        print(f"\n🛌 Batch cooldown: {cooldown_seconds}s ({cooldown_seconds//60} minutes)")
        print("   Letting IP reputation recover before next batch...\n")
        
        for remaining in range(cooldown_seconds, 0, -60):
            mins = remaining // 60
            secs = remaining % 60
            print(f"   ⏳ {mins}m {secs}s remaining...", end='\r')
            time.sleep(60)
        
        print("\n" + "="*75)


if __name__ == "__main__":
    main()