import sqlite3
import time
import random
import urllib.parse
import os
import pickle
import json
from datetime import datetime, timedelta
from seleniumbase import Driver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

class GoogleScholarScraper:
    def __init__(self, db_name="neurips_research.db", session_dir="sessions"):
        self.db_name = db_name
        self.base_url = "https://scholar.google.com"
        self.driver = None
        self.session_dir = session_dir
        self.block_start_time = None
        self.consecutive_blocks = 0
        self.last_request_time = None
        
        # Create session directory
        os.makedirs(session_dir, exist_ok=True)
        
        # User agents for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
        ]
        
        # Referers for realistic browsing
        self.referers = [
            "https://www.google.com/",
            "https://www.bing.com/",
            "https://duckduckgo.com/",
            "https://scholar.google.com/",
            None,  # No referer sometimes
        ]

    def _get_random_user_agent(self):
        """Get a random user agent"""
        return random.choice(self.user_agents)

    def _get_random_referer(self):
        """Get a random referer"""
        return random.choice(self.referers)

    def _get_session_file(self, session_id):
        """Get path to session file"""
        return os.path.join(self.session_dir, f"session_{session_id}.pkl")

    def _save_cookies(self, session_id):
        """Save cookies from current driver"""
        if self.driver:
            try:
                cookies = self.driver.get_cookies()
                session_file = self._get_session_file(session_id)
                with open(session_file, 'wb') as f:
                    pickle.dump(cookies, f)
            except:
                pass

    def _load_cookies(self, session_id):
        """Load cookies into driver"""
        session_file = self._get_session_file(session_id)
        if self.driver and os.path.exists(session_file):
            try:
                with open(session_file, 'rb') as f:
                    cookies = pickle.load(f)
                for cookie in cookies:
                    try:
                        self.driver.add_cookie(cookie)
                    except:
                        pass
            except:
                pass

    def _start_browser(self, session_id="default"):
        """Start browser with anti-detection measures"""
        self._kill_browser()
        print("   🌐 Starting browser...")
        
        user_agent = self._get_random_user_agent()
        session_dir = os.path.join(self.session_dir, session_id)
        
        try:
            # Create directories if they don't exist
            os.makedirs(session_dir, exist_ok=True)
            
            self.driver = Driver(
                uc=True,  # Undetected Chrome
                headless=False,  # Headless is easier to detect
            )
            
            # Set user agent via CDP (Chrome DevTools Protocol)
            self.driver.execute_cdp_cmd("Network.setUserAgentOverride", {
                "userAgent": user_agent,
                "platform": "MacIntel" if "Mac" in user_agent else "Linux",
                "platformVersion": "10.15.7" if "Mac" in user_agent else "5.15.0",
            })
            
            # Navigate to base URL
            self.driver.uc_open_with_reconnect(self.base_url, reconnect_time=4)
            time.sleep(random.uniform(2, 4))
            
            print(f"   ✅ Browser started")
            
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
        
        # Soft block indicators (rate limiting)
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
            print("   💤 Sleeping for 2 hours before retry...")
            self.block_start_time = datetime.now()
            time.sleep(7200)  # 2 hours
            self.consecutive_blocks = 0
            return True
        
        elif block_type == "soft_block":
            print("\n   ⚠️ SOFT BLOCK DETECTED (Rate Limiting)")
            wait_time = min(300 * self.consecutive_blocks, 1800)  # Up to 30 minutes
            print(f"   😴 Backing off for {wait_time}s ({wait_time//60}m)...")
            time.sleep(wait_time)
            return True
        
        elif block_type == "captcha":
            print("\n   🚨 CAPTCHA DETECTED!")
            print("   ⏱️ CAPTCHA requires manual solving.")
            print("   ⏸️ Pausing for 30 minutes to let the block expire...")
            time.sleep(1800)  # 30 minutes
            return True
        
        return False

    def _calculate_smart_delay(self):
        """Calculate intelligent delay with randomization"""
        base_delay = 15  # Minimum 8 seconds
        
        # Increase delays based on consecutive blocks
        if self.consecutive_blocks > 0:
            base_delay = min(base_delay + (self.consecutive_blocks * 10), 60)
        
        # Add random jitter (±20%)
        jitter = random.uniform(0.8, 1.2)
        delay = base_delay * jitter
        
        # Occasional longer pauses (5% chance of 3-8 minute pause)
        if random.random() < 0.05:
            delay = random.uniform(180, 200)
        
        return delay

    def _is_valid_result(self, first_result):
        """Check if result looks like actual search result"""
        if not first_result:
            return False
        
        # Should have a title element
        if not first_result.select_one('.gs_rt a'):
            return False
        
        return True

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
                
                # Smart delay between requests
                delay = self._calculate_smart_delay()
                print(f"   ⏳ Waiting {delay:.1f}s before next request...\n")
                time.sleep(delay)
                
            except Exception as e:
                error_msg = str(e).lower()
                print(f"   ❌ Error: {e}\n")
                
                if "hard block" in error_msg or "unresolvable" in error_msg:
                    print("   🛑 Stopping pipeline due to hard block.")
                    break
                
                # Mark as failed but continue
                cursor.execute(
                    "UPDATE papers SET is_processed = 1, notes = ? WHERE paper_id = ?",
                    (f"Error: {str(e)[:100]}", paper_id)
                )
                conn.commit()
        
        conn.close()
        self._kill_browser()
        
        print(f"\n{'='*75}")
        print(f"Pipeline complete: {successful_count} papers processed successfully")
        print(f"{'='*75}\n")
        
        return True

    def _process_paper(self, cursor, paper_id, title):
        """Process a single paper"""
        
        # Build search URL
        encoded_title = urllib.parse.quote(title)
        search_url = f"{self.base_url}/scholar?hl=en&q={encoded_title}"
        
        # Navigate to search
        self.driver.uc_open_with_reconnect(search_url, reconnect_time=3)
        time.sleep(random.uniform(2, 4))
        
        # Get page content
        html = self.driver.get_page_source()
        soup = BeautifulSoup(html, "html.parser")
        
        # Check for blocks
        block_type = self._detect_block(html)
        if block_type:
            if self._handle_block(block_type):
                # Retry after handling block
                print("   🔄 Retrying after block handling...")
                self.driver.uc_open_with_reconnect(search_url, reconnect_time=3)
                time.sleep(random.uniform(2, 4))
                html = self.driver.get_page_source()
                soup = BeautifulSoup(html, "html.parser")
                
                # Check again
                block_type = self._detect_block(html)
                if block_type:
                    raise Exception(f"Block persists after wait: {block_type}")
        
        # Find first result
        first_result = soup.select_one('.gs_ri')
        
        if not self._is_valid_result(first_result):
            print("   ⚠️ No valid results found")
            cursor.execute(
                "UPDATE papers SET is_processed = 1, notes = ? WHERE paper_id = ?",
                ("No results found on Google Scholar", paper_id)
            )
            return False
        
        # Extract abstract
        abstract_el = first_result.select_one('.gs_rs')
        abstract = abstract_el.text.strip().replace('\n', ' ') if abstract_el else "No abstract"
        
        # Extract paper title from result
        title_el = first_result.select_one('.gs_rt a')
        result_title = title_el.text.strip() if title_el else title
        
        # Extract URL
        result_url = None
        if title_el and title_el.get('href'):
            result_url = title_el['href']
        
        # Update paper record
        cursor.execute("""
            UPDATE papers SET abstract = ?, is_processed = 1 WHERE paper_id = ?
        """, (abstract, paper_id))

        # Extract authors with Google Scholar profiles
        author_links = first_result.select('.gs_a a[href*="/citations?user="]')
        
        if author_links:
            print(f"   👥 Found {len(author_links)} Google Scholar authors")
            
            for author_link in author_links:
                author_name = author_link.text.strip()
                gs_url = self.base_url + author_link['href']
                
                # Insert author
                cursor.execute("""
                    INSERT OR IGNORE INTO authors (name, gs_url) 
                    VALUES (?, ?)
                """, (author_name, gs_url))
                
                # Get author_id
                cursor.execute("SELECT author_id FROM authors WHERE gs_url = ?", (gs_url,))
                author_row = cursor.fetchone()
                
                if author_row:
                    author_id = author_row[0]
                    cursor.execute("""
                        INSERT OR IGNORE INTO paper_authors (paper_id, author_id) 
                        VALUES (?, ?)
                    """, (paper_id, author_id))
        else:
            print("   ⚠️ No linked Google Scholar authors found")
        
        print("   ✅ Paper processed successfully")
        return True


def main():
    """Main entry point"""
    
    print("""
    ╔═══════════════════════════════════════════════════════════════╗
    ║        Google Scholar Scraper - Self-Hosted Version          ║
    ║                    No External Services                       ║
    ╚═══════════════════════════════════════════════════════════════╝
    """)
    
    scraper = GoogleScholarScraper(db_name="neurips_research.db")
    
    batch_num = 1
    total_processed = 0
    
    while True:
        print(f"\n🚀 BATCH {batch_num}")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Process batch (100 papers at a time to be conservative)
        has_more = scraper.run_pipeline(limit=100)
        
        if not has_more:
            print("\n✅ All papers processed!")
            break
        
        batch_num += 1
        
        # Long cooldown between batches
        cooldown_seconds = 600  # 10 min
        print(f"\n🛌 Batch cooldown: {cooldown_seconds}s ({cooldown_seconds//60} minutes)")
        
        for remaining in range(cooldown_seconds, 0, -60):
            print(f"   ⏳ {remaining}s remaining...", end='\r')
            time.sleep(60)
        
        print("\n" + "="*75)


if __name__ == "__main__":
    main()