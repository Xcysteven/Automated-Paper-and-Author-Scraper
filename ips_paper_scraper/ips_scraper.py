import sqlite3
import time
from seleniumbase import Driver
from bs4 import BeautifulSoup

class NeurIPSGoogleScholarScraper:
    def __init__(self, db_name="neurips_research.db"):
        self.db_name = db_name
        self.init_db()

    def init_db(self):
        """Creates the database and tables if they don't exist."""
        print("🗄️ Initializing SQLite Database...")
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        # Papers Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS papers (
                paper_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT UNIQUE,
                abstract TEXT,
                conference TEXT,
                year INTEGER,
                is_processed INTEGER DEFAULT 0
            )
        ''')

        # Authors Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS authors (
                author_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                gs_url TEXT UNIQUE,
                headline TEXT,
                homepage_url TEXT,
                citations INTEGER,
                is_crawled INTEGER DEFAULT 0
            )
        ''')

        # Link Table (Paper to Authors)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS paper_authors (
                paper_id INTEGER,
                author_id INTEGER,
                UNIQUE(paper_id, author_id)
            )
        ''')

        # Contacts Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                author_id INTEGER PRIMARY KEY,
                email TEXT,
                linkedin_url TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def scrape_neurips_titles(self):
        """Scrapes ALL paper titles from NeurIPS 2024 handling Pagination."""
        print("\n🌐 Booting up browser to scrape NeurIPS 2024...")
        driver = Driver(uc=True, headless=False) 
        
        try:
            # Start at the base URL (Page 1)
            driver.get("https://nips.cc/virtual/2024/papers.html")
            print("⏳ Waiting for initial load...")
            time.sleep(5) 
            
            conn = sqlite3.connect(self.db_name)
            cursor = conn.cursor()
            
            page_num = 1
            total_count = 0
            
            while True:
                print(f"\n📄 Scraping Page {page_num}...")
                
                # 1. Extract the HTML for the current page
                html = driver.get_page_source()
                soup = BeautifulSoup(html, "html.parser")
                
                papers = soup.select("li a[href^='/virtual/2024/poster/'], ul.Cards li a") 
                
                if not papers:
                    print("   ⚠️ No papers found on this page. We might be done.")
                    break
                    
                page_count = 0
                for paper in papers:
                    title = paper.text.strip()
                    if title:
                        try:
                            # Save to database
                            cursor.execute('INSERT INTO papers (title, conference, year) VALUES (?, ?, ?)', 
                                         (title, 'NeurIPS', 2024))
                            page_count += 1
                        except sqlite3.IntegrityError:
                            pass # Skip if we already saved this title
                
                total_count += page_count
                conn.commit()
                print(f"   ✅ Saved {page_count} new titles. (Total so far: {total_count})")
                
                # 2. Try to find and click the "Next" page button
                try:
                    # We check if a "Next" link or button is visible on the screen
                    if driver.is_element_visible('a:contains("Next")') or driver.is_element_visible('button:contains("Next")') or driver.is_element_visible('.pagination .next'):
                        print("   ➡️ Clicking 'Next' page...")
                        # Click it using SeleniumBase's text selector
                        driver.click('a:contains("Next"), button:contains("Next"), .pagination .next')
                        
                        # Wait for the next 400 papers to fully load
                        time.sleep(4) 
                        page_num += 1
                    else:
                        print("🛑 No 'Next' button found. We have reached the final page!")
                        break
                except Exception as e:
                    print("🛑 Reached the end of pagination (or couldn't click Next).")
                    break
            
            conn.close()
            print(f"\n🎉 Finished! Successfully saved a grand total of {total_count} papers to the database.")
            
        except Exception as e:
            print(f"❌ Error during scraping: {e}")
        finally:
            driver.quit()

if __name__ == "__main__":
    scraper = NeurIPSGoogleScholarScraper()
    scraper.scrape_neurips_titles()