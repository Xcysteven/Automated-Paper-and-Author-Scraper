import time
import random
import re
import hashlib
import pandas as pd
from seleniumbase import Driver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class SemanticScholarScraper:
    def __init__(self, query: str = "computer architecture", limit: int = 50):
        self.query = query
        self.limit = limit
        self.base_url = "https://www.semanticscholar.org"
        self.papers = []
        self.authors = {}
        self.paper_authors = []
        self.driver = None  # We now track the driver at the class level

    def _start_browser(self):
        """Spins up a fresh browser instance and clears initial checks."""
        self._kill_browser()  # Ensure any old instances are dead
        print("   Starting a fresh browser instance...")
        self.driver = Driver(uc=True, headless=False)
        self.driver.uc_open_with_reconnect(self.base_url, reconnect_time=5)
        time.sleep(3)

    def _kill_browser(self):
        """Safely shuts down the current browser."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def run(self):
        print(f"--- Starting Hard-Reset Scraper for: '{self.query}' ---")
        self._start_browser()
        
        try:
            self._scrape_interleaved()
        except Exception as e:
            print(f"\nScraping interrupted by fatal error: {e}")
        finally:
            self._export_data()
            self._kill_browser()
            print("\n--- Scraping Complete ---")

    def _handle_verification(self):
        """Attempts to bypass human verification popups."""
        try:
            if "verify" in self.driver.current_url.lower() or self.driver.is_element_present("iframe[src*='turnstile']"):
                print(" CAPTCHA detected! Attempting bypass...", end=" ")
                self.driver.uc_gui_click_captcha()
                time.sleep(4)
        except:
            pass

    def _scrape_interleaved(self):
        page_count = 1
        
        while len(self.papers) < self.limit:
            search_url = f"{self.base_url}/search?q={self.query.replace(' ', '%20')}&sort=relevance&page={page_count}"
            print(f"\n⚓ Loading Search Page {page_count}...")
            
            try:
                self.driver.uc_open_with_reconnect(search_url, reconnect_time=4)
                self._handle_verification()

                # Wait for search results
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".cl-paper-row, [data-test-id='search-result']"))
                )
            except Exception as e:
                print(f"   Page {page_count} blocked or failed to load. Initiating Hard Reset...")
                self._start_browser()
                continue  # Retry the exact same search page
            
            html = self.driver.get_page_source()
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select(".cl-paper-row, [data-test-id='search-result']")
            
            if not cards:
                print("   No paper cards found on this page. Moving to next.")
                page_count += 1
                continue

            author_queue = []

            for card in cards:
                if len(self.papers) >= self.limit: break
                
                title_el = card.select_one('h3, h2, .cl-paper-title')
                title = title_el.text.strip() if title_el else "Unknown"
                link_el = card.select_one('a[href*="/paper/"]')
                paper_url = self.base_url + link_el['href'] if link_el else "N/A"
                paper_id = paper_url.split('/')[-1] if paper_url != "N/A" else hashlib.md5(title.encode()).hexdigest()[:16]

                self.papers.append({"paper_id": paper_id, "title": title, "url": paper_url})
                
                for order, auth_el in enumerate(card.select('a[href*="/author/"]'), 1):
                    auth_href = auth_el.get('href', '')
                    if not auth_href: continue
                    auth_id = auth_href.split('/')[-1]
                    
                    self.paper_authors.append({"paper_id": paper_id, "author_id": auth_id, "author_order": order})
                    
                    if auth_id not in self.authors:
                        self.authors[auth_id] = {
                            "author_id": auth_id, 
                            "author_name": auth_el.text.strip(), 
                            "author_profile_url": self.base_url + auth_href, 
                            "citation_count": None
                        }
                        author_queue.append(auth_id)

            if author_queue:
                print(f"   👥 Processing {len(author_queue)} authors...")
                idx = 0
                retries = 0
                
                # Using a while loop so we can retry the same index if it fails
                while idx < len(author_queue):
                    aid = author_queue[idx]
                    try:
                        self._scrape_single_author(aid)
                        time.sleep(random.uniform(2.0, 3.5))
                        idx += 1       # Success
                        retries = 0    # Reset retries
                        
                    except Exception as e:
                        retries += 1
                        print(f"\n      💥 Browser crashed or blocked! (Attempt {retries}/3)")
                        
                        if retries > 2:
                            print(f"      ⏭️ Skipping author {aid} after 3 failed browser resets.")
                            idx += 1
                            retries = 0
                        else:
                            self._start_browser() # The Hard Reset

            page_count += 1

    def _scrape_single_author(self, author_id):
        author_data = self.authors[author_id]
        print(f"      👤 {author_data['author_name'][:30]}...", end=" ", flush=True)
        
        self.driver.uc_open_with_reconnect(author_data['author_profile_url'], reconnect_time=3)
        
        try:
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".author-detail-card__stats-row__value, .author-detail-card"))
            )
        except:
            self._handle_verification()
            # If it still fails after trying to handle it, we force an error to trigger the Hard Reset
            WebDriverWait(self.driver, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".author-detail-card"))
            )

        html = self.driver.get_page_source()
        soup = BeautifulSoup(html, "html.parser")
        citation_count = self._extract_author_citations_only(soup)
        
        self.authors[author_id]['citation_count'] = citation_count
        print(f"✓ {citation_count}")

    def _extract_author_citations_only(self, soup):
        citation_count = None
        stats_rows = soup.select('.author-detail-card__stats-row')
        for row in stats_rows:
            label = row.select_one('.author-detail-card__stats-row__label')
            value = row.select_one('.author-detail-card__stats-row__value')
            if label and value:
                label_text = label.get_text().strip().lower()
                if 'citation' in label_text and 'influential' not in label_text:
                    value_text = value.get_text().replace(',', '').strip()
                    if 'k' in value_text.lower():
                        try:
                            citation_count = int(float(value_text.lower().replace('k', '')) * 1000)
                            break
                        except: pass
                    else:
                        match = re.search(r'(\d+)', value_text)
                        if match:
                            citation_count = int(match.group(1))
                            break
        
        if citation_count is None:
            page_text = soup.get_text()
            if "Co-Authors" in page_text or "Co-Author" in page_text:
                main_section = page_text.split("Co-Author")[0]
            else:
                main_section = page_text[:2000]
            match = re.search(r'([\d,]+)\s+Citations', main_section)
            if match:
                citation_count = int(match.group(1).replace(',', ''))
        
        if citation_count is None:
            main_card = soup.select_one('.author-detail-card')
            if main_card:
                card_text = main_card.get_text()
                match = re.search(r'([\d,]+)\s+Citations', card_text)
                if match:
                    citation_count = int(match.group(1).replace(',', ''))
        
        return citation_count if citation_count else 0

    def _export_data(self):
        try:
            pd.DataFrame(self.papers).drop_duplicates(subset='paper_id').to_csv("papers.csv", index=False)
            pd.DataFrame(list(self.authors.values())).to_csv("authors.csv", index=False)
            pd.DataFrame(self.paper_authors).drop_duplicates().to_csv("paper_authors.csv", index=False)
            print("   Data successfully exported to CSVs.")
        except Exception as e:
            print(f"   Failed to export data: {e}")

if __name__ == "__main__":
    scraper = SemanticScholarScraper(query="computer architecture", limit=50)
    scraper.run()