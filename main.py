import asyncio
import random
import re
import hashlib
from typing import List, Dict, Set
import pandas as pd
from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth


class SemanticScholarScraper:
    
    def __init__(self, query: str = "computer architecture", limit: int = 50):
        self.query = query
        self.limit = limit
        self.base_url = "https://www.semanticscholar.org"
        self.search_url = f"{self.base_url}/search?q={query.replace(' ', '%20')}&sort=relevance"
        
        self.papers: List = []
        self.authors: Dict = {}
        self.paper_authors: List = []
        self.author_urls_to_scrape: Set[str] = set()
        self.stealth = Stealth()

    async def run(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox"
                ]
            )
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                }
            )
            
            page = await context.new_page()
            await self.stealth.apply_stealth_async(page)
            
            print(f"--- Starting Scraper for query: '{self.query}' (with stealth mode ✓) ---")
            
            await self._scrape_search_results(page)
            
            print(f"--- Extracting details for {len(self.author_urls_to_scrape)} unique authors ---")
            await self._scrape_author_profiles(context)
            
            await browser.close()
            self._export_data()
            print("--- Scraping Complete. Data saved to CSV. ---")

    async def _scrape_search_results(self, page: Page):
        print(f"Navigating to: {self.search_url}")
        await page.goto(self.search_url, wait_until="networkidle")
        await asyncio.sleep(2)
        
        page_count = 0
        while len(self.papers) < self.limit:
            page_count += 1
            print(f"Processing search page {page_count}. Papers collected so far: {len(self.papers)}")
            
            try:
                await page.wait_for_selector('[data-test-id="search-result"]', timeout=10000)
            except PlaywrightTimeoutError:
                try:
                    await page.wait_for_selector('.cl-paper-row', timeout=5000)
                except PlaywrightTimeoutError:
                    print("Timeout waiting for paper results. Taking screenshot for debug.")
                    await page.screenshot(path=f"debug_timeout_page_{page_count}.png")
                    with open(f"debug_page_{page_count}.html", "w", encoding="utf-8") as f:
                        f.write(await page.content())
                    print(f"Saved debug files: debug_timeout_page_{page_count}.png and debug_page_{page_count}.html")
                    break

            cards = []
            selectors_to_try = [
                '[data-test-id="search-result"]',
                '[data-test-id="paper-card"]',
                '.cl-paper-row',
                'div[class*="search-result"]'
            ]
            
            for selector in selectors_to_try:
                cards = await page.query_selector_all(selector)
                if cards:
                    print(f"Found {len(cards)} papers using selector: {selector}")
                    break
            
            if not cards:
                print("No paper cards found with any selector. Stopping.")
                break
            
            for card in cards:
                if len(self.papers) >= self.limit:
                    break
                
                title = "Unknown Title"
                title_selectors = ['h3 a', 'h2 a', 'a[data-heap-id="paper_title"]', '.cl-paper-title']
                
                for title_sel in title_selectors:
                    title_el = await card.query_selector(title_sel)
                    if title_el:
                        title = await title_el.inner_text()
                        title = title.strip()
                        break
                
                paper_url = "N/A"
                paper_s2_id = None
                
                link_selectors = [
                    'a[href*="/paper/"]',
                    'h3 a[href]',
                    'h2 a[href]',
                    'a[data-heap-id="paper_title"]'
                ]
                
                for link_sel in link_selectors:
                    link_el = await card.query_selector(link_sel)
                    if link_el:
                        relative_url = await link_el.get_attribute('href')
                        if relative_url and '/paper/' in relative_url:
                            if not relative_url.startswith('http'):
                                paper_url = self.base_url + relative_url
                            else:
                                paper_url = relative_url
                            
                            parts = relative_url.rstrip('/').split('/')
                            paper_s2_id = parts[-1]
                            break
                
                if not paper_s2_id:
                    paper_s2_id = hashlib.md5(title.encode()).hexdigest()[:16]
                
                year = None
                citation_count = None
                
                year_el = await card.query_selector('.cl-paper-pubdates')
                if year_el:
                    year_text = await year_el.inner_text()
                    year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                    if year_match:
                        year = int(year_match.group())
                
                citation_el = await card.query_selector('.cl-paper-stats__citation-pdp-link')
                if not citation_el:
                    citation_el = await card.query_selector('[data-heap-id="citation_count"]')
                
                if citation_el:
                    citation_text = await citation_el.inner_text()
                    citation_match = re.search(r'([\d,]+)', citation_text)
                    if citation_match:
                        try:
                            citation_count = int(citation_match.group(1).replace(',', ''))
                        except ValueError:
                            pass
                
                self.papers.append({
                    "paper_id": paper_s2_id,
                    "title": title,
                    "url": paper_url,
                    "year": year,
                    "citation_count": citation_count
                })
                
                author_selectors = [
                    '.cl-paper-authors a.author-list__link',
                    '.cl-paper-authors a',
                    'span[data-heap-id="heap_author_list"] a',
                    'a[href*="/author/"]'
                ]
                
                author_els = []
                for auth_sel in author_selectors:
                    author_els = await card.query_selector_all(auth_sel)
                    if author_els:
                        break
                
                for auth_el in author_els:
                    auth_name = await auth_el.inner_text()
                    auth_name = auth_name.strip()
                    auth_href = await auth_el.get_attribute('href')
                    
                    if auth_href and '/author/' in auth_href:
                        if not auth_href.startswith('http'):
                            full_auth_url = self.base_url + auth_href
                        else:
                            full_auth_url = auth_href
                        
                        parts = auth_href.rstrip('/').split('/')
                        author_id = parts[-1]
                        
                        self.paper_authors.append({
                            "paper_id": paper_s2_id,
                            "author_id": author_id
                        })
                        
                        if author_id not in self.authors:
                            self.authors[author_id] = {
                                "author_id": author_id,
                                "name": auth_name,
                                "profile_url": full_auth_url,
                                "citation_count": None
                            }
                            self.author_urls_to_scrape.add(full_auth_url)

            if len(self.papers) < self.limit:
                print("Looking for next page...")
                
                next_button = None
                next_selectors = [
                    '[data-test-id="pagination-next-button"]',
                    'button[aria-label="Next page"]',
                    '.cl-pager__button--next',
                    'a.cl-pager__button[rel="next"]'
                ]
                
                for next_sel in next_selectors:
                    next_button = await page.query_selector(next_sel)
                    if next_button:
                        break
                
                if next_button:
                    is_disabled = await next_button.get_attribute('disabled')
                    is_enabled = await next_button.is_enabled()
                    
                    if is_enabled and not is_disabled:
                        print("Navigating to next page...")
                        await next_button.click()
                        await page.wait_for_load_state('networkidle')
                        await asyncio.sleep(random.uniform(2.0, 4.0))
                    else:
                        print("Next button is disabled. No more pages available.")
                        break
                else:
                    print("No next button found. Reached end of results.")
                    break
            
            if page_count > 20:
                print("Reached maximum page limit (20 pages). Stopping.")
                break

    async def _scrape_author_profiles(self, context: BrowserContext):
        total_authors = len(self.author_urls_to_scrape)
        successful = 0
        failed = 0
        
        for i, auth_url in enumerate(self.author_urls_to_scrape, 1):
            author_id = auth_url.split('/')[-1]
            
            if i % 5 == 0 or i == total_authors:
                print(f"Scraping author {i}/{total_authors} (✓ {successful}, ✗ {failed})")

            page = await context.new_page()
            await self.stealth.apply_stealth_async(page)
            
            try:
                await page.goto(auth_url, wait_until="domcontentloaded", timeout=15000)
                
                citation_count = 0
                
                try:
                    await page.wait_for_selector('.author-detail-cards', timeout=5000)
                    
                    selectors_to_try = [
                        '.author-detail-cards__stat-value',
                        '[data-test-id="author-citation-count"]',
                        '.stats-row__stat-value',
                        'div.author-stats'
                    ]
                    
                    for selector in selectors_to_try:
                        elements = await page.query_selector_all(selector)
                        for elem in elements:
                            text = await elem.inner_text()
                            match = re.search(r'([\d,]+)', text)
                            if match:
                                try:
                                    potential_count = int(match.group(1).replace(',', ''))
                                    if 0 <= potential_count <= 1000000:
                                        citation_count = max(citation_count, potential_count)
                                except ValueError:
                                    pass
                    
                    if citation_count == 0:
                        page_text = await page.inner_text('body')
                        patterns = [
                            r'([\d,]+)\s*(?:Total\s+)?Citations?',
                            r'Citations?\s*[:\-]?\s*([\d,]+)',
                            r'([\d,]+)\s*\n\s*Citations?'
                        ]
                        
                        for pattern in patterns:
                            match = re.search(pattern, page_text, re.IGNORECASE)
                            if match:
                                count_str = match.group(1).replace(',', '')
                                try:
                                    citation_count = int(count_str)
                                    break
                                except ValueError:
                                    pass
                    
                except PlaywrightTimeoutError:
                    pass
                
                self.authors[author_id]['citation_count'] = citation_count
                successful += 1
                
            except Exception as e:
                print(f"Failed to load profile {auth_url}: {str(e)[:100]}")
                failed += 1
                self.authors[author_id]['citation_count'] = None
            
            finally:
                await page.close()
                await asyncio.sleep(random.uniform(1.0, 2.5))
        
        print(f"Author scraping complete: {successful} successful, {failed} failed")

    def _export_data(self):
        print("\n--- Exporting Data ---")
        
        df_papers = pd.DataFrame(self.papers)
        df_papers.drop_duplicates(subset='paper_id', inplace=True)
        df_papers.to_csv("papers.csv", index=False)
        print(f"✓ Exported {len(df_papers)} papers to papers.csv")
        
        df_authors = pd.DataFrame(list(self.authors.values()))
        df_authors.to_csv("authors.csv", index=False)
        print(f"✓ Exported {len(df_authors)} authors to authors.csv")
        
        df_paper_authors = pd.DataFrame(self.paper_authors)
        df_paper_authors.drop_duplicates(inplace=True)
        df_paper_authors.to_csv("paper_authors.csv", index=False)
        print(f"✓ Exported {len(df_paper_authors)} paper-author relationships to paper_authors.csv")
        
        print("\n--- Summary Statistics ---")
        print(f"Total papers scraped: {len(df_papers)}")
        print(f"Total unique authors: {len(df_authors)}")
        print(f"Average authors per paper: {len(df_paper_authors) / len(df_papers):.1f}")
        if 'citation_count' in df_papers.columns:
            print(f"Papers with citation data: {df_papers['citation_count'].notna().sum()}")
        if 'citation_count' in df_authors.columns:
            print(f"Authors with citation data: {df_authors['citation_count'].notna().sum()}")


if __name__ == "__main__":
    import sys
    
    query = "computer architecture"
    limit = 50
    
    if len(sys.argv) > 1:
        query = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            limit = int(sys.argv[2])
        except ValueError:
            print(f"Invalid limit '{sys.argv[2]}', using default: 50")
    
    print(f"Starting scraper with query='{query}', limit={limit}")
    print()
    
    scraper = SemanticScholarScraper(query=query, limit=limit)
    asyncio.run(scraper.run())