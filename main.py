import asyncio
import random
import re
import hashlib
from typing import List, Dict, Set
import pandas as pd
from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeoutError

try:
    from playwright_stealth import stealth_async
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    print("Warning: playwright-stealth not installed. Running without stealth mode.")
    print("Install with: pip install playwright-stealth")


class SemanticScholarScraper:
    """
    A robust, stealthy scraper for Semantic Scholar designed to extract paper
    metadata and author citation metrics using Playwright.
    
    This is for educational purposes to learn Playwright.
    """

    def __init__(self, query: str = "computer architecture", limit: int = 50):
        self.query = query
        self.limit = limit
        self.base_url = "https://www.semanticscholar.org"
        # URL encoding the query manually or relying on browser handling
        self.search_url = f"{self.base_url}/search?q={query.replace(' ', '%20')}&sort=relevance"
        
        # FIX #1: Added missing list initializers []
        self.papers: List = []  # Was: self.papers: List =
        self.authors: Dict = {}  # Key: Author ID, Value: Author Dict
        self.paper_authors: List = []  # Was: self.paper_authors: List =
        
        # Tracking unique authors to visit
        self.author_urls_to_scrape: Set[str] = set()

    async def run(self):
        """
        Main execution method that orchestrates the scraping pipeline.
        """
        async with async_playwright() as p:
            # Launch browser in headless mode with specific args to reduce detection
            browser = await p.chromium.launch(
                headless=True,  # Set to False for debugging
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox"
                ]
            )
            
            # Create a context with a realistic user agent and viewport
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                # FIX #2: Added extra headers to appear more human-like
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
                }
            )
            
            # Create a page and apply stealth scripts if available
            page = await context.new_page()
            if STEALTH_AVAILABLE:
                await stealth_async(page)
            
            print(f"--- Starting Scraper for query: '{self.query}' ---")
            
            # Phase 1: Scrape Papers
            await self._scrape_search_results(page)
            
            # Phase 2: Scrape Author Details
            print(f"--- Extracting details for {len(self.author_urls_to_scrape)} unique authors ---")
            await self._scrape_author_profiles(context)
            
            # Phase 3: Export Data
            await browser.close()
            self._export_data()
            print("--- Scraping Complete. Data saved to CSV. ---")

    async def _scrape_search_results(self, page: Page):
        """
        Navigates search results pages and extracts paper metadata.
        """
        print(f"Navigating to: {self.search_url}")
        await page.goto(self.search_url, wait_until="networkidle")
        
        # FIX #3: Added initial wait for page to stabilize
        await asyncio.sleep(2)
        
        page_count = 0
        while len(self.papers) < self.limit:
            page_count += 1
            print(f"Processing search page {page_count}. Papers collected so far: {len(self.papers)}")
            
            # FIX #4: Try multiple selector strategies for robustness
            try:
                # First, try the most specific selector
                await page.wait_for_selector('[data-test-id="search-result"]', timeout=10000)
            except PlaywrightTimeoutError:
                # If that fails, try a more generic selector
                try:
                    await page.wait_for_selector('.cl-paper-row', timeout=5000)
                except PlaywrightTimeoutError:
                    print("Timeout waiting for paper results. Taking screenshot for debug.")
                    await page.screenshot(path=f"debug_timeout_page_{page_count}.png")
                    # FIX #5: Save HTML for debugging
                    with open(f"debug_page_{page_count}.html", "w", encoding="utf-8") as f:
                        f.write(await page.content())
                    print(f"Saved debug files: debug_timeout_page_{page_count}.png and debug_page_{page_count}.html")
                    break

            # FIX #6: Use more flexible selector that works with current Semantic Scholar structure
            # Try multiple selectors in order of preference
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
                
                # --- Extract Title ---
                # FIX #7: Try multiple title selectors
                title = "Unknown Title"
                title_selectors = ['h3 a', 'h2 a', 'a[data-heap-id="paper_title"]', '.cl-paper-title']
                
                for title_sel in title_selectors:
                    title_el = await card.query_selector(title_sel)
                    if title_el:
                        title = await title_el.inner_text()
                        title = title.strip()
                        break
                
                # --- Extract URL ---
                # FIX #8: More robust URL extraction
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
                            
                            # Extract Semantic Scholar ID from URL
                            # URL format: /paper/Title-Slug/ID or /paper/ID
                            parts = relative_url.rstrip('/').split('/')
                            paper_s2_id = parts[-1]
                            break
                
                # Fallback ID if extraction failed
                if not paper_s2_id:
                    paper_s2_id = hashlib.md5(title.encode()).hexdigest()[:16]
                
                # FIX #9: Extract additional metadata (year, citations)
                year = None
                citation_count = None
                
                # Try to extract year
                year_el = await card.query_selector('.cl-paper-pubdates')
                if year_el:
                    year_text = await year_el.inner_text()
                    year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                    if year_match:
                        year = int(year_match.group())
                
                # Try to extract citation count
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
                
                # Store Paper Data
                self.papers.append({
                    "paper_id": paper_s2_id,
                    "title": title,
                    "url": paper_url,
                    "year": year,
                    "citation_count": citation_count
                })
                
                # --- Extract Authors ---
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
                        # Handle both relative and absolute URLs
                        if not auth_href.startswith('http'):
                            full_auth_url = self.base_url + auth_href
                        else:
                            full_auth_url = auth_href
                        
                        # Extract Author ID from URL: /author/Name/12345
                        parts = auth_href.rstrip('/').split('/')
                        author_id = parts[-1]
                        
                        # Add to relation table
                        self.paper_authors.append({
                            "paper_id": paper_s2_id,
                            "author_id": author_id
                        })
                        
                        # Add to authors map (initially without citation count)
                        if author_id not in self.authors:
                            self.authors[author_id] = {
                                "author_id": author_id,
                                "name": auth_name,
                                "profile_url": full_auth_url,
                                "citation_count": None  # To be filled
                            }
                            self.author_urls_to_scrape.add(full_auth_url)

            # --- Pagination Logic ---
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
                
                # Check if button exists and is not disabled
                if next_button:
                    is_disabled = await next_button.get_attribute('disabled')
                    is_enabled = await next_button.is_enabled()
                    
                    if is_enabled and not is_disabled:
                        print("Navigating to next page...")
                        await next_button.click()
                        # FIX #12: Better wait strategy
                        await page.wait_for_load_state('networkidle')
                        # Add random delay to appear more human-like
                        await asyncio.sleep(random.uniform(2.0, 4.0))
                    else:
                        print("Next button is disabled. No more pages available.")
                        break
                else:
                    print("No next button found. Reached end of results.")
                    break
            
            # FIX #13: Safety limit to prevent infinite loops
            if page_count > 20:
                print("Reached maximum page limit (20 pages). Stopping.")
                break

    async def _scrape_author_profiles(self, context: BrowserContext):
        """
        Visits each unique author profile to extract citation counts.
        """
        # FIX #14: Added progress tracking and error handling
        total_authors = len(self.author_urls_to_scrape)
        successful = 0
        failed = 0
        
        for i, auth_url in enumerate(self.author_urls_to_scrape, 1):
            # Extract ID to update the correct record
            author_id = auth_url.split('/')[-1]
            
            # Progress log
            if i % 5 == 0 or i == total_authors:
                print(f"Scraping author {i}/{total_authors} (✓ {successful}, ✗ {failed})")

            page = await context.new_page()
            if STEALTH_AVAILABLE:
                await stealth_async(page)
            
            try:
                await page.goto(auth_url, wait_until="domcontentloaded", timeout=15000)
                
                # FIX #15: More robust citation extraction with multiple strategies
                citation_count = 0
                
                try:
                    # Strategy 1: Wait for author stats section
                    await page.wait_for_selector('.author-detail-cards', timeout=5000)
                    
                    # Strategy 2: Look for citation text patterns
                    # Semantic Scholar often displays stats in various formats
                    selectors_to_try = [
                        '.author-detail-cards__stat-value',
                        '[data-test-id="author-citation-count"]',
                        '.stats-row__stat-value',
                        'div.author-stats'
                    ]
                    
                    # Try each selector
                    for selector in selectors_to_try:
                        elements = await page.query_selector_all(selector)
                        for elem in elements:
                            text = await elem.inner_text()
                            # Look for numbers that might be citations
                            match = re.search(r'([\d,]+)', text)
                            if match:
                                try:
                                    potential_count = int(match.group(1).replace(',', ''))
                                    # Sanity check: citations are usually reasonable numbers
                                    if 0 <= potential_count <= 1000000:
                                        citation_count = max(citation_count, potential_count)
                                except ValueError:
                                    pass
                    
                    # Strategy 3: Text-based search for "Citations"
                    if citation_count == 0:
                        page_text = await page.inner_text('body')
                        # Look for patterns like "12,450 Citations" or "12450\nCitations"
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
                    # If page doesn't load properly, we'll keep citation_count as 0
                    pass
                
                # Update the author record
                self.authors[author_id]['citation_count'] = citation_count
                successful += 1
                
            except Exception as e:
                print(f"Failed to load profile {auth_url}: {str(e)[:100]}")
                failed += 1
                # Still update with None to indicate we tried
                self.authors[author_id]['citation_count'] = None
            
            finally:
                await page.close()
                # FIX #16: Increased rate limiting to be more respectful
                await asyncio.sleep(random.uniform(1.0, 2.5))
        
        print(f"Author scraping complete: {successful} successful, {failed} failed")

    def _export_data(self):
        """
        Writes the collected data to three CSV files.
        """
        print("\n--- Exporting Data ---")
        
        # 1. Papers CSV
        df_papers = pd.DataFrame(self.papers)
        # Drop duplicates if any
        df_papers.drop_duplicates(subset='paper_id', inplace=True)
        df_papers.to_csv("papers.csv", index=False)
        print(f"✓ Exported {len(df_papers)} papers to papers.csv")
        
        # 2. Authors CSV
        df_authors = pd.DataFrame(list(self.authors.values()))
        df_authors.to_csv("authors.csv", index=False)
        print(f"✓ Exported {len(df_authors)} authors to authors.csv")
        
        # 3. Paper-Authors CSV
        df_paper_authors = pd.DataFrame(self.paper_authors)
        df_paper_authors.drop_duplicates(inplace=True)
        df_paper_authors.to_csv("paper_authors.csv", index=False)
        print(f"✓ Exported {len(df_paper_authors)} paper-author relationships to paper_authors.csv")
        
        # FIX #17: Added summary statistics
        print("\n--- Summary Statistics ---")
        print(f"Total papers scraped: {len(df_papers)}")
        print(f"Total unique authors: {len(df_authors)}")
        print(f"Average authors per paper: {len(df_paper_authors) / len(df_papers):.1f}")
        if 'citation_count' in df_papers.columns:
            print(f"Papers with citation data: {df_papers['citation_count'].notna().sum()}")
        if 'citation_count' in df_authors.columns:
            print(f"Authors with citation data: {df_authors['citation_count'].notna().sum()}")

if __name__ == "__main__":
    # FIX #18: Added command-line argument support for easy testing
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
    print("You can also run: python script.py 'your query' 100")
    print()
    
    scraper = SemanticScholarScraper(query=query, limit=limit)
    asyncio.run(scraper.run())