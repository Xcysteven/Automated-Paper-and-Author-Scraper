# Semantic Scholar Automated Scraper
This project is an automated web scraper built to extract academic papers, their authors, and author citation counts from Semantic Scholar. It is designed to navigate modern anti-bot protections, handle dynamic JavaScript rendering, and gracefully recover from session blocks.

# Approach and Key Design Choices
Undetected Chromedriver: Utilized seleniumbase with Undetected Chromedriver (uc=True) to bypass basic automated browser detection and handle Cloudflare Turnstile challenges.

Interleaved Scraping: Instead of scraping all papers and then all authors (which creates an unnatural, easily detectable traffic spike), the scraper processes a page of papers and immediately visits those specific authors' profiles. This mimics human browsing behavior.

The "Hard Reset" Strategy: To combat severe IP or session blocks, the scraper implements a self-healing browser mechanism. If a page fails to load or triggers an unresolvable CAPTCHA, the script completely kills the ChromeDriver process, dumps the flagged session cookies, and spins up a fresh browser to retry the exact same URL without losing previously scraped data.

Explicit Waits over Static Sleeps: Used Selenium's WebDriverWait to dynamically wait for specific DOM elements (like paper cards and citation numbers) to render. This eliminates the "skipped page" issue and speeds up the script when the network is fast.

# How Exactly 50 Results Were Ensured
The exact limit of 50 papers was strictly enforced using a combination of loop conditions and immediate break statements:

A limit parameter is passed into the class (defaulting to 50).

The primary pagination loop (while len(self.papers) < self.limit) ensures the scraper keeps requesting new search pages until the quota is met.

An inner loop iterates through the paper cards on each page. At the very beginning of this loop, a strict check (if len(self.papers) >= self.limit: break) halts execution the moment the 50th paper is added to the list, preventing any overflow from the final page of search results.

# Assumptions About "Citation Count" and Extraction Logic
Assumption: The required metric was the total accumulated citations for the main author of the profile being visited, excluding the specific citation counts of their co-authors listed on the same page.

Wait Strategy: Because Semantic Scholar loads the author header before the citation statistics, the script explicitly waits for the .author-detail-card__stats-row__value element to render to avoid scraping a premature "0".

Tiered Extraction Logic: To ensure accuracy, the script uses a fallback extraction strategy:

Exact UI Targeting: Looks for the specific stat row labeled "Citations" and extracts the adjacent value (converting "k" formats like "6.4k" to 6400). # although I don't recall if there's any (but I just have it there just be safe).

Contextual Regex: If the UI changes, it scrapes the raw text of the page, explicitly splitting the text at the "Co-Authors" section to guarantee it only searches for the [Number] Citations pattern within the main author's profile data.

Card Fallback: Finally, it targets the general .author-detail-card container as a last-resort regex search.

# Known Limitations and Production Improvements
Limitations
Speed vs. Stealth: The scraper relies on natural human pacing (randomized delays of 2–4 seconds) and browser resets. This makes it highly robust but relatively slow for massive datasets. (Super duper slow as it takes like 6 minutes to run and it will continuously open tabs)

UI Brittleness: The extraction relies on specific CSS selectors (.cl-paper-row, .author-detail-card). If Semantic Scholar updates its frontend layout, the scraper will require maintenance.

Hardware Overhead: Running full browser instances (even headless) consumes significant RAM and CPU compared to raw HTTP requests.

# Production Improvements
API Integration: In a true production environment, scraping Semantic Scholar is unnecessary and inefficient. I would transition this to use the official Semantic Scholar Academic Graph (S2AG) API, which is legal, infinitely faster, and provides cleaner JSON data. (And I only need less than 100 line of code to get the things we wanted (but I assume this is not what we wanted so I chose to use this))

Checkpointing: For scraping thousands of records, I would implement database checkpointing (e.g., SQLite) rather than in-memory Python lists to prevent data loss in the event of a fatal system crash. 