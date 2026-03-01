import asyncio
import csv
import re
import argparse
import random
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from urllib.parse import urljoin
from abc import ABC, abstractmethod
import subprocess
import sys

# Robust selectors for 2024/2025

# Global debug flag (set via CLI)
DEBUG_MODE = False
BUSINESS_CARD_SELECTOR = 'a[href*="/maps/place/"]'
NAME_SELECTOR = 'div.fontHeadlineSmall'
WEBSITE_SELECTOR = 'a[data-item-id="authority"]'
PHONE_SELECTOR = 'button[data-tooltip="Copy phone number"]'
ADDRESS_SELECTOR = 'button[data-item-id="address"]'
RATING_SELECTOR = 'span.ceNzR' 
REVIEWS_SELECTOR = 'span[aria-label*="reviews"]' 
CATEGORY_SELECTOR = 'button[jsaction="pane.rating.category"]'

EMAIL_REGEX = r'[a-zA-Z0-9._%+-]+(?:\s*\[at\]\s*|\s*@\s*)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

class Fetcher(ABC):
    """Base class for all fetchers."""
    @abstractmethod
    def fetch(self, *args, **kwargs):
        pass

class AsyncFetcher(Fetcher):
    """Fetcher base with asyncio support and concurrency management."""
    def __init__(self, concurrency=5):
        self.semaphore = asyncio.Semaphore(concurrency)

    @abstractmethod
    async def fetch(self, *args, **kwargs): # type: ignore
        pass

class StealthyFetcher(AsyncFetcher):
    def __init__(self, concurrency=5, user_agent=None, locale="en-GB"):
        super().__init__(concurrency)
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        self.locale = locale

    async def apply_stealth(self, page):
        await Stealth().apply_stealth_async(page)

class DynamicFetcher(StealthyFetcher):
    def __init__(self, query, max_results=10, concurrency=8, debug=False):
        super().__init__(concurrency)
        self.query = query
        self.max_results = max_results
        self.results = []
        self.processed_names = set()
        self.playwright = None
        self.debug = debug

    async def extract_website_data(self, browser, url: str):
        """Deep Spider: Aggressive extraction of emails and social links."""
        async with self.semaphore:
            if not url or url == "N/A":
                return {"Emails": "N/A", "Social Links": "N/A"}
            
            print(f"  Deep Spidering: {url}")
            emails, socials = await self.spider_website(browser, url)

            social_str = " | ".join([f"{k}: {v}" for k, v in socials.items()])
            return {
                "Emails": ", ".join(list(emails)) if emails else "N/A",
                "Social Links": social_str if social_str else "N/A"
            }

    async def spider_website(self, browser, url: str):
        """Recursive discovery of contact information across subpages."""
        if not url or url == 'N/A':
            return set(), {}

        emails, socials = set(), {}
        visited = set()
        to_visit = [url]
        keywords = ['contact', 'about', 'team', 'staff', 'legal', 'terms', 'privacy', 'faq', 'help', 'support']
        
        try:
            context = await browser.new_context(user_agent=self.user_agent)
            page = await context.new_page()
            await self.apply_stealth(page)
                
            while to_visit and len(visited) < 5:
                current_url = to_visit.pop(0)
                if current_url in visited: continue
                visited.add(current_url)
                
                try:
                    print(f"    Spidering: {current_url}")
                    await page.goto(current_url, wait_until="domcontentloaded", timeout=15000)
                    content = await page.content()
                    
                    e, s = self.extract_contacts(content)
                    emails.update(e)
                    socials.update(s)
                    
                    if current_url == url:
                        links = await page.query_selector_all('a')
                        for link in links:
                            href = await link.get_attribute('href')
                            if href:
                                full_url = urljoin(url, href)
                                # Strict domain check: only follow links within the same domain
                                if url in full_url and any(kw in full_url.lower() for kw in keywords):
                                    if full_url not in visited and full_url not in to_visit:
                                        to_visit.append(full_url)
                except Exception as page_err:
                    print(f"      [!] Error on {current_url}: {page_err}")
            
            await context.close()
        except Exception as e:
            print(f"    [!] Spider Error for {url}: {e}")

        return emails, socials

    def extract_contacts(self, html: str):
        """Advanced regex for email and social discovery."""
        raw_emails = re.findall(EMAIL_REGEX, html)
        emails = {e.replace(' [at] ', '@').replace(' ', '') for e in raw_emails}
        
        social_patterns = {
            'Facebook': r'facebook\.com/[^/\s"\'>]+',
            'Instagram': r'instagram\.com/[^/\s"\'>]+',
            'LinkedIn': r'linkedin\.com/(?:company|in)/[^/\s"\'>]+',
            'Twitter/X': r'(?:twitter\.com|x\.com)/[^/\s"\'>]+',
            'TikTok': r'tiktok\.com/@[^/\s"\'>]+',
            'YouTube': r'youtube\.com/(?:c|user|channel)/[^/\s"\'>]+'
        }
        
        found_socials = {}
        for platform, pattern in social_patterns.items():
            matches = re.findall(pattern, html)
            if matches:
                found_socials[platform] = f"https://{matches[0].split('?')[0]}"

        return emails, found_socials

    async def fetch(self, *args, **kwargs):
        async with async_playwright() as p:
            self.playwright = p
            main_browser = await p.chromium.launch(headless=not self.debug)
            context = await main_browser.new_context(user_agent=self.user_agent, locale=self.locale)
            page = await context.new_page()
            await self.apply_stealth(page)
            
            print(f"Searching Google Maps for: {self.query}")
            await page.goto(f"https://www.google.com/maps/search/{self.query.replace(' ', '+')}")
            
            try:
                consent_btn = page.get_by_role("button", name=re.compile("Accept all", re.IGNORECASE))
                if await consent_btn.count() > 0:
                    await consent_btn.click()
                    await page.wait_for_timeout(2000)
            except: pass

            try:
                await page.wait_for_selector(BUSINESS_CARD_SELECTOR, timeout=10000)
            except:
                print("No results found.")
                await main_browser.close()
                return

            while len(self.results) < self.max_results:
                cards = await page.locator(BUSINESS_CARD_SELECTOR).all()
                if not cards: break

                for card in cards:
                    if len(self.results) >= self.max_results: break
                    try:
                        name = (await card.get_attribute("aria-label")) or "N/A"
                        if name in self.processed_names: continue
                        self.processed_names.add(name)
                        print(f"Processing: {name}")

                        await card.click()
                        # Use wait_for_selector instead of hardcoded timeout for the name overlay
                        try:
                            await page.wait_for_selector(NAME_SELECTOR, timeout=5000)
                        except: pass
                        
                        # Sequential processing with jittered delay
                        await asyncio.sleep(random.uniform(1.5, 3.0))

                        res = {
                            "Business Name": name,
                            "Website": "N/A",
                            "Phone Number": "N/A",
                            "Address": "N/A",
                            "Rating": "N/A",
                            "Category": "N/A"
                        }
                        try:
                            if await page.locator(WEBSITE_SELECTOR).count() > 0:
                                res["Website"] = await page.locator(WEBSITE_SELECTOR).first.get_attribute("href") or "N/A"
                            if await page.locator(PHONE_SELECTOR).count() > 0:
                                phone_aria = await page.locator(PHONE_SELECTOR).first.get_attribute("aria-label") or ""
                                res["Phone Number"] = phone_aria.replace("Phone: ", "").replace("Calling ", "") or "N/A"
                            if await page.locator(ADDRESS_SELECTOR).count() > 0:
                                res["Address"] = (await page.locator(ADDRESS_SELECTOR).first.inner_text()).replace("\n", " ")
                            if await page.locator(RATING_SELECTOR).count() > 0:
                                res["Rating"] = await page.locator(RATING_SELECTOR).first.get_attribute("aria-label") or "N/A"
                            if await page.locator(CATEGORY_SELECTOR).count() > 0:
                                res["Category"] = await page.locator(CATEGORY_SELECTOR).first.inner_text()
                        except Exception as field_e:
                            print(f"  Warning: {field_e}")

                        self.results.append(res)
                    except Exception as e:
                        print(f"Error: {e}")
                        continue
                
                # Robust Scroll
                try:
                    feed = page.locator('div[role="feed"]')
                    if await feed.count() > 0:
                        last_count = len(await page.locator(BUSINESS_CARD_SELECTOR).all())
                        await feed.evaluate("node => node.scrollTop += 3000")
                        await asyncio.sleep(2)
                        new_count = len(await page.locator(BUSINESS_CARD_SELECTOR).all())
                        if new_count == last_count:
                             # Try one more time with a larger scroll if no new cards appeared
                             await feed.evaluate("node => node.scrollTop += 5000")
                             await asyncio.sleep(2)
                    if (await page.locator(BUSINESS_CARD_SELECTOR).count()) <= len(self.processed_names): 
                        # Check if "You've reached the end of the list" is visible
                        if await page.get_by_text("You've reached the end of the list").is_visible():
                            break
                        # If not, maybe more results are loading
                except: break

            # Deep Spidering Phase
            print(f"Starting Deep Spider for {len(self.results)} firms...")
            website_tasks = [self.extract_website_data(main_browser, str(record["Website"])) for record in self.results]
            all_contact_info = await asyncio.gather(*website_tasks)
            
            for i, contact_info in enumerate(all_contact_info):
                self.results[i].update(contact_info)
            
            await main_browser.close()

    def save(self, filename="results_optimized.csv"):
        if not self.results: return
        keys = self.results[0].keys()
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.results)
        print(f"Saved {len(self.results)} entries to {filename}")

async def main():
    parser = argparse.ArgumentParser(description="Deep Maps Scraper")
    parser.add_argument("query", type=str, help="Search query")
    parser.add_argument("--max", type=int, default=10, help="Max results")
    parser.add_argument("--out", type=str, default="results_optimized.csv", help="Output CSV name")
    parser.add_argument("--debug", action="store_true", help="Run browser in visible mode for debugging")
    args = parser.parse_args()

    global DEBUG_MODE
    DEBUG_MODE = args.debug

    # Ensure Playwright browsers are installed
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install"], check=True)
    except Exception as e:
        print(f"[!] Failed to install Playwright browsers: {e}")
        return

    fetcher = DynamicFetcher(args.query, args.max, debug=DEBUG_MODE)
    try:
        await fetcher.fetch()
        fetcher.save(args.out)
    except Exception as e:
        print(f"[!] Scraper encountered an error: {e}")
        # Optionally, you could add more cleanup here

if __name__ == "__main__":
    asyncio.run(main())
