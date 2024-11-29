import os
import asyncio
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTO
import aiofiles
import random

seasons = list(range(2019, 2025))
DATA_DIR = "data"
SCHEDULES_DIR = os.path.join(DATA_DIR, "schedules")
SCORES_DIR = os.path.join(DATA_DIR, "scores")

MAX_CONCURRENT_REQUESTS = 3
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def get_html(url, selector, sleep_time=5, retry=6):
    html = None
    for i in range(1, retry + 1):
        await asyncio.sleep(sleep_time * i)  # Non-blocking delay with exponential backoff
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                page = await browser.new_page()
                await page.goto(url)
                print(await page.title())
                html = await page.inner_html(selector)
                await browser.close()
        except PlaywrightTO:
            print(f"Timeout Error for {url} (Attempt {i}/{retry})")
            continue
        else:
            break
    if html is None:
        print(f"Failed to retrieve HTML for {url} after {retry} retries.")
    return html

async def scrape_schedule(season):
    url = f"https://www.basketball-reference.com/leagues/NBA_{season}_games.html"
    html = await get_html(url, "#content .filter")
    if not html:
        return

    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a")
    href = [link["href"] for link in links]
    schedules_pages = [f"https://www.basketball-reference.com{link}" for link in href]

    for url in schedules_pages:
        save_path = os.path.join(SCHEDULES_DIR, url.split("/")[-1])
        if os.path.exists(save_path):
            continue

        html = await get_html(url, "#all_schedule")
        if html:
            async with aiofiles.open(save_path, "w+", encoding="utf-8") as f:
                await f.write(html)

async def scrape_sched_helper():
    for season in seasons:
        await scrape_schedule(season)

async def scrape_game(schedules_file):
    async with aiofiles.open(schedules_file, "r", encoding="utf-8") as f:
        html = await f.read()

    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a")
    hrefs = [link.get("href") for link in links]

    box_scores = [link for link in hrefs if link and "boxscore" in link and ".html" in link]
    box_scores = [f"https://www.basketball-reference.com{link}" for link in box_scores]

    for url in box_scores:
        save_path = os.path.join(SCORES_DIR, url.split("/")[-1])
        if os.path.exists(save_path):
            continue

        async with semaphore:
            html = await get_html_random_delay(url, "#content")
            if html:
                async with aiofiles.open(save_path, "w+", encoding="utf-8") as f:
                    await f.write(html)

async def get_html_random_delay(url, selector):
    delay = random.uniform(2, 5)
    await asyncio.sleep(delay)
    return await get_html(url, selector)

async def main():
    await scrape_sched_helper()

    schedules_files = os.listdir(SCHEDULES_DIR)
    schedules_files = [s for s in schedules_files if ".html" in s]
    await asyncio.gather(
        *(scrape_game(os.path.join(SCHEDULES_DIR, file)) for file in schedules_files)
    )

asyncio.run(main())
