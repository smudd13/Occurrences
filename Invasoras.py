import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ScraperAPI Key
SCRAPER_API_KEY = "d4d86748a59efc1fcccca0975027cf1e"

# Occurrence URLs
occurrence_urls = [
    "https://www.gbif.org/occurrence/1270744105",
    "https://www.gbif.org/occurrence/1270744166",
    "https://www.gbif.org/occurrence/1270744127",
    # Add remaining URLs here
]

# Image save directory
destination_dir = r"C:\Users\smudd\images_invasoras"
os.makedirs(destination_dir, exist_ok=True)

# Construct ScraperAPI URL
def scraperapi_url(url):
    return f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url}"

# Function to scrape a single page with aiohttp
async def scrape_with_aiohttp(session: aiohttp.ClientSession, url: str):
    try:
        logging.debug(f"Scraping occurrence page URL: {url}")
        async with session.get(scraperapi_url(url), timeout=15) as response:
            logging.debug(f"Response status for {url}: {response.status}")
            if response.status != 200:
                logging.error(f"Failed to scrape {url}, status code: {response.status}")
                return []

            html = await response.text()
            logging.debug(f"HTML content for {url} retrieved")
            soup = BeautifulSoup(html, 'html.parser')

            # Extract image page links from occurrence page
            image_page_links = []
            media_section = soup.find('section', id='occurrencePage_media')
            if media_section:
                logging.debug(f"Media section found for {url}")
                media_items = media_section.find_all('img', class_='imgContainer')
                for tag in media_items:
                    parent_a_tag = tag.find_parent('a')
                    if parent_a_tag and parent_a_tag.get('href'):
                        href = parent_a_tag.get('href')
                        full_url = f"https://www.gbif.org{href}"
                        logging.debug(f"Found image page link: {full_url}")
                        image_page_links.append(full_url)
                    else:
                        logging.debug(f"Image link not found in tag: {tag}")
            else:
                logging.debug(f"No media section found for {url}")

            # Remove duplicates
            image_page_links = list(set(image_page_links))
            logging.debug(f"Found {len(image_page_links)} image page(s) at {url}")
            return image_page_links
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return []

# Function to scrape an image page and download the image
async def scrape_and_download_image(session: aiohttp.ClientSession, image_page_url: str, occurrence_id: str, index: int):
    try:
        logging.debug(f"Scraping image page URL: {image_page_url}")
        async with session.get(scraperapi_url(image_page_url), timeout=15) as response:
            logging.debug(f"Response status for {image_page_url}: {response.status}")
            if response.status != 200:
                logging.error(f"Failed to scrape image page {image_page_url}, status code: {response.status}")
                return

            html = await response.text()
            logging.debug(f"HTML content for image page {image_page_url} retrieved")
            soup = BeautifulSoup(html, 'html.parser')

            # Extract the direct image URL from the secondary page
            img_tag = soup.find('img', class_='imgContainer')
            if img_tag and img_tag.get('src'):
                image_url = f"https:{img_tag.get('src')}"
                logging.debug(f"Found final image URL: {image_url}")
                await download_image(session, image_url, occurrence_id, index)
            else:
                logging.error(f"No image link found on image page {image_page_url}")
    except Exception as e:
        logging.error(f"Error scraping image page {image_page_url}: {e}")

# Function to download an image
async def download_image(session: aiohttp.ClientSession, image_url: str, occurrence_id: str, index: int):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'}
    try:
        logging.debug(f"Downloading image from URL: {image_url}")
        async with session.get(image_url, headers=headers, timeout=15) as response:
            logging.debug(f"Response status for image URL {image_url}: {response.status}")
            if response.status != 200:
                logging.error(f"Failed to download image {image_url}, status code: {response.status}")
                return

            content_type = response.headers.get('Content-Type', '')
            logging.debug(f"Content-Type for image {image_url}: {content_type}")
            if not content_type.startswith('image'):
                logging.error(f"Invalid content type for image {image_url}: {content_type}")
                return

            image_extension = content_type.split('/')[-1]
            if image_extension == 'jpeg':
                image_extension = 'jpg'

            image_filename = f"{occurrence_id}_{index}.{image_extension}"
            image_path = os.path.join(destination_dir, image_filename)

            with open(image_path, 'wb') as image_file:
                image_file.write(await response.read())
            logging.info(f"Image saved: {image_filename}")

    except Exception as e:
        logging.error(f"Error downloading image {image_url}: {e}")

# Main function
async def main():
    connector = aiohttp.TCPConnector(limit=100)  # Increase concurrent connections for speed
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in occurrence_urls:
            occurrence_id = url.rstrip('/').split('/')[-1]
            tasks.append(scrape_with_aiohttp(session, url))

        # Gather all scraping tasks concurrently
        results = await asyncio.gather(*tasks)

        # Download images concurrently using a two-step process
        download_tasks = []
        for occurrence_id, image_urls in zip([url.split('/')[-1] for url in occurrence_urls], results):
            if image_urls:
                for idx, image_page_url in enumerate(image_urls, start=1):
                    logging.debug(f"Processing image page URL: {image_page_url} for occurrence ID: {occurrence_id}")
                    download_tasks.append(scrape_and_download_image(session, image_page_url, occurrence_id, idx))

        await asyncio.gather(*download_tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("\nScript interrupted. Exiting gracefully.")