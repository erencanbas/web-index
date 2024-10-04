from tqdm.asyncio import tqdm_asyncio
import asyncio
import aiohttp
import os
import requests
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials
import json


SCOPES = ["https://www.googleapis.com/auth/indexing"]
ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"
URLS_PER_ACCOUNT = 200

from aiohttp.client_exceptions import ServerDisconnectedError

async def send_url(session, http, url):
    content = {
        'url': url.strip(),
        'type': "URL_UPDATED"
    }
    for _ in range(3): 
        try:
            async with session.post(ENDPOINT, json=content, headers={"Authorization": f"Bearer {http}"}, ssl=False) as response:
                return await response.text()
        except ServerDisconnectedError:
            await asyncio.sleep(2)  
            continue
    return '{"error": {"code": 500, "message": "Server Disconnected after multiple retries"}}'  # Return a custom error message after all retries fail

async def indexURL(http, urls):
    successful_urls = 0
    error_429_count = 0
    other_errors_count = 0
    tasks = []

    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(send_url(session, http, url))

        results = await tqdm_asyncio.gather(*tasks, desc="Processing URLs", unit="url")

        for result in results:
            data = json.loads(result)
            if "error" in data:
                if data["error"]["code"] == 429:
                    error_429_count += 1
                else:
                    other_errors_count += 1
            else:
                successful_urls += 1

    print(f"\nTotal URLs Tried: {len(urls)}")
    print(f"Successful URLs: {successful_urls}")
    print(f"URLs with Error 429: {error_429_count}")

def setup_http_client(json_key_file):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key_file, scopes=SCOPES)
    token = credentials.get_access_token().access_token
    return token

def fetch_urls_from_sitemap(url):
    urls = []
    try:
        response = requests.get(url)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            
            for elem in root.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
                urls.append(elem.text)
        else:
            print(f"Error fetching sitemap {url}: HTTP {response.status_code}")
    except Exception as e:
        print(f"Error fetching or parsing sitemap {url}: {e}")
    return urls

async def main_async(num_accounts, all_urls):
    for i in range(num_accounts):
        print(f"\nProcessing URLs for Account {i+6}...")
        json_key_file = f"account{i+1}.json"

        
        if not os.path.exists(json_key_file):
            print(f"Error: {json_key_file} not found!")
            continue

        start_index = i * URLS_PER_ACCOUNT
        end_index = start_index + URLS_PER_ACCOUNT
        urls_for_account = all_urls[start_index:end_index]
        
        http = setup_http_client(json_key_file)
        await indexURL(http, urls_for_account)

def main():
    
    num_accounts = int(input("How many accounts have you created (1-5)? "))
    if not 1 <= num_accounts <= 5:
        print("Invalid number of accounts. Please enter a number between 1 and 5.")
        return

    
    sitemap_urls = [
        "https://crowstv.com/post-sitemap.xml"
    ]

    
    all_urls = []
    for sitemap_url in sitemap_urls:
        all_urls.extend(fetch_urls_from_sitemap(sitemap_url))

    if not all_urls:
        print("Error: No URLs found in the sitemaps!")
        return

    
    asyncio.run(main_async(num_accounts, all_urls))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript paused. Press Enter to resume or Ctrl+C again to exit.")
        input()
        main()
