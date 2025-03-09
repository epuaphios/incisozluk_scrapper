#!/usr/bin/env python3
import asyncio
import aiohttp
import logging
import random
import time
import unicodedata
from datetime import datetime
from typing import Optional, Dict, Any
from selectolax.parser import HTMLParser
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import ClientSession, TCPConnector

# ---------------------------- Config ----------------------------
MONGO_URI = "mongodb://scraper:scraper123@192.168.2.105:27017/incisozluk?authSource=incisozluk"
DB_NAME = "incisozluk"
COLLECTION_NAME = "entries"

CONCURRENT_TASKS = 250  # Eşzamanlı request sayısı
REQUEST_TIMEOUT = 25
MAX_RETRIES = 15
BASE_DELAY = 0.3
ENTRY_START = 3500000
ENTRY_END = 3500002

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15'
]

# ---------------------------- Setup ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('InciScraper')

# ---------------------------- Helpers ----------------------------
def clean_text(text: str) -> str:
    """Metni normalize etme ve temizleme"""
    text = unicodedata.normalize('NFKC', text)
    return text.encode('utf-8', 'ignore').decode('utf-8').strip()

import uuid

def generate_random_cookies():
    # Rastgele çerez adı ve değeri oluştur
    cookie_name = f"cookie_{uuid.uuid4().hex[:6]}"
    cookie_value = uuid.uuid4().hex
    return f"{cookie_name}={cookie_value}"

# ---------------------------- Parser ----------------------------
async def parse_html(html: str, entry_id: int) -> Optional[Dict[str, Any]]:
    try:
        if not html:  # HTML boşsa
            logger.warning(f"Empty HTML for #{entry_id}. Skipping...")
            return None  # İşlemi durdur

        tree = HTMLParser(html)

        # Başlık ve Wiki bilgisi
        title_tag = tree.css_first('h1.title a')
        baslik = clean_text(title_tag.text()) if title_tag else ""
        wiki_slug = title_tag.attrs.get('href', '').split('/w/')[-1] if title_tag else ""

        # Ana entry
        main_entry = tree.css_first('li.entry')
        if not main_entry:
            return None

        # Tarih
        date_tag = main_entry.css_first('a.entry-tarih')
        raw_date = date_tag.attrs.get('title', '') if date_tag else ""
        try:
            parsed_date = datetime.strptime(raw_date, "%d-%m-%Y %H:%M").isoformat()
        except ValueError:
            parsed_date = datetime.now().isoformat()

        # Oylar
        upvote = main_entry.css_first('span.puan_suku strong')
        downvote = main_entry.css_first('span.puan_cuku strong')

        # Başlık ilk yorumu
        baslik_ilk_yorum = await get_wiki_first_comment(wiki_slug) if wiki_slug else ""

        data = {
            "entry_id": entry_id,
            "baslik": baslik,
            "entry": clean_text(main_entry.css_first('div.entry-text-wrap').text()),
            "baslik_ilk_yorum": baslik_ilk_yorum,
            "tarih": parsed_date,
            "entry_oy": {
                "artı": clean_text(upvote.text()) if upvote else "0",
                "eksi": clean_text(downvote.text()) if downvote else "0"
            },
            "entry_cevaplar": []
        }

        # Cevaplar
        reply_section = main_entry.next
        while reply_section and not reply_section.css_matches('li.replay'):
            reply_section = reply_section.next

        if reply_section:
            for reply in reply_section.css('li.entry'):
                reply_text = reply.css_first('div.entry-text-wrap')
                reply_up = reply.css_first('span.puan_suku strong')
                reply_down = reply.css_first('span.puan_cuku strong')

                data["entry_cevaplar"].append({
                    "entry": clean_text(reply_text.text()) if reply_text else "",
                    "oy": {
                        "artı": clean_text(reply_up.text()) if reply_up else "0",
                        "eksi": clean_text(reply_down.text()) if reply_down else "0"
                    }
                })

        return data
    except Exception as e:
        logger.error(f"Parse error #{entry_id}: {str(e)}")
        return None
async def get_wiki_first_comment(slug: str) -> str:
    """Wiki ilk yorumu çekme"""
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Cookie': generate_random_cookies(),  # Rastgele çerezler
            'Referer': 'https://incisozluk.co/',
            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
        }
        url = f"https://incisozluk.co/w/{slug}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    html = await response.text()
                    if not html:  # HTML boşsa
                        logger.warning(f"Empty HTML for wiki slug: {slug}. Skipping...")
                        return ""  # İşlemi durdur

                    tree = HTMLParser(html)
                    first_entry = tree.css_first('li.entry div.entry-text-wrap')
                    return clean_text(first_entry.text()) if first_entry else ""
                return ""
    except Exception as e:
        logger.error(f"Wiki error: {str(e)}")
        return ""
# ---------------------------- Scraper Core ----------------------------
class InciScraper:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(CONCURRENT_TASKS)
        self.rate_limiter = RateLimiter(requests_per_second=100)
        self.client = AsyncIOMotorClient(MONGO_URI)
        self.collection = self.client[DB_NAME][COLLECTION_NAME]

    async def fetch_entry(self, session: ClientSession, entry_id: int) -> Optional[Dict]:
        """Entry çekme ve işleme"""
        while True:  # Sonsuz döngü, başarılı olana kadar veya manuel olarak durdurulana kadar devam eder
            for attempt in range(MAX_RETRIES):
                try:
                    async with self.semaphore, self.rate_limiter:
                        url = f"https://incisozluk.co/e/{entry_id}"
                        headers = {
                            'User-Agent': random.choice(USER_AGENTS),
                            'Cookie': generate_random_cookies(),  # Rastgele çerezler
                            'Referer': 'https://incisozluk.co/',
                            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
                        }

                        try:
                            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                                if response.status == 200:
                                    html = await response.text()
                                    if not html:  # HTML boşsa
                                        logger.warning(f"Empty HTML for #{entry_id}. Skipping...")
                                        return None  # İşlemi durdur

                                    data = await parse_html(html, entry_id)
                                    if data:
                                        await self.collection.update_one(
                                            {"entry_id": entry_id},
                                            {"$set": data},
                                            upsert=True
                                        )
                                        logger.info(f"Processed #{entry_id}")
                                    return data
                                elif response.status == 404:
                                    logger.debug(f"Entry {entry_id} not found")
                                    return None
                                else:
                                    raise Exception(f"HTTP {response.status}")
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout for #{entry_id}, attempt {attempt + 1}/{MAX_RETRIES}")
                            if attempt < MAX_RETRIES - 1:
                                await asyncio.sleep((attempt + 1) * 5)  # Artan bekleme süresi
                            else:
                                raise
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed for #{entry_id}: {str(e)}")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep((attempt + 1) * 2)  # Backoff
                    else:
                        logger.error(f"Max retries reached for #{entry_id}. Waiting 10 minutes before retrying...")
                        await asyncio.sleep(600)  # 10 dakika bekle
                        break
    async def run(self):
        """Ana çalıştırıcı"""
        connector = TCPConnector(limit=0, ssl=False)
        async with ClientSession(connector=connector) as session:
            tasks = []
            for entry_id in range(ENTRY_START, ENTRY_END + 1):
                task = asyncio.create_task(self.fetch_entry(session, entry_id))
                tasks.append(task)
                if len(tasks) >= CONCURRENT_TASKS * 2:
                    await asyncio.gather(*tasks)
                    tasks = []
                    logger.info(f"Progress: {entry_id}/{ENTRY_END}")
            await asyncio.gather(*tasks)

# ---------------------------- Rate Limiter ----------------------------
class RateLimiter:
    """Dinamik rate limiter"""
    def __init__(self, requests_per_second: int):
        self.rate = requests_per_second
        self.tokens = requests_per_second
        self.last_update = time.monotonic()

    async def __aenter__(self):
        """Asenkron context manager girişi"""
        while self.tokens <= 0:
            self._add_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Asenkron context manager çıkışı"""
        pass  # Herhangi bir temizlik yapmaya gerek yok

    def _add_tokens(self):
        """Yeni token'lar ekleme"""
        now = time.monotonic()
        elapsed = now - self.last_update
        new_tokens = elapsed * self.rate
        self.tokens = min(self.rate, self.tokens + new_tokens)
        self.last_update = now

# ---------------------------- Main ----------------------------
if __name__ == "__main__":
    scraper = InciScraper()
    try:
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        logger.info("Scraper stopped by user")
    finally:
        scraper.client.close()