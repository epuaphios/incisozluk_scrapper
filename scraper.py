import threading
import queue
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random
from datetime import datetime
import unicodedata

# MongoDB Ayarları
MONGO_URI = "mongodb://scraper:scraper123@localhost:27017/incisozluk?authSource=incisozluk"
DB_NAME = "incisozluk"
COLLECTION_NAME = "entries"

# Scraper Ayarları
THREAD_COUNT = 10
START_ID = 100102
END_ID = 100102
REQUEST_DELAY = 1
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15'
]

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

task_queue = queue.Queue()

def clean_text(text):
    text = unicodedata.normalize('NFKC', str(text))
    return text.encode('utf-8', 'ignore').decode('utf-8').strip()

def get_wiki_first_comment(slug):
    try:
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        url = f"https://incisozluk.co/w/{slug}"
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content.decode('utf-8', 'ignore'), 'lxml')
            first_entry = soup.find('li', class_='entry')
            if first_entry:
                entry_div = first_entry.find('div', class_='entry-text-wrap')
                return clean_text(entry_div.get_text()) if entry_div else ""
        return ""
    except Exception as e:
        print(f"Wiki Çekme Hatası: {str(e)}")
        return ""

def parse_html(html, entry_id):
    try:
        soup = BeautifulSoup(html.decode('utf-8', 'ignore'), 'lxml')

        # Başlık ve wiki slug'ını çek
        title_tag = soup.find('h1', class_='title')
        baslik = ""
        wiki_slug = ""
        if title_tag:
            title_link = title_tag.find('a')
            if title_link:
                wiki_slug = title_link.get('href', '').split('/w/')[-1].strip('/')
                baslik = clean_text(title_link.get_text())

        # Ana entry verileri
        main_entry = soup.find('li', class_='entry')
        if not main_entry:
            return None

        # Tarih işleme
        date_tag = main_entry.find('a', class_='entry-tarih')
        raw_date = date_tag.get('title', '') if date_tag else ""
        try:
            parsed_date = datetime.strptime(raw_date, "%d-%m-%Y %H:%M").isoformat()
        except:
            parsed_date = datetime.now().isoformat()

        # Oy bilgileri
        upvote = main_entry.find('span', class_='puan_suku')
        downvote = main_entry.find('span', class_='puan_cuku')

        # Başlığın ilk yorumunu çek
        baslik_ilk_yorum = get_wiki_first_comment(wiki_slug) if wiki_slug else ""

        data = {
            "entry_id": entry_id,
            "baslik": baslik,
            "entry": clean_text(main_entry.find('div', class_='entry-text-wrap').get_text()),
            "baslik_ilk_yorum": baslik_ilk_yorum,
            "tarih": parsed_date,
            "entry_oy": {
                "artı": clean_text(upvote.find('strong').get_text()) if upvote and upvote.find('strong') else "0",
                "eksi": clean_text(downvote.find('strong').get_text()) if downvote and downvote.find('strong') else "0"
            },
            "entry_cevaplar": []
        }

        # Cevapları işle
        reply_section = main_entry.find_next_sibling('li', class_='replay')
        if reply_section:
            for reply in reply_section.find_all('li', class_='entry'):
                reply_text = reply.find('div', class_='entry-text-wrap')
                reply_upvote = reply.find('span', class_='puan_suku')
                reply_downvote = reply.find('span', class_='puan_cuku')

                data["entry_cevaplar"].append({
                    "entry": clean_text(reply_text.get_text()) if reply_text else "",
                    "oy": {
                        "artı": clean_text(reply_upvote.find('strong').get_text()) if reply_upvote and reply_upvote.find('strong') else "0",
                        "eksi": clean_text(reply_downvote.find('strong').get_text()) if reply_downvote and reply_downvote.find('strong') else "0"
                    }
                })

        return data

    except Exception as e:
        print(f"Parse Hatası: {str(e)}")
        return None

def worker():
    while True:
        entry_id = task_queue.get()

        try:
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            time.sleep(REQUEST_DELAY * random.uniform(0.8, 1.2))

            url = f"https://incisozluk.co/e/{entry_id}"
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                data = parse_html(response.content, entry_id)
                if data:
                    collection.update_one(
                        {"entry_id": entry_id},
                        {"$set": data},
                        upsert=True
                    )
                    print(f"✅ #{entry_id} kaydedildi - İlk yorum: {data['baslik_ilk_yorum'][:30]}...")

            elif response.status_code == 404:
                print(f"⏩ #{entry_id} bulunamadı")
            else:
                print(f"⚠️ #{entry_id} hata: {response.status_code}")

        except Exception as e:
            print(f"🔥 #{entry_id} hata: {str(e)}")
        finally:
            task_queue.task_done()

# Thread'leri Başlat
for _ in range(THREAD_COUNT):
    threading.Thread(target=worker, daemon=True).start()

# Queue'yu Doldur
for entry_id in range(START_ID, END_ID + 1):
    task_queue.put(entry_id)

# Bekleme ve Temizlik
task_queue.join()
client.close()
print("🎉 İşlem tamamlandı!")