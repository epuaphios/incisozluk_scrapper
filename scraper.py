import threading
import queue
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random
from datetime import datetime

# MongoDB Ayarları
MONGO_URI = "mongodb://scraper:scraper123@localhost:27017/incisozluk?authSource=incisozluk"
DB_NAME = "incisozluk"
COLLECTION_NAME = "entries"

# Scraper Ayarları
THREAD_COUNT = 3
START_ID = 208028525
END_ID = 208028525  # Tek entry testi
REQUEST_DELAY = 1
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15'
]

# MongoDB Bağlantısı
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

task_queue = queue.Queue()

def parse_single_entry(entry_tag, is_main_entry=False):
    # Ortak Parsing
    entry_text = entry_tag.find('div', class_='entry-text-wrap').text.strip()

    author_tag = entry_tag.find('a', class_='username')
    author_name = author_tag.text.strip() if author_tag else None
    author_profile = author_tag['href'] if author_tag else None

    date_tag = entry_tag.find('a', class_='entry-tarih')
    raw_date = date_tag.get('title') if date_tag else ""

    try:
        parsed_date = datetime.strptime(raw_date, "%d-%m-%Y %H:%M").isoformat()
    except:
        parsed_date = datetime.now().isoformat()

    # Oy Sayıları
    upvote = entry_tag.find('span', class_='puan_suku')
    downvote = entry_tag.find('span', class_='puan_cuku')

    data = {
        "entry": entry_text,
        "yazar": {
            "username": author_name,
            "profile": author_profile
        },
        "tarih": parsed_date,
        "oy": {
            "artı": upvote.find('strong').text.strip() if upvote and upvote.find('strong') else "0",
            "eksi": downvote.find('strong').text.strip() if downvote and downvote.find('strong') else "0"
        }
    }

    # Sadece ana entry'de başlık
    if is_main_entry:
        data["baslik"] = entry_tag.find_previous('h1', class_='title').text.strip()

    return data

def parse_html(html, entry_id):
    soup = BeautifulSoup(html, 'lxml')

    # Ana Entry
    main_entry = soup.find('li', class_='entry')
    if not main_entry:
        return None

    data = parse_single_entry(main_entry, is_main_entry=True)
    data['entry_id'] = entry_id

    # Cevaplar
    replies = []
    reply_section = main_entry.find_next_sibling('li', class_='replay')
    if reply_section:
        for reply in reply_section.find_all('li', class_='entry'):
            reply_data = parse_single_entry(reply)
            replies.append(reply_data)

    data['cevaplar'] = replies
    return data

def worker():
    while True:
        entry_id = task_queue.get()

        try:
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            time.sleep(REQUEST_DELAY * random.uniform(0.8, 1.2))

            url = f"https://incisozluk.co/e/{entry_id}"
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                data = parse_html(response.text, entry_id)
                if data:
                    collection.update_one(
                        {"entry_id": entry_id},
                        {"$set": data},
                        upsert=True
                    )
                    print(f"✅ #{entry_id} kaydedildi - {len(data['cevaplar'])} cevap")
                else:
                    print(f"⚠️ #{entry_id} geçersiz HTML yapısı")

            elif response.status_code == 404:
                print(f"⏩ #{entry_id} bulunamadı")
            else:
                print(f"⚠️ #{entry_id} hata kodu: {response.status_code}")

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