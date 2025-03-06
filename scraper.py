import threading
import queue
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random
from datetime import datetime
import unicodedata
import re

# MongoDB Ayarları
MONGO_URI = "mongodb://scraper:scraper123@localhost:27017/incisozluk?authSource=incisozluk"
DB_NAME = "incisozluk"
COLLECTION_NAME = "entries"

# Scraper Ayarları
THREAD_COUNT = 8
START_ID = 1122443
END_ID = 1122443
REQUEST_DELAY = 1.2
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15'
]

# MongoDB Bağlantısı
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

task_queue = queue.Queue()
wiki_data = {}
lock = threading.Lock()

def clean_text(text):
    """Gelişmiş metin temizleme ve normalizasyon"""
    text = unicodedata.normalize('NFKC', str(text))
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def baslik_to_slug(baslik):
    """Başlıktan slug oluşturma"""
    baslik = baslik.lower()
    baslik = re.sub(r'[^a-z0-9 ğüşıöç]', '', baslik)
    baslik = baslik.replace(' ', '-')
    baslik = unicodedata.normalize('NFKD', baslik).encode('ascii', 'ignore').decode('ascii')
    return baslik

def parse_wiki(slug):
    """Wiki sayfasından başlık ve ilk entry'i çekme"""
    try:
        url = f"https://incisozluk.co/w/{slug}"
        response = requests.get(url, headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=20)

        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'lxml')

        # Başlık ve ilk entry
        baslik = clean_text(soup.find('h1', {'class': 'title'}).text)
        first_entry = soup.find('li', {'class': 'entry'})

        # Entry detayları
        entry_text = clean_text(first_entry.find('div', {'class': 'entry-text-wrap'}).text)
        entry_id = int(re.search(r'/e/(\d+)', first_entry.find('a', {'class': 'entry-tarih'})['href']).group(1))

        # Oy bilgileri
        votes = {
            "artı": clean_text(first_entry.find('span', {'class': 'puan_suku'}).text),
            "eksi": clean_text(first_entry.find('span', {'class': 'puan_cuku'}).text)
        }

        return {
            "baslik": baslik,
            "entry_id": entry_id,
            "entry": entry_text,
            "oy": votes
        }

    except Exception as e:
        print(f"Wiki hatası ({slug}): {str(e)}")
        return None

def parse_entry(entry_id):
    """Normal entry sayfasını parse etme"""
    try:
        url = f"https://incisozluk.co/e/{entry_id}"
        response = requests.get(url, headers={'User-Agent': random.choice(USER_AGENTS)}, timeout=20)

        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'lxml')

        # Temel bilgiler
        baslik = clean_text(soup.find('h1', {'class': 'title'}).text)
        main_entry = soup.find('li', {'class': 'entry'})

        # Ana entry detayları
        entry_text = clean_text(main_entry.find('div', {'class': 'entry-text-wrap'}).text)
        tarih = datetime.strptime(main_entry.find('a', {'class': 'entry-tarih'})['title'], "%d-%m-%Y %H:%M")

        # Oy bilgileri
        votes = {
            "artı": clean_text(main_entry.find('span', {'class': 'puan_suku'}).text),
            "eksi": clean_text(main_entry.find('span', {'class': 'puan_cuku'}).text)
        }

        # Cevaplar
        cevaplar = []
        reply_section = main_entry.find_next_sibling('li', {'class': 'replay'})
        if reply_section:
            for reply in reply_section.find_all('li', {'class': 'entry'}):
                reply_text = clean_text(reply.find('div', {'class': 'entry-text-wrap'}).text)
                reply_votes = {
                    "artı": clean_text(reply.find('span', {'class': 'puan_suku'}).text),
                    "eksi": clean_text(reply.find('span', {'class': 'puan_cuku'}).text)
                }
                cevaplar.append({
                    "entry": reply_text,
                    "oy": reply_votes
                })

        return {
            "entry_id": entry_id,
            "baslik": baslik,
            "entry": entry_text,
            "tarih": tarih.isoformat(),
            "entry_oy": votes,
            "entry_cevaplar": cevaplar
        }

    except Exception as e:
        print(f"Entry hatası (#{entry_id}): {str(e)}")
        return None

def worker():
    while True:
        task = task_queue.get()

        try:
            # Wiki işleme
            if isinstance(task, str):
                wiki_result = parse_wiki(task)
                if wiki_result:
                    with lock:
                        slug = baslik_to_slug(wiki_result['baslik'])
                        wiki_data[slug] = {
                            "baslik": wiki_result['baslik'],
                            "entry": wiki_result['entry'],
                            "oy": wiki_result['oy']
                        }
                    print(f"📖 Wiki kaydedildi: {wiki_result['baslik']}")

            # Normal entry işleme
            elif isinstance(task, int):
                time.sleep(REQUEST_DELAY * random.uniform(0.9, 1.1))
                entry_data = parse_entry(task)

                if entry_data:
                    # Wiki verilerini entegre et
                    slug = baslik_to_slug(entry_data['baslik'])
                    with lock:
                        if slug in wiki_data:
                            entry_data["baslik_ilk_yorum"] = {
                                "entry": wiki_data[slug]['entry'],
                                "oy": wiki_data[slug]['oy']
                            }

                    # MongoDB'ye kaydet
                    collection.update_one(
                        {"entry_id": entry_data['entry_id']},
                        {"$set": entry_data},
                        upsert=True
                    )
                    print(f"✅ #{entry_data['entry_id']} kaydedildi")

        except Exception as e:
            print(f"🔥 Kritik hata: {str(e)}")

        finally:
            task_queue.task_done()

# Thread'leri başlat
for _ in range(THREAD_COUNT):
    threading.Thread(target=worker, daemon=True).start()


# Normal entry'leri ekle
for entry_id in range(START_ID, END_ID + 1):
    task_queue.put(entry_id)

# İşlemleri bekle
task_queue.join()
client.close()
print("🎉 Tüm veriler başarıyla kaydedildi!")