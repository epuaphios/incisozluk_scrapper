import threading
import queue
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random

# Ayarlar
THREAD_COUNT = 5  # Eşzamanlı thread sayısı
START_ID = 10000
END_ID = 100000
REQUEST_DELAY = 0.5  # Saniye
USER_AGENTS = [...]  # User-Agent listesi ekle

# MongoDB bağlantısı (Thread-safe pool için)
client = MongoClient("mongodb://localhost:27017/")
db = client["incisozluk"]

# Queue oluştur
task_queue = queue.Queue()

def worker():
    while True:
        entry_id = task_queue.get()

        try:
            # Rastgele User-Agent ve delay
            headers = {'User-Agent': random.choice(USER_AGENTS)}
            time.sleep(REQUEST_DELAY * random.uniform(0.5, 1.5))

            # Veri çek
            url = f"https://incisozluk.co/e/{entry_id}"
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                # Parse et
                soup = BeautifulSoup(response.text, 'lxml')
                data = {
                    "entry_id": entry_id,
                    "title": soup.find('h1', class_='title').text.strip(),
                    "content": soup.find('div', class_='content').text.strip(),
                    "author": soup.find('a', class_='author').text.strip()
                }

                # MongoDB'ye yaz (upsert)
                db.entries.update_one(
                    {"entry_id": entry_id},
                    {"$set": data},
                    upsert=True
                )
                print(f"✅ #{entry_id} kaydedildi")
            else:
                print(f"❌ #{entry_id} hata: {response.status_code}")

        except Exception as e:
            print(f"🔥 #{entry_id} kritik hata: {str(e)}")

        finally:
            task_queue.task_done()

# Thread'leri başlat
for _ in range(THREAD_COUNT):
    threading.Thread(target=worker, daemon=True).start()

# Queue'yu doldur
for entry_id in range(START_ID, END_ID + 1):
    task_queue.put(entry_id)

# Tüm task'ler bitene kadar bekle
task_queue.join()
print("🎉 Tüm işlemler tamamlandı!")