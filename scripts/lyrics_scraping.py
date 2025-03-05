# ==========================
# 1. Импорт модулей
# ==========================
import requests
from bs4 import BeautifulSoup
import csv
import time
import pandas as pd

# ==========================
# 2. Настройка сессии и заголовков
# ==========================
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}
session = requests.Session()
session.headers.update(headers)

def fetch_url(url, retries=3, delay=5):
    """
    Пытается выполнить запрос к URL с заданным количеством попыток.
    При неудаче делает задержку и повторяет запрос.
    """
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                return response
            else:
                print(f"Неправильный статус {response.status_code} для {url}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе {url}: {e}. Попытка {attempt} из {retries}.")
        time.sleep(delay)
    return None

# ==========================
# 3. Настройка параметров парсинга
# ==========================
base_url = "https://text-pesenok.ru"
csv_filename = "scripts/songs.csv"
max_songs = 100  # ограничиваемся первыми 100 песнями
song_count = 0
first_song_printed = False

# ==========================
# 4. Парсинг страниц и сбор данных
# ==========================
with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["song_url", "song_title", "song_artist", "song_text"])

    # Для примера пройдём по страницам с 1 до 50 (или пока не соберем 100 песен)
    for page_num in range(1, 51):
        if song_count >= max_songs:
            break

        page_url = f"{base_url}/?page={page_num}"
        print(f"\nПарсим страницу {page_num}: {page_url}")

        resp = fetch_url(page_url)
        if not resp:
            print(f"Не удалось загрузить страницу {page_url}. Пропускаем.")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        song_cards = soup.select("div.item")
        if not song_cards:
            print("Песен на странице не найдено. Прерываем.")
            break

        for card in song_cards[:1]:
            if song_count >= max_songs:
                break

            title_link = card.select_one("h2 a")
            if not title_link:
                continue

            song_title = title_link.get_text(strip=True)
            song_href = title_link.get("href", "")
            if song_href.startswith("/"):
                song_href = base_url + song_href

            artist_div = card.select_one("div.item__artist")
            song_artist = artist_div.get_text(strip=True) if artist_div else ""

            # Задержка перед запросом страницы песни
            time.sleep(1)

            resp_song = fetch_url(song_href)
            if not resp_song:
                print(f"Не удалось загрузить страницу песни {song_href}. Пропускаем.")
                continue

            soup_song = BeautifulSoup(resp_song.text, "html.parser")
            # Предположим, что текст песни находится в блоке <div class="text">
            lyrics_div = soup_song.select_one("div.text")
            song_text = lyrics_div.get_text("\n", strip=True) if lyrics_div else ""

            writer.writerow([song_href, song_title, song_artist, song_text])
            song_count += 1

            if not first_song_printed:
                print("\nПервая песня:")
                print("URL:", song_href)
                print("Название:", song_title)
                print("Исполнитель:", song_artist)
                print("Текст (первые 200 символов):", (song_text[:200] + "...") if len(song_text) > 200 else song_text)
                first_song_printed = True

        # Задержка после каждой страницы
        time.sleep(2)

print(f"\nСобрано песен: {song_count}")
print(f"Парсинг завершён! Результаты в файле: {csv_filename}")

# ==========================
# 5. Вывод первых строк CSV
# ==========================
df = pd.read_csv(csv_filename)
print("\nПервые 5 строк из файла songs.csv:")
print(df.head())
