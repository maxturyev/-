from yandex_music import Client
import polars as pl
import logging
import requests
import re
import csv


# Логирование
logging.getLogger().handlers.clear()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yandex_music_parsing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def search_artist(client, name):
    """
    Ищет исполнителя по имени и возвращает объект Artist, если найден
    """
    try:
        result = client.search(name, type_='artist')
        return result.artists.results[0] if result.artists else None
    except Exception as e:
        logging.error(f"Artist search error {name}: {str(e)}")
        return None

def get_albums(artist, max_albums=10):
    """
    Возвращает список (до max_albums) альбомов исполнителя
    """
    try:
        albums = artist.get_albums(page_size=max_albums, sort_by='rating')
        return albums
    except Exception as e:
        logging.error(f"Album load error for artist {artist.name}: {str(e)}")
        return []

def get_tracks(album):
    """
    Возвращает список треков альбома
    """
    try:
        return album.with_tracks().volumes[0]
        # return artist.get_tracks(page_size=100)
    except Exception as e:
        logging.error(f"Tracks load error: {str(e)}")
        return []

def get_lyrics(track):
    try:
        url = track.get_lyrics().download_url
        response = requests.get(url, timeout=5)
        lyrics = response.text
    except Exception as e:
        logging.warning(f"Primary lyrics source failed")
        try:
            lyrics_obj = track.get_supplement().lyrics
            lyrics = lyrics_obj.full_lyrics
        except Exception as e:
            logging.warning(f"Supplement lyrics source failed")
            lyrics = None
    return lyrics

def process_lyrics(lyrics):
    """
    Возвращает кол-во слов и уникальных слов в тексте
    """
    if not lyrics:
        return None, None
    words = re.findall(r'\w+', lyrics.lower())
    return len(words), len(set(words))

def get_track_data(track, album, artist):
    """
    Формирует словарь с нужными данными по треку
    """
    try:
        lyrics = get_lyrics(track)
        word_count, unique_word_count = process_lyrics(lyrics)
        return {
            'title': track.title,
            'is_best': track.id in album.bests if album.bests else False,
            'lyrics': lyrics,
            'word_count': word_count,
            'unique_word_count': unique_word_count,
            'duration_ms': track.duration_ms,
            'content_warning': track.content_warning,
            'available': track.available,
            'main_artist': track.artists[0].name if track.artists else None,
            'other_artists': ', '.join([a.name for a in track.artists[1:]]) if len(track.artists) > 1 else None,
            'artist_monthly_rating': artist.ratings.month if artist.ratings else None,
            'album': album.title,
            'album_track_count': album.track_count,
            'album_genre': album.genre,
            'album_release_year': album.year,
            'album_release_date': album.release_date,
            'album_likes_count': album.likes_count
        }
    except Exception as e:
        logging.warning(f"Track processing error: {str(e)}")
        return None


def main():
    # Авторизация клиента
    TOKEN = 'AQAAAAAc8xzfAAG8Xn5eHJ_jg0lwgCtl2Lgdd84'
    client = Client(TOKEN).init()

    # Загрузка файла с исполнителями
    with open('api_parsing/russian_artists.txt', 'r', encoding='utf-8') as f:
        artists = f.read().splitlines()

    # Парсинг данных о треках
    data = []
    for artist_name in artists:
        try:
            logging.info(f"Processing artist: {artist_name}")
            artist = search_artist(client, artist_name)
            if not artist:
                continue

            albums = get_albums(artist)
            for album in albums:
                try:
                    logging.info(f"Processing album: {album.title}")
                    tracks = get_tracks(album)
                    for track in tracks:
                        track_data = get_track_data(track, album, artist)
                        data.append(track_data)
                        logging.info(f"Added song: {track_data['title']}")
                except Exception as e:
                    logging.error(f"Album {album.id} error: {str(e)}")
        except Exception as e:
            logging.error(f"Error processing {artist_name}: {str(e)}", exc_info=True)

    # Сохранение данных
    if data:
        df = pl.DataFrame(data)
        df = df.with_columns((pl.col('duration_ms') / 1000).alias('duration_s'))
        file = 'tracks.csv'
        df.write_csv(file, include_bom=True)
        logging.info(f"Dataset {file} created with {len(df)} records")
    else:
        logging.warning("No data collected")

if __name__ == '__main__':
    main()

    # Check dataset
    df = pl.read_csv('tracks.csv')
    print(df)

    # Get tracks with no lyrics
    df.filter(df['lyrics'].is_null()).select('title', 'main_artist').write_csv('no_lyrics.csv')