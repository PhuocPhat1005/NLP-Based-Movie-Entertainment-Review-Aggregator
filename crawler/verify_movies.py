import requests
from bs4 import BeautifulSoup
import json
import time

input_file = "output/filtered_movies.json"
output_file = "output/filtered_movies_verified.json"

def search_imdb_id(movie_name):
    query = movie_name.replace(' ', '+')
    url = f"https://www.imdb.com/find?q={query}&s=tt"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        first_result = soup.select_one("td.result_text a")
        if first_result and first_result['href']:
            href = first_result['href']
            if href.startswith("/title/tt"):
                imdb_id = href.split('/')[2]
                return imdb_id
        return None
    except Exception as e:
        print(f"❌ Error searching for {movie_name}: {e}")
        return None

def main():
    with open(input_file, "r", encoding="utf-8") as f:
        movies = json.load(f)

    updated_movies = []
    seen_names = set()

    for idx, movie in enumerate(movies, 1):
        name_key = movie['name'].strip().lower()

        if name_key in seen_names:
            print(f"⚡ Skipping duplicate name: {movie['name']}")
            continue

        print(f"🔍 [{idx}/{len(movies)}] Searching IMDb ID for: {movie['name']}")
        correct_id = search_imdb_id(movie['name'])
        if correct_id:
            movie['id'] = correct_id

        updated_movies.append(movie)
        seen_names.add(name_key)

        time.sleep(1)  # nhẹ nhàng tránh bị chặn

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(updated_movies, f, indent=4, ensure_ascii=False)

    print(f"\n✅ Finished updating and deduplicating. Total movies: {len(updated_movies)}")
    print(f"Output saved to {output_file}")

if __name__ == "__main__":
    main()
