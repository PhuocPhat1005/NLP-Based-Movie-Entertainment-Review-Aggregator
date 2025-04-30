import json
import os
import re
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920x1080')
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def fetch_chart(driver, url, title_type_hint):
    driver.get(url)
    time.sleep(5)  # Chờ page load hoàn chỉnh

    soup = BeautifulSoup(driver.page_source, "html.parser")

    movies = []

    if "chart/top" in url:
        movie_rows = soup.select("table.chart.full-width tr")
    else:
        movie_rows = soup.select("ul.ipc-metadata-list li.ipc-metadata-list-summary-item")

    if not movie_rows:
        print(f"Warning: No movies found on page {url}")
        return []

    for row in movie_rows:
        try:
            if "chart/top" in url:
                title_column = row.select_one("td.titleColumn")
                rating_column = row.select_one("td.imdbRating")

                if not title_column:
                    continue

                link = title_column.find("a")
                href = link['href']
                movie_id = href.split('/')[2]
                name = link.get_text(strip=True)

                year_text = title_column.find("span", class_="secondaryInfo").get_text()
                year = int(re.sub(r'[()]', '', year_text))

                rating = 0.0
                if rating_column:
                    rating_text = rating_column.get_text(strip=True)
                    if rating_text:
                        rating = float(rating_text)

            else:
                link_tag = row.select_one("a.ipc-title-link-wrapper")
                if not link_tag:
                    continue

                href = link_tag['href']
                movie_id = href.split('/')[2]
                name = link_tag.get_text(strip=True)

                year_tag = row.select_one("span.ipc-metadata-list-summary-item__li")
                year = None
                if year_tag:
                    year_text = year_tag.get_text(strip=True)
                    match = re.search(r'(\d{4})', year_text)
                    if match:
                        year = int(match.group(1))

                rating = 0.0
                rating_tag = row.select_one("span.ipc-rating-star")
                if rating_tag and rating_tag.get_text(strip=True):
                    try:
                        rating = float(rating_tag.get_text(strip=True))
                    except:
                        pass

                if not year:
                    year = 0

            movies.append({
                "id": movie_id,
                "name": name,
                "year": year,
                "rating": rating,
                "votes": 0,
                "genres": [],
                "countries": [],
                "user_reviews_count": 0,
                "title_type": title_type_hint,
                "languages": []
            })
        except Exception as e:
            print(f"Error parsing item: {e}")

    return movies

def get_top_movies_and_tv():
    driver = init_driver()
    all_movies = []

    sources = [
        ("https://www.imdb.com/chart/top", "movie"),
        ("https://www.imdb.com/chart/moviemeter", "movie"),
        ("https://www.imdb.com/chart/toptv", "tvSeries"),
        ("https://www.imdb.com/chart/tvmeter", "tvSeries"),
    ]

    for url, title_type in sources:
        print(f"Fetching from: {url}")
        movies = fetch_chart(driver, url, title_type)
        all_movies.extend(movies)

    driver.quit()

    # Remove duplicates by ID
    unique_movies = {movie['id']: movie for movie in all_movies}

    print(f"Total unique titles collected: {len(unique_movies)}")

    return list(unique_movies.values())

def save_movies(movies, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(movies, f, indent=4, ensure_ascii=False)

    print(f"Saved {len(movies)} movies/TV shows to {output_path}")

if __name__ == "__main__":
    if not os.path.exists("./output"):
        os.makedirs("./output")

    top_titles = get_top_movies_and_tv()
    save_movies(top_titles, "output/filtered_movies.json")
