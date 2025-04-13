import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from movie_crawler.imdb_crawler import IMDBCrawler
from movie_crawler.metacritic_crawler import MetacriticCrawler
from movie_crawler.rotten_crawler import RottenTomatoesCrawler

load_dotenv()

def get_existing_links():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("DB_PORT", "5432"),
        )
        cursor = conn.cursor()
        cursor.execute("SELECT link FROM movies")
        links = {row[0] for row in cursor.fetchall()}
        return links
    except Exception as e:
        print(f"Error fetching existing links: {e}")
        return set()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def normalize_review(review, source: str):
    if not isinstance(review, dict):
        print(f"Skipping invalid review item: {review} (type: {type(review)})")
        return None
    role = review.get("role", "user")  # Lấy role từ review, mặc định là "user"
    if source == "imdb":
        return {
            "review": review.get("review", "No Review"),
            "score": review.get("score", "No Score"),
            "author_name": review.get("author_name", "No Author"),
            "review_date": review.get("review_date", None),
            "link": review.get("link", None),
            "role": role,
        }
    elif source == "rotten":
        sentiment = review.get("sentiment", "N/A")
        score = {
            "POSITIVE": "Positive",
            "NEGATIVE": "Negative",
            "NEUTRAL": "Neutral",
            "N/A": "No Score",
        }.get(sentiment, "No Score")
        return {
            "review": review.get("review", "No Review"),
            "score": score,
            "author_name": review.get("author", "N/A"),
            "review_date": review.get("review_date", None),
            "link": review.get("link", None),
            "role": role,
        }
    elif source == "metacritic":
        return {
            "review": review.get("review", "No Review"),
            "score": review.get("score", "No Score"),
            "author_name": review.get("author_name", "No Author"),
            "review_date": review.get("review_date", None),
            "link": review.get("link", None),
            "role": role,
        }
    return None

def save_reviews_to_postgres(reviews, movie_name: str, source: str, movie_link: str = None):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("DB_PORT", "5432"),
        )
        cursor = conn.cursor()

        # Đảm bảo movie_name không phải None
        if not movie_name:
            movie_name = "Unknown Movie"

        # Lưu phim vào bảng movies
        query = """
        INSERT INTO movies (movie_name, link)
        VALUES (%s, %s)
        ON CONFLICT (movie_name, link) DO UPDATE
        SET movie_name = EXCLUDED.movie_name
        RETURNING movie_id
        """
        cursor.execute(query, (movie_name, movie_link))
        movie_id = cursor.fetchone()[0]

        if not isinstance(reviews, list):
            print(f"Reviews is not a list: {reviews} (type: {type(reviews)})")
            return

        # Chuẩn hóa đánh giá
        normalized_reviews = [normalize_review(r, source) for r in reviews]
        normalized_reviews = [r for r in normalized_reviews if r is not None]
        if not normalized_reviews:
            print(f"No valid reviews to save for {movie_name}")
            return

        # Lưu đánh giá vào bảng reviews, bao gồm trường role
        query = """
        INSERT INTO reviews (movie_id, review, score, author_name, review_date, source, role)
        VALUES %s
        """
        values = [
            (movie_id, r["review"], r["score"], r["author_name"], r["review_date"], source, r["role"])
            for r in normalized_reviews
        ]
        execute_values(cursor, query, values)

        conn.commit()
        print(f"Saved {len(normalized_reviews)} {source} reviews for {movie_name}")

    except Exception as e:
        print(f"Error saving reviews to database: {e}")
        if conn:
            conn.rollback()
        raise  # Ném lỗi để ReviewsAPIView có thể bắt
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            

if __name__ == "__main__":
    existing_links = get_existing_links()
    
    # Initialize crawlers
    imdb_crawler = IMDBCrawler()
    metacritic_crawler = MetacriticCrawler()
    rotten_crawler = RottenTomatoesCrawler()

    # Number of films to crawl
    target_films = 10

    # 1. Rotten Tomatoes
    print("Crawling films from Rotten Tomatoes...")
    rotten_base_url = "https://www.rottentomatoes.com/browse/movies_at_home/"
    rotten_film_list = rotten_crawler.get_film_list(rotten_base_url, target_films=target_films)
    print(f"Total films from Rotten Tomatoes: {len(rotten_film_list)}")

    # Get reviews for each film
    for title, link in list(rotten_film_list.items())[:target_films]:
        if link in existing_links:
            print(f"Skipping {title} (Rotten Tomatoes) - Already in database")
            continue
        
        print(f"Crawling reviews for {title} (Rotten Tomatoes)...")
        rotten_critic_reviews = rotten_crawler.get_reviews(f"{link}/reviews", role="critic")
        rotten_user_reviews = rotten_crawler.get_reviews(f"{link}/reviews?type=user", role="user")
        rotten_reviews = rotten_critic_reviews + rotten_user_reviews
        save_reviews_to_postgres(rotten_reviews, title, "rotten", link)

    # 2. Metacritic
    print("\nCrawling films from Metacritic...")
    metacritic_base_url = "https://www.metacritic.com/browse/movie/all/all/all-time/new/?releaseYearMin=1910&releaseYearMax=2025&page="
    metacritic_film_list = {}

    metacritic_film_list = metacritic_crawler.get_movie_list(metacritic_base_url, min_movies=target_films)

    print(f"Total films from Metacritic: {len(metacritic_film_list)}")

    # Get reviews for each film
    for title, link in metacritic_film_list.items():
        if link in existing_links:
            print(f"Skipping {title} (Metacritic) - Already in database")
            continue
        print(f"Crawling reviews for {title} (Metacritic)...")
        critic_review_url = f"{link}critic-reviews/"
        user_review_url = f"{link}user-reviews/"
        
        meta_reviews = metacritic_crawler.get_reviews(critic_review_url, role="critic")
        meta_reviews += metacritic_crawler.get_reviews(user_review_url, role="user")
        
        save_reviews_to_postgres(meta_reviews, title, "metacritic", link)

    # 3. IMDb
    print("\nCrawling films from IMDb...")
    imdb_base_url = "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,desc"
    imdb_film_list = imdb_crawler.get_film_list(imdb_base_url, target_films=target_films)
    print(f"Total films from IMDb: {len(imdb_film_list)}")

    # Get reviews for each film
    for title, link in list(imdb_film_list.items())[:target_films]:
        if link in existing_links:
            print(f"Skipping {title} (IMDB) - Already in database")
            continue
        print(f"Crawling reviews for {title} (IMDb)...")
        review_url = imdb_crawler.convert_to_review_url(link)
        imdb_reviews = imdb_crawler.get_reviews(review_url)
        save_reviews_to_postgres(imdb_reviews, title, "imdb", link)

    print("Finished crawling and saving films and reviews to database.")