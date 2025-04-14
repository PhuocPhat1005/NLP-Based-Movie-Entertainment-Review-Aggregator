# views.py
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from movie_crawler.rotten_crawler import RottenTomatoesCrawler
from movie_crawler.imdb_crawler import IMDBCrawler
from movie_crawler.metacritic_crawler import MetacriticCrawler
from .crawl_reviews import save_reviews_to_postgres
from model.model import ABSAProcessor

absa_processor = ABSAProcessor(
        model_name="yangheng/deberta-v3-base-absa-v1.1",
        max_length=128,
        test_size=0.2,
        random_state=42
    )

absa_processor.load_model(load_path="./absa_model")

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("DB_PORT", "5432"),
    )

class FilmListAPIView(APIView):
    def get(self, request):
        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            query = "SELECT movie_name, link FROM movies"
            cursor.execute(query)
            films = cursor.fetchall()
            film_list = {film["movie_name"]: film["link"] for film in films}
            return Response({"films": film_list}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

class ReviewsAPIView(APIView):
    def get(self, request):
        movie_link = request.query_params.get("link", None)
        if not movie_link:
            return Response({"error": "Link is required"}, status=status.HTTP_400_BAD_REQUEST)

        source = None
        if "rottentomatoes.com" in movie_link:
            source = "rotten"
        elif "imdb.com" in movie_link:
            source = "imdb"
        elif "metacritic.com" in movie_link:
            source = "metacritic"
        else:
            return Response({"error": "Unsupported link source"}, status=status.HTTP_400_BAD_REQUEST)

        conn = None
        cursor = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Truy vấn movie từ link
            query = "SELECT movie_id, movie_name FROM movies WHERE link = %s"
            cursor.execute(query, (movie_link,))
            movie = cursor.fetchone()

            if movie:
                movie_id = movie["movie_id"]
                movie_name = movie["movie_name"]

                # Truy vấn reviews
                query = """
                SELECT r.review_id, r.review, r.score, r.author_name, r.review_date, r.source, m.link, r.role
                FROM reviews r
                JOIN movies m ON r.movie_id = m.movie_id
                WHERE r.movie_id = %s
                """
                cursor.execute(query, (movie_id,))
                reviews = cursor.fetchall()

                aspects = ["direction", "acting", "plot", "overall", "visuals", "themes", "pacing"]  # Danh sách khía cạnh
                formatted_reviews = []

                for review in reviews:
                    review_id = review["review_id"]
                    review_text = review["review"]

                    # Kiểm tra xem aspect_sentiment đã tồn tại trong bảng aspect_sentiment hay chưa
                    query_aspect = """
                    SELECT aspect, sentiment
                    FROM aspect_sentiment
                    WHERE review_id = %s
                    """
                    cursor.execute(query_aspect, (review_id,))
                    aspect_sentiments = cursor.fetchall()

                    # Chuyển đổi dữ liệu aspect sentiment thành dictionary
                    absa_results = {}
                    if aspect_sentiments:
                        for aspect_sentiment in aspect_sentiments:
                            aspect = aspect_sentiment["aspect"]
                            sentiment = aspect_sentiment["sentiment"]
                            absa_results[aspect] = {"sentiment": sentiment}  # Không có confidence trong DB
                    else:
                        # Nếu chưa có aspect sentiment, chạy mô hình ABSA và lưu vào bảng aspect_sentiment
                        for aspect in aspects:
                            sentiment= absa_processor.predict_sentiment(review_text, aspect)
                            absa_results[aspect] = {"sentiment": sentiment}

                            # Lưu vào bảng aspect_sentiment
                            insert_query = """
                            INSERT INTO aspect_sentiment (review_id, aspect, sentiment)
                            VALUES (%s, %s, %s)
                            """
                            cursor.execute(insert_query, (review_id, aspect, sentiment))
                        conn.commit()

                    # Định dạng dữ liệu trả về cho client
                    formatted_reviews.append({
                        "movie_name": movie_name,
                        "author": review["author_name"],
                        "review": review_text,
                        "link": review["link"],
                        "score": review["score"],
                        "role": review["role"],
                        "source": review["source"],
                        "review_date": review["review_date"],
                        "aspect_sentiments": absa_results
                    })

                response_data = {
                    "movie": {
                        "movie_name": movie_name,
                        "link": movie_link
                    },
                    "reviews": formatted_reviews
                }
                return Response(response_data, status=status.HTTP_200_OK)
            else:
                # Nếu không tìm thấy movie trong DB, tiến hành crawl dữ liệu
                reviews = []
                crawler = None
                if source == "rotten":
                    crawler = RottenTomatoesCrawler()
                elif source == "imdb":
                    crawler = IMDBCrawler()
                else:
                    crawler = MetacriticCrawler()

                if source == "rotten":
                    rotten_critic_reviews = crawler.get_reviews(f"{movie_link}/reviews", role="critic")
                    rotten_user_reviews = crawler.get_reviews(f"{movie_link}/reviews?type=user", role="user")
                    rotten_reviews = rotten_critic_reviews + rotten_user_reviews
                    if not rotten_reviews:
                        return Response({"error": "No reviews found for this movie"}, status=status.HTTP_404_NOT_FOUND)
                    reviews.extend(rotten_reviews)
                    save_reviews_to_postgres(rotten_reviews, movie_name, "rotten", movie_link)
                elif source == "imdb":
                    review_url = crawler.convert_to_review_url(movie_link)
                    imdb_reviews = crawler.get_reviews(review_url, movie_name)
                    if not imdb_reviews:
                        return Response({"error": "No reviews found for this movie"}, status=status.HTTP_404_NOT_FOUND)
                    movie_name = imdb_reviews[0].get("movie_name", movie_name) if imdb_reviews else "Unknown Movie"
                    reviews.extend(imdb_reviews)
                    save_reviews_to_postgres(imdb_reviews, movie_name, "imdb", movie_link)
                else:
                    critic_reviews = crawler.get_reviews(f"{movie_link}critic-reviews/", role="critic")
                    user_reviews = crawler.get_reviews(f"{movie_link}user-reviews/", role="user")
                    meta_reviews = critic_reviews + user_reviews
                    if not meta_reviews:
                        return Response({"error": "No reviews found for this movie"}, status=status.HTTP_404_NOT_FOUND)
                    reviews.extend(meta_reviews)
                    save_reviews_to_postgres(meta_reviews, movie_name, "metacritic", movie_link)

                # Truy vấn lại movie sau khi crawl và lưu vào DB
                query = "SELECT movie_id, movie_name FROM movies WHERE link = %s"
                cursor.execute(query, (movie_link,))
                movie = cursor.fetchone()

                if not movie:
                    return Response({"error": "Failed to save movie to database"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

                movie_id = movie["movie_id"]
                movie_name = movie["movie_name"]

                # Truy vấn lại reviews sau khi crawl
                query = """
                SELECT r.review_id, r.review, r.score, r.author_name, r.review_date, r.source, m.link, r.role
                FROM reviews r
                JOIN movies m ON r.movie_id = m.movie_id
                WHERE r.movie_id = %s
                """
                cursor.execute(query, (movie_id,))
                reviews = cursor.fetchall()

                aspects = ["direction", "acting", "plot", "overall", "visuals", "themes", "pacing"]
                formatted_reviews = []

                for review in reviews:
                    review_id = review["review_id"]
                    review_text = review["review"]

                    # Kiểm tra xem aspect_sentiment đã tồn tại trong bảng aspect_sentiment hay chưa
                    query_aspect = """
                    SELECT aspect, sentiment
                    FROM aspect_sentiment
                    WHERE review_id = %s
                    """
                    cursor.execute(query_aspect, (review_id,))
                    aspect_sentiments = cursor.fetchall()

                    # Chuyển đổi dữ liệu aspect sentiment thành dictionary
                    absa_results = {}
                    if aspect_sentiments:
                        for aspect_sentiment in aspect_sentiments:
                            aspect = aspect_sentiment["aspect"]
                            sentiment = aspect_sentiment["sentiment"]
                            absa_results[aspect] = {"sentiment": sentiment}  # Không có confidence trong DB
                    else:
                        # Nếu chưa có aspect sentiment, chạy mô hình ABSA và lưu vào bảng aspect_sentiment
                        for aspect in aspects:
                            sentiment= absa_processor.predict_sentiment(review_text, aspect)
                            absa_results[aspect] = {"sentiment": sentiment}

                            # Lưu vào bảng aspect_sentiment
                            insert_query = """
                            INSERT INTO aspect_sentiment (review_id, aspect, sentiment)
                            VALUES (%s, %s, %s)
                            """
                            cursor.execute(insert_query, (review_id, aspect, sentiment))
                        conn.commit()

                    # Định dạng dữ liệu trả về cho client
                    formatted_reviews.append({
                        "movie_name": movie_name,
                        "author": review["author_name"],
                        "review": review_text,
                        "link": review["link"],
                        "score": review["score"],
                        "role": review["role"],
                        "source": review["source"],
                        "review_date": review["review_date"],
                        "aspect_sentiments": absa_results
                    })

                response_data = {
                    "movie": {
                        "movie_name": movie_name,
                        "link": movie_link
                    },
                    "reviews": formatted_reviews
                }
                return Response(response_data, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()