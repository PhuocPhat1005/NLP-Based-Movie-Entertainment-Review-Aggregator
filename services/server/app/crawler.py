import os
import traceback
from datetime import datetime
from apify_client import ApifyClient
from .config import Config

def normalize_review_date(date_str):
    """
    Converts string date like 'January 1, 2020' to 'YYYY-MM-DD'.
    Returns None if parsing fails.
    """
    try:
        return datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')
    except Exception:
        return None

def get_reviews_from_source(movie_id, max_reviews=100):
    """
    Fetches raw reviews for a movie from IMDb using Apify.
    Returns a list of dicts in the format expected by reviews_raw.
    """
    try:
        print(f"[Crawler] Starting to fetch reviews for movie_id: {movie_id}")

        # Load Apify token from env or config
        apify_token = os.environ.get("APIFY_API_TOKEN") or Config.APIFY_API_TOKEN
        if not apify_token:
            raise ValueError("Missing APIFY_API_TOKEN in environment or config.")

        # Initialize Apify client
        client = ApifyClient(apify_token)

        # Run actor
        run_input = {
            "movieIds": [movie_id],
            "maxReviews": max_reviews,
        }
        run = client.actor("apify/imdb-reviews-scraper").call(run_input=run_input)
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise ValueError("Failed to retrieve dataset ID from Apify run.")

        results = []
        for item in client.dataset(dataset_id).iterate_items():
            review_id = f"{movie_id}_{item.get('reviewId')}"  # Ensure global uniqueness
            review = {
                "review_id": review_id,
                "reviewer_username": item.get("username"),
                "submission_date": normalize_review_date(item.get("date")),
                "rating": item.get("rating"),
                "like_count": item.get("helpfulnessScore", {}).get("upVotes", 0),
                "dislike_count": item.get("helpfulnessScore", {}).get("downVotes", 0),
                "review_text_raw": item.get("text"),
                "movie_id": movie_id,
            }
            results.append(review)

        print(f"[Crawler] Retrieved {len(results)} reviews for {movie_id}")
        return results

    except Exception as e:
        print(f"[Crawler] Error while crawling movie {movie_id}: {e}")
        traceback.print_exc()
        return []
