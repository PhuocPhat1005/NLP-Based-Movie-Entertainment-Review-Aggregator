# services/server/app/tasks.py
from . import celery
# --- KẾT THÚC THÊM ---


from .config import Config # Giữ dòng này
import psycopg2
from psycopg2.extras import RealDictCursor
from .crawler import get_reviews_from_source
from .absa import load_absa_models, analyze_sentiment
from .database import get_db_connection, update_movie_absa_status, save_raw_reviews, save_absa_results
from datetime import datetime, date # Needed for date/time types if saving to raw



# Load ABSA models when Celery worker starts
@celery.on_after_configure.connect
def load_models_on_worker_init(sender, **kwargs):
    print("Celery worker starting, loading ABSA models...")
    load_absa_models(Config.NLP_MODEL_PATH, Config.VECTORIZER_PATH)
    print("ABSA models loaded (or failed to load).")


@celery.task(bind=True, max_retries=5, default_retry_delay=90) # Increased retries and delay
@celery.task(bind=True, max_retries=5, default_retry_delay=90)
def process_movie_reviews(self, movie_id, trigger_type='crawl_absa', movie_title=None, movie_year=None):
    """
    Celery task for either:
    - 'crawl_absa': Crawl reviews + ABSA + Save
    - 'absa_only': Load existing reviews + ABSA + Save
    """
    db_conn = None
    try:
        print(f"Task [{trigger_type}] started for movie {movie_id} (retry #{self.request.retries})")

        # 1. Update status at beginning
        db_conn = get_db_connection()
        if db_conn:
            if trigger_type == 'crawl_absa':
                update_movie_absa_status(db_conn, movie_id, Config.STATUS_PROCESSING_CRAWL_ABSA, movie_title, movie_year)
            elif trigger_type == 'absa_only':
                update_movie_absa_status(db_conn, movie_id, Config.STATUS_PROCESSING_ABSA_ONLY)
            db_conn.close()

        # 2. Get reviews (source differs by trigger_type)
        if trigger_type == 'crawl_absa':
            raw_reviews = get_reviews_from_source(movie_id)
            print(f"[Crawl] Fetched {len(raw_reviews)} raw reviews for movie {movie_id}")

            if not raw_reviews:
                db_conn = get_db_connection()
                if db_conn:
                    update_movie_absa_status(db_conn, movie_id, Config.STATUS_COMPLETED_NO_REVIEWS)
                    db_conn.close()
                return {"status": "completed", "note": "No reviews found from crawl."}

            # Normalize + Save raw reviews
            for r in raw_reviews:
                r['movie_id'] = movie_id
                if isinstance(r.get('submission_date'), str):
                    try:
                        r['submission_date'] = datetime.strptime(r['submission_date'], '%Y-%m-%d').date()
                    except:
                        r['submission_date'] = None
            db_conn = get_db_connection()
            if db_conn:
                save_raw_reviews(db_conn, raw_reviews)
                db_conn.close()

        elif trigger_type == 'absa_only':
            db_conn = get_db_connection()
            if db_conn:
                raw_reviews = load_reviews_for_absa(db_conn, movie_id)
                db_conn.close()
            print(f"[ABSA only] Loaded {len(raw_reviews)} existing reviews for movie {movie_id}")

            if not raw_reviews:
                db_conn = get_db_connection()
                if db_conn:
                    update_movie_absa_status(db_conn, movie_id, Config.STATUS_COMPLETED_NO_REVIEWS)
                    db_conn.close()
                return {"status": "completed", "note": "No reviews available in DB."}

        else:
            raise ValueError(f"Unknown trigger_type: {trigger_type}")

        # 3. Run ABSA
        absa_results = []
        if analyze_sentiment is None:
            print("ABSA model not loaded.")
            db_conn = get_db_connection()
            if db_conn:
                update_movie_absa_status(db_conn, movie_id, Config.STATUS_FAILED_ABSA)
                db_conn.close()
            return {"status": "failed", "note": "ABSA model not available"}

        for r in raw_reviews:
            review_id = r.get('review_id')
            review_text = r.get('review_text_raw') or r.get('review_text')  # fallback if raw missing
            if not review_id or not review_text:
                continue

            # Run ABSA (you may replace this with multi-aspect model)
            sentiment_int, confidence = analyze_sentiment(review_text)
            absa_results.append({
                'review_id': review_id,
                'aspect': 'Overall',
                'sentiment_int': sentiment_int,
                'confidence': confidence
            })

        # 4. Save ABSA
        if absa_results:
            db_conn = get_db_connection()
            if db_conn:
                save_absa_results(db_conn, absa_results)
                db_conn.close()
        else:
            print("No ABSA results to save.")

        # 5. Finalize status
        db_conn = get_db_connection()
        if db_conn:
            update_movie_absa_status(db_conn, movie_id, Config.STATUS_COMPLETED_ABSA)
            db_conn.close()

        print(f"Task [{trigger_type}] completed successfully for {movie_id}")
        return {"status": "completed", "movie_id": movie_id, "absa_results_saved": len(absa_results)}

    except Exception as e:
        print(f"Task [{trigger_type}] failed for movie {movie_id}: {e}")
        db_conn = get_db_connection()
        if db_conn:
            update_movie_absa_status(db_conn, movie_id, Config.STATUS_FAILED_CRAWL if trigger_type == 'crawl_absa' else Config.STATUS_FAILED_ABSA)
            db_conn.close()
        raise self.retry(exc=e)
