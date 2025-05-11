from flask import Flask, request, jsonify
from .config import Config # Assuming config.py is in the same directory
from .database import get_db_connection, check_movie_absa_status, update_movie_absa_status, get_absa_results # Import DB functions
from .tasks import process_movie_reviews # Import your Celery task

app = Flask(__name__)
app.config.from_object(Config)

# Optional: Load ABSA models in Flask app context if needed elsewhere (e.g., for a sync task)
# But primarily loaded in Celery workers via @celery.on_after_configure

# Context processor to manage DB connection per request (optional but good practice)
# from flask import g
# @app.before_request
# def before_request():
#     g.db = get_db_connection()

# @app.teardown_appcontext
# def teardown_db(exception):
#     db = g.pop('db', None)
#     if db is not None:
#         db.close()


@app.route('/')
def index():
    # Simple homepage or a form to input movie ID
    return "Welcome to Movie ABSA Backend. Use /get_absa/<movie_id> to get analysis."

@app.route('/get_absa/<string:movie_id>', methods=['GET'])
def get_movie_absa(movie_id):
    """
    API endpoint to get movie ABSA results.
    Checks DB cache first. Triggers background job if needed.
    """
    if not movie_id:
        return jsonify({"error": "Missing movie ID"}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        # 1. Check DB cache for processed results
        absa_results = get_absa_results(conn, movie_id)

        if absa_results:
            # Results found (Cache Hit)
            print(f"ABSA results found in cache for {movie_id}")
            return jsonify({"status": "completed", "movie_id": movie_id, "results": absa_results}), 200
        else:
            # Results not found (Cache Miss)
            print(f"No ABSA results found in cache for {movie_id}")
            # Check processing status
            status = check_movie_absa_status(conn, movie_id)

            if status == 'PROCESSING':
                # Job is already running
                print(f"Processing already underway for {movie_id}")
                return jsonify({"status": "processing", "message": "Analysis is currently being processed. Please check back later."}), 202 # Accepted

            elif status == 'COMPLETED':
                 # Status is completed, but no reviews were found/saved.
                 # This might happen if the crawler found no reviews or saving failed later.
                 print(f"Status is COMPLETED, but no reviews found for {movie_id}. Might indicate no reviews available or previous error.")
                 # Decide whether to re-trigger or just return "No reviews found".
                 # For simplicity, let's return No reviews found.
                 return jsonify({"status": "completed_no_reviews", "message": "No reviews found or processed for this movie."}), 200 # OK, but no data

            elif status == 'FAILED':
                # Previous job failed. Can decide to re-trigger automatically or require manual retry.
                print(f"Previous processing attempt failed for {movie_id}. Re-triggering.")
                # Re-trigger the job
                update_movie_absa_status(conn, movie_id, 'NOT_STARTED') # Reset status before triggering
                conn.commit() # Commit status update before closing conn
                process_movie_reviews.delay(movie_id) # Asynchronously trigger Celery task
                return jsonify({"status": "processing_restarted", "message": "Previous analysis failed. Retrying now. Please check back later."}), 202 # Accepted

            else: # status is None or 'NOT_STARTED' (or any other unhandled state)
                # Movie not in DB or status is NOT_STARTED
                print(f"Triggering new processing job for {movie_id}")
                # Update status to PROCESSING (create movie entry if needed)
                update_movie_absa_status(conn, movie_id, 'PROCESSING') # Handles insert/update
                conn.commit() # Commit status update before closing conn

                # Trigger the background task
                process_movie_reviews.delay(movie_id) # .delay() is a shortcut for .apply_async()

                return jsonify({"status": "processing_started", "message": "Analysis processing has started. Please check back later."}), 202 # Accepted

    except Exception as e:
        print(f"An error occurred in /get_absa/{movie_id}: {e}")
        conn.rollback() # Rollback any partial changes
        return jsonify({"error": "Internal server error"}), 500
    finally:
        # Close connection in the route handler if not using Flask's teardown
        if conn:
             conn.close()


# If you need other existing routes like /populate-matches or /recommend, include them here
# Make sure they are compatible with your template rendering and data sources.
# The sentiment analysis part in /recommend would now ideally call get_absa_results from DB
# instead of doing sentiment analysis on scraped data, or only do it for *new* reviews
# if the DB doesn't contain them yet (which would be complex).
# For the current flow, the assumption is sentiment is done *once* in the background job
# and results are served from the DB.

if __name__ == '__main__':
    # In production, you would run Flask using a WSGI server (like Gunicorn)
    # and run Celery workers separately.
    # For development:
    # 1. Run Redis server
    # 2. Run Celery worker: celery -A app.celery worker -l info (adjust app name if needed)
    # 3. Run Flask app: python app.py
    print("Starting Flask App. Ensure Redis and Celery worker are running.")
    app.run(debug=True)