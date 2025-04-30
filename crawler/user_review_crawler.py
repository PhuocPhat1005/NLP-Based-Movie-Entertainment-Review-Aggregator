import json
import time
from datetime import datetime
import requests
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from utils.logger import setup_logger # Assuming utils.logger exists
import os
import html
import logging # Using standard logging if setup_logger is complex/unavailable
import sys    # For stream handler

# --- Simple Logger Setup (replace or use your utils.logger) ---
def setup_logger(name, log_file, level=logging.INFO):
    # Ensure log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print(f"Error creating log directory {log_dir}: {e}")
            # Handle error appropriately, maybe exit or log to console only

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')

    # File Handler
    try:
        handler = logging.FileHandler(log_file, encoding='utf-8')
        handler.setFormatter(formatter)
    except IOError as e:
        print(f"Error setting up file handler for {log_file}: {e}")
        handler = None # Indicate failure

    # Stream Handler (to console)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    # Clear existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()
    if handler: # Only add file handler if successfully created
        logger.addHandler(handler)
    logger.addHandler(stream_handler) # Always add stream handler

    return logger
# --- End Simple Logger Setup ---


class UserReviewCrawler:
    def __init__(self):
        # Setup logger
        log_file = f'./logs/user_review_crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        self.logger = setup_logger('UserReviewCrawler', log_file)

        self.PAGE_SIZE = 25
        self.MAX_REVIEWS_PER_MOVIE = 50 # Limit remains 50
        self.base_url = "https://caching.graphql.imdb.com/"
        self.headers = {
            'accept': 'application/graphql+json, application/json',
            'accept-language': 'vi-VN,vi;q=0.9,en-GB;q=0.8,en;q=0.7,fr-FR;q=0.6,fr;q=0.5,en-US;q=0.4',
            'content-type': 'application/json',
            'origin': 'https://www.imdb.com',
            'referer': 'https://www.imdb.com/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36', # Consider updating Chrome version if needed
            'x-imdb-client-name': 'imdb-web-next',
            'x-imdb-user-country': 'VN',
            'x-imdb-user-language': 'vi-VN'
        }

        self.output_folder = './output'
        # Ensure output directory exists (but not input file)
        if not os.path.exists(self.output_folder):
            try:
                os.makedirs(self.output_folder)
                self.logger.info(f"Created output directory: {self.output_folder}")
            except OSError as e:
                 self.logger.error(f"Could not create output directory {self.output_folder}: {e}")
                 # Decide if script should exit if output dir cannot be created

        self.session = requests.Session()
        self._init_session()

    def _init_session(self):
        """Initialize session with browser automation"""
        try:
            self.logger.info("Setting up Chrome for session initialization...")
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox') # Often needed in Linux environments
            chrome_options.add_argument('--disable-dev-shm-usage') # Overcomes limited resource problems
            # Make sure chromedriver is accessible via PATH or specify its path explicitly
            # from selenium.webdriver.chrome.service import Service
            # service = Service('/path/to/your/chromedriver')
            # driver = webdriver.Chrome(service=service, options=chrome_options)
            driver = webdriver.Chrome(options=chrome_options) # Assumes chromedriver is in PATH
            self.logger.info("Chrome driver initialized")

            driver.get('https://www.imdb.com/')
            self.logger.info("Visiting IMDb homepage")

            # Increased sleep time might be necessary if network is slow or IMDb loads lazily
            time.sleep(7)

            cookies = driver.get_cookies()
            self.logger.info(f"Retrieved {len(cookies)} cookies")

            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])

            driver.quit()
            self.logger.info("Session initialization completed successfully")

        except Exception as e:
            # Log more specific error if possible (e.g., WebDriverException)
            self.logger.error(f"Error initializing Selenium session: {type(e).__name__} - {str(e)}")
            self.logger.warning("Proceeding without browser-based session cookies. Crawl might fail or be blocked.")
            # Consider exiting if session init is critical: raise SystemExit("Failed to initialize session")

    # --- get_movie_reviews, _fetch_reviews_page, _extract_review_data methods remain the same as the previous good version ---
    def get_movie_reviews(self, movie_id, movie_name, original_title):
        """Get up to MAX_REVIEWS_PER_MOVIE reviews for a specific movie"""
        after_token = ""
        has_next = True
        all_reviews = []
        page = 1

        self.logger.info(f"Starting to fetch reviews for movie {movie_name} ({movie_id}), limit: {self.MAX_REVIEWS_PER_MOVIE}")

        while has_next and len(all_reviews) < self.MAX_REVIEWS_PER_MOVIE:
            try:
                self.logger.debug(f"Fetching page {page} with after_token: {after_token} (Collected: {len(all_reviews)}/{self.MAX_REVIEWS_PER_MOVIE})") # Use debug level
                reviews_page = self._fetch_reviews_page(movie_id, after_token)

                if not reviews_page or 'data' not in reviews_page or not reviews_page['data'].get('title'):
                    error_detail = "No data or invalid structure"
                    if reviews_page and 'errors' in reviews_page:
                        error_detail = f"API errors: {reviews_page['errors']}"
                    elif reviews_page:
                         error_detail = f"Missing 'title' key in data. Keys present: {reviews_page['data'].keys() if 'data' in reviews_page else 'N/A'}"

                    self.logger.error(f"Failed to fetch page {page}. Detail: {error_detail}")
                    break

                reviews_data = reviews_page['data']['title'].get('reviews')
                if not reviews_data:
                    self.logger.warning(f"No 'reviews' key found in data for page {page}. Stopping for this movie.")
                    break

                edges = reviews_data.get('edges', [])
                if not edges and page == 1: # No reviews found at all
                     self.logger.info(f"No reviews found for movie {movie_name} ({movie_id}) on the first page.")
                     # break # Exit loop if no reviews found on first page

                reviews_processed_this_page = 0
                for edge in edges:
                    if len(all_reviews) >= self.MAX_REVIEWS_PER_MOVIE:
                        self.logger.info(f"Reached review limit ({self.MAX_REVIEWS_PER_MOVIE}) while processing page {page}. Stopping fetch for this movie.")
                        has_next = False
                        break

                    review = self._extract_review_data(edge, movie_id, movie_name, original_title)
                    if review:
                        all_reviews.append(review)
                        reviews_processed_this_page += 1
                
                if reviews_processed_this_page > 0: # Only log if reviews were actually processed
                    self.logger.debug(f"Processed {reviews_processed_this_page} reviews on page {page}. Total collected: {len(all_reviews)}")

                # Update pagination info ONLY if we haven't hit the limit and there are more pages
                if len(all_reviews) < self.MAX_REVIEWS_PER_MOVIE:
                    page_info = reviews_data.get('pageInfo', {})
                    has_next = page_info.get('hasNextPage', False)
                    after_token = page_info.get('endCursor', '')

                    if not has_next:
                        self.logger.info(f"Reached last available page of reviews for {movie_name}.")
                        break
                # else: has_next was set to False above if limit reached mid-page

                page += 1
                if has_next:
                    time.sleep(random.uniform(1.5, 3.0)) # Use random sleep time

            except requests.exceptions.RequestException as req_err:
                 self.logger.error(f"Network error fetching page {page} for {movie_id}: {req_err}")
                 time.sleep(5) # Wait longer after network error before potentially retrying or breaking
                 # Implement retry logic here if desired
                 break # Or just break on error
            except Exception as e:
                self.logger.exception(f"Unexpected error processing page {page} for {movie_id}: {str(e)}") # Use logger.exception to include traceback
                break # Exit while loop on unexpected error

        self.logger.info(f"Finished fetching for {movie_name}. Collected {len(all_reviews)} reviews.")
        return all_reviews

    def _fetch_reviews_page(self, movie_id, after_token=""):
        # This method remains largely the same, ensure timeout and error handling are robust
        variables = {
            "after": after_token,
            "const": movie_id,
            "filter": {},
            "first": self.PAGE_SIZE,
            "locale": "vi-VN",
            "sort": {
                "by": "HELPFULNESS_SCORE",
                "order": "DESC"
            }
        }
        extensions = {
            "persistedQuery": {
                "sha256Hash": "89aff4cd7503e060ff1dd5aba91885d8bac0f7a21aa1e1f781848a786a5bdc19",
                "version": 1
            }
        }
        encoded_variables = quote(json.dumps(variables))
        encoded_extensions = quote(json.dumps(extensions))
        url = f"{self.base_url}?operationName=TitleReviewsRefine&variables={encoded_variables}&extensions={encoded_extensions}"

        try:
            response = self.session.get(url, headers=self.headers, timeout=20) # Increased timeout
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.Timeout:
            self.logger.warning(f"API request timed out fetching reviews for {movie_id} (token: {after_token}).")
            return None
        except requests.exceptions.HTTPError as http_err:
             self.logger.error(f"HTTP error fetching reviews for {movie_id}: {http_err}. Response: {http_err.response.text[:200]}...")
             return None
        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"Network error fetching reviews for {movie_id}: {req_err}")
            return None
        except json.JSONDecodeError:
             # Log the response status and text snippet if JSON decoding fails
             status_code = response.status_code if 'response' in locals() else 'N/A'
             text_snippet = response.text[:200] if 'response' in locals() else 'N/A'
             self.logger.error(f"Failed to decode JSON response for {movie_id}. Status: {status_code}, Response Text: {text_snippet}...")
             return None


    def _extract_review_data(self, edge, movie_id, movie_name, original_title):
        # This method remains largely the same, ensure safe access with .get()
        try:
            node = edge.get('node', {})
            if not node:
                self.logger.warning("Encountered an edge with no node data.")
                return None

            raw_content = node.get('text', {}).get('originalText', {}).get('plaidHtml', '')
            raw_title = node.get('summary', {}).get('originalText', '')

            clean_content = html.unescape(raw_content or '')
            clean_content = clean_content.replace('<br/>', '\n').replace('<br>', '\n').strip() # Add strip()

            clean_title = html.unescape(raw_title or '').strip() # Add strip()

            review = {
                'review_id': node.get('id'),
                'movie_id': movie_id,
                'movie_name': movie_name,
                'original_title': original_title,
                'review_title': clean_title,
                'review_content': clean_content,
                'spoiler': node.get('spoiler', False),
                'rating': node.get('authorRating'),
                'like': node.get('helpfulness', {}).get('upVotes', 0),
                'dislike': node.get('helpfulness', {}).get('downVotes', 0),
                'reviewer_username': node.get('author', {}).get('nickName'),
                'submission_date': node.get('submissionDate'),
                'updated_at': datetime.now().isoformat()
            }

            if not review['review_id'] or not review['review_content']: # Basic check
                 self.logger.warning(f"Skipping review due to missing ID or content for movie {movie_id}.")
                 return None

            return review

        except Exception as e:
            self.logger.exception(f"Error extracting review data for movie {movie_id}: {str(e)}") # Use exception
            return None


    def crawl_movies_reviews(self, input_file, output_file):
        """Crawl reviews for all movies in the input file"""
        # --- Ensure input file exists BEFORE starting ---
        if not os.path.exists(input_file):
            self.logger.critical(f"CRITICAL: Input file not found: {input_file}. Please ensure the file exists and the path is correct. Exiting.")
            print(f"Lỗi: Không tìm thấy file input: {input_file}. Vui lòng kiểm tra lại đường dẫn.")
            return [] # Return empty list to indicate failure

        try:
            # Read movies from input file
            self.logger.info(f"Reading movies from: {input_file}")
            with open(input_file, 'r', encoding='utf-8') as f:
                try:
                    movies = json.load(f)
                    if not isinstance(movies, list):
                         self.logger.error(f"Input file {input_file} does not contain a valid JSON list. Content type: {type(movies)}")
                         return []
                    self.logger.info(f"Successfully loaded {len(movies)} movie entries from {input_file}")
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error decoding JSON from {input_file}: {e}. Please check the file format.")
                    return []
                except Exception as e: # Catch other potential file reading errors
                    self.logger.error(f"Error reading file {input_file}: {e}")
                    return []


            all_reviews_collected = [] # Store all collected reviews

            # Process each movie
            total_movies = len(movies)
            for index, movie in enumerate(movies):
                movie_id = movie.get('id')
                movie_name = movie.get('name', f'Unknown Name (Index {index})')
                original_title = movie.get('original_title', movie_name)

                if not movie_id:
                    self.logger.warning(f"Skipping movie at index {index} due to missing 'id'. Data: {movie}")
                    continue

                self.logger.info(f"\n--- Processing movie {index + 1}/{total_movies}: {movie_name} ({movie_id}) ---")

                # Get reviews for this movie (respecting the limit inside the method)
                movie_reviews = self.get_movie_reviews(movie_id, movie_name, original_title)
                if movie_reviews: # Only extend if reviews were found
                    all_reviews_collected.extend(movie_reviews)
                    self.logger.info(f"Added {len(movie_reviews)} reviews for {movie_name}. Total reviews now: {len(all_reviews_collected)}")
                else:
                    self.logger.info(f"No reviews were added for {movie_name}.")


                # Save progress incrementally after processing each movie
                self._save_reviews(all_reviews_collected, output_file)
                self.logger.debug(f"Saved progress ({len(all_reviews_collected)} reviews) to {output_file} after processing movie {index + 1}")

                # Optional: Add a slightly longer, randomized sleep between movies to be polite
                sleep_time = random.uniform(3.0, 6.0)
                self.logger.debug(f"Sleeping for {sleep_time:.1f} seconds before next movie...")
                time.sleep(sleep_time)

            self.logger.info(f"--- Finished processing all {total_movies} movies. ---")
            return all_reviews_collected

        except FileNotFoundError:
             # This case is handled by the check at the beginning, but kept for safety
             self.logger.critical(f"Input file not found during processing (should have been caught earlier): {input_file}")
             return []
        except Exception as e:
            self.logger.exception(f"Critical error during crawl_movies_reviews execution: {str(e)}")
            # Attempt to save partial results before exiting
            if 'all_reviews_collected' in locals() and all_reviews_collected:
                 error_output_file = output_file + ".error_partial"
                 self.logger.info(f"Attempting to save partial results ({len(all_reviews_collected)} reviews) to {error_output_file}")
                 self._save_reviews(all_reviews_collected, error_output_file)
            return [] # Return empty list or partial results depending on requirements

    def _save_reviews(self, reviews, output_file):
        """Save reviews to output file safely"""
        try:
            temp_output_file = output_file + ".tmp"
            with open(temp_output_file, 'w', encoding='utf-8') as f:
                json.dump(reviews, f, ensure_ascii=False, indent=4)
            os.replace(temp_output_file, output_file)
            # Log saving success less frequently, maybe only in crawl_movies_reviews after saving
        except IOError as e:
            self.logger.error(f"I/O error saving reviews to {output_file}: {str(e)}")
        except Exception as e:
             self.logger.exception(f"Unexpected error saving reviews to {output_file}: {str(e)}")


import random # Add random for sleep timing

def main():
    # Create required directories robustly
    for directory in ['./logs', './output']:
        try:
            os.makedirs(directory, exist_ok=True) # exist_ok=True prevents error if directory already exists
            print(f"Directory ensured: {directory}")
        except OSError as e:
             print(f"Error creating directory {directory}: {e}")
             return # Exit if essential directories cannot be made

    crawler = UserReviewCrawler()

    # Input and output files - CRITICAL: Ensure this path is correct!
    input_file = "output/filtered_movies.json"
    output_file = "output/movie_reviews_50.json" # Keeps the specific name

    # --- DUMMY FILE CREATION BLOCK REMOVED ---
    # The script now relies on 'output/filtered_movies.json' existing.

    print(f"Starting crawler. Reading movies from: {input_file}")
    print(f"Reviews will be limited to {crawler.MAX_REVIEWS_PER_MOVIE} per movie.")
    print(f"Output will be saved to: {output_file}")

    # Crawl reviews
    reviews = crawler.crawl_movies_reviews(input_file, output_file)

    # Final summary message
    if reviews:
        final_count = len(reviews)
        crawler.logger.info(f"\nCrawling completed. Final total reviews collected: {final_count}")
        print(f"\nCrawling completed successfully.")
        print(f"Total reviews collected: {final_count}")
        print(f"Final reviews saved to: {output_file}")
    else:
         # Check if the input file actually existed, as that's the most likely cause now
         if not os.path.exists(input_file):
              message = f"Crawling finished, but no reviews were collected. Reason: Input file '{input_file}' not found."
              crawler.logger.error(message)
              print(f"\n{message}")
         else:
              message = "Crawling finished, but no reviews were collected or an error occurred."
              crawler.logger.warning(f"\n{message} Please check the log file in './logs' for details.")
              print(f"\n{message} Check logs for potential errors.")


if __name__ == "__main__":
    main()