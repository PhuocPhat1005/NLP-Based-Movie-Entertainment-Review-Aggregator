# services/server/app/database.py

import os
import psycopg2
from psycopg2.extras import RealDictCursor # Để fetch kết quả dưới dạng dictionary
from .config import Config # Import class cấu hình
from datetime import date, datetime # Để làm việc với kiểu dữ liệu ngày giờ

# Hàm lấy kết nối database - Giữ nguyên như đã sửa lỗi
def get_db_connection():
    """Establishes a new database connection."""
    try:
        db_url = Config.DATABASE_URL
        # Giữ các dòng debug print nếu bạn vẫn cần chẩn đoán DSN
        print(f"DEBUG_DB: Value of os.environ.get('DATABASE_URL') inside get_db_connection: {os.environ.get('DATABASE_URL')}")
        print(f"DEBUG_DB: Value of Config.DATABASE_URL inside get_db_connection (just before connect): {db_url}")
        print(f"Attempting to connect to DB with DSN: {db_url}")
        conn = psycopg2.connect(db_url)
        print("Database connection successful!")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# Hàm helper map giá trị smallint sentiment từ DB sang string
# Giữ nguyên hàm này
def map_sentiment_smallint_to_string(sentiment_int):
    """Maps the smallint sentiment value from DB to a human-readable string."""
    if sentiment_int is None:
        return 'Unknown'
    # Giả định schema DB của bạn dùng 1 cho Positive, -1 cho Negative, 0 cho Neutral
    if sentiment_int > 0:
        return 'Positive'
    if sentiment_int < 0:
        return 'Negative'
    return 'Neutral' # Giá trị 0

# --- CÁC HÀM MỚI VÀ SỬA ĐỔI THEO FLOWCHART MỚI ---

def check_movie_exists(conn, movie_id):
    """
    Checks if a movie with the given movie_id exists in the movies table.
    Used in Step B of the flowchart.
    """
    if conn is None:
        print("No database connection to check movie existence.")
        # Quyết định cách xử lý lỗi DB khi check: trả về False để không trigger job
        return False
    try:
        with conn.cursor() as cur:
            # Chỉ cần kiểm tra sự tồn tại
            cur.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
            # fetchone() trả về None nếu không có dòng nào
            return cur.fetchone() is not None # Trả về True nếu tìm thấy phim, False nếu không

    except Exception as e:
        print(f"Error checking movie existence for {movie_id}: {e}")
        # Xử lý lỗi DB khi check, trả về False để coi như phim chưa có trong DB (hoặc lỗi check)
        return False

def check_absa_exists(conn, movie_id):
    """
    Checks if ABSA results exist for a movie in the review_aspects table.
    Used in Step C of the flowchart.
    """
    if conn is None:
        print(f"No database connection to check ABSA existence for {movie_id}.")
        return False
    try:
        with conn.cursor() as cur:
            # Kiểm tra xem có bất kỳ dòng nào trong review_aspects liên kết với movie_id này không.
            # Phải join qua reviews_clean vì review_aspects không có movie_id trực tiếp.
            cur.execute(
                """
                SELECT 1
                FROM review_aspects ra
                JOIN reviews_clean rc ON ra.review_id = rc.review_id
                WHERE rc.movie_id = %s
                LIMIT 1 -- Chỉ cần tìm thấy ít nhất 1 kết quả ABSA
                """,
                (movie_id,)
            )
            return cur.fetchone() is not None # Trả về True nếu tìm thấy kết quả ABSA, False nếu không

    except Exception as e:
        print(f"Error checking ABSA existence for {movie_id}: {e}")
        # Xử lý lỗi DB khi check, trả về False để coi như ABSA chưa có
        return False

# Hàm đã sửa đổi để cập nhật trạng thái phim và/hoặc thêm mới nếu chưa tồn tại
# Sử dụng các hằng số trạng thái từ Config
def update_movie_absa_status(conn, movie_id, status, title=None, year=None):
    """
    Updates the processing status of a movie in the movies table.
    Inserts a new movie entry if it does not exist.
    Used in Step D1, D2 (before triggering task) and within the Background job.
    """
    if conn is None:
        print(f"No database connection to update status for {movie_id}.")
        return False # Không thể cập nhật nếu không có kết nối DB

    # Tùy chọn: Thêm check để đảm bảo status là một trong các giá trị hợp lệ của bạn
    # if status not in [Config.STATUS_NOT_STARTED, Config.STATUS_PROCESSING_CRAWL_ABSA, Config.STATUS_COMPLETED_ABSA, ...]:
    #    print(f"Warning: Unknown status '{status}' for movie_id {movie_id}. Using it anyway.")

    try:
        with conn.cursor() as cur:
             # Kiểm tra xem phim đã tồn tại chưa bằng SELECT FOR UPDATE để tránh race condition (tùy chọn)
             # Tuy nhiên, với ON CONFLICT hoặc logic insert/update đơn giản là đủ cho nhiều trường hợp
            cur.execute("SELECT movie_id FROM movies WHERE movie_id = %s", (movie_id,))
            movie_entry = cur.fetchone()

            if movie_entry is None:
                 # Insert mới nếu chưa tồn tại. Set created_at là thời gian hiện tại.
                 # Cần đảm bảo cột 'imdb_rating' trong bảng 'movies' chấp nhận NULL
                 cur.execute(
                     "INSERT INTO movies (movie_id, title, year, processing_status, created_at) VALUES (%s, %s, %s, %s, %s)",
                     (movie_id, title, year, status, datetime.now())
                 )
                 print(f"Inserted new movie {movie_id} with initial status {status}.")
            else:
                # Update trạng thái cho phim đã tồn tại
                cur.execute("UPDATE movies SET processing_status = %s WHERE movie_id = %s", (status, movie_id))
                print(f"Updated status for movie {movie_id} to {status}.")

            conn.commit() # Commit thay đổi
            return True # Trả về True nếu thành công

    except Exception as e:
        print(f"Error updating movie ABSA status for {movie_id} to {status}: {e}")
        conn.rollback() # Rollback nếu có lỗi
        return False # Trả về False nếu thất bại

# Hàm mới để load review từ DB khi ABSA chưa có
# Được dùng trong Background job (luồng D2 -> F2)
def load_reviews_for_absa(conn, movie_id):
    """
    Loads reviews for a movie from DB (reviews_clean) that need ABSA processing.
    Used in the Background job (ABSA only path).
    Returns a list of dictionaries, each with 'review_id' and 'review_text'.
    """
    if conn is None:
        print(f"No database connection to load reviews for {movie_id}.")
        return [] # Trả về danh sách rỗng nếu không có kết nối

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Lấy review_id và review_text từ reviews_clean cho phim này
            # Đây là dữ liệu sẽ được gửi đến API ABSA.
            cur.execute(
                """
                SELECT review_id, review_text
                FROM reviews_clean
                WHERE movie_id = %s
                -- Tùy chọn: Có thể thêm logic phức tạp hơn ở đây
                -- để chỉ lấy các review_id chưa có kết quả trong review_aspects
                -- nhưng điều đó làm truy vấn phức tạp. Xử lý duplicates khi lưu sẽ dễ hơn.
                """,
                (movie_id,)
            )
            reviews_data = cur.fetchall()
            print(f"Loaded {len(reviews_data)} reviews for ABSA processing for movie {movie_id} from DB.")
            return reviews_data

    except Exception as e:
        print(f"Error loading reviews for ABSA for {movie_id} from DB: {e}")
        # Xử lý lỗi, trả về danh sách rỗng
        return []

# Hàm lưu review thô - Giữ nguyên như đã sửa đổi
# Được dùng trong Background job (luồng D1 -> F1 sau crawl)
def save_raw_reviews(conn, reviews_list):
    """Saves raw reviews to the reviews_raw table."""
    if conn is None or not reviews_list:
        print("No connection or empty review list for saving raw reviews.")
        return False
    try:
        with conn.cursor() as cur:
            # INSERT statement, đảm bảo tên cột và thứ tự khớp với bảng reviews_raw
            insert_query = """
                INSERT INTO reviews_raw (movie_id, review_id, reviewer_username, submission_date, rating, raw_json, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO NOTHING; -- Bỏ qua nếu review_id đã tồn tại (vì review_id là PK)
            """
            # Chuẩn bị dữ liệu theo đúng thứ tự cột
            data_to_insert = [
                (
                    r.get('movie_id'), r.get('review_id'), r.get('reviewer_username'),
                    r.get('submission_date'), r.get('rating'), r.get('raw_json'), datetime.now()
                ) for r in reviews_list if r.get('review_id') and r.get('movie_id') # Chỉ xử lý review có ID và movie_id
            ]
            # execute_values(cur, insert_query, data_to_insert) # Tùy chọn dùng execute_values
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            print(f"Attempted to save {len(data_to_insert)} raw reviews. Saved {cur.rowcount} new raw reviews.")
            return True
    except Exception as e:
        print(f"Error saving raw reviews: {e}")
        conn.rollback()
        return False

# Hàm lưu kết quả ABSA - Giữ nguyên cấu trúc, đảm bảo schema khớp review_aspects
# Được dùng trong Background job (sau bước H)
def save_absa_results(conn, absa_results_list):
    """
    Saves processed ABSA results to the review_aspects table.
    Used in the Background job (after running ABSA model).
    """
    if conn is None or not absa_results_list:
        print("No connection or empty ABSA results list for saving.")
        return False
    try:
        with conn.cursor() as cur:
             # INSERT statement, đảm bảo tên cột và thứ tự khớp với bảng review_aspects
             # Đảm bảo dữ liệu đầu vào có review_id, aspect, sentiment_int, confidence
            insert_query = """
                INSERT INTO review_aspects (review_id, aspect, sentiment, confidence)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (review_id, aspect) DO UPDATE SET sentiment = EXCLUDED.sentiment, confidence = EXCLUDED.confidence; -- Cập nhật nếu đã có kết quả cho cặp (review_id, aspect) này
            """
            # Chuẩn bị dữ liệu theo đúng thứ tự cột
            # Giả định absa_results_list là list các dict: {'review_id': '...', 'aspect': '...', 'sentiment_int': 1 or -1 or 0, 'confidence': 0.9}
            data_to_insert = [
                (
                    ar.get('review_id'), ar.get('aspect'), ar.get('sentiment_int'), ar.get('confidence')
                ) for ar in absa_results_list if ar.get('review_id') and ar.get('aspect') is not None and ar.get('sentiment_int') is not None
            ]
            # execute_values(cur, insert_query, data_to_insert) # Tùy chọn dùng execute_values
            cur.executemany(insert_query, data_to_insert)
            conn.commit()
            print(f"Attempted to save {len(data_to_insert)} ABSA results. Saved {cur.rowcount} new/updated results.")
            return True
    except Exception as e:
        print(f"Error saving ABSA results: {e}")
        conn.rollback()
        # Xử lý lỗi lưu: có thể cập nhật trạng thái phim thành FAILED ở đây hoặc trong task
        return False

# Hàm lấy kết quả ABSA cho client hiển thị - Giữ nguyên cấu trúc đã sửa đổi, thêm reviewer info
# Được dùng trong Flask endpoint (luồng C --> C1)
def get_absa_results(conn, movie_id):
    """
    Fetches processed ABSA results for a movie from DB (review_aspects + reviews_clean + reviews_raw).
    Used in the Flask endpoint (completed_absa path).
    Returns a list of dictionaries formatted for display by the client.
    """
    if conn is None:
        print(f"No database connection to fetch ABSA results for {movie_id}.")
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Join review_aspects (ra) với reviews_clean (rc) trên review_id
            # Join tiếp với reviews_raw (rr) trên review_id để lấy username và submission_date
            cur.execute(
                """
                SELECT
                    ra.aspect,
                    ra.sentiment AS sentiment_int, -- Lấy giá trị int
                    ra.confidence,
                    rc.review_id,
                    rc.review_text, -- Lấy nội dung review đã làm sạch
                    rc.rating, -- Rating từ reviews_clean
                    rc.like_count,
                    rc.dislike_count,
                    rr.reviewer_username, -- Username từ reviews_raw
                    rr.submission_date -- Ngày gửi từ reviews_raw
                FROM review_aspects ra
                JOIN reviews_clean rc ON ra.review_id = rc.review_id
                LEFT JOIN reviews_raw rr ON rc.review_id = rr.review_id -- Dùng LEFT JOIN đề phòng reviews_raw thiếu (ít khả năng)
                WHERE rc.movie_id = %s
                ORDER BY rc.like_count - rc.dislike_count DESC NULLS LAST, ra.review_id, ra.aspect -- Sắp xếp theo hữu ích, rồi review_id, rồi aspect
                """,
                (movie_id,)
            )
            results = cur.fetchall()

            # Map sentiment integer sang string cho kết quả trả về
            for row in results:
                row['sentiment'] = map_sentiment_smallint_to_string(row.get('sentiment_int'))
                # Tùy chọn: xóa cột sentiment_int nếu client không cần
                # row.pop('sentiment_int', None)

            print(f"Fetched {len(results)} ABSA result rows for movie {movie_id}.")
            return results

    except Exception as e:
        print(f"Error fetching ABSA results for {movie_id}: {e}")
        # Xử lý lỗi fetch, trả về danh sách rỗng
        return []