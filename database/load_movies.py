import psycopg2
from psycopg2.extras import execute_values
import glob
import csv
import os
import json
import re
from typing import Dict, Any, List, Tuple, Set

# --- Cấu hình ---
DB_PARAMS = {
    "host": "localhost",
    "port": "5432",
    "database": "movie_reviews_dw",
    "user": "postgres",
    "password": "postgre2025" 
}

CSV_INPUT_DIR = "data_csv"

# Các trường có trong file CSV của bạn
CSV_FIELDNAMES_FROM_FILE = [
    "review_id", "movie_id", "review_text", "text_len",
    "rating", "like_count", "dislike_count", "submission_date",
    "reviewer_username", "review_title", "spoiler",
    "movie_name", "original_title"
]

def get_db_connection():
    """Thiết lập kết nối đến database PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Kết nối PostgreSQL thành công!")
        return conn
    except psycopg2.Error as e:
        print(f"Lỗi: Không thể kết nối đến PostgreSQL. {e}")
        raise

def load_movies_data(conn, csv_files: List[str]):
    """
    Đọc tất cả các file CSV để trích xuất thông tin phim duy nhất
    và nạp vào bảng 'movies'.
    """
    print("\n--- Bắt đầu nạp dữ liệu cho bảng 'movies' ---")
    cursor = conn.cursor()
    unique_movies: Set[Tuple[str, str]] = set()
    movies_to_insert: List[Tuple[str, str, Any, Any]] = []

    print(f"Đang quét {len(csv_files)} file CSV để lấy thông tin phim...")
    for csv_filepath in csv_files:
        try:
            with open(csv_filepath, 'r', encoding='utf-8') as f_in:
                reader = csv.DictReader(f_in)
                for row in reader:
                    movie_id = row.get("movie_id")
                    title = row.get("original_title") if row.get("original_title") else row.get("movie_name")
                    if movie_id and title:
                        unique_movies.add((movie_id, title))
        except Exception as e:
            print(f"Lỗi khi đọc file {csv_filepath} cho bảng movies: {e}")
            continue
            
    if not unique_movies:
        print("Không tìm thấy thông tin phim nào để nạp vào bảng 'movies'.")
        cursor.close()
        return

    for movie_id, title in unique_movies:
        movies_to_insert.append((movie_id, title, None, None)) # year=None, imdb_rating=None

    insert_query = """
    INSERT INTO movies (movie_id, title, year, imdb_rating)
    VALUES %s
    ON CONFLICT (movie_id) DO UPDATE SET title = EXCLUDED.title;
    """
    try:
        execute_values(cursor, insert_query, movies_to_insert, page_size=500)
        conn.commit()
        inserted_count = cursor.rowcount 
        print(f"Đã nạp/cập nhật thành công {inserted_count if inserted_count is not None else 'không xác định (kiểm tra DB)'} dòng vào bảng 'movies'.")
        print(f"(Tổng số phim duy nhất tìm thấy: {len(unique_movies)})")
    except psycopg2.Error as e:
        print(f"Lỗi khi nạp dữ liệu vào bảng 'movies': {e}")
        conn.rollback()
    finally:
        cursor.close()

def load_reviews_raw_data(conn, csv_files: List[str]):
    """Nạp dữ liệu từ các file CSV vào bảng 'reviews_raw'."""
    print("\n--- Bắt đầu nạp dữ liệu cho bảng 'reviews_raw' ---")
    cursor = conn.cursor()
    total_rows_processed_csv = 0
    grand_total_rows_affected_db = 0

    for csv_filepath in csv_files:
        print(f"  Đang xử lý file cho reviews_raw: {os.path.basename(csv_filepath)}")
        data_to_load_tuples = []
        current_file_rows_read_from_csv = 0
        
        # Sử dụng set để theo dõi review_id đã xử lý trong batch hiện tại của file này
        processed_review_ids_in_batch = set() 
        
        try:
            with open(csv_filepath, 'r', encoding='utf-8') as f_in:
                reader = csv.DictReader(f_in)
                for i, row_dict in enumerate(reader):
                    total_rows_processed_csv +=1
                    current_file_rows_read_from_csv +=1
                    
                    review_id = row_dict.get("review_id")
                    if not review_id:
                        print(f"    Cảnh báo: Bỏ qua dòng {i+1} file {os.path.basename(csv_filepath)} do thiếu review_id.")
                        continue

                    # Chỉ thêm vào batch nếu review_id này chưa được xử lý trong batch hiện tại
                    if review_id in processed_review_ids_in_batch:
                        # print(f"    DEBUG: Bỏ qua review_id trùng lặp '{review_id}' trong cùng batch của file {os.path.basename(csv_filepath)}.")
                        continue
                    processed_review_ids_in_batch.add(review_id)

                    raw_json_content = {key: row_dict.get(key) for key in CSV_FIELDNAMES_FROM_FILE}
                    try:
                        if raw_json_content.get("rating"): raw_json_content["rating"] = float(raw_json_content["rating"])
                        if raw_json_content.get("like_count"): raw_json_content["like_count"] = int(raw_json_content["like_count"])
                        if raw_json_content.get("dislike_count"): raw_json_content["dislike_count"] = int(raw_json_content["dislike_count"])
                        if raw_json_content.get("spoiler"): raw_json_content["spoiler"] = str(raw_json_content["spoiler"]).lower() == 'true'
                    except (ValueError, TypeError) as e:
                        print(f"    Cảnh báo: Lỗi chuyển đổi kiểu dữ liệu cho raw_json_content dòng {i+1} file {os.path.basename(csv_filepath)}: {e}. Một số trường có thể là None.")

                    data_to_load_tuples.append((
                        review_id,
                        row_dict.get("movie_id"),
                        row_dict.get("reviewer_username"),
                        row_dict.get("submission_date") if row_dict.get("submission_date") else None,
                        float(row_dict["rating"]) if row_dict.get("rating") else None,
                        json.dumps(raw_json_content)
                    ))
                
                if data_to_load_tuples:
                    # Sử dụng ON CONFLICT DO NOTHING để bỏ qua nếu review_id đã tồn tại trong DB
                    insert_query_raw = """
                    INSERT INTO reviews_raw (review_id, movie_id, reviewer_username, submission_date, rating, raw_json)
                    VALUES %s
                    ON CONFLICT (review_id) DO NOTHING; 
                    """
                    execute_values(cursor, insert_query_raw, data_to_load_tuples, page_size=1000)
                    current_file_affected_db_count = cursor.rowcount if cursor.rowcount is not None else 0
                    grand_total_rows_affected_db += current_file_affected_db_count
                    conn.commit()
                    print(f"    -> Đã xử lý {current_file_rows_read_from_csv} dòng CSV. Nạp/bỏ qua {current_file_affected_db_count} dòng vào 'reviews_raw' từ {os.path.basename(csv_filepath)}.")
                else:
                    print(f"    Không có dữ liệu hợp lệ để nạp từ file {os.path.basename(csv_filepath)}.")

        except FileNotFoundError:
            print(f"Lỗi: Không tìm thấy file {csv_filepath}")
        except psycopg2.Error as e:
            print(f"Lỗi database khi xử lý file {csv_filepath} cho 'reviews_raw': {e}")
            conn.rollback()
        except Exception as e:
            print(f"Lỗi không mong muốn khi xử lý file {csv_filepath} cho 'reviews_raw': {e}")
            conn.rollback()
    
    print(f"Hoàn tất nạp 'reviews_raw'. Tổng review đã đọc từ CSV: {total_rows_processed_csv}. Tổng review đã ảnh hưởng (nạp/bỏ qua) trong DB: {grand_total_rows_affected_db}.")
    try:
        cursor.execute("SELECT COUNT(*) FROM reviews_raw;")
        actual_count_in_db = cursor.fetchone()[0]
        print(f"Số dòng thực tế trong bảng 'reviews_raw' sau khi nạp: {actual_count_in_db}")
    except Exception as e:
        print(f"Không thể kiểm tra số dòng trong 'reviews_raw': {e}")
    finally:
        cursor.close()

def load_reviews_clean_data(conn, csv_files: List[str]):
    """Nạp dữ liệu từ các file CSV vào bảng 'reviews_clean' bằng execute_values."""
    print("\n--- Bắt đầu nạp dữ liệu cho bảng 'reviews_clean' ---")
    cursor = conn.cursor()
    total_rows_processed_csv = 0
    grand_total_rows_affected_db = 0

    # Các cột trong bảng reviews_clean:
    # review_id, movie_id, review_text, text_len, rating, like_count, dislike_count
    
    for csv_filepath in csv_files:
        print(f"  Đang xử lý file cho reviews_clean: {os.path.basename(csv_filepath)}")
        data_to_load_tuples = []
        current_file_rows_read_from_csv = 0
        processed_review_ids_in_batch = set() # Để tránh trùng lặp review_id trong cùng batch

        try:
            with open(csv_filepath, 'r', encoding='utf-8') as f_in:
                reader = csv.DictReader(f_in)
                for i, row_dict in enumerate(reader):
                    total_rows_processed_csv += 1
                    current_file_rows_read_from_csv += 1

                    review_id = row_dict.get("review_id")
                    if not review_id:
                        print(f"    Cảnh báo: Bỏ qua dòng {i+1} file {os.path.basename(csv_filepath)} cho reviews_clean do thiếu review_id.")
                        continue
                    
                    if review_id in processed_review_ids_in_batch:
                        continue
                    processed_review_ids_in_batch.add(review_id)

                    try:
                        text_len_val = int(row_dict["text_len"]) if row_dict.get("text_len") else None
                        rating_val = float(row_dict["rating"]) if row_dict.get("rating") else None
                        like_val = int(row_dict["like_count"]) if row_dict.get("like_count") else None
                        dislike_val = int(row_dict["dislike_count"]) if row_dict.get("dislike_count") else None
                    except (ValueError, TypeError) as e:
                        print(f"    Cảnh báo: Lỗi chuyển đổi kiểu dữ liệu dòng {i+1} file {os.path.basename(csv_filepath)} cho reviews_clean: {e}. Dòng sẽ bị bỏ qua cho batch này.")
                        continue # Bỏ qua dòng này nếu có lỗi chuyển đổi kiểu dữ liệu cơ bản

                    data_to_load_tuples.append((
                        review_id,
                        row_dict.get("movie_id"),
                        row_dict.get("review_text"),
                        text_len_val,
                        rating_val,
                        like_val,
                        dislike_val
                    ))
                
                if data_to_load_tuples:
                    insert_query_clean = """
                    INSERT INTO reviews_clean (review_id, movie_id, review_text, text_len, rating, like_count, dislike_count)
                    VALUES %s
                    ON CONFLICT (review_id) DO NOTHING; 
                    """
                    # Nếu muốn cập nhật:
                    # ON CONFLICT (review_id) DO UPDATE SET
                    #   movie_id = EXCLUDED.movie_id,
                    #   review_text = EXCLUDED.review_text,
                    #   text_len = EXCLUDED.text_len,
                    #   rating = EXCLUDED.rating,
                    #   like_count = EXCLUDED.like_count,
                    #   dislike_count = EXCLUDED.dislike_count;
                    
                    execute_values(cursor, insert_query_clean, data_to_load_tuples, page_size=1000)
                    current_file_affected_db_count = cursor.rowcount if cursor.rowcount is not None else 0
                    grand_total_rows_affected_db += current_file_affected_db_count
                    conn.commit()
                    print(f"    -> Đã xử lý {current_file_rows_read_from_csv} dòng CSV. Nạp/bỏ qua {current_file_affected_db_count} dòng vào 'reviews_clean' từ {os.path.basename(csv_filepath)}.")
                else:
                    print(f"    Không có dữ liệu hợp lệ để nạp từ file {os.path.basename(csv_filepath)}.")

        except FileNotFoundError:
            print(f"Lỗi: Không tìm thấy file {csv_filepath}")
        except psycopg2.Error as e:
            print(f"Lỗi database khi xử lý file {csv_filepath} cho 'reviews_clean': {e}")
            conn.rollback()
        except Exception as e:
            print(f"Lỗi không mong muốn khi xử lý file {csv_filepath} cho 'reviews_clean': {e}")
            conn.rollback()
            
    print("Hoàn tất nạp 'reviews_clean'.")
    try:
        cursor.execute("SELECT COUNT(*) FROM reviews_clean;")
        actual_count_in_db = cursor.fetchone()[0]
        print(f"Số dòng thực tế trong bảng 'reviews_clean' sau khi nạp: {actual_count_in_db}")
    except Exception as e:
        print(f"Không thể kiểm tra số dòng trong 'reviews_clean': {e}")
    finally:
        cursor.close()

def main():
    conn = None
    try:
        conn = get_db_connection()
        csv_file_pattern = os.path.join(CSV_INPUT_DIR, "reviews_part_*_processed.csv")
        all_csv_files = sorted(glob.glob(csv_file_pattern))

        if not all_csv_files:
            print(f"Không tìm thấy file CSV nào khớp với pattern '{csv_file_pattern}' trong thư mục '{CSV_INPUT_DIR}'.")
            return

        print(f"Tìm thấy {len(all_csv_files)} file CSV để xử lý.")
        
        load_movies_data(conn, all_csv_files)
        load_reviews_raw_data(conn, all_csv_files) 
        load_reviews_clean_data(conn, all_csv_files)

        print("\n--- Quá trình nạp dữ liệu hoàn tất! ---")
        print("Các bước tiếp theo có thể là:")
        print("1. Chạy model ABSA để phân tích 'review_text' từ bảng 'reviews_clean'.")
        print("2. Nạp kết quả ABSA vào bảng 'review_aspects'.")
        print("3. Refresh Materialized View: REFRESH MATERIALIZED VIEW mv_movie_sentiment;")

    except psycopg2.Error as e:
        print(f"Lỗi PostgreSQL tổng thể: {e}")
    except Exception as e:
        print(f"Lỗi không mong muốn trong quá trình main: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\nĐã đóng kết nối PostgreSQL.")

if __name__ == "__main__":
    if not os.path.exists(CSV_INPUT_DIR):
        os.makedirs(CSV_INPUT_DIR)
        print(f"Đã tạo thư mục CSV input giả lập: {CSV_INPUT_DIR}")
    
    sample_csv_path = os.path.join(CSV_INPUT_DIR, "reviews_part_000_sample.csv")
    if not glob.glob(os.path.join(CSV_INPUT_DIR, "reviews_part_*.csv")):
        # Cập nhật file sample để khớp với CSV_FIELDNAMES_FROM_FILE
        with open(sample_csv_path, 'w', newline='', encoding='utf-8') as sf:
            writer = csv.DictWriter(sf, fieldnames=CSV_FIELDNAMES_FROM_FILE, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerow({
                "review_id": "rw_sample_001", "movie_id": "tt_sample_001", 
                "review_text": "This is a sample review text, with a comma and a \"quote\".\nIt also has a newline.", 
                "text_len": 70, # Cần tính toán đúng hoặc đảm bảo có trong CSV
                "rating": "8.0", # Để là string để DictReader đọc, sau đó sẽ convert
                "like_count": "10", 
                "dislike_count": "1", 
                "submission_date": "2024-01-15", 
                "reviewer_username": "testuser",
                "review_title": "Sample Title with, comma", 
                "spoiler": "False",
                "movie_name": "Sample Movie", 
                "original_title": "Sample Original Movie"
            })
        print(f"Đã tạo file CSV mẫu (sử dụng QUOTE_ALL): {sample_csv_path}")
            
    main()
