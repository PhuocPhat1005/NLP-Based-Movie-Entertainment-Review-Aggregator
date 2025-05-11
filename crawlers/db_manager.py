# db_manager.py
# Module quản lý kết nối và thao tác với database PostgreSQL

import psycopg2
from psycopg2.extras import DictCursor # Sử dụng DictCursor để dễ truy cập cột bằng tên
from typing import List, Dict, Any, Optional, Tuple
import logging
from datetime import datetime # Thêm datetime để xử lý updated_at
from config import DB_PARAMS # Import cấu hình DB

logger = logging.getLogger(__name__)

def get_db_connection():
    """Thiết lập và trả về một kết nối đến database PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info("Kết nối PostgreSQL thành công!")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Lỗi: Không thể kết nối đến PostgreSQL. {e}")
        raise # Ném lại lỗi để orchestrator có thể xử lý

def check_movie_exists(conn, movie_id: str) -> Optional[Dict[str, Any]]:
    """
    Kiểm tra xem một phim đã tồn tại trong bảng 'movies' chưa
    và trả về thông tin cơ bản (bao gồm updated_at) nếu có.
    """
    # Thêm updated_at vào câu SELECT
    query = "SELECT movie_id, title, year, imdb_rating, created_at, updated_at FROM movies WHERE movie_id = %s;"
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur: # Sử dụng DictCursor
            cur.execute(query, (movie_id,))
            movie_data = cur.fetchone()
            return dict(movie_data) if movie_data else None
    except psycopg2.Error as e:
        logger.error(f"Lỗi khi kiểm tra phim {movie_id}: {e}")
        return None

def save_movie_data(conn, movie_data: Dict[str, Any]) -> bool:
    """
    Lưu hoặc cập nhật thông tin phim vào bảng 'movies'.
    Sử dụng ON CONFLICT để xử lý cả INSERT và UPDATE.
    Cập nhật trường 'updated_at' mỗi khi có thay đổi hoặc insert.
    """
    if not movie_data.get("movie_id") or not movie_data.get("title"):
        logger.warning(f"Bỏ qua lưu phim do thiếu movie_id hoặc title: {movie_data.get('movie_id')}")
        return False

    # Chuẩn bị các giá trị, đảm bảo chúng là None nếu không có hoặc không hợp lệ
    movie_id_val = movie_data.get("movie_id")
    title_val = movie_data.get("title")
    
    year_val = movie_data.get("year")
    if isinstance(year_val, str): # Xử lý trường hợp year là chuỗi số
        try: year_val = int(year_val)
        except ValueError: year_val = None
    elif not isinstance(year_val, (int, type(None))):
        year_val = None

    imdb_rating_val = movie_data.get("imdb_rating")
    if isinstance(imdb_rating_val, str): # Xử lý trường hợp rating là chuỗi số
        try: imdb_rating_val = float(imdb_rating_val)
        except ValueError: imdb_rating_val = None
    elif not isinstance(imdb_rating_val, (float, int, type(None))):
        imdb_rating_val = None

    # Câu lệnh INSERT ... ON CONFLICT DO UPDATE
    # Luôn cập nhật 'updated_at' khi có INSERT hoặc UPDATE.
    # Sử dụng COALESCE để chỉ cập nhật nếu giá trị mới (EXCLUDED) không phải là NULL,
    # giữ lại giá trị cũ trong bảng (movies.column) nếu giá trị mới là NULL.
    # Điều này hữu ích nếu OMDb không trả về một số trường mà bạn đã có từ nguồn khác.
    query = """
    INSERT INTO movies (movie_id, title, year, imdb_rating, created_at, updated_at)
    VALUES (%s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (movie_id) DO UPDATE SET
        title = EXCLUDED.title,
        year = COALESCE(EXCLUDED.year, movies.year),
        imdb_rating = COALESCE(EXCLUDED.imdb_rating, movies.imdb_rating),
        updated_at = NOW()
    RETURNING movie_id, title; -- Trả về movie_id và title để xác nhận
    """
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, (
                movie_id_val,
                title_val,
                year_val,
                imdb_rating_val
            ))
            result = cur.fetchone()
            conn.commit()
            if result:
                logger.info(f"Đã lưu/cập nhật thành công phim: {result['movie_id']} - {result['title']}")
                return True
            else:
                # Trường hợp này ít xảy ra với RETURNING, trừ khi có lỗi không mong muốn
                logger.warning(f"Không có kết quả trả về sau khi lưu/cập nhật phim {movie_id_val}, kiểm tra lại logic.")
                return False # Hoặc True nếu bạn coi ON CONFLICT DO NOTHING (nếu dùng) là thành công
    except psycopg2.Error as e:
        logger.error(f"Lỗi database khi lưu phim {movie_id_val}: {e}")
        conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Lỗi không mong muốn khi lưu phim {movie_id_val}: {e}")
        conn.rollback()
        return False

if __name__ == '__main__':
    # Ví dụ cách sử dụng (chỉ để test module này)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    sample_movie_new = {
        "movie_id": "tt0000002", # ID mới để test INSERT
        "title": "Test Movie For Direct DB Save",
        "year": 2025,
        "imdb_rating": 7.8
    }
    sample_movie_update_existing = { # Giả sử tt0000001 đã có từ lần test trước
        "movie_id": "tt0000001", 
        "title": "Test Movie Updated Title Again",
        "year": "2027", # Test year là string
        "imdb_rating": "8.1" # Test rating là string
    }
    sample_movie_update_with_nulls = {
        "movie_id": "tt0000001", 
        "title": "Test Movie Title With Null Update",
        "year": None, # Giả sử OMDb không trả về năm
        "imdb_rating": None # Giả sử OMDb không trả về rating
    }


    connection = None
    try:
        connection = get_db_connection()
        if connection:
            print(f"\nĐang thử lưu phim mới: {sample_movie_new['movie_id']}")
            save_movie_data(connection, sample_movie_new)
            
            retrieved_new = check_movie_exists(connection, sample_movie_new['movie_id'])
            if retrieved_new: print(f"  Đã tìm thấy sau khi lưu: {retrieved_new}")

            print(f"\nĐang thử cập nhật phim đã có: {sample_movie_update_existing['movie_id']}")
            save_movie_data(connection, sample_movie_update_existing)

            retrieved_updated = check_movie_exists(connection, sample_movie_update_existing['movie_id'])
            if retrieved_updated: print(f"  Đã tìm thấy sau khi cập nhật: {retrieved_updated}")

            print(f"\nĐang thử cập nhật phim với giá trị NULL (để giữ giá trị cũ): {sample_movie_update_with_nulls['movie_id']}")
            save_movie_data(connection, sample_movie_update_with_nulls)
            
            retrieved_after_null_update = check_movie_exists(connection, sample_movie_update_with_nulls['movie_id'])
            if retrieved_after_null_update: print(f"  Đã tìm thấy sau khi cập nhật với NULL: {retrieved_after_null_update}")


    except Exception as e:
        print(f"Lỗi trong quá trình test db_manager: {e}")
    finally:
        if connection:
            connection.close()
            print("\nĐã đóng kết nối database.")
