# tmdb_client.py
# Module tương tác với The Movie Database (TMDB) API
# Hàm get_movies_by_year_from_tmdb cơ bản đã ổn để lấy danh sách phim tiềm năng.
# Không cần thay đổi lớn ở đây cho việc ghi trực tiếp vào DB,
# vì module này chỉ cung cấp danh sách phim ban đầu.

import requests
import logging
from typing import List, Dict, Any, Optional
from config import TMDB_API_KEY, TMDB_PAGES_TO_FETCH_PER_YEAR # Lấy số trang từ config
import time

logger = logging.getLogger(__name__)
TMDB_BASE_URL = "https://api.themoviedb.org/3"

def get_movies_by_year_from_tmdb(year: int, num_pages_to_fetch: int = TMDB_PAGES_TO_FETCH_PER_YEAR) -> List[Dict[str, Any]]:
    """
    Lấy danh sách các phim phổ biến từ TMDB cho một năm cụ thể.
    Trả về danh sách các dictionary, mỗi dict chứa 'tmdb_id', 'title', 'original_title', 'overview', 'release_date'.
    """
    if not TMDB_API_KEY or TMDB_API_KEY == "YOUR_TMDB_API_KEY_V3":
        logger.error("TMDB_API_KEY chưa được cấu hình trong config.py.")
        return []

    discovered_movies: List[Dict[str, Any]] = []
    # Sử dụng num_pages_to_fetch được truyền vào hoặc từ config
    actual_num_pages = num_pages_to_fetch if num_pages_to_fetch > 0 else TMDB_PAGES_TO_FETCH_PER_YEAR

    logger.info(f"Bắt đầu lấy danh sách phim từ TMDB cho năm {year}, số trang tối đa: {actual_num_pages}")

    for page in range(1, actual_num_pages + 1):
        params = {
            'api_key': TMDB_API_KEY,
            'primary_release_year': year,
            'language': 'en-US',
            'sort_by': 'popularity.desc',
            'page': page,
            'include_adult': 'false',
            'include_video': 'false'
        }
        try:
            response = requests.get(f"{TMDB_BASE_URL}/discover/movie", params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('results', [])
            if not results:
                logger.info(f"Không tìm thấy thêm phim nào ở trang {page} cho năm {year}.")
                break 

            for movie in results:
                if movie.get('id') and movie.get('title'):
                    discovered_movies.append({
                        "tmdb_id": movie.get('id'),
                        "title": movie.get('title'),
                        "original_title": movie.get('original_title'),
                        "overview": movie.get('overview'),
                        "release_date": movie.get('release_date')
                    })
            
            logger.info(f"  Trang {page}/{actual_num_pages}: Lấy được {len(results)} phim. Tổng số phim tiềm năng: {len(discovered_movies)}")
            
            if page < actual_num_pages:
                time.sleep(0.5) 

        except requests.exceptions.RequestException as e:
            logger.error(f"Lỗi khi gọi TMDB API cho năm {year}, trang {page}: {e}")
            break 
        except Exception as e:
            logger.error(f"Lỗi không mong muốn khi xử lý dữ liệu TMDB cho năm {year}, trang {page}: {e}")
            break
            
    logger.info(f"Hoàn tất lấy phim từ TMDB cho năm {year}. Tổng số phim tiềm năng: {len(discovered_movies)}")
    return discovered_movies

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    test_year = 2023
    # Lấy số trang mặc định từ config
    movies_2023 = get_movies_by_year_from_tmdb(test_year) 
    if movies_2023:
        print(f"\nTìm thấy {len(movies_2023)} phim cho năm {test_year} (sử dụng TMDB_PAGES_TO_FETCH_PER_YEAR từ config):")
        for i, movie in enumerate(movies_2023[:3]): # In 3 phim đầu
            print(f"  {i+1}. ID (TMDB): {movie.get('tmdb_id')}, Title: {movie.get('title')}")
    else:
        print(f"Không tìm thấy phim nào cho năm {test_year} hoặc có lỗi xảy ra.")
