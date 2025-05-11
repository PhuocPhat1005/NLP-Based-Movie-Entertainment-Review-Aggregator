# omdb_client.py
# Module tương tác với OMDb API
# Module này đã khá ổn, nó trả về dictionary chứa thông tin chi tiết phim,
# việc xử lý và lưu vào DB sẽ do orchestrator đảm nhận.

import requests
import logging
from typing import Dict, Optional, Any
from config import OMDB_API_KEY
import time

logger = logging.getLogger(__name__)
OMDB_BASE_URL = "http://www.omdbapi.com/"

def get_movie_details_from_omdb(title: Optional[str] = None, imdb_id: Optional[str] = None, year: Optional[str]=None) -> Optional[Dict[str, Any]]:
    """
    Lấy thông tin chi tiết của một phim từ OMDb, ưu tiên tìm theo IMDb ID nếu có.
    Thêm tham số year để tìm kiếm chính xác hơn khi chỉ có title.
    """
    if not OMDB_API_KEY or OMDB_API_KEY == "YOUR_OMDB_API_KEY":
        logger.error("OMDB_API_KEY chưa được cấu hình trong config.py.")
        return None

    if not title and not imdb_id:
        logger.warning("Cần cung cấp title hoặc imdb_id để tìm kiếm trên OMDb.")
        return None

    params = {'apikey': OMDB_API_KEY}
    search_param_log_parts = []

    if imdb_id:
        params['i'] = imdb_id
        search_param_log_parts.append(f"IMDb ID='{imdb_id}'")
    elif title:
        params['t'] = title
        search_param_log_parts.append(f"Title='{title}'")
        if year: # Nếu tìm theo title, có thêm year sẽ giúp kết quả chính xác hơn
            params['y'] = str(year) # OMDb API chấp nhận year là string
            search_param_log_parts.append(f"Year='{year}'")
    
    search_param_log = ", ".join(search_param_log_parts)

    try:
        logger.debug(f"Đang gọi OMDb API với: {search_param_log}")
        response = requests.get(OMDB_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("Response") == "True":
            logger.info(f"Tìm thấy thông tin phim trên OMDb cho [{search_param_log}]: {data.get('Title')} ({data.get('imdbID')})")
            return data
        else:
            logger.warning(f"OMDb API không tìm thấy phim cho [{search_param_log}]. Phản hồi: {data.get('Error', 'Lỗi không xác định')}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi gọi OMDb API cho [{search_param_log}]: {e}")
        return None
    except Exception as e:
        logger.error(f"Lỗi không mong muốn khi xử lý dữ liệu OMDb cho [{search_param_log}]: {e}")
        return None

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    print("\nTest tìm theo IMDb ID (tt0111161):")
    details_by_id = get_movie_details_from_omdb(imdb_id="tt0111161")
    if details_by_id: print(f"  Kết quả: {details_by_id.get('Title')}, {details_by_id.get('Year')}, Rating: {details_by_id.get('imdbRating')}")

    print("\nTest tìm theo Title (Inception) và Year (2010):")
    details_by_title_year = get_movie_details_from_omdb(title="Inception", year="2010")
    if details_by_title_year: print(f"  Kết quả: {details_by_title_year.get('imdbID')}, {details_by_title_year.get('Title')}, Rating: {details_by_title_year.get('imdbRating')}")

    print("\nTest tìm theo Title (Avatar) không có năm (có thể ra nhiều bản):")
    details_by_title_only = get_movie_details_from_omdb(title="Avatar")
    if details_by_title_only: print(f"  Kết quả: {details_by_title_only.get('imdbID')}, {details_by_title_only.get('Title')}, Year: {details_by_title_only.get('Year')}")
