# services/server/app/routes.py

from flask import jsonify, request
import json # Đảm bảo import json nếu bạn dùng nó trong hàm (ví dụ: khi xử lý lỗi)
# Import instance ứng dụng Flask từ __init__.py
# Đảm bảo dòng này đúng với cách bạn import app trong __init__.py
from . import app # Ví dụ: from services.server.app import app nếu cần import tuyệt đối

# Import các hàm tương tác database mới và đã sửa đổi
from .database import (
    get_db_connection, # Hàm kết nối DB
    check_movie_exists, # Hàm mới: kiểm tra phim có trong movies chưa
    check_absa_exists, # Hàm mới: kiểm tra ABSA có trong review_aspects chưa
    update_movie_absa_status, # Hàm đã sửa: cập nhật status trong movies
    get_absa_results # Hàm đã sửa: lấy kết quả ABSA cho client
)
# Import Celery task
from .tasks import process_movie_reviews

# Import class Config để sử dụng các hằng số trạng thái
from .config import Config # Đảm bảo import Config


# Định nghĩa route cho endpoint lấy kết quả ABSA
# @app.route('/get_absa/<string:movie_id>', methods=['GET']) # Giữ nguyên định nghĩa route này
@app.route('/get_absa/<string:movie_id>', methods=['GET'])
def get_movie_absa(movie_id):
    """
    API endpoint để lấy kết quả ABSA cho một phim.
    Triển khai logic 3 nhánh của flowchart:
    - Trả kết quả nếu đã có ABSA.
    - Trigger job Crawl+ABSA nếu phim chưa có trong DB.
    - Trigger job ABSA Only nếu phim có trong DB nhưng chưa có ABSA.
    Returns ABSA results (200), processing status (202), or errors (400, 500).
    """
    # Basic validation
    if not movie_id:
        return jsonify({"error": "Missing movie ID"}), 400

    # Get database connection
    conn = get_db_connection()
    if not conn:
        # Trả về 500 nếu không kết nối được DB ngay lúc này
        return jsonify({"error": "Database connection failed"}), 500

    try:
        # --- BẮT ĐẦU TRIỂN KHAI LOGIC FLOWCHART TRONG ROUTE ---

        # Step B: Check if movie exists in DB (movies table)
        movie_exists = check_movie_exists(conn, movie_id)
        print(f"Route Logic: Movie {movie_id} exists check returned: {movie_exists}") # Debug log

        if not movie_exists:
            # Path B -- Không: Phim chưa có trong DB
            print(f"Route Logic: Movie {movie_id} not found in DB. Proceeding to trigger Crawl + ABSA.")

            # Step D1: Trigger job: Crawl + ABSA + Save
            # Cập nhật trạng thái phim thành BẮT ĐẦU XỬ LÝ (crawl+absa).
            # Hàm update_movie_absa_status sẽ tự động thêm entry phim nếu nó chưa tồn tại.
            # Title và year có thể lấy từ nguồn khác (ví dụ TMDB API) trước khi trigger nếu cần lưu ngay.
            # Tạm thời chỉ dùng movie_id, task sẽ có thể fetch info sau nếu cần.
            update_movie_absa_status(conn, movie_id, Config.STATUS_PROCESSING_CRAWL_ABSA) # Set trạng thái
            conn.commit() # Commit thay đổi trạng thái trước khi trigger task

            # Trigger Celery task: Loại trigger 'crawl_absa'
            process_movie_reviews.delay(movie_id, trigger_type='crawl_absa') # Pass movie_id và trigger_type

            # Step E: Thông báo user: "Hệ thống đang xử lý..."
            return jsonify({
                "status": Config.STATUS_PROCESSING_CRAWL_ABSA, # Trạng thái hiện tại
                "movie_id": movie_id,
                "message": "Movie not found in DB. Crawling and analysis starting. Please check back later."
            }), 202 # HTTP 202 Accepted

        else:
            # Path B -- Có: Phim đã có trong DB (movies table)
            print(f"Route Logic: Movie {movie_id} found in DB. Proceeding to check for ABSA results.")

            # Step C: Check if movie has ABSA results (in review_aspects table)
            absa_exists = check_absa_exists(conn, movie_id)
            print(f"Route Logic: ABSA exists check for {movie_id} returned: {absa_exists}") # Debug log

            if absa_exists:
                # Path C -- Có: Phim đã có kết quả ABSA
                print(f"Route Logic: ABSA results found for {movie_id}. Fetching results for client.")

                # Step C1: Query toàn bộ review_text + ABSA result từ DB
                absa_results_data = get_absa_results(conn, movie_id) # Sử dụng hàm lấy kết quả đã có

                # Step Z1: Trả kết quả ngay cho user
                # Kiểm tra kết quả fetch có rỗng không (mặc dù check_absa_exists là True)
                if absa_results_data:
                     print(f"Route Logic: Successfully fetched {len(absa_results_data)} result rows for {movie_id}.")
                     return jsonify({
                         "status": Config.STATUS_COMPLETED_ABSA, # Có kết quả ABSA nghĩa là trạng thái hoàn thành
                         "movie_id": movie_id,
                         "results": absa_results_data, # Trả về dữ liệu kết quả
                         "message": "Successfully fetched ABSA results."
                     }), 200 # HTTP 200 OK
                else:
                    # Trường hợp hiếm: check_absa_exists là True nhưng get_absa_results trả về rỗng.
                    # Có thể do lỗi DB lúc fetch hoặc dữ liệu không nhất quán.
                    print(f"Route Logic: Data inconsistency? ABSA exists is True, but get_absa_results is empty for {movie_id}.")
                    # Coi như không có kết quả sẵn sàng. Chuyển sang kiểm tra trạng thái xử lý để xem điều gì đã xảy ra.
                    pass # Fall through to status check


            # Path C -- Chưa: Phim có trong DB nhưng chưa có kết quả ABSA (hoặc fetch bị lỗi như trên)
            print(f"Route Logic: No ABSA results found (or fetch failed) for {movie_id}. Proceeding to check status.")
            # Kiểm tra trạng thái xử lý hiện tại của phim
            current_status = check_movie_absa_status(conn, movie_id)
            print(f"Route Logic: Current status for {movie_id}: {current_status}") # Debug log

            # Xử lý các trường hợp dựa trên trạng thái hiện tại
            if current_status in [Config.STATUS_PROCESSING_CRAWL_ABSA, Config.STATUS_PROCESSING_ABSA_ONLY]:
                # Job đang chạy (crawl+absa hoặc absa only)
                print(f"Route Logic: Processing already underway for {movie_id}. Status: {current_status}.")
                # Step E: Thông báo user: "Đang xử lý..."
                return jsonify({
                    "status": current_status,
                    "movie_id": movie_id,
                    "message": "Analysis is currently being processed. Please check back later."
                }), 202 # HTTP 202 Accepted

            elif current_status in [Config.STATUS_COMPLETED_ABSA, Config.STATUS_COMPLETED_NO_REVIEWS]:
                # Trạng thái hoàn thành, nhưng không có kết quả ABSA nào được tìm thấy (ở bước check C).
                # STATUS_COMPLETED_ABSA ở đây chỉ xảy ra nếu có sự không nhất quán (status complete nhưng check_absa_exists là False).
                # Xử lý chung là báo không có kết quả.
                print(f"Route Logic: Previous job status is {current_status}, but no ABSA results found. Reporting no results.")
                # Step E: Thông báo user: "Không có kết quả."
                return jsonify({
                    "status": Config.STATUS_COMPLETED_NO_REVIEWS, # Trạng thái thực tế của dữ liệu tìm thấy
                    "movie_id": movie_id,
                    "message": f"Analysis completed ({current_status}), but no ABSA results were found for this movie."
                }), 200 # HTTP 200 OK, báo không có data

            elif current_status in [Config.STATUS_FAILED_CRAWL, Config.STATUS_FAILED_ABSA]:
                # Job trước đó đã thất bại.
                print(f"Route Logic: Previous job failed for {movie_id}. Status: {current_status}. Offering retry.")
                # Tùy chọn: Cập nhật lại trạng thái thành NOT_STARTED để cho phép trigger lại dễ dàng.
                update_movie_absa_status(conn, movie_id, Config.STATUS_NOT_STARTED) # Cập nhật lại status
                conn.commit() # Commit thay đổi trạng thái

                # Step D2: Trigger job: ABSA + Save (khiến job bắt đầu lại)
                process_movie_reviews.delay(movie_id, trigger_type='absa_only') # Trigger lại task

                # Cập nhật trạng thái thành BẮT ĐẦU XỬ LÝ (ABSA only) sau khi trigger
                update_movie_absa_status(conn, movie_id, Config.STATUS_PROCESSING_ABSA_ONLY)
                conn.commit() # Commit thay đổi trạng thái cuối cùng

                # Step E: Thông báo user: "Đang xử lý lại..."
                return jsonify({
                    "status": Config.STATUS_PROCESSING_ABSA_ONLY, # Trạng thái mới
                    "movie_id": movie_id,
                    "message": f"Previous analysis failed ({current_status}). Retrying analysis now. Please check back later."
                }), 202 # HTTP 202 Accepted


            else: # status is Config.STATUS_NOT_STARTED (hoặc None - phim tồn tại nhưng chưa có status)
                # Phim tồn tại, nhưng chưa có ABSA và trạng thái chưa bắt đầu xử lý.
                print(f"Route Logic: Movie {movie_id} exists, status is {current_status}. Triggering ABSA only job.")
                # Step D2: Trigger job: ABSA + Save
                # Cập nhật trạng thái thành BẮT ĐẦU XỬ LÝ (ABSA only).
                update_movie_absa_status(conn, movie_id, Config.STATUS_PROCESSING_ABSA_ONLY) # Set trạng thái
                conn.commit() # Commit thay đổi trạng thái trước khi trigger task
                process_movie_reviews.delay(movie_id, trigger_type='absa_only') # Pass movie_id và trigger_type

                # Step E: Thông báo user: "Đang xử lý..."
                return jsonify({
                    "status": Config.STATUS_PROCESSING_ABSA_ONLY, # Trạng thái hiện tại
                    "movie_id": movie_id,
                    "message": "Movie found in DB, analysis starting. Please check back later."
                }), 202 # HTTP 202 Accepted

        # --- KẾT THÚC TRIỂN KHAI LOGIC FLOWCHART TRONG ROUTE ---

    except Exception as e:
        # Bắt bất kỳ lỗi ngoại lệ nào xảy ra trong quá trình xử lý request trong hàm này
        print(f"Route Logic: An unexpected error occurred for {movie_id}: {e}")
        # Đảm bảo rollback các thay đổi DB nếu có lỗi xảy ra
        if conn:
             conn.rollback()

        # Trả về lỗi 500 Internal Server Error cho client
        # Có thể bao gồm chi tiết lỗi trong response (chỉ trong dev mode)
        return jsonify({"error": "Internal server error", "details": str(e)}), 500

    finally:
        # Đảm bảo kết nối DB được đóng sau khi request kết thúc
        if conn:
             conn.close()

# Bạn có thể thêm các route khác tại đây nếu cần
# @app.route('/some_other_route')
# def some_other_function():
#    pass