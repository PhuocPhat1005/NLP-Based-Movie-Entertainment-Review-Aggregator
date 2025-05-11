# services/server/app/config.py
import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'hard to guess string'
    # Database Configuration
    # Đảm bảo giá trị mặc định này là chính xác và sử dụng nó nếu không set biến môi trường
    DATABASE_URL = os.environ.get('DATABASE_URL') or "postgresql://postgres:postgre2025@localhost:5432/movie_reviews_dw"

    # Celery Configuration
    # Đảm bảo các URL này trỏ đến Redis (hoặc broker/backend bạn dùng)
    CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL') or 'redis://localhost:6379/0'
    CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND') or 'redis://localhost:6379/0'


    # ABSA Model Paths (Vẫn cần nếu model cục bộ là fallback hoặc dùng cho mục đích khác)
    NLP_MODEL_PATH = os.environ.get('NLP_MODEL_PATH') or 'models/nlp_model.pkl' # <-- Đảm bảo đường dẫn này đúng so với gốc dự án
    VECTORIZER_PATH = os.environ.get('VECTORIZER_PATH') or 'models/tranform.pkl' # <-- Đảm bảo đường dẫn này đúng so với gốc dự án

    # --- THÊM CẤU HÌNH MỚI CHO ABSA API ---
    # URL của API model ABSA đã deploy
    ABSA_API_URL = os.environ.get('ABSA_API_URL') or 'http://localhost:8000/analyze_reviews' # <-- Thay đổi URL này nếu API của bạn chạy ở địa chỉ/port khác

    # --- ĐỊNH NGHĨA CÁC TRẠNG THÁI XỬ LÝ PHIM MỚI ---
    # Các giá trị cho cột 'processing_status' trong bảng 'movies'
    STATUS_NOT_STARTED = 'NOT_STARTED'
    STATUS_PROCESSING_CRAWL_ABSA = 'PROCESSING_CRAWL_ABSA' # Đang crawl và phân tích
    STATUS_PROCESSING_ABSA_ONLY = 'PROCESSING_ABSA_ONLY' # Đã có review, chỉ phân tích ABSA
    STATUS_COMPLETED_ABSA = 'COMPLETED_ABSA' # Đã hoàn thành ABSA và lưu kết quả
    STATUS_COMPLETED_NO_REVIEWS = 'COMPLETED_NO_REVIEWS' # Hoàn thành xử lý, nhưng không tìm thấy review hoặc kết quả ABSA nào
    STATUS_FAILED_CRAWL = 'FAILED_CRAWL' # Lỗi trong quá trình crawl
    STATUS_FAILED_ABSA = 'FAILED_ABSA' # Lỗi trong quá trình phân tích ABSA hoặc lưu kết quả
    # Bạn có thể thêm các trạng thái lỗi chi tiết hơn nếu cần (ví dụ: FAILED_SAVE_REVIEWS, FAILED_API_CALL)

    # --- Cấu hình khác (tùy chọn) ---
    # Ví dụ: Thời gian chờ tối đa cho API ABSA
    ABSA_API_TIMEOUT_SECONDS = 60 # Tùy chỉnh nếu cần