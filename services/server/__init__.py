# services/server/app/__init__.py
from flask import Flask
from celery import Celery
from .app.config import Config # Import cấu hình từ config.py

# 1. Khởi tạo ứng dụng Flask
app = Flask(__name__)
app.config.from_object(Config) # Load cấu hình từ Config class

# 2. Khởi tạo ứng dụng Celery
# Lấy cấu hình broker và backend từ Config
celery = Celery(__name__, broker=Config.CELERY_BROKER_URL)

# Cập nhật cấu hình Celery từ Config
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    result_backend=Config.CELERY_RESULT_BACKEND,
    task_track_started=True,
    # Thêm các cấu hình Celery khác nếu có trong Config
)

# --- Tùy chọn: Cấu hình context của Flask cho Celery tasks ---
# (Như đã giải thích trước đó, không bắt buộc nếu task không dùng app context)
# class ContextTask(celery.Task):
#     def __call__(self, *args, **kwargs):
#         with app.app_context():
#             return super().__call__(*args, **kwargs)
# celery.Task = ContextTask


# 3. Import Routes và Tasks SAU KHI app VÀ celery ĐƯỢC TẠO
# Import module routes để các route được đăng ký với instance 'app'



print("Flask app và Celery instance đã được khởi tạo trong app/__init__.py") # Log để kiểm tra