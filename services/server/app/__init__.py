# services/server/app/__init__.py (Code mới, sạch)
from flask import Flask, jsonify
from celery import Celery
from services.server.app.config import Config # <-- Đảm bảo dòng này

app = Flask(__name__)
app.config.from_object(Config)

celery = Celery(__name__, broker=Config.CELERY_BROKER_URL)

celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    result_backend=Config.CELERY_RESULT_BACKEND,
    task_track_started=True,
)

@app.route('/test', methods=['GET'])
def test_route():
    print("Test route was hit!")
    return jsonify({"message": "Minimal test successful!"}), 200

from services.server.app import routes # <-- Đảm bảo dòng này
from services.server.app import tasks # <-- Đảm bảo dòng này

print("Flask app và Celery instance đã được khởi tạo trong app/__init__.py")