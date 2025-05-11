# services/server/run.py
import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))

project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))

sys.path.insert(0, project_root)

sys.path.append(project_root)

from services.server.app import app

if __name__ == '__main__':
    print(f"sys.path sau workaround: {sys.path}")
    print("Starting Flask Development Server...")
    # Chạy Flask trên port 5001
    app.run(debug=True, port=5001) # <-- THÊM port=5001