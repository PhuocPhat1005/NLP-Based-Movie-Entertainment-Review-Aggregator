# scripts/json_to_csv_multiple.py
import glob
import json
import csv
import pathlib
import os

# --- Cấu hình ---
# Thư mục chứa các file .jsonl đã được xử lý (output từ script preprocess_jsonl.py)
INPUT_JSONL_DIR = pathlib.Path("output_processed_jsonl") # Ví dụ: "output_processed_jsonl"
# Thư mục để lưu các file .csv đầu ra
OUTPUT_CSV_DIR = pathlib.Path("data_csv")

# Các trường (cột) bạn muốn có trong file CSV
# Đảm bảo các key này khớp với các key trong file JSON Lines của bạn
# hoặc bạn sẽ cần xử lý .get(key, default_value) trong vòng lặp
FIELDS = [
    "review_id",
    "movie_id",
    "review_text", # Sẽ lấy từ "review_content" trong JSONL
    "text_len",
    "rating",
    "like_count",    # Sẽ lấy từ "like" trong JSONL
    "dislike_count", # Sẽ lấy từ "dislike" trong JSONL
    "submission_date",
    "reviewer_username",
    # Thêm các trường khác nếu cần, ví dụ:
    "review_title",
    "spoiler",
    "movie_name",
    "original_title"
]

def convert_jsonl_to_csv_multiple_files(input_dir: pathlib.Path, output_dir: pathlib.Path, field_names: list):
    """
    Chuyển đổi các file JSON Lines (.jsonl) trong thư mục input_dir
    thành các file CSV riêng biệt trong thư mục output_dir.
    """
    # Tạo thư mục output nếu nó chưa tồn tại
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Thư mục output CSV: {output_dir.resolve()}")

    # Tìm tất cả các file .jsonl trong thư mục input
    # Giả sử các file jsonl của bạn có pattern là "reviews_part_*_processed.jsonl"
    # hoặc một pattern chung hơn là "*.jsonl"
    jsonl_files = sorted(list(input_dir.glob("reviews_part_*_processed.jsonl")))
    # Nếu không tìm thấy với pattern cụ thể, thử pattern chung hơn
    if not jsonl_files:
        print(f"Không tìm thấy file nào với pattern 'reviews_part_*_processed.jsonl' trong {input_dir}, thử với '*.jsonl'...")
        jsonl_files = sorted(list(input_dir.glob("*.jsonl")))


    if not jsonl_files:
        print(f"Không tìm thấy file .jsonl nào trong thư mục: {input_dir.resolve()}")
        return

    print(f"Tìm thấy {len(jsonl_files)} file .jsonl để xử lý.")

    for input_jsonl_path in jsonl_files:
        # Tạo tên file CSV output tương ứng
        # Ví dụ: reviews_part_001_processed.jsonl -> reviews_part_001_processed.csv
        csv_filename = input_jsonl_path.stem + ".csv" # Lấy tên file không có phần mở rộng cuối cùng và thêm .csv
        output_csv_path = output_dir / csv_filename

        print(f"  Đang xử lý: {input_jsonl_path.name}  ->  {output_csv_path.name}")

        try:
            with open(input_jsonl_path, 'r', encoding='utf-8') as f_in, \
                 open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:

                writer = csv.DictWriter(f_out, fieldnames=field_names, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader() # Ghi dòng header

                line_count = 0
                for line in f_in:
                    try:
                        data_dict = json.loads(line)
                        
                        # Chuẩn bị dòng để ghi vào CSV, ánh xạ từ key JSONL sang tên cột CSV
                        # và xử lý các trường hợp key có thể bị thiếu bằng .get()
                        row_to_write = {
                            "review_id": data_dict.get("review_id"),
                            "movie_id": data_dict.get("movie_id"),
                            "review_text": data_dict.get("review_content"), # Lấy từ "review_content"
                            "text_len": len(data_dict.get("review_content", "")), # Tính độ dài
                            "rating": data_dict.get("rating"),
                            "like_count": data_dict.get("like"), # Lấy từ "like"
                            "dislike_count": data_dict.get("dislike"), # Lấy từ "dislike"
                            "submission_date": data_dict.get("submission_date"),
                            "reviewer_username": data_dict.get("reviewer_username"),
                            # Lấy các trường tùy chọn khác nếu có trong FIELDS
                            "review_title": data_dict.get("review_title"),
                            "spoiler": data_dict.get("spoiler"),
                            "movie_name": data_dict.get("movie_name"),
                            "original_title": data_dict.get("original_title")
                        }
                        
                        # Chỉ giữ lại các key có trong field_names để ghi
                        filtered_row = {key: row_to_write.get(key) for key in field_names}
                        writer.writerow(filtered_row)
                        line_count += 1

                    except json.JSONDecodeError:
                        print(f"    Cảnh báo: Bỏ qua dòng không phải JSON hợp lệ trong {input_jsonl_path.name}: {line.strip()[:100]}...")
                    except Exception as e:
                        print(f"    Lỗi khi xử lý dòng: {line.strip()[:100]}... trong file {input_jsonl_path.name}. Lỗi: {e}")
            
            print(f"    -> Đã chuyển đổi {line_count} dòng và lưu vào {output_csv_path.name}")

        except IOError as e:
            print(f"  Lỗi I/O khi xử lý file {input_jsonl_path.name} hoặc {output_csv_path.name}: {e}")
        except Exception as e:
            print(f"  Lỗi không mong muốn khi xử lý file {input_jsonl_path.name}: {e}")

    print("\nHoàn tất quá trình chuyển đổi.")

if __name__ == "__main__":
    # Chạy hàm chính
    convert_jsonl_to_csv_multiple_files(INPUT_JSONL_DIR, OUTPUT_CSV_DIR, FIELDS)
