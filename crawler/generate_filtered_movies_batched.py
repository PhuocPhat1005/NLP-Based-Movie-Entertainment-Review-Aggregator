import os
import json
import time
import google.generativeai as genai

# Cấu hình API Key Gemini
GEMINI_API_KEY = "AIzaSyAMNpfuhJfwFw3lFv-Lu9FxeH9max1dAJA"  # <<< Thay bằng API Key Gemini Flash 2.0 của bạn

genai.configure(api_key=GEMINI_API_KEY)

def generate_batch(prompt, batch_name):
    model = genai.GenerativeModel(model_name="gemini-1.5-flash-latest")
    print(f"\n⚡ Generating batch: {batch_name}")

    for attempt in range(3):
        try:
            response = model.generate_content(prompt)
            text = response.text

            start_idx = text.index('[')
            end_idx = text.rindex(']') + 1
            json_text = text[start_idx:end_idx]

            movie_list = json.loads(json_text)

            print(f"✅ Batch {batch_name}: {len(movie_list)} movies generated.")
            return movie_list

        except Exception as e:
            print(f"❌ Error in batch {batch_name}, attempt {attempt+1}: {e}")
            time.sleep(2)

    print(f"🚨 Failed batch {batch_name} after 3 attempts.")
    return []

def save_movies(movie_list, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(movie_list, f, indent=4, ensure_ascii=False)
    print(f"\n✅ Saved {len(movie_list)} movies to {output_path}")

if __name__ == "__main__":
    output_folder = "./output"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    all_movies = []
    seen_names = set()

    prompts = {
        "movies_2020s": (
            "Liệt kê 250 bộ phim nổi tiếng phát hành từ năm 2000 đến 2025, "
            "gồm id (IMDb ID), name (tên phim), original_title (tên gốc). "
            "Đảm bảo id (IMDb ID) chính xác từ IMDb "
            "Không lặp lại phim. Chỉ trả JSON array thuần. "
            "Nếu không có original_title riêng, dùng luôn name."
            
        ),
        "classic_movies": (
            "Liệt kê 250 bộ phim kinh điển nổi tiếng phát hành từ 1950 đến 1999, "
            "gồm id (IMDb ID), name (tên phim), original_title (tên gốc). "
            "Đảm bảo id (IMDb ID) chính xác từ IMDb "
            "Không lặp lại phim. Chỉ trả JSON array thuần."
            "Nếu không có original_title riêng, dùng luôn name."
            
        ),
        "oscar_movies": (
            "Liệt kê 250 bộ phim đã từng thắng hoặc được đề cử Oscar Best Picture, "
            "gồm id (IMDb ID), name (tên phim), original_title (tên gốc). "
            "Đảm bảo id (IMDb ID) chính xác từ IMDb "
            "Không lặp lại phim. Chỉ trả JSON array thuần."
             "Nếu không có original_title riêng, dùng luôn name."
            
        ),
        "asian_movies": (
            "Liệt kê 250 bộ phim nổi tiếng đến từ châu Á, "
            "gồm id (IMDb ID), name (tên phim), original_title (tên gốc). "
            "Đảm bảo id (IMDb ID) chính xác từ IMDb "
            "Không lặp lại phim. Chỉ trả JSON array thuần."
            "Nếu không có original_title riêng, dùng luôn name."
           
        ),
        "indie_documentary": (
            "Liệt kê 250 bộ phim indie và phim tài liệu nổi tiếng trên toàn thế giới, "
            "gồm id (IMDb ID), name (tên phim), original_title (tên gốc). "
            "Đảm bảo id (IMDb ID) chính xác từ IMDb"
            "Không lặp lại phim. Chỉ trả JSON array thuần."
            "Nếu không có original_title riêng, dùng luôn name."
            
        ),
        "action_scifi": (
            "Liệt kê 250 bộ phim hành động và khoa học viễn tưởng nổi tiếng mọi thời đại, "
            "gồm id (IMDb ID), name (tên phim), original_title (tên gốc). "
            "Đảm bảo id (IMDb ID) chính xác từ IMDb  "
            "Không lặp lại phim. Chỉ trả JSON array thuần."
            "Nếu không có original_title riêng, dùng luôn name."
           
        )
    }

    for batch_name, prompt in prompts.items():
        batch_movies = generate_batch(prompt, batch_name)
        if batch_movies:
            for movie in batch_movies:
                name_key = movie['name'].strip().lower()
                if name_key not in seen_names:
                    seen_names.add(name_key)
                    all_movies.append(movie)
        time.sleep(2)  # nghỉ nhẹ giữa các batch

    print(f"\n🚀 Total unique movies collected: {len(all_movies)}")

    if all_movies:
        save_movies(all_movies, os.path.join(output_folder, "filtered_movies_advanced.json"))
    else:
        print("❌ No movies collected.")
