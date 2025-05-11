import streamlit as st
import requests
import re
import textwrap
import os
import json

# ---------------------------------------------------------------------------
# Streamlit page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="ABSA Backend Tester", page_icon="🔬", layout="wide")

st.title("🔬 ABSA Backend Tester")
st.write("Nhập IMDb URL hoặc Movie ID để kiểm tra endpoint `/get_absa/<movie_id>` của backend.")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# URL gốc của Flask backend của bạn
# Mặc định là http://localhost:5000
# Bạn có thể thay đổi ở đây hoặc dùng sidebar
DEFAULT_BACKEND_URL = "http://localhost:5001"
BACKEND_URL_ENV_VAR = "BACKEND_SERVICE_URL" # Tên biến môi trường tùy chọn

backend_url = st.sidebar.text_input(
    "Backend URL",
    value=os.getenv(BACKEND_URL_ENV_VAR, DEFAULT_BACKEND_URL),
    help=f"URL của Flask backend (ví dụ: {DEFAULT_BACKEND_URL}). Có thể cấu hình bằng biến môi trường '{BACKEND_URL_ENV_VAR}'."
)

# Regex để trích xuất Movie ID từ URL hoặc chuỗi
ID_RE = re.compile(r"(tt\d{6,8})")

def extract_movie_id(text: str) -> str | None:
    """Extracts movie ID from IMDb URL or ID string."""
    if text.startswith("http"):
        m = ID_RE.search(text)
        return m.group(1) if m else None
    # Assume direct ID if it matches the pattern
    return text if ID_RE.fullmatch(text) else None

# ---------------------------------------------------------------------------
# Main Interface
# ---------------------------------------------------------------------------
user_input = st.text_input(
    "IMDb URL / Movie ID",
    placeholder="https://www.imdb.com/title/tt1375666/ or tt1375666"
)

if st.button("✨ Get ABSA Results"):
    movie_id = extract_movie_id(user_input.strip())

    if not movie_id:
        st.error("❌ Invalid IMDb URL / movie ID format!")
        st.stop()

    st.info(f"Requesting ABSA for Movie ID: **{movie_id}**")

    # Construct the full request URL
    request_url = f"{backend_url}/get_absa/{movie_id}"

    with st.spinner(f"Sending request to backend: {request_url} ..."):
        try:
            response = requests.get(request_url)

            # Check the response status code
            if response.status_code == 200:
                # Success - Got ABSA results or "No reviews found" message
                data = response.json()
                status = data.get("status")

                if status == "completed" and data.get("results"):
                    st.success(f"✅ Successfully fetched ABSA results for {movie_id}!")
                    absa_results = data.get("results", [])

                    if absa_results:
                        st.subheader("ABSA Results:")
                        # Display results grouped by review or simply listed
                        # For simplicity, let's list them per aspect per review found
                        reviews_map = {} # Group results by review_id
                        for res in absa_results:
                             review_id = res.get('review_id', 'N/A Review ID')
                             if review_id not in reviews_map:
                                 reviews_map[review_id] = {
                                     'text': res.get('review_text', 'No review text available.'),
                                     'aspects': [],
                                     'rating': res.get('rating', 'n/a'),
                                     'like_count': res.get('like_count', 'n/a'),
                                     'dislike_count': res.get('dislike_count', 'n/a'),
                                 }
                             reviews_map[review_id]['aspects'].append({
                                 'aspect': res.get('aspect', 'General'),
                                 'sentiment': res.get('sentiment', 'Unknown'), # This should be string now
                                 'confidence': res.get('confidence', 'n/a'),
                             })

                        st.write(f"Found ABSA results for {len(reviews_map)} reviews.")

                        for review_id, review_data in reviews_map.items():
                            st.markdown(f"---")
                            header = (
                                f"**Review ID:** `{review_id}` • "
                                f"⭐ **Rating:** {review_data['rating']} • "
                                f"👍 {review_data['like_count']}/{review_data['dislike_count']}"
                            )
                            st.markdown(header)
                            st.write(textwrap.fill(review_data['text'], width=100))
                            st.markdown("**Aspect Sentiments:**")
                            for aspect_info in review_data['aspects']:
                                sentiment_emoji = "😊" if aspect_info['sentiment'] == 'Positive' else "😞" if aspect_info['sentiment'] == 'Negative' else "😐"
                                st.write(
                                    f"- **{aspect_info['aspect']}:** {aspect_info['sentiment']} {sentiment_emoji} "
                                    f"(Confidence: {aspect_info['confidence']})"
                                )


                    else:
                         # Status is 'completed' but results list is empty
                         st.warning(f"✅ Backend reported status 'completed', but no ABSA results were found for {movie_id}.")
                         st.info("This might mean no reviews were available for this movie, or processing didn't yield scorable aspects.")


                elif status == "completed_no_reviews":
                    # Specific status from backend indicating completed but no reviews saved
                     st.warning(f"✅ Backend reported status 'completed_no_reviews' for {movie_id}.")
                     st.info("This likely means the background job finished, but no reviews were available or processed for this movie ID.")


                else:
                    # Unexpected 200 response structure
                    st.warning(f"✅ Received unexpected 200 response status: {status}. Data:")
                    st.json(data)


            elif response.status_code == 202:
                # Accepted - Processing is ongoing or starting
                data = response.json()
                status = data.get("status")
                message = data.get("message", "Processing status unknown.")

                if status in ["processing_started", "processing", "processing_restarted"]:
                    st.info(f"⏳ Backend Status: **{status}**")
                    st.write(f"Message: {message}")
                    st.warning("Analysis is in progress. Please wait a moment and click the 'Get ABSA Results' button again to check for results.")
                else:
                    st.warning(f"⏳ Received unexpected 202 response status: {status}. Message: {message}")
                    st.json(data)

            else:
                # Other HTTP errors (bao gồm 403)
                st.error(f"❌ Backend Request Failed: HTTP Status Code {response.status_code}")
                try:
                    error_data = response.json()
                    st.write("Response body (JSON):")
                    st.json(error_data)
                except json.JSONDecodeError:
                    st.write("Response body (text):")
                    st.text(response.text) # I

        except requests.exceptions.RequestException as e:
            # Catch network or connection errors
            st.error(f"❌ Failed to connect to backend at {backend_url}: {e}")
            st.warning("Please ensure your Flask backend, Celery worker, and message broker (Redis) are running.")

        except Exception as e:
            # Catch any other unexpected errors
            st.exception(e)