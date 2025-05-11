import os
import re
import textwrap
from typing import List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st

# ---------------------------------------------------------------------------
# Streamlit page config – must be first call
# ---------------------------------------------------------------------------
st.set_page_config(page_title="IMDb Review Explorer", page_icon="🎬", layout="wide")

"""IMDb Review Explorer  
Interactive Streamlit app to browse reviews stored in **PostgreSQL**.

* Paste an IMDb URL (e.g. https://www.imdb.com/title/tt1375666/) **or** just the
  `tt########` id.
* The app joins `reviews_clean` ↔︎ `reviews_raw` to display reviewer & date,
  and also looks up movie **title + year** from the `movies` table so you see
  the full film name.

Run locally:
```bash
export DATABASE_URL="postgresql://postgres:postgres2025@localhost:5432/movie_reviews_dw"
streamlit run streamlit_imdb_review_explorer.py
```
"""

# ---------------------------------------------------------------------------
# Regex & default DSN
# ---------------------------------------------------------------------------
ID_RE = re.compile(r"(tt\d{6,8})")

DEFAULT_DSN = psycopg2.extensions.parse_dsn(
    os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgre2025@localhost:5432/movie_reviews_dw",
    )
)

def make_dsn(host: str, port: str, db: str, user: str, pw: str) -> str:
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


# ---------------------------------------------------------------------------
# Cached connection
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def connect_cached(dsn: str):
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def extract_movie_id(text: str) -> Optional[str]:
    if text.startswith("http"):
        m = ID_RE.search(text)
        return m.group(1) if m else None
    return text if ID_RE.fullmatch(text) else None


def fetch_movie_info(conn, movie_id: str) -> Optional[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT title, year FROM movies WHERE movie_id = %s", (movie_id,))
        return cur.fetchone()


def fetch_reviews(conn, movie_id: str, limit: int = 300) -> List[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT  rc.review_id,
                    rr.reviewer_username,
                    rr.submission_date,
                    rc.rating,
                    rc.like_count,
                    rc.dislike_count,
                    rc.review_text
            FROM    reviews_clean rc
            LEFT JOIN reviews_raw rr USING (review_id)
            WHERE   rc.movie_id = %s
            ORDER   BY rc.like_count - rc.dislike_count DESC NULLS LAST
            LIMIT   %s;
            """,
            (movie_id, limit),
        )
        return cur.fetchall()

# ---------------------------------------------------------------------------
# Sidebar – connection settings
# ---------------------------------------------------------------------------
st.sidebar.header("🔧 Database connection")
host = st.sidebar.text_input("Host", DEFAULT_DSN.get("host", "localhost"))
port = st.sidebar.text_input("Port", DEFAULT_DSN.get("port", "5432"))
db   = st.sidebar.text_input("Database", DEFAULT_DSN.get("dbname", "postgres"))
user = st.sidebar.text_input("User", DEFAULT_DSN.get("user", "postgres"))
pw   = st.sidebar.text_input("Password", DEFAULT_DSN.get("password", ""), type="password")

if st.sidebar.button("Test connection"):
    try:
        with st.spinner("Connecting …"):
            _ = connect_cached(make_dsn(host, port, db, user, pw))
        st.sidebar.success("✅ Connected!")
    except Exception as e:
        st.sidebar.error(f"❌ {e}")

# ---------------------------------------------------------------------------
# Main interface
# ---------------------------------------------------------------------------
st.title("🎬 IMDb Movie Review Explorer")
st.write(
    "Paste an IMDb URL / `tt` id, tweak DB settings in sidebar if needed, then"
    " click **Fetch Reviews**. The film title is looked up from the **movies**"
    " table so you can confirm the correct movie."
)

col1, col2 = st.columns([4, 1])
with col1:
    user_input = st.text_input(
        "IMDb URL / Movie ID", placeholder="https://www.imdb.com/title/tt1375666/"
    )
with col2:
    limit = st.number_input("Max reviews", 10, 500, 100, 10)

if st.button("🔍 Fetch Reviews"):
    movie_id = extract_movie_id(user_input.strip())
    if not movie_id:
        st.error("❌ Invalid IMDb URL / movie id!")
        st.stop()

    dsn = make_dsn(host, port, db, user, pw)
    try:
        conn = connect_cached(dsn)
    except Exception as e:
        st.error(f"Cannot connect to DB: {e}")
        st.stop()

    with st.spinner("Fetching movie info …"):
        info = fetch_movie_info(conn, movie_id)
        if not info:
            st.warning(f"Movie id {movie_id} not found in `movies` table.")
            title_display = movie_id
        else:
            title_display = f"{info['title']} ({info['year'] or 'n/a'})"

    st.header(f"🎞️  {title_display}")

    with st.spinner("Querying reviews …"):
        try:
            rows = fetch_reviews(conn, movie_id, int(limit))
        except Exception as e:
            st.exception(e)
            st.stop()

    if not rows:
        st.info("No reviews found.")
    else:
        st.success(f"Showing top {len(rows)} reviews sorted by helpful votes")
        for r in rows:
            header = (
                f"### {r['review_id']}  •  ⭐ {r['rating'] or 'n/a'}  "
                f"•  👍 {r['like_count']}/{r['dislike_count']}  "
                f"•  👤 {r['reviewer_username'] or 'anon'}  "
                f"•  📅 {r['submission_date'] or 'n/a'}"
            )
            st.markdown(header)
            st.write(textwrap.fill(r["review_text"], width=100))
            st.markdown("---")