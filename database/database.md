# For user movie review data base

## SQL Command for ABSA
Cài PostGreSQL native hoặc host bằng docker (đọc file dành cho docker)


```
-- Xóa các đối tượng theo thứ tự phụ thuộc ngược lại hoặc dùng CASCADE
-- Xóa Materialized View trước nếu nó phụ thuộc vào các bảng sắp bị xóa
DROP MATERIALIZED VIEW IF EXISTS mv_movie_sentiment;

-- Xóa Index của Materialized View (nếu bạn tạo riêng và không dùng CASCADE với VIEW)
-- Tuy nhiên, DROP MATERIALIZED VIEW thường sẽ xóa cả index của nó.
-- Để chắc chắn, bạn có thể thêm:
DROP INDEX IF EXISTS idx_mv_movie_aspect;

-- Xóa các bảng
DROP TABLE IF EXISTS review_aspects CASCADE;
DROP TABLE IF EXISTS reviews_clean CASCADE;
DROP TABLE IF EXISTS reviews_raw CASCADE;
DROP TABLE IF EXISTS movies CASCADE;

-- 1. Danh mục phim (dimension)
CREATE TABLE movies (
    movie_id    VARCHAR PRIMARY KEY,        -- tt1234567
    title       TEXT NOT NULL,
    year        SMALLINT,
    imdb_rating NUMERIC(3,1),               -- Ví dụ: 7.8
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- 2. Review gốc (fact_raw)  – giữ JSONB nếu muốn debug
CREATE TABLE reviews_raw (
    review_id   VARCHAR PRIMARY KEY,        -- rw1234
    movie_id    VARCHAR REFERENCES movies(movie_id) ON DELETE SET NULL ON UPDATE CASCADE, -- Thêm ON DELETE, ON UPDATE
    reviewer_username TEXT,
    submission_date DATE,
    rating      NUMERIC(3,1),               -- Rating gốc của review, có thể khác imdb_rating của phim
    raw_json    JSONB,                      -- lưu tất cả trường gốc
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- 3. Review sạch (fact_clean)  – text đã qua dedup + filter EN
CREATE TABLE reviews_clean (
    review_id   VARCHAR PRIMARY KEY REFERENCES reviews_raw(review_id) ON DELETE CASCADE ON UPDATE CASCADE,
    movie_id    VARCHAR, -- Không có REFERENCES trực tiếp ở đây, nhưng sẽ được dùng để JOIN
    review_text TEXT NOT NULL,
    text_len    INT,
    rating      NUMERIC(3,1), -- Rating của review (có thể lấy từ reviews_raw.rating)
    like_count  INT,
    dislike_count INT,
    tsv         tsvector GENERATED ALWAYS
                AS (to_tsvector('english', review_text)) STORED
);

-- Tạo index sau khi bảng được tạo
CREATE INDEX idx_clean_movie_id       ON reviews_clean (movie_id);
CREATE INDEX idx_clean_tsv            ON reviews_clean USING GIN (tsv);
CREATE INDEX idx_clean_text_len       ON reviews_clean (text_len);
-- Cân nhắc thêm index cho reviews_raw.movie_id nếu thường xuyên join reviews_raw với movies
CREATE INDEX IF NOT EXISTS idx_raw_movie_id ON reviews_raw (movie_id);


-- 4. Kết quả ABSA (fact_absa)
CREATE TABLE review_aspects (
    review_id   VARCHAR REFERENCES reviews_clean(review_id) ON DELETE CASCADE ON UPDATE CASCADE,
    aspect      TEXT,               -- 'acting', 'music', …
    sentiment   SMALLINT,           -- 1:pos, 0:neu, -1:neg (hoặc dùng kiểu ENUM nếu muốn)
    confidence  NUMERIC(4,3),       -- Ví dụ: 0.987
    PRIMARY KEY (review_id, aspect)
);

-- 5. Thống kê gộp (materialized view)
-- Đảm bảo các bảng review_aspects và reviews_clean đã được tạo và có dữ liệu trước khi tạo view này hiệu quả
CREATE MATERIALIZED VIEW mv_movie_sentiment AS
SELECT
    rc.movie_id,  -- Lấy movie_id từ bảng reviews_clean (đặt alias là rc)
    ra.aspect,
    SUM(CASE WHEN ra.sentiment = 1  THEN 1 ELSE 0 END) AS pos,  -- Sửa lại để SUM hoạt động đúng
    SUM(CASE WHEN ra.sentiment = 0  THEN 1 ELSE 0 END) AS neu,  -- Sửa lại để SUM hoạt động đúng
    SUM(CASE WHEN ra.sentiment = -1 THEN 1 ELSE 0 END) AS neg, -- Sửa lại để SUM hoạt động đúng
    COUNT(ra.review_id) AS total_reviews_for_aspect -- Đếm số review có aspect đó
FROM
    review_aspects ra
JOIN
    reviews_clean rc ON ra.review_id = rc.review_id
GROUP BY
    rc.movie_id, ra.aspect;

```





```

-- Kết nối tới database movie_reviews_dw trước khi chạy các lệnh này.
-- Nếu bạn đang dùng psql, gõ: \c movie_reviews_dw

-- =================================================================================
-- KIỂM TRA CHUNG
-- =================================================================================

-- 1. Đếm số lượng dòng trong mỗi bảng
SELECT 'movies' AS table_name, COUNT(*) AS row_count FROM movies
UNION ALL
SELECT 'reviews_raw' AS table_name, COUNT(*) AS row_count FROM reviews_raw
UNION ALL
SELECT 'reviews_clean' AS table_name, COUNT(*) AS row_count FROM reviews_clean
UNION ALL
SELECT 'review_aspects' AS table_name, COUNT(*) AS row_count FROM review_aspects; -- Bảng này sẽ rỗng cho đến khi bạn chạy ABSA

-- =================================================================================
-- KIỂM TRA BẢNG `movies`
-- =================================================================================

-- 2. Xem 10 dòng dữ liệu mẫu từ bảng `movies`
SELECT * FROM movies LIMIT 10;

-- 3. Kiểm tra các giá trị NULL trong các cột quan trọng của `movies`
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN movie_id IS NULL THEN 1 ELSE 0 END) AS null_movie_id,
    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_title,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year, -- Bạn có thể có nhiều NULL ở đây ban đầu
    SUM(CASE WHEN imdb_rating IS NULL THEN 1 ELSE 0 END) AS null_imdb_rating -- Tương tự, có thể nhiều NULL
FROM movies;

-- 4. Phân phối số lượng phim theo năm (nếu cột 'year' đã có dữ liệu)
SELECT year, COUNT(*) AS number_of_movies
FROM movies
WHERE year IS NOT NULL
GROUP BY year
ORDER BY year DESC
LIMIT 20; -- Xem 20 năm gần nhất có phim

-- 5. Kiểm tra các phim có rating cao nhất (nếu cột 'imdb_rating' đã có dữ liệu)
SELECT movie_id, title, imdb_rating
FROM movies
WHERE imdb_rating IS NOT NULL
ORDER BY imdb_rating DESC
LIMIT 10;

-- =================================================================================
-- KIỂM TRA BẢNG `reviews_raw`
-- =================================================================================

-- 6. Xem 5 dòng dữ liệu mẫu từ bảng `reviews_raw`
SELECT review_id, movie_id, reviewer_username, submission_date, rating, left(raw_json::text, 200) as raw_json_snippet
FROM reviews_raw
LIMIT 5;

-- 7. Kiểm tra các giá trị NULL trong các cột quan trọng của `reviews_raw`
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) AS null_review_id,
    SUM(CASE WHEN movie_id IS NULL THEN 1 ELSE 0 END) AS null_movie_id,
    SUM(CASE WHEN submission_date IS NULL THEN 1 ELSE 0 END) AS null_submission_date,
    SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) AS null_rating,
    SUM(CASE WHEN raw_json IS NULL THEN 1 ELSE 0 END) AS null_raw_json
FROM reviews_raw;

-- 8. Kiểm tra xem tất cả `movie_id` trong `reviews_raw` có tồn tại trong bảng `movies` không
-- (Lệnh này sẽ không trả về gì nếu tất cả movie_id đều hợp lệ)
SELECT rr.movie_id, COUNT(rr.review_id) as num_reviews
FROM reviews_raw rr
LEFT JOIN movies m ON rr.movie_id = m.movie_id
WHERE m.movie_id IS NULL -- Tìm các movie_id trong reviews_raw không có trong bảng movies
GROUP BY rr.movie_id;

-- 9. Phân phối số lượng review theo rating (nếu cột 'rating' đã có dữ liệu)
SELECT rating, COUNT(*) AS number_of_reviews
FROM reviews_raw
WHERE rating IS NOT NULL
GROUP BY rating
ORDER BY rating;

-- 10. Xem nội dung của một vài `raw_json` để kiểm tra
SELECT review_id, raw_json ->> 'review_title' AS review_title_from_json, raw_json ->> 'review_content' AS review_content_from_json
FROM reviews_raw
WHERE raw_json IS NOT NULL
LIMIT 5;

-- =================================================================================
-- KIỂM TRA BẢNG `reviews_clean`
-- =================================================================================

-- 11. Xem 5 dòng dữ liệu mẫu từ bảng `reviews_clean`
SELECT review_id, movie_id, LEFT(review_text, 150) AS review_text_snippet, text_len, rating, like_count, dislike_count
FROM reviews_clean
LIMIT 5;

-- 12. Kiểm tra các giá trị NULL trong các cột quan trọng của `reviews_clean`
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) AS null_review_id,
    SUM(CASE WHEN movie_id IS NULL THEN 1 ELSE 0 END) AS null_movie_id,
    SUM(CASE WHEN review_text IS NULL THEN 1 ELSE 0 END) AS null_review_text,
    SUM(CASE WHEN text_len IS NULL THEN 1 ELSE 0 END) AS null_text_len,
    SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) AS null_rating
FROM reviews_clean;

-- 13. Kiểm tra xem tất cả `review_id` trong `reviews_clean` có tồn tại trong `reviews_raw` không
-- (Lệnh này sẽ không trả về gì nếu tất cả review_id đều hợp lệ)
SELECT rc.review_id
FROM reviews_clean rc
LEFT JOIN reviews_raw rr ON rc.review_id = rr.review_id
WHERE rr.review_id IS NULL;

-- 14. Kiểm tra phân phối độ dài review (`text_len`)
SELECT
    MIN(text_len) AS min_length,
    MAX(text_len) AS max_length,
    AVG(text_len)::numeric(10,2) AS avg_length,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY text_len) AS median_length -- Tính trung vị
FROM reviews_clean;

-- 15. Thử nghiệm tìm kiếm Full-Text trên cột `tsv` (nếu đã có dữ liệu)
-- Tìm các review chứa từ "amazing" VÀ "plot"
SELECT review_id, movie_id, LEFT(review_text, 100) AS snippet
FROM reviews_clean
WHERE tsv @@ to_tsquery('english', 'amazing & plot') -- @@ là toán tử match
LIMIT 10;

-- Tìm các review chứa cụm từ "great acting"
SELECT review_id, movie_id, LEFT(review_text, 100) AS snippet
FROM reviews_clean
WHERE tsv @@ phraseto_tsquery('english', 'great acting')
LIMIT 10;

-- 16. Kiểm tra các review có like_count hoặc dislike_count cao bất thường (nếu có dữ liệu)
SELECT review_id, movie_id, like_count, dislike_count, LEFT(review_text, 100)
FROM reviews_clean
ORDER BY like_count DESC NULLS LAST -- Sắp xếp theo like giảm dần, NULL ở cuối
LIMIT 10;

SELECT review_id, movie_id, like_count, dislike_count, LEFT(review_text, 100)
FROM reviews_clean
ORDER BY dislike_count DESC NULLS LAST
LIMIT 10;

-- =================================================================================
-- KIỂM TRA BẢNG `review_aspects` (Sẽ rỗng cho đến khi bạn chạy ABSA và nạp dữ liệu)
-- =================================================================================

-- 17. Xem 5 dòng dữ liệu mẫu (nếu có)
SELECT * FROM review_aspects LIMIT 5;

-- 18. Đếm số lượng aspect duy nhất (nếu có dữ liệu)
SELECT aspect, COUNT(*) as num_occurrences
FROM review_aspects
GROUP BY aspect
ORDER BY num_occurrences DESC;

-- 19. Phân phối sentiment (nếu có dữ liệu)
SELECT
    sentiment,
    CASE
        WHEN sentiment = 1 THEN 'Positive'
        WHEN sentiment = 0 THEN 'Neutral'
        WHEN sentiment = -1 THEN 'Negative'
        ELSE 'Unknown'
    END as sentiment_label,
    COUNT(*) as count
FROM review_aspects
GROUP BY sentiment
ORDER BY sentiment;

-- =================================================================================
-- KIỂM TRA MATERIALIZED VIEW `mv_movie_sentiment`
-- (Sẽ rỗng hoặc không chính xác cho đến khi `review_aspects` có dữ liệu và view được REFRESH)
-- =================================================================================

-- 20. Xem 10 dòng dữ liệu mẫu từ Materialized View (sau khi REFRESH)
-- Nhớ chạy: REFRESH MATERIALIZED VIEW mv_movie_sentiment; trước khi truy vấn nếu review_aspects đã thay đổi
SELECT * FROM mv_movie_sentiment LIMIT 10;

-- 21. Tìm các aspect phổ biến nhất cho một phim cụ thể (sau khi REFRESH)
SELECT movie_id, aspect, total_reviews_for_aspect, pos, neu, neg
FROM mv_movie_sentiment
WHERE movie_id = 'tt0111161' -- Thay bằng movie_id bạn muốn kiểm tra
ORDER BY total_reviews_for_aspect DESC;
```

