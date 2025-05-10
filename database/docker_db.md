# Docker Commands for Checking and Initializing movie_reviews_dw

Đây là bản tóm tắt các lệnh Docker để khởi tạo và kiểm tra trạng thái dữ liệu của cơ sở dữ liệu `movie-postgres` trong container `movie-postgres`. Các lệnh này rất quan trọng để quản lý và đảm bảo tính toàn vẹn dữ liệu trong dự án ABSA.

| Command | Function | Sample Output | Where to Run |
|---|---|---|---|
| `docker run --name movie-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -v movie_postgres_data:/var/lib/postgresql/data -d postgres:16` | Tạo một container `movie-postgres` mới với volume persistent để lưu trữ dữ liệu. | Container movie-postgres running, volume movie_postgres_data created | Terminal |
| `docker ps` | Liệt kê các container đang chạy, xác nhận `movie-postgres` đang hoạt động. | movie-postgres with port 5432 | Terminal |
| `docker ps -a` | Liệt kê tất cả các container (đang chạy và đã dừng), xác nhận trạng thái của `movie-postgres`. | movie-postgres with status Exited | Terminal |
| `docker start movie-postgres` | Khởi động container đã dừng `movie-postgres`. | movie-postgres | Terminal |
| `docker volume ls` | Liệt kê các volume, xác nhận volume `movie_postgres_data` tồn tại. | local movie_postgres_data | Terminal |
| `docker volume inspect movie_postgres_data` | Hiển thị thông tin chi tiết về volume (vị trí lưu trữ dữ liệu). | Mountpoint: `/var/lib/docker/volumes/movie_postgres_data/_data` | Terminal |
| `docker exec -it movie-postgres ls -l /var/lib/postgresql/data` | Liệt kê các tệp trong thư mục dữ liệu bên trong container. | base, global, postgresql.conf, etc. | Terminal |
| `docker exec -it movie-postgres psql -U postgres -d movie_reviews_dw` | Mở psql để chạy các lệnh SQL. | psql prompt | Terminal |
| `\l` | Liệt kê tất cả các cơ sở dữ liệu, xác nhận `movie_reviews_dw` tồn tại. | List of databases | psql |
| `\dt` | Liệt kê các bảng trong cơ sở dữ liệu. | movies, review_aspects, reviews_clean, reviews_raw | psql |
| `SELECT count(*) FROM <table>;` | Đếm số lượng bản ghi trong một bảng. | 5250, 0, 360228, 360229 | psql |
| `SELECT * FROM <table> LIMIT 5;` | Xem 5 bản ghi đầu tiên từ một bảng. | 5 rows from reviews_raw | psql |
| `docker logs movie-postgres` | Xem nhật ký container `movie-postgres` để phát hiện lỗi. | PostgreSQL startup logs | Terminal |
| `docker inspect movie-postgres` | Xem cấu hình container (RAM, CPU, volume). | Config with Memory: 4GB, Cpus: 4 | Terminal |
| `docker exec movie-postgres pg_dump -U postgres -d movie_reviews_dw -f /Users/tom/nguyen/movie_reviews_dw_backup.sql` | Sao lưu cơ sở dữ liệu vào một tệp SQL. | File movie_reviews_dw_backup.sql | Terminal |
| `df -h` | Kiểm tra dung lượng đĩa (I/O errors). | 50G available | Terminal |

**Notes:**

* **Data Location:** Dữ liệu được lưu trữ trong volume Docker tại `/var/lib/docker/volumes/movie_postgres_data/_data` trên host.
* **Project Integration:** Các lệnh này đảm bảo cơ sở dữ liệu `movie_reviews_dw` (với các bảng `movies`, `reviews_raw`, `reviews_clean`, và `review_aspects`) đã sẵn sàng cho pipeline ABSA, bao gồm cả dữ liệu từ S3 (`movie-absa-lake`).
* **Next Steps:** Chạy các phân tích ABSA để populate `review_aspects` (hiện đang có 0 rows).