from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup
import os


class IMDBCrawler:
    def __init__(
        self,
        chromedriver_path: str = "D:/NLP/chromedriver-win64/chromedriver-win64/chromedriver.exe",
    ):
        """Initialize the IMDBCrawler with a path to chromedriver."""
        self.chromedriver_path = chromedriver_path
        self.browser = None

    def initialize_chrome_driver(self) -> webdriver.Chrome:
        """Initialize and return a headless Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument("--headless")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        self.browser = webdriver.Chrome(
            service=Service(self.chromedriver_path), options=chrome_options
        )
        return self.browser

    def close_browser(self):
        """Close the browser if it’s open."""
        if self.browser:
            self.browser.quit()
            self.browser = None

    def get_film_list(self, base_url: str, target_films: int = 100) -> dict:
        """Fetch at least target_films films from IMDb by clicking '50 more' until the target is met."""
        self.initialize_chrome_driver()
        self.browser.get(base_url)

        # Wait for the initial list of films to load
        WebDriverWait(self.browser, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "ipc-title-link-wrapper"))
        )

        film_list = {}
        load_count = 0

        while len(film_list) < target_films:
            # Parse current page content
            soup = BeautifulSoup(self.browser.page_source, "lxml")
            poster_cards = soup.find_all("a", class_="ipc-title-link-wrapper")

            new_films_count = 0
            for poster_card in poster_cards:
                title_elem = poster_card.find("h3")
                if title_elem:
                    full_title = title_elem.text.strip()
                    title = full_title.split(".", 1)[1].strip()
                    href_value = "https://www.imdb.com" + poster_card["href"]
                    if title not in film_list:
                        film_list[title] = href_value
                        new_films_count += 1
                        print(f"Title: {title}, Link: {href_value}")
                        if len(film_list) >= target_films:
                            break

            print(
                f"Load {load_count}: Added {new_films_count} new films. Total so far: {len(film_list)}"
            )

            if len(film_list) >= target_films:
                break

            # Try to load more films
            try:
                see_more_button = WebDriverWait(self.browser, 20).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[contains(@class, 'ipc-see-more__button')]")
                    )
                )
                self.browser.execute_script(
                    "arguments[0].scrollIntoView(true);", see_more_button
                )
                self.browser.execute_script("arguments[0].click();", see_more_button)
                print(f"Clicked '50 more' (Load {load_count + 1})...")
                time.sleep(15)  # Wait for content to load

                # Scroll to bottom to ensure all content is loaded
                last_height = self.browser.execute_script(
                    "return document.body.scrollHeight"
                )
                while True:
                    self.browser.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    time.sleep(5)
                    new_height = self.browser.execute_script(
                        "return document.body.scrollHeight"
                    )
                    if new_height == last_height:
                        break
                    last_height = new_height

                load_count += 1
            except Exception as e:
                print(f"Error clicking '50 more' or no more films to load: {e}")
                break

        self.close_browser()
        print(f"Finished crawling. Total films collected: {len(film_list)}")
        return film_list

    def convert_to_review_url(self, movie_url: str) -> str:
        """Convert a movie URL to its reviews URL."""
        base_url = movie_url.split("?")[0] if "?" in movie_url else movie_url
        return base_url + "reviews/"

    def get_reviews(self, url: str) -> list:
        """Fetch reviews for a specific movie from IMDb."""
        self.initialize_chrome_driver()
        self.browser.get(url)
        print(f"Opening URL: {url.split('reviews')[0]}")

        time.sleep(3)

        # Click "See More" to get all reviews
        have_more_reviews = True
        while have_more_reviews:
            try:
                span_element = WebDriverWait(self.browser, 10).until(
                    EC.presence_of_element_located(
                        (By.CLASS_NAME, "chained-see-more-button")
                    )
                )
                button_inside_span = span_element.find_element(
                    By.CLASS_NAME, "ipc-see-more__button"
                )
                self.browser.execute_script("arguments[0].click();", button_inside_span)
                time.sleep(5)
            except:
                have_more_reviews = False
                print("Không tìm thấy nút 'See More', có thể đã tải hết reviews.")

        # Open all spoiler comments
        spoiler_buttons = self.browser.find_elements(
            By.CLASS_NAME, "review-spoiler-button"
        )
        for button in spoiler_buttons:
            if button.is_displayed():
                self.browser.execute_script("arguments[0].click();", button)
                time.sleep(1)

        # Extract HTML after opening spoilers
        soup = BeautifulSoup(self.browser.page_source, "html.parser")
        
        movie_name = soup.find("section", class_="ipc-page-section").find("h2").text.strip()
        # Extract reviews
        reviews = []
        for review_card in soup.find_all("article", class_="user-review-item"):
            # Get review content
            review = review_card.find("div", class_="ipc-list-card__content")
            content_elem = review.find("div", class_="ipc-html-content-inner-div")
            content = content_elem.text.strip() if content_elem else "No Review"

            # Get review score
            score_elem = review.find("span", class_="ipc-rating-star--rating")
            score = score_elem.text.strip() if score_elem else "No Score"

            review_author = review_card.find_all("li", class_="ipc-inline-list__item")
            author = (
                review_author[0].find("a").text.strip()
                if len(review_author) > 0
                else "No Author"
            )
            date = (
                review_author[1].text.strip() if len(review_author) > 1 else None
            )

            reviews.append(
                {
                    "movie_name": movie_name,
                    "review": content,
                    "score": score,
                    "link": url.split("?")[0] if "?" in url else url.split("reviews")[0],
                    "author_name": author,
                    "review_date": date,
                }
            )
            

        self.close_browser()
        return reviews


if __name__ == "__main__":
    crawler = IMDBCrawler()
    # base_url = (
    #     "https://www.imdb.com/search/title/?title_type=feature&sort=num_votes,asc"
    # )
    # film_list = crawler.get_film_list(base_url, target_films=100)
    # print(f"Total films loaded: {len(film_list)}")

    # Example: Get reviews for "Nàng Bạch Tuyết"
    review_url = crawler.convert_to_review_url("https://www.imdb.com/title/tt31806037/")
    reviews = crawler.get_reviews(review_url)
    print(reviews)
    # print(f"Total reviews: {len(reviews)}")
    # for i, review in enumerate(reviews[:3]):
    #     print(f"Review {i+1}: {review}")
