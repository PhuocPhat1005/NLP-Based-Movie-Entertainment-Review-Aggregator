from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup
import json
import os


class MetacriticCrawler:
    def __init__(
        self,
        chromedriver_path: str = "D:/NLP/chromedriver-win64/chromedriver-win64/chromedriver.exe",
        state_file: str = "metacritic_state.json",
    ):
        """Initialize the MetacriticCrawler with chromedriver path and state file."""
        self.chromedriver_path = chromedriver_path
        self.state_file = state_file
        self.browser = None

    def initialize_chrome_driver(self) -> webdriver.Chrome:
        """Initialize and return a Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--headless")  # Uncomment if you want headless mode
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument("--allow-insecure-localhost")

        driver = webdriver.Chrome(
            service=Service(self.chromedriver_path), options=chrome_options
        )
        driver.set_page_load_timeout(300)
        self.browser = driver
        return self.browser

    def close_browser(self):
        """Close the browser if it’s open."""
        if self.browser:
            self.browser.quit()
            self.browser = None

    def get_movie_list(
        self, base_url: str, start_page: int = 1, min_movies: int = 100
    ) -> dict:
        """Fetch a list of movies from Metacritic until at least min_movies are collected."""
        movie_list = {}
        page = start_page

        while len(movie_list) < min_movies:
            self.initialize_chrome_driver()
            url = base_url + str(page)
            print(f"Crawling page {page}: {url}")

            try:
                self.browser.get(url)
                WebDriverWait(self.browser, 20).until(
                    EC.presence_of_element_located(
                        (By.CLASS_NAME, "c-finderProductCard_container")
                    )
                )
            except Exception as e:
                print(f"Error loading page {page}: {e}. Retrying in 10 seconds...")
                time.sleep(10)
                self.browser.get(url)
                WebDriverWait(self.browser, 20).until(
                    EC.presence_of_element_located(
                        (By.CLASS_NAME, "c-finderProductCard_container")
                    )
                )

            soup = BeautifulSoup(self.browser.page_source, "lxml")
            poster_cards = soup.find_all("a", class_="c-finderProductCard_container")

            new_movies_count = 0
            for poster_card in poster_cards:
                if poster_card and poster_card.find(
                    "div", class_="c-siteReviewScore_background"
                ):
                    href_value = "https://www.metacritic.com" + poster_card["href"]
                    title_elem = poster_card.find("h3")
                    if title_elem:
                        spans = title_elem.find_all("span")
                        if len(spans) > 1:
                            title = spans[1].text.strip()
                        else:
                            title = spans[0].text.strip()
                        if title not in movie_list:  # Avoid duplicates
                            movie_list[title] = href_value
                            new_movies_count += 1
                            print(f"Title: {title}, Link: {href_value}")
                            if(len(movie_list) >= min_movies):
                                break

            self.close_browser()
            print(
                f"Found {new_movies_count} new movies on page {page}. Total so far: {len(movie_list)}"
            )

            if new_movies_count == 0:  # No new movies found, end of list
                print(f"No new movies found on page {page}. Stopping crawl.")
                break

            page += 1  # Move to the next page

        print(f"Finished crawling. Total movies collected: {len(movie_list)}")
        return movie_list

    def convert_to_review_urls(self, movie_list: list) -> list:
        """Convert movie URLs to their critic review URLs."""
        res = []
        for href in movie_list:
            res.append({"href": href + "critic-reviews/"})
            res.append({"href": href + "user-reviews/"})
                
        return res

    def get_reviews(self, review_url: str, role: str) -> list:
        """Fetch critic reviews for a specific movie from Metacritic."""
        self.initialize_chrome_driver()

        word_split = "critic-reviews" if role == "critic" else "user-reviews"
        movie_url = review_url.split(word_split)[0]
        print(f"Crawling reviews for: {movie_url}")

        try:
            self.browser.get(review_url)
            WebDriverWait(self.browser, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "c-siteReview_main"))
            )
        except Exception as e:
            if WebDriverWait(self.browser, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "c-pageProductReviews_message"))
            ):
                print(f"There are no {role} reviews yet")
                self.close_browser()
                return []

        soup = BeautifulSoup(self.browser.page_source, "lxml")
        movie_name = soup.find("a", class_="c-productSubpageHeader_back").text.strip()
        reviews = []
        review_cards = soup.find_all("div", class_="c-siteReview")

        if not review_cards:
            print(f"No reviews found for '{movie_name}'.")
            self.close_browser()
            return reviews

        for review_card in review_cards:
            body = review_card.find("div", class_="c-siteReview_main")
            score_card = body.find("div", class_="c-siteReviewHeader_reviewScore")
            score = score_card.find("span").text.strip() if score_card else "N/A"

            comment_card = body.find("div", class_="c-siteReview_quote")
            comment = (
                comment_card.find("span").text.strip()
                if comment_card
                else "No review text"
            )

            date = body.find("div", class_="c-siteReviewHeader").find("div", class_="c-siteReviewHeader_reviewDate").text.strip()
            
            try:
                author = review_card.find(
                    "a", class_="c-siteReview_criticName" if role == "critic" else "c-siteReviewHeader_username"
                ).text.strip()[3:]
            except AttributeError:
                author = review_card.find(
                    "span", class_="c-siteReview_criticName" if role == "critic" else "c-siteReviewHeader_username"
                ).text.strip()[3:]
                
            review = {
                "movie_name": movie_name,
                "review": comment,
                "score": score,
                "link": movie_url,
                "author_name": author,
                "review_date": date if date else None,
                "role": role,
            }
            reviews.append(review)
            print(
                f"Review for '{movie_name}': Score={score}, Comment={comment}, Link={review_url}, Author={author}, Date={date}"
            )

        self.close_browser()
        return reviews


if __name__ == "__main__":
    crawler = MetacriticCrawler()
    # base_url = "https://www.metacritic.com/browse/movie/all/all/all-time/new/?releaseYearMin=1910&releaseYearMax=2025&page="

    # movie_list = {}

    # # Crawl one page for testing
    # new_movies = crawler.get_movie_list(base_url, 1)
    # movie_list.update(new_movies)

    review_links = crawler.convert_to_review_urls(["https://www.metacritic.com/movie/army-of-shadows/"])

    print(review_links)
    reviews = []
    for item in review_links:
        role = "critic" if "critic-reviews" in item["href"] else "user"
        print("Role" + role)
        movie_reviews = crawler.get_reviews(item["href"], role)
        reviews.extend(movie_reviews)

    print(f"Total reviews collected: {len(reviews)}")
    for i, review in enumerate(reviews[:3]):
        print(f"Review {i+1}: {review}")
