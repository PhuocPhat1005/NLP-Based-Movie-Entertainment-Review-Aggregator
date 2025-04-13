from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    TimeoutException,
    ElementNotInteractableException,
    WebDriverException,
)
import time
from bs4 import BeautifulSoup
import json
import os
import validators


class RottenTomatoesCrawler:
    def __init__(
        self,
        chromedriver_path: str = "D:/NLP/chromedriver-win64/chromedriver-win64/chromedriver.exe",
        state_file: str = "rotten_state.json",
    ):
        """Initialize the RottenTomatoesCrawler with chromedriver path and state file."""
        self.chromedriver_path = chromedriver_path
        self.state_file = state_file
        self.browser = None

    def initialize_chrome_driver(self) -> webdriver.Chrome:
        """Initialize and return a Chrome WebDriver."""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        # chrome_options.add_argument('--tls-v1.2')
        # chrome_options.add_argument("--proxy-server=http://138.197.102.119:80")
        # chrome_options.add_argument("--headless")  # Uncomment for headless mode
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
        """Fetch at least target_films films from Rotten Tomatoes by clicking 'Load More' until the target is met."""
        self.initialize_chrome_driver()

        # Load the initial page
        flag = False
        while not flag:
            try:
                self.browser.get(base_url)
                WebDriverWait(self.browser, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "flex-container"))
                )
                flag = True
            except Exception as e:
                print(f"Error loading page: {e}. Retrying in 5 seconds...")
                time.sleep(5)

        # Initialize film list (no state loading)
        film_list = {}
        load_count = 0
        print(
            f"Starting fresh: {len(film_list)} films, clicked 'Load More' {load_count} times."
        )

        while len(film_list) < target_films:
            # Parse current page content
            soup = BeautifulSoup(self.browser.page_source, "lxml")
            poster_cards = soup.find_all("div", class_="flex-container")

            new_films_count = 0
            for poster_card in poster_cards:
                title_elem = poster_card.find("a")
                if title_elem:
                    sentiment_elem = title_elem.find("score-icon-critics")
                    if sentiment_elem and sentiment_elem.get("sentiment") == "empty":
                        print("Skipping empty sentiment film.")
                        continue
                    full_title = title_elem.find("span", class_="p--small").text.strip()
                    href_value = "https://www.rottentomatoes.com" + title_elem["href"]
                    if full_title not in film_list:
                        film_list[full_title] = href_value
                        new_films_count += 1
                        print(f"Title: {full_title}, Link: {href_value}")
                        if len(film_list) >= target_films:
                            break

            print(
                f"Load {load_count}: Added {new_films_count} new films. Total so far: {len(film_list)}"
            )

            if len(film_list) >= target_films:
                break

            # Try to load more films by clicking the "Load More" button
            try:
                load_more_button = WebDriverWait(self.browser, 20).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//button[@data-qa='dlp-load-more-button']")
                    )
                )
                self.browser.execute_script(
                    "arguments[0].scrollIntoView(true);", load_more_button
                )
                self.browser.execute_script("arguments[0].click();", load_more_button)
                print(f"Clicked 'Load More' (Load {load_count + 1})...")
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
                print(f"Error clicking 'Load More' or no more films to load: {e}")
                break

        self.close_browser()
        print(f"Finished crawling. Total films collected: {len(film_list)}")
        return film_list

    def convert_to_review_urls(self, movie_list: list) -> list:
        """Convert movie URLs to their review URLs based on role (critic or user)."""
        res = []
        for href in movie_list:
            res.append({"href": href + "reviews"})
            res.append({"href": href + "reviews?type=user"})
        return res

    def get_reviews(self, review_url: str, role: str = "critic") -> list:
        """Fetch reviews for a specific movie from Rotten Tomatoes."""
        if not validators.url(review_url):
            print(f"Invalid URL provided: {review_url}")
            return []

        self.initialize_chrome_driver()
        print(f"Crawling reviews: {review_url}")

        try:
            # Load the page with retry logic
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    self.browser.get(review_url)
                    WebDriverWait(self.browser, 20).until(
                        EC.presence_of_element_located(
                            (
                                By.CLASS_NAME,
                                "review-row"
                                if role == "critic"
                                else "audience-review-row",
                            )
                        )
                    )
                    break
                except TimeoutException as e:
                    print(
                        f"Timeout loading review page : {e}. Retrying ({attempt + 1}/{max_retries})..."
                    )
                    time.sleep(5)
                except WebDriverException as e:
                    print(
                        f"WebDriver error : {e}. Retrying ({attempt + 1}/{max_retries})..."
                    )
                    time.sleep(5)
            else:
                print(
                    f"Failed to load review page after {max_retries} attempts."
                )
                return []

            # Load more reviews
            previous_review_count = 0
            max_clicks = 50
            click_count = 0

            while click_count < max_clicks:
                try:
                    load_more = WebDriverWait(self.browser, 10).until(
                        EC.element_to_be_clickable(
                            (By.CLASS_NAME, "load-more-container")
                        )
                    )
                    if not load_more.is_displayed() or not load_more.is_enabled():
                        print(f"No more reviews to load.")
                        break

                    ActionChains(self.browser).move_to_element(
                        load_more
                    ).click().perform()
                    time.sleep(5)
                    soup = BeautifulSoup(self.browser.page_source, "lxml")

                    current_review_count = len(
                        soup.find_all(
                            "div",
                            class_="review-row"
                            if role == "critic"
                            else "audience-review-row",
                        )
                    )
                    if current_review_count == previous_review_count:
                        print(
                            f"No new reviews loaded after {click_count + 1} clicks."
                        )
                        break
                    previous_review_count = current_review_count
                    click_count += 1
                except (TimeoutException, ElementNotInteractableException):
                    print(
                        f"No more reviews to loadafter {click_count} clicks."
                    )
                    break

            # Extract reviews
            soup = BeautifulSoup(self.browser.page_source, "lxml")
            movie_name = soup.find("a", class_="sidebar-title").text.strip()
            reviews = []
            review_cards = soup.find_all(
                "div",
                class_="review-row" if role == "critic" else "audience-review-row",
            )

            if not review_cards:
                print(f"No reviews found for '{movie_name}'.")
                return reviews

            for review_card in review_cards:
                review_data = review_card.find(
                    "div",
                    class_="review-data"
                )
                
                user_name = None
                
                try:
                    user_name = (
                        review_data.find(
                            "a",
                            class_="display-name"
                            if role == "critic"
                            else "audience-reviews__name",
                        ).text.strip()
                    ) 
                except AttributeError:
                    user_name = (
                        review_data.find(
                            "span",
                            class_="display-name"
                            if role == "critic"
                            else "audience-reviews__name",
                        )
                    )
                    
                    if user_name:
                        user_name = user_name.text.strip()
                    else:
                        user_name = "No author name"
                        
                sentiment_elem = (
                    review_data.find("score-icon-critics") if review_data else None
                )
                sentiment = (
                    sentiment_elem["sentiment"]
                    if sentiment_elem and "sentiment" in sentiment_elem.attrs
                    else "N/A"
                )
                comment = (
                    review_card.find(
                        "p",
                        class_="review-text"
                        if role == "critic"
                        else "audience-reviews__review",
                    ).text.strip()
                    if review_card.find(
                        "p",
                        class_="review-text"
                        if role == "critic"
                        else "audience-reviews__review",
                    )
                    else "No review text"
                )
                review_date = (
                    review_card.find(
                        "p",
                        class_="original-score-and-url"
                        if role == "critic"
                        else "audience-reviews__duration",
                    ).find("span").text.strip() 
                    if review_card.find(
                        "p",
                        class_="original-score-and-url"
                        if role == "critic"
                        else "audience-reviews__duration",
                    )
                    else None
                )
                review = {
                    "movie_name": movie_name,
                    "author": user_name,
                    "review": comment,
                    "link": review_url.split("reviews")[0],
                    "review_date": review_date,
                    "sentiment": sentiment,
                    "role": role,
                }
                
                print(review)
                reviews.append(review)

        finally:
            self.close_browser()

        return reviews


if __name__ == "__main__":
    crawler = RottenTomatoesCrawler()

    # Test getting film list
    # base_url = "https://www.rottentomatoes.com/browse/movies_at_home/"
    # film_list = crawler.get_film_list(base_url, target_films=100)
    # print(f"Total films loaded: {len(film_list)}")

    # Test getting reviews for "Mickey 17"
    critic_reviews = crawler.get_reviews("https://www.rottentomatoes.com/m/star_wars_episode_iii_revenge_of_the_sith/reviews", role="critic")
    # user_reviews = crawler.get_reviews("https://www.rottentomatoes.com/m/mickey_17/reviews?type=user", role="user")
    # reviews = critic_reviews + user_reviews
    # print(f"Found {len(reviews)} reviews for Mickey 17")
    # for i, review in enumerate(reviews[:3]):
    #     print(f"Review {i+1}: {review}")
