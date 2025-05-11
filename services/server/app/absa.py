import pickle
import numpy as np
import os

clf = None
vectorizer = None

def load_absa_models(nlp_model_path, vectorizer_path):
    """Loads the NLP model and vectorizer from disk."""
    global clf, vectorizer
    try:
        # Assuming models are in the 'models' directory at the project root
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models'))
        nlp_model_full_path = os.path.join(base_dir, nlp_model_path)
        vectorizer_full_path = os.path.join(base_dir, vectorizer_path)

        with open(nlp_model_full_path, 'rb') as f:
            clf = pickle.load(f)
        with open(vectorizer_full_path, 'rb') as f:
            vectorizer = pickle.load(f)
        print("NLP models loaded successfully.")
    except FileNotFoundError as e:
        print(f"Error loading NLP models: {e}. Sentiment analysis will not work.")
        clf = None
        vectorizer = None
    except Exception as e:
        print(f"An error occurred while loading NLP models: {e}")
        clf = None
        vectorizer = None

def analyze_sentiment(text):
    """
    Performs sentiment analysis on a given text.
    Returns smallint: 1 for Positive, -1 for Negative, 0 for Neutral/Unknown.
    Also returns a dummy confidence score (replace with actual if model provides).
    """
    if clf is None or vectorizer is None:
        print("Sentiment models not loaded.")
        return 0, 0.0 # Return 0 for sentiment and 0.0 for confidence

    if not text:
        return 0, 0.0 # Handle empty text

    try:
        text_vector = vectorizer.transform([text])
        prediction = clf.predict(text_vector)
        # Assuming binary classification (0 or 1)
        # Map 0/1 to smallint: Assuming 1 is Positive, 0 is Negative based on common use
        # You might need to verify how your model outputs map to your DB schema's smallint
        sentiment_int = 1 if prediction[0] > 0 else -1 # Map 1 -> 1, 0 -> -1
        # If your model can output neutral, you need more logic here

        # Placeholder for confidence - replace with actual confidence score if available
        confidence = 1.0 # Dummy confidence

        return sentiment_int, confidence # Return sentiment as smallint and confidence

    except Exception as e:
        print(f"Error during sentiment analysis for text: '{text[:50]}...': {e}")
        return 0, 0.0 # Return 0 for sentiment and 0.0 for confidence on error

# In a real ABSA scenario:
# You would have a function that takes raw_text, extracts aspects,
# segments text per aspect, and calls analyze_sentiment for each segment.
# process_review_for_absa(raw_text, review_id, movie_id) -> list of {'review_id': id, 'aspect': '...', 'sentiment_int': ..., 'confidence': ...}