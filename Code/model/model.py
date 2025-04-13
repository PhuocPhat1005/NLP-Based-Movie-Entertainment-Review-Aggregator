import json
import pandas as pd
from sklearn.model_selection import train_test_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TrainingArguments, Trainer
from datasets import Dataset
import torch
import tensorflow as tf
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
import spacy
import string
import subprocess

def ensure_nltk_resources():
    try:
        nltk.download('stopwords', quiet=True)
        nltk.download('punkt', quiet=True)
        nltk.download('punkt_tab', quiet=True)
    except Exception as e:
        print(f"Error when install nltk: {e}")
        raise

ensure_nltk_resources()

def load_spacy_model(model_name="en_core_web_sm"):
    try:
        return spacy.load(model_name)
    except OSError:
        print(f"{model_name} has not been installed. Installing...")
        subprocess.run(["python", "-m", "spacy", "download", model_name], check=True)
        return spacy.load(model_name)

# Download NLTK resources
nlp = load_spacy_model("en_core_web_sm")
print("Spacy Model has been downloaded successfully!")

# Predefined aspect lexicon for ABSA
ASPECT_LEXICON = {
    "Direction": ["director", "directing", "direction", "filmmaking", "vision"],
    # "Music": ["music", "soundtrack", "score", "sound", "audio"],
    # "Script": ["script", "writing", "dialogue", "screenplay", "storyline"],
    "Acting": ["acting", "actor", "actress", "performance", "cast"],
    "Plot": ["plot", "story", "narrative", "structure", "twist"],
    "Overall": ["movie", "film", "overall", "general", "experience"],
    "Visuals": ["visuals", "cinematography", "effects", "visual", "imagery"],
    "Themes": ["themes", "message", "theme", "meaning", "subtext"],
    "Pacing": ["pacing", "rhythm", "tempo", "flow", "speed"]
}

class TextPreprocessor:
    """
    A class to preprocess text data for training a machine learning model.
    """
    
    def __init__(self):
        """
        Initialize the TextPreprocessor with necessary tools.
        """
        self.stop_words = set(stopwords.words('english'))
        
    def preprocess_text(self, text, keep_stopwords=True):
        """
        Preprocess text: clean and optionally remove stop words.
        
        Args:
            text (str): Text to preprocess.
            keep_stopwords (bool): Whether to keep stop words (recommended for transformers).
            
        Returns:
            str: Preprocessed text.
        """
        # Convert to lowercase
        text = text.lower()
        
        # Ignore html tags
        text = re.sub(r'<[^>]+>', '', text) 
        
        # Word tokenization
        tokens = word_tokenize(text)
        
        # If keep_stopwords is False, remove stop words and punctuation
        if not keep_stopwords:
            tokens = [token for token in tokens if token not in self.stop_words and token not in string.punctuation]
        else:
            tokens = [token for token in tokens if token not in string.punctuation]
        
        # Convert tokens back to string
        return ' '.join(tokens)

class ABSAProcessor:
    """
    A class to handle Aspect-Based Sentiment Analysis (ABSA) using a pre-trained transformer model.
    """

    def __init__(self, model_name="yangheng/deberta-v3-base-absa-v1.1", max_length=128, test_size=0.2, random_state=42):
        """
        Initialize the class with configuration parameters.

        Args:
            model_name (str): Name of the ABSA model from Hugging Face.
            max_length (int): Maximum sequence length after tokenization.
            test_size (float): Proportion of the test set when splitting data.
            random_state (int): Seed for reproducibility.
        """
        self.model_name = model_name
        self.max_length = max_length
        self.test_size = test_size
        self.random_state = random_state
        self.tokenizer = None
        self.model = None
        self.preprocessor = TextPreprocessor()
        self.aspects = ['Acting', 'Plot', 'Direction', 'Visuals', 'Themes', 'Pacing', 'Overall']
        self.label_mapping = {'Positive': 2, 'Negative': 0, 'Neutral': 1}
        self.label_mapping_reverse = {2: 'Positive', 0: 'Negative', 1: 'Neutral'}

    def extract_aspects(self, review):
        """
        Extract aspects mentioned in a review using a predefined lexicon.
        
        Args:
            review (str): Review text.
            
        Returns:
            set: Set of aspects mentioned in the review.
        """
        # Preprocess the review text
        processed_review = self.preprocessor.preprocess_text(review, keep_stopwords=True)
        
        # Use Spacy for tokenization and named entity recognition
        doc = nlp(processed_review)
        
        mentioned_aspects = set()
        
        # Iterate through tokens and check for aspect keywords
        for token in doc:
            for aspect, keywords in ASPECT_LEXICON.items():
                if token.text in keywords:
                    # Normalize aspect names
                    normalized_aspect = aspect if aspect != "Movie" else "Overall"
                    normalized_aspect = "Direction" if aspect == "Directing" else normalized_aspect
                    if normalized_aspect in self.aspects:
                        mentioned_aspects.add(normalized_aspect)
                    break
        
        return mentioned_aspects

    def load_data(self, filepath):
        """
        Load data from a JSON file, preprocess the text, and prepare it for ABSA.

        Args:
            filepath (str): Path to the JSON file containing the data.

        Returns:
            pd.DataFrame: DataFrame containing the prepared ABSA data.
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            if "reviews" in json_data:
                data = json_data["reviews"]
            else:
                data = json_data

        df = pd.DataFrame(data)

        df['review'] = df['review'].apply(lambda x: self.preprocessor.preprocess_text(x, keep_stopwords=True))

        # Split the aspect sentiment into separate columns
        aspect_sentiment_df = pd.DataFrame(df['aspect_sentiment'].tolist())
        df = pd.concat([df[['review']], aspect_sentiment_df], axis=1)

        # Tạo danh sách dữ liệu cho từng khía cạnh
        data_samples = []
        for idx, row in df.iterrows():
            review_text = row['review']
            # Trích xuất các khía cạnh được đề cập trong đánh giá
            mentioned_aspects = self.extract_aspects(review_text)
            
            # Chỉ thêm dữ liệu cho các khía cạnh được đề cập
            for aspect in self.aspects:
                sentiment = row[aspect]
                if sentiment is not None and aspect in mentioned_aspects:  # Chỉ lấy các khía cạnh được đề cập và có giá trị
                    data_samples.append({
                        'text': f"{review_text} [SEP] {aspect}",
                        'label': sentiment
                    })

        # Chuyển thành DataFrame
        absa_df = pd.DataFrame(data_samples)

        # Mã hóa nhãn
        absa_df['label'] = absa_df['label'].map(self.label_mapping)
        return absa_df

    def prepare_datasets(self, absa_df):
        """
        Split data into training and test sets, then tokenize and format the data.

        Args:
            absa_df (pd.DataFrame): DataFrame containing ABSA data.

        Returns:
            tuple: (train_dataset, test_dataset)
        """
        # Chia dữ liệu
        train_df, test_df = train_test_split(absa_df, test_size=self.test_size, random_state=self.random_state)

        # Chuyển DataFrame thành Dataset
        train_dataset = Dataset.from_pandas(train_df)
        test_dataset = Dataset.from_pandas(test_df)

        # Tokenize dữ liệu
        def tokenize_function(examples):
            return self.tokenizer(examples['text'], padding="max_length", truncation=True, max_length=self.max_length)

        train_dataset = train_dataset.map(tokenize_function, batched=True)
        test_dataset = test_dataset.map(tokenize_function, batched=True)

        # Định dạng dữ liệu cho PyTorch
        train_dataset = train_dataset.rename_column("label", "labels")
        test_dataset = test_dataset.rename_column("label", "labels")
        train_dataset.set_format("torch", columns=["input_ids", "attention_mask", "labels"])
        test_dataset.set_format("torch", columns=["input_ids", "attention_mask", "labels"])

        return train_dataset, test_dataset

    def initialize_model(self):
        """
        Load the tokenizer and model from Hugging Face.
        """
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name, num_labels=3)

    def train_model(self, train_dataset, test_dataset, num_epochs=3, learning_rate=2e-5):
        """
        Train the ABSA model.

        Args:
            train_dataset (Dataset): Training dataset.
            test_dataset (Dataset): Test dataset.
            num_epochs (int): Number of training epochs.
            learning_rate (float): Learning rate for training.

        Returns:
            dict: Evaluation results after training.
        """
        # Thiết lập tham số huấn luyện
        training_args = TrainingArguments(
            output_dir="./results",
            eval_strategy="epoch",
            learning_rate=learning_rate,
            per_device_train_batch_size=16,
            per_device_eval_batch_size=16,
            num_train_epochs=num_epochs,
            weight_decay=0.01,
            logging_dir='./logs',
            logging_steps=10,
        )

        # Khởi tạo Trainer
        trainer = Trainer(
            model=self.model,
            args=training_args,
            train_dataset=train_dataset,
            eval_dataset=test_dataset,
        )

        # Huấn luyện mô hình
        trainer.train()

        # Đánh giá mô hình
        eval_results = trainer.evaluate()
        print("Evaluation Results:", eval_results)
        return eval_results

    def continue_training(self, filepath, num_epochs=3, learning_rate=2e-5, load_path="./absa_model"):
        """
        Continue training the model on new data.

        Args:
            filepath (str): Path to the JSON file containing new data.
            num_epochs (int): Number of additional epochs to train.
            learning_rate (float): Learning rate for continued training.
            load_path (str): Path to the saved model to load.

        Returns:
            dict: Evaluation results after continued training.
        """
        # Tải mô hình đã huấn luyện
        self.load_model(load_path=load_path)
        print("Loaded previously trained model for continued training.")

        # Đọc và chuẩn bị dữ liệu mới
        absa_df = self.load_data(filepath)
        print("New data prepared successfully.")

        # Chuẩn bị dataset
        train_dataset, test_dataset = self.prepare_datasets(absa_df)
        print("Datasets prepared successfully.")

        # Tiếp tục huấn luyện mô hình
        eval_results = self.train_model(train_dataset, test_dataset, num_epochs=num_epochs, learning_rate=learning_rate)

        # Lưu mô hình sau khi huấn luyện tiếp
        self.save_model(save_path=load_path)
        print("Model saved after continued training.")

        return eval_results

    def predict_sentiment(self, review, aspect):
        """
        Predict sentiment for a single aspect based on the review text.

        Args:
            review (str): Review text.
            aspect (str): Aspect to predict (Acting, Plot, etc.).

        Returns:
            str: Predicted sentiment (Positive, Negative, Neutral).
        """
        # Tiền xử lý văn bản trước khi dự đoán
        preprocessed_review = self.preprocessor.preprocess_text(review, keep_stopwords=True)

        # Chuẩn bị đầu vào
        input_text = f"{preprocessed_review} [SEP] {aspect}"
        inputs = self.tokenizer(input_text, return_tensors="pt", padding=True, truncation=True, max_length=self.max_length)

        # Đưa mô hình vào chế độ đánh giá
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits
            predicted_label = torch.argmax(logits, dim=1).item()

        # Chuyển đổi nhãn số thành cảm xúc
        return self.label_mapping_reverse[predicted_label]

    def predict_all_aspects(self, review, filter_mentioned_aspects=False):
        """
        Predict sentiment for all aspects of a review, optionally filtering by mentioned aspects.

        Args:
            review (str): Review text.
            filter_mentioned_aspects (bool): If True, only predict for aspects mentioned in the review.

        Returns:
            dict: Dictionary with aspects as keys and predicted sentiments as values.
        """
        # Tiền xử lý văn bản
        preprocessed_review = self.preprocessor.preprocess_text(review, keep_stopwords=True)

        # Lọc các khía cạnh được đề cập (nếu bật tùy chọn)
        if filter_mentioned_aspects:
            mentioned_aspects = self.extract_aspects(review)
            if not mentioned_aspects:  # Nếu không tìm thấy khía cạnh nào, dự đoán cho tất cả
                mentioned_aspects = self.aspects
        else:
            mentioned_aspects = self.aspects

        # Dự đoán cho các khía cạnh được chọn
        predictions = {}
        for aspect in mentioned_aspects:
            sentiment = self.predict_sentiment(preprocessed_review, aspect)
            predictions[aspect] = sentiment

        return predictions

    def save_model(self, save_path="./absa_model"):
        """
        Save the model and tokenizer.

        Args:
            save_path (str): Path to save the model.
        """
        self.model.save_pretrained(save_path)
        self.tokenizer.save_pretrained(save_path)

    def load_model(self, load_path="./absa_model"):
        """
        Load the model and tokenizer from a saved path.

        Args:
            load_path (str): Path containing the saved model.
        """
        self.model = AutoModelForSequenceClassification.from_pretrained(load_path)
        self.tokenizer = AutoTokenizer.from_pretrained(load_path)

    def run_pipeline(self, filepath):
        """
        Run the entire pipeline: load data, preprocess, prepare, train, and save the model.

        Args:
            filepath (str): Path to the JSON file containing the data.

        Returns:
            dict: Evaluation results after training.
        """
        # Đọc và chuẩn bị dữ liệu
        absa_df = self.load_data(filepath)
        print("Data prepared successfully.")

        # Khởi tạo mô hình
        self.initialize_model()
        print("Model initialized successfully.")

        # Chuẩn bị dataset
        train_dataset, test_dataset = self.prepare_datasets(absa_df)
        print("Datasets prepared successfully.")

        # Huấn luyện mô hình
        eval_results = self.train_model(train_dataset, test_dataset)

        # Lưu mô hình
        self.save_model()
        print("Model saved successfully.")

        return eval_results

# Sử dụng class
if __name__ == "__main__":
    # Khởi tạo đối tượng ABSAProcessor
    absa_processor = ABSAProcessor(
        model_name="yangheng/deberta-v3-base-absa-v1.1",
        max_length=128,
        test_size=0.2,
        random_state=42
    )

    absa_processor.load_model(load_path="./absa_model")
    print("Model loaded successfully.")
    
    # absa_processor.initialize_model()
    # You can change the path to your JSON file
    absa_processor.run_pipeline(filepath="D:\\NLP\\Final_Project\\NLP-Based-Movie-Entertainment-Review-Aggregator\\Code\\aspect_sentiment_reviews.json")

    review = "Disney's live-action Snow White is finally here, and after watching it, I can confidently say that the Magic Mirror needs a new prescription. This movie is less ""fairest of them all"" and more ""fairest at failing upwards."" If you ever wondered what it would look like if someone took the classic 1937 animated masterpiece, ran it through a corporate buzzword generator, and then tossed in some awkward CGI for good measure-congratulations, you've found your answer!A Princess with a Personality (Kind Of?)Rachel Zegler takes on the role of Snow White, though you'd be forgiven for thinking she was actually playing a medieval TED Talk speaker. Instead of the wide-eyed, kind-hearted princess we all knew, this Snow White is a Strong Independent Woman™-because saying ""I want to be a queen, not a bride"" apparently counts as character development these days. That's right, folks, forget charming dwarfs, woodland creatures, or actual chemistry with anyone; this Snow White has dreams, and she's here to remind you about them every five minutes.But despite all that, her biggest challenge in the movie isn't even the Evil Queen-it's keeping the audience awake.Gal Gadot: Evil Queen or Instagram Influencer?Now, let's talk about Gal Gadot's Evil Queen. You'd think that playing a narcissistic, beauty-obsessed villain would be an easy fit for Hollywood, but somehow, even with all the pouting, dress-swishing, and over-the-top glowering, she ends up about as menacing as a fashion blogger with a bad attitude.Her obsession with being ""the fairest of them all"" is laughable when you realize that, well... she already is the fairest of them all. No offense to Snow White, but if the mirror told me that Gal Gadot wasn't the hottest person in the kingdom, I'd be smashing that thing to pieces too. The real villain here is the mirror's manufacturer.The Dwarfs: Now with 90% Less Dwarfs!Ah yes, the seven dwarfs-except, surprise! They're not really dwarfs anymore. Instead, we get a diverse group of CGI-enhanced ""magical creatures"" who look like they were rejected from The Lord of the Rings for being too unsettling. Imagine if a bunch of carnival performers got stuck in a blender with bad CGI, and you've got these guys.Their role in the movie? Mostly to exist, deliver unfunny one-liners, and make you wonder if we should start a petition to bring back actual actors instead of whatever motion-capture madness this is. If I wanted to spend two hours looking at weirdly animated characters, I'd just play a bad video game.The Apple FiascoWe all know the story: Evil Queen poisons apple, Snow White eats apple, Snow White falls into a death nap, and then a prince shows up to wake her with true love's kiss. Simple, right? Nope. Not in this version.Instead, we get a long, drawn-out scene where Snow White almost eats the apple, but then stops to give another speech about believing in yourself or some nonsense. And when she does finally bite it, the whole moment is ruined by some weird slow-motion effects that make it look like an overly dramatic shampoo commercial.Honestly, I was rooting for the apple at that point. Maybe if she stayed in an enchanted coma, we'd all be spared another unnecessary Disney remake.Final Verdict: The Fairest Disaster of Them AllThis movie is what happens when you take a beloved classic, strip away everything that made it charming, and replace it with corporate-approved ""modernization"" that pleases no one. It's a film that wants to be empowering, but instead feels like a checklist of forced inclusivity and soulless spectacle.Snow White (2025) is proof that sometimes, it's better to leave well enough alone. If you're looking for magic, wonder, and nostalgia, just rewatch the 1937 version. If you're looking for two hours of your life you'll never get back, then by all means, go ahead and buy a ticket."
    predictions = absa_processor.predict_all_aspects(review, filter_mentioned_aspects=True)
    
    print(f"\nReview: {review}")
    print("Predicted Sentiments for Mentioned Aspects:")
    for aspect, sentiment in predictions.items():
        print(f"{aspect}: {sentiment}")