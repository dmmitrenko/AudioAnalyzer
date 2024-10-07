import os
import wave
import numpy as np
import requests
import json
import logging
import deepspeech
import spacy
import tempfile
from textblob import TextBlob
from rabbitmq_dispatcher import RabbitMQDispatcher
from config import RabbitMQConfig
from pydub import AudioSegment

nlp = spacy.load("en_core_web_sm")
MODEL_PATH = 'deepspeech-0.9.3-models.pbmm'
SCORER_PATH = 'deepspeech-0.9.3-models.scorer'
model = deepspeech.Model(MODEL_PATH)
model.enableExternalScorer(SCORER_PATH)
AudioSegment.converter = r'C:\path\to\ffmpeg.exe'

class MessageHandler:
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

    def process_message(self, body):
        try:
            message = body.decode('utf-8')
            data = json.loads(message)
            audio_url = data.get('AudioUrl')
            call_id = data.get('Id')

            if not audio_url:
                raise ValueError("Audio URL is missing in the message")
            
            print(audio_url)
            audio_file_path = self.download_audio(audio_url)
            print(audio_file_path)
            converted_audio_file_path = self.convert_to_wav(audio_file_path)
            text = self.transcribe_audio(converted_audio_file_path)
            emotional_tone = self.analyze_emotion(text)
            location = self.extract_location(text)
            categories = self.get_and_categorize_text(text)

            result = {
                "id": call_id,
                "text": text,
                "emotional_tone": emotional_tone,
                "location": location,
                "categories": categories
            }

            self.dispatcher.send_message("audio.result", result)
            logging.info(f"Processed message successfully: {result}")

        except ValueError as e:
            logging.error(f"Value Error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    @staticmethod
    def download_audio(url):
        try:
            if url.startswith("http://") or url.startswith("https://"):
                response = requests.get(url)
                if response.status_code == 200:
                    file_extension = url.split('.')[-1]
                    file_path = f"audio_file.{file_extension}"
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    return file_path
            elif os.path.exists(url):
                return url
            else:
                raise ValueError("Failed to download audio from URL")
        except requests.RequestException as e:
            logging.error(f"Error downloading audio: {e}")
            raise

    @staticmethod
    def convert_to_wav(file_path):
        try:
            print(f"Attempting to open file at1: {file_path}")
            audio = AudioSegment.from_file(file_path)
            print(f"Attempting to open file at1: {converted_file_path}")
            audio = audio.set_frame_rate(16000).set_channels(1).set_sample_width(2)
            print(f"Attempting to open file at2: {converted_file_path}")

            with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as temp_wav_file:
                print(f"Attempting to open file at3: {converted_file_path}")
                audio.export(temp_wav_file.name, format="wav")
                return temp_wav_file.name
        except Exception as e:
            logging.error(f"Error converting audio: {e}")
            raise
    
    @staticmethod
    def transcribe_audio(file_path):
        with wave.open(file_path, 'rb') as wav:
            audio_data = np.frombuffer(wav.readframes(wav.getnframes()), dtype=np.int16)
            return model.stt(audio_data)

    @staticmethod
    def analyze_emotion(text):
        analysis = TextBlob(text)
        if analysis.sentiment.polarity > 0.2:
            return "Positive"
        elif analysis.sentiment.polarity < -0.2:
            return "Negative"
        elif -0.5 < analysis.sentiment.polarity < -0.2:
            return "Angry"
        else:
            return "Neutral"

    @staticmethod
    def extract_location(text):
        doc = nlp(text)
        for ent in doc.ents:
            if ent.label_ == "GPE":
                return ent.text
        return None

    @staticmethod
    def get_categories():
        try:
            url = "https://localhost:44388/category"
            response = requests.get(url, verify=False)
            if response.status_code == 200:
                categories_data = response.json()
                return {category.get("title"): category.get("points", []) for category in categories_data if category.get("title") and category.get("points")}
            else:
                raise ValueError("Failed to fetch categories from external service")
        except requests.RequestException as e:
            logging.error(f"Error fetching categories: {e}")
            return {}

    def get_and_categorize_text(self, text):
        categories_keywords = self.get_categories()
        detected_categories = set()

        for category, keywords in categories_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text.lower():
                    detected_categories.add(category)

        return list(detected_categories) if detected_categories else ["General Inquiry"]
