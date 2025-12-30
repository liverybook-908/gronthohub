from google.api_core import exceptions
import google.generativeai as genai
import json
import time
import random
import os
import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from internetarchive import search_items
import firebase_admin
from firebase_admin import credentials, firestore
from tenacity import retry, stop_after_attempt, wait_exponential
from dataclasses import dataclass, asdict
import unicodedata

# ================================
# লগিং কনফিগারেশন
# ================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('book_hunter.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================================
# ইউটিলিটি ফাংশন
# ================================
class SlugGenerator:
    """SEO-বান্ধব স্লাগ তৈরি করার ক্লাস"""
    
    # বাংলা থেকে ইংরেজি ট্রান্সলিটারেশন ম্যাপিং
    BANGLA_TO_ENGLISH = {
        'অ': 'o', 'আ': 'a', 'ই': 'i', 'ঈ': 'ee', 'উ': 'u', 'ঊ': 'uu',
        'ঋ': 'ri', 'এ': 'e', 'ঐ': 'oi', 'ও': 'o', 'ঔ': 'ou',
        'ক': 'k', 'খ': 'kh', 'গ': 'g', 'ঘ': 'gh', 'ঙ': 'ng',
        'চ': 'ch', 'ছ': 'chh', 'জ': 'j', 'ঝ': 'jh', 'ঞ': 'ny',
        'ট': 't', 'ঠ': 'th', 'ড': 'd', 'ঢ': 'dh', 'ণ': 'n',
        'ত': 't', 'থ': 'th', 'দ': 'd', 'ধ': 'dh', 'ন': 'n',
        'প': 'p', 'ফ': 'ph', 'ব': 'b', 'ভ': 'bh', 'ম': 'm',
        'য': 'z', 'র': 'r', 'ল': 'l', 'শ': 'sh', 'ষ': 'sh',
        'স': 's', 'হ': 'h', 'ড়': 'r', 'ঢ়': 'rh', 'য়': 'y',
        'ৎ': 't', 'ং': 'ng', 'ঃ': 'h', 'ঁ': '',
        'া': 'a', 'ি': 'i', 'ী': 'ee', 'ু': 'u', 'ূ': 'uu',
        'ৃ': 'ri', 'ে': 'e', 'ৈ': 'oi', 'ো': 'o', 'ৌ': 'ou',
        '্': '', 'ৰ': 'r', 'ৱ': 'w', 'ৗ': 'ou'
    }
    
    @classmethod
    def create_slug(cls, text: str) -> str:
        """
        বাংলা/ইংরেজি টেক্সট থেকে SEO-বান্ধব স্লাগ তৈরি
        
        উদাহরণ:
        "পথের পাঁচালী PDF" -> "pother-panchali-pdf"
        "Feluda Series বই" -> "feluda-series-boi"
        """
        if not text:
            return "untitled-book"
        
        # লোয়ারকেস
        text = text.lower().strip()
        
        # বাংলা সংখ্যা ইংরেজিতে কনভার্ট
        bangla_digits = '০১২৩৪৫৬৭৮৯'
        english_digits = '0123456789'
        for b, e in zip(bangla_digits, english_digits):
            text = text.replace(b, e)
        
        # বাংলা অক্ষর ট্রান্সলিটারেট করা
        result = []
        for char in text:
            if char in cls.BANGLA_TO_ENGLISH:
                result.append(cls.BANGLA_TO_ENGLISH[char])
            elif char.isalnum() or char in ['-', '_']:
                result.append(char)
            elif char.isspace():
                result.append('-')
        
        slug = ''.join(result)
        
        # একাধিক হাইফেন রিমুভ
        slug = re.sub(r'-+', '-', slug)
        
        # শুরু এবং শেষের হাইফেন রিমুভ
        slug = slug.strip('-')
        
        # খুব লম্বা স্লাগ কাট
        if len(slug) > 100:
            slug = slug[:100].rsplit('-', 1)[0]
        
        return slug if slug else "bangla-book"
    
    @classmethod
    def validate_and_fix_slug(cls, slug: str) -> str:
        """Gemini দেওয়া স্লাগ ভ্যালিডেট এবং ফিক্স করা"""
        # যদি বাংলা অক্ষর থাকে বা invalid হয়
        if not slug or re.search(r'[\u0980-\u09FF]', slug):
            return ""  # খালি রিটার্ন করলে নতুন করে তৈরি হবে
        
        # স্ট্যান্ডার্ড ক্লিনআপ
        slug = slug.lower().strip()
        slug = re.sub(r'[^a-z0-9-]', '-', slug)
        slug = re.sub(r'-+', '-', slug)
        slug = slug.strip('-')
        
        return slug if len(slug) > 3 else ""

# ================================
# ডাটা মডেল
# ================================
@dataclass
class BookMetadata:
    id: str
    title: str
    author: str
    url: str
    downloads: int
    
@dataclass
class SEOContent:
    bangla_title: str
    slug: str
    meta_desc: str
    category: str
    summary: str
    tags: List[str]
    archive_id: str = ""
    download_url: str = ""
    publish_at: Optional[datetime] = None
    status: str = "draft"
    created_at: Optional[datetime] = None

# ================================
# কনফিগারেশন ক্লাস
# ================================
class Config:
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    FIREBASE_KEYS_JSON = os.getenv("FIREBASE_KEYS")
    MAX_BOOKS_PER_RUN = 2
    MIN_REVIEW_LENGTH = 300
    MAX_RETRIES = 3
    RATE_LIMIT_DELAY = 30
    PUBLISH_DELAY_MIN_HOURS = 3
    PUBLISH_DELAY_MAX_HOURS = 12
    
    # Gemini Model Selection
    GEMINI_MODEL = "gemini-exp-1206"  # Latest experimental model
    # অন্যান্য অপশন: "gemini-2.0-flash-exp", "gemini-1.5-pro"
    
    @classmethod
    def validate(cls):
        """Environment variables ভ্যালিডেশন"""
        if not cls.GEMINI_API_KEY:
            raise ValueError("❌ GEMINI_API_KEY পাওয়া যায়নি!")
        if not cls.FIREBASE_KEYS_JSON:
            raise ValueError("❌ FIREBASE_KEYS পাওয়া যায়নি!")
        
        logger.info(f"✅ ব্যবহৃত Gemini Model: {cls.GEMINI_MODEL}")

# ================================
# Firebase Manager
# ================================
class FirebaseManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Firebase initialization - Fix for Certificate Error"""
        try:
            # গিটহাব সিক্রেট থেকে ডাটা নেওয়া
            firebase_raw = os.getenv("FIREBASE_KEYS")
            
            if not firebase_raw:
                logger.error("❌ FIREBASE_KEYS এনভায়রনমেন্ট ভেরিয়েবল খুঁজে পাওয়া যায়নি!")
                return

            # স্ট্রিং থেকে ডিকশনারিতে কনভার্ট করা
            key_data = json.loads(firebase_raw)

            if not firebase_admin._apps:
                # সরাসরি ডিকশনারি ব্যবহার করে ফায়ারবেস কানেক্ট করা (ফাইলের দরকার নেই)
                cred = credentials.Certificate(key_data)
                firebase_admin.initialize_app(cred)
            
            self.db = firestore.client()
            logger.info("✅ Firebase সফলভাবে কানেক্ট হয়েছে")
        except json.JSONDecodeError:
            logger.error("❌ FIREBASE_KEYS এর ফরম্যাট সঠিক নয় (JSON Error)!")
        except Exception as e:
            logger.error(f"❌ Firebase initialization error: {e}")
    
    def get_processed_book_ids(self) -> List[str]:
        """ডাটাবেজ থেকে প্রসেসড বই আইডি নেওয়া"""
        try:
            docs = self.db.collection('books').stream()
            return [doc.id for doc in docs]
        except Exception as e:
            logger.error(f"❌ প্রসেসড আইডি নিতে সমস্যা: {e}")
            return []
    
    def check_slug_exists(self, slug: str) -> bool:
        """স্লাগ ডুপ্লিকেট চেক করা"""
        try:
            docs = self.db.collection('books').where('slug', '==', slug).limit(1).stream()
            return len(list(docs)) > 0
        except Exception as e:
            logger.error(f"❌ স্লাগ চেক করতে সমস্যা: {e}")
            return False
    
    def save_book(self, book_id: str, data: Dict) -> bool:
        """Firestore-এ বই সেভ করা"""
        try:
            # Firestore timestamp-এ datetime কনভার্ট
            if 'created_at' in data and data['created_at']:
                data['created_at'] = firestore.SERVER_TIMESTAMP
            if 'publish_at' in data and isinstance(data['publish_at'], datetime):
                # datetime অবজেক্ট সরাসরি Firestore সাপোর্ট করে
                pass
            
            self.db.collection('books').document(book_id).set(data)
            logger.info(f"✅ সেভ সফল: {book_id}")
            return True
        except Exception as e:
            logger.error(f"❌ সেভ করতে ব্যর্থ {book_id}: {e}")
            return False

# ================================
# Archive.org Book Fetcher
# ================================
class ArchiveFetcher:
    @staticmethod
    def fetch_trending_books(limit: int = 10) -> List[BookMetadata]:
        """Archive.org থেকে ট্রেন্ডিং বাংলা বই খোঁজা"""
        logger.info("🔍 Archive.org থেকে ট্রেন্ডিং বই খুঁজছি...")
        
        query = 'language:bengali AND mediatype:texts'
        found_books = []
        
        try:
            results = search_items(query)
            firebase_manager = FirebaseManager()
            processed_ids = firebase_manager.get_processed_book_ids()
            
            for item in results.iter_as_items():
                if item.identifier in processed_ids:
                    continue
                
                metadata = item.metadata
                title = metadata.get('title', 'Unknown Title').split(':')[0].strip()
                author = metadata.get('creator', 'অজানা লেখক')
                
                if isinstance(author, list):
                    author = author[0]
                
                book = BookMetadata(
                    id=item.identifier,
                    title=title,
                    author=author,
                    url=f"https://archive.org/download/{item.identifier}/{item.identifier}.pdf",
                    downloads=int(metadata.get('downloads', 0))
                )
                
                found_books.append(book)
                
                if len(found_books) >= limit:
                    break
            
            logger.info(f"✅ {len(found_books)}টি ট্রেন্ডিং বই পাওয়া গেছে")
            return found_books
            
        except Exception as e:
            logger.error(f"❌ Archive থেকে বই নিতে সমস্যা: {e}")
            return []

# ================================
# Gemini Content Generator
# ================================
class GeminiGenerator:
    def __init__(self):
        genai.configure(api_key=Config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(
            model_name=Config.GEMINI_MODEL,
            generation_config={
                "temperature": 0.8,
                "max_output_tokens": 4000,
            }
        )
    
    @retry(
        stop=stop_after_attempt(Config.MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def generate_human_like_content(self, title: str, author: str) -> Optional[SEOContent]:
        """Gemini দিয়ে মানবিক কন্টেন্ট তৈরি"""
        logger.info(f"🤖 Gemini দিয়ে কন্টেন্ট তৈরি করছি: {title}")
        
        prompt = f"""তুমি একজন অভিজ্ঞ বাংলা সাহিত্য সমালোচক এবং বই রিভিউয়ার। "{title}" বইটি সম্পর্কে একটি সত্যিকারের মানুষের মতো রিভিউ লেখো। লেখক: {author}।

⚠️ গুরুত্বপূর্ণ নির্দেশনা:
- প্রাকৃতিক কথোপকথনের ভাষা ব্যবহার করো (যেন একজন পাঠক আরেকজন পাঠককে বলছে)
- AI-টাইপ শব্দ এড়িয়ে যাও: "অসাধারণ", "অনবদ্য", "সমৃদ্ধ", "অনন্য"
- ছোট এবং সহজ বাক্য ব্যবহার করো
- ব্যক্তিগত অনুভূতি এবং অভিজ্ঞতা যোগ করো
- রিভিউটি অবশ্যই {Config.MIN_REVIEW_LENGTH} শব্দের বেশি হতে হবে
- বইয়ের থিম, প্লট এবং চরিত্র নিয়ে সংক্ষিপ্ত আলোচনা করো

OUTPUT FORMAT (শুধুমাত্র JSON - কোনো markdown ব্যাকটিক নয়):
{{
    "bangla_title": "{title} PDF – {author} বই ডাউনলোড করুন",
    "slug": "শুধু-ইংরেজি-ছোট-হাতের-অক্ষর-এবং-হাইফেন",
    "meta_desc": "১৫০-১৬০ অক্ষরের SEO-বান্ধব বর্ণনা",
    "category": "উপন্যাস/গল্প/কবিতা/প্রবন্ধ (উপযুক্ত একটি)",
    "summary": "তোমার লেখা {Config.MIN_REVIEW_LENGTH}+ শব্দের মানবিক রিভিউ",
    "tags": ["বাংলা বই", "{author}", "PDF ডাউনলোড", "আরও ২-৩টি প্রাসঙ্গিক ট্যাগ"]
}}"""

        try:
            response = self.model.generate_content(prompt)
            response = self.model.generate_content(prompt)
            response_text = response.text.strip()
            
            # Markdown backticks রিমুভ
            response_text = re.sub(r'```json\s*|\s*```', '', response_text)
            
            # JSON খুঁজে বের করা
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if not json_match:
                logger.warning("⚠️ JSON ফরম্যাট পাওয়া যায়নি")
                return None
            
            data = json.loads(json_match.group(0))
            
            # স্লাগ ভ্যালিডেশন ও ফিক্স
            original_slug = data.get('slug', '')
            validated_slug = SlugGenerator.validate_and_fix_slug(original_slug)
            
            if not validated_slug:
                # Gemini-র স্লাগ invalid হলে নিজে তৈরি করি
                logger.warning(f"⚠️ Invalid slug '{original_slug}', নতুন তৈরি করছি...")
                validated_slug = SlugGenerator.create_slug(title)
            
            # ডুপ্লিকেট স্লাগ চেক
            firebase = FirebaseManager()
            if firebase.check_slug_exists(validated_slug):
                # ডুপ্লিকেট হলে timestamp যোগ করি
                validated_slug = f"{validated_slug}-{int(time.time())}"
                logger.info(f"🔄 Duplicate slug detected, new: {validated_slug}")
            
            data['slug'] = validated_slug
            
            # রিভিউ দৈর্ঘ্য ভ্যালিডেশন
            summary_word_count = len(data.get('summary', '').split())
            if summary_word_count < Config.MIN_REVIEW_LENGTH:
                logger.warning(f"⚠️ রিভিউ খুব ছোট ({summary_word_count} শব্দ)")
                return None
            
            seo_content = SEOContent(**data)
            logger.info(f"✅ কন্টেন্ট তৈরি সফল: {title} | Slug: {validated_slug}")
            return seo_content
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON পার্স error: {e}")
            logger.debug(f"Response text: {response_text[:200]}...")
            return None
        except Exception as e:
            logger.error(f"❌ Gemini generation error: {e}")
            raise

# ================================
# Main Hunter Bot
# ================================
class BookHunterBot:
    def __init__(self):
        Config.validate()
        self.firebase = FirebaseManager()
        self.archive = ArchiveFetcher()
        self.gemini = GeminiGenerator()
    
    def calculate_publish_time(self, index: int) -> datetime:
        """প্রকাশের সময় হিসাব করা"""
        current_time = datetime.now()
        min_hours = Config.PUBLISH_DELAY_MIN_HOURS
        max_hours = Config.PUBLISH_DELAY_MAX_HOURS
        
        # প্রতিটি বইয়ের জন্য আলাদা সময়সীমা
        random_hours = random.randint(
            min_hours + (index * 2),
            max_hours + (index * 2)
        )
        return current_time + timedelta(hours=random_hours)
    
    def process_book(self, book: BookMetadata, index: int) -> bool:
        """একটি বই প্রসেস করা"""
        logger.info(f"📖 প্রসেস করছি: {book.title} (Downloads: {book.downloads})")
        
        try:
            # কন্টেন্ট তৈরি
            seo_content = self.gemini.generate_human_like_content(
                book.title, 
                book.author
            )
            
            if not seo_content:
                logger.warning(f"⚠️ কন্টেন্ট তৈরি ব্যর্থ: {book.title}")
                return False
            
            # অতিরিক্ত ডাটা যোগ করা
            seo_content.archive_id = book.id
            seo_content.download_url = book.url
            seo_content.publish_at = self.calculate_publish_time(index)
            seo_content.status = 'scheduled'
            seo_content.created_at = datetime.now()
            
            # Firestore-এ সেভ করা (asdict automatic datetime handle করবে)
            book_data = asdict(seo_content)
            success = self.firebase.save_book(book.id, book_data)
            
            if success:
                logger.info(f"✅ শিডিউল: {seo_content.publish_at.strftime('%Y-%m-%d %H:%M')}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ বই প্রসেস করতে সমস্যা {book.title}: {e}")
            return False
    
    def run(self):
        """মূল এক্সিকিউশন"""
        logger.info("🚀 Book Hunter Bot শুরু হচ্ছে...")
        logger.info(f"📊 Model: {Config.GEMINI_MODEL} | Max Books: {Config.MAX_BOOKS_PER_RUN}")
        start_time = time.time()
        
        try:
            # ট্রেন্ডিং বই আনা
            trending_books = self.archive.fetch_trending_books(
                limit=Config.MAX_BOOKS_PER_RUN
            )
            
            if not trending_books:
                logger.warning("⚠️ কোনো নতুন বই পাওয়া যায়নি")
                return
            
            # প্রতিটি বই প্রসেস করা
            success_count = 0
            for idx, book in enumerate(trending_books):
                if self.process_book(book, idx):
                    success_count += 1
                
                # Rate limiting
                if idx < len(trending_books) - 1:
                    logger.info(f"⏳ {Config.RATE_LIMIT_DELAY} সেকেন্ড অপেক্ষা করছি...")
                    time.sleep(Config.RATE_LIMIT_DELAY)
            
            # সামারি
            elapsed_time = time.time() - start_time
            logger.info(f"""
╔════════════════════════════════════════╗
║         🎯 এক্সিকিউশন সামারি          ║
╠════════════════════════════════════════╣
║ Model: {Config.GEMINI_MODEL[:20]:<20} ║
║ মোট বই প্রসেস: {len(trending_books):<20} ║
║ সফল: {success_count:<29} ║
║ ব্যর্থ: {len(trending_books) - success_count:<29} ║
║ সময়: {elapsed_time:.2f} সেকেন্ড{'':<18} ║
╚════════════════════════════════════════╝
            """)
            
        except Exception as e:
            logger.error(f"❌ Fatal error: {e}", exc_info=True)
            raise

# ================================
# Entry Point
# ================================
if __name__ == "__main__":
    try:
        bot = BookHunterBot()
        bot.run()
    except KeyboardInterrupt:
        logger.info("⏸️  ব্যবহারকারী দ্বারা বন্ধ করা হয়েছে")
    except Exception as e:
        logger.critical(f"💥 Program crashed: {e}", exc_info=True)

        exit(1)




