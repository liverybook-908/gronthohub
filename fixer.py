"""
Advanced Fixer Bot - Database Cleanup & Integrity Checker
==========================================================
এই বট Firestore ডাটাবেজের বইগুলোর লিংক চেক করে এবং ডাটা ইনটেগ্রিটি নিশ্চিত করে।

Features:
- Parallel processing with threading
- Smart filtering (only checks old records)
- Retry logic for failed requests
- Comprehensive error handling
- Detailed statistics and reporting
"""

import firebase_admin
from firebase_admin import credentials, firestore
import requests
import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass, field
from enum import Enum
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================================
# কনফিগারেশন ক্লাস
# ================================

class BookStatus(Enum):
    """বইয়ের বিভিন্ন স্ট্যাটাস"""
    PUBLISHED = "published"
    SCHEDULED = "scheduled"
    BROKEN = "broken"
    PENDING = "pending"
    ARCHIVED = "archived"
    DRAFT = "draft"

@dataclass
class FixerConfig:
    """Fixer Bot এর কনফিগারেশন"""
    # Network settings
    request_timeout: int = 15
    retry_attempts: int = 3
    
    # Rate limiting
    rate_limit_delay: float = 0.5  # Archive.org এর জন্য delay
    
    # Parallel processing
    max_workers: int = 4  # একসাথে কতগুলো thread চলবে
    enable_parallel: bool = True  # Parallel processing চালু/বন্ধ
    
    # Smart filtering
    check_interval_days: int = 7  # কত দিন পর পর চেক করবে
    enable_smart_filter: bool = True  # শুধু পুরনো রেকর্ড চেক করবে
    
    # Batch processing
    batch_size: Optional[int] = None  # প্রতিবার কতগুলো বই চেক করবে (None = সব)
    max_execution_time: int = 300  # Maximum execution time (5 minutes)
    
    # Data validation
    default_category: str = "উপন্যাস"
    default_status: str = BookStatus.PUBLISHED.value  # PENDING এর বদলে PUBLISHED
    required_fields: List[str] = field(default_factory=lambda: [
        'bangla_title',
        'category',
        'download_url',
        'status'
    ])
    
    # Logging
    verbose: bool = True  # Detailed logs চাইলে True

@dataclass
class FixerStats:
    """Fixer Bot এর পরিসংখ্যান"""
    total_scanned: int = 0
    working_links: int = 0
    broken_links: int = 0
    fixed_records: int = 0
    skipped_records: int = 0
    errors: int = 0
    timeout_stop: bool = False
    execution_time: float = 0.0
    
    def print_report(self):
        """সুন্দর রিপোর্ট প্রিন্ট করা"""
        print("\n" + "="*60)
        print("📊 FIXER BOT - EXECUTION REPORT")
        print("="*60)
        print(f"🔍 Total Books Scanned    : {self.total_scanned}")
        print(f"✅ Working Links          : {self.working_links}")
        print(f"❌ Broken Links Found     : {self.broken_links}")
        print(f"🔧 Records Fixed          : {self.fixed_records}")
        print(f"⏭️  Records Skipped        : {self.skipped_records}")
        print(f"⚠️  Errors Encountered    : {self.errors}")
        print(f"⏱️  Execution Time        : {self.execution_time:.2f}s")
        
        if self.timeout_stop:
            print(f"⏰ Stopped due to timeout limit")
        
        if self.total_scanned > 0:
            success_rate = (self.working_links / self.total_scanned) * 100
            print(f"\n📈 Success Rate           : {success_rate:.2f}%")
        
        print("="*60 + "\n")

# ================================
# লগিং সেটআপ
# ================================

def setup_logging(verbose: bool = True):
    """Professional logging সেটআপ"""
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

logger = None  # Will be initialized in main

# ================================
# ফায়ারবেস সেটআপ
# ================================

def initialize_firebase() -> firestore.Client:
    """ফায়ারবেস ইনিশিয়ালাইজ করা"""
    try:
        firebase_keys_json = os.getenv("FIREBASE_KEYS")
        
        if not firebase_keys_json:
            raise ValueError("FIREBASE_KEYS environment variable not found")
        
        # Temporary file তৈরি করা
        with open("firebase-key.json", "w") as f:
            f.write(firebase_keys_json)
        
        if not firebase_admin._apps:
            cred = credentials.Certificate("firebase-key.json")
            firebase_admin.initialize_app(cred)
            logger.info("✅ Firebase initialized successfully")
        
        return firestore.client()
    
    except Exception as e:
        logger.error(f"❌ Firebase initialization failed: {e}")
        raise

# ================================
# HTTP সেশন সেটআপ (Retry Logic সহ)
# ================================

def create_session(config: FixerConfig) -> requests.Session:
    """Retry logic সহ HTTP session তৈরি"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=config.retry_attempts,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# ================================
# লিং চেকার ক্লাস
# ================================

class LinkChecker:
    """Advanced link checking functionality"""
    
    def __init__(self, config: FixerConfig):
        self.config = config
        self.session = create_session(config)
    
    def check_link(self, url: str) -> Tuple[bool, Optional[str]]:
        """
        লিংক চেক করা এবং স্ট্যাটাস রিটার্ন করা
        Returns: (is_working, error_message)
        """
        if not url:
            return False, "URL missing"
        
        try:
            response = self.session.head(
                url,
                allow_redirects=True,
                timeout=self.config.request_timeout
            )
            
            if response.status_code == 200:
                return True, None
            else:
                return False, f"Status code: {response.status_code}"
        
        except requests.Timeout:
            return False, "Request timeout"
        except requests.ConnectionError:
            return False, "Connection error"
        except Exception as e:
            return False, f"Unknown error: {str(e)[:50]}"
    
    def __del__(self):
        """Session close করা"""
        if hasattr(self, 'session'):
            self.session.close()

# ================================
# ডাটা ভ্যালিডেটর ক্লাস
# ================================

class DataValidator:
    """ডাটা ইনটেগ্রিটি চেক করা"""
    
    def __init__(self, config: FixerConfig):
        self.config = config
    
    def validate_book(self, book_data: Dict) -> Dict[str, any]:
        """
        বইয়ের ডাটা ভ্যালিডেট করা এবং ফিক্সড ডাটা রিটার্ন করা
        Returns: Dictionary of fields to update
        """
        updates = {}
        
        # Required fields চেক করা
        for field in self.config.required_fields:
            if field not in book_data or not book_data[field]:
                if field == 'category':
                    updates['category'] = self.config.default_category
                elif field == 'status':
                    # PENDING এর বদলে PUBLISHED দিচ্ছি (ফ্রন্টএন্ডে দেখানোর জন্য)
                    updates['status'] = self.config.default_status
        
        # বাংলা টাইটেল স্পেস ট্রিম করা
        if 'bangla_title' in book_data and book_data['bangla_title']:
            trimmed_title = book_data['bangla_title'].strip()
            if trimmed_title != book_data['bangla_title']:
                updates['bangla_title'] = trimmed_title
        
        # ইংরেজি টাইটেল চেক
        if 'english_title' in book_data and book_data['english_title']:
            trimmed_eng = book_data['english_title'].strip()
            if trimmed_eng != book_data['english_title']:
                updates['english_title'] = trimmed_eng
        
        # লেখকের নাম ট্রিম
        if 'author' in book_data and book_data['author']:
            trimmed_author = book_data['author'].strip()
            if trimmed_author != book_data['author']:
                updates['author'] = trimmed_author
        
        return updates

# ================================
# মেইন ফিক্সার ক্লাস
# ================================

class FixerBot:
    """Main Fixer Bot class"""
    
    def __init__(self, config: FixerConfig = None):
        self.config = config or FixerConfig()
        self.db = initialize_firebase()
        self.link_checker = LinkChecker(self.config)
        self.data_validator = DataValidator(self.config)
        self.stats = FixerStats()
        self.start_time = None
    
    def _should_stop_execution(self) -> bool:
        """Timeout check করা"""
        if self.start_time is None:
            return False
        
        elapsed = time.time() - self.start_time
        if elapsed >= self.config.max_execution_time:
            logger.warning(f"⏰ Timeout reached ({elapsed:.0f}s). Stopping execution...")
            self.stats.timeout_stop = True
            return True
        return False
    
    def process_book(self, doc) -> None:
        """একটি বই প্রসেস করা"""
        try:
            book_data = doc.to_dict()
            doc_id = doc.id
            self.stats.total_scanned += 1
            
            book_title = book_data.get('bangla_title', 'Unknown')
            
            if self.config.verbose:
                logger.info(f"🔍 [{self.stats.total_scanned}] Checking: {book_title}")
            
            updates = {}
            
            # ১. লিংক চেক করা
            pdf_url = book_data.get('download_url')
            if pdf_url:
                is_working, error_msg = self.link_checker.check_link(pdf_url)
                
                if not is_working:
                    logger.warning(f"❌ Broken link for '{book_title}': {error_msg}")
                    updates['status'] = BookStatus.BROKEN.value
                    updates['error_message'] = error_msg
                    self.stats.broken_links += 1
                else:
                    if self.config.verbose:
                        logger.info(f"✅ Link working for '{book_title}'")
                    self.stats.working_links += 1
                    
                    # যদি আগে broken ছিল, তাহলে ঠিক করা
                    if book_data.get('status') == BookStatus.BROKEN.value:
                        updates['status'] = BookStatus.PUBLISHED.value
                        if 'error_message' in book_data:
                            updates['error_message'] = firestore.DELETE_FIELD
            
            # ২. ডাটা ইনটেগ্রিটি চেক
            data_updates = self.data_validator.validate_book(book_data)
            updates.update(data_updates)
            
            # ৩. Last checked timestamp আপডেট
            updates['last_checked'] = firestore.SERVER_TIMESTAMP
            updates['last_fixer_run'] = datetime.now().isoformat()
            
            # ৪. Firestore আপডেট করা
            if updates:
                self.db.collection('books').document(doc_id).update(updates)
                self.stats.fixed_records += 1
                if self.config.verbose:
                    logger.info(f"🔧 Updated {len(updates)} fields")
            else:
                self.stats.skipped_records += 1
            
            # Rate limiting (শুধু serial processing এর জন্য)
            if not self.config.enable_parallel:
                time.sleep(self.config.rate_limit_delay)
        
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"❌ Error processing book: {e}")
    
    def _get_query(self) -> firestore.Query:
        """Smart filtering সহ query তৈরি করা"""
        query = self.db.collection('books')
        
        # Smart filtering: শুধু পুরনো রেকর্ড চেক করা
        if self.config.enable_smart_filter:
            cutoff_date = datetime.now() - timedelta(days=self.config.check_interval_days)
            
            # যেসব বই গত X দিনে চেক করা হয়নি
            # Note: Firestore এ 'last_checked' field না থাকলেও query কাজ করবে
            logger.info(f"📅 Filtering books not checked since {cutoff_date.strftime('%Y-%m-%d')}")
            query = query.where('last_checked', '<', cutoff_date)
        
        # Batch size limit
        if self.config.batch_size:
            query = query.limit(self.config.batch_size)
        
        return query
    
    def run_parallel(self, docs: List) -> None:
        """Parallel processing দিয়ে বই চেক করা"""
        logger.info(f"🚀 Starting parallel processing with {self.config.max_workers} workers")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # সব docs এর জন্য futures তৈরি করা
            futures = []
            for doc in docs:
                if self._should_stop_execution():
                    break
                
                future = executor.submit(self.process_book, doc)
                futures.append(future)
                
                # Rate limiting: প্রতিবার submit এর পর একটু delay
                time.sleep(self.config.rate_limit_delay / self.config.max_workers)
            
            # সব futures complete হওয়ার জন্য অপেক্ষা
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"❌ Thread error: {e}")
                    self.stats.errors += 1
    
    def run_serial(self, docs: List) -> None:
        """Serial processing (একটার পর একটা)"""
        logger.info(f"🔄 Starting serial processing")
        
        for doc in docs:
            if self._should_stop_execution():
                break
            self.process_book(doc)
    
    def run(self) -> FixerStats:
        """মেইন execution function"""
        logger.info("🛠️  Fixer Bot started...")
        self.start_time = time.time()
        
        try:
            # Query তৈরি করা
            query = self._get_query()
            
            # Documents fetch করা
            docs = list(query.stream())
            logger.info(f"📚 Found {len(docs)} books to process")
            
            if len(docs) == 0:
                logger.info("✨ All books are up to date! No processing needed.")
                self.stats.execution_time = time.time() - self.start_time
                return self.stats
            
            # Parallel বা Serial processing
            if self.config.enable_parallel and len(docs) > 5:
                self.run_parallel(docs)
            else:
                self.run_serial(docs)
            
            # Execution time calculate
            self.stats.execution_time = time.time() - self.start_time
            logger.info(f"⏱️  Execution completed in {self.stats.execution_time:.2f} seconds")
            
            # Final report
            self.stats.print_report()
            
            return self.stats
        
        except Exception as e:
            logger.error(f"❌ Critical error in Fixer Bot: {e}")
            raise
        
        finally:
            # Cleanup
            if hasattr(self, 'link_checker'):
                del self.link_checker

# ================================
# মেইন এক্সিকিউশন
# ================================

def main():
    """Main entry point"""
    global logger
    
    try:
        # Custom configuration
        config = FixerConfig(
            # Network settings
            request_timeout=15,
            retry_attempts=3,
            
            # Rate limiting
            rate_limit_delay=0.6,  # Archive.org এর জন্য
            
            # Parallel processing (GitHub Actions এর জন্য অপ্টিমাইজ করা)
            max_workers=3,  # 3-4 একসাথে চলবে
            enable_parallel=True,
            
            # Smart filtering (শুধু পুরনো রেকর্ড চেক করা)
            check_interval_days=7,  # গত ৭ দিনে চেক করা হয়নি এমন বই
            enable_smart_filter=True,
            
            # Batch processing
            batch_size=None,  # None = সব চেক করবে
            max_execution_time=300,  # 5 minutes (GitHub Actions timeout এর আগেই থামবে)
            
            # Data settings
            default_category="উপন্যাস",
            default_status=BookStatus.PUBLISHED.value,  # ফ্রন্টএন্ডে দেখানোর জন্য
            
            # Logging
            verbose=True  # Detailed logs
        )
        
        # Logger initialize
        logger = setup_logging(config.verbose)
        
        # Fixer Bot চালানো
        fixer = FixerBot(config)
        stats = fixer.run()
        
        # Exit code based on results
        if stats.errors > 0:
            logger.warning(f"⚠️  Completed with {stats.errors} errors")
            exit(1)
        else:
            logger.info("✅ Completed successfully")
            exit(0)
    
    except Exception as e:
        if logger:
            logger.error(f"❌ Fatal error: {e}")
        else:
            print(f"❌ Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    main()