import os
import json
import logging
import time
import urllib.parse  # ক্যাটাগরি লিংকের জন্য নতুন যোগ করা হয়েছে
from datetime import datetime
from typing import List, Dict
import firebase_admin
from firebase_admin import credentials, firestore
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2 import service_account # Indexing API এর জন্য
from google.auth.transport.requests import Request

# ================================
# লগিং সেটআপ
# ================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('promotor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================================
# কনফিগারেশন
# ================================
class Config:
    # গিটহাব সিক্রেটস থেকে নেবে, না থাকলে ডিফল্ট
    SITE_URL = os.getenv("SITE_URL", "https://your-site.web.app").rstrip('/')
    SITE_NAME = os.getenv("SITE_NAME", "আমার বই লাইব্রেরি")
    SITE_DESCRIPTION = os.getenv("SITE_DESCRIPTION", "বাংলা বইয়ের অনলাইন লাইব্রেরি")
    
    # Retry Configuration
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2

# ================================
# Firebase Initialization
# ================================
def initialize_firebase():
    try:
        firebase_keys_json = os.getenv("FIREBASE_KEYS")
        if firebase_keys_json:
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
# Helper: URL Safe Slugging
# ================================
def slugify(text: str) -> str:
    """বাংলা বা ইংরেজি টেক্সটকে URL ফ্রেন্ডলি করে"""
    return urllib.parse.quote(text.strip().replace(' ', '-'))

# ================================
# Sitemap Generation
# ================================
def generate_sitemap(books: List[Dict]) -> bool:
    try:
        logger.info("🌐 Generating sitemap.xml...")
        os.makedirs('public', exist_ok=True)
        
        now = datetime.now().strftime('%Y-%m-%d')
        
        xml_lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"',
            '        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">'
        ]
        
        # ১. হোমপেজ
        xml_lines.append(f'''
    <url>
        <loc>{Config.SITE_URL}/</loc>
        <lastmod>{now}</lastmod>
        <changefreq>daily</changefreq>
        <priority>1.0</priority>
    </url>''')

        # ২. ক্যাটাগরি পেজ (স্লাগ ঠিক করা হয়েছে)
        categories = set(book.get('category') for book in books if book.get('category'))
        for cat in categories:
            cat_slug = slugify(cat)
            xml_lines.append(f'''
    <url>
        <loc>{Config.SITE_URL}/category/{cat_slug}</loc>
        <lastmod>{now}</lastmod>
        <changefreq>weekly</changefreq>
        <priority>0.7</priority>
    </url>''')

        # ৩. বইয়ের পেজ
        for book in books:
            slug = book.get('slug')
            if not slug: continue
                
            # টাইমস্ট্যাম্প হ্যান্ডলিং
            raw_date = book.get('publish_at') or book.get('created_at')
            lastmod = raw_date.strftime('%Y-%m-%d') if hasattr(raw_date, 'strftime') else now
            
            # ইমেজের জন্য গুগল রুলস মানা
            image_url = book.get('cover_image')
            image_tag = ""
            if image_url and image_url.startswith('http'):
                image_tag = f'''
        <image:image>
            <image:loc>{image_url}</image:loc>
            <image:title><![CDATA[{book.get('title', 'বই')}]]></image:title>
        </image:image>'''
            
            xml_lines.append(f'''
    <url>
        <loc>{Config.SITE_URL}/book/{slug}</loc>
        <lastmod>{lastmod}</lastmod>
        <changefreq>weekly</changefreq>
        <priority>0.8</priority>{image_tag}
    </url>''')
            
        xml_lines.append('</urlset>')
        
        with open("public/sitemap.xml", "w", encoding="utf-8") as f:
            f.write("\n".join(xml_lines))
        
        return True
    except Exception as e:
        logger.error(f"❌ Sitemap failed: {e}")
        return False

# ================================
# RSS Feed (Date formatting fixed)
# ================================
def generate_rss_feed(books: List[Dict]) -> bool:
    try:
        logger.info("📡 Generating RSS feed...")
        # নতুন ৫টি বইয়ের জন্য RSS
        sorted_books = sorted(books, key=lambda x: str(x.get('created_at')), reverse=True)[:15]
        
        now_rfc = datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0000')
        
        rss = [
            '<?xml version="1.0" encoding="UTF-8" ?>',
            '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">',
            '<channel>',
            f'<title>{Config.SITE_NAME}</title>',
            f'<link>{Config.SITE_URL}</link>',
            f'<description>{Config.SITE_DESCRIPTION}</description>',
            f'<lastBuildDate>{now_rfc}</lastBuildDate>'
        ]
        
        for book in sorted_books:
            pub_date = book.get('publish_at') or datetime.now()
            pub_date_rfc = pub_date.strftime('%a, %d %b %Y %H:%M:%S +0000') if hasattr(pub_date, 'strftime') else now_rfc
            
            rss.append(f'''
    <item>
        <title><![CDATA[{book.get('bangla_title', 'বই')}]]></title>
        <link>{Config.SITE_URL}/book/{book.get('slug')}</link>
        <description><![CDATA[{book.get('summary', '')[:200]}...]]></description>
        <pubDate>{pub_date_rfc}</pubDate>
        <guid>{Config.SITE_URL}/book/{book.get('slug')}</guid>
    </item>''')
            
        rss.extend(['</channel>', '</rss>'])
        with open("public/rss.xml", "w", encoding="utf-8") as f:
            f.write("\n".join(rss))
        return True
    except Exception as e:
        logger.error(f"❌ RSS failed: {e}")
        return False

# ================================
# Google Indexing API (সরাসরি কার্যকর করা হয়েছে)
# ================================
def notify_google_indexing(books: List[Dict]):
    """নতুন বইগুলোকে গুগল ইনডেক্সিং এ পাঠানো"""
    try:
        if not os.path.exists("firebase-key.json"):
            return
            
        logger.info("🔍 Notifying Google Indexing API...")
        scopes = ['https://www.googleapis.com/auth/indexing']
        endpoint = "https://indexing.googleapis.com/v3/urlNotifications:publish"
        
        credentials_obj = service_account.Credentials.from_service_account_file(
            'firebase-key.json', scopes=scopes)
        
        # গত ২৪ ঘণ্টার বইগুলো পাঠানো
        for book in books[:5]: 
            url = f"{Config.SITE_URL}/book/{book.get('slug')}"
            
            credentials_obj.refresh(Request())
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {credentials_obj.token}'
            }
            
            payload = {"url": url, "type": "URL_UPDATED"}
            res = requests.post(endpoint, json=payload, headers=headers)
            
            if res.status_code == 200:
                logger.info(f"✅ Indexed: {url}")
            else:
                logger.warning(f"⚠️ Indexing status {res.status_code} for {url}")
                
    except Exception as e:
        logger.warning(f"ℹ️ Google Indexing API skipped: {e}")

# ================================
# Robots.txt (Updated)
# ================================
def generate_robots_txt():
    content = f"User-agent: *\nAllow: /\n\nSitemap: {Config.SITE_URL}/sitemap.xml\nSitemap: {Config.SITE_URL}/rss.xml"
    with open("public/robots.txt", "w") as f:
        f.write(content)
    logger.info("✅ robots.txt generated")

# ================================
# Main Execution
# ================================
def run_promotor():
    start_time = time.time()
    try:
        db = initialize_firebase()
        
        # শুধু পাবলিশড বই আনা (এবং যেগুলো শিডিউল টাইম পার হয়েছে)
        now = datetime.now()
        docs = db.collection('books').stream()
        
        books = []
        for doc in docs:
            b = doc.to_dict()
            # ফিউচার শিডিউল চেক
            p_at = b.get('publish_at')
            if p_at and hasattr(p_at, 'timestamp') and p_at > now:
                continue
            books.append(b)
        
        if books:
            generate_sitemap(books)
            generate_rss_feed(books)
            generate_robots_txt()
            notify_google_indexing(books)
            
            # Ping engines
            s_url = f"{Config.SITE_URL}/sitemap.xml"
            requests.get(f"https://www.google.com/ping?sitemap={s_url}")
            requests.get(f"https://www.bing.com/ping?sitemap={s_url}")
            
            logger.info(f"✨ COMPLETED in {time.time() - start_time:.2f}s")
        else:
            logger.warning("📭 No books to promote.")

    except Exception as e:
        logger.error(f"💥 Error: {e}")

if __name__ == "__main__":

    run_promotor()
