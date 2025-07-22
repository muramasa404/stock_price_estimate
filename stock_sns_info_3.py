import asyncio
import tweepy
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
from dotenv import load_dotenv
from typing import List, Dict
from transformers import pipeline

# Load environment variables from .env file
load_dotenv()

# Configuration
X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")

if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET]):
    raise ValueError("트위터 API 키가 환경 변수에 설정되어 있지 않습니다. .env 파일을 확인하세요.")

# List of stock market analysts' X accounts
ANALYST_ACCOUNTS = [
    "GONOGO_Korea", "parkthomson075", "spottedseal82", "ohmahahm", "9uro9uru",
    "apt_1ab", "gyeranmarie_", "sunshine_mong", "PLANTAB_forlife"
]

# URLs for securities reports and market updates
DATA_SOURCES = [
    "https://paxnet.co.kr/stock/report/report?menuCode=1111",
    "https://globalmonitor.einfomax.co.kr/mr_usa_hts.html#/01/01",
    "https://finance.naver.com/research/",
    "https://markets.hankyung.com/consensus"
]

# Initialize X API client
auth = tweepy.OAuthHandler(X_API_KEY, X_API_SECRET)
auth.set_access_token(X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET)
x_api = tweepy.API(auth, wait_on_rate_limit=True)

# Initialize summarization pipeline
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

async def fetch_x_posts(accounts: List[str], start_date: datetime, end_date: datetime) -> List[Dict]:
    """Fetch posts from specified X accounts within a date range."""
    posts = []
    for account in accounts:
        try:
            tweets = x_api.user_timeline(screen_name=account, count=200, tweet_mode="extended")
            for tweet in tweets:
                tweet_date = tweet.created_at.replace(tzinfo=None)
                if start_date <= tweet_date <= end_date:
                    text = tweet.full_text if hasattr(tweet, "full_text") else tweet.text
                    posts.append({
                        "account": account,
                        "text": text,
                        "created_at": tweet_date.isoformat(),
                        "id": tweet.id_str
                    })
        except Exception as e:
            print(f"Error fetching posts for {account}: {e}")
    return posts

def fetch_web_content(url: str) -> str:
    """Fetch and extract text content from a webpage."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        content = soup.get_text(separator=" ", strip=True)
        return content[:5000]
    except Exception as e:
        print(f"Error fetching content from {url}: {e}")
        return ""

async def summarize_content(content: List[Dict], date: str) -> str:
    """Summarize content using a local AI model."""
    try:
        summary = f"# Stock Market Summary for {date}\n\n"
        
        # Summarize X posts
        x_text = " ".join([post["text"] for post in content["x_posts"]])
        if x_text:
            x_summary = summarizer(x_text, max_length=150, min_length=50, do_sample=False)[0]["summary_text"]
            summary += "## X Posts\n" + x_summary + "\n"
        else:
            summary += "## X Posts\nNo posts found for the specified date.\n"
        
        # Summarize web content
        summary += "\n## Web Reports\n"
        for web in content["web_content"]:
            web_summary = summarizer(web["content"], max_length=150, min_length=50, do_sample=False)[0]["summary_text"]
            summary += f"- {web['source']}:\n{web_summary}\n"
        
        return summary
    except Exception as e:
        print(f"Error summarizing content: {e}")
        return "Summary generation failed."

async def generate_daily_summary():
    """Generate daily stock market summary."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)
    
    x_posts = await fetch_x_posts(ANALYST_ACCOUNTS, start_date, end_date)
    web_content = []
    for url in DATA_SOURCES:
        content = fetch_web_content(url)
        if content:
            web_content.append({"source": url, "content": content})
    
    all_content = {
        "x_posts": x_posts,
        "web_content": web_content
    }
    
    summary = await summarize_content(all_content, start_date.strftime("%Y-%m-%d"))
    
    output_file = f"stock_summary_{start_date.strftime('%Y%m%d')}.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(summary)
    
    return output_file

async def main():
    """Main function to run the daily summary process."""
    output_file = await generate_daily_summary()
    print(f"Daily summary generated: {output_file}")

if __name__ == "__main__":
    asyncio.run(main())


