"""
pip install tweepy transformers requests beautifulsoup4
"""

import asyncio
import tweepy
import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from typing import List, Dict
from transformers import pipeline
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# Twitter API v2 Bearer Token
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

if not BEARER_TOKEN:
    raise ValueError("트위터 베어러 토큰이 환경 변수에 설정되어 있지 않습니다. .env 파일을 확인하세요.")

# Analyst X accounts
ANALYST_ACCOUNTS = [
    "GONOGO_Korea", "parkthomson075", "spottedseal82", "ohmahahm", "9uro9uru",
    "apt_1ab", "gyeranmarie_", "sunshine_mong", "PLANTAB_forlife"
]

# Data source URLs
DATA_SOURCES = [
    "https://paxnet.co.kr/stock/report/report?menuCode=1111",
    "https://globalmonitor.einfomax.co.kr/mr_usa_hts.html#/01/01",
    "https://finance.naver.com/research/",
    "https://markets.hankyung.com/consensus"
]

# Initialize Twitter API v2 client
client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

# Initialize summarizer and sentiment analysis models
# Using a smaller model for sentiment analysis for faster inference if applicable,
# or ensure the chosen model is suitable for the language of the tweets.
# For Korean sentiment analysis, a specific Korean pre-trained model might be better.
# For this example, 'cardiffnlp/twitter-roberta-base-sentiment' is good for English Twitter.
# If tweets are primarily Korean, consider a model like 'beomi/kcbert-base' fine-tuned for sentiment.
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
sentiment_analyzer = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")

async def get_user_id(username: str) -> str:
    """Fetches a Twitter user's ID asynchronously."""
    try:
        user = await asyncio.to_thread(client.get_user, username=username)
        return user.data.id
    except Exception as e:
        print(f"Error getting user ID for {username}: {e}")
        return None

async def fetch_v2_tweets(username: str, start_time: datetime, end_time: datetime) -> List[Dict]:
    """Fetches tweets for a given user within a time range asynchronously."""
    try:
        user_id = await get_user_id(username)
        if not user_id:
            return []

        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        tweets_response = await asyncio.to_thread(
            client.get_users_tweets,
            id=user_id,
            max_results=100,  # Max results per request
            start_time=start_str,
            end_time=end_str,
            tweet_fields=["created_at", "text"]
        )

        result = []
        if tweets_response and tweets_response.data:
            for tweet in tweets_response.data:
                result.append({
                    "account": username,
                    "text": tweet.text,
                    "created_at": tweet.created_at.isoformat(),
                    "id": tweet.id
                })
        return result
    except Exception as e:
        print(f"Error fetching tweets for {username}: {e}")
        return []

async def fetch_x_posts(accounts: List[str], start_date: datetime, end_date: datetime) -> List[Dict]:
    """Fetches X (Twitter) posts from multiple accounts concurrently."""
    tasks = [fetch_v2_tweets(account, start_date, end_date) for account in accounts]
    # asyncio.gather runs tasks concurrently and returns results in the order of tasks
    all_posts_lists = await asyncio.gather(*tasks)
    
    all_posts = []
    for posts_list in all_posts_lists:
        all_posts.extend(posts_list)
    return all_posts

async def fetch_web_content(url: str) -> str:
    """Fetches and extracts text content from a given URL asynchronously."""
    try:
        response = await asyncio.to_thread(requests.get, url, timeout=10)
        response.raise_for_status()
        soup = await asyncio.to_thread(BeautifulSoup, response.content, "html.parser")
        content = soup.get_text(separator=" ", strip=True)
        return content[:5000]  # Limit content to prevent overly long inputs to models
    except Exception as e:
        print(f"Error fetching content from {url}: {e}")
        return ""

def chunk_text(text: str, max_tokens: int = 800) -> List[str]:
    """Splits text into chunks of maximum tokens."""
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0

    for word in words:
        if current_length + len(word) + 1 > max_tokens and current_chunk:
            chunks.append(" ".join(current_chunk))
            current_chunk = [word]
            current_length = len(word)
        else:
            current_chunk.append(word)
            current_length += len(word) + 1
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    return chunks

async def analyze_sentiment_batch(texts: List[str]) -> List[Dict]:
    """Analyzes sentiment for a batch of texts using the pre-trained model."""
    if not texts:
        return []
    try:
        # Run sentiment analysis in a separate thread to avoid blocking
        sentiments = await asyncio.to_thread(sentiment_analyzer, texts)
        return sentiments
    except Exception as e:
        print(f"Error during sentiment analysis: {e}")
        # Return a default/error sentiment for each text if an error occurs
        return [{"label": "ERROR", "score": 0.0} for _ in texts]

async def summarize_text_batch(texts: List[str], max_length: int = 150, min_length: int = 50) -> List[str]:
    """Summarizes a batch of texts using the pre-trained model."""
    if not texts:
        return []
    try:
        # Run summarization in a separate thread to avoid blocking
        summaries_output = await asyncio.to_thread(
            summarizer,
            texts,
            max_length=max_length,
            min_length=min_length,
            do_sample=False  # Deterministic output for better consistency
        )
        return [s["summary_text"] for s in summaries_output]
    except Exception as e:
        print(f"Error during summarization: {e}")
        # Return an error message for each text if an error occurs
        return ["요약 실패: 모델 처리 오류"] * len(texts)

async def summarize_content(content: Dict, date: str) -> str:
    """Generates a comprehensive summary including X posts and web reports with sentiment analysis."""
    try:
        summary = f"📈 주식 시장 요약 ({date})\n\n"

        # --- X Posts Summarization and Sentiment Analysis ---
        x_texts = [post["text"] for post in content["x_posts"]]
        if x_texts:
            # Batch summarization for X posts
            x_chunks = []
            for text in x_texts:
                x_chunks.extend(chunk_text(text))
            
            x_summaries = await summarize_text_batch(x_chunks)
            x_summary_combined = "\n".join(x_summaries)
            
            # Batch sentiment analysis for original X texts
            x_sentiments = await analyze_sentiment_batch(x_texts)
            
            positive_count = sum(1 for s in x_sentiments if s["label"] == "POSITIVE")
            negative_count = sum(1 for s in x_sentiments if s["label"] == "NEGATIVE")
            neutral_count = sum(1 for s in x_sentiments if s["label"] == "NEUTRAL")

            total_tweets = len(x_texts)
            if total_tweets > 0:
                sentiment_analysis_text = (
                    f"총 {total_tweets}개 트윗 중 긍정 {positive_count}개, "
                    f"부정 {negative_count}개, 중립 {neutral_count}개로 분석됨."
                )
            else:
                sentiment_analysis_text = "감정 분석 대상 트윗 없음."

            summary += "🔹 X(트위터) 분석 요약:\n"
            summary += f"*{sentiment_analysis_text}*\n"
            summary += x_summary_combined + "\n"
        else:
            summary += "🔹 X(트위터): 해당 일자 게시글 없음\n"

        # --- Web Report Summarization ---
        summary += "\n🔹 웹 리포트 요약:\n"
        web_content_summaries = []
        for web in content["web_content"]:
            web_chunks = chunk_text(web["content"])
            # Batch summarization for web content chunks
            summaries_for_source = await summarize_text_batch(web_chunks)
            web_summary_combined = "\n".join(summaries_for_source)
            web_content_summaries.append(f"- {web['source']}:\n{web_summary_combined}\n")
        
        if web_content_summaries:
            summary += "\n".join(web_content_summaries)
        else:
            summary += "웹 리포트 없음.\n"

        return summary
    except Exception as e:
        print(f"Error summarizing content: {e}")
        return "요약 생성 실패"

async def generate_summary_by_date(date_str: str):
    """Coordinates fetching data, summarizing, and saving the report for a specific date."""
    try:
        base_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        start_date = datetime(base_date.year, base_date.month, base_date.day, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)

        print(f"🔍 Fetching X posts for {date_str}...")
        x_posts = await fetch_x_posts(ANALYST_ACCOUNTS, start_date, end_date)
        print(f"✅ Fetched {len(x_posts)} X posts.")

        print("🌐 Fetching web content...")
        # Fetch web content concurrently
        web_content_tasks = [fetch_web_content(url) for url in DATA_SOURCES]
        fetched_web_contents = await asyncio.gather(*web_content_tasks)
        
        web_content = []
        for url, content_text in zip(DATA_SOURCES, fetched_web_contents):
            if content_text: # Only add if content was successfully fetched
                web_content.append({"source": url, "content": content_text})
        print(f"✅ Fetched {len(web_content)} web sources.")

        all_content = {
            "x_posts": x_posts,
            "web_content": web_content
        }

        print("📝 Generating summary...")
        summary = await summarize_content(all_content, base_date.strftime("%Y-%m-%d"))

        output_file = f"stock_summary_{date_str}.txt"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(summary)

        print(f"\n✅ Summary saved to: {output_file}")
    except Exception as e:
        print(f"❌ Error generating summary: {e}")

async def main():
    """Main function to prompt for a date and initiate summary generation."""
    date_input = input("📅 요약할 날짜를 입력하세요 (예: 20250711): ").strip()
    await generate_summary_by_date(date_input)

if __name__ == "__main__":
    asyncio.run(main())


