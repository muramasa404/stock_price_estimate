"""


pip install openai snscrape python-dotenv tweepy


"""

import tweepy
import openai
import os
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# ✅ OpenAI API 키 환경 변수에서 불러오기
openai.api_key = os.getenv("OPENAI_API_KEY")

if not openai.api_key:
    raise ValueError("OpenAI API 키가 환경 변수에 설정되어 있지 않습니다. .env 파일을 확인하세요.")

# ✅ Tweepy 인증 (X API 키 환경 변수에서 불러오기)
consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

if not all([consumer_key, consumer_secret, access_token, access_token_secret]):
    raise ValueError("트위터 API 키가 환경 변수에 설정되어 있지 않습니다. .env 파일을 확인하세요.")

auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# ✅ 분석가 계정 목록
analysts = [
    "jungmjae",
    "economyplus",
    "kisu_shin",
    "johnleevalue",
    "seungbeomko",
]

# ✅ 트윗 수집 함수 (최대 10개)
def fetch_tweets(username, limit=10):
    tweets = []
    try:
        for tweet in api.user_timeline(screen_name=username, count=limit, tweet_mode="extended"):
            date_str = tweet.created_at.strftime("%Y-%m-%d")
            tweets.append({"date": date_str, "content": tweet.full_text})
    except Exception as e:
        print(f"[오류] {username} 수집 실패: {e}")
    return tweets

# ✅ GPT 요약 함수
def summarize_text(text):
    prompt = f"다음 주식 관련 트윗 내용을 한국어로 요약해줘:\n\n{text}\n\n요약:"
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",  # 또는 gpt-3.5-turbo
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=400
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"[요약 실패] {e}"

# ✅ 요약 저장 함수
def save_summary_by_date(summaries_by_date, analyst):
    os.makedirs("summaries", exist_ok=True)
    for date, summaries in summaries_by_date.items():
        filename = f"summaries/{date}_{analyst}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"[{analyst}] {date} 요약 결과\n\n")
            f.write("\n\n".join(summaries))
        print(f"📁 저장됨: {filename}")

# ✅ 메인 함수
def main():
    for analyst in analysts:
        print(f"🔍 {analyst} 트윗 수집 중...")
        tweets = fetch_tweets(analyst, limit=5)
        summaries_by_date = defaultdict(list)
        for tweet in tweets:
            summary = summarize_text(tweet['content'])
            summaries_by_date[tweet['date']].append(summary)
        save_summary_by_date(summaries_by_date, analyst)
        print(f"✅ {analyst} 완료\n")

if __name__ == "__main__":
    main()

