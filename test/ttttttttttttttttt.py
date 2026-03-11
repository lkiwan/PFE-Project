import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# 1. Your dictionary mapping Boursenews slugs to your clean tickers
target_stocks = {
    "AKDITAL": "aktidal", 
    "ALLIANCES": "alliances-p",
    "ATW": "attijariwafa-bank",
    "CIH": "cih-p",
    "ADDOHA-P": "addoha-p", 
    "IAM": "maroc-telecom",
    "JET CONTRACTORS": "jet-contractors-p",
    "SGTM": "sgtm-p",
    "SODEP": "sodep-p",
    "TAQA": "taqa-morocco-p",
    "TGCC": "tgcc"
}

def scrape_boursenews():
    print("Starting the scraper...")
    news_dataset = []

    # Headers to mimic a real browser and avoid getting blocked
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Target URL for the main market feed
    url = "https://boursenews.ma/articles/marches"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Check for HTTP errors
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Boursenews: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # IMPORTANT: These class names are placeholders. You will need to right-click -> "Inspect" 
    # on the Boursenews website to find the exact HTML tags they use for articles.
    article_containers = soup.find_all('div', class_='article-item') 
    
    if not article_containers:
        print("Warning: Could not find any articles. The HTML class name 'article-item' might need to be updated.")

    for article in article_containers:
        # Extract Headline
        title_tag = article.find('h2')
        headline = title_tag.text.strip() if title_tag else ""
        
        # Extract Snippet
        snippet_tag = article.find('p')
        snippet = snippet_tag.text.strip() if snippet_tag else ""
        
        # Extract the Article Link
        link_tag = article.find('a', href=True)
        article_url = link_tag['href'] if link_tag else ""
        
        # Filter and Match
        for ticker, slug in target_stocks.items():
            # Check if the slug is in the URL or the company name is in the headline
            if slug in article_url.lower() or slug.replace("-p", "") in headline.lower():
                
                # Extract the Date/Time
                time_tag = article.find('time') 
                article_time = time_tag.text.strip() if time_tag else "Unknown Time"
                
                news_dataset.append({
                    "Date_Time": article_time,
                    "Ticker": ticker,
                    "Headline": headline,
                    "Snippet": snippet,
                    "URL": article_url # Keeping the URL is good for debugging
                })
                print(f"Matched: {ticker} - {headline[:30]}...")
                break # Move to the next article once we have a match

    # Convert to DataFrame
    df = pd.DataFrame(news_dataset)

    # Save to CSV if we found data
    if not df.empty:
        # Attempt to clean up the date format if possible
        try:
            df['Date_Time'] = pd.to_datetime(df['Date_Time'], errors='ignore')
        except Exception:
            pass # Keep as raw text if conversion fails on the first try

        filename = 'boursenews_market_data.csv'
        # utf-8-sig ensures French accents display perfectly in Excel
        df.to_csv(filename, index=False, encoding='utf-8-sig') 
        print(f"\nSuccess! Found {len(df)} articles.")
        print(f"Data saved to {filename}")
        print("\nHere is a preview:")
        print(df[['Date_Time', 'Ticker', 'Headline']].head())
    else:
        print("\nNo articles matched your target stocks in this run.")

# Run the function
if __name__ == "__main__":
    scrape_boursenews()