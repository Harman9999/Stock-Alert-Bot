import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

import feedparser
import httpx

import discord
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.historical.news import NewsClient
from alpaca.data.requests import CryptoLatestTradeRequest, NewsRequest, StockLatestTradeRequest
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError
from flask import Flask
from threading import Thread

#Running on Server
app = Flask(__name__)

@app.route("/")
def home():
    return "Discord bot is running!"

def run_web():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

Thread(target=run_web, daemon=True).start()

from database import (
    add_crypto_alert,
    add_stock_alert,
    disable_news_for_guild,
    enable_news_for_guild,
    get_active_crypto_alerts,
    get_active_stock_alerts,
    get_enabled_news_channels,
    get_today_ai_articles,
    get_user_crypto_alerts,
    get_user_stock_alerts,
    has_news_been_sent,
    initialize_database,
    mark_crypto_alert_triggered,
    mark_news_sent,
    mark_stock_alert_triggered,
    record_ai_usage,
    remove_crypto_alert,
    remove_stock_alert,
    save_news_analysis,
    set_user_style,
    upsert_news_article,
    set_guild_news_settings,
    get_guild_news_settings,
)

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "").strip()
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
COINDESK_RSS_URL = os.getenv(
    "COINDESK_RSS_URL",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
).strip()
CUSTOM_RSS_FEEDS = [
    url.strip()
    for url in os.getenv("CUSTOM_RSS_FEEDS", "").split(",")
    if url.strip()
]
NEWS_IMPORTANCE_THRESHOLD = int(os.getenv("NEWS_IMPORTANCE_THRESHOLD", "8"))
NEWS_POLL_MINUTES = int(os.getenv("NEWS_POLL_MINUTES", "5"))
NEWS_LOOKBACK_MINUTES = int(os.getenv("NEWS_LOOKBACK_MINUTES", "20"))
NEWS_BATCH_SIZE = int(os.getenv("NEWS_BATCH_SIZE", "8"))
NEWS_MAX_AI_ARTICLES_PER_CYCLE = int(os.getenv("NEWS_MAX_AI_ARTICLES_PER_CYCLE", "12"))
NEWS_MAX_AI_ARTICLES_PER_DAY = int(os.getenv("NEWS_MAX_AI_ARTICLES_PER_DAY", "100"))

if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN is missing from .env")
if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    raise RuntimeError("Alpaca API credentials are missing from .env")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is missing from .env")
if not 1 <= NEWS_IMPORTANCE_THRESHOLD <= 10:
    raise RuntimeError("NEWS_IMPORTANCE_THRESHOLD must be between 1 and 10")
if not 1 <= NEWS_POLL_MINUTES <= 60:
    raise RuntimeError("NEWS_POLL_MINUTES must be between 1 and 60")
if not 5 <= NEWS_LOOKBACK_MINUTES <= 1440:
    raise RuntimeError("NEWS_LOOKBACK_MINUTES must be between 5 and 1440")
if not 1 <= NEWS_BATCH_SIZE <= 20:
    raise RuntimeError("NEWS_BATCH_SIZE must be between 1 and 20")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logging.getLogger("httpx").disabled = True
logging.getLogger("httpcore").disabled = True
logger = logging.getLogger("stock-alert-bot")

stock_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
crypto_client = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
news_client = NewsClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, raw_data=True)
openai_client = OpenAI(api_key=OPENAI_API_KEY, max_retries=0)


class StockAlertBot(discord.Client):
    def __init__(self) -> None:
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        initialize_database()
        synced = await self.tree.sync()
        logger.info("Synchronized %s global root command(s)", len(synced))
        for command in synced:
            logger.info("Registered command: /%s", command.name)

        if not stock_alert_checker.is_running():
            stock_alert_checker.start()
        if not crypto_alert_checker.is_running():
            crypto_alert_checker.start()
        if not market_news_checker.is_running():
            market_news_checker.change_interval(minutes=NEWS_POLL_MINUTES)
            market_news_checker.start()


bot = StockAlertBot()


def get_latest_stock_price(ticker: str) -> float:
    ticker = ticker.upper().strip()
    result = stock_client.get_stock_latest_trade(
        StockLatestTradeRequest(symbol_or_symbols=[ticker])
    )
    trade = result.get(ticker)
    if trade is None:
        raise ValueError(f"No data for {ticker}")
    return float(trade.price)


def normalize_crypto_symbol(symbol: str) -> str:
    clean = symbol.upper().strip().replace(" ", "").replace("-", "/")
    if "/" in clean:
        parts = clean.split("/")
        if len(parts) != 2 or not all(parts):
            raise ValueError("Invalid crypto pair")
        return f"{parts[0]}/{parts[1]}"
    for quote in ("USDT", "USDC", "USD", "BTC", "ETH"):
        if clean.endswith(quote) and clean[:-len(quote)]:
            return f"{clean[:-len(quote)]}/{quote}"
    if clean.isalnum():
        return f"{clean}/USD"
    raise ValueError("Invalid crypto pair")


def get_latest_crypto_price(symbol: str) -> tuple[str, float]:
    pair = normalize_crypto_symbol(symbol)
    result = crypto_client.get_crypto_latest_trade(
        CryptoLatestTradeRequest(symbol_or_symbols=[pair])
    )
    trade = result.get(pair)
    if trade is None:
        raise ValueError(f"No data for {pair}")
    return pair, float(trade.price)


def alert_triggered(condition: str, current: float, target: float) -> bool:
    return current >= target if condition == "above" else current <= target


IMPORTANT_TERMS = {
    "acquisition", "acquires", "bankruptcy", "breach", "buyback", "cpi",
    "earnings", "etf", "fda", "federal reserve", "fomc", "fraud", "guidance",
    "hack", "inflation", "investigation", "lawsuit", "layoffs", "merger",
    "rate cut", "rate hike", "recall", "regulator", "sec", "tariff",
}
LOW_VALUE_TERMS = {
    "conference appearance", "fireside chat", "investor conference",
    "price target raised", "price target lowered", "scheduled to present", "webinar",
}
MAJOR_ASSETS = {
    "AAPL", "AMZN", "AMD", "AVGO", "COIN", "GOOG", "GOOGL", "META", "MSFT",
    "NFLX", "NVDA", "PLTR", "QQQ", "SPY", "TSLA", "BTC", "BTCUSD", "ETH",
    "ETHUSD", "SOL", "SOLUSD", "XRP", "XRPUSD",
}


def article_value(article: object, field: str, default: Any = None) -> Any:
    return article.get(field, default) if isinstance(article, dict) else getattr(article, field, default)


def extract_news(response: object) -> list[dict[str, Any]]:
    if isinstance(response, dict):
        news = response.get("news", [])
        return news if isinstance(news, list) else []
    data = getattr(response, "data", None)
    if isinstance(data, dict):
        news = data.get("news", [])
        return news if isinstance(news, list) else []
    return []


def normalize_article(
    *,
    source_id: str,
    source_name: str,
    headline: str,
    summary: str,
    url: str,
    created_at: str,
    symbols: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Return one common article format for every news provider."""
    return {
        "id": f"{source_name.lower()}:{source_id}",
        "headline": headline.strip(),
        "summary": summary.strip(),
        "url": url.strip(),
        "source": source_name.strip(),
        "created_at": created_at,
        "symbols": symbols or [],
    }


def fetch_alpaca_news() -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    request = NewsRequest(
        start=now - timedelta(minutes=NEWS_LOOKBACK_MINUTES),
        end=now,
        limit=50,
        include_content=False,
        exclude_contentless=False,
    )

    normalized: list[dict[str, Any]] = []

    for item in extract_news(news_client.get_news(request)):
        normalized.append(
            normalize_article(
                source_id=str(article_value(item, "id", article_id(item))),
                source_name=str(
                    article_value(item, "source", "Alpaca/Benzinga")
                    or "Alpaca/Benzinga"
                ),
                headline=str(article_value(item, "headline", "")),
                summary=str(article_value(item, "summary", "") or ""),
                url=str(article_value(item, "url", "") or ""),
                created_at=str(article_value(item, "created_at", "") or ""),
                symbols=article_symbols(item),
            )
        )

    logger.info("Alpaca returned %s article(s).", len(normalized))
    return normalized


def fetch_finnhub_news() -> list[dict[str, Any]]:
    """
    Fetch general market news from Finnhub.

    This source is optional. Leave FINNHUB_API_KEY blank to disable it.
    """
    if not FINNHUB_API_KEY:
        return []

    response = httpx.get(
        "https://finnhub.io/api/v1/news",
        params={
            "category": "general",
            "token": FINNHUB_API_KEY,
        },
        timeout=20.0,
    )
    response.raise_for_status()

    payload = response.json()
    normalized: list[dict[str, Any]] = []

    if not isinstance(payload, list):
        return normalized

    cutoff = datetime.now(timezone.utc) - timedelta(
        minutes=NEWS_LOOKBACK_MINUTES
    )

    for item in payload:
        published_unix = int(item.get("datetime", 0) or 0)

        if published_unix:
            published = datetime.fromtimestamp(
                published_unix,
                tz=timezone.utc,
            )

            if published < cutoff:
                continue

            created_at = published.isoformat()
        else:
            created_at = ""

        normalized.append(
            normalize_article(
                source_id=str(
                    item.get("id")
                    or item.get("url")
                    or item.get("headline", "")
                ),
                source_name=str(item.get("source") or "Finnhub"),
                headline=str(item.get("headline") or ""),
                summary=str(item.get("summary") or ""),
                url=str(item.get("url") or ""),
                created_at=created_at,
                symbols=[],
            )
        )

    logger.info("Finnhub returned %s recent article(s).", len(normalized))
    return normalized


def _rss_entry_datetime(entry: object) -> str:
    parsed = getattr(entry, "published_parsed", None)
    if parsed is None:
        parsed = getattr(entry, "updated_parsed", None)

    if parsed is None:
        return ""

    return datetime.fromtimestamp(
        time.mktime(parsed),
        tz=timezone.utc,
    ).isoformat()


def fetch_rss_feed(
    url: str,
    source_name: str,
) -> list[dict[str, Any]]:
    feed = feedparser.parse(url)

    if getattr(feed, "bozo", False):
        logger.warning(
            "RSS parser reported a problem for %s: %s",
            url,
            getattr(feed, "bozo_exception", "unknown error"),
        )

    cutoff = datetime.now(timezone.utc) - timedelta(
        minutes=NEWS_LOOKBACK_MINUTES
    )
    normalized: list[dict[str, Any]] = []

    for entry in getattr(feed, "entries", []):
        created_at = _rss_entry_datetime(entry)

        if created_at:
            try:
                published = datetime.fromisoformat(created_at)

                if published < cutoff:
                    continue
            except ValueError:
                pass

        headline = str(getattr(entry, "title", "") or "")
        url_value = str(getattr(entry, "link", "") or "")
        summary = str(
            getattr(entry, "summary", "")
            or getattr(entry, "description", "")
            or ""
        )

        entry_id = str(
            getattr(entry, "id", "")
            or url_value
            or headline
        )

        normalized.append(
            normalize_article(
                source_id=entry_id,
                source_name=source_name,
                headline=headline,
                summary=summary,
                url=url_value,
                created_at=created_at,
                symbols=[],
            )
        )

    logger.info(
        "%s RSS returned %s recent article(s).",
        source_name,
        len(normalized),
    )
    return normalized


def get_recent_market_news() -> list[dict[str, Any]]:
    """
    Merge Alpaca, optional Finnhub, CoinDesk RSS, and custom RSS sources.

    Source failures are isolated so one broken provider does not stop the
    entire news feed.
    """
    articles: list[dict[str, Any]] = []

    source_calls = [
        ("Alpaca", fetch_alpaca_news),
        ("Finnhub", fetch_finnhub_news),
        (
            "CoinDesk",
            lambda: fetch_rss_feed(
                COINDESK_RSS_URL,
                "CoinDesk",
            ),
        ),
    ]

    for index, rss_url in enumerate(CUSTOM_RSS_FEEDS, start=1):
        source_calls.append(
            (
                f"Custom RSS {index}",
                lambda url=rss_url, number=index: fetch_rss_feed(
                    url,
                    f"Custom RSS {number}",
                ),
            )
        )

    for source_label, source_call in source_calls:
        try:
            articles.extend(source_call())
        except Exception:
            logger.exception(
                "Could not retrieve news from %s.",
                source_label,
            )

    logger.info(
        "Combined news sources returned %s article(s).",
        len(articles),
    )
    return articles


def article_symbols(article: object) -> list[str]:
    raw = article_value(article, "symbols", [])
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [str(item).upper().strip() for item in raw if str(item).strip()]


def article_id(article: object) -> str:
    value = article_value(article, "id")
    if value is not None:
        return str(value)
    return "|".join([
        str(article_value(article, "created_at", "")),
        str(article_value(article, "headline", "")),
        str(article_value(article, "url", "")),
    ])


def passes_free_prefilter(article: object) -> bool:
    headline = str(article_value(article, "headline", "")).lower()
    summary = str(article_value(article, "summary", "") or "").lower()
    combined = f"{headline} {summary}"
    symbols = set(article_symbols(article))

    if any(term in combined for term in LOW_VALUE_TERMS):
        return False
    if symbols.intersection(MAJOR_ASSETS):
        return True
    if any(term in combined for term in IMPORTANT_TERMS):
        return True
    return any(symbol.startswith(("BTC", "ETH", "SOL", "XRP")) for symbol in symbols)


def prepare_candidates(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for article in articles:
        if not passes_free_prefilter(article):
            continue

        aid = article_id(article)
        headline = str(article_value(article, "headline", "")).strip()
        if not headline:
            continue

        source = str(article_value(article, "source", "Alpaca News") or "Alpaca News").strip()
        url = str(article_value(article, "url", "") or "").strip()
        created_at = str(article_value(article, "created_at", "") or "")
        symbols = article_symbols(article)

        is_new = upsert_news_article(aid, headline, source, url, created_at, symbols)
        if not is_new:
            continue

        candidates.append({
            "id": aid,
            "headline": headline,
            "summary": str(article_value(article, "summary", "") or "").strip()[:1200],
            "source": source,
            "url": url,
            "created_at": created_at,
            "symbols": symbols,
        })

    return candidates[:NEWS_MAX_AI_ARTICLES_PER_CYCLE]


def chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def analyze_news_batch(batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    compact = [
        {
            "id": item["id"],
            "headline": item["headline"],
            "summary": item["summary"],
            "symbols": item["symbols"],
            "source": item["source"],
        }
        for item in batch
    ]

    prompt = (
        "Analyze these financial-news articles conservatively. Use only supplied facts. "
        "Do not give buy/sell advice. Importance: 1-3 routine/noise, 4-6 limited relevance, "
        "7-8 meaningful and potentially market-moving, 9-10 major event. Keep ai_summary under "
        "55 words and why_it_matters under 45 words.\n\n" + json.dumps(compact)
    )

    response = openai_client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "system", "content": "You are a conservative financial-news classifier."},
            {"role": "user", "content": prompt},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "batched_market_news_analysis",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "results": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "string"},
                                    "importance": {"type": "integer", "minimum": 1, "maximum": 10},
                                    "category": {"type": "string", "enum": ["stock", "crypto", "general"]},
                                    "sentiment": {"type": "string", "enum": ["bullish", "bearish", "neutral", "mixed"]},
                                    "ai_summary": {"type": "string"},
                                    "why_it_matters": {"type": "string"},
                                    "affected_assets": {"type": "array", "items": {"type": "string"}},
                                },
                                "required": ["id", "importance", "category", "sentiment", "ai_summary", "why_it_matters", "affected_assets"],
                                "additionalProperties": False,
                            },
                        }
                    },
                    "required": ["results"],
                    "additionalProperties": False,
                },
            }
        },
    )

    parsed = json.loads(response.output_text)
    usage = getattr(response, "usage", None)
    input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
    output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
    return parsed["results"], input_tokens, output_tokens


def category_label(value: str) -> str:
    return {"stock": "📈 Stock", "crypto": "🪙 Crypto", "general": "🌐 General"}.get(value, "🌐 General")


def sentiment_label(value: str) -> str:
    return {"bullish": "🟢 Bullish", "bearish": "🔴 Bearish", "neutral": "⚪ Neutral", "mixed": "🟡 Mixed"}.get(value, "⚪ Neutral")


def parse_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result if result.tzinfo else result.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


@bot.tree.command(name="ping", description="Check whether the bot is online")
async def ping(interaction: discord.Interaction) -> None:
    await interaction.response.send_message("🏓 Stock Alerts Bot is online")


@bot.tree.command(name="help", description="Show all bot commands")
async def help_command(interaction: discord.Interaction) -> None:
    embed = discord.Embed(
        title="📈 Stock Alerts Bot",
        description="Public prices, private alerts, and cost-controlled AI market news",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="📊 Prices", value="`/price` • `/crypto_price`", inline=False)
    embed.add_field(name="🔔 Stock alerts", value="`/alert_add` • `/alerts` • `/alert_remove`", inline=False)
    embed.add_field(name="🪙 Crypto alerts", value="`/crypto_alert_add` • `/crypto_alerts` • `/crypto_alert_remove`", inline=False)
    embed.add_field(name="📰 AI news", value="`/news enable #channel` • `/news disable`\nRequires **Manage Server**", inline=False)
    embed.add_field(name="⚙️ Settings", value="`/profile_style`", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="price",
    description="Check the latest stock price.",
)
@app_commands.describe(
    ticker="Examples: AAPL, TSLA, NVDA, MSFT, AMZN, META",
)
async def price_command(
    interaction: discord.Interaction,
    ticker: str,
) -> None:

    await interaction.response.defer(ephemeral=False)

    ticker = ticker.upper().strip()

    try:
        price = await asyncio.to_thread(
            get_latest_stock_price,
            ticker,
        )

    except Exception:
        await interaction.followup.send(
            f"❌ I couldn't retrieve **{ticker}**.\n\n"
            "**Examples:** AAPL, TSLA, NVDA, MSFT, AMZN, META",
            ephemeral=False,
        )
        return

    recommendations = {
        "AAPL": "MSFT • GOOGL • AMZN",
        "MSFT": "AAPL • NVDA • ORCL",
        "NVDA": "AMD • TSM • AVGO",
        "AMD": "NVDA • TSM • INTC",
        "TSLA": "RIVN • LCID • F",
        "META": "GOOGL • SNAP • PINS",
        "AMZN": "AAPL • WMT • COST",
        "GOOGL": "META • MSFT • AAPL",
        "NFLX": "DIS • PARA • WBD",
        "PLTR": "SNOW • MDB • CFLT",
    }

    embed = discord.Embed(
        title=f"📈 {ticker} Stock Price",
        color=discord.Color.green(),
    )

    embed.add_field(
        name="Latest Available Price",
        value=f"**${price:,.2f}**",
        inline=True,
    )

    embed.add_field(
        name="Symbol",
        value=ticker,
        inline=True,
    )

    embed.add_field(
        name="Requested By",
        value=interaction.user.mention,
        inline=True,
    )

    embed.add_field(
        name="⭐ Similar Stocks",
        value=recommendations.get(
            ticker,
            "SPY • QQQ • DIA",
        ),
        inline=False,
    )

    embed.set_footer(
        text="Market data may be delayed."
    )

    await interaction.followup.send(
        embed=embed,
        ephemeral=False,
    )


@bot.tree.command(
    name="crypto_price",
    description="Check the latest cryptocurrency price.",
)
@app_commands.describe(
    symbol="Examples: BTCUSD, ETHUSD, SOLUSD, XRPUSD, DOGEUSD, BONKUSD",
)
async def crypto_price_command(
    interaction: discord.Interaction,
    symbol: str,
) -> None:

    await interaction.response.defer(ephemeral=False)

    try:
        pair, price = await asyncio.to_thread(
            get_latest_crypto_price,
            symbol,
        )

    except Exception:
        await interaction.followup.send(
            "❌ I couldn't retrieve that cryptocurrency pair.\n\n"
            "**Examples:** BTCUSD, ETHUSD, SOLUSD, XRPUSD, DOGEUSD, BONKUSD",
            ephemeral=False,
        )
        return

    recommendations = {
        "BTC/USD": "ETH/USD • SOL/USD • XRP/USD",
        "ETH/USD": "BTC/USD • SOL/USD • LINK/USD",
        "SOL/USD": "ETH/USD • BONK/USD • JUP/USD",
        "XRP/USD": "XLM/USD • ADA/USD • HBAR/USD",
        "DOGE/USD": "SHIB/USD • BONK/USD • PEPE/USD",
        "BONK/USD": "DOGE/USD • SHIB/USD • PEPE/USD",
        "ADA/USD": "XRP/USD • SOL/USD • DOT/USD",
        "AVAX/USD": "SOL/USD • DOT/USD • ATOM/USD",
        "LINK/USD": "ETH/USD • AVAX/USD • SOL/USD",
        "LTC/USD": "BTC/USD • BCH/USD • DOGE/USD",
    }

    embed = discord.Embed(
        title=f"🪙 {pair} Crypto Price",
        color=discord.Color.gold(),
    )

    embed.add_field(
        name="Latest Available Price",
        value=f"**${price:,.8f}**",
        inline=True,
    )

    embed.add_field(
        name="Pair",
        value=pair,
        inline=True,
    )

    embed.add_field(
        name="Requested By",
        value=interaction.user.mention,
        inline=True,
    )

    embed.add_field(
        name="⭐ Similar Cryptocurrencies",
        value=recommendations.get(
            pair,
            "BTC/USD • ETH/USD • SOL/USD",
        ),
        inline=False,
    )

    embed.set_footer(
        text="Crypto prices can change quickly. Market data may be delayed."
    )

    await interaction.followup.send(
        embed=embed,
        ephemeral=False,
    )


@bot.tree.command(
    name="alert_add",
    description="Create a private stock price alert.",
)
@app_commands.describe(
    ticker="Examples: AAPL, TSLA, NVDA, MSFT, AMZN, META",
    condition="Trigger when the stock goes above or below your target",
    target_price="Example: 250 or 185.50",
    personal_note="Optional reminder that only you can see",
)
async def alert_add_command(
    interaction: discord.Interaction,
    ticker: str,
    condition: Literal["above", "below"],
    target_price: float,
    personal_note: Optional[str] = None,
) -> None:
    await interaction.response.defer(ephemeral=True)

    if target_price <= 0:
        await interaction.followup.send(
            "❌ Target price must be greater than zero.",
            ephemeral=True,
        )
        return

    clean_ticker = ticker.upper().strip()

    try:
        current_price = await asyncio.to_thread(
            get_latest_stock_price,
            clean_ticker,
        )

    except Exception:
        await interaction.followup.send(
            (
                "❌ I couldn't find that stock ticker.\n\n"
                "**Examples:** AAPL, TSLA, NVDA, MSFT, AMZN, META"
            ),
            ephemeral=True,
        )
        return

    alert_id = await asyncio.to_thread(
        add_stock_alert,
        interaction.user.id,
        clean_ticker,
        condition,
        target_price,
        personal_note,
    )

    embed = discord.Embed(
        title="🔔 Stock Alert Created",
        description=(
            f"I'll notify you privately when **{clean_ticker}** "
            f"is **{condition} ${target_price:,.2f}**."
        ),
        color=discord.Color.green(),
    )

    embed.add_field(
        name="Alert ID",
        value=f"#{alert_id}",
        inline=True,
    )

    embed.add_field(
        name="Current Price",
        value=f"${current_price:,.2f}",
        inline=True,
    )

    embed.add_field(
        name="Condition",
        value=condition.capitalize(),
        inline=True,
    )

    if personal_note:
        embed.add_field(
            name="📝 Personal Note",
            value=personal_note,
            inline=False,
        )

    embed.set_footer(
        text=(
            "Your stock alerts are private and only visible to you. "
            "Market prices may be delayed."
        )
    )

    await interaction.followup.send(
        embed=embed,
        ephemeral=True,
    )


@bot.tree.command(name="alerts", description="View your private stock alerts")
async def alerts_command(interaction: discord.Interaction) -> None:
    rows = await asyncio.to_thread(get_user_stock_alerts, interaction.user.id)
    if not rows:
        await interaction.response.send_message("You have no stock alerts", ephemeral=True)
        return
    lines = []
    for row in rows:
        status = "Active" if row["active"] else "Triggered"
        text = f"**#{row['id']} {row['ticker']}** — {row['condition']} ${row['target_price']:,.2f} • {status}"
        if row["personal_note"]:
            text += f"\nNote: {row['personal_note']}"
        lines.append(text)
    await interaction.response.send_message("\n\n".join(lines)[:1900], ephemeral=True)


@bot.tree.command(name="alert_remove", description="Remove a stock alert")
async def alert_remove_command(interaction: discord.Interaction, alert_id: int) -> None:
    removed = await asyncio.to_thread(remove_stock_alert, alert_id, interaction.user.id)
    await interaction.response.send_message(f"Alert #{alert_id} removed" if removed else "Alert not found", ephemeral=True)


@bot.tree.command(
    name="crypto_alert_add",
    description="Create a private cryptocurrency price alert."
)
@app_commands.describe(
    symbol="Examples: BTCUSD, ETHUSD, SOLUSD, XRPUSD, DOGEUSD, BONKUSD",
    condition="Trigger when the price goes above or below your target",
    target_price="Example: 65000 or 3500",
    personal_note="Optional reminder that only you can see"
)
async def crypto_alert_add_command(
    interaction: discord.Interaction,
    symbol: str,
    condition: Literal["above", "below"],
    target_price: float,
    personal_note: Optional[str] = None,
) -> None:

    await interaction.response.defer(ephemeral=True)

    if target_price <= 0:
        await interaction.followup.send(
            "❌ Target price must be greater than zero.",
            ephemeral=True,
        )
        return

    try:
        pair, current = await asyncio.to_thread(
            get_latest_crypto_price,
            symbol,
        )

    except Exception:
        await interaction.followup.send(
            "❌ I couldn't find that cryptocurrency pair.\n\n"
            "**Examples:** BTCUSD, ETHUSD, SOLUSD, XRPUSD, DOGEUSD, BONKUSD",
            ephemeral=True,
        )
        return

    alert_id = await asyncio.to_thread(
        add_crypto_alert,
        interaction.user.id,
        pair,
        condition,
        target_price,
        personal_note,
    )

    embed = discord.Embed(
        title="🪙 Crypto Alert Created",
        description=(
            f"I'll notify you privately when **{pair}** "
            f"is **{condition}** **${target_price:,.8f}**."
        ),
        color=discord.Color.green(),
    )

    embed.add_field(
        name="Alert ID",
        value=f"#{alert_id}",
        inline=True,
    )

    embed.add_field(
        name="Current Price",
        value=f"${current:,.8f}",
        inline=True,
    )

    embed.add_field(
        name="Condition",
        value=condition.capitalize(),
        inline=True,
    )

    if personal_note:
        embed.add_field(
            name="📝 Personal Note",
            value=personal_note,
            inline=False,
        )

    embed.set_footer(
        text="Your crypto alerts are private and only visible to you."
    )

    await interaction.followup.send(
        embed=embed,
        ephemeral=True,
    )


@bot.tree.command(name="crypto_alerts", description="View your private crypto alerts")
async def crypto_alerts_command(interaction: discord.Interaction) -> None:
    rows = await asyncio.to_thread(get_user_crypto_alerts, interaction.user.id)
    if not rows:
        await interaction.response.send_message("You have no crypto alerts", ephemeral=True)
        return
    lines = []
    for row in rows:
        status = "Active" if row["active"] else "Triggered"
        text = f"**#{row['id']} {row['symbol']}** — {row['condition']} ${row['target_price']:,.8f} • {status}"
        if row["personal_note"]:
            text += f"\nNote: {row['personal_note']}"
        lines.append(text)
    await interaction.response.send_message("\n\n".join(lines)[:1900], ephemeral=True)


@bot.tree.command(name="crypto_alert_remove", description="Remove a crypto alert")
async def crypto_alert_remove_command(interaction: discord.Interaction, alert_id: int) -> None:
    removed = await asyncio.to_thread(remove_crypto_alert, alert_id, interaction.user.id)
    await interaction.response.send_message(f"Crypto alert #{alert_id} removed" if removed else "Crypto alert not found", ephemeral=True)


@bot.tree.command(name="profile_style", description="Set your alert message style")
async def profile_style_command(
    interaction: discord.Interaction,
    style: Literal["concise", "detailed", "calm", "energetic"],
) -> None:
    await asyncio.to_thread(set_user_style, interaction.user.id, style)
    await interaction.response.send_message(f"Your alert style is now **{style}**", ephemeral=True)


news_group = app_commands.Group(name="news", description="Enable or disable AI-filtered market news")
@news_group.command(
    name="settings",
    description="Customize AI news settings for this server"
)
@app_commands.checks.has_permissions(manage_guild=True)
async def news_settings(
    interaction: discord.Interaction,
    importance: app_commands.Range[int, 1, 10],
    polling: app_commands.Range[int, 1, 60],
    daily_limit: app_commands.Range[int, 1, 100]
) -> None:

    if interaction.guild is None:
        await interaction.response.send_message(
            "Use this command inside a Discord server.",
            ephemeral=True
        )
        return

    guild_id = interaction.guild.id

    settings = await asyncio.to_thread(
        get_guild_news_settings,
        guild_id
    )

    if settings is None:
        await interaction.response.send_message(
            "Enable news first using /news enable.",
            ephemeral=True
        )
        return

    await asyncio.to_thread(
        set_guild_news_settings,
        guild_id,
        importance,
        polling,
        daily_limit
    )

    embed = discord.Embed(
        title="⚙️ News Settings Updated",
        color=discord.Color.green()
    )

    embed.add_field(
        name="Importance Threshold",
        value=f"{importance}/10",
        inline=False
    )

    embed.add_field(
        name="Polling Interval",
        value=f"{polling} minute(s)",
        inline=False
    )

    embed.add_field(
        name="Daily AI Limit",
        value=f"{daily_limit} articles",
        inline=False
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True
    )


@news_group.command(
    name="enable",
    description="Enable important AI-filtered news in a specific channel.",
)
@app_commands.checks.has_permissions(manage_guild=True)
@app_commands.describe(
    channel="The channel where AI news should be posted."
)
async def news_enable(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "Use this command inside a server.",
            ephemeral=True,
        )
        return

    await asyncio.to_thread(
        enable_news_for_guild,
        interaction.guild.id,
        channel.id,
    )
    settings = await asyncio.to_thread(
        get_guild_news_settings,
        interaction.guild.id
    )

    embed = discord.Embed(
        title="📰 AI News Enabled",
        description=(
            f"Important AI-filtered stock and crypto news "
            f"will now be posted in {channel.mention}."
        ),
        color=discord.Color.green(),
    )

    embed.add_field(
        name="Importance Threshold",
        value=f"{settings['importance_threshold']}/10",
        inline=True
    )

    embed.add_field(
        name="Polling Interval",
        value=f"{settings['polling_interval']} minute(s)",
        inline=True
    )

    embed.add_field(
        name="Daily AI Limit",
        value=f"{settings['daily_ai_limit']} articles",
        inline=True
    )

    embed.set_footer(
        text="Use /news disable to stop the news feed."
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=False,
    )


@news_group.command(name="disable", description="Disable AI-filtered news for this server")
@app_commands.checks.has_permissions(manage_guild=True)
async def news_disable(interaction: discord.Interaction) -> None:
    if interaction.guild is None:
        await interaction.response.send_message("Use this command inside a server", ephemeral=True)
        return
    disabled = await asyncio.to_thread(disable_news_for_guild, interaction.guild.id)
    await interaction.response.send_message("🔕 AI news disabled" if disabled else "News was not enabled", ephemeral=False)


bot.tree.add_command(news_group)


@tasks.loop(seconds=60)
async def stock_alert_checker() -> None:
    for alert in await asyncio.to_thread(
        get_active_stock_alerts
    ):
        try:
            current = await asyncio.to_thread(
                get_latest_stock_price,
                str(alert["ticker"]),
            )

            if not alert_triggered(
                str(alert["condition"]),
                current,
                float(alert["target_price"]),
            ):
                continue

            user = bot.get_user(
                int(alert["user_id"])
            ) or await bot.fetch_user(
                int(alert["user_id"])
            )

            embed = discord.Embed(
                title=f"🔔 {alert['ticker']} stock alert",
                description=(
                    f"Your stock alert triggered because "
                    f"**{alert['ticker']}** is now "
                    f"**{str(alert['condition']).lower()}** "
                    f"your target price."
                ),
                color=discord.Color.green(),
            )

            embed.add_field(
                name="Latest available price",
                value=f"${current:,.2f}",
                inline=True,
            )

            embed.add_field(
                name="Your target",
                value=f"${float(alert['target_price']):,.2f}",
                inline=True,
            )

            embed.add_field(
                name="Condition",
                value=str(alert["condition"]).capitalize(),
                inline=True,
            )

            if alert["personal_note"]:
                embed.add_field(
                    name="Your note",
                    value=str(alert["personal_note"]),
                    inline=False,
                )

            embed.set_footer(
                text=(
                    "Market prices may be delayed. "
                    "Informational only, not financial advice."
                )
            )

            await user.send(embed=embed)

            await asyncio.to_thread(
                mark_stock_alert_triggered,
                int(alert["id"]),
            )

        except discord.Forbidden:
            logger.warning(
                "Could not DM stock alert user."
            )

        except Exception:
            logger.exception(
                "Stock alert checker failed."
            )


@stock_alert_checker.before_loop
async def before_stock_alert_checker() -> None:
    await bot.wait_until_ready()


@tasks.loop(seconds=60)
async def crypto_alert_checker() -> None:
    for alert in await asyncio.to_thread(
        get_active_crypto_alerts
    ):
        try:
            pair, current = await asyncio.to_thread(
                get_latest_crypto_price,
                str(alert["symbol"]),
            )

            if not alert_triggered(
                str(alert["condition"]),
                current,
                float(alert["target_price"]),
            ):
                continue

            user = bot.get_user(
                int(alert["user_id"])
            ) or await bot.fetch_user(
                int(alert["user_id"])
            )

            embed = discord.Embed(
                title=f"🪙 {pair} crypto alert",
                description=(
                    f"Your crypto alert triggered because **{pair}** "
                    f"is now **{str(alert['condition']).lower()}** "
                    f"your target price."
                ),
                color=discord.Color.green(),
            )

            embed.add_field(
                name="Latest available price",
                value=f"${current:,.8f}",
                inline=True,
            )

            embed.add_field(
                name="Your target",
                value=f"${float(alert['target_price']):,.8f}",
                inline=True,
            )

            embed.add_field(
                name="Condition",
                value=str(alert["condition"]).capitalize(),
                inline=True,
            )

            if alert["personal_note"]:
                embed.add_field(
                    name="Your note",
                    value=str(alert["personal_note"]),
                    inline=False,
                )

            embed.set_footer(
                text=(
                    "Crypto prices can move quickly. "
                    "Informational only, not financial advice."
                )
            )

            await user.send(embed=embed)

            await asyncio.to_thread(
                mark_crypto_alert_triggered,
                int(alert["id"]),
            )

        except discord.Forbidden:
            logger.warning(
                "Could not DM crypto alert user."
            )

        except Exception:
            logger.exception(
                "Crypto alert checker failed."
            )


@crypto_alert_checker.before_loop
async def before_crypto_alert_checker() -> None:
    await bot.wait_until_ready()


@tasks.loop(minutes=5)
async def market_news_checker() -> None:
    channels = await asyncio.to_thread(get_enabled_news_channels)
    if not channels:
        return

    remaining = NEWS_MAX_AI_ARTICLES_PER_DAY - await asyncio.to_thread(get_today_ai_articles)
    if remaining <= 0:
        logger.warning("Daily AI article cap reached (%s)", NEWS_MAX_AI_ARTICLES_PER_DAY)
        return

    try:
        raw_articles = await asyncio.to_thread(get_recent_market_news)
    except Exception:
        logger.exception("Alpaca news request failed")
        return

    candidates = (await asyncio.to_thread(prepare_candidates, raw_articles))[:remaining]
    if not candidates:
        logger.info("News cycle: %s fetched, 0 passed free filtering/dedupe", len(raw_articles))
        return

    logger.info("News cycle: %s combined articles fetched, %s unique candidates sent to AI", len(raw_articles), len(candidates))
    analyses: dict[str, dict[str, Any]] = {}

    for batch in chunks(candidates, NEWS_BATCH_SIZE):
        try:
            results, input_tokens, output_tokens = await asyncio.to_thread(analyze_news_batch, batch)
        except RateLimitError as error:
            if "insufficient_quota" in str(error):
                logger.error("OpenAI quota unavailable; stopping news cycle")
            else:
                logger.warning("OpenAI rate limit reached")
            return
        except Exception:
            logger.exception("OpenAI batch analysis failed")
            continue

        await asyncio.to_thread(record_ai_usage, len(batch), 1, input_tokens, output_tokens)
        for result in results:
            aid = str(result["id"])
            analyses[aid] = result
            await asyncio.to_thread(save_news_analysis, aid, result)
            logger.info("AI analyzed article %s with score %s", aid, result["importance"])

    by_id = {item["id"]: item for item in candidates}

    for aid, analysis in analyses.items():
        importance = int(analysis["importance"])
        if importance < NEWS_IMPORTANCE_THRESHOLD:
            continue
        article = by_id.get(aid)
        if article is None:
            continue

        assets = analysis.get("affected_assets", []) or article["symbols"]
        assets_text = " ".join(f"`{str(asset).upper()}`" for asset in assets[:12]) if assets else "General market"

        embed = discord.Embed(
            title=f"🚨 {article['headline'][:245]}",
            description=str(analysis["ai_summary"])[:1000],
            url=article["url"] or None,
            color=discord.Color.orange(),
        )
        embed.add_field(name="🤖 Why it matters", value=str(analysis["why_it_matters"])[:1024], inline=False)
        embed.add_field(name="Category", value=category_label(str(analysis["category"])), inline=True)
        embed.add_field(name="Sentiment", value=sentiment_label(str(analysis["sentiment"])), inline=True)
        embed.add_field(name="Importance", value=f"{importance}/10", inline=True)
        embed.add_field(name="Affected assets", value=assets_text[:1024], inline=False)
        embed.add_field(name="Source", value=article["source"][:1024], inline=False)
        parsed = parse_time(article["created_at"])
        if parsed:
            embed.timestamp = parsed
        embed.set_footer(text="AI-filtered news • Verify the source • Not financial advice")

        for setting in channels:
            guild_id = int(setting["guild_id"])
            channel_id = int(setting["channel_id"])
            if await asyncio.to_thread(has_news_been_sent, guild_id, aid):
                continue
            channel = bot.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    logger.warning("Cannot access news channel %s", channel_id)
                    continue
            try:
                await channel.send(embed=embed)
                await asyncio.to_thread(mark_news_sent, guild_id, aid)
            except discord.Forbidden:
                logger.warning("Missing Send Messages or Embed Links in %s", channel_id)
            except discord.HTTPException:
                logger.exception("Discord failed to send news")


@market_news_checker.before_loop
async def before_market_news_checker() -> None:
    await bot.wait_until_ready()


@bot.event
async def on_ready() -> None:
    if bot.user:
        logger.info("Bot logged in as %s with ID %s", bot.user, bot.user.id)
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="stocks, crypto, and filtered news",
            )
        )


@bot.event
async def on_guild_join(guild: discord.Guild) -> None:
    me = guild.me
    channel = guild.system_channel
    if channel is None or me is None or not channel.permissions_for(me).send_messages:
        channel = next(
            (
                item for item in guild.text_channels
                if me and item.permissions_for(me).send_messages and item.permissions_for(me).embed_links
            ),
            None,
        )
    if channel is None:
        return
    embed = discord.Embed(
        title="👋 Hello! I am Stock Alerts Bot",
        description="I provide public stock and crypto prices, private price alerts, and cost-controlled AI market news",
        color=discord.Color.green(),
    )
    embed.add_field(name="Get started", value="Use **`/help`** to view commands\nA server manager can use **`/news enable`**", inline=False)
    await channel.send(embed=embed)


@bot.tree.error
async def command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    if isinstance(error, app_commands.MissingPermissions):
        message = "You need **Manage Server** to use that command"
    else:
        logger.exception("Slash-command error", exc_info=error)
        message = "Something went wrong. Check the PowerShell terminal"
    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
