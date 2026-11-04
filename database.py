import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Optional

DATABASE_PATH = Path(__file__).with_name("alerts.db")


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database() -> None:
    with get_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS stock_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                condition TEXT NOT NULL,
                target_price REAL NOT NULL,
                personal_note TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS crypto_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                condition TEXT NOT NULL,
                target_price REAL NOT NULL,
                personal_note TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id INTEGER PRIMARY KEY,
                alert_style TEXT NOT NULL DEFAULT 'concise'
            );

            CREATE TABLE IF NOT EXISTS news_settings (
                guild_id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                importance_threshold INTEGER NOT NULL DEFAULT 8,
                polling_interval INTEGER NOT NULL DEFAULT 1,
                daily_ai_limit INTEGER NOT NULL DEFAULT 100,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS news_articles (
                article_id TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL UNIQUE,
                headline TEXT NOT NULL,
                source TEXT,
                url TEXT,
                published_at TEXT,
                raw_symbols_json TEXT NOT NULL DEFAULT '[]',
                analysis_json TEXT,
                importance INTEGER,
                analyzed_at TEXT,
                first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sent_news (
                guild_id INTEGER NOT NULL,
                article_id TEXT NOT NULL,
                sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (guild_id, article_id)
            );

            CREATE TABLE IF NOT EXISTS ai_usage_daily (
                usage_date TEXT PRIMARY KEY,
                articles_analyzed INTEGER NOT NULL DEFAULT 0,
                requests_made INTEGER NOT NULL DEFAULT 0,
                input_tokens INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS guild_ai_articles (
                guild_id INTEGER NOT NULL,
                article_id TEXT NOT NULL,
                usage_date TEXT NOT NULL,
                PRIMARY KEY (guild_id, article_id)
            );
            """
        )

        # Add missing columns to existing databases
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(news_settings)"
            )
        }

        additions = {
            "importance_threshold": "INTEGER NOT NULL DEFAULT 8",
            "polling_interval": "INTEGER NOT NULL DEFAULT 1",
            "daily_ai_limit": "INTEGER NOT NULL DEFAULT 100",
            "updated_at": "TEXT",
        }

        for column, definition in additions.items():
            if column not in columns:
                connection.execute(
                    f"ALTER TABLE news_settings ADD COLUMN {column} {definition}"
                )
        connection.commit()


def _add_alert(table: str, symbol_column: str, user_id: int, symbol: str,
               condition: str, target_price: float,
               personal_note: Optional[str]) -> int:
    with get_connection() as connection:
        cursor = connection.execute(
            f"""
            INSERT INTO {table}
                (user_id, {symbol_column}, condition, target_price, personal_note)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, symbol.upper(), condition, target_price, personal_note),
        )
        connection.commit()
        return int(cursor.lastrowid)


def add_stock_alert(user_id: int, ticker: str, condition: str,
                    target_price: float, personal_note: Optional[str]) -> int:
    return _add_alert("stock_alerts", "ticker", user_id, ticker,
                      condition, target_price, personal_note)


def add_crypto_alert(user_id: int, symbol: str, condition: str,
                     target_price: float, personal_note: Optional[str]) -> int:
    return _add_alert("crypto_alerts", "symbol", user_id, symbol,
                      condition, target_price, personal_note)


def get_active_stock_alerts() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            "SELECT id,user_id,ticker,condition,target_price,personal_note "
            "FROM stock_alerts WHERE active=1 ORDER BY id"
        ).fetchall()


def get_active_crypto_alerts() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            "SELECT id,user_id,symbol,condition,target_price,personal_note "
            "FROM crypto_alerts WHERE active=1 ORDER BY id"
        ).fetchall()


def get_user_stock_alerts(user_id: int) -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            "SELECT id,ticker,condition,target_price,personal_note,active "
            "FROM stock_alerts WHERE user_id=? ORDER BY id DESC",
            (user_id,),
        ).fetchall()


def get_user_crypto_alerts(user_id: int) -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            "SELECT id,symbol,condition,target_price,personal_note,active "
            "FROM crypto_alerts WHERE user_id=? ORDER BY id DESC",
            (user_id,),
        ).fetchall()


def _remove_alert(table: str, alert_id: int, user_id: int) -> bool:
    with get_connection() as connection:
        cursor = connection.execute(
            f"DELETE FROM {table} WHERE id=? AND user_id=?",
            (alert_id, user_id),
        )
        connection.commit()
        return cursor.rowcount > 0


def remove_stock_alert(alert_id: int, user_id: int) -> bool:
    return _remove_alert("stock_alerts", alert_id, user_id)


def remove_crypto_alert(alert_id: int, user_id: int) -> bool:
    return _remove_alert("crypto_alerts", alert_id, user_id)


def mark_stock_alert_triggered(alert_id: int) -> None:
    with get_connection() as connection:
        connection.execute("UPDATE stock_alerts SET active=0 WHERE id=?", (alert_id,))
        connection.commit()


def mark_crypto_alert_triggered(alert_id: int) -> None:
    with get_connection() as connection:
        connection.execute("UPDATE crypto_alerts SET active=0 WHERE id=?", (alert_id,))
        connection.commit()


def set_user_style(user_id: int, style: str) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO user_preferences (user_id, alert_style) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET alert_style=excluded.alert_style
            """,
            (user_id, style),
        )
        connection.commit()


def enable_news_for_guild(guild_id: int, channel_id: int) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO news_settings (guild_id,channel_id,enabled) VALUES (?,?,1)
            ON CONFLICT(guild_id) DO UPDATE SET
                channel_id=excluded.channel_id,
                enabled=1,
                updated_at=CURRENT_TIMESTAMP
            """,
            (guild_id, channel_id),
        )
        connection.commit()


def disable_news_for_guild(guild_id: int) -> bool:
    with get_connection() as connection:
        cursor = connection.execute(
            "UPDATE news_settings SET enabled=0,updated_at=CURRENT_TIMESTAMP WHERE guild_id=?",
            (guild_id,),
        )
        connection.commit()
        return cursor.rowcount > 0


def get_enabled_news_channels() -> list[sqlite3.Row]:
    with get_connection() as connection:
        return connection.execute(
            "SELECT guild_id,channel_id FROM news_settings WHERE enabled=1"
        ).fetchall()


def headline_fingerprint(headline: str) -> str:
    normalized = " ".join(
        "".join(ch.lower() if ch.isalnum() else " " for ch in headline).split()
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def upsert_news_article(article_id: str, headline: str, source: str,
                        url: str, published_at: str,
                        symbols: list[str]) -> bool:
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO news_articles
                (article_id,fingerprint,headline,source,url,published_at,raw_symbols_json)
            VALUES (?,?,?,?,?,?,?)
            """,
            (article_id, headline_fingerprint(headline), headline, source,
             url, published_at, json.dumps(symbols)),
        )
        connection.commit()
        return cursor.rowcount > 0


def save_news_analysis(article_id: str, analysis: dict[str, Any]) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            UPDATE news_articles SET analysis_json=?,importance=?,analyzed_at=CURRENT_TIMESTAMP
            WHERE article_id=?
            """,
            (json.dumps(analysis), int(analysis["importance"]), article_id),
        )
        connection.commit()


def has_news_been_sent(guild_id: int, article_id: str) -> bool:
    with get_connection() as connection:
        return connection.execute(
            "SELECT 1 FROM sent_news WHERE guild_id=? AND article_id=?",
            (guild_id, article_id),
        ).fetchone() is not None


def mark_news_sent(guild_id: int, article_id: str) -> None:
    with get_connection() as connection:
        connection.execute(
            "INSERT OR IGNORE INTO sent_news (guild_id,article_id) VALUES (?,?)",
            (guild_id, article_id),
        )
        connection.commit()


def get_today_ai_articles() -> int:
    today = date.today().isoformat()
    with get_connection() as connection:
        row = connection.execute(
            "SELECT articles_analyzed FROM ai_usage_daily WHERE usage_date=?",
            (today,),
        ).fetchone()
    return int(row["articles_analyzed"]) if row else 0


def record_ai_usage(articles_analyzed: int, requests_made: int,
                    input_tokens: int = 0, output_tokens: int = 0) -> None:
    today = date.today().isoformat()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO ai_usage_daily
                (usage_date,articles_analyzed,requests_made,input_tokens,output_tokens)
            VALUES (?,?,?,?,?)
            ON CONFLICT(usage_date) DO UPDATE SET
                articles_analyzed=articles_analyzed+excluded.articles_analyzed,
                requests_made=requests_made+excluded.requests_made,
                input_tokens=input_tokens+excluded.input_tokens,
                output_tokens=output_tokens+excluded.output_tokens
            """,
            (today, articles_analyzed, requests_made, input_tokens, output_tokens),
        )
        connection.commit()

def set_guild_news_settings(
    guild_id: int,
    importance: int,
    polling: int,
    daily_limit: int
) -> None:

    if not 1 <= importance <= 10:
        raise ValueError("Importance must be 1-10")

    if not 1 <= polling <= 60:
        raise ValueError("Polling must be 1-60 minutes")

    if not 1 <= daily_limit <= 100:
        raise ValueError("Daily limit must be 1-100")

    with get_connection() as connection:
        connection.execute(
            """
            UPDATE news_settings
            SET importance_threshold=?,
                polling_interval=?,
                daily_ai_limit=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE guild_id=?
            """,
            (importance, polling, daily_limit, guild_id)
        )
        connection.commit()


def get_guild_news_settings(guild_id: int):
    with get_connection() as connection:
        return connection.execute(
            """
            SELECT *
            FROM news_settings
            WHERE guild_id=?
            """,
            (guild_id,)
        ).fetchone()
        
def record_guild_ai_article(
    guild_id: int,
    article_id: str,
    daily_limit: int
) -> bool:

    with get_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")

        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM guild_ai_articles
            WHERE guild_id=? AND usage_date=date('now')
            """,
            (guild_id,)
        ).fetchone()

        if row["total"] >= daily_limit:
            return False

        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO guild_ai_articles
            (guild_id, article_id, usage_date)
            VALUES (?, ?, date('now'))
            """,
            (guild_id, article_id)
        )

        return cursor.rowcount == 1
    
