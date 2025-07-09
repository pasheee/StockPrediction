import sqlite3
import os

def create_writeto_database(parsed_news, db_path="database/news.db"):
    """
    Inserts parsed news data into a SQLite database.
    Creates a table named 'news' if it does not exist, with columns for an auto-incrementing ID,
    a unique timestamp, and the news text. Inserts each (time, news) tuple from the parsed_news
    list into the table, ignoring duplicates based on the timestamp.
    Args:
        parsed_news (list of tuple): List of (time, news) tuples to insert into the database.
        db_path (str, optional): Path to the SQLite database file. Defaults to "news.db".
    Raises:
        sqlite3.DatabaseError: If a database error occurs during the operation.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
                
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time INTEGER UNIQUE,
            news TEXT
        )
    """)

    c.executemany(
        "INSERT OR IGNORE INTO news (time, news) VALUES (?, ?)",
        parsed_news
    )
    conn.commit()
    conn.close()