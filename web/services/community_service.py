import sqlite3
from pathlib import Path

DB_PATH = Path("database/quantfusion.db")

def get_connection():
    return sqlite3.connect(DB_PATH)

def create_post(data: dict):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO community_posts
        (symbol, entry, stop_loss, target, commentary, image_path)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        data["symbol"],
        data.get("entry"),
        data.get("stop_loss"),
        data.get("target"),
        data.get("commentary"),
        data.get("image_path"),
    ))

    conn.commit()
    conn.close()

def fetch_all_posts():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, symbol, entry, stop_loss, target,
               commentary, image_path, created_at
        FROM community_posts
        ORDER BY created_at DESC
    """)

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": r[0],
            "symbol": r[1],
            "entry": r[2],
            "stop_loss": r[3],
            "target": r[4],
            "commentary": r[5],
            "image_path": r[6],
            "created_at": r[7],
        }
        for r in rows
    ]
