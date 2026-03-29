import sqlite3
from datetime import datetime

def apply_schema_changes(conn):
    c = conn.cursor()
    # Add columns to vouches table if they don't exist
    try:
        c.execute("ALTER TABLE vouches ADD COLUMN confirmed BOOLEAN DEFAULT 0;")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE vouches ADD COLUMN type TEXT DEFAULT 'positive';")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE vouches ADD COLUMN resolved BOOLEAN DEFAULT 0;")
    except sqlite3.OperationalError:
        pass
    # Create user_stats table if not exists
    c.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            username TEXT PRIMARY KEY,
            total_vouches INTEGER DEFAULT 0,
            confirmed_vouches INTEGER DEFAULT 0,
            neg_vouches INTEGER DEFAULT 0,
            trust_score INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

def calculate_score(confirmed, total, neg):
    unconfirmed = total - confirmed
    return (confirmed * 3) + (unconfirmed * 1) - (neg * 4)

def update_user_stats(conn, username):
    c = conn.cursor()
    c.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN confirmed=1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN type='negative' AND resolved=0 THEN 1 ELSE 0 END)
        FROM vouches
        WHERE target_username=?
    """, (username,))
    total, confirmed, neg = c.fetchone()
    total = total or 0
    confirmed = confirmed or 0
    neg = neg or 0
    score = calculate_score(confirmed, total, neg)
    c.execute("""
        INSERT INTO user_stats (username, total_vouches, confirmed_vouches, neg_vouches, trust_score, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET
            total_vouches=excluded.total_vouches,
            confirmed_vouches=excluded.confirmed_vouches,
            neg_vouches=excluded.neg_vouches,
            trust_score=excluded.trust_score,
            last_updated=excluded.last_updated;
    """, (username, total, confirmed, neg, score, datetime.now()))
    conn.commit()

def update_all_users():
    conn = sqlite3.connect("vouches.db")
    apply_schema_changes(conn)
    c = conn.cursor()
    c.execute("SELECT DISTINCT target_username FROM vouches;")
    users = [row[0] for row in c.fetchall()]
    for username in users:
        update_user_stats(conn, username)
    print(f"Updated stats for {len(users)} users.")
    conn.close()

if __name__ == "__main__":
    update_all_users()
