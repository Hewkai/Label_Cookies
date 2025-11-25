import mysql.connector

def get_db():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="cookies_db"
    )
    cursor = db.cursor(dictionary=True)
    return db, cursor


def reset_labels():
    db, cursor = get_db()

    print("[*] Resetting ALL cookie labels...")

    cursor.execute("""
        UPDATE cookies
        SET label = NULL,
            label_source = NULL,
            label_source_url = NULL
    """)

    db.commit()
    cursor.close()
    db.close()

    print("Reset complete! All labels cleared.\n")


if __name__ == "__main__":
    reset_labels()
