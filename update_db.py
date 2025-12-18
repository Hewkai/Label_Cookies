from mysql.connector import connect

db = connect(
    host="localhost",
    user="root",
    password="",
    database="cookies_db"
)

cursor = db.cursor()
cursor.execute("""
ALTER TABLE cookies
    ADD COLUMN label VARCHAR(100) NULL,
    ADD COLUMN label_source VARCHAR(50) NULL,
    ADD COLUMN label_source_url TEXT NULL;
""")

db.commit()
cursor.close()
db.close()

print("Migration completed")
