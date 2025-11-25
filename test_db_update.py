import mysql.connector

def get_db():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="cookies_db",
    )
    return db, db.cursor(dictionary=True)

if __name__ == "__main__":
    db, cursor = get_db()

    # 1) ดูว่า row id=1 ปัจจุบันเป็นอะไร
    cursor.execute("SELECT id, name, label, label_source FROM cookies WHERE id = 1")
    row = cursor.fetchone()
    print("ก่อนอัปเดต:", row)

    # 2) ลองอัปเดต label แบบฮาร์ดโค้ด
    cursor.execute("""
        UPDATE cookies
        SET label = %s,
            label_source = %s
        WHERE id = %s
    """, ("TEST_LABEL", "manual_test", 1))
    print("UPDATE rowcount =", cursor.rowcount)

    db.commit()

    # 3) อ่านซ้ำจาก DB เดิม
    cursor.execute("SELECT id, name, label, label_source FROM cookies WHERE id = 1")
    row2 = cursor.fetchone()
    print("หลังอัปเดต:", row2)

    cursor.close()
    db.close()
