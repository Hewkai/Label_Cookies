เข้า ven10 (cmd)
- venv10\Scripts\activate.bat

ติดตั้ง package 
- pip install requests beautifulsoup4 mysql-connector-python

update db เนื่องจากใช้ db เดียวกับ crawler
- python update_db.py

ทำการ label 
- python label.py

จะมีการเก็บ progress เพื่อกลับมาทำงานต่อในภายหลัง


ตั้งค่า Batch Size (Optional)
ปรับที่บรรทัดล่างสุดของไฟล์:

label_cookies_from_db(cookie_batch_size=100)

100 → default
50 - 100 → ถ้า Cookiepedia ขึ้น rate limit บ่อย


!!!reset_lable.py จะลบ label ในตารางทั้งหมด!!!!
