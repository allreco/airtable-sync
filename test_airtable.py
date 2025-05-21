from pyairtable import Table

AIRTABLE_ACCESS_TOKEN = "patJdAgUGOsalQ0iZ.ccec57f06bea8b5885a668c1534f8ac245c0c2c83664e94c936692a4926785d1"
BASE_ID = "appT3Hn3kruQ0ZxHA"
TABLE_NAME = "231ALL Table"

table = Table(AIRTABLE_ACCESS_TOKEN, BASE_ID, TABLE_NAME)
records = table.all()
print(records)
