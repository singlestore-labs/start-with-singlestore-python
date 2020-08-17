import sys
import traceback
import mariadb

# main is called at the bottom
def main():

  # TODO: pull from config
  host = 'localhost'
  port = 3306
  user = 'root'
  password = ''
  database = 'acme'

  connection = mariadb.connect(user=user, password=password, database=database, host=host, port=port)
  cursor = connection.cursor()

  try:
    
    id = create(cursor, "Inserted row")
    print("Inserted row {0}".format(id))

    row = read_one(cursor, id)
    print(row, sep =',')

    update(cursor, id, "Updated row")
    print("Updated row {0}".format(id))

    rows = read_all(cursor)
    print("All rows:")
    for row in rows:
      print(row, sep ='\t')

    delete(cursor, id)
    print("Deleted row {0}".format(id))

  except Exception as e:
    print("Error")
    print(e)
    traceback.print_exc(file =sys.stdout)

  cursor.close()
  connection.close()


def create(cursor, content):
  sql = "INSERT INTO messages (content) VALUES (?)"
  data = (content,)
  cursor.execute(sql, data)
  id = cursor.lastrowid
  return id

def read_one(cursor, id):
  sql = "SELECT id, content, createdate FROM messages WHERE id = ?"
  data = (id,)
  cursor.execute(sql, data)
  row = cursor.fetchone()
  return row

def read_all(cursor):
  sql = "SELECT * FROM messages ORDER BY id"
  cursor.execute(sql)
  rows = cursor.fetchall()
  return rows

def update(cursor, id, content):
  sql = "UPDATE messages SET content =? WHERE id =?"
  data = (content, id)
  cursor.execute(sql, data)

def delete(cursor, id):
  sql = "DELETE FROM messages WHERE id =?"
  data = (id, )
  cursor.execute(sql, data)

if __name__ == '__main__':
  main()
