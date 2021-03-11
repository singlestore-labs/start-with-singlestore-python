import sys
import traceback
from memsql.common import database

# main is called at the bottom
def main():

  # TODO: pull from config
  HOST = 'localhost'
  PORT = 3306
  USER = 'root'
  PASSWORD = 'password_here'
  DATABASE = 'acme'

  conn = database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE)

  try:

    conn.ping()

    id = create(conn, "Inserted row")
    print("Inserted row {0}".format(id))

    row = read_one(conn, id)
    print(row, sep =',')

    update(conn, id, "Updated row")
    print("Updated row {0}".format(id))

    rows = read_all(conn)
    print("All rows:")
    for row in rows:
      print(row, sep ='\t')

    delete(conn, id)
    print("Deleted row {0}".format(id))

  except Exception as e:
    print("Error")
    print(e)
    traceback.print_exc(file =sys.stdout)

  finally:
    conn.close()


def create(conn, content):
  sql = "INSERT INTO messages (content) VALUES (%s)"
  id = conn.execute_lastrowid(sql, content)
  return id

def read_one(conn, id):
  sql = "SELECT id, content, createdate FROM messages WHERE id = %s"
  row = conn.get(sql, id)
  return row

def read_all(conn):
  sql = "SELECT * FROM messages ORDER BY id"
  rows = conn.query(sql)
  return rows

def update(conn, id, content):
  sql = "UPDATE messages SET content = %s WHERE id = %s"
  conn.query(sql, content, id)

def delete(conn, id):
  sql = "DELETE FROM messages WHERE id = %s"
  conn.query(sql, id)

if __name__ == '__main__':
  main()
