import sqlite3
from flask import Flask, request

app = Flask(__name__)

@app.route('/add_new', methods=['GET', 'POST'])
def insert_new_user():
    con = sqlite3.connect('test.db')
    name = request.args.get('name')
    con.execute('INSERT INTO users (name) VALUES (?)', (name,))
    con.commit()
    con.close()
    return 'New user inserted'

if __name__ == '__main__':
    app.run()