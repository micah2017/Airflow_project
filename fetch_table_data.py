from flask import Flask, request, jsonify
from flask_httpauth import HTTPTokenAuth
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2  # For PostgreSQL interaction
import jwt
import datetime
from functools import wraps
import os  # For environment variables
from jproperties import Properties

configs = Properties()

with open('../configurations/config.properties', 'rb') as config_file:
    configs.load(config_file)

app = Flask(__name__)
auth = HTTPTokenAuth(scheme='Bearer')

# --- Configuration ---
DATABASE_URL = str(configs.get("DW_API_URL").data)
SECRET_KEY = str(configs.get("DW_API_SECRET_KEY").data) 
TOKEN_EXPIRATION_MINUTES = str(configs.get("TOKEN_EXPIRATION_MINUTES").data).strip()

# --- User Authentication ---
users = {}  # Will be loaded from the database

def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
    return conn

def fetch_users():
    global users
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT username, password_hash FROM users")
            rows = cur.fetchall()
            users.clear()
            for row in rows:
                users[row[0]] = row[1]
            cur.close()
        except psycopg2.Error as e:
            print(f"Error fetching users from database: {e}")
        finally:
            conn.close()
    else:
        print("Failed to connect to the database to fetch users.")

# Fetch users on application startup
with app.app_context():
    fetch_users()

def create_token(username):
    payload = {
        'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=TOKEN_EXPIRATION_MINUTES),
        'iat': datetime.datetime.utcnow(),
        'sub': username
    }
    return jwt.encode(payload, SECRET_KEY, algorithm='HS256')

def verify_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        return payload['sub']
    except jwt.ExpiredSignatureError:
        return None  # Token has expired
    except jwt.InvalidTokenError:
        return None  # Invalid token

@auth.verify_token
def verify_auth_token(token):
    username = verify_token(token)
    if username:
        return username
    return None

@app.route('/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if username in users and check_password_hash(users.get(username), password):
        token = create_token(username)
        return jsonify({'token': token})
    return jsonify({'message': 'Invalid credentials'}), 401

# --- Database Query Endpoint ---
def get_db():
    conn = get_db_connection()
    if conn:
        return conn.cursor()
    return None

def close_db(cur):
    if cur:
        cur.connection.close()

@app.route('/api/data/<string:table_name>', methods=['GET'])
@auth.login_required
def query_table(table_name):
    cur = get_db()
    if not cur:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    page = request.args.get('page', default=1, type=int)
    per_page = request.args.get('per_page', default=10, type=int)

    if page < 1 or per_page < 1:
        close_db(cur)
        return jsonify({'error': 'Invalid page or per_page value'}), 400

    offset = (page - 1) * per_page

    try:
        # Sanitize table name to prevent SQL injection (basic example, more robust needed)
        if not table_name.isalnum():
            close_db(cur)
            return jsonify({'error': 'Invalid table name'}), 400

        count_query = f"SELECT COUNT(*) FROM {table_name}"
        cur.execute(count_query)
        total_items = cur.fetchone()[0]

        query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
        cur.execute(query, (per_page, offset))
        columns = [desc[0] for desc in cur.description]
        results = [dict(zip(columns, row)) for row in cur.fetchall()]

        total_pages = (total_items + per_page - 1) // per_page

        close_db(cur)
        return jsonify({
            'items': results,
            'page': page,
            'per_page': per_page,
            'total_items': total_items,
            'total_pages': total_pages
        })
    except psycopg2.Error as e:
        close_db(cur)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)