import os
from datetime import date
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, Response
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
auth = HTTPBasicAuth()

users = {"citibike": generate_password_hash("cycling")}

@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username

@app.route("/")
@auth.login_required
def index():
    return "Welcome to the UFO sightings API!"

# @app.route("/recent/<period>", defaults={"amount": 1}, methods=["GET"])
# @app.route("/recent/<period>/<amount>", methods=["GET"])
# @auth.login_required
# def get_recent_sightings(period: str, amount: int):
#     """
#     Return recent UFO sightings from the past <amount> <period>s.
#     :param period: either "minute", "hour", or "day"
#     :param amount: the number of periods
#     :return:
#     """
#     if period not in ("minute", "hour", "day"):
#         return Response("Period can only be 'minute', 'hour', or 'day'!", status=422)

#     conn = psycopg2.connect(
#         database=os.environ["POSTGRES_DATABASE"],
#         user=os.environ["POSTGRES_USERNAME"],
#         host=os.environ["POSTGRES_HOST"],
#         password=os.environ["POSTGRES_PASSWORD"],
#     )
#     cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
#     year_offset = date.today().year - int(os.environ["DATA_YEAR"])
#     # Construct the query
#     q = f"""SELECT shape, duration, date_time + INTERVAL '{year_offset} YEARS' AS date_time, city, state, country, city_latitude, city_longitude FROM ufosightings WHERE date_time + INTERVAL '{year_offset} YEARS' <= NOW() AND date_time + INTERVAL '{year_offset} YEARS' >= NOW() - INTERVAL '{amount} {period}s';
# """

#     cursor.execute(q)
#     data = cursor.fetchall()

#     return jsonify(data)


@app.route("/sightings/<date>", methods=["GET"])
@auth.login_required
def get_sightings_by_date(date: str):
    """
    Return UFO sightings for a specific date.
    :param date: the date in yyyy-mm-dd format
    :return:
    """
    try:
        date_obj = datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return Response("Date must be in yyyy-mm-dd format!", status=422)

    conn = psycopg2.connect(
        database=os.environ["POSTGRES_DATABASE"],
        user=os.environ["POSTGRES_USERNAME"],
        host=os.environ["POSTGRES_HOST"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    year_offset = date_obj.today().year - int(os.environ["DATA_YEAR"])

    # Construct the query
    # q = f"""SELECT shape, duration, date_time + INTERVAL '{year_offset} YEARS' AS date_time, city, state, country, city_latitude, city_longitude FROM ufosightings WHERE date_trunc('day', date_time + INTERVAL '{year_offset} YEARS') = TIMESTAMP '{date_obj}'::TIMESTAMP;"""

    q = f"""SELECT 
                summary,
                country,
                city,
                state,
                date_time + INTERVAL '{year_offset} YEARS' AS date_time,
                shape,
                duration,
                stats,
                report_link,
                text,
                posted,
                city_latitude,
                city_longitude
            FROM ufosightings WHERE date_trunc('day', date_time + INTERVAL '{year_offset} YEARS') = TIMESTAMP '{date_obj}'::TIMESTAMP;"""
            
    cursor.execute(q)
    data = cursor.fetchall()

    return jsonify(data)






if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=5000)
