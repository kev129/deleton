from flask import Flask, request

import utils.api_utils as api_utils

app = Flask(__name__)


@app.route("/")
def welcome():
    return "Welcome to the Deloton API"


@app.route("/rides", methods=["GET"])
def get_all_stories():
    return api_utils.ride_json


@app.route("/rides/<id>", methods=["GET"])
def ride_by_id(id):
    list_id = id.split(",")
    return api_utils.get_ride_by_id(list_id)


@app.route("/rides/<id>", methods=["DELETE"])
def delete_ride_by_id(id):
    list_id = id.split(",")
    return api_utils.delete_ride_by_id(list_id)


@app.route("/rider", methods=["GET"])
def get_riders():
    return api_utils.get_riders()


@app.route("/rider/<user_id>", methods=["GET"])
def get_rider_info(user_id):
    list_id = user_id.split(",")
    return api_utils.get_rider_info(list_id)


@app.route("/rider/<user_id>/rides", methods=["GET"])
def get_all_rides_of_user(user_id):
    return api_utils.get_all_rides_of_user(user_id)


@app.route("/daily", methods=["GET"])
def get_rides_for_day():
    date = request.args.get("date")
    return api_utils.get_rides_for_day(date)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
