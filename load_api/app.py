from flask import Flask, request

app = Flask(__name__)


@app.route("/")
def welcome():
    return "Welcome to the Deloton API"


@app.route("/rides", methods=["GET"])


@app.route("/rides/<id>", methods=["GET"])


@app.route("/rides/<id>", methods=["DELETE"])


@app.route("/rider", methods=["GET"])


@app.route("/rider/<user_id>", methods=["GET"])


@app.route("/rider/<user_id>/rides", methods=["GET"])


@app.route("/daily", methods=["GET"])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)