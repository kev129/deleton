FROM python:3.10
ENV APP_HOME /app
WORKDIR $APP_HOME

ADD api_requirements.txt ./

RUN pip install -r api_requirements.txt

COPY . .

ENV PYTHONUNBUFFERED True

CMD python3 app.py