FROM python:3.10
ENV APP_HOME /app
WORKDIR $APP_HOME

ADD dashboard_requirements.txt ./
RUN pip3 install -r dashboard_requirements.txt

COPY . .

ENV PYTHONUNBUFFERED True

CMD python app.py