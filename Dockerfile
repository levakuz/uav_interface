FROM python:3.9-alpine3.13
RUN apk add git
RUN apk add gcc
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev
RUN pip3 install flask
RUN pip3 install psycopg2
RUN pip3 install waitress
RUN git clone https://github.com/levakuz/uav_interface.git
WORKDIR uav_interface