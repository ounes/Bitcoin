FROM python:3-alpine

COPY ./requirements.txt /tmp/requirements.txt
RUN pip install --no-cache -r /tmp/requirements.txt
RUN rm /tmp/requirements.txt

RUN mkdir -p /app
COPY . /app
WORKDIR /app

CMD python dashboard.py
