FROM python:3.11.8-slim-bullseye

RUN mkdir -p /usr/src/app/to_process && \
    mkdir -p /usr/src/app/to_process

COPY * ./usr/src/app
WORKDIR /usr/src/app

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --no-cache-dir -r requirements.txt

ENV ORIGIN=to_process

CMD ["python", "main.py" ]

# docker create --name sms-prod -e TEST=FALSE sms:0.6