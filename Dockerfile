FROM python:3.11.9-slim-bullseye

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /usr/src/app/to_process && \
    mkdir -p /usr/src/app/processed

COPY . .

ENV ORIGIN=to_process

CMD ["python3", "main.py" ]

# IMAGE CREATION ========================================
# docker build -t sms:0.8 .
# PRODUCTION CONTAINER ==================================
# docker create --name sms-prod -e TEST=FALSE sms:0.8
# TEST CONTAINER ========================================
# docker create --name sms-test -e TEST=TRUE sms:0.8
# =======================================================