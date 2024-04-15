FROM python:3.11.8-slim-bullseye

RUN mkdir -p /usr/src/app/to_process && \
    mkdir -p /usr/src/app/processed

COPY * ./usr/src/app
WORKDIR /usr/src/app

RUN pip install --no-cache-dir -r requirements.txt

ENV ORIGIN=to_process

CMD ["python", "main.py" ]

# PRODUCTION ======================================================================
# docker build -t sms:0.7 . && docker create --name sms-prod -e TEST=FALSE sms:0.7
# TEST ============================================================================
# docker build -t sms:0.7 . && docker create --name sms-prod -e TEST=TRUE sms:0.7
# =================================================================================