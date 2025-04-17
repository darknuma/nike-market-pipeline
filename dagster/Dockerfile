FROM python:3.11-slim

RUN apt-get update

RUN apt-get install -y --no-install-recommends curl

RUN pip install uv 

RUN uv install -r requirements.txt 



