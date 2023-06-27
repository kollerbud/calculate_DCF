FROM python:3.9-slim

WORKDIR /src

COPY ./src ./src

RUN python -m pip install -U pip

EXPOSE 8080
