FROM python:3.10-slim

WORKDIR /src/finance_section


ENV PYTHONPATH "${PYTHONPATH}:."

COPY . .

RUN python -m pip install -U pip

COPY requirements.txt requirements.txt

RUN pip install -U --no-cache-dir -r requirements.txt

EXPOSE 8080

CMD exec functions_framework --target=main