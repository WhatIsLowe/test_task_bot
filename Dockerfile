FROM python:3.11.6-alpine

WORKDIR /bot

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENV BOT_TOKEN=<bot token>

ENV MONGO_AUTH=<mongo db connection string>

ENV MONGO_DB=<mongodb database name>

ENV MONGO_COLLECTION=<mongodb collection name>

CMD [ "python", "bot.py" ]