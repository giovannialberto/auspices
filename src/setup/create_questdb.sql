CREATE TABLE topic_sentiment (
  timestamp TIMESTAMP,
  stock SYMBOL,
  sentiment DOUBLE,
  tweet VARCHAR
)
TIMESTAMP(timestamp)
PARTITION BY DAY;