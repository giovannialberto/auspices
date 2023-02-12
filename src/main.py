import os
import json
import datetime
from textblob import TextBlob
import tweepy
import requests
from questdb.ingress import Sender, IngressError
from flask import Flask, jsonify
from dotenv import load_dotenv


def calculate_sentiment(tweet):
    """
    Utility function to classify the polarity of a tweet
    using textblob.
    """

    analysis = TextBlob(tweet)
    return analysis.sentiment.polarity


def create_table():
    query = """
    CREATE TABLE IF NOT EXISTS topic_sentiment (
        timestamp TIMESTAMP,
        topic SYMBOL,
        sentiment DOUBLE,
        tweet VARCHAR
    )
    TIMESTAMP(timestamp)
    PARTITION BY DAY;
    """

    response = requests.get(
        'http://database:9000/exec',
    {
        'query': query
    })

    return jsonify(response.json())


class MyStreamListener(tweepy.StreamingClient):
    """
    A custom stream listener class that inherits from tweepy.StreamingClient.
    This class overrides the on_data method to extract and process tweets as they come in.
    It also keeps track of the rules that have been added to the stream.
    
    Attributes:
        rules (list): A list of all the rules currently being applied to the stream.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the stream listener object and retrieves the current list of rules.
        """

        super().__init__(*args, **kwargs)
        self.rules = self.get_rules()

    def on_data(self, data):
        """
        This method is called every time new data is received from the stream.
        It parses the raw data of the tweet, calculates the sentiment of the tweet, 
        and inserts the data into a QuestDB database.
        
        Args:
            data (str): The raw data of the tweet in string format.
        """

        # Parse the raw data of the tweet using the json library
        tweet = json.loads(data)

        # Get the text of the tweet
        tweet_text = tweet['data']['text']

        # Compute the sentiment of the tweet text using TextBlob
        sentiment = calculate_sentiment(tweet_text)

        # infer topic from matching rule id
        matching_rule_id = tweet['matching_rules'][0]['id']
        matching_rule = next(rule for rule in self.rules.data if rule.id == matching_rule_id)
        topic = matching_rule.value

        # Print the username of the user who tweeted it and the sentiment of the tweet
        print(f"======== Tweet ======= \n {tweet_text} \n=======================")
        print(f"-----> Topic: {topic}")
        print(f"-----> Sentiment: {sentiment}")
        print(f"======================")

        # Insert the data into the 'topic_sentiment' table
        try:
            with Sender('database', 9009) as sender:
                sender.row(
                    'topic_sentiment',
                    symbols={
                        'topic': topic
                        },
                    columns={
                        'sentiment': sentiment,
                        'tweet': tweet_text
                        },
                    at=datetime.datetime.utcnow())

                # flush
                sender.flush()
        except IngressError as e:
            # todo: handle error here
            print(f'Got error: {e}\n')

# create the flask app
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')


@app.route('/start')
def start():
    """
    Starts streaming process and returns success message to client.
    """

    # create the topic_sentiment table if it doesn't exist yet
    table_created = create_table()

    if not listener.running:
        listener.filter(threaded=True)
        data = {"message": "Stream started successfully"}
        return json.dumps(data), 200, {'ContentType':'application/json'}
    else:
        data = {"message": "Stream is already connected"}
        return json.dumps(data), 400, {'ContentType':'application/json'}


@app.route('/stop')
def stop():
    """
    Stops the streaming process and returns success message to client.
    """

    if listener.running:
        listener.disconnect()
        data = {"message": "Stream stopped successfully"}
        return json.dumps(data), 200, {'ContentType':'application/json'}
    else:
        data = {"message": "Stream is already stopped"}
        return json.dumps(data), 400, {'ContentType':'application/json'}


@app.route('/status')
def status():
    """
    Returns the streaming process status to the client.
    """

    listener_status = "Active" if listener.running else "Stopped"
    data = {"message": f"The streaming client is {listener_status}"}
    return json.dumps(data), 200, {'ContentType':'application/json'}


@app.route('/track/<topic>')
def track(topic):
    """
    Tracks a given topic by adding a stream rule for it to the stream listener object, updates the internal rules list and returns a success message.
    """

    listener.add_rules(tweepy.StreamRule(topic))
    listener.rules = listener.get_rules()
    data = {"message": f"Tracking topic: {topic}"}
    return json.dumps(data), 200, {'ContentType':'application/json'}


@app.route('/untrack/<topic>')
def untrack(topic):
    """
    Untracks a given topic by removing its stream rule from a stream listener object, updates the internal rules list and returns a success message.
    """

    matching_rule = next(rule for rule in listener.rules.data if rule.value == topic)
    listener.delete_rules(matching_rule.id)
    data = {"message": f"Untracked topic: {topic}"}
    return json.dumps(data), 200, {'ContentType':'application/json'}


@app.route('/rules')
def rules():
    """
    Retrieves the current stream rules from a stream listener object and returns them in a json format.
    """

    rules = listener.get_rules()
    data = {"Active rules": f"{rules.data}"}
    return json.dumps(data), 200, {'ContentType':'application/json'}


@app.route('/query/<topic>')
def query(topic):
    """
    Retrieves data for a specific topic from the database and returns the results in json format.
    """

    response = requests.get(
    'http://database:9000/exec',
    {
        'query': f"SELECT * FROM topic_sentiment WHERE topic = '{topic}'"
    })

    return json.dumps(response.json()), 200, {'ContentType':'application/json'}


if __name__ == '__main__':
    # Get twitter API token from .env file
    load_dotenv()
    bearer_token = os.getenv('BEARER_TOKEN')

    # Create a Twitter stream
    listener = MyStreamListener(bearer_token=bearer_token, daemon=True)

    # run the flask app
    app.run(host='0.0.0.0', port=5000, debug=True)
