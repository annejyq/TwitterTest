import json
import time
from codecs import open
from dateutil import parser
import boto3
import tweepy
import certifi
import pykafka

consumer_key = 'ylY13IOjpXr7SOfjHKwsi0B5q'
consumer_secret = '5nFY7ulbZHKRzWbKxmqnyI0nGevgxrHuIvLDm16Nh6YhhErdQd'
access_token = '765007753467363328-h66wqYE0pq4iL5fbDpLhtkKOiMJSA89'
access_token_secret = 'bM3krkwgOK2pgnZV3iKjmiPnoorYMm2EYfV45JrsecVIA'

client = pykafka.KafkaClient(hosts="54.226.229.230:9092")
topic = client.topics['twitter']
producer = topic.get_sync_producer()

def appendlog(f, s):
    f.write(u'[{0}] {1}\n'.format(time.strftime('%Y-%m-%dT%H:%M:%SZ'), s))
    f.flush()


class TwittMapListener(tweepy.StreamListener):
    def __init__(self, f):
        super(TwittMapListener, self).__init__()
        self.f = f

    def on_data(self, data):
        try:
            decoded = json.loads(data)
            geo = decoded.get('place')

            if geo and decoded.get('lang') == 'en':
                cord = geo['bounding_box']['coordinates'][0][0]
                timestamp = parser.parse(decoded['created_at'])
                timestamp = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
                tweet = {
                    'user': decoded['user']['screen_name'],
                    'text': decoded['text'],
                    'geo': cord,
                    'time': timestamp
                }
                encoded = json.dumps(tweet,ensure_ascii=False)
                tweet_id = decoded['id_str']
                producer.produce(encoded.encode('utf-8'))
                appendlog(self.f, u'{0}: {1}'.format(tweet_id, encoded))
        except Exception as e:
            appendlog(self.f, '{0}: {1}'.format(type(e), str(e)))

    def on_error(self, status):
        if status == 420:  # Rate limited
            appendlog(self.f, 'Error 420')
            return False



if __name__ == '__main__':
    with open('streaming.log', 'a', encoding='utf8') as f:
        appendlog(f, 'Program starts')
        ls = TwittMapListener(f)

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        stream = tweepy.Stream(auth, ls)
        stream.filter(track=["Trump", "Hillary", "Sanders", "Facebook", "LinkedIn", "Amazon", "Google", "Uber", "Columbia", "New York"])

