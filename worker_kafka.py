import boto3
from natural_reco import Analysis
import json
from multiprocessing import Pool
import pykafka

sns = boto3.resource('sns')
topic = sns.create_topic(Name='notification_tweet')
api_link = "https://gateway.watsonplatform.net/natural-language-understanding/api/v1/analyze"

client = pykafka.KafkaClient(hosts="54.226.229.230:9092")
topic_kafka = client.topics['twitter']
consumer = topic_kafka.get_simple_consumer(queued_max_messages = 10)

if __name__ == '__main__':
	consumer.start()         # Start Kafka, by this command, the system will set up worker pools automatically
	for message in consumer:      #read messages from Kafka
		if message is not None:
			tweet = message.value
			print tweet
			tweet = json.loads(tweet)
			content = tweet['text'].encode("utf-8")
			p = Analysis(content,api_link)         # Process twitter content with Natural Language Understand API
			res = p.request_api()
			decoded = json.loads(res)
			if decoded.has_key('sentiment'):     # In case of twitter doesn't consist of keywords
				attitude = decoded['sentiment']['document']['label']
				tweet['sentiment'] = attitude
				encoded = json.dumps(tweet, ensure_ascii=False)
				topic.publish(Message=encoded)       # Send notification to SNS

				print encoded
