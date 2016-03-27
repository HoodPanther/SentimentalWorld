def main():
	from twitter_credentials import *
	from nltk.sentiment.vader import SentimentIntensityAnalyzer
	from time import time
	import csv
	import os
	import tweepy

	sid = SentimentIntensityAnalyzer()

	auth = tweepy.OAuthHandler(twitter_credentials['API key'], twitter_credentials['API secret'])
	auth.set_access_token(twitter_credentials['token'], twitter_credentials['token secret'])

	api = tweepy.API(auth)

	#override tweepy.StreamListener to add logic to on_status
	class MyStreamListener(tweepy.StreamListener):
		
		limit = 0
		sanders_counter = 0
		clinton_counter = 0
		trump_counter = 0
		cruz_counter = 0
		unknown_counter = 0
		data_limit = 1e8

		def on_status(self, tweet):
			
			if hasattr(tweet, 'text'):

				if hasattr(tweet, 'retweeted_status'): # twitter sends newlines sometimes to keep the connection alive
					tweet = tweet.retweeted_status
				
				lower = tweet.text.lower() # convert to lower case
				
				if 'sanders' in lower:
					fn = 'data_sanders_'+str(self.sanders_counter).zfill(5)+'.csv'
					file_size = os.path.getsize(fn)
	            	if file_size > self.data_limit: # if file too lage, start a new file
	            		sanders_counter += 1
				elif 'clinton' in lower:
					fn = 'data_clinton_'+str(self.clinton_counter.zfill(5))+'.csv'
					file_size = os.path.getsize(fn)
	            	if file_size > self.data_limit:
	            		clinton_counter += 1
				elif 'trump' in lower:
					fn = 'data_trump_'+str(self.trump_counter).zfill(5)+'.csv'
					file_size = os.path.getsize(fn)
	            	if file_size > self.data_limit:
	            		trump_counter += 1
				elif 'cruz' in lower:
					fn = 'data_cruz_'+str(self.cruz_counter).zfill(5)+'.csv'
					file_size = os.path.getsize(fn)
	            	if file_size > self.data_limit:
	            		cruz_counter += 1
				else:
					fn = 'data_unknown_'+str(self.unknown_counter).zfill(5)+'.csv'
					file_size = os.path.getsize(fn)
	            	if file_size > self.data_limit:
	            		unknown_counter += 1

	            mood = sid.polarity_scores(tweet.text)['compound']

				with open(fn, 'ab') as csvfile:
						spamwriter = csv.writer(csvfile, delimiter=' ',
												quotechar='|', quoting=csv.QUOTE_MINIMAL)
						spamwriter.writerow([time(), mood])
			
		def on_error(self, status_code):
			print status_code
			if status_code == 420:
				#returning False in on_data disconnects the stream
				return False

	myStreamListener = MyStreamListener()
	myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
	myStream.timeout = 60*60 # one hour

	myStream.filter(track=['Sanders, Clinton, Trump, Ted Cruz'], languages=['en'])

if __name__ == ‘__main__’: main()