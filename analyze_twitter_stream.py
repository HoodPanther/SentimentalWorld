#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twitter_credentials import *

def main():
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
		counter = {
			'sanders': 0,
			'clinton': 0,
			'trump': 0,
			'cruz': 0,
			'unknown': 0
		}
		data_limit = 1e8

		def on_status(self, tweet):

			try:
				
				if hasattr(tweet, 'text'):

					if hasattr(tweet, 'retweeted_status'): # twitter sends newlines sometimes to keep the connection alive
						tweet = tweet.retweeted_status
					
					lower = tweet.text.lower() # convert to lower case
					
					if 'sanders' in lower:
						candidate = 'sanders'
					elif 'clinton' in lower:
						candidate = 'clinton'
					elif 'trump' in lower:
						candidate = 'trump'
					elif 'cruz' in lower:
						candidate = 'cruz'
					else:
						candidate = 'unknown'

					fn = 'data_'+candidate+'_'+str(self.counter[candidate]).zfill(5)+'.csv'

					mood = sid.polarity_scores(tweet.text)['compound']

					with open(fn, 'ab') as csvfile:
							spamwriter = csv.writer(csvfile, delimiter=' ',
													quotechar='|', quoting=csv.QUOTE_MINIMAL)
							spamwriter.writerow([time(), mood])

					file_size = os.path.getsize(fn)
					if file_size > self.data_limit:
						self.counter[candidate] += 1
			except Exception:
				pass
			
		def on_error(self, status_code):
			print status_code
			if status_code == 420:
				#returning False in on_data disconnects the stream
				return False

	myStreamListener = MyStreamListener()
	myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
	myStream.timeout = 60*60 # one hour

	myStream.filter(track=['Sanders, Clinton, Trump, Ted Cruz'], languages=['en'])

if __name__ == '__main__': main()