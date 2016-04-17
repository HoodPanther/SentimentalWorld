#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from twitter_credentials import *

def main():
	from nltk.sentiment.vader import SentimentIntensityAnalyzer
	import time,os,tweepy,sys,traceback
	# import csv
	import sqlite3
	from datetime import datetime

	def get_trace():
		return ''.join(traceback.format_exception(*sys.exc_info()))

	# def save_tweet_csv(fn, datetime, sentiment, tweetID):
	# 	with open(fn, 'ab') as csvfile:
	# 		spamwriter = csv.writer(csvfile, delimiter=',',
	# 								quotechar='|', quoting=csv.QUOTE_MINIMAL)
	# 		spamwriter.writerow([datetime, sentiment, tweetID])

	def save_tweet_db(candidate, datetime, sentiment, tweetID):
		try:
			conn = sqlite3.connect('data.sqlite')
			c = conn.cursor()
			c.execute('PRAGMA journal_mode=wal')
			c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweetID) VALUES (?, ?, ?)''', (datetime, sentiment, tweetID))
			conn.commit()
			conn.close()
		except Exception, ex:
			err =  "'%s' Error '%s' '%s'"%(str(datetime.now()), str(ex), get_trace())
			print err
			file('errors.log','a').write(err+'\n')

	sid = SentimentIntensityAnalyzer()

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
		data_limit = 1e9

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

					sentiment = sid.polarity_scores(tweet.text)['compound']

					save_tweet_db(candidate, time.time(), sentiment, tweet.id_str)

					# fn = 'data_'+candidate+'_'+str(self.counter[candidate]).zfill(5)+'.csv'

					# with open(fn, 'ab') as csvfile:
					# 		spamwriter = csv.writer(csvfile, delimiter=',',
					# 								quotechar='|', quoting=csv.QUOTE_MINIMAL)
					# 		spamwriter.writerow([time.time(), mood, tweet.id_str])

					# save_tweet_csv(fn, time.time(), sentiment, tweet.id_str)

					# file_size = os.path.getsize(fn)
					# if file_size > self.data_limit:
					# 	self.counter[candidate] += 1
			except Exception:
				pass
			
		def on_error(self, status_code):
			print status_code
			if status_code == 420:
				#returning False in on_data disconnects the stream
				return False

	while 1:
		try:

			auth = tweepy.OAuthHandler(twitter_credentials['API key'], twitter_credentials['API secret'])
			auth.set_access_token(twitter_credentials['token'], twitter_credentials['token secret'])

			api = tweepy.API(auth)
			myStreamListener = MyStreamListener()
			myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
			myStream.timeout = 60*2 # two minutes
			myStream.filter(track=['Sanders, Clinton, Trump, Ted Cruz'], languages=['en'], stall_warnings=True)
		except KeyboardInterrupt:
			print myStream.running
			break
		except Exception, ex:
			err =  "'%s' Error '%s' '%s'"%(str(datetime.now()), str(ex), get_trace())
			print err
			file('errors.log','a').write(err+'\n')
			print 'Connected: '+str(myStream.running)
			continue

if __name__ == '__main__': main()
