#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

def main():
	import numpy as np
	import os, sched, time, sqlite3, json

	bin_size = 30 * 60 # 30 minutes, in seconds

	db = 'data.sqlite'
	downsampled_db = '../jeroendelcour.nl/2016election/data_downsampled.sqlite'

	if not os.path.isfile(db):
		print 'Database not found: '+db

	if not os.path.isfile(downsampled_db):
		print 'Downsampled database file not found, creating new database.'
		conn = sqlite3.connect(downsampled_db)
		c = conn.cursor()
		c.execute('''CREATE TABLE sanders
			   (datetime      REAL,
			   sentiment      REAL,
			   tweet_count    INTEGER);''')
		c.execute('''CREATE TABLE trump
			   (datetime      REAL,
			   sentiment      REAL,
			   tweet_count    INTEGER);''')
		c.execute('''CREATE TABLE clinton
			   (datetime      REAL,
			   sentiment      REAL,
			   tweet_count    INTEGER);''')
		c.execute('''CREATE TABLE cruz
			   (datetime      REAL,
			   sentiment      REAL,
			   tweet_count    INTEGER);''')
		conn.commit()
		conn.close()

	candidates = ['sanders', 'trump', 'clinton', 'cruz']

	def diff_smooth(x, n):
		a = np.zeros_like(x)
		for i in range(len(x)-n):
			i += n
			a[i] = np.nanmean(x[i:i+n], axis=0) - np.nanmean(x[i-n:i], axis=0)
		return a

	def get_peaks(x, threshold):
		peaks = []
		peaking = False
		start = None
		for a in x:
			if not peaking:
				if a[1] > threshold:
					start = a[0]
					peaking = True
			elif peaking:
				if a[1] < threshold:
					end = a[0]
					peaks.append([start, end])
					start = None
					peaking = False
		return np.array(peaks)
	
	def downsample():

		min_tweets = 100 # minimum number of tweets for a valid entry

		for candidate in candidates:
			
			conn = sqlite3.connect(downsampled_db)
			c = conn.cursor()
			c.execute('''SELECT * FROM '''+candidate+''' LIMIT 1;''')
			row = c.fetchone()
			conn.close()
			
			if not row:
				# table is empty, start from scratch

				conn = sqlite3.connect(db)
				c = conn.cursor()
				c.execute('''SELECT * FROM '''+candidate+''';''')
				all_rows = c.fetchall()
				conn.close()
				
				prev_time = None
				sentiments = []
				tweet_count = 0
				for row in all_rows:
					sentiments.append(row[1])
					tweet_count += 1
					if not prev_time:
						prev_time = row[0]
						continue
					time = row[0]
					if time - prev_time > bin_size:
						# we've passed bin_size, wrap it up
						if time - prev_time > bin_size*2:
							# more than 2 bin_sizes have passed, we're missing data. Add an empty entry.
							conn = sqlite3.connect(downsampled_db)
							c = conn.cursor()
							c.execute('PRAGMA journal_mode=wal')
							c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
									 (time - (time-prev_time)/2, None, tweet_count))
							conn.commit()
							conn.close()
							prev_time = time
							sentiments = []
							tweet_count = 0
						elif tweet_count >= min_tweets: # check if we have a reasonable number of tweets to get a mean sentiment from
							conn = sqlite3.connect(downsampled_db)
							c = conn.cursor()
							c.execute('PRAGMA journal_mode=wal')
							c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
									 (time - (time-prev_time)/2, np.mean(sentiments), tweet_count))
							conn.commit()
							conn.close()
							prev_time = time
							sentiments = []
							tweet_count = 0
				
			else: # table is not empty
				
				conn = sqlite3.connect(downsampled_db)
				c = conn.cursor()
				c.execute('''SELECT datetime FROM '''+candidate+''' ORDER BY rowid DESC LIMIT 1;''')
				row = c.fetchone()
				downsampled_lasttime = row[0]
				conn.close()
				
				conn = sqlite3.connect(db)
				c = conn.cursor()
				c.execute('''SELECT datetime FROM '''+candidate+''' ORDER BY rowid DESC LIMIT 1;''')
				row = c.fetchone()
				data_lasttime = row[0]
				conn.close()
				
				if data_lasttime - downsampled_lasttime > bin_size:
					
					# time to add another datapoint

					conn = sqlite3.connect(db)
					c = conn.cursor()
					c.execute('''SELECT * FROM '''+candidate+''' WHERE datetime > '''+str(downsampled_lasttime)+''';''')
					rows = c.fetchall()
					conn.close()

					prev_time = downsampled_lasttime
					sentiments = []
					tweet_count = 0
					for row in rows:
						sentiments.append(row[1])
						tweet_count += 1
						if not prev_time:
							prev_time = row[0]
							continue
						time = row[0]
						if time - prev_time > bin_size:
							# we've passed bin_size, wrap it up
							if time - prev_time > bin_size*2:
								# more than 2 bin_sizes have passed, we're missing data. Add an empty entry.
								conn = sqlite3.connect(downsampled_db)
								c = conn.cursor()
								c.execute('PRAGMA journal_mode=wal')
								c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
										 (time - (time-prev_time)/2, None, tweet_count))
								conn.commit()
								conn.close()
							elif tweet_count >= min_tweets: # check if we have a reasonable number of tweets to get a mean sentiment from
								conn = sqlite3.connect(downsampled_db)
								c = conn.cursor()
								c.execute('PRAGMA journal_mode=wal')
								c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
										 (time - (time-prev_time)/2, np.mean(sentiments), tweet_count))
								conn.commit()
								conn.close()
							
							prev_time = time
							sentiments = []
							tweet_count = 0

	def get_viral_tweets():

		viral_tweets = {}

		for candidate in candidates:

			# get downsampled data
			conn = sqlite3.connect(downsampled_db)
			c = conn.cursor()
			c.execute('''SELECT * FROM '''+candidate+''' WHERE datetime > 1459456469;''')
    			# 1459456469 is the epoch time after which I started recording tweetIDs
			rows = c.fetchall()
			conn.close()

			rows = np.array(rows).astype(np.float32)

			# calculate derivative of sentiment and tweets per bin
			data_diff = rows.copy()
			data_diff[:,1] = np.abs(diff_smooth(rows[:,1], 10))
			data_diff[:,2] = diff_smooth(rows[:,2], 2)

			# get peaks above a threshold
			sentiment_diff_threshold = np.std(data_diff[:,1])*3
			tps_diff_threshold = np.std(data_diff[:,2])*3
			sentiment_diff_peaks = get_peaks(data_diff[:,[0,1]], sentiment_diff_threshold)
			tps_diff_peaks = get_peaks(data_diff[:,[0,2]], tps_diff_threshold)

			# find most common tweetID of each peak
			peaks = np.sort(np.vstack((sentiment_diff_peaks, tps_diff_peaks)), axis=0)
			top_tweets = []
			prev_top_tweet = None
			for p in peaks:
				
				# get most common tweetID during the peak
				conn = sqlite3.connect(db)
				c = conn.cursor()
				c.execute('''
				SELECT datetime, tweetID FROM '''+candidate+'''
				WHERE datetime > '''+str('%f' % p[0])+'''
				AND datetime < '''+str('%f' % p[1])+''';
				''')
				rows = c.fetchall()
				conn.close()

				rows = np.array(rows)
				
				if len(rows) < 1:
					continue

				(values,counts) = np.unique(rows[:,1],return_counts=True)
				ind=np.argmax(counts)
				top_tweet = values[ind]

				if top_tweet == None:
					continue

				# check if same tweet as previous peak
				if prev_top_tweet and top_tweet == prev_top_tweet:
					continue
				prev_top_tweet = top_tweet
				top_tweets.append({'datetime': int(np.mean(p)), 'tweetID': top_tweet})
			
			viral_tweets[candidate] = top_tweets

		with open('../jeroendelcour.nl/public/2016election/viraltweets.json', 'w') as f:
			json.dump(viral_tweets, f, indent=4)

	R = bin_size

	s = sched.scheduler(time.time, time.sleep)
	def do_things(sc):

		print 'Downsampling...'
		downsample()
		print 'Done.'
		print 'Getting viral tweets...'
		get_viral_tweets()
		print 'Done.'

		sc.enter(R, 1, do_things, (sc,))

	s.enter(1, 1, do_things, (s,))
	s.run()


if __name__ == '__main__': main()
