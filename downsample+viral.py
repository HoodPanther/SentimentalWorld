#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

def main():
	import numpy as np
	import os, sched, time, sqlite3, json

	bin_size = 60 * 60 * 2 # 2 hours, in seconds

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
		for i in range(int(round(n/2)),len(x)-n):
			i += n
			a[i] = np.nanmean(x[i-int(round(n/2)):i+int(round(n/2))], axis=0) - np.nanmean(x[i-n:i], axis=0)
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

		min_tweets = 1000 # minimum number of tweets for a valid entry

		for candidate in candidates:

			print 'Downsampling {}'.format(candidate)
			
			conn = sqlite3.connect(downsampled_db)
			c = conn.cursor()
			c.execute('''SELECT * FROM '''+candidate+''' LIMIT 1;''')
			row = c.fetchone()
			conn.close()
			
			if not row:
				# table is empty, start from scratch

				conn = sqlite3.connect(db)
				c = conn.cursor()
				c.execute('''SELECT datetime FROM '''+candidate+''' LIMIT 1;''')
				start_datetime = c.fetchone()[0]
				conn.close()

				step_size = bin_size*100;
				for i in np.arange(start_datetime, time.time(), step_size):

					conn = sqlite3.connect(db)
					c = conn.cursor()
					c.execute('''SELECT * FROM '''+candidate+''' WHERE datetime BETWEEN '''+str(i)+''' AND '''+str(i+step_size)+''';''')
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
						rowtime = row[0]
						if (rowtime - prev_time > bin_size) and (tweet_count >= min_tweets):
							# we've passed bin_size, wrap it up
							if rowtime - prev_time > bin_size*2:
								# more than 2 bin sizes have passed, we're missing data. Add an empty entry.
								conn = sqlite3.connect(downsampled_db)
								c = conn.cursor()
								c.execute('PRAGMA journal_mode=wal')
								c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
										 (rowtime - (rowtime-prev_time)/2, None, tweet_count))
								conn.commit()
								conn.close()
								prev_time = rowtime
								sentiments = []
								tweet_count = 0
							else:
								conn = sqlite3.connect(downsampled_db)
								c = conn.cursor()
								c.execute('PRAGMA journal_mode=wal')
								c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
										 (rowtime - (rowtime-prev_time)/2, np.mean(sentiments), tweet_count))
								conn.commit()
								conn.close()
								prev_time = rowtime
								sentiments = []
								tweet_count = 0

					# finish up last bin (since it won't trigger the above if statement)
					if rowtime - prev_time > bin_size*2 or tweet_count < min_tweets:
						# more than 2 bin sizes have passed, we're missing data. Or we don't have enough tweets. Add an empty entry.
						conn = sqlite3.connect(downsampled_db)
						c = conn.cursor()
						c.execute('PRAGMA journal_mode=wal')
						c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
								 (rowtime - (rowtime-prev_time)/2, None, tweet_count))
						conn.commit()
						conn.close()
						prev_time = time
						sentiments = []
						tweet_count = 0
					else:
						conn = sqlite3.connect(downsampled_db)
						c = conn.cursor()
						c.execute('PRAGMA journal_mode=wal')
						c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
								 (rowtime - (rowtime-prev_time)/2, np.mean(sentiments), tweet_count))
						conn.commit()
						conn.close()
						prev_time = rowtime
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
						rowtime = row[0]
						if rowtime - prev_time > bin_size and tweet_count >= min_tweets:
							# we've passed bin_size, wrap it up
							if rowtime - prev_time > bin_size*2:
								# more than 2 bin sizes have passed, we're missing data. Add an empty entry.
								conn = sqlite3.connect(downsampled_db)
								c = conn.cursor()
								c.execute('PRAGMA journal_mode=wal')
								c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
										 (rowtime - (rowtime-prev_time)/2, None, tweet_count))
								conn.commit()
								conn.close()
							else:
							# elif tweet_count >= min_tweets: # check if we have a reasonable number of tweets to get a mean sentiment
								conn = sqlite3.connect(downsampled_db)
								c = conn.cursor()
								c.execute('PRAGMA journal_mode=wal')
								c.execute('''INSERT INTO '''+candidate+'''(datetime, sentiment, tweet_count) VALUES (?,?,?);''',
										 (rowtime - (rowtime-prev_time)/2, np.mean(sentiments), tweet_count))
								conn.commit()
								conn.close()
							
							prev_time = rowtime
							sentiments = []
							tweet_count = 0

	def get_viral_tweets(since):

		if since:
			print 'Getting viral tweets (since '+str(since)+')...'
		else:
			print 'Getting ALL viral tweets...'

		viral_tweets = {}

		for candidate in candidates:

			print 'Getting viral tweets for {}'.format(candidate)

			# get downsampled data
			print 'Getting downsampled data for {}...'.format(candidate)
			conn = sqlite3.connect(downsampled_db)
			c = conn.cursor()
			c.execute('''SELECT * FROM '''+candidate+''' WHERE datetime > 1459456469;''')
			# 1459456469 is the epoch time after which I started recording tweetIDs
			rows = c.fetchall()
			conn.close()

			rows = np.array(rows).astype(np.float32)

			# calculate derivative of sentiment and tweets per bin
			print 'Calculating derivatives...'
			data_diff = rows.copy()
			data_diff[:,1] = np.abs(diff_smooth(rows[:,1], 2))
			data_diff[:,2] = diff_smooth(rows[:,2], 1)

			# set threshold
			sentiment_diff_threshold = np.nanstd(data_diff[:,1])*3
			tps_diff_threshold = np.nanstd(data_diff[:,2])*2

			print 'Finding peaks...'
			# get peaks above threshold
			if since: # only search for peaks since provided date
				data_selected = data_diff[data_diff[:,0] > since]
			else:
				data_selected = data_diff
			sentiment_diff_peaks = get_peaks(data_selected[1:,[0,1]], sentiment_diff_threshold)
			tps_diff_peaks = get_peaks(data_selected[1:,[0,2]], tps_diff_threshold)

			# find most common tweetID of each peak
			print 'Finding most comment tweetID for each peak...'
			if len(sentiment_diff_peaks) >= 1:
				if len(tps_diff_peaks) >= 1:
					peaks = np.sort(np.vstack((sentiment_diff_peaks, tps_diff_peaks)), axis=0)
				else:
					peaks = sentiment_diff_peaks
			else:
				peaks = tps_diff_peaks
			top_tweets = []
			prev_top_tweet = None
			for p in peaks:
				
				# get most common tweetID during the peak
				conn = sqlite3.connect(db)
				c = conn.cursor()
				c.execute('''
				SELECT tweetid, COUNT(tweetid) FROM '''+candidate+'''
				WHERE datetime BETWEEN '''+str('%f' % p[0])+'''
				AND '''+str('%f' % p[1])+''' GROUP BY tweetid ORDER BY COUNT(tweetid) DESC LIMIT 1;
				''')
				top_tweet = c.fetchone()[0]
				conn.close()

				if top_tweet == None:
					continue

				# check if same tweet as previous peak
				if prev_top_tweet and top_tweet == prev_top_tweet:
					continue
				prev_top_tweet = top_tweet
				top_tweets.append({'datetime': int(np.mean(p)), 'tweetID': top_tweet})
			
			viral_tweets[candidate] = top_tweets

		print 'Saving result to file...'
		if since: # replace any previously found viral tweets during this since with the new ones
			with open('../jeroendelcour.nl/public/2016election/viraltweets.json', 'r') as f:
				fjson = json.load(f)
				for i,c in enumerate(fjson.itervalues()):
					for j,t in enumerate(c):
						if int(t['datetime']) > since:
							c.pop(j)
					for t in viral_tweets[fjson.keys()[i]]:
						c.append(t)
			with open('../jeroendelcour.nl/public/2016election/viraltweets.json', 'w') as f:
				json.dump(fjson, f, indent=4)
		else:
			with open('../jeroendelcour.nl/public/2016election/viraltweets.json', 'w') as f:
				json.dump(viral_tweets, f, indent=4)
		print 'Done with {}'.format(candidate)

	R = bin_size

	s = sched.scheduler(time.time, time.sleep)
	def do_things(sc):

		print 'Downsampling...'
		downsample()
		print 'Done.'
		get_viral_tweets(since=time.time()-60*60*24*7) # past 7 days
		print 'Done.'

		sc.enter(R, 1, do_things, (sc,))

	s.enter(1, 1, do_things, (s,))
	s.run()


if __name__ == '__main__': main()
