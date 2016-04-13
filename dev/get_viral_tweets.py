#!/usr/bin/env python
# -*- coding: utf-8 -*-

def main():
	import numpy as np
	import pandas as pd
	import csv, os, StringIO, json, sched, time

	def get_viral_tweets():

		candidates = ['sanders', 'trump', 'clinton', 'cruz']
		viral_tweets = {'sanders': [], 'trump': [], 'clinton': [], 'cruz': []}

		for candidate in candidates:
			print 'Candidate: '+candidate

			print 'Loading data...'
			data_downsampled = np.genfromtxt('data_'+candidate+'_downsampled.csv', delimiter=',')
			data = pd.read_csv('data_'+candidate+'_00000.csv', header=None,
						   names=['date', 'sentiment', 'tweetID'],
						   dtype={'date': np.float64, 'senitiment': np.float64, 'tweetID': str})
			data = data.dropna()
			data_downsampled = np.array([d for d in data_downsampled if d[0] >= data.at[data.index[0], 'date']]) # limit to data since when tweetids are recorded

			def diff_smooth(x, n):
				a = np.zeros_like(x)
				for i in range(len(x)-n):
					i += n
					a[i] = np.nanmean(x[i:i+n], axis=0) - np.nanmean(x[i-n:i], axis=0)
				return a

			print 'Calculating derivatives...'
			data_diff = data_downsampled.copy()
			data_diff[:,1] = diff_smooth(data_downsampled[:,1], 3)
			data_diff[:,2] = diff_smooth(data_downsampled[:,2], 3)

			# peak detection
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

			print 'Detecting peaks...'
			sentiment_diff_threshold = np.std(data_diff[:,1])*2
			tps_diff_threshold = np.std(data_diff[:,2])*2
			sentiment_diff_peaks = get_peaks(data_diff[:,[0,1]], sentiment_diff_threshold)
			tps_diff_peaks = get_peaks(data_diff[:,[0,2]], tps_diff_threshold)

			peaks = np.sort(np.vstack((sentiment_diff_peaks, tps_diff_peaks)), axis=0)

			print 'Getting most common tweetid per peak...'
			top_tweets = []
			prev_top_tweet = None
			for i in range(len(peaks)):
				p = peaks[i]
				# get data points within this peak
				b = data.loc[(data['date'] > p[0]) & (data['date'] < p[1])]
				# get most common tweet ID
				top_tweet = b['tweetID'].value_counts().index
				if len(top_tweet) > 0:
					top_tweet = top_tweet[0]
				else:
					continue
				# check if same tweet as previous peak
				if prev_top_tweet and top_tweet == prev_top_tweet:
					continue
				prev_top_tweet = top_tweet
				top_tweets.append({'datetime': np.mean(p), 'tweetID': top_tweet})

			viral_tweets[candidate] = top_tweets

			print 'Done.'

		print 'Saving viral tweets to file...'
		with open('viraltweets.json', 'w') as f:
			json.dump(viral_tweets, f, indent=4)

	R = 60 * 30 # run every 30 minutes
	s = sched.scheduler(time.time, time.sleep)
	def run(sc):
		get_viral_tweets()
		sc.enter(R, 1, run, (sc,))

	s.enter(1, 1, run, (s,))
	s.run()

if __name__ == '__main__': main()
