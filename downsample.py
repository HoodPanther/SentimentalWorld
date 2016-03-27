#!/usr/bin/env python
# -*- coding: utf-8 -*-

def main():
	import numpy as np
	import sched, time, csv, os
	
	def downsample(files, outfile, R):
	with open(outfile, 'wb') as f:
		spamwriter = csv.writer(f, delimiter=',',
								quotechar='|', quoting=csv.QUOTE_MINIMAL)
		spamwriter.writerow(['date','mood'])
	for fn in files:
		prev_time = 0
		mood = []
		counter = 0
		with open(fn, 'rb') as f:
			spamreader = csv.reader(f, delimiter=' ', quotechar='|')
			for row in spamreader:
				mood.append(float(row[1]))
				if prev_time == 0:
					prev_time = float(row[0])
					continue
				if float(row[0]) - prev_time > R:
					with open(outfile, 'ab') as f:
						spamwriter = csv.writer(f, delimiter=',',
												quotechar='|', quoting=csv.QUOTE_MINIMAL)
						spamwriter.writerow([float(row[0])-60, np.mean(mood)])
					mood = []
					prev_time = float(row[0])

	R = 10 * 60 # 10 minute bins

	s = sched.scheduler(time.time, time.sleep)
	def do_something(sc):

		print 'Downsampling...'
		
		files = os.listdir('.')
		files = [fn for fn in files if fn.endswith('.csv')]

		files_sanders = sorted([fn for fn in files if 'data_sanders' in fn])
		print files_sanders
		files_trump = sorted([fn for fn in files if 'data_trump' in fn])
		print files_trump
		files_clinton = sorted([fn for fn in files if 'data_clinton' in fn])
		print files_clinton
		files_cruz = sorted([fn for fn in files if 'data_cruz' in fn])
		print files_cruz

		downsample(files_sanders, '../jeroendelcour.nl/public/2016election/data_sanders_downsampled.csv', R)
		downsample(files_trump, '../jeroendelcour.nl/public/2016election/data_trump_downsampled.csv', R)
		downsample(files_clinton, '../jeroendelcour.nl/public/2016election/data_clinton_downsampled.csv', R)
		downsample(files_cruz, '../jeroendelcour.nl/public/2016election/data_cruz_downsampled.csv', R)
		sc.enter(R, 1, do_something, (sc,))

	s.enter(R, 1, do_something, (s,))
	s.run()


if __name__ == '__main__': main()