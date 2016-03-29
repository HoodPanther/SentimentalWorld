#!/usr/bin/env python
# -*- coding: utf-8 -*-

def main():
	import numpy as np
	import sched, time, csv, os, StringIO
	
	def downsample(files, outfile, R):
		with open(outfile, 'wb') as f:
			spamwriter = csv.writer(f, delimiter=',',
									quotechar='|', quoting=csv.QUOTE_MINIMAL)
			spamwriter.writerow(['date','mood','tweets'])
		for fn in files:
			prev_time = 0
			mood = []
			counter = 0
			# with open(fn, 'rb') as f:
			# 	spamreader = csv.reader(f, delimiter=',', quotechar='|')
			# 	for row in spamreader:

			f = file(fn)
			f.seek(0,2)
			lastline = f.tell()
			f.seek(0)
			tweets = 0
			min_tweets = 100
			while f.tell() < lastline:
				where = f.tell()
				line = f.readline()
				if not line:
					time.sleep(0.1)
					f.seek(where)
				else:
					tweets += 1
					a = StringIO.StringIO(line)
					reader = csv.reader(a, delimiter=',')
					row = list(reader)[0]
					mood.append(float(row[1]))
					if prev_time == 0:
						prev_time = float(row[0])
						continue
					time_diff = float(row[0]) - prev_time
					if time_diff > R and tweets > min_tweets:
						if int(time_diff / R) > 1:
							with open(outfile, 'ab') as outf:
								spamwriter = csv.writer(outf, delimiter=',',
														quotechar='|', quoting=csv.QUOTE_MINIMAL)
								spamwriter.writerow([float(row[0])-(time_diff*0.5), None, 0])
						with open(outfile, 'ab') as outf:
							spamwriter = csv.writer(outf, delimiter=',',
													quotechar='|', quoting=csv.QUOTE_MINIMAL)
							spamwriter.writerow([float(row[0])-60, np.mean(mood), tweets])
						mood = []
						prev_time = float(row[0])
			f.close()
	R = 10 * 60 # run every 10 minutes

	s = sched.scheduler(time.time, time.sleep)
	def do_something(sc):
		files = os.listdir('.')
		files = [fn for fn in files if fn.endswith('.csv')]

		files_sanders = sorted([fn for fn in files if 'data_sanders' in fn])
		files_trump = sorted([fn for fn in files if 'data_trump' in fn])
		files_clinton = sorted([fn for fn in files if 'data_clinton' in fn])
		files_cruz = sorted([fn for fn in files if 'data_cruz' in fn])

		downsample(files_sanders, '../jeroendelcour.nl/public/2016election/data_sanders_downsampled.csv', R)
		downsample(files_trump, '../jeroendelcour.nl/public/2016election/data_trump_downsampled.csv', R)
		downsample(files_clinton, '../jeroendelcour.nl/public/2016election/data_clinton_downsampled.csv', R)
		downsample(files_cruz, '../jeroendelcour.nl/public/2016election/data_cruz_downsampled.csv', R)
		sc.enter(R, 1, do_something, (sc,))

	s.enter(1, 1, do_something, (s,))
	s.run()


if __name__ == '__main__': main()
