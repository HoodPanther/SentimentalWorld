#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

def main():

	import sqlite3, sqlitebck
	conn1 = sqlite3.connect('data.sqlite')
	conn2 = sqlite3.connect('data backup/data_backup.sqlite')
	sqlitebck.copy(conn1, conn2)
	conn1.close()
	conn2.close()


if __name__ == '__main__': main()