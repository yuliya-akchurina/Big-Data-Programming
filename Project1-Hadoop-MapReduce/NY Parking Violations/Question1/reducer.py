#!/usr/bin/python3
# Project1 - P1Q1 - reducer.py
# 2 columns 'Issue Date' - col 5, 'Violation Time' - col 20

import sys

dict_car_count = {}

for line in sys.stdin:
    line = line.strip()
    date_time, count = line.split('\t')
    
    if date_time in dict_car_count.keys():
        count = int(count)
        dict_car_count[date_time] = int(dict_car_count.get(date_time, 0)) + count

    else:
        dict_car_count[date_time] = int(count)

# sort dictionary by value and save to a list
sorted_tuples = sorted(dict_car_count.items(), key=lambda item: item[1], reverse=True)
# the most frequent date and time combinations
#print('%s\t%s' % (sorted_tuples[0][0], sorted_tuples[0][1]))

# print top 5 most frequent date and time combinations when the tickets are issued 
for i in range(6):
	print('%s\t%s' % (sorted_tuples[i][0], sorted_tuples[i][1]))