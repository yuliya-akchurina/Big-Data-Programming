#!/usr/bin/python3
# Project1 - P1Q4 - reducer.py
from operator import itemgetter
import sys

dict_color_count = {}

for line in sys.stdin:
	line = line.strip()
		   
	color, count = line.split('\t')
	
	if color in dict_color_count.keys():
		count = int(count)
		dict_color_count[color] = int(dict_color_count.get(color, 0)) + count

	else:
		dict_color_count[color] = int(count)

# sort dictionary by value and save to a list             
sorted_tuples = sorted(dict_color_count.items(), key=lambda item: item[1], reverse=True)

# most frequent color 
#print('%s\t%s' % (sorted_tuples[0][0], sorted_tuples[0][1])

#print top 5 most frequent colors 
for i in range(6):
        print('%s\t%s' % (sorted_tuples[i][0], sorted_tuples[i][1]))