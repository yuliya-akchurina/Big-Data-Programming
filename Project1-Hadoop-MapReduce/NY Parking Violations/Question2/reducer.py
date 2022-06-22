#!/usr/bin/python3
# Project1 - P1Q2 - reducer.py

import sys

dict_car_count = {}

for line in sys.stdin:
	line = line.strip()
		   
	type_year, count = line.split('\t')
	
	if type_year in dict_car_count.keys():
            count = int(count)
            dict_car_count[type_year] = int(dict_car_count.get(type_year, 0)) + count

        else:
            dict_car_count[type_year] = int(count)

# sort dictionary by value and save to a list             
sorted_tuples = sorted(dict_car_count.items(), key=lambda item: item[1], reverse=True)

# the most common type and year of the car to get ticket 
print('%s\t%s' % (sorted_tuples[0][0], sorted_tuples[0][1]))
	
# The top 5 most common type and year of the car to get ticket 
for i in range(6):
	print('%s\t%s' % (sorted_tuples[i][0], sorted_tuples[i][1]))