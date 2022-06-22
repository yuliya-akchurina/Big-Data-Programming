#!/usr/bin/python3
# Project1 - P1Q3 - reducer.py

import sys

dict_location_count = {}

for line in sys.stdin:
    line = line.strip()
    street_location, count = line.split('\t')

    if street_location in dict_location_count.keys():
        count = int(count)
        dict_location_count[street_location] = int(dict_location_count.get(street_location, 0)) + count

    else:
        dict_location_count[street_location] = int(count)

# sort dictionary by value and save to a list
sorted_tuples = sorted(dict_location_count.items(), key=lambda item: item[1], reverse=True)
# most frequent location for receiving tickets 
print('%s\t%s' % (sorted_tuples[0][0], sorted_tuples[0][1]))