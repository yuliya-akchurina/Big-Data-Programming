#!/usr/bin/python3
# Project1 - P2Q2 - reducer.py

import sys
import re

def get_new_centroids():
    current_centroid = None
    sum_x = 0
    sum_y = 0
    sum_z = 0
    count = 0
    
    # input comes from STDIN
    for line in sys.stdin:
        line = line.strip()
        centroid_index, xyz = line.split('\t')
        x,y,z = xyz.split(',')
        
        # convert x,y,z from string to float
        try:
            x = float(x)
            y = float(y)
            z = float(z)
        except ValueError:
            continue
        
        # this IF-switch works because Hadoop sorts map output by key (here: key is cluster index) before passing it to reducer
        if current_centroid == centroid_index:
            count += 1
            sum_x += x
            sum_y += y
            sum_z += z
        else:
            if count != 0:
                # print the average of every cluster to get new centroids, rounded to 2 decimals
                print(str(round((sum_x / count), 2)) + ", " + str(round((sum_y / count), 2)) + ", " + str(round((sum_z / count), 2)))
            
            current_centroid = centroid_index
            sum_x = x
            sum_y = y
            sum_z = z
            count = 1
    
    # print centroids for last cluster
    if current_centroid == centroid_index and count != 0:
        print(str(round((sum_x / count), 2)) + ", " + str(round((sum_y / count), 2)) + ", " + str(round((sum_z / count), 2)))
    
if __name__ == "__main__":
    get_new_centroids()
