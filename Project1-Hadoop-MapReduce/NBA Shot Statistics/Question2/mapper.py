#!/usr/bin/python3
# Project1 - P2Q2 - mapper.py

import sys
import re
from math import sqrt
from math import pow

def get_centroids(centroids_filepath):
    centroids = []
    
    with open(centroids_filepath) as f:
        lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            line_list = line.split(',')
            centroids.append([float(line_list[0]), float(line_list[1]), float(line_list[2])])
    #print(centroids)
    return(centroids)

def assign_clusters(centroids):
    player_list = ['james harden', 'chris paul', 'stephen curry', 'lebron james']
    #player_list = ['stephen curry']

    # get data points {x = SHOT DIST, y = CLOSE DEF DIST, z = SHOT CLOCK}
    for line in sys.stdin:
        line = line.strip()
        line_list = re.split(r',(?=(?:"[^"]*?(?: [^"]*)*))|,(?=[^",]+(?:,|$))', line)
        min_dist = 1000000
        index = -1
        
        #skip rows that do not have columns in this range and include only 4 player
        if len(line_list)>=19 and line_list[19].strip().lower() in player_list:
            shot_clock = line_list[8].strip()
            shot_dist = line_list[11].strip()
            close_def_dist = line_list[16].strip()
            #player_name = line_list[19].strip()   # player_name 
            #shot_result = line_list[13].strip()   # shot_result

            if len(shot_clock)!=0 and len(shot_dist)!=0 and len(close_def_dist)!=0:
                try:
                    shot_clock = float(shot_clock)
                    shot_dist = float(shot_dist)
                    close_def_dist = float(close_def_dist)
		
                except ValueError:
                    continue
		        # list of floats
                xyz = [shot_clock, shot_dist, close_def_dist]
                
                for centroid in centroids:
                    # Euclidean distance from every point of dataset to every centroid
                    cur_dist = sqrt(pow(xyz[0] - centroid[0], 2) + pow(xyz[1] - centroid[1], 2) + pow(xyz[2] - centroid[2], 2))
                    # find the centroid that is closer to the point
                    if cur_dist <= min_dist:
                        min_dist = cur_dist
                        index = centroids.index(centroid)
                var = "%s\t%s,%s,%s" % (index, xyz[0], xyz[1], xyz[2])
                print(var)

if __name__ == "__main__":
    centroids = get_centroids('centroids.txt')
    #centroids = [[8.0,9.3,3.6], [20.5, 23.3, 20.1], [6.0,24.5,3.8], [10.6,4.3,3.2]]
    assign_clusters(centroids)	
