#!/usr/bin/python3
# Project1 - P2Q2 - mapper2.py

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

def assign_to_clusters():
    player_list = ['james harden', 'chris paul', 'stephen curry', 'lebron james']
    
    min_dist = 10000000
    index = -1

    for line in sys.stdin:
        line = line.strip()
        line_list = re.split(r',(?=(?:"[^"]*?(?: [^"]*)*))|,(?=[^",]+(?:,|$))', line)

        if len(line_list) >= 19 and line_list[19].strip().lower() in player_list:
            #strip white space from each cell value
            shot_clock = line_list[8].strip()
            shot_dist = line_list[11].strip()
            close_def_dist = line_list[16].strip()
            player_name = line_list[19].strip()
            shot_result = line_list[13].strip()

            if len(shot_clock)!= 0 and len(shot_dist)!=0 and len(close_def_dist)!=0 and len(player_name)!=0 and len(shot_result)!=0:
                try:
                    shot_clock = float(shot_clock)
                    shot_dist = float(shot_dist)
                    close_def_dist = float(close_def_dist)
                except ValueError:
                    continue
                # list of floats
                xyz = [shot_clock, shot_dist, close_def_dist]
                
                centroids = get_centroids('new_centroids.txt')
                for centroid in centroids:
                    # Euclidean distance from every point of dataset to every centroid
                    cur_dist = sqrt(pow(xyz[0] - centroid[0], 2) + pow(xyz[1] - centroid[1], 2) + pow(xyz[2] - centroid[2], 2))
                    # find the centroid which is closer to the point
                    if cur_dist <= min_dist:
                        min_dist = cur_dist
                        index = centroids.index(centroid)
                #var = "%s\t%s,%s,%s" % (index, xyz[0], xyz[1], xyz[2])

                if shot_result == 'made':
                    print('%s,%s\t%s,%s,%s,%s' % (index, player_name, shot_clock, shot_dist, close_def_dist, 1))
                if shot_result == 'missed':
                    print('%s,%s\t%s,%s,%s,%s' % (index, player_name, shot_clock, shot_dist, close_def_dist, 0))

if __name__ == "__main__":
    assign_to_clusters()