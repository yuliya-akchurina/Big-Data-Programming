#!/usr/bin/python3
# Project1 - P2Q2 - reducer2.py

import sys
import re

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

# Append value to an existing key by converting a key to list type
def append_value(dict_obj, key, value):
    # Check if key exist in dict or not
    if key in dict_obj:
        # Key exist in dict. Check if type of value of key is list or not
        if not isinstance(dict_obj[key], list):
            # If type is not list then make it list
            dict_obj[key] = [dict_obj[key]]
        # Append the value in list
        dict_obj[key].append(value)
    else:
        # As key is not in dict, add key-value pair
        dict_obj[key] = [value]

def calculate_zone_hitrate():
    current_centroid = None
    current_player = None
    count = 0
    sum_score = 0
    dict_player = {}

    for line in sys.stdin:
        line = line.strip()
        cindex_player, xyz_score = line.split('\t')
        centroid_index, player = cindex_player.split(',')
        shot_clock, shot_dist, close_def_dist, score = xyz_score.split(',')

        if len(shot_clock)!=0 and len(shot_dist)!=0 and len(close_def_dist)!=0 and len(score)!=0:
            try:
                centroid_index = int(centroid_index)
                shot_clock = float(shot_clock)
                shot_dist = float(shot_dist)
                close_def_dist = float(close_def_dist)
                score = int(score)
            except ValueError:
                continue
			
			# this IF-switch works because Hadoop sorts map output by key (here: key is unique value of cluster index, player) before passing it to reducer
            if current_centroid == centroid_index:
                if current_player == player:
                    count += 1
                    sum_score += score

                else:
                    if count!=0:
                        #print(current_centroid, ', ', current_player, ', ', round(sum_score/count,2))
                        append_value(dict_player, current_player,[round(sum_score/count,2), current_centroid])

                    current_player = player
                    sum_score = score            
                    count = 1
                
            else:
                if count != 0:
                    #print(current_centroid, ', ', current_player, ', ', round(sum_score/count,2))
                    append_value(dict_player, current_player,[round(sum_score/count,2), current_centroid])
        
                current_centroid = centroid_index
                current_player = player
                sum_score = score            
                count = 1
        
    # print last cluster's centroids
    if current_centroid == centroid_index and count != 0:
        #print(current_centroid, ', ', current_player, ', ', round(sum_score/count,2))
        append_value(dict_player, current_player,[round(sum_score/count,2), current_centroid])
    
    #print("dict_player", dict_player)
    finalized_centroids = get_centroids('new_centroids.txt')

    # sort dictionary 
    res = dict()
    for key in sorted(dict_player):
        res[key] = sorted(dict_player[key], reverse = True)

    # print player \t index and centroids for the cluster with the highest hit rate 
    for key in res:
        print('%s\t%s, %s' % (key, res[key][0][1], finalized_centroids[res[key][0][1]]))

if __name__ == "__main__":
    calculate_zone_hitrate()