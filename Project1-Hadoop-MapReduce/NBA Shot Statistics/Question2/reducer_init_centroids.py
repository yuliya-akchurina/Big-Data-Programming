#!/usr/bin/python3

#{x = SHOT DIST, y = CLOSE DEF DIST, z = SHOT CLOCK}
# ['james harden', 'chris paul', 'stephen curry', 'lebron james']

import sys
import random

def initialize_centroids():
    current_player = None

    dict_curry = {}
    dict_harden = {}
    dict_paul = {}
    dict_james = {}

    for line in sys.stdin:
        line = line.strip()
        player_name, xyz = line.split('\t')
        shot_dist, defender_dist, shot_clock = xyz.split(',')

        try:
            shot_dist = float(shot_dist)
            defender_dist = float(defender_dist)
            shot_clock = float(shot_clock)
        except ValueError:
            continue

        if player_name == "chris paul":
            dict_paul[shot_dist, defender_dist, shot_clock] = ['chris paul']
            c1 = random.sample(dict_paul.keys(),1)

        elif player_name == "james harden":
            dict_harden[shot_dist, defender_dist, shot_clock] = ['james harden']
            c2 = random.sample(dict_harden.keys(),1)

        elif player_name == "lebron james":
            dict_james[shot_dist, defender_dist, shot_clock] = ['lebron james']
            c3 = random.sample(dict_james.keys(),1)
        elif player_name == "stephen curry":
            dict_curry[shot_dist, defender_dist, shot_clock] = ['stephen curry']
            c4 = random.sample(dict_curry.keys(),1)

        else:
            pass

    c1 = str(c1).lstrip('[(').rstrip(')]')
    c2 = str(c2).lstrip('[(').rstrip(')]')
    c3 = str(c3).lstrip('[(').rstrip(')]')
    c4 = str(c4).lstrip('[(').rstrip(')]')
    print('%s\n%s\n%s\n%s' % (c1, c2, c3, c4))
    
if __name__ == "__main__":
    initialize_centroids()
    


