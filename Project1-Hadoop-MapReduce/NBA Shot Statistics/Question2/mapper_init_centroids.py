#!/usr/bin/python3
# mapper to initialize centroids 
# {x = SHOT DIST, y = CLOSE DEF DIST, z = SHOT CLOCK}

import sys
import re

def read_data():
    player_list = ['james harden', 'chris paul', 'stephen curry', 'lebron james']

    for line in sys.stdin:
        line = line.strip()
        line_list = re.split(r',(?=(?:"[^"]*?(?: [^"]*)*))|,(?=[^",]+(?:,|$))', line)
        if len(line_list)>19 and line_list[19].strip().lower() in player_list: 
            shot_clock = line_list[8].strip()
            shot_dist = line_list[11].strip()
            defender_dist = line_list[16].strip()
            player_name = line_list[19].strip()

            if len(shot_clock)!=0 and len(shot_dist)!=0 and len(defender_dist)!=0 and len(player_name)!=0:
                print('%s\t%s,%s,%s' % (player_name, shot_dist, defender_dist, shot_clock))
            
            else: 
                pass
        else:
            pass

if __name__ == "__main__":
    read_data()