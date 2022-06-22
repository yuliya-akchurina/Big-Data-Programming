#!/usr/bin/python3
# Project1 - P2Q1 - reducer.py
import sys

shot_count_dict = {}
miss_dict_byplayer = {}

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

for line in sys.stdin:
    line = line.strip()
	player_defender, score = line.split('\t')
	
	#separate player and defender
	player, defender = player_defender.split(',')
	
	hit_count = 0
	
	if player_defender in shot_count_dict.keys():
		score = int(score)
		
		hit_count = int(shot_count_dict.get(player_defender[0], 0)) + score
		
		# update dictionary Values index 0 - hit count( sum score ) and index 1 - count score
		shot_count_dict[player_defender] = [shot_count_dict[player_defender][0] + hit_count, 
                                                shot_count_dict[player_defender][1] + 1]
		
	else:
		hit_count = int(score)
		shot_count_dict[player_defender] = [int(score), 1]
		
for player_defender in shot_count_dict.keys():
    miss_rate = (shot_count_dict[player_defender][1] - shot_count_dict[player_defender][0])/shot_count_dict[player_defender][1]
        
    player, defender = player_defender.split(',')
    
    # create a dictionary {player: [miss_rate1, defender1], [miss_rate2, defender2], etc}
    append_value(miss_dict_byplayer, player, [miss_rate, defender])

# sort dictionary 
res = dict()
for key in sorted(miss_dict_byplayer):
    res[key] = sorted(miss_dict_byplayer[key], reverse = True)

# print player \t most unwanted defender
for key in res:
    print('%s\t%s' % (key, res[key][0][1]))
	