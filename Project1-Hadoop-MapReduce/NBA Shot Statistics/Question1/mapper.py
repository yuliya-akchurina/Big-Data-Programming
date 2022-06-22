#!/usr/bin/python3
# Project1 - P2Q1 - mapper.py
import sys
import re

for line in sys.stdin:
    line = line.strip()
    
	line_list = re.split(r',(?=(?:"[^"]*?(?: [^"]*)*))|,(?=[^",]+(?:,|$))', line)
	
	#skip rows that do not have columns in this range
	if len(line_list) >= 19:
		#strip white space from each cell value
		line_list[13] = line_list[13].strip()  # shot_result
		line_list[14] = line_list[14].strip().rstrip('\"').lstrip('\"')  # defender name         
		line_list[19] = line_list[19].strip()   # player name
        
		# change defender name format to be first name last name
		if ',' in line_list[14]:
			def_name_lst = line_list[14].split(',')
			def_name = def_name_lst[1].strip() + " " + def_name_lst[0].strip()
			
		else:
			def_name = line_list[14]
		
		if len(line_list[13])!= 0 and len(def_name)!=0 and len(line_list[19])!=0:

			if line_list[13] == 'made':
				print('%s,%s\t%s' % (line_list[19], def_name, 1))                    
				
			elif line_list[14] == 'missed': 
				print('%s,%s\t%s' % (line_list[19], def_name, 0)) 
				
			else: 
				pass
		else:
			pass
	else:
		pass