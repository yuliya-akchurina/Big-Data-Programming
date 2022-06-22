#!/usr/bin/python3
# Project1 - P1Q2 - mapper.py
import sys

for line in sys.stdin:
        line = line.strip()
        line_list = line.split(',')
		
		if len(line_list)> 5:
            #strip white space from each cell value 
            line_list[6] = line_list[6].strip()
            line_list[35] = line_list[35].strip()            
        
            if len(line_list[6])!= 0 and len(line_list[35])!=0 and line_list[35].isnumeric() and int(line_list[35]) in range(1885,2022):  
                print('%s,%s\t%s' % (line_list[6], line_list[35], 1))
                
            else:
                pass
        else:
            pass