#!/usr/bin/python3
# Project1 - P1Q1 - mapper.py
# 2 columns 'Issue Date' - col 5, 'Violation Time' - col 20

import sys

for line in sys.stdin:
    line = line.strip()
    line_list = line.split(',')

    if len(line_list)>3:
        #strip white space from each cell value 
        line_list[4] = line_list[4].strip()
        line_list[19] = line_list[19].strip()            
        
        if len(line_list[4])!=0 and len(line_list[19])!=0:
            print('%s,%s\t%s' % (line_list[4], line_list[19], 1))

        else:
            pass
    else:
        pass