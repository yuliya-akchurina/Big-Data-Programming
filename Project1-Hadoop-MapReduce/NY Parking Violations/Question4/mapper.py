#!/usr/bin/python3
# Project1 - P1Q4 - mapper.py
import sys

for line in sys.stdin:
        line = line.strip()
        line_list = line.split(',')
        
        if len(line_list)> 33:           
            if len(line_list[33])!= 0 and line_list[33].isalpha():                
                print('%s\t%s' % (line_list[33], 1))                
        else:
            pass