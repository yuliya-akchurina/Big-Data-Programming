#!/usr/bin/python3
# Project1 - P1Q3 - mapper.py

import sys

# Col 23 - House Number
# Col 24 - Street Name

for line in sys.stdin:
    line = line.strip()
    line_list = line.split(',')

    if len(line_list)>24:
        #strip white space from each cell value 
        line_list[23] = line_list[23].strip()
        line_list[24] = line_list[24].strip()            
        
        if len(line_list[23])!=0 and len(line_list[24])!=0:
            print('%s,%s\t%s' % (line_list[23], line_list[24], 1))
            #print('%s\t%s' % (line_list[24], 1))

        else:
            pass
    else:
        pass