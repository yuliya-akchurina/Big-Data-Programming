#!/usr/bin/python

from operator import itemgetter
import sys

dict_ip_count = {}
userhour = sys.argv[0]

for line in sys.stdin:
        line = line.strip()
        ip, num = line.split('\t')
        try:
                num = int(num)
                dict_ip_count[ip] = dict_ip_count.get(ip, 0) + num
        except ValueError:
                pass

output_list = []

sorted_dict_ip_count = sorted(dict_ip_count.items(), key=itemgetter(0))
for ip, count in sorted_dict_ip_count:
        hour, ipval = ip.split('-')
        output_list.append([hour, count, ipval])

rows = output_list
d = {}
for row in rows:
        if row[0] not in d:
                d[row[0]] = []
        d[row[0]].append(row[1:])

for lst2d in d.values():
        lst2d.sort(key=lambda lst: lst[0], reverse = True)

hours_conversion_dict = {1:'00:00', 2:'01:00', 3:'02:00', 4:'03:00', 5:'04:00', 6:'05:00',
                      7:'06:00', 8:'07:00', 9:'08:00', 10:'09:00', 11:'10:00', 12:'11:00',
                      13:'12:00',14:'13:00', 15:'14:00', 16:'15:00', 17:'16:00',
                      18:'17:00', 19:'18:00', 20:'19:00', 21:'20:00', 22:'21:00',
                      23:'22:00', 24:'23:00'}

if userhour!=0:
    hour = hours_conversion_dict.get(userhour)
    if hour in d.keys():
        if len(d[hour])>2:
            print('%s\t%s%s%s' % (hour, d[hour][0], d[hour][1], d[hour][2]))
    else:
        print('No IP address hits during this hour: ', userhour)