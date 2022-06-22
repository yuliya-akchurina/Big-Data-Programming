#!/usr/bin/python
from operator import itemgetter
import sys

dict_ip_count = {}

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

for k, v in d.items():
	if len(v)>2:
		print('%s\t%s%s%s' % (k, v[0], v[1], v[2]))
	
