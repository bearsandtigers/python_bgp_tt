#!/usr/bin/env python3

import re
import sys
import ipaddress
from mysql import connector
from itertools import islice
from collections import defaultdict

db = { 'host' : 'localhost',
       'database' : 'bgp',
       'user'     : 'bgp',
       'passwd' : 'bgp'
}

#bgp_file = './test5000000.bgp'
#bgp_file = './test1000000.bgp'
bgp_file = '/home/user/oix-full-snapshot-2018-12-01-0200'

asn_list = { 'GOOGLE': '15169',
             'YANDEX': '13238'
}

#  dict of lists to store networks list for each company
networks = { i: list() for i in asn_list.values() }

skip_network = ''

def print_networks():
    global asn_list, networks
    for company, asn in asn_list.items():
        print('  {0} ({1})'.format(company, asn))
        i = 0
        for n in sorted(networks[asn]):
            i += 1
            print('  {0}   {1} '.format(i, n))
#           print('    {1} '.format(i, n))

def match_network(network, asn):
    global asn_list, networks
    if asn in asn_list.values():
        network = ipaddress.ip_network(network)
        prefix = network.prefixlen
#  skip network if it is a subnet of already enlisted network
        for n in networks[asn]:
#  in python 3.7 we could just use 'subnet_of' method:
#
#  ...if network.subnetof(n): return False...
#
#  in 3.6 we have to iterate manually
            n_prefix = n.prefixlen
            if prefix <= n_prefix: continue
            if network in n.subnets(new_prefix=prefix): return False
        networks[asn].append(network)

f = open(bgp_file, 'r')

#  filter yandex and google networks first

#  skip first 5 lines of the input file
for line in islice(f, 5, None):
    _, network, _, _, _, _, *asns = line.split()
    #  skip same network lines
    if skip_network == network:
        continue
    else:
        skip_network = network
    asn = str(asns[-2])
    asn = re.sub('[^0-9]', '', asn)
#  pick networks belonging to yandex or google
    match_network(network, asn)
f.close()

#  aggregate networks

print('\nNetworks (before aggregation):')
print_networks()

for asn in networks.keys():
    print('\nAggregating asn {0}...'.format(asn))
    prev_network = None
#  go throughout networks list,
#  from the longest network prefix to the shortest one
    for prefix in reversed(range(1,32)):
#  create temporary list for this prefix
        temp_list = list()
        for network in networks[asn]:
            if network.prefixlen == prefix:
                temp_list.append(network)
#  try to aggregate each 2 adjacent networks...
        for network in sorted(temp_list):
            if prev_network == None:
                prev_network = network
                continue
            supernet1 = prev_network.supernet()
            supernet2 = network.supernet()

            if not supernet1 == supernet2:
                pass
            else:
#  ...and if they both belong to one common supernet,
#  delete them from list, and add their supernet instead
#                print('aggregating ' + str(prev_network) + ' and ' +
#                   str(network) + ' into ' + str(supernet1))
                networks[asn].remove(prev_network)
                networks[asn].remove(network)
                networks[asn].append(supernet1)
            prev_network = network

print('\nNetworks (aggregated):')
print_networks()
print('\nWriting data into database...')
try:
    mydb = connector.connect(
        host = db['host'],
        database = db['database'],
        user = db['user'],
        passwd = db['passwd']
    )
except Exception as err:
    print("Database connection error: {0}".format(err))
    exit(1)
mycursor = mydb.cursor()
tables = [ """
CREATE TABLE IF NOT EXISTS `asn` (
  `asn` int NOT NULL,
  `company` varchar(100) NOT NULL,
  PRIMARY KEY (`asn`)
);
""" , """
CREATE TABLE IF NOT EXISTS  `network` (
  `network` varchar(20) NOT NULL,
  `asn` int NOT NULL,
  PRIMARY KEY (`network`),
  FOREIGN KEY (`asn`) REFERENCES asn(`asn`)
);
"""
]

try:
    for t in tables:
        mycursor.execute(t)
except Exception as err:
    print("Tables creation error: {0}".format(err))
    exit(1)

try:
    sql = """REPLACE INTO asn(asn, company) VALUES (%s, %s);"""
    values = [ ( asn, company ) for company, asn in asn_list.items() ]
    mycursor.executemany(sql, values)
    mydb.commit()

    sql = """REPLACE INTO network(network, asn) VALUES (%s, %s);"""
    values = [ (str(network), asn) for asn in networks.keys() for network in networks[asn] ]
    mycursor.executemany(sql, values)
    mydb.commit()
except Exception as err:
    print("Inserting data error: {0}".format(err))
    exit(1)

mydb.close()

exit(0)

