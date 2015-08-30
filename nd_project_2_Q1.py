#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
import collections
import pprint
import operator
from pymongo import MongoClient
from ggplot import *
from pandas import DataFrame
"""

<tag k="addr:housenumber" v="5158"/>
<tag k="addr:street" v="North Lincoln Avenue"/>
<tag k="addr:street:name" v="Lincoln"/>
<tag k="addr:street:prefix" v="North"/>
<tag k="addr:street:type" v="Avenue"/>
<tag k="amenity" v="pharmacy"/>

  should be turned into:

{...
"address": {
    "housenumber": 5158,
    "street": "North Lincoln Avenue"
}
"amenity": "pharmacy",
...
}

- for "way" specifically:

  <nd ref="305896090"/>
  <nd ref="1719825889"/>

should be turned into
"node_refs": ["305896090", "1719825889"]
"""

INPUT_FILE = 'shasta_county_map.osm'

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
OTHER_KEYS = ['id', 'visible']
DATA_TYPES = ['node']

def summarize_original_data(file_in):
    count = 0
    osm_datum = {}
    count = 0
    description = {}
    description['contributors'] = {}
    description['data_points'] = {}
    description['tags'] = {}
    for d in DATA_TYPES:
        for _, element in ET.iterparse(file_in):
            if element.tag == d:
                osm_datum[count] = []
                for item in element:   
                    osm_datum[count].append(item.attrib)     
                osm_datum[count].append(element.attrib)
                #Histogram of user contributors
                if 'user' in element.attrib.keys():
                    if element.attrib['user'] in description['contributors'].keys():
                        description['contributors'][element.attrib['user']] += 1
                    else:
                        description['contributors'][element.attrib['user']] = 1    
                #Histogram of number of datapoints included in node
                if len(osm_datum[count]) in description['data_points'].keys():
                    description['data_points'][len(osm_datum[count])] += 1
                else:
                    description['data_points'][len(osm_datum[count])] = 1
                #Histogram of tag frequency
                for point in osm_datum[count]:
                    for t in point.keys():    
                        if t == 'k':                                
                            if point[t] in description['tags'].keys():
                                description['tags'][point[t]] += 1
                            else:
                                description['tags'][point[t]] = 1           
                count += 1


        sorted_users = sorted(description['contributors'].items(), key=operator.itemgetter(1), reverse=True)
        sorted_data_points = sorted(description['data_points'].items(), key=operator.itemgetter(1), reverse=True)
        sorted_tags = sorted(description['tags'].items(), key=operator.itemgetter(1), reverse=True)

        #Print the total number of records
        print "Total records for OSM {0}:".format(d)
        print count

        print "Number of descriptive tags for OSM {0}:".format(d)
        pprint.pprint(sorted_data_points)

        print "Most common descriptive tags for OSM {0}:".format(d)
        pprint.pprint(sorted_tags)

        print "Contributors to OSM {0} by contribution numbers:".format(d)
        pprint.pprint(sorted_users) 

        print "Total unique contributors to OSM {0}:".format(d)
        print len(sorted_users)

        #create histogram for contributor users
        user_df = DataFrame(sorted_users)
        user_df = user_df.rename(columns = {0:'user', 1:'contributions'})
        other_sum = user_df[7:]['contributions'].sum()
        user_df.loc[7] = ['other', other_sum]
        user_df = user_df[0:8]
        user_df['rank_col'] = user_df.index
        #user_df_other = DataFrame({'user':'other','contributions':other_sum}, index=index)
        print user_df
        # user_df = user_df.append(user_df_other)
        # print user_df
        p = ggplot(user_df, aes(x='rank_col', fill= 'contributions', weight='contributions')) +\
        geom_bar(aes(width=0.7), position='stack') +\
        xlim(-0.5, 7.5) +\
        xlab("User contribution rank")
        # ylab("Contributions") +\
        # ggtitle("Contributions to Shasta County OSM Nodes")
        print p

         

        #Count all nodes and append all keys - return distribution of the last key to see what most nodes are

    #In the Northern California dataset there are "{0}".format(way_count), with the most common being ""

    #There are "{0}".format(nodes), with the most common being ""

        #     #Count all ways
    #     if element.tag == "way":
    #         way_attrib.append(dict())
    #         for item in element:
    #             #pprint.pprint(way_attrib[way_count])
    #             way_attrib[way_count].update(item.attrib)     
    #             element_count = 0
    #             if 'k' in way_attrib[way_count]:
    #                 if way_attrib[way_count]['k'] in way_type.keys():
    #                     way_type[way_attrib[way_count]['k']] += 1
    #                 else:
    #                     way_type[way_attrib[way_count]['k']] = 1
    #             element_count += 1
    #         print element_count
    #         way_count += 1

    # pprint.pprint(len(way_attrib))
    # print way_count
    # sorted_types = sorted(way_type.items(), key=operator.itemgetter(1), reverse=True)
    # print sorted_types



def shape_element(element):
    node = {}
    node['created'] = {}
    node['pos'] = []
    node['node_refs'] = []
    if element.tag == "node" or element.tag == "way" :
        node['type'] = element.tag
        for i in CREATED:
            node['created'].update({i:element.attrib[i]})  
        for l in ['lat', 'lon']:
            if l in element.attrib:
                node['pos'].append(float(element.attrib[l]))
        for key in OTHER_KEYS:
            if key in element.attrib:
                node[key] = element.attrib[key]
        for item in element:
            if 'k' in item.attrib:
                k = item.attrib['k']
                v = None if problemchars.search(k) else item.attrib['v']
                if k[0:5] == 'addr:' and ':' in k[6:-1]:
                    pass
                elif k[0:5] == 'addr:':
                    if 'address' not in node.keys():
                        node['address'] = {}
                    node['address'].update({k.replace('addr:',''):v})
                else:
                    node['k'] = v
            if 'ref' in item.attrib:
                node['node_refs'].append(item.attrib['ref'])
            
        if not node['node_refs']:
            del node['node_refs']
            
        return node
    else:
        return None


def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

def insert_in_mongo(data):
    client = MongoClient("mongodb://localhost:27017")
    db = client.examples
    
    db.redding_streets.insert(data)
    print "OSM data for Redding added to Mongo Client"


#data = process_map(INPUT_FILE, pretty=False)
#insert_in_mongo(data)

summarize_original_data(INPUT_FILE)


#summarize_clean_data(data)



#def test():
    # NOTE: if you are running this code on your computer, with a larger dataset, 
    # call the process_map procedure with pretty=False. The pretty=True option adds 
    # additional spaces to the output, making it significantly larger.