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
import pymongo
from pymongo import TEXT
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
DATA_TYPES = ['node', 'way']


#Mongo data analysis
def mongo_db_analysis():
    c = MongoClient()
    db = c.examples
    data = db.redding_streets

    #Number of total records
    print "Total records:"
    print data.find().count()

    #Number of nodes
    print "Total nodes:"
    print data.find({"type":"node"}).count()

    #Number of ways
    print "Total ways:"
    print data.find({"type":"way"}).count()

    #Number of unique uesrs
    print "Total unique users:"
    print len(data.distinct("created.user"))
    
    #Top 7 contributors
    print "Top 7 unique users by contributions:"
    top_users = data.aggregate([{"$group":{"_id":"$created.user", "count": 
            {"$sum":1}}}, {"$sort":{"count": -1}}, 
            {"$limit":7}])
    for u in top_users:
        pprint.pprint(u)

    #Top 8 types of amenities (from Node)
    print "Top 8 amenities (from nodes):"
    amenities = data.aggregate([{"$match" : { "type":"node"}},
            {"$group":{"_id":"$amenity", "count": 
            {"$sum":1}}}, {"$sort":{"count": -1}}, 
            {"$limit":8}])
    for a in amenities:
        pprint.pprint(a)

    toilet_list = []
    print "All toilet ids:"
    toilets = data.find({"amenity":"toilets"})
    for t in toilets:
        pprint.pprint(t['id'])
        toilet_list.append(t['id'])
    print len(toilet_list)


    # #Top 7 contributors
    # print "Top 7 unique users by contributions in nodes:"
    # top_users = data.aggregate([
    #         {"$match" : { "type":"node"}},
    #         {"$group":{"_id":"$created.user", "count": 
    #         {"$sum":1}}}, {"$sort":{"count": -1}}, 
    #         {"$limit":7}])
    # for u in top_users:
    #     pprint.pprint(u)

    # print "Number of entries marked waterway:"
    # top_waterways = data.aggregate([
    #         {"$group":{"_id":"$waterway", "count": 
    #         {"$sum":1}}}, {"$sort":{"count": -1}}, 
    #         {"$limit":8}])
    # for w in top_waterways:
    #     pprint.pprint(w)

    # print "Number of entries marked highway:"
    # top_highway = data.aggregate([
    #         {"$group":{"_id":"$highway", "count": 
    #         {"$sum":1}}}, {"$sort":{"count": -1}}, 
    #         {"$limit":8}])
    # for h in top_highway:
    #     pprint.pprint(h)

    # print "Named entries completed by tiger:"
    # tiger_source = data.aggregate([
    #         {"$group":{"_id":"$tiger:name_base", "count": 
    #         {"$sum":1}}}, {"$sort":{"count": -1}},
    #         {"$limit":8}])
    
    # for t in tiger_source:
    #     pprint.pprint(t)

    # total_tiger = data.aggregate([
    #          {"$group":{"_id":"$tiger:source", "count": 
    #          {"$sum":1}}}
    #          ])
    # print "Total records added by tiger:"
    # for t in total_tiger:
    #     print t

    # print "Cuisines:"
    # cuisine = data.aggregate([
    #         {"$group":{"_id":"$cuisine", "count": 
    #         {"$sum":1}}}, {"$sort":{"count": -1}}])

    # for c in cuisine:
    #     pprint.pprint(c)


def check_for_errors(file_in):
    address_list = {}
    add_count = 0
    for _, element in ET.iterparse(file_in):
        if element.tag == 'node':
            address_list[add_count] = []
            for item in element: 
                if item.attrib['k'][0:4] == 'addr':                
                    address_list[add_count].append(item.attrib)      
            if not address_list[add_count]:
                del address_list[add_count]
            add_count += 1

    #pprint.pprint(address_list)

    postcode_list = []
    street_list = []
    housenumber_list = []
    street_names = []

    for ele in address_list.values():
        for d in ele:
            if d['k'] == 'addr:postcode':
                postcode_list.append(ele)
            if d['k'] == 'addr:street':
                street_list.append(ele)
            if d['k'] == 'addr:housenumber':
                housenumber_list.append(ele)


    #Total number of nodes with any address information
    print "Number of nodes with any address information:"
    print len(address_list)

    #Total number of addresses with 'addr:postcode'
    print "Number of nodes with postcode information:"
    print len(postcode_list)

    #Total number of addresses with 'addr:street'
    print "Number of nodes with street information:"
    print len(street_list)
    for s in street_list:
        for d in s:
            if d['k'] == 'addr:street':
                street_names.append(d['v'])

    #Total number of addresses with 'addr:housenumber'
    print "Number of nodes with housenumber information"
    print len(housenumber_list)

    #List of street names included for reference
    print "List of street names included in dataset:"
    pprint.pprint(street_names)


#Python script analysis
def summarize_original_data(file_in):
    count = 0
    osm_datum = {}
    description = {}
    description['contributors'] = {}
    description['data_points'] = {}
    description['tags'] = {}
    description['amenities'] = {}
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
                        if t == 'k' and point[t] == 'amenity':
                            if point['v'] in description['amenities'].keys():
                                description['amenities'][point['v']] += 1
                            else:
                                description['amenities'][point['v']] = 1

                count += 1
        sorted_users = sorted(description['contributors'].items(), key=operator.itemgetter(1), reverse=True)
        sorted_data_points = sorted(description['data_points'].items(), key=operator.itemgetter(1), reverse=True)
        sorted_tags = sorted(description['tags'].items(), key=operator.itemgetter(1), reverse=True)

        sorted_amenities = sorted(description['amenities'].items(), key=operator.itemgetter(1), reverse=True)

        #Print the total number of records
        print "Total records for OSM {0}:".format(d)
        print count

        # print "Number of descriptive tags for OSM {0}:".format(d)
        # pprint.pprint(sorted_data_points)

        # print "Most common descriptive tags for OSM {0}:".format(d)
        # pprint.pprint(sorted_tags)

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
        user_df['rank_col'] = (user_df.index + 1)
        total_user_countributions = user_df['contributions'].sum()
        user_df['percentage'] = user_df['contributions'] / total_user_countributions
        print user_df
        
        p = ggplot(user_df, aes(x='rank_col', fill= 'percentage', weight='percentage')) +\
        geom_bar(aes(width=0.7, binwidth=2)) +\
        scale_x_continuous(limits = (0.5,8.5), breaks = range(1,9)) +\
        xlab("User contribution rank") +\
        ylab("Share of total contributions")
        # ggtitle("Contributions to Shasta County OSM Nodes")
        # print p

        #create histogram table for node types
        tag_df = DataFrame(sorted_tags)
        tag_df = tag_df.rename(columns = {0:'tag', 1:'occurrences'})
        other_sum = tag_df[7:]['occurrences'].sum()
        tag_df.loc[7] = ['other', other_sum]
        tag_df = tag_df[0:8]
        tag_df['rank_col'] = (tag_df.index + 1)
        total_occurrences = tag_df['occurrences'].sum()
        tag_df['percentage'] = tag_df['occurrences'] / total_occurrences
        #user_df_other = DataFrame({'user':'other','contributions':other_sum}, index=index)
        # print tag_df

        #create histogram table for amenities
        amenity_df = DataFrame(sorted_amenities)
        amenity_df = amenity_df.rename(columns = {0:'amenity', 1:'occurrences'})
        print amenity_df['occurrences'].sum()

        other_sum = amenity_df[7:]['occurrences'].sum()
        amenity_df.loc[7] = ['other', other_sum]
        amenity_df = amenity_df[0:8]
        amenity_df['rank_col'] = (amenity_df.index + 1)
        total_occurrences = amenity_df['occurrences'].sum()
        amenity_df['percentage'] = amenity_df['occurrences'] / total_occurrences
        #user_df_other = DataFrame({'user':'other','contributions':other_sum}, index=index)
        print amenity_df

mongo_db_analysis()
#check_for_errors(INPUT_FILE)
#summarize_original_data(INPUT_FILE)

