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
from sets import Set

INPUT_FILE = 'shasta_county_map.osm'

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
OTHER_KEYS = ['id', 'visible']
ADDRESS_REPLACEMENTS = {'Trl':'Trail', 'Ln':'Lane', 'Rd':'Road','rd':'Road'}

def shape_element(element, city_zipcode_dict):
    node = {}
    node['created'] = {}
    node['pos'] = []
    node['node_refs'] = []
    if element.tag == "node" or element.tag == "way" :
        node['el_type'] = element.tag
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
                    if k == 'addr:street':
                    	for term in ADDRESS_REPLACEMENTS.keys():
                    		v = v.replace(term,ADDRESS_REPLACEMENTS[term])
                    node['address'].update({k.replace('addr:',''):v})
                else:
                    node[k] = v
            if 'ref' in item.attrib:
                node['node_refs'].append(item.attrib['ref'])   
        if not node['node_refs']:
            del node['node_refs']
        if 'address' in node.keys():
            if 'city' not in node['address'].keys() and 'postcode' in node['address'].keys():
                for c in city_zipcode_dict.keys():
                    if node['address']['postcode'] in city_zipcode_dict[c]:
                        node['address']['city'] = c
        return node
    else:
        return None

def generate_city_zipcode_dict(file_in):
    city_zipcode_dict = {}
    city_postal_list = {}
    add_count = 0
    for _, element in ET.iterparse(file_in):
        if element.tag == 'node':
            city_postal_list[add_count] = []
            has_city_or_code = 0
            for item in element:
                if item.attrib['k'] == 'addr:city' or item.attrib['k'] == 'addr:postcode':
                    has_city_or_code += 1
                    city_postal_list[add_count].append(item.attrib['v'])
                    if item.attrib['k'] == 'addr:city' and item.attrib['v'] not in city_zipcode_dict.keys():
                        city_zipcode_dict[item.attrib['v']] = Set([])
                    elif len(city_postal_list[add_count]) == 2:
                        city_zipcode_dict[city_postal_list[add_count][0]].add(city_postal_list[add_count][1])
            add_count += 1
    return city_zipcode_dict

def process_map(file_in, city_zipcode_dict, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element, city_zipcode_dict)
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
    print "OSM data for Shasta County added to Mongo Client"

city_zipcode_dict = generate_city_zipcode_dict(INPUT_FILE)
print city_zipcode_dict
data = process_map(INPUT_FILE, city_zipcode_dict, pretty=False)
insert_in_mongo(data)