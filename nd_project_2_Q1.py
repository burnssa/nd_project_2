#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
import collections
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
    print data
    return data
    

process_map(INPUT_FILE, pretty=False)


#def test():
    # NOTE: if you are running this code on your computer, with a larger dataset, 
    # call the process_map procedure with pretty=False. The pretty=True option adds 
    # additional spaces to the output, making it significantly larger.