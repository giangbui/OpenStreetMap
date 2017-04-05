#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
from collections import defaultdict

"""
Your task is to wrangle the data and transform the shape of the data
into the model we mentioned earlier. The output should be a list of dictionaries
that look like this:

{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Ave"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}

You have to complete the function 'shape_element'.
We have provided a function that will parse the map file, and call the function with the element
as an argument. You should return a dictionary, containing the shaped data for that element.
We have also provided a way to save the data in a file, so that you could use
mongoimport later on to import the shaped data into MongoDB. 

Note that in this exercise we do not use the 'update street name' procedures
you worked on in the previous exercise. If you are using this code in your final
project, you are strongly encouraged to use the code from previous exercise to 
update the street names before you save them to JSON. 

In particular the following things should be done:
- you should process only 2 types of top level tags: "node" and "way"
- all attributes of "node" and "way" should be turned into regular key/value pairs, except:
    - attributes in the CREATED array should be added under a key "created"
    - attributes for latitude and longitude should be added to a "pos" array,
      for use in geospacial indexing. Make sure the values inside "pos" array are floats
      and not strings. 
- if the second level tag "k" value contains problematic characters, it should be ignored
- if the second level tag "k" value starts with "addr:", it should be added to a dictionary "address"
- if the second level tag "k" value does not start with "addr:", but contains ":", you can
  process it in a way that you feel is best. For example, you might split it into a two-level
  dictionary like with "addr:", or otherwise convert the ":" to create a valid key.
- if there is a second ":" that separates the type/direction of a street,
  the tag should be ignored, for example:

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


lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
postal_codes = re.compile(r'\d{5}(?:[-\s]\d{4})?$')

street_types = re.compile(r'\b\S+\.?$', re.IGNORECASE)

CREATED = ["version", "changeset", "timestamp", "user", "uid"]
ATTRIB = ["id", "visible", "amenity", "cuisine", "name", "phone"]

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road","US-40","MO-7","MO-33",
            "Trail", "Parkway", "Commons", "Crescent", "West", "South", "East", "North", "Vista","Creek","Plaza","Circle",
            "Gardens", "Circle", "Gate", "Heights", "Park", "Way", "Mews", "Keep", "Westway", "Glenway", "Queensway", "Wood", "Path", "Terrace", "Appleway","Highway","Broadway", "Trafficway","Indian"]

street_mapping = {"Ave ": "Avenue",
                    "St. Pkwy": "Street Parkway",
                    "Rd #A": "Road",
                   "St. ": "Street",
                   "St.": "Street",
                   "Rd.": "Road",
                   "Pkwy": "Parkway",
                   "Rd" : "Road"
                   }

fixed_street_names = {}
bad_postal_codes = []


def standarize_name(street_name):
    lower = street_name.lower()
    l = list(lower)
    l[0] = l[0].upper()
    return str(l)

def audit_street_type(street_name):
    """Return the fixed street name or return untouched street name if expected."""

    m = street_types.search(street_name)
    if m:
        street_type = standarize_name(m.group())
        if street_type not in expected:
            return update_street_name(street_name, street_mapping)

    # TODO: rather None/null or a bad street name?
    return street_name


def update_street_name(name, mapping):
     # YOUR CODE HERE
    m = street_types.search(name)
    street_type = m.group()
    try:
        fixed_name = name.replace(street_type, mapping[street_type])
        fixed_street_names[name] = fixed_name
    except KeyError:
        pass
    return name


def is_street_name(address_key):
    return address_key == 'addr:street'


def audit_postal_code(postal_code):
    """Return matched postal code and add bad ones to list."""
    l = postal_codes.findall(postal_code)
    if len(l)>0:
        return l[0]

    # TODO: rather None/null or a bad postal code?
    bad_postal_codes.append(postal_code)
    return postal_code


def is_postal_code(address_key):
    return address_key == 'addr:postcode'


def shape_element(element):
    """
    Parse, validate and format node and way xml elements.
    Return list of dictionaries

    Keyword arguments:
    element -- element object from xml element tree iterparse
    """
    if element.tag == 'node' or element.tag == 'way':


        # Add empty tags - created (dictionary) and type (key/value )
        node = {'created': {}, 'type': element.tag}

        # Update pos array with lat and lon
        if 'lat' in element.attrib and 'lon' in element.attrib:
            node['pos'] = [float(element.attrib['lat']), float(element.attrib['lon'])]

        # Deal with node and way attributes
        for k in element.attrib:

            if k == 'lat' or k == 'lon':
                continue
            if k in CREATED:
                node['created'][k] = element.attrib[k]
            else:
                # Add everything else
                node[k] = element.attrib[k]

        # Deal with second level tag items
        for tag in element.iter('tag'):

            k = tag.attrib['k']
            v = tag.attrib['v']

            # Search for problem characters in 'k' and ignore them
            if problemchars.search(k):
                # Add to array to print out later
                continue
            elif k.startswith('addr:'):
                
                address = k.split(':')
                if len(address) == 2:
                    if 'address' not in node:
                        node['address'] = {}                    
                    if is_street_name(k):
                        v = audit_street_type(v)
                    if is_postal_code(k):
                        v = audit_postal_code(v)
                    try:                        
                        node['address'][address[1]] = v                       
                    except TypeError:
                        pass
                                    
            else:
                if(k!='address'):
                    node[k] = v

        # Add nd ref as key/value pair from way
        node_refs = []
        for nd in element.iter('nd'):
            node_refs.append(nd.attrib['ref'])

        # Only add node_refs array to node if exists
        if len(node_refs) > 0:
            node['node_refs'] = node_refs

        return node
    else:
        return None


def process_map(file_in, pretty=True):
    file_out = "{0}.json".format(file_in)
    data = []
    N = 0
    with codecs.open(file_out, "w") as fo:
        fo.write("[\n")
        for idx, element in ET.iterparse(file_in):
            el = shape_element(element)            
            if el:                
                data.append(el)
        for idx,el in enumerate(data):
            if pretty:
                if(idx<len(data)-1):
                    fo.write(json.dumps(el, indent=2) + ",\n")
                else:
                    fo.write(json.dumps(el, indent=2) + "\n")
            else:
                if(idx<len(data)-1):
                    fo.write(json.dumps(el) + ",\n")
                else:
                    fo.write(json.dumps(el) + "\n")
        fo.write("]\n")

        # Keep track of things
        print 'Fixed street names:', fixed_street_names
        print 'Bad postal code:', bad_postal_codes

        for k,v in fixed_street_names.items():
            print k,"===>",v

    return data


def test():
    # call the process_map procedure with pretty=False. The pretty=True option adds
    # additional spaces to the output, making it significantly larger.
    #data = process_map('kansas-city-lawrence-topeka_kansas.osm', False)    
    data = process_map('sample.osm', False)    
    #pprint.pprint(data)


if __name__ == "__main__":
    test()


