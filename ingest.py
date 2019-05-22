#!/usr/bin/env python

'''
Ingests Product from input metadata.
'''

from __future__ import print_function
import os
import json
import urllib3
import dateutil.parser
import requests
from hysds.celery import app
import usgs_retrieve

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROD_SHORT_NAME = 'EO1_Hyperion'
VERSION = "v1.0"
ALLOWED_PRODUCT_FORMATS = ['L1R', 'L1Gst', 'L1T']
# determined globals
PROD = "{}-{}-{}-{}" #EO1_Hyperion-L1R-20190514T341405_20190514T341435-v1.0
INPUT_TYPE = 'metadata-{}'.format(PROD_SHORT_NAME)
INDEX = 'grq_{}_{}-{}'.format(VERSION, PROD_SHORT_NAME, '{}')

def main():
    '''Localizes and ingests product from input metadata blob'''
    # load parameters
    ctx = load_context()
    metadata = ctx.get("metadata", False)
    prod_type = ctx.get("prod_type", False)
    if not prod_type == INPUT_TYPE:
        raise Exception("input needs to be {}. Input is of type: {}".format(INPUT_TYPE, prod_type))
    prod_format = ctx.get("product_format", False)
    if not prod_format or not prod_format in ALLOWED_PRODUCT_FORMATS:
        raise Exception("input product format of {} is invalid. Must be one of type: {}".format(prod_format, ', '.join(ALLOWED_PRODUCT_FORMATS)))
    starttime = ctx.get("starttime", False)
    endtime = ctx.get("endtime", False)
    location = ctx.get("location", False)
    shortname = metadata.get('short_name')
    #ingest the product
    ingest_product(shortname, starttime, endtime, location, metadata, prod_format)

def ingest_product(shortname, starttime, endtime, location, metadata, prod_format):
    '''determines if the product is localized. if not localizes and ingests the product'''
    # generate product id
    prod_id = gen_prod_id(shortname, starttime, endtime, prod_format)
    # determine if product exists on grq
    if exists(prod_id, prod_format):
        print('product with id: {} already exists. Exiting.'.format(prod_id))
        return
    #attempt to localize product
    print('attempting to localize product: {}'.format(prod_id))
    #build the product id for subproducts
    #run usgs localization
    granule_id = metadata.get('title')
    usgs_retrieve.retrieve(granule_id, shortname, prod_id, prod_format)
    if os.path.exists(prod_id):
        # generate product
        dst, met = gen_jsons(prod_id, starttime, endtime, location, metadata)
        # save the metadata fo;es
        save_product_met(prod_id, dst, met)
        generate_browse(prod_id, prod_id)

def generate_browse(product_dir, prod_id):
    '''generates a browse from an input product path. prioritize jpegs, then pngs, then tifs'''
    allowed_extensions = ['jpg', 'jpeg', 'png', 'tif']
    files = [f for f in os.listdir(product_dir) if os.path.isfile(os.path.join(product_dir, f))]
    for allowed_extension in allowed_extensions:
        for fil in files:
            product_path = os.path.join(product_dir, fil)
            extension = os.path.splitext(fil)[1].strip('.').lower()
            if not extension == allowed_extension:
                continue
            #attempt to generate browse
            browse_path = os.path.join(prod_id, '{}.browse.png'.format(prod_id))
            browse_small_path = os.path.join(prod_id, '{}.browse_small.png'.format(prod_id))
            if os.path.exists(browse_path):
                return
            #conver to png
            os.system("convert {} -transparent black {}".format(product_path, browse_path))
            #convert to small png
            os.system("convert {} -transparent black -resize 700 {}".format(product_path, browse_small_path))
            if os.path.exists(browse_path) and os.path.exists(browse_small_path):
                return

def gen_prod_id(shortname, starttime, endtime, prod_format):
    '''generates the product id from the input metadata & params'''
    start = dateutil.parser.parse(starttime).strftime('%Y%m%dT%H%M%S')
    end = dateutil.parser.parse(endtime).strftime('%Y%m%dT%H%M%S')
    time_str = '{}_{}'.format(start, end)
    return PROD.format(shortname, prod_format, time_str, VERSION)

def exists(uid, prod_format):
    '''queries grq to see if the input id exists. Returns True if it does, False if not'''
    idx = INDEX.format(prod_format)
    grq_ip = app.conf['GRQ_ES_URL']#.replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/{1}/_search'.format(grq_ip, idx)
    es_query = {"query": {"bool": {"must": [{"term": {"id.raw": uid}}]}}, "from": 0, "size": 1}
    return query_es(grq_url, es_query)

def query_es(grq_url, es_query):
    '''simple single elasticsearch query, used for existence. returns count of result.'''
    print('querying: {} with {}'.format(grq_url, es_query))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    try:
        response.raise_for_status()
    except:
        # if there is an error (or 404,just publish
        return 0
    results = json.loads(response.text, encoding='ascii')
    #results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    return int(total_count)

def gen_jsons(prod_id, starttime, endtime, location, metadata):
    '''generates ds and met json blobs'''
    dst = {"label": prod_id, "starttime": starttime, "endtime": endtime, "location": location, "version": VERSION}
    met = metadata
    return dst, met

def save_product_met(prod_id, ds_obj, met_obj):
    '''generates the appropriate product json files in the product directory'''
    if not os.path.exists(prod_id):
        os.mkdir(prod_id)
    outpath = os.path.join(prod_id, '{}.dataset.json'.format(prod_id))
    with open(outpath, 'w') as outf:
        json.dump(ds_obj, outf)
    outpath = os.path.join(prod_id, '{}.met.json'.format(prod_id))
    with open(outpath, 'w') as outf:
        json.dump(met_obj, outf)

def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')

if __name__ == '__main__':
    main()
