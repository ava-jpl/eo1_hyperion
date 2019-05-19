#!/usr/bin/env python

'''
localizes USGS Granule & products to product dir
'''
from __future__ import print_function
import os
import json
import requests
import subprocess as sub
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

DATASET_MAPPING = {'EO1_ALI': 'EO1_ALI_PUB', 'EO1_Hyperion': 'EO1_HYP_PUB'}
PRODUCT_MAPPING = {'STANDARD': 'ZIP', 'L1T': 'TIF', 'L1R': 'ZIP', 'L1Gst': 'ZIP', 'FRB': 'jpg', 'GRB': 'ZIP'}

def retrieve(granule_id, short_name, prod_id, prod_format):
    '''pulls the granule using the usgs cli. creates & saves product to product_id folder if the product format exists'''
    dataset = DATASET_MAPPING.get(short_name)
    cmd = ['usgs', 'download-options', '--node', 'EE', dataset, granule_id]
    #print('running: {}'.format(' '.join(cmd)))
    result = json.loads(sub.check_output(cmd))
    data = result.get('data', [])[0].get('downloadOptions', False)
    if not data:
        print('No products returned for: {}'.format(' '.join(cmd)))
        return 0
    download_format_list = [x.get('downloadCode', False) for x in data]
    print('Product formats available for {}: {}'.format(granule_id, ', '.join(download_format_list)))
    if not prod_format in download_format_list:
        print('USGS granule: {} unable to generate product type: {}. Product types available: {}'.format(granule_id, prod_format, ', '.join(download_format_list)))
        return 0
    #localize & extract proper product
    print("Product type: {} is available. Attempting to ingest".format(prod_format))
    success = localize(dataset, granule_id, prod_id, prod_format)
    if success:
        #localize browse
        localize(dataset, granule_id, prod_id, 'FRB')
    else:
        print('failed to localize product')
    return success


def localize(dataset, granule_id, prod_id, download_format):
    '''localizes the product with the given inputs'''
    # get the appropriate url
    cmd = ['usgs', 'download-url', dataset, granule_id, '--node', 'EE', '--product', download_format]
    print(' '.join(cmd))
    result = json.loads(sub.check_output(cmd)).get('data', [])
    print('download url result: {}'.format(result))
    for item in result:
        url = item.get('url', False)
        if not url:
            continue
        # determine the path
        extension = PRODUCT_MAPPING.get(download_format)
        prod_filename = '{}.{}'.format(prod_id, extension)
        if not os.path.exists(prod_id):
            os.mkdir(prod_id)
        out_path = os.path.join(prod_id, prod_filename)
        print('localizing product: {}'.format(prod_filename))
        success = download(url, out_path)
        if extension == 'ZIP':
            print('extracting product: {}'.format(out_path))
            sub.check_output(['unzip', out_path])
        return success

def download(url, product_path):
    '''downloads the product. Returns True if successful, False otherwise'''
    s = requests.Session()
    purl = 'https://ers.cr.usgs.gov/login/'
    user_agent = {'User-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36'}
    r = s.post(purl, headers=user_agent)
    #build the full path if it does not exist
    f = open(product_path, 'wb')
    r = s.get(url, headers=user_agent)
    #print r.text
    for chunk in r.iter_content(chunk_size=512 * 1024):
        if chunk: # filter out keep-alive new chunks
            f.write(chunk)
    f.close()
    #determine if the finished file is too small. if it is, remove it
    if os.path.exists(product_path):
        sizeMB = os.path.getsize(product_path)/1024
        if (sizeMB < .1):
            print('retrieved file only {} megabytes, removing.'.format(sizeMB))
            os.remove(product_path)
            return False
    return True
 
if __name__ == '__main__':
    #test
    retrieve('EO1H0420362005021110KZ_AGS_01', 'EO1_Hyperion', 'EO1_Hyperion-TEST_GRANULE-v1.0', 'L1T')
