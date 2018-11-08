
#!/usr/bin/python

import sys
import re
import socket
import os
import json
import time
import datetime
import httplib
from urlparse import urlparse
from urllib2 import URLError,Request,urlopen,HTTPError
import mysql.connector
from dremiovdsutils import prepare_vds_request
from readconfig import get_config_params
from testsocket import test_socket

def retrieve_dremio_token(config_dict):
    """ Retrieve token from Dremio host """
    url_dict = urlparse(config_dict['dremio_auth_url'])
    # Test socket connectivity to Dremio instance
    if test_socket(
            url_dict.hostname, url_dict.port, "Dremio server") is False:
        print('Unable to connect to Dremio instance. Exiting program!')
        quit(1)
    # Construct URL request for authentication
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'}
    loginData = {
        'userName': config_dict['dremio_user'],
        'password': config_dict['dremio_pass']}
    req = Request(
        config_dict['dremio_auth_url'],
        data=json.dumps(loginData),
        headers=headers)
    try:
        response = urlopen(req, timeout=30)
        data = json.loads(response.read())
        # retrieve the login token
        token = data['token']
        return {
            'content-type': 'application/json',
            'accept': 'application/json',
            'authorization': '_dremio{authToken}'.format(
                authToken=token)}
    except (URLError, HTTPError) as e:
        print 'Dremio authentication failed with error:', e.read()


def retrieve_views(conn_params):
    sql_stmt = "select tbl_name,view_original_text from TBLS " +\
               "where db_id = (" +\
               "select db_id from DBS where name = '%s') " \
                              % conn_params["hivedb_name"] +\
               "and tbl_type = 'VIRTUAL_VIEW'"

    # Connect to MySQL database
    try:
        conn = mysql.connector.connect(
            host=conn_params['metastore_host'],
            database=conn_params['metastore_db'],
            user=conn_params['metastore_user'],
            password=conn_params['metastore_pass'])
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
            print('%s INFO: Hive Database : %s' % (datetime.datetime.now(),conn_params['hivedb_name']))
            print('%s INFO: Total view count: %d' % (datetime.datetime.now(),cursor.rowcount))
            cursor.close()
            conn.close()
            return rows
    except mysql.connector.Error as e:
        print(e)
        return []

def main():

    # If config file explicitly passed, use it. Else fall back to
    # dremio_config.ini as default filename
    config_file = sys.argv[1] if len(
        sys.argv) >= 2 else os.path.join(
        os.path.dirname(__file__),
        "dremio_config.ini")
    config_dict = {}
    config_dict = get_config_params(config_file)
    hive_views = []
    hive_views = retrieve_views(config_dict)

    if (len(hive_views) > 0):
        dremio_auth_headers = retrieve_dremio_token(config_dict)
        vds_create_success = {}
        vds_create_failure = {}
        vds_create_success, vds_create_failure = prepare_vds_request(
            hive_views, config_dict, dremio_auth_headers)
        with open('vds_create_success.json', 'w') as sfp:
            json.dump(vds_create_success, sfp, indent=4, sort_keys=True)
        with open('vds_create_failure.json', 'w') as ffp:
            json.dump(vds_create_failure, ffp, indent=4, sort_keys=True)
    else:
        print "ERROR : No views retrieved from %s" % config_dict['hivedb_name']


if __name__ == "__main__":
    main()
