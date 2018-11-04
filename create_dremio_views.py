
#!/usr/bin/python

import subprocess
import base64
import sys
import socket
import re
import os
import json
import time
import string
import urllib2
import httplib
import urlparse
from urlparse import urlparse
from urllib2 import URLError
import ConfigParser
from ConfigParser import SafeConfigParser
import mysql.connector
from mysql.connector import Error


def decode(key, enc):
    dec = []
    enc = base64.urlsafe_b64decode(enc)
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)


def get_config_params(config_file):
    try:
        with open(config_file) as f:
            try:
                parser = SafeConfigParser()
                parser.readfp(f)
            except ConfigParser.Error as err:
                print 'Could not parse: %s Exiting', err
                sys.exit(1)
    except IOError as e:
        print "Unable to access %s. Error %s \nExiting" % (config_file, e)
        sys.exit(1)

    # Prepare dictionary object with config variables populated
    config_dict = {}
    config_dict["dremio_auth_url"] = "http://%s:%d/apiv2/login" % (
        parser.get('dremio_config', 'host'), int(parser.get('dremio_config', 'port')))
    config_dict["dremio_catalog_url"] = "http://%s:%d/api/v3/catalog" % (
        parser.get('dremio_config', 'host'), int(parser.get('dremio_config', 'port')))
    config_dict["dremio_user"] = parser.get('dremio_config', 'user')
    config_dict["dremio_source"] = parser.get('dremio_config', 'source')
    config_dict["reserved_keywords"] = parser.get('dremio_config', 'keywords')
    config_dict["metastore_host"] = parser.get('metastore_config', 'host')
    config_dict["metastore_db"] = parser.get('metastore_config', 'mysqldb')
    config_dict["metastore_user"] = parser.get('metastore_config', 'user')
    config_dict["hivedb_name"] = parser.get('metastore_config', 'hivedb')
    config_dict["dremio_space"] = parser.get('metastore_config', 'hivedb')

    config_dict["dremio_pass"] = decode(
        "NOTAVERYSAFEKEY", parser.get(
            'dremio_config', 'pass'))
    config_dict["metastore_pass"] = decode(
        "NOTAVERYSAFEKEY", parser.get(
            'metastore_config', 'pass'))

    return config_dict


def test_socket(socket_host, socket_port, service_name):
    # Test socket connectivity to requested service port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((socket_host, int(socket_port)))
    except Exception as e:
        print(
            "Unable to connect to %s host %s:%d. Exception is %s\nExiting!" %
            (service_name, socket_host, int(socket_port), e))
        sys.exit(1)
    finally:
        s.close()

# Retrieve token from Dremio host


def retrieve_dremio_token(config_dict):
   # Test socket connectivity to Dremio server port
    url_dict = urlparse(config_dict['dremio_auth_url'])
    test_socket(url_dict.hostname, url_dict.port, "Dremio server")

    # Construct URL request for authentication
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'}
    loginData = {
        'userName': config_dict['dremio_user'],
        'password': config_dict['dremio_pass']}
    req = urllib2.Request(
        config_dict['dremio_auth_url'],
        data=json.dumps(loginData),
        headers=headers)
    try:
        response = urllib2.urlopen(req, timeout=30)
        data = json.loads(response.read())
        # retrieve the login token
        token = data['token']
        return {
            'content-type': 'application/json',
            'accept': 'application/json',
            'authorization': '_dremio{authToken}'.format(
                authToken=token)}
    except (urllib2.URLError, urllib2.HTTPError) as e:
        print 'Dremio authentication failed with error:', e.read()


def retrieve_views(config_dict):
    sql_stmt = "select tbl_name,view_original_text " +\
               "from TBLS " +\
               "where db_id = (select db_id from DBS where name = '%s') " \
                              % config_dict["hivedb_name"] +\
               "and tbl_type = 'VIRTUAL_VIEW'"
                   
    # Connect to MySQL database
    try:
        conn = mysql.connector.connect(
            host=config_dict['metastore_host'],
            database=config_dict['metastore_db'],
            user=config_dict['metastore_user'],
            password=config_dict['metastore_pass'])
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(sql_stmt)
            rows = cursor.fetchall()
            print('Total Row(s):', cursor.rowcount)
            cursor.close()
            conn.close()
            return rows
    except Error as e:
        print(e)
        return []


def check_word(words, reserved_words):
""" Dremio freaks out in the following conditions :
  1) A column name overlaps with a reserved keywords
  2) A column name or table name starts with a digit
  3) An unknown UDF is invoked.
 For all three cases, we check the word and put it
 in quotes to avoid a query failure in Dremio.
"""
    replace_pattern = ""
    curr_word = words.split('.')
    for i in range(words.count('.') + 1):
        if (curr_word[i].upper() in reserved_words) or (
                curr_word[i][0].isdigit()):
            if (words.count('.') == 0):
                replace_pattern += re.sub(r'\b%s\b' %
                                          curr_word[i], r'"%s"' %
                                          curr_word[i], curr_word[i]) + " "
            else:
                replace_pattern += re.sub(r'\b%s\b' %
                                          curr_word[i], r'"%s"' %
                                          curr_word[i], curr_word[i]) + "."
        else:
            if (words.count('.') == 0):
                replace_pattern += curr_word[i] + " "
            else:
                replace_pattern += curr_word[i] + "."
    return replace_pattern[:-1]

   # Prepare json object for VDS request


def prepare_vds_request(views, config_dict, dremio_auth_headers):
    success_requests = {}
    failed_requests = {}
    status_type = {}
    query_error = {}
    reserved_words = config_dict["reserved_keywords"].replace("\"", "").split()
    print reserved_words
    drop_and_create_space(config_dict, dremio_auth_headers)
    for view in views:
        view_name = str(view).split(",", 1)[0][3:-1]
        statement = str(view).strip().split(",",1)[1][3:-2].\
                                            replace("\\n"," ").\
...                                         replace("\\","").\
...                                         replace(","," , ").\
...                                         replace(")"," ) ").\
...                                         replace("("," ( ")
        dremio_statement = ""
        words_list = iter(statement.split())
        for words in words_list:
            if (words.find("DataMask") != -1):
                # Assuming last word is DataMask. Checking rest for  reserved
                # keywords
                for i in range(0, words.count('.') - 1):
                    dremio_statement += check_word(
                        words.split('.')[i], reserved_words) + "."
                # next should be (
                words = words_list.next()
                # next should be arguments
                words = words_list.next()
                dremio_statement += "REGEXP_REPLACE(%s,'[a-zA-Z0-9]','X') " % check_word(
                    words, reserved_words)
                word = words_list.next()
            else:
                dremio_statement += check_word(words, reserved_words) + " "
        vds_request_json = create_vds_request(
            view_name, config_dict, dremio_statement)
        # Execute the create view request on Dremio
        vdsname, status, output = execute_vds_create(
            config_dict, vds_request_json, dremio_auth_headers)
        if (status == "Success"):
            success_requests[vdsname] = output
        else:
            failed_requests[vdsname] = output
            #print(" Request: " + vds_request_json)
            #print("View: " + str(view))
            print("\n Failed Query : " + dremio_statement)
        status_type[status] = status_type.get(status, 0) + 1
        if (status != "Success"):
            query_error[output['errorMessage']] = query_error.get(
                output['errorMessage'], 0) + 1

    # Print summary of success and failure
    for types in sorted(status_type.iterkeys(), reverse=True):
        print types + " : " + str(status_type[types])

    # Print summarized error types for failures
    for items in sorted(query_error.iterkeys()):
        print "      " + str(query_error[items]) + " " + str(items)

    return success_requests, failed_requests


def create_vds_request(view_name, config_dict, statement):
    vds_dict = {}
    vds_dict["entityType"] = "dataset"
    vds_dict["type"] = "VIRTUAL_DATASET"
    vds_dict["path"] = [config_dict['dremio_space'], view_name]
    vds_dict["sqlContext"] = [config_dict['dremio_source']]
    vds_dict["sql"] = statement
    vds_request_json = json.dumps(vds_dict, indent=4, sort_keys=True)
    return vds_request_json


def execute_vds_create(config_dict, vds_request, dremio_auth_headers):
    req = urllib2.Request(
        config_dict['dremio_catalog_url'],
        data=vds_request,
        headers=dremio_auth_headers)
    try:
        response = urllib2.urlopen(req, timeout=30)
        vds_output = json.loads(response.read())
        return json.loads(vds_request)["path"][1], "Success", vds_output['id']
    except (urllib2.URLError, urllib2.HTTPError) as e:
        error_msg = json.loads(e.read())
        return json.loads(vds_request)[
            "path"][1], "Failure URLError/HTTPError", error_msg
    except httplib.HTTPException as e:
        error_msg = json.loads(e.read())
        return json.loads(vds_request)[
            "path"][1], "Failure HTTPException", error_msg
    except TypeError as e:
        error_msg = json.loads(e.read())
        return json.loads(vds_request)[
            "path"][1], "Failure TypeError", error_msg
    except socket.timeout as e:
        error_msg = json.loads('{"Error":"' + str(e) + '"}')
        return json.loads(vds_request)[
            "path"][1], "Failure Socket timeout", error_msg
    except Exception as e:
        error_msg = {'errorMessage': str(type(e))}
        return json.loads(vds_request)["path"][1], "Failure Unknown", error_msg


def drop_and_create_space(config_dict, dremio_auth_headers):

    space_id = get_space_id(config_dict, dremio_auth_headers)
    if (space_id):
        drop_status = drop_space(space_id, config_dict, dremio_auth_headers)
    create_status = create_space(config_dict, dremio_auth_headers)
    return True


def drop_space(space_id, config_dict, dremio_auth_headers):
    del_req = urllib2.Request(
        config_dict['dremio_catalog_url'] + "/" + space_id,
        headers=dremio_auth_headers)
    del_req.get_method = lambda: 'DELETE'
    try:
        response = urllib2.urlopen(del_req, timeout=30)
        print "Space %s : ID: %s Deleted" % (config_dict['dremio_space'], space_id)
        return True
    except (urllib2.URLError, urllib2.HTTPError) as e:
        print e.read()
    except httplib.HTTPException as e:
        print e.errorMessage()
    except Exception as e:
        print e


def create_space(config_dict, dremio_auth_headers):
    space_dict = {}
    space_dict["entityType"] = "space"
    space_dict["type"] = "space"
    space_dict["path"] = [config_dict['dremio_space']]
    space_dict["name"] = config_dict['dremio_space']
    create_req = urllib2.Request(
        config_dict['dremio_catalog_url'],
        data=json.dumps(space_dict),
        headers=dremio_auth_headers)
    try:
        response = urllib2.urlopen(create_req, timeout=30)
        print "Space %s created" % space_dict["name"]
        return True
    except (urllib2.URLError, urllib2.HTTPError) as e:
        print e.read()
    except httplib.HTTPException as e:
        print e.errorMessage()
    except Exception as e:
        print e


def get_space_id(config_dict, dremio_auth_headers):
    req = urllib2.Request(
        config_dict['dremio_catalog_url'],
        headers=dremio_auth_headers)
    try:
        response = urllib2.urlopen(req, timeout=30)
        data = json.loads(response.read())['data']
        for item in data:
            if(item['path'][0] == config_dict['dremio_space']):
                return item['id']
    except (urllib2.URLError, urllib2.HTTPError) as e:
        print e.read()
        return False
    except httplib.HTTPException as e:
        print e.errorMessage()
        return False
    except Exception as e:
        print e
        return False


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
        print "No views retrieved from %s" % config_dict['hivedb_name']


if __name__ == "__main__":
    main()
