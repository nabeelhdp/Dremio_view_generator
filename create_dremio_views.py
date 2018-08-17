#!/usr/bin/python

import subprocess
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

def get_config_params(config_file):
  try:
    with open(config_file) as f:
      try:
        parser = SafeConfigParser()
        parser.readfp(f)
      except ConfigParser.Error, err:
        print 'Could not parse: %s Exiting', err
        sys.exit(1)
  except IOError as e:
    print "Unable to access %s. Error %s \nExiting" % (config_file, e)
    sys.exit(1)

  # Prepare dictionary object with config variables populated
  config_dict = {}
  config_dict["dremio_auth_url"] = "http://%s:%d/apiv2/login" % (parser.get('dremio_config', 'host'), int(parser.get('dremio_config','port')))
  config_dict["dremio_catalog_url"] = "http://%s:%d/api/v3/catalog" % (parser.get('dremio_config', 'host'), int(parser.get('dremio_config','port')))
  config_dict["dremio_user"] = parser.get('dremio_config', 'user')
  config_dict["dremio_pass"] = parser.get('dremio_config', 'pass')
  config_dict["dremio_source"] = parser.get('dremio_config', 'source')
  config_dict["metastore_host"] = parser.get('metastore_config', 'host')
  config_dict["metastore_db"] = parser.get('metastore_config', 'mysqldb')
  config_dict["metastore_user"] = parser.get('metastore_config', 'user')
  config_dict["metastore_pass"] = parser.get('metastore_config', 'pass')
  config_dict["hivedb_name"] = parser.get('metastore_config', 'hivedb')
  config_dict["dremio_space"] = parser.get('metastore_config', 'hivedb')
  return config_dict

def test_socket(socket_host,socket_port,service_name):
  # Test socket connectivity to requested service port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((socket_host,int(socket_port)))
    except Exception as e:
      print("Unable to connect to %s host %s:%d. Exception is %s\nExiting!" % (service_name, socket_host,int(socket_port),e))
      sys.exit(1)
    finally:
      s.close()

# Retrieve token from Dremio host
def retrieve_dremio_token(config_dict):
   # Test socket connectivity to Dremio server port
    url_dict = urlparse(config_dict['dremio_auth_url'])
    test_socket(url_dict.hostname,url_dict.port,"Dremio server")

    # Construct URL request for authentication
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    loginData = {'userName': config_dict['dremio_user'], 'password': config_dict['dremio_pass']}
    #print loginData,config_dict['dremio_auth_url']
    req = urllib2.Request(config_dict['dremio_auth_url'],data=json.dumps(loginData),headers=headers)
    try:
          response = urllib2.urlopen(req,timeout=30)
          data = json.loads(response.read())
          # retrieve the login token
          token = data['token']
          # print token
          return {'content-type':'application/json', 'accept':'application/json','authorization':'_dremio{authToken}'.format(authToken=token)}
    except (urllib2.URLError, urllib2.HTTPError) as e:
      print 'Dremio authentication failed with error:', e.read()

def retrieve_views(config_dict):
  sql_stmt = "select tbl_name,view_original_text from TBLS where db_id = (select db_id from DBS where name = '%s') and tbl_type = 'VIRTUAL_VIEW'" % config_dict["hivedb_name"]
  # Connect to MySQL database
  try:
    conn = mysql.connector.connect(host=config_dict['metastore_host'], database=config_dict['metastore_db'],user=config_dict['metastore_user'],password=config_dict['metastore_pass'])
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

# Prepare json object for VDS request
def construct_vds_request(views,config_dict,dremio_auth_headers):
  vds_requests = {}
  status_type = {}
  query_error = {}
  drop_and_create_space(config_dict,dremio_auth_headers)
  for view in views:
    view_name = str(view).split(",",1)[0][3:-1]
    view_stmt = str(view).strip().split(",",1)[1][3:-2].replace("\\", "").replace(","," , ")
    statement = view_stmt.strip()
    stmt_tokens = view_stmt.split()
    for words in stmt_tokens:
      if (words.find(".") != -1 and words != stmt_tokens[-1]):
        temp_str = string.replace(str(words),".",".\"")
        temp_str2 = temp_str + "\""
        statement = string.replace(statement,words,temp_str2)
    if (stmt_tokens[-2] == "from" ):
      dremio_stmt = string.replace(statement,stmt_tokens[-1],"\"%s\"" % (string.replace(str(stmt_tokens[-1]),".","\".\"")))
      vds_dict = {}
      vds_dict["entityType"] = "dataset"
      vds_dict["type"] = "VIRTUAL_DATASET"
      vds_dict["path"] = [config_dict['dremio_space'],view_name]
      vds_dict["sqlContext"] = [config_dict['dremio_source']]
      vds_dict["sql"] = dremio_stmt
      vds_request_json=json.dumps(vds_dict, indent=4, sort_keys=True)
      vdsname,status,output = execute_vds_create(config_dict,vds_request_json,dremio_auth_headers)
      vds_requests[vdsname] = output
      status_type[status] = status_type.get(status,0) + 1
      if (status != "Success"):
        query_error[output['errorMessage']] = query_error.get(output['errorMessage'],0) + 1
  for types in sorted(status_type.iterkeys(),reverse=True):
    print types + " : " + str(status_type[types])
  for items in sorted(query_error.iterkeys()):
    print "      " + str(query_error[items]) + " " + str(items)
  return vds_requests


def execute_vds_create(config_dict,vds_request,dremio_auth_headers):
  #print dremio_auth_headers
  req = urllib2.Request(config_dict['dremio_catalog_url'],data=vds_request,headers=dremio_auth_headers)
  try:
    response = urllib2.urlopen(req,timeout=30)
    vds_output = json.loads(response.read())
#    print " id :%s" % vds_output['id']
    return json.loads(vds_request)["path"][1],"Success", vds_output['id']
  except (urllib2.URLError, urllib2.HTTPError) as e:
    error_msg = json.loads(e.read())
    #print error_msg['code'] + " " + json.dumps(error_msg['errorMessage'])
    #error_msg = json.loads(e.read())['details']['errors']
    return json.loads(vds_request)["path"][1],"Failure URLError/HTTPError", error_msg
  except httplib.HTTPException, e:
    error_msg = json.loads(e.read())
#    print  "HTTPException",e.errorMessage()
    return json.loads(vds_request)["path"][1],"Failure HTTPException", error_msg
  except TypeError, e:
#    print "TypeError",e.message
    error_msg = json.loads(e.read())
    return json.loads(vds_request)["path"][1],"Failure TypeError", error_msg
  except socket.timeout, e:
#    print "Socket Timeout"
    error_msg = {'errorMessage':'Socket Timed Out'}
    return json.loads(vds_request)["path"][1],"Failure Socket timeout", error_msg
  except Exception, e:
#    print "Unknown exception ", dir(e)
    error_msg = {'errorMessage': str(type(e)) }
    return json.loads(vds_request)["path"][1],"Failure Unknown", error_msg

def drop_and_create_space(config_dict,dremio_auth_headers):

  space_id = get_space_id(config_dict,dremio_auth_headers)
  if (space_id) :
    drop_status = drop_space(space_id,config_dict,dremio_auth_headers)
  create_status = create_space(config_dict,dremio_auth_headers)
  return True

def drop_space(space_id,config_dict,dremio_auth_headers):
  del_req = urllib2.Request(config_dict['dremio_catalog_url'] + "/" + space_id,headers=dremio_auth_headers)
  del_req.get_method = lambda: 'DELETE'
  try:
    response = urllib2.urlopen(del_req,timeout=30)
    print "Space %s : ID: %s Deleted" % ( config_dict['dremio_space'],space_id)
    return True
  except (urllib2.URLError, urllib2.HTTPError) as e:
    print e.read()
  except httplib.HTTPException, e:
    print e.errorMessage()
  except Exception, e:
    print e

def create_space(config_dict,dremio_auth_headers):
  space_dict = {}
  space_dict["entityType"] = "space"
  space_dict["type"] = "space"
  space_dict["path"] = [config_dict['dremio_space']]
  space_dict["name"] = config_dict['dremio_space']
  create_req = urllib2.Request(config_dict['dremio_catalog_url'],data=json.dumps(space_dict),headers=dremio_auth_headers)
  try:
    response = urllib2.urlopen(create_req,timeout=30)
    print "Space %s created" % space_dict["name"]
    return True
  except (urllib2.URLError, urllib2.HTTPError) as e:
    print e.read()
  except httplib.HTTPException, e:
    print e.errorMessage()
  except Exception, e:
    print e


def get_space_id(config_dict,dremio_auth_headers):
  req = urllib2.Request(config_dict['dremio_catalog_url'],headers=dremio_auth_headers)
  try:
    response = urllib2.urlopen(req,timeout=30)
    data = json.loads(response.read())['data']
    for item in data:
      if(item['path'][0] == config_dict['dremio_space']):
        return item['id']
  except (urllib2.URLError, urllib2.HTTPError) as e:
    print e.read()
    return False
  except httplib.HTTPException, e:
    print e.errorMessage()
    return False
  except Exception, e:
    print e
    return False

def main():

  # If config file explicitly passed, use it. Else fall back to dremio_config.ini as default filename
  config_file = sys.argv[1] if len(sys.argv) >= 2 else os.path.join(os.path.dirname(__file__),"dremio_config.ini")

  config_dict = {}
  config_dict = get_config_params(config_file)

  hive_views = []
  hive_views = retrieve_views(config_dict)

  dremio_auth_headers = retrieve_dremio_token(config_dict)

  vds_create_status = {}
  vds_create_status = construct_vds_request(hive_views,config_dict,dremio_auth_headers)
  with open('vds_create_status.json','w') as fp:
    json.dump(vds_create_status,fp, indent=4, sort_keys=True)

if __name__ == "__main__":
  main()
 
