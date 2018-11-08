
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
from dremiospaceutils import recreate_space
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


def fix_keywords(words, reserved_words):
    """ Dremio errors out in the following conditions :
    1) A column name overlaps with a reserved keywords
    2) A column name or table name starts with a digit
    3) An unknown UDF is invoked.
    4) String datatype is encountered
    5) Reserved keyword "AT" is in query as an alias etc
    For cases 1 and 2, we check the word and put it
    in quotes in this function to avoid a query failure in Dremio.
    Case 3 is handled elsewhere
    For case 4, Dremio does not have String type, so we replace
    it with varchar
    For case 5, we append _1 to at as column alias
    """

    # Avoid getting into loop for standalone words
    if (words.count('.') == 0):
        if words.lower() == "string":
            return "varchar"
        if words.isdigit():
            return words
        if words.lower() == "at":
            return "at_1"
        if words.upper() in reserved_words:
            return re.sub(r'\b%s\b' % words, r'"%s"' % words, words)
        return words

    # Loop for words separated by a dot
    curr_word = words.split('.')
    replace_pattern = ""

    for i in range(words.count('.') + 1):
        if curr_word[i].isdigit():
            replace_pattern += curr_word[i] + "."
            continue
        if curr_word[i].lower() == "at":
            replace_pattern += "at_1."
            continue
        # For column names beginning with numbers followed by any other
        # characters, wrap column name in quotes
        if curr_word[i].upper() in reserved_words or (
                curr_word[i][0].isdigit()):
            replace_pattern += re.sub(r'\b%s\b' %
                                      curr_word[i], r'"%s"' %
                                      curr_word[i], curr_word[i]) + "."
        else:
            replace_pattern += curr_word[i] + "."
    return replace_pattern[:-1]

   # Prepare json object for VDS request

def strip_statement(sql_statement):
    """ The SQL statement read from the Hive Metastore is subjected to
    the following alterations :
    1) The following special characters are replaced with a whitespace :
    \n  \t  \\
    2) The  following characters have whitespaces inserted around them to
    separate the words around them:
    ,  =  (  )
    """
    return str(sql_statement).strip().split(",",1)[1][3:-2].\
                                            replace("\\n"," ").\
                                            replace("\\t"," ").\
                                            replace("\\","").\
                                            replace(","," , ").\
                                            replace("="," = ").\
                                            replace(")"," ) ").\
                                            replace("("," ( ")


def prepare_vds_request(views, config_dict, dremio_auth_headers):
    """ The SQL statement read from the Hive Metastore is subjected to
    the following alterations :
    1) The UDF in Hive called DataMask is replaced with a local implementation
    2) Refer help for fix_keywords() for other changes
    """
    success_requests = {}
    fail_requests = {}
    status_type = {}
    dremio_response_time = {}
    query_error = {}
    reserved_words = config_dict["reserved_keywords"].replace("\"", "").split()
    recreate_space(config_dict['dremio_space'], config_dict['dremio_catalog_url'], dremio_auth_headers)
    for view in views:
        view_name = str(view).split(",", 1)[0][3:-1]
        statement = strip_statement(view)
        dremio_statement = ""
        words_list = iter(statement.split())
        for words in words_list:
            if (words.find("DataMask") != -1):
                # Assuming last word is DataMask
                # Checking rest for reserved keywords
                for i in range(0, words.count('.') - 1):
                    dremio_statement += fix_keywords(
                        words.split('.')[i], reserved_words) + "."
                # DataMask should be followed by pattern ( <args> )
                # skip the opening and closing parantheses using next()
                words = words_list.next()
                words = words_list.next()
                dremio_statement += "REGEXP_REPLACE(%s,'[a-zA-Z0-9]','X') " % (
                    fix_keywords(words, reserved_words))
                word = words_list.next()
            else:
                dremio_statement += fix_keywords(words, reserved_words) + " "
        vds_request_json = create_vds_request(
            view_name, config_dict, dremio_statement)

        # Execute the create view request on Dremio and capture response time
        s_time = time.time()
        vdsname, status, output = execute_vds_create(
            config_dict, vds_request_json, dremio_auth_headers)
        e_time = time.time()
        dremio_response_time['Total'] = dremio_response_time.get(
                                                'Total',0) + (e_time - s_time)
        dremio_response_time['Count'] = dremio_response_time.get('Count',0) + 1

        if dremio_response_time['Count'] % 100 == 0:
           print("%s INFO: %d VDS create requests sent to Dremio" %
                 (datetime.datetime.now(),dremio_response_time['Count']))

        if status == "Success":
            success_requests[vdsname] = output
        else:
            fail_requests[vdsname] = output
            #print(" Request: " + vds_request_json)
            #print("\n View: " + str(view))
            #print("\n Failed Query : " + dremio_statement)
        status_type[status] = status_type.get(status, 0) + 1

        if (status != "Success"):
            try:
                moreinfo =  output['moreInfo']
                moreinfo =  re.sub(r"\$\d+","",moreinfo)
                query_error[moreinfo] = query_error.get(moreinfo, 0) + 1
                fail_requests[vdsname]['failedQuery'] = dremio_statement
            except KeyError as e:
                query_error[output['errorMessage']] = query_error.get(
                    output['errorMessage'], 0) + 1

    print_summary(dremio_response_time,status_type,query_error)

    return success_requests, fail_requests

def print_summary(dremio_response_time,status_type,query_error):
    # Print Dremio response time summary
    print "\n=== Dremio Response time summary ==="
    print "Total queries sent to Dremio server: %d" % int(
               dremio_response_time['Count'])
    print "Aggregate response time from Dremio Server : %.6f" % float(
               dremio_response_time['Total'])
    print "Average response time from Dremio Server : %.6f" % (
               float(dremio_response_time['Total']) / int(
               dremio_response_time['Count']))
    # Print summary of success and failure
    print "\n=== Dremio Result summary ==="
    for types in sorted(status_type.iterkeys(), reverse=True):
        print types + " : " + str(status_type[types])

    # Print summarized error types for failures
    for items in sorted(query_error.iterkeys()):
        print str(query_error[items]) + " " + str(items)


def create_vds_request(view_name, config_dict, statement):
    """Returns a JSON object in the format required by Dremio to create a virtual dataset
    """
    vds_dict = {}
    vds_dict["entityType"] = "dataset"
    vds_dict["type"] = "VIRTUAL_DATASET"
    vds_dict["path"] = [config_dict['dremio_space'], view_name]
    vds_dict["sqlContext"] = [config_dict['dremio_source']]
    vds_dict["sql"] = statement
    vds_request_json = json.dumps(vds_dict, indent=4, sort_keys=True)
    return vds_request_json


def execute_vds_create(config_dict, vds_request, dremio_auth_headers):
    """Connects to Dremio, passes the JSON object for virtual dataset creation
       Returns View name as String
               Status as a String ( "Success"/"Failure <FailureType>" )
               Response output as JSON
    """
    req = Request(
        config_dict['dremio_catalog_url'],
        data=vds_request,
        headers=dremio_auth_headers)
    try:
        response = urlopen(req, timeout=30)
        vds_output = json.loads(response.read())
        return json.loads(vds_request)["path"][1], "Success", vds_output['id']
    except (URLError, HTTPError) as e:
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
