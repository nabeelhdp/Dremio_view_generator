
#!/usr/bin/python

import json
import datetime
import httplib
from urllib2 import URLError,Request,urlopen,HTTPError


def recreate_space(space_name, dremio_catalog_url, dremio_auth_headers):
    """Drops and recreates a specified Space from Dremio

        Args:
            space_name (str): Name of space to be deleted in Dremio
            dremio_catalog_url (str): Dremio Catalogue URL REST endpoint
            dremio_auth_headers (dict): JSON object containing
                HTTP headers with authorization token
       Returns:
            bool: Dremio Response status. True for success, False otherwise.
    """
    space_id = get_space_id(space_name, dremio_catalog_url, dremio_auth_headers)
    drop_status = True
    if space_id != 'False':
        drop_status = drop_space(space_name, space_id, dremio_catalog_url, dremio_auth_headers)
    if space_id and drop_status:
        create_status = create_space(space_name, dremio_catalog_url, dremio_auth_headers)
    if create_status:
        return True
    else:
        return False


def drop_space(space_name, space_id, dremio_catalog_url, dremio_auth_headers):
    """Drops a specified Space from Dremio.

        Args:
            space_name (str): Name of space to be deleted in Dremio
            space_id (str): ID of the space to be deleted in Dremio
            dremio_catalog_url (str): Dremio Catalogue URL REST endpoint
            dremio_auth_headers (dict): JSON object containing
                HTTP headers with authorization token
        Returns:
            bool: Dremio Response status. True for success, False otherwise.
    """
    del_req = Request(
        dremio_catalog_url + "/" + space_id,
        headers=dremio_auth_headers)
    del_req.get_method = lambda: 'DELETE'
    try:
        response = urlopen(del_req, timeout=30)
        if response.code == 204:
            print "%s INFO: Deleted %s : " \
                  "ID: %s" % (
                datetime.datetime.now(),space_name, space_id)
            return True
        if response.code == 403:
            print "%s ERROR: Failed to delete %s : " \
                  "Permission Denied" % (
                datetime.datetime.now(),space_name)
            return False
        if response.code == 404:
            print "%s ERROR: Failed to delete %s : " \
                  "Catalog Entity with specified ID not found" % (
                datetime.datetime.now(), space_name)
            return False
    except (URLError, HTTPError) as e:
        print e.read()
        return False
    except httplib.HTTPException as e:
        print e
        return False
    except Exception as e:
        print e
        return False


def create_space(space_name, dremio_catalog_url, dremio_auth_headers):
    """Creates a specified Space from Dremio.

        Args:
            space_name (str): Name of space to be deleted in Dremio
            dremio_catalog_url (str): Dremio Catalogue URL REST endpoint
            dremio_auth_headers (dict): JSON object containing
                HTTP headers with authorization token

        Returns:
            bool: Dremio Response status. True for success, False otherwise.
    """
    space_dict = {}
    space_dict["entityType"] = "space"
    space_dict["type"] = "space"
    space_dict["path"] = space_name
    space_dict["name"] = space_name
    create_req = Request(
        dremio_catalog_url,
        data=json.dumps(space_dict),
        headers=dremio_auth_headers)
    try:
        response = urlopen(create_req, timeout=30)
        print "%s INFO: Created %s " % (
            datetime.datetime.now(),space_dict["name"])
        return True
    except (URLError, HTTPError) as e:
        print e.read()
    except httplib.HTTPException as e:
        print e
    except Exception as e:
        print e


def get_space_id(space_name, dremio_catalog_url, dremio_auth_headers):
    req = Request(
        dremio_catalog_url,
        headers=dremio_auth_headers)
    try:
        response = urlopen(req, timeout=30)
        data = json.loads(response.read())['data']
        for item in data:
            if item['path'][0] == space_name:
                return item['id']
    except (URLError, HTTPError) as e:
        print e.read()
        return 'False'
    except httplib.HTTPException as e:
        print e
        return 'False'
    except Exception as e:
        print e
        return 'False'