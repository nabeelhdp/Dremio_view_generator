
#!/usr/bin/python

import base64
import sys
import ConfigParser
from ConfigParser import SafeConfigParser

def decode(key, enc):
    dec = []
    enc = base64.urlsafe_b64decode(enc)
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)


def get_config_params(config_file):
    """Read configuration file and save parameters to dictionary

            Args:
                config_file (str): Path of config filename

            Returns:
                config_dict (dict): Dictionary object with
                    all configuration parameters
        """

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
