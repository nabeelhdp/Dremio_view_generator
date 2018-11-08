#!/usr/bin/python

import base64
import sys
import getpass
import ConfigParser
from ConfigParser import SafeConfigParser
import os


def encode(key, clear):
    enc = []
    for i in range(len(clear)):
        key_c = key[i % len(key)]
        enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    return base64.urlsafe_b64encode("".join(enc))


def set_config_params(configfile, section, password):
    try:
        with open(configfile) as f:
            try:
                parser = SafeConfigParser()
                parser.readfp(f)
            except ConfigParser.Error as err:
                print 'Could not parse: %s Exiting', err
                sys.exit(1)
    except IOError as e:
        print "Unable to access %s. Error %s \nExiting" % (configfile, e)
        sys.exit(1)

    # Prepare dictionary object with config variables populated
    parser.set(section, 'pass', password)
    with open(configfile, 'w') as writefile:
        parser.write(writefile)


def main():

    configfile = os.path.join(os.path.dirname(__file__), "dremio_config.ini")
    # If config file explicitly passed, use it. Else fall back to
    # dremio_config.ini as default filename
    if(sys.argv[1].lower() == 'mysql'):
        password = encode("NOTAVERYSAFEKEY", getpass.getpass())
        set_config_params(configfile, "metastore_config", password)
    if(sys.argv[1].lower() == 'dremio'):
        password = encode("NOTAVERYSAFEKEY", getpass.getpass())
        set_config_params(configfile, "dremio_config", password)
    if(sys.argv[1].lower() != 'mysql' and sys.argv[1].lower() != 'dremio'):
        print 'Syntax error. Run script with either dremio or mysql as argument'
        sys.exit(1)


if __name__ == "__main__":
    main()
