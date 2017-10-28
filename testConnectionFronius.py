import unittest
import json
import requests
from fronius import FroniusInverter
from fronius import FroniusArchiveJson
import testFronius

#
# Connection tests configuration
#

# IP address for working and connected Fronius inverter
inverter_ip = "192.168.1.154"

# ip adress that will connect to local device, but wont reply to the rest calls
gateway_ip = "192.168.1.1"

# ip address that will timeout
timeout_ip = "192.168.1.233"

# name rather than IP.  won't reply to rest calls
google = "www.google.com"

class FroniusInverter_positive(unittest.TestCase):
    def test_ctor(self):
        fi=FroniusInverter(inverter_ip)

class FroniusInverternUnitTests(unittest.TestCase):
    def test_class_get_channels(self):
        self.assertEqual(len(FroniusInverter.get_all_channels()), 24)

class FroniusInverter_slow(unittest.TestCase):
    def test_ctor_google(self):
        fi = FroniusInverter(google)
        with self.assertRaises(Exception):
            fi.check_server_compatibility()

    def test_ctor_gateway(self):
        fi = FroniusInverter(gateway_ip)
        with self.assertRaises(Exception):
            fi.check_server_compatibility()

    def test_ctor_timeout(self):
        fi=FroniusInverter(timeout_ip)
        with self.assertRaises(Exception):
            fi.check_server_compatibility()


if __name__ == '__main__':
    unittest.main()
