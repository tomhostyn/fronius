import unittest
import json
import requests
from fronius import FroniusInverter
from fronius import FroniusArchiveJson
import testFronius
import os


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

    def test_class_get_channels(self):
        self.assertEqual(len(FroniusInverter.get_all_channels()), 24)

    def test_version_compatibility(self):
        fi = FroniusInverter(inverter_ip)
        compatible, response = fi.check_server_compatibility()
        self.assertTrue(compatible)
        self.assertTrue(type(response), dict)



skip_timeout_tests = os.getenv('SKIP_TIMEOUT_TESTS', False)

class FroniusInverter_timeout_tests(unittest.TestCase):
    def test_ctor_google(self):
        if skip_timeout_tests:
            self.skipTest('skipped test due to SKIP_TIMEOUT_TESTS')
        else:
            fi = FroniusInverter(google)
            with self.assertRaises(Exception):
                fi.check_server_compatibility()

    def test_ctor_gateway(self):
        if skip_timeout_tests:
            self.skipTest('skipped test due to SKIP_TIMEOUT_TESTS')
        else:
            fi = FroniusInverter(gateway_ip)
            with self.assertRaises(Exception):
                fi.check_server_compatibility()

    def test_ctor_timeout(self):
        if skip_timeout_tests:
            self.skipTest('skipped test due to SKIP_TIMEOUT_TESTS')
        else:
            fi=FroniusInverter(timeout_ip)
            with self.assertRaises(Exception):
                fi.check_server_compatibility()


if __name__ == '__main__':
    unittest.main()
