import unittest
from fronius import FroniusInverter
from fronius import FroniusArchiveJson
import testFronius

#
# Connection tests configuration
#

inverter_ip = "192.168.1.154"

class MyTestCase(unittest.TestCase):
    def test_ctor(self):
        FroniusInverter(inverter_ip)


if __name__ == '__main__':
    unittest.main()
