import unittest
from fronius import FroniusInverter
from fronius import FroniusArchiveJson
from fronius import FroniusJson
from fronius import FroniusRealTimeJson
import pandas
import os
import datetime
import pytz

inverter_ip = "192.168.1.154"

inverter_installation_time = datetime.datetime(2017, 10, 11, 13, 5)
inverter_installation_time = pytz.utc.localize(inverter_installation_time, is_dst=None)

#find_earliest_data_binary
#find_earliest_data_linear

class TestFindingEarliestData(unittest.TestCase):
    def test_find_earliest_linear_time(self):
        fi=FroniusInverter(inverter_ip)
        found = fi.find_earliest_data_linear()
        self.assertEqual(found - inverter_installation_time, datetime.timedelta(seconds=0))

    def test_find_earliest_log_n_time(self):
        fi=FroniusInverter(inverter_ip)
        found = fi.find_earliest_data_binary()
        self.assertEqual(found - inverter_installation_time, datetime.timedelta(seconds=0))

    def test_find_earliest(self):
        fi=FroniusInverter(inverter_ip)
        found = fi.find_earliest_data()
        self.assertEqual(found - inverter_installation_time, datetime.timedelta(seconds=0))

if __name__ == '__main__':
    unittest.main()
