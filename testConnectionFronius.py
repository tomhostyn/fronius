import unittest
from fronius import FroniusInverter
from fronius import FroniusArchiveJson
from fronius import FroniusJson
from fronius import FroniusRealTimeJson
import pandas
import os
import datetime
import pytz


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

class FroniusInverter_RT_positive(unittest.TestCase):
    def test_ctor(self):
        fi=FroniusInverter(inverter_ip)

    def test_class_get_channels(self):
        self.assertEqual(len(FroniusInverter.get_all_channels()), 24)

    def test_version_compatibility(self):
        fi = FroniusInverter(inverter_ip)
        compatible, response = fi.check_server_compatibility()
        self.assertTrue(compatible)
        self.assertEqual(type(response), dict)

    def test_realtime_data(self):
        fi = FroniusInverter(inverter_ip)
        rtd = fi.get_inverter_realtime_data()
        self.assertEqual(type(rtd), dict)

    def test_getRealtimeData(self):
        fi = FroniusInverter(inverter_ip)
        json = fi.get_inverter_realtime_data()
        frt = FroniusRealTimeJson(json)
        rtd = frt.data()
        self.assertEqual(type(rtd), pandas.core.frame.DataFrame)
        self.assertEqual(len(rtd), 1)

    def test_FroniusInverter_RT_rename_timestamp(self):
        fi = FroniusInverter(inverter_ip)
        json = fi.get_inverter_realtime_data()
        frt = FroniusRealTimeJson(json)
        label = "foo"
        rtd = frt.data(label)
        self.assertEqual(type(rtd), pandas.core.frame.DataFrame)
        self.assertEqual(len(rtd), 1)
        self.assertTrue(label in list(rtd))

    def test_FroniusInverter_RT_test_append(self):
        fi = FroniusInverter(inverter_ip)
        json = fi.get_inverter_realtime_data()
        frt = FroniusRealTimeJson(json)
        rtd = frt.data()
        # get 2nd observation.  occasionally the inverter will return the same value.
        # iterate until a new observation is returned.  iterate at most 10 times

        rtd2 = rtd
        retry = 10
        while (rtd.iloc[0,:]['ts'] == rtd2.iloc[0,:]['ts']) and (0 < retry):
            json = fi.get_inverter_realtime_data()
            frt = FroniusRealTimeJson(json)
            rtd2 = frt.data('ts', append=rtd)
            retry -= 1

        self.assertEqual(type(rtd2), pandas.core.frame.DataFrame)
        self.assertEqual(len(rtd2), 2)
        self.assertEqual(len(list(set(rtd2['ts']))), 2)

tz = pytz.timezone("Europe/Paris")
date_with_data = datetime.datetime(year=2017, month=11, day=1) + datetime.timedelta(seconds=1)
date_with_data = tz.localize(date_with_data, is_dst=None)

class FroniusInverter_Historical_positive(unittest.TestCase):

    def check_devices(self, response, substrings):
        found_all = True
        for substring in substrings:
            found = False
            for k in response.keys():
                found = found or (substring in k)
            found_all = found_all and found
        self.assertTrue(found_all)

    def check_channels(self, response, channels):
        found_all = True
        for substring in channels:
            found = False
            for v in response.values():
                for k in list(v.columns):
                    found = found or (substring in k)
            found_all = found_all and found
        self.assertTrue(found_all)

    def check_date(self, response, from_date, to_date, ts_label):
        all_series = []
        for v in response.values():
            all_series += [v[ts_label]]

        all_dates = pandas.concat(all_series)
        earliest = min(all_dates)
        latest = max(all_dates)
        print (earliest, latest)
        print (from_date, to_date)
        self.assertTrue(from_date <= earliest)
        self.assertTrue(latest <= to_date)


    def test_FroniusInverter_Historical_test_12_hour_range(self):
        get_channels = ["Digital_PowerManagementRelay_Out_1", "Current_AC_Phase_1"]
        fi = FroniusInverter(inverter_ip)
        from_date = date_with_data
        to_date = date_with_data + datetime.timedelta(hours=12)
        data_1_day = fi.get_historical_data(from_date, to_date, get_channels)
        # should have datamager and inverter data
        self.assertEqual(len(data_1_day.keys()), 2)
        self.check_devices(data_1_day, ["datamanager:/", "inverter/"])
        self.check_channels(data_1_day, get_channels)
        self.check_date(data_1_day, from_date, to_date, "ts")

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
