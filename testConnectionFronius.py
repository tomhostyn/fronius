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
        #print (earliest, latest)
        #print (from_date, to_date)
        self.assertTrue(from_date <= earliest)
        self.assertTrue(latest <= to_date)

    def test_FroniusInverter_Historical_test_12_hour_range(self):
        get_channels = ["Digital_PowerManagementRelay_Out_1", "Current_AC_Phase_1"]
        fi = FroniusInverter(inverter_ip)
        from_date = date_with_data
        to_date = date_with_data + datetime.timedelta(hours=12)
        data_1_day = fi.get_historical_data(from_date, to_date, get_channels)
        # should have datamager and inverter data
        self.assertTrue(data_1_day is not None)
        if data_1_day is not None:
            self.assertEqual(len(data_1_day.keys()), 2)
            self.check_devices(data_1_day, ["datamanager:/", "inverter/"])
            self.check_channels(data_1_day, get_channels)
            self.check_date(data_1_day, from_date, to_date, "ts")

    def test_FroniusInverter_Historical_test_only_datamanager_data(self):
        get_channels = ["Digital_PowerManagementRelay_Out_1"]
        fi = FroniusInverter(inverter_ip)
        from_date = date_with_data
        to_date = date_with_data + datetime.timedelta(hours=48)
        data_1_day = fi.get_historical_data(from_date, to_date, get_channels)
        # should have datamager and inverter data
        self.assertTrue(data_1_day is not None)
        if data_1_day is not None:
            self.assertEqual(len(data_1_day.keys()), 1)
            self.check_devices(data_1_day, ["datamanager:/"])
            self.check_channels(data_1_day, get_channels)
            self.check_date(data_1_day, from_date, to_date, "ts")

    def test_FroniusInverter_Historical_test_only_inverter_data(self):
        get_channels = ["Current_AC_Phase_1"]
        fi = FroniusInverter(inverter_ip)
        from_date = date_with_data
        to_date = date_with_data + datetime.timedelta(hours=12)
        data_1_day = fi.get_historical_data(from_date, to_date, get_channels)
        # should have datamager and inverter data
        self.assertTrue(data_1_day is not None)
        if data_1_day is not None:
            self.assertEqual(len(data_1_day.keys()), 1)
            self.check_devices(data_1_day, [ "inverter/"])
            self.check_channels(data_1_day, get_channels)
            self.check_date(data_1_day, from_date, to_date, "ts")

    def test_FroniusInverter_Historical_test_UTC_8_datetimes(self):
        """
            the fronius server does not respond well to UTC+8 for some reason
        """

        t1_utc_8 = datetime.datetime(year=2017, month=11, day=1, hour=4) - datetime.timedelta(hours=8)
        t1_utc_8 = pytz.timezone('Etc/GMT+8').localize(t1_utc_8, is_dst=None)

        f = t1_utc_8
        t = t1_utc_8 + datetime.timedelta(hours=16)
        fi = FroniusInverter(inverter_ip)
        get_channels = ["Digital_PowerManagementRelay_Out_1", "Current_AC_Phase_1"]

        data_liberal = fi.get_historical_data(f, t, get_channels, strict=False)
        self.assertTrue(data_liberal is not None)

        if data_liberal is not None:
            self.assertEqual(len(data_liberal.keys()), 2)
            self.check_devices(data_liberal, ["datamanager:/", "inverter/"])
            self.check_channels(data_liberal, get_channels)
            self.check_date(data_liberal, from_date, to_date, "ts")

    def test_FroniusInverter_Historical_test_strict_vs_liberal_datetimes(self):
        """
            test - liberal vs strict
            fetch 16 hours of data in liberal mode
            fetch  D   04:00 UTC   - D 20:00 UTC   - should yield data at most 24 hours of data

            fetch the same measurements using strict mode

            pick 1 measurement. fetch that single measurement using strict mode
        """

        channels = ["Current_AC_Phase_1"]

        t1_utc = datetime.datetime(year=2017, month=11, day=1, hour=4)
        t1_utc = pytz.timezone('UCT').localize(t1_utc, is_dst=None)

        fi = FroniusInverter(inverter_ip)
        key = 'inverter/1'
        ts = 'ts'

        f = t1_utc
        t = t1_utc + datetime.timedelta(hours=16)
        data_liberal = fi.get_historical_data(f, t, channels, strict=False)

        timestamps_liberal = data_liberal[key][ts]
        earliest_liberal_utc = min(timestamps_liberal)
        latest_liberal_utc = max(timestamps_liberal)
        count_liberal_utc = len(timestamps_liberal)

        data_strict = fi.get_historical_data(earliest_liberal_utc, latest_liberal_utc + datetime.timedelta(seconds=1), channels,
                                             strict=True)

        timestamps_strict = data_strict[key][ts]
        earliest_strict_utc = min(timestamps_strict)
        latest_strict_utc = max(timestamps_strict)
        count_strict_utc = len(timestamps_strict)

        self.assertEqual(latest_liberal_utc - earliest_liberal_utc, latest_strict_utc - earliest_strict_utc)
        self.assertEqual(count_liberal_utc, count_strict_utc)

        data_strict = fi.get_historical_data(earliest_liberal_utc, earliest_liberal_utc + datetime.timedelta(seconds=1),
                                             channels, strict=True)

        timestamps_strict = data_strict[key][ts]
        earliest_strict_utc = min(timestamps_strict)
        latest_strict_utc = max(timestamps_strict)
        count_strict_utc = len(timestamps_strict)

        self.assertEqual(datetime.timedelta(seconds=0), latest_strict_utc - earliest_strict_utc)
        self.assertEqual(1, count_strict_utc)


skip_inverter_quirk_tests = os.getenv('SKIP_INVERTER_QUIRK_TESTS', False)
#skip_inverter_quirk_tests = False

class FroniusInverter_Historical_JSON_Quirks(unittest.TestCase):

    """
    It seems that requesting some data in a day, the inverter will respond with ALL data in that day.
    day = local time midnight to midnight minus one second
    """

    def test_confirm_assumption_1_second(self):
        if skip_inverter_quirk_tests:
            self.skipTest('skipped test due to SKIP_INVERTER_QUIRK_TESTS')
        else:
            fi = FroniusInverter(inverter_ip)
            day = datetime.datetime(year=2017, month=11, day=4, hour=8, minute=0, second=0)
            data_1_day_J = fi.get_historical_data_json(day, day + datetime.timedelta(seconds=1),
                                                       ["Current_AC_Phase_1"])

            request_start = data_1_day_J['Head']['RequestArguments']['StartDate']
            expected_request_start = '2017-11-04T00:00:00+01:00'
            request_end = data_1_day_J['Head']['RequestArguments']['EndDate']
            expected_request_end = '2017-11-04T23:59:59+01:00'
            self.assertEqual(request_start, expected_request_start)
            self.assertEqual(request_end, expected_request_end)

    def test_confirm_assumption_1_minute(self):
        if skip_inverter_quirk_tests:
            self.skipTest('skipped test due to SKIP_INVERTER_QUIRK_TESTS')
        else:
            fi = FroniusInverter(inverter_ip)
            day = datetime.datetime(year=2017, month=11, day=4, hour=3, minute=0, second=0)
            data_1_day_J = fi.get_historical_data_json(day, day + datetime.timedelta(minutes=1), ["Current_AC_Phase_1"])

            request_start = data_1_day_J['Head']['RequestArguments']['StartDate']
            expected_request_start = '2017-11-04T00:00:00+01:00'
            request_end = data_1_day_J['Head']['RequestArguments']['EndDate']
            expected_request_end = '2017-11-04T23:59:59+01:00'
            self.assertEqual(request_start, expected_request_start)
            self.assertEqual(request_end, expected_request_end)

    def test_confirm_assumption_1_hour(self):
        if skip_inverter_quirk_tests:
            self.skipTest('skipped test due to SKIP_INVERTER_QUIRK_TESTS')
        else:
            fi = FroniusInverter(inverter_ip)
            day = datetime.datetime(year=2017, month=11, day=4, hour=2, minute=10, second=0)
            data_1_day_J = fi.get_historical_data_json(day, day + datetime.timedelta(hours=1), ["Current_AC_Phase_1"])

            request_start = data_1_day_J['Head']['RequestArguments']['StartDate']
            expected_request_start = '2017-11-04T00:00:00+01:00'
            request_end = data_1_day_J['Head']['RequestArguments']['EndDate']
            expected_request_end = '2017-11-04T23:59:59+01:00'
            self.assertEqual(request_start, expected_request_start)
            self.assertEqual(request_end, expected_request_end)

    def test_confirm_assumption_24_hours_minus_one_second(self):
        if skip_inverter_quirk_tests:
            self.skipTest('skipped test due to SKIP_INVERTER_QUIRK_TESTS')
        else:
            fi = FroniusInverter(inverter_ip)
            day = datetime.datetime(year=2017, month=11, day=4, hour=0, minute=0, second=0)
            data_1_day_J = fi.get_historical_data_json(day, day + datetime.timedelta(hours=24)
                                                       - datetime.timedelta(seconds=1), ["Current_AC_Phase_1"])

            request_start = data_1_day_J['Head']['RequestArguments']['StartDate']
            expected_request_start = '2017-11-04T00:00:00+01:00'
            request_end = data_1_day_J['Head']['RequestArguments']['EndDate']
            expected_request_end = '2017-11-04T23:59:59+01:00'
            self.assertEqual(request_start, expected_request_start)
            self.assertEqual(request_end, expected_request_end)

    def test_confirm_assumption_24_hours_minus_one_second(self):
        if skip_inverter_quirk_tests:
            self.skipTest('skipped test due to SKIP_INVERTER_QUIRK_TESTS')
        else:
            fi = FroniusInverter(inverter_ip)
            day = datetime.datetime(year=2017, month=11, day=4, hour=0, minute=0, second=0)
            data_1_day_J = fi.get_historical_data_json(day, day + datetime.timedelta(hours=24), ["Current_AC_Phase_1"])

            request_start = data_1_day_J['Head']['RequestArguments']['StartDate']
            expected_request_start = '2017-11-04T00:00:00+01:00'
            request_end = data_1_day_J['Head']['RequestArguments']['EndDate']
            expected_request_end = '2017-11-05T23:59:59+01:00'
            self.assertEqual(request_start, expected_request_start)
            self.assertEqual(request_end, expected_request_end)

    def test_confirm_assumption_8_hours_over_2_days(self):
        if skip_inverter_quirk_tests:
            self.skipTest('skipped test due to SKIP_INVERTER_QUIRK_TESTS')
        else:
            fi = FroniusInverter(inverter_ip)
            day = datetime.datetime(year=2017, month=11, day=4, hour=20, minute=0, second=0)
            data_1_day_J = fi.get_historical_data_json(day, day + datetime.timedelta(hours=8), ["Current_AC_Phase_1"])

            request_start = data_1_day_J['Head']['RequestArguments']['StartDate']
            expected_request_start = '2017-11-04T00:00:00+01:00'
            request_end = data_1_day_J['Head']['RequestArguments']['EndDate']
            expected_request_end = '2017-11-05T23:59:59+01:00'
            self.assertEqual(request_start, expected_request_start)
            self.assertEqual(request_end, expected_request_end)

    def test_confirm_assumption_Server_uses_UTC(self):
        """
        assume the server returns a full day of data on a given UTC date, and interprets the data as UTC

        try sending it UTC +1 data that covers 1 day in UTC and 2 days in UTC+1:
            06:00 - 23:59  UTC
            14:00 - 08:59  UTC +1
            confirm that only data of day 1 is returned
        """

        if skip_inverter_quirk_tests:
            self.skipTest('skipped test due to SKIP_INVERTER_QUIRK_TESTS')
        else:
            fi = FroniusInverter(inverter_ip)
            day = datetime.datetime(year=2017, month=11, day=4, hour=20, minute=0, second=0)
            data_1_day_J = fi.get_historical_data_json(day, day + datetime.timedelta(hours=8), ["Current_AC_Phase_1"])

            request_start = data_1_day_J['Head']['RequestArguments']['StartDate']
            expected_request_start = '2017-11-04T00:00:00+01:00'
            request_end = data_1_day_J['Head']['RequestArguments']['EndDate']
            expected_request_end = '2017-11-05T23:59:59+01:00'
            self.assertEqual(request_start, expected_request_start)
            self.assertEqual(request_end, expected_request_end)



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
