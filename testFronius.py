import unittest
import fronius
from fronius import FroniusInverter
from fronius import FroniusArchiveJson
import dateutil



#
# Tests for FroniusArchiveJson class
#
error_json = {'Body': {'Data': {}},
 'Head': {'RequestArguments': {'Channel': 'Hybrid_Operating_State',
   'EndDate': '2017-10-29T23:59:59+01:00',
   'HumanReadable': 'True',
   'Scope': 'System',
   'SeriesType': 'Detail',
   'StartDate': '2017-10-01T00:00:00+02:00'},
  'Status': {'Code': 255,
   'ErrorDetail': {'Nodes': []},
   'Reason': 'Query interval is restricted to 16 days',
   'UserMessage': ''},
  'Timestamp': '2017-10-24T10:29:59+02:00'}}

realtime_json={'Body': {'Data': {'DAY_ENERGY': {'Unit': 'Wh', 'Values': {'1': 57}},
   'PAC': {'Unit': 'W', 'Values': {'1': 183}},
   'TOTAL_ENERGY': {'Unit': 'Wh', 'Values': {'1': 163543}},
   'YEAR_ENERGY': {'Unit': 'Wh', 'Values': {'1': 163542}}}},
 'Head': {'RequestArguments': {'DeviceClass': 'Inverter', 'Scope': 'System'},
  'Status': {'Code': 0, 'Reason': '', 'UserMessage': ''},
  'Timestamp': '2017-10-25T09:10:14+02:00'}}

regular_json = {'Body': {'Data': {'datamanager:/dc/f0056cc6/': {'Data': {'Digital_PowerManagementRelay_Out_1': {'Unit': '1',
      'Values': {'28469': 0},
      '_comment': 'channelId=123407124'}},
    'End': '2017-10-25T23:59:59+02:00',
    'Start': '2017-10-25T00:00:00+02:00'},
   'inverter/1': {'Data': {'TimeSpanInSec': {'Unit': 'sec',
      'Values': {'12900': 72,
       '1800': 53,
       '24000': 71,
       '28500': 82,
       '28800': 51,
       '29100': 279,
       '29400': 302,
       '29700': 297,
       '30000': 302,
       '30300': 297,
       '30600': 302,
       '30900': 297,
       '31200': 302,
       '31500': 297,
       '31800': 302,
       '32100': 302,
       '32400': 297,
       '32700': 302,
       '33000': 297,
       '33300': 302},
      '_comment': 'channelId=65549'}},
    'DeviceType': 77,
    'End': '2017-10-25T23:59:59+02:00',
    'NodeType': 97,
    'Start': '2017-10-25T00:00:00+02:00'}}},
 'Head': {'RequestArguments': {'Channel': 'TimeSpanInSec',
   'EndDate': '2017-10-25T23:59:59+02:00',
   'HumanReadable': 'True',
   'Scope': 'System',
   'SeriesType': 'Detail',
   'StartDate': '2017-10-25T00:00:00+02:00'},
  'Status': {'Code': 0,
   'ErrorDetail': {'Nodes': []},
   'Reason': '',
   'UserMessage': ''},
  'Timestamp': '2017-10-25T09:17:20+02:00'}}



class FroniusArchiveJsonTests(unittest.TestCase):
    def test_constructor_cannot_accept_string(self):
        with self.assertRaises(AssertionError):
            FroniusArchiveJson("not a json object")

    def test_constructor_cannot_accept_empty_dict(self):
        with self.assertRaises(AssertionError):
            FroniusArchiveJson(dict())

    def test_constructor_can_accept_error_response(self):
        FroniusArchiveJson(error_json)

    def test_constructor_can_accept_regular_response(self):
        FroniusArchiveJson(regular_json)

    def test_is_empty_with_empty(self):
        faj=FroniusArchiveJson(error_json)
        self.assertTrue(self, faj.is_empty())

    def test_is_not_empty_with_not_empty(self):
        faj = FroniusArchiveJson(regular_json)
        self.assertTrue(self, faj.is_empty())

    def test_device_ids_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        self.assertNotEqual(0, len(faj.device_ids()))

    def test_device_ids_with_error(self):
        faj = FroniusArchiveJson(error_json)
        self.assertEqual(0, len(faj.device_ids()))

    def test_channels_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        self.assertNotEqual(0, len(faj.channels()))

    def test_start_date_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        dt = dateutil.parser.parse('2017-10-25T00:00:00+02:00')
        self.assertEqual(dt, faj.start_date())

    def test_start_date_with_error(self):
        faj = FroniusArchiveJson(error_json)
        dt = dateutil.parser.parse('2017-10-01T00:00:00+02:00')
        self.assertEqual(dt, faj.start_date())

    def test_end_date_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        dt = dateutil.parser.parse('2017-10-25T23:59:59+02:00')
        self.assertEqual(dt, faj.end_date())

    def test_end_date_with_error(self):
        faj = FroniusArchiveJson(error_json)
        dt = dateutil.parser.parse('2017-10-29T23:59:59+01:00')
        self.assertEqual(dt, faj.end_date())

    def test_timestamp_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        dt = dateutil.parser.parse('2017-10-25T09:17:20+02:00')
        self.assertEqual(dt, faj.timestamp())

    def test_timestamp_with_error(self):
        faj = FroniusArchiveJson(error_json)
        dt = dateutil.parser.parse('2017-10-24T10:29:59+02:00')
        self.assertEqual(dt, faj.timestamp())

    def test_errorStatus_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        self.assertTrue(type(faj.error_status()), str)

    def test_errorStatus_with_error(self):
        faj = FroniusArchiveJson(error_json)
        self.assertTrue(type(faj.error_status()), str)

    def test_error_code_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        self.assertEqual(faj.error_code(), 0)

    def test_error_code_with_error(self):
        faj = FroniusArchiveJson(error_json)
        self.assertEqual(faj.error_code(), 255)

    def test_data_with_regular(self):
        faj = FroniusArchiveJson(regular_json)
        self.assertEqual(len(faj.data()), 2)
        self.assertEqual((faj.device_ids()), ['datamanager:/dc/f0056cc6/', 'inverter/1'])
        self.assertEqual((faj.channels('datamanager:/dc/f0056cc6/')), ['Digital_PowerManagementRelay_Out_1'])
        self.assertEqual((faj.channels('inverter/1')), ['TimeSpanInSec'])
        self.assertEqual(len(faj.data()['inverter/1']), 20)

    def test_data_with_error(self):
        faj = FroniusArchiveJson(error_json)
        self.assertEqual(faj.data(), {})

class FroniusInverternUnitTests(unittest.TestCase):
    def test_class_get_channels(self):
        self.assertEqual(len(FroniusInverter.get_all_channels()), 24)

class FroniusInverternConnectionTests(unittest.TestCase):
    def test_class_get_channels(self):
        self.assertEqual(len(FroniusInverter.get_all_channels()), 24)


if __name__ == '__main__':
    unittest.main()
