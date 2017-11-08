import requests
import warnings
import datetime
import dateutil
import pytz
import pandas as pd


class FroniusInverter:
    'class implementing Fronius Solar API v1'

    tested_server_versions = ["1.5-4"]
    api_version = 1
    debug = False
    timestamp_colname = "ts"

    # earliest possible data is set to the publishing date of Fronius Solar API V1 document
    epoch = pytz.utc.localize(datetime.datetime(2017, 6, 8), is_dst=None)

    channel_dict = {"TimeSpanInSec": "sec", "Digital_PowerManagementRelay_Out_1": "1",
                    "EnergyReal_WAC_Sum_Produced": "Wh", "Current_DC_String_1": "1A", "Current_DC_String_2": "1A",
                    "Voltage_DC_String_1": "1V", "Voltage_DC_String_2": "1V", "Temperature_Powerstage": "1C",
                    "Voltage_AC_Phase_1": "1V", "Voltage_AC_Phase_2": "1V", "Voltage_AC_Phase_3": "1V",
                    "Current_AC_Phase_1": "1A", "Current_AC_Phase_2": "1A", "Current_AC_Phase_3": "1A",
                    "PowerReal_PAC_Sum": "1W", "EnergyReal_WAC_Minus_Absolute": "1Wh",
                    "EnergyReal_WAC_Plus_Absolute": "1Wh", "Meter_Location_Current": "1", "Temperature_Channel_1": "1",
                    "Temperature_Channel_2": "1", "Digital_Channel_1": "1", "Digital_Channel_2": "1", "Radiation": "1",
                    "Hybrid_Operating_State": "1"}

    max_query_time = datetime.timedelta(days=15)
    """ 
        the inverter will return an error when asking for more than 16 days of data
        with error "Query interval is restricted to 16 days"
        however, smaller intervals may cause similar issues.
        set value to suboptimal value that works
    """

    def __init__(self, host):
        self.host = host
        self.base_url = "http://" + host + "/solar_api/v" + str(self.api_version) + "/"

    def check_server_compatibility(self):
        url = "http://" + self.host + "/solar_api/GetAPIVersion.cgi"
        r = requests.get(url)
        api_vers = r.json()
        compatible = True
        assert isinstance(api_vers, dict)
        if (api_vers['APIVersion'] != self.api_version):
            warnings.warn(
                "using api version newer than last tested (" + self.api_version + "): " + api_vers['APIVersion'])
            compatible = False
        if not api_vers['CompatibilityRange'] in FroniusInverter.tested_server_versions:
            warnings.warn(
                "using api compatibility range newer than last tested (" + FroniusInverter.tested_server_versions + "): " +
                api_vers['CompatibilityRange'])
            compatible = False
        return compatible, api_vers

    @classmethod
    def get_all_channels(cls):
        return list(cls.get_all_channel_dict().keys())

    @classmethod
    def get_all_channel_dict(cls):
        return cls.channel_dict

    def get_inverter_realtime_data(self):
        payload = {"Scope": "System"}
        url = self.base_url + "GetInverterRealtimeData.cgi"
        if FroniusInverter.debug:
            print(url)
        r = requests.get(url, params=payload)
        return r.json()

    def get_historical_data(self, fromDate, toDate, channels=None, strict=True):

        returndf = None
        error = 0

        if fromDate.tzinfo is None:
            warnings.warn("fromDate is not timezone aware. assuming local timezone")
            tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
            fromDate = fromDate.astimezone(tz)

        if toDate.tzinfo is None:
            warnings.warn("toDate is not timezone aware. assuming local timezone")
            tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
            toDate = toDate.astimezone(tz)

        # the fronius controller is picky when it comes to local timezones and may throw an error
        # convert to UTC
        fromDate = fromDate.astimezone(pytz.utc)
        toDate = toDate.astimezone(pytz.utc)

        fdate = fromDate
        while ((fdate < toDate) and (error == 0)):
            tdate = min(toDate, fdate + self.max_query_time - datetime.timedelta(seconds=1))
            jsondata = self.get_historical_data_json(fdate, tdate, channels)
            fdate = tdate

            faj = FroniusArchiveJson(jsondata)
            error = faj.error_code()
            if (faj.error_code() != 0):
                warnings.warn(str(faj.error_status()))
            else:
                if (not faj.is_empty()):
                    df = faj.data()
                    if (returndf is None):
                        returndf = df
                    else:
                        # merge the dictionaries for different device_ids
                        for key, value in df.items():
                            if key in returndf:
                                returndf[key] = pd.concat([returndf[key], value])
                                returndf[key] = returndf[key].sort_values(self.timestamp_colname)
                            else:
                                returndf[key] = value

                    if strict:
                        for key, value in returndf.items():
                            returndf[key] = returndf[key].loc[fromDate <= returndf[key][self.timestamp_colname]]
                            returndf[key] = returndf[key].loc[returndf[key][self.timestamp_colname] < toDate]

        return returndf

    def get_historical_data_json(self, fromDate, toDate, channels=None):

        if self.max_query_time < toDate - fromDate:
            warnings.warn("time period exceeds maximal query time")

        if (channels is None):
            channels = self.get_all_channels()

        payload = {"Scope": "System", "StartDate": fromDate, "EndDate": toDate, "Channel": channels}
        url = self.base_url + "GetArchiveData.cgi"
        if FroniusInverter.debug:
            print(url, str(fromDate), "->", str(toDate))
        r = requests.get(url, params=payload)
        return r.json()

    def get_historical_events_json(self, fromDate, toDate):
        payload = {"Scope": "System", "StartDate": fromDate, "EndDate": toDate,
                   "Channel": ["InverterEvents", "InverterErrors"]}
        url = self.base_url + "GetArchiveData.cgi"
        if FroniusInverter.debug:
            print(url, str(fromDate), "->", str(toDate))
        r = requests.get(url, params=payload)
        return r.json()

    def _getStartOfEvents(self, eventjson):
        data = eventjson["Body"]["Data"]
        assert (len(data) == 1)
        inverterID = (list(data.keys())[0])

        offset = int(list(((eventjson["Body"]["Data"]))[inverterID]["Data"]["TimeSpanInSec"]["Values"].keys())[0])
        seconds = datetime.timedelta(seconds=offset)

        datestring = eventjson["Head"]["RequestArguments"]["StartDate"]
        date = dateutil.parser.parse(datestring)

        return date + seconds

    def find_earliest_data(self, fromDate=None):
        return self.find_earliest_data_binary(fromDate)

    def find_earliest_data_linear(self, fromDate=None):
        channel = "TimeSpanInSec"

        if (fromDate is None):
            fromDate = self.epoch
        toDate = datetime.datetime.now(pytz.utc)

        assert (fromDate < toDate)

        step = self.max_query_time
        found = False
        result = None
        while (not found and (fromDate < toDate)):
            result = self.get_historical_data_json(fromDate, fromDate + step, [channel])
            if (1 == len(result["Body"]["Data"])):
                found = True
            fromDate += step

        if (found):
            return self._getStartOfEvents(result)
        else:
            return None

    def find_earliest_data_binary(self, fromDate=None, toDate=None):

        channel = "TimeSpanInSec"

        if (fromDate is None):
            fromDate = self.epoch
        if (toDate is None):
            toDate = datetime.datetime.now(pytz.utc)

        sampleScope = self.max_query_time

        # print("find_earliest_data:", str(fromDate), " - ", str(toDate), "[ ", str(toDate - fromDate), " ]")
        assert (fromDate < toDate)

        testTimeStart = max(fromDate, fromDate + (toDate - fromDate) / 2 - sampleScope / 2)
        testTimeEnd = min(toDate, testTimeStart + sampleScope)
        result = self.get_historical_data_json(testTimeStart, testTimeEnd, [channel])

        if (0 == len(result["Body"]["Data"])):
            # no data was found in this interval

            if testTimeEnd == toDate:
                # No data found at all!
                return None
            else:
                # search the data later than the test time + scope
                return (self.find_earliest_data_binary(testTimeEnd, toDate))
        else:
            # data was found.
            earliestFound = self._getStartOfEvents(result)
            # print("earliest data at : ", earliestFound)
            if testTimeStart == fromDate:
                # we found the earliest point
                return earliestFound
            else:
                # look for earlier data
                return self.find_earliest_data_binary(fromDate, earliestFound + datetime.timedelta(seconds=1))


class FroniusJson:
    def __init__(self, json):
        assert isinstance(json, dict)
        assert ('Body' in json)
        assert isinstance(json["Body"], dict)
        assert ('Head' in json)
        assert isinstance(json["Head"], dict)
        self.json = json

    def start_date(self):
        return dateutil.parser.parse(self.json["Head"]["RequestArguments"]["StartDate"])

    def end_date(self):
        return dateutil.parser.parse(self.json["Head"]["RequestArguments"]["EndDate"])

    def timestamp(self):
        return dateutil.parser.parse(self.json["Head"]["Timestamp"])

    def error_code(self):
        return int(self.json["Head"]["Status"]["Code"])

    def error_status(self):
        return (self.json["Head"]["Status"])

    def is_empty(self):
        return len(self.json["Body"]["Data"]) == 0


class FroniusRealTimeJson(FroniusJson):
    def __init__(self, json):
        super().__init__(json)
        if (self.error_code() == 0):
            data = self.json["Body"]["Data"]
            assert ('YEAR_ENERGY' in (data.keys()))

    def data(self, timestamp_colname="ts", append=None):
        series = [pd.Series([self.timestamp()], name=timestamp_colname)]
        for key, value in self.json['Body']['Data'].items():
            v = value['Values']['1']
            s = pd.Series([v], name=key)
            series += [s]

        result = pd.concat(series, axis=1)

        if append is not None:
            result = pd.merge(append, result, how='outer')
        return result


class FroniusArchiveJson(FroniusJson):
    def device_ids(self):
        return list(self.json["Body"]["Data"].keys())

    def channels(self, deviceID=None):
        if (deviceID is None):
            deviceID = self.device_ids()[0]
        return list(self.json["Body"]["Data"][deviceID]["Data"].keys())

    def data(self, timestamp_colname="ts"):
        result = {}
        for deviceID in self.device_ids():
            deviceDf = None
            channels = self.channels(deviceID)
            for channel in channels:
                my_dict = self.json["Body"]["Data"][deviceID]["Data"][channel]["Values"]

                start = self.start_date()
                offsets = pd.Series(list(my_dict.keys()))
                timestamps = offsets.map(lambda x: datetime.timedelta(seconds=int(x)) + start)

                measurements = pd.Series(list(my_dict.values()))

                df = pd.DataFrame({timestamp_colname: timestamps, channel: measurements})

                if (deviceDf is None):
                    deviceDf = df
                else:
                    deviceDf = pd.merge(deviceDf, df, how='outer')

            # Arrange the rows to start with the timestamp.  match the order of the json file.
            columnOrder = [timestamp_colname] + channels
            deviceDf = deviceDf[columnOrder]
            result[deviceID] = deviceDf

        return result
