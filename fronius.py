import requests
import warnings
import datetime
import dateutil
import pandas as pd


class FroniusInverter:
    'class implementing Fronius Solar API v1'

    tested_server_versions = ["1.5-4"]
    api_version = 1

    channel_dict = {"TimeSpanInSec": "sec", "Digital_PowerManagementRelay_Out_1": "1",
                    "EnergyReal_WAC_Sum_Produced": "Wh", "Current_DC_String_1": "1A", "Current_DC_String_2": "1A",
                    "Voltage_DC_String_1": "1V", "Voltage_DC_String_2": "1V", "Temperature_Powerstage": "1C",
                    "Voltage_AC_Phase_1": "1V", "Voltage_AC_Phase_2": "1V", "Voltage_AC_Phase_3": "1V",
                    "Current_AC_Phase_1": "1A", "Current_AC_Phase_2": "1A", "Current_AC_Phase_3": "1A",
                    "PowerReal_PAC_Sum": "1W", "EnergyReal_WAC_Minus_Absolute": "1Wh",
                    "EnergyReal_WAC_Plus_Absolute": "1Wh", "Meter_Location_Current": "1", "Temperature_Channel_1": "1",
                    "Temperature_Channel_2": "1", "Digital_Channel_1": "1", "Digital_Channel_2": "1", "Radiation": "1",
                    "Hybrid_Operating_State": "1"}

    max_query_time = datetime.timedelta(
        days=16)  # the inverter will return an error when asking for more than 16 days of data

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
                "using api compatibility range newer than last tested (" + FroniusInverter.tested_server_versions + "): " + api_vers[
                    'CompatibilityRange'])
            compatible = False
        return compatible, api_vers

    @classmethod
    def get_all_channels(cls):
        return list(cls.get_all_channel_dict().keys())

    @classmethod
    def get_all_channel_dict(cls):
        return cls.channel_dict

    def getInverterRealTimeData(self):
        payload = {"Scope": "System"}
        url = self.base_url + "GetInverterRealtimeData.cgi"
#        print(url)
        r = requests.get(url, params=payload)
        return r.json()

    def getHistoricalData(self, fromDate, toDate, channels=None):

        returndf = None
        error = 0
        while ((fromDate < toDate) and (error == 0)):
            to = min(toDate, fromDate + self.max_query_time - datetime.timedelta(seconds=1))
            jsondata = self.getHistoricalDataJson(fromDate, to, channels)
            fromDate += self.max_query_time

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
                                returndf[key] = returndf[key].sort_values(FroniusArchiveJson.timestamp_colname())
                            else:
                                returndf[key] = value
        return returndf

    def getHistoricalDataJson(self, fromDate, toDate, channels=None):

        if (channels == None):
            channels = self.get_all_channels()

        payload = {"Scope": "System", "StartDate": fromDate, "EndDate": toDate, "Channel": channels}
        url = self.base_url + "GetArchiveData.cgi"
        print(url, str(fromDate), "->", str(toDate))
        r = requests.get(url, params=payload)
        return r.json()

    def getHistoricalEventsJson(self, fromDate, toDate):
        payload = {"Scope": "System", "StartDate": fromDate, "EndDate": toDate,
                   "Channel": ["InverterEvents", "InverterErrors"]}
        url = self.base_url + "GetArchiveData.cgi"
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

    def findEarliestData(self, fromDate=None):
        return self.findEarliestDataLineary(fromDate)

    def findEarliestDataLineary(self, fromDate=None):
        epoch = datetime.datetime(2017, 9, 1)
        channel = "TimeSpanInSec"

        if (fromDate == None):
            fromDate = epoch
        toDate = datetime.datetime.now()

        assert (fromDate < toDate)

        step = datetime.timedelta(days=14)
        found = False
        result = None
        while (not found and (fromDate < toDate)):
            result = self.getHistoricalDataJson(fromDate, fromDate + step, [channel])
            if (1 == len(result["Body"]["Data"])):
                found = True
            fromDate += step

        if (found):
            return self._getStartOfEvents(result)
        else:
            return None

    def findEarliestDataBinary(self, fromDate=None, toDate=None, sampleScope=None, stopScope=None):
        warnings.warning(
            "sometimes the fronius device returns values outside of the requested interval. this screws up the binary search.  check later")
        epoch = datetime.datetime(2017, 1, 1)
        channel = "TimeSpanInSec"

        if (fromDate == None):
            fromDate = epoch
        if (toDate == None):
            toDate = datetime.datetime.now()
        if (sampleScope == None):
            sampleScope = datetime.timedelta(1)
        if (stopScope == None):
            stopScope = sampleScope * 2

        print("findEarliestData:", str(fromDate), " - ", str(toDate), "[ ", str(toDate - fromDate), " ]")
        assert (fromDate < toDate)
        assert (sampleScope < stopScope)

        searchScope = (toDate - fromDate) / 2
        testTime = fromDate + searchScope
        result = self.getHistoricalDataJson(testTime, testTime + sampleScope, [channel])

        if (0 == len(result["Body"]["Data"])):
            # no data was found in this interval
            # search the data later than the test time + scope
            return (self.findEarliestDataBinary(testTime + sampleScope, toDate, sampleScope, stopScope))
        else:
            # data was found.
            print("earliest data at : ", self._getStartOfEvents(result))
            if (searchScope < stopScope):
                # we found the earliest point within scope
                return self._getStartOfEvents(result)
            else:
                # look for earlier data
                return (self.findEarliestDataBinary(fromDate, testTime + sampleScope, sampleScope, stopScope))

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


class FroniusArchiveJson(FroniusJson):
    def device_ids(self):
        return list(self.json["Body"]["Data"].keys())

    def channels(self, deviceID=None):
        if (deviceID == None):
            deviceID = self.device_ids()[0]
        return list(self.json["Body"]["Data"][deviceID]["Data"].keys())

    def is_empty(self):
        return len(self.json["Body"]["Data"]) == 0

    @classmethod
    def timestamp_colname(cls):
        return "ts"

    def data(self):
        result = {}
        timestampCol = self.timestamp_colname()
        for deviceID in self.device_ids():
            deviceDf = None
            channels = self.channels(deviceID)
            for channel in channels:
                my_dict = self.json["Body"]["Data"][deviceID]["Data"][channel]["Values"]

                start = self.start_date()
                offsets = pd.Series(list(my_dict.keys()))
                timestamps = offsets.map(lambda x: datetime.timedelta(seconds=int(x)) + start)

                measurements = pd.Series(list(my_dict.values()))

                df = pd.DataFrame({timestampCol: timestamps, channel: measurements})

                if (deviceDf is None):
                    deviceDf = df
                else:
                    deviceDf = pd.merge(deviceDf, df, how='outer')

            # Arrange the rows to start with the timestamp.  match the order of the json file.
            columnOrder = [timestampCol] + channels
            deviceDf = deviceDf[columnOrder]
            result[deviceID] = deviceDf

        return result
