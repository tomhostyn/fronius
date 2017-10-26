class FroniusInverter:
    'class implementig Fronius Solar API v1'
    tested_server_version = "1.5-4"

    channelUnitDict = {"TimeSpanInSec": "sec", "Digital_PowerManagementRelay_Out_1": "1",
        "EnergyReal_WAC_Sum_Produced": "Wh", "Current_DC_String_1": "1A", "Current_DC_String_2": "1A",
        "Voltage_DC_String_1": "1V", "Voltage_DC_String_2": "1V", "Temperature_Powerstage": "1C",
        "Voltage_AC_Phase_1": "1V", "Voltage_AC_Phase_2": "1V", "Voltage_AC_Phase_3": "1V", "Current_AC_Phase_1": "1A",
        "Current_AC_Phase_2": "1A", "Current_AC_Phase_3": "1A", "PowerReal_PAC_Sum": "1W",
        "EnergyReal_WAC_Minus_Absolute": "1Wh", "EnergyReal_WAC_Plus_Absolute": "1Wh", "Meter_Location_Current": "1",
        "Temperature_Channel_1": "1", "Temperature_Channel_2": "1", "Digital_Channel_1": "1", "Digital_Channel_2": "1",
        "Radiation": "1", "Digital_PowerManagementRelay_Out_1": "1", "Hybrid_Operating_State": "1"}

    maxQueryTime = datetime.timedelta(
        days=16)  # the inverter will return an error when asking for more than 16 days of data

    def __init__(self, host):
        self.host = host
        self.baseURL = self.getBaseURL()

    def getBaseURL(self):
        url = "http://" + self.host + "/solar_api/GetAPIVersion.cgi"
        r = requests.get(url)
        api_version = r.json()

        assert (api_version['APIVersion'] == 1)
        if (api_version['CompatibilityRange'] != FroniusInverter.tested_server_version):
            warnings.warn("using api version newer than last tested (" + FroniusInverter.tested_server_version + "): " +
                          api_version['CompatibilityRange'])

        return "http://" + self.host + api_version['BaseURL']

    @classmethod
    def getChannels(cls):
        return list(cls.getChannelUnitDict().keys())

    @classmethod
    def getChannelUnitDict(cls):
        return cls.channelUnitDict

    def getInverterRealTimeData(self):
        payload = {"Scope": "System"}
        url = self.baseURL + "GetInverterRealtimeData.cgi"
        print(url)
        r = requests.get(url, params=payload)
        return r.json()

    def getHistoricalData(self, fromDate, toDate, channels=None):

        returndf = None
        error = 0
        while ((fromDate < toDate) and (error == 0)):
            to = min(toDate, fromDate + self.maxQueryTime - datetime.timedelta(seconds=1))
            jsondata = self.getHistoricalDataJson(fromDate, to, channels)
            fromDate += self.maxQueryTime

            faj = FroniusArchiveJson(jsondata)
            error = faj.errorCode()
            if (faj.errorCode() != 0):
                warnings.warn(str(faj.errorStatus()))
            else:
                if (not faj.isEmpty()):
                    df = faj.data()
                    if (returndf is None):
                        returndf = df
                    else:
                        # merge the dictionaries for different deviceIDs
                        for key, value in df.items():
                            if key in returndf:
                                returndf[key] = pd.concat([returndf[key], value])
                                returndf[key] = returndf[key].sort_values(FroniusArchiveJson.timestampColname())
                            else:
                                returndf[key] = value
        return returndf

    def getHistoricalDataJson(self, fromDate, toDate, channels=None):

        if (channels == None):
            channels = self.getChannels()

        payload = {"Scope": "System", "StartDate": fromDate, "EndDate": toDate, "Channel": channels}
        url = self.baseURL + "GetArchiveData.cgi"
        print(url, str(fromDate), "->", str(toDate))
        r = requests.get(url, params=payload)
        return r.json()

    def getHistoricalEventsJson(self, fromDate, toDate):
        payload = {"Scope": "System", "StartDate": fromDate, "EndDate": toDate,
                   "Channel": ["InverterEvents", "InverterErrors"]}
        url = self.baseURL + "GetArchiveData.cgi"
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
            return (self.findEarliestData(testTime + sampleScope, toDate, sampleScope, stopScope))
        else:
            # data was found.
            print("earliest data at : ", self._getStartOfEvents(result))
            if (searchScope < stopScope):
                # we found the earliest point within scope
                return self._getStartOfEvents(result)
            else:
                # look for earlier data
                return (self.findEarliestData(fromDate, testTime + sampleScope, sampleScope, stopScope))


class FroniusArchiveJson:
    def __init__(self, json):
        self.json = json

    def deviceIDs(self):
        return list(self.json["Body"]["Data"].keys())

    def channels(self, deviceID=None):
        if (deviceID == None):
            deviceID = self.deviceIDs()[0]
        return list(self.json["Body"]["Data"][deviceID]["Data"].keys())

    def startDate(self):
        return dateutil.parser.parse(self.json["Head"]["RequestArguments"]["StartDate"])

    def endDate(self):
        return dateutil.parser.parse(self.json["Head"]["RequestArguments"]["EndDate"])

    def timestamp(self):
        return dateutil.parser.parse(self.json["Head"]["Timestamp"])

    def isEmpty(self):
        return len(self.json["Body"]["Data"]) == 0

    def errorCode(self):
        return int(self.json["Head"]["Status"]["Code"])

    def errorStatus(self):
        return (self.json["Head"]["Status"])

    @classmethod
    def timestampColname(cls):
        return "ts"

    def data(self):
        result = {}
        timestampCol = self.timestampColname()
        for deviceID in self.deviceIDs():
            deviceDf = None
            channels = self.channels(deviceID)
            for channel in channels:
                my_dict = self.json["Body"]["Data"][deviceID]["Data"][channel]["Values"]

                start = self.startDate()
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

