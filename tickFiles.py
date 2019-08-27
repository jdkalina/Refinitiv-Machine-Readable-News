import pandas as pd
import numpy as np
import requests
import json
import time
import shutil
import os

def generateToken(myUsername = '',myPassword = ''):
    """Generates the token object from Tick History to pair with later calls.
    """
    requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"
    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json"
    }
    requestBody={"Credentials": {"Username": myUsername,"Password": myPassword}}
    r1 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)

    if r1.status_code == 200 :
        jsonResponse = json.loads(r1.text.encode('ascii', 'ignore'))
        token = jsonResponse["value"]
        print ('\tSTATUS: Authentication token (valid 24 hours):')

    else:
        print ('Replace myUserName and myPassword with valid credentials, then repeat the request')
    return token

def logCalls(ric, timestamp, start, end, file = '/home/josh/Documents/trthlogfile.txt'):
    with open(file, 'a') as f:
        mytime = time.strftime('%Y-%m-%d %H:%M', time.localtime())
        string = mytime + ':\n\t' + ric + '\n\tStarting call range:\t' + str(start) + '\n\tStorytime:\t' + str(timestamp) + '\n\tending call range:\t' + end
        f.write(string + '\n\n')
        
def generateRequestBody(token, dataframe, index, ric_column = 'subjects', time_col = 'timestamp'):
    """

    :param token: the token received from Authentication call to TRTH servers in step 1
    :param dataframe: should be the output of the mrnReader methods
    :param index: this is for the iterator in a for loop
    :param ric_column: this is the text name of the column with Ids. For MRN, this is the coIds column
    :param time_col: THis is the name of the time column in the dataframe. Should already be converted using pandas.to_datetime()
    :return: a tuple with three object: the requestURL, requestHeaders, and requestBody
    """
    rics = dataframe.iloc[index][ric_column]
    timesDf = pd.DataFrame(data = {'max': [0], 'min':[0]})
    timesDf['min'] = pd.to_datetime(dataframe.iloc[index][time_col]) - pd.Timedelta(minutes=3)
    timesDf['max'] = pd.to_datetime(dataframe.iloc[index][time_col]) + pd.Timedelta(minutes=3)
    timesDf['min'] = timesDf['min'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    timesDf['max'] = timesDf['max'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    requestUrl='https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'

    requestHeaders={
        "Prefer":"respond-async",
        "Content-Type":"application/json",
        "Authorization": "token " + token
    }

    requestBody={
        "ExtractionRequest": {
            "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryTimeAndSalesExtractionRequest",
            "ContentFieldNames": ['Quote - Ask Price',
                                  'Quote - Bid Price',
                                  'Quote - Bid Size',
                                  'Trade - Market VWAP',
                                  'Trade - Price',
                                  'Trade - Volatility',
                                  'Trade - Volume'],
            "IdentifierList": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                "InstrumentIdentifiers": []
            },
            "Condition": {
                "MessageTimeStampIn": "GmtUtc",
                "ReportDateRangeType": "Range",
                "QueryStartDate": timesDf.iloc[0]['min'],
                "QueryEndDate": timesDf.iloc[0]['max'],
                "DisplaySourceRIC":"true"
            }
        }
    }
    logrics = ''
    for ric in rics:
        instType, inst = ric.split(":")
        if instType == "R":
            requestBody["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": inst,"IdentifierType": "Ric"})
            logrics = inst + ',' + logrics

    logCalls(logrics, dataframe.iloc[index][time_col],timesDf.iloc[0]['min'], timesDf.iloc[0]['max'])
    return requestUrl,requestHeaders,requestBody
