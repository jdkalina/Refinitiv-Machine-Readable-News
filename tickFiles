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


def generateRequestBody(token, dataframe, index, ric_column = 'coIds', time_col = 'timestamp'):
    """This function is meant to generate json request body and headers for a later post to tick history servers via api.

    :param token: the token received from Authentication call to TRTH servers in step 1
    :param dataframe: should be the output of the mrnReader methods
    :param index: this is for the iterator in a for loop
    :param ric_column: this is the text name of the column with Ids. For MRN, this is the coIds column
    :param time_col: THis is the name of the time column in the dataframe. Should already be converted using pandas.to_datetime()
    :return: a tuple with three object: the requestURL, requestHeaders, and requestBody
    """
    rics = dataframe.iloc[index][ric_column]
    timesDf = pd.DataFrame(data = {'max': [0], 'min':[0]})
    timesDf['min'] = pd.to_datetime(dataframe[time_col]) - pd.Timedelta(minutes=2)
    timesDf['max'] = pd.to_datetime(dataframe[time_col]) + pd.Timedelta(minutes=2)
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
    for ric in rics:
        instType, inst = ric.split(":")
        if instType == "R":
            requestBody["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append({"Identifier": inst,"IdentifierType": "Ric"})

    return requestUrl,requestHeaders,requestBody


def generateFiles(token, requestUrl,requestHeaders,requestBody, iterator = 0 ,filePath = "/home/josh/Documents/mrnout/"):
    r2 = requests.post(requestUrl, json=requestBody,headers=requestHeaders)
    status_code = r2.status_code
    print ("\tSTATUS: HTTP status of the response: " + str(status_code))
    if status_code == 202 :
        requestUrl = r2.headers["location"]
        print ('\tSTATUS: Begin Polling Pickup Location URL')

        requestHeaders={
            "Prefer":"respond-async",
            "Content-Type":"application/json",
            "Authorization":"token " + token
        }

        #As long as the status of the request is 202, the extraction is not finished;
        #we must wait, and poll the status until it is no longer 202:
        while (status_code == 202):
            print ('\tSTATUS: 202; waiting 15 seconds to poll again (need status 200 to continue)')
            time.sleep(15)
            r3 = requests.get(requestUrl,headers=requestHeaders)
            status_code = r3.status_code

        #When the status of the request is 200 the extraction is complete;
        #we retrieve and display the jobId and the extraction notes (it is recommended to analyse their content)):
        if status_code == 200 :
            print('\tSTATUS: 200')
            r3Json = json.loads(r3.text.encode('ascii', 'ignore'))
            jobId = r3Json["JobId"]

        #If instead of a status 200 we receive a different status, there was an error:
        if status_code != 200 :
            print ('ERROR: An error occurred. Try to run this cell again. If it fails, re-run the previous cell.\n')

        requestUrl = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults" + "('" + jobId + "')" + "/$value"
        useAws = True

        if useAws:
            requestHeaders={
                "Prefer":"respond-async",
                "Content-Type":"text/plain",
                "Accept-Encoding":"gzip",
                "X-Direct-Download":"true",
                "Authorization": "token " + token
            }
        else:
            requestHeaders={
                "Prefer":"respond-async",
                "Content-Type":"text/plain",
                "Accept-Encoding":"gzip",
                "Authorization": "token " + token
            }

        r5 = requests.get(requestUrl,headers=requestHeaders,stream=True)
        r5.raw.decode_content = False
        if useAws:
            print ('Content response headers (AWS server): type: ' + r5.headers["Content-Type"] + '\n')
            #AWS does not set header Content-Encoding="gzip".
        else:
            print ('Content response headers (TRTH server): type: ' + r5.headers["Content-Type"] + ' - encoding: ' + r5.headers["Content-Encoding"] + '\n')

        fileNameRoot = 'tickfile' + str(iterator)
        fileName = filePath + fileNameRoot + ".step5.csv.gz"

        print ('Saving compressed data to file:' + fileName)
        chunk_size = 1024
        rr = r5.raw
        with open(fileName, 'wb') as fd:
            shutil.copyfileobj(rr, fd, chunk_size)
        return fileName
        print ('\tSTATUS: Finished saving compressed data to file:' + fileName + '\n')
