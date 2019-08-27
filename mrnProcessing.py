def show(dataframe):
    for i in dataframe.columns:
        print("\n'", i, "'\ncolumn position: [",dataframe.columns.get_loc(i),' of ', len(dataframe.columns) - 1,']\n', type(dataframe.iloc[0][i]))
        print(dataframe[i].head())

def findFiles(fileDir = '/home/josh/Documents/mrnfiles/'):
    files = os.listdir(fileDir)
    mrnFiles = []
    for file in files:
        x = os.listdir(fileDir + file)
        for i in x:
            mrnFiles.append(fileDir + file + '/' + i)
    return mrnFiles

def to_dataframe(mrnFiles , filter56 = True, sequence_filter = True):
    """

    :param mrnFiles: This is a list of files with paths to the MRN files meant to download.
    :param filter56: This is a True/False Flag on whether the output should be filtered for contentFlag == '56'
    :param sequence_filter: This is a True/False flag on whether the output should be filtered to sequence == 1.
    :return: returns a concatenated dataframe from a list of MRN Headline Direct files, which are themselves json files.
    """
    timestamp = []
    contentFlags = []
    coIds = []
    headline = []
    takeSequence = []
    messageType = []
    firstCreated = []
    subjects = []
    for file in mrnFiles:
        data = pd.read_json(file, compression='gzip', encoding = 'utf-8')
        data.dropna(inplace=True)
        db = pd.DataFrame(index = np.arange(data.shape[0]),columns = ['id', 'altId', 'versionCreated', 'subjects', 'headline', 'timestamp', 'source', 'language', 'name', 'provider', 'takeSequence', 'audiences', 'messageType', 'instancesOf', 'coIds', 'pubStatus', 'firstCreated','guid'])
        # df[['RIC', 'Type', 'Start', 'End', 'Created', 'MajorVersion','MinorVersion']] = data[['RIC', 'Type', 'Start', 'End', 'Created', 'MajorVersion','MinorVersion']]
        for topKey, topVal in data['Items'].items():  # Parsing out the Items column this will result in a key pair that is key(index) and value(dict).
            for secKey, secVal in topVal.items():  # Parsing out the topVal will result in a key pair that is [ guid:<class 'str'>,     timestamps:<class 'list'> containing a 'dict',  contentFlags:<class 'str'>, data:+<class 'dict'> ]
                if type(secVal) is dict:
                    for subKey, subVal in secVal.items():
                        if subKey == 'coIds':
                            coIds.append(subVal)
                        elif subKey == 'headline':
                            headline.append(subVal)
                        elif subKey == 'takeSequence':
                            takeSequence.append(subVal)
                        elif subKey == 'messageType':
                            messageType.append(subVal)
                        elif subKey == 'firstCreated':
                            firstCreated.append(subVal)
                        elif subKey == 'subjects':
                            subjects.append(subVal)
                elif type(secVal) is list:
                    for subKey, subVal in secVal[0].items():
                        if subKey == 'timestamp':
                            timestamp.append(subVal)
                elif secKey == 'contentFlags':
                    contentFlags.append(secVal)
    output = pd.DataFrame(data={'timestamp':timestamp, 'contentFlags':contentFlags,'coIds':coIds,'headline':headline, 'takeSequence':takeSequence, 'messageType':messageType, 'subjects':subjects, 'firstCreated':firstCreated})
    if filter56:
        output = output[output['contentFlags'] == '56']
    if sequence_filter:
        output = output[output['takeSequence'] == 1]
    return output

def filter_timerange(dataframe, time_column_name = 'timestamp',starttime = '11:30:00', endtime = '20:15:00'):
    """
    This is an additional filter to select just mrn headline events that occur during the US trading hours.

    :param df: a PANDAS dataframe.
    :param starttime: Format should be '%H:%M:%S'. Note the MRN times are in GMT, so this time should be rationalized to GMT.
    :param endtime: Format should be '%H:%M:%S'. Note the MRN times are in GMT, so this time should be rationalized to GMT.
    :return:
    """
    dataframe.index = pd.to_datetime(dataframe[time_column_name])
    dataframe = dataframe.between_time(starttime,endtime)
    dataframe.reset_index(inplace=True,drop=True)
    return dataframe
