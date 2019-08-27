import chartingOutput as cht
import mrnProcessing as mrn
import tickFiles as tick

fls = mrn.findFiles()
db = mrn.to_dataframe(fls)
db = mrn.filter_timerange(db)
tk = tick.generateToken()
for i,v in db.iterrows():
    rU,rH,rB = tick.generateRequestBody(token=tk,dataframe=db,index=i)
    f = tick.generateFiles(token = tk, requestUrl=rU,requestHeaders=rH,requestBody=rB, iterator = i)
    output = pd.read_csv(f,compression='gzip')
    cht.chartReturnData(dataframe = db, i=i,v=v, tickOutput=output)
