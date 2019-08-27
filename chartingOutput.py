import pandas as pd
import matplotlib.pyplot as plt

def chartDimensions(num):
    if (num % 3) == 0:
        x,y = (num/3, 3)
    elif (num % 2) == 0:
        x,y = (num/2, 2)
    else:
        x,y = (num,1)
    return int(x),int(y)

def chartReturnData(dataframe,i,v, tickOutput):
    output = pd.DataFrame()
    print('1')
    v['timestamp'] = pd.to_datetime(v['timestamp'])
    tickOutput['Date-Time'] = pd.to_datetime(tickOutput['Date-Time'])
    for ric in v['subjects']:
        instType, inst = ric.split(":")
        if instType == "R":
            output = output.append(tickOutput[(tickOutput['#RIC'] == inst) & (tickOutput['Date-Time'] > (v['timestamp'] - pd.Timedelta(minutes=2))) & (tickOutput['Date-Time'] < (v['timestamp'] + pd.Timedelta(minutes=2)))])
    output['Bid Price'] = output['Bid Price'].fillna(method='bfill')
    output['Ask Price'] = output['Ask Price'].fillna(method='bfill')
    output.drop_duplicates(inplace = True)
    size = len(output['#RIC'].unique())
    chX, chY = chartDimensions(size)
    if size > 0:
        fig, ax = plt.subplots(nrows=chY,ncols=chX, figsize = (15,10))
        num = 0
        ric = output['#RIC'].unique()
        mintime = output['Date-Time'].min()
        maxtime = output['Date-Time'].max()
        for suba in ax:
            for a in suba:
                tempOutput = output[output['#RIC'] == ric[num]].copy()
                a.plot(tempOutput['Date-Time'], tempOutput['Bid Price'], alpha = .7, fillstyle = 'left', linewidth = .5)
                a.plot(tempOutput['Date-Time'], tempOutput['Ask Price'], alpha = .7, fillstyle = 'left', linewidth = .5)
                a.plot(tempOutput['Date-Time'], tempOutput['Price'], alpha = .7, linestyle = '-.', marker = 'o')
                a.set_xlim(left=mintime, right=maxtime)
                a.axvline(x = v['timestamp'] , linestyle = '--', color ='r')
                a.set_title(ric[num], loc='center', fontsize=12)
                num +=1
        fig.suptitle("STORY REUTERS EXCLUSIVE:\n" + v['headline'], fontsize=13, color = 'black')
        # fig.tight_layout()
        fig.savefig('/home/josh/Documents/picture' + str(i) + '.jpg',dpi=300)
        plt.close('all')
