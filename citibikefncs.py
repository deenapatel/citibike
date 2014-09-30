import datetime as dt
import pandas as pd
import numpy as np

def indexByBikeidTime(df):
    #original name triptobike
    #create a bikedf dataframe that is indexed by bikeid and starttime.
    bikedf=df[['starttime','stoptime','start station id','end station id','bikeid']].sort('starttime')
    bikedf=bikedf.set_index(['bikeid','starttime'], drop=False)
    bikedf=bikedf.drop(['bikeid'],axis=1)
    bikedf = bikedf.sortlevel(0)
    bikedf.index.names=['bikeid','time']
    return bikedf

def tripToStation(bikedf,startdate,numberofmonths,startrow):
    #original name biketowhere
    #converts the bike trip dataframe into the whereisbike datafame
    #which gives a table where rows are date-time hours, columns are bike id number, 
    #and entries are the station number
    #startrow tells are where bikes are from the last hour of the previous month
    
    #hourmarker is a df with hourly time as index,these will be the time markers 
    #where we look to see where the bikes are.
    timerange=pd.date_range(startdate,periods=24*31*numberofmonths,freq='H')
    hourmarker=pd.DataFrame(index=timerange)

    whereisbike=pd.DataFrame(index=timerange)

    for bikeid in bikedf.index.levels[0]:
        #dataframe with the start station info, indexed by start time
        startdf=bikedf.loc[bikeid][['start station id']]
        #dataframe with the end stationo info, indexed by stop time
        stopdf=bikedf.loc[bikeid][['stoptime','end station id']].set_index('stoptime')
        #merge these dataframes
        startstopdf=startdf.join(stopdf,how='outer')
        #change the start station ids to 0, so we know a bike is out on a ride
        startstopdf['start station id']=startstopdf['start station id']-startstopdf['start station id']
        #combine the start and stop columns into one
        ss=pd.concat([startstopdf['start station id'].dropna(),startstopdf['end station id'].dropna()])
        ss.name='station'
        #merge with hourmarker dataframe
        ss=hourmarker.join(ss,how='outer')
        if bikeid in startrow.index:
            ss.station[0]=startrow[bikeid]
        #forwardfill so that time markers now have info from the last stop or start the bike was at 
        ss=ss.fillna(method='ffill')
        #extract out only the hourly times
        whereisbike[str(bikeid)]=hourmarker.join(ss,how='left')

    whereisbike.index.name='datetime'
    whereisbike.rename(columns = lambda x: int(x), inplace=True) #renaming column names to be ints not strings
    return whereisbike

def unknownBikes(whereisbike,datetime):
    #returns the number of unaccounted for bikes at time(row)=datetime
    #these bikes have not yet been logged at a station as of that time.
    return whereisbike.loc[datetime].shape[0]-whereisbike.loc[datetime].count()
    #count doesn't include nans while shape does
    
def stationFill(whereisbike,stationinfo,startdate=dt.datetime(2014,5,1,0),numberofmonths=1):
    #returns number of bikes at a station (each station is a column) by hour 
    #of the month (each hour is a row)
    timerange=pd.date_range(startdate,periods=24*31*numberofmonths,freq='H')
    hourmarker=pd.DataFrame(index=timerange)
    stationfills=pd.DataFrame(index=stationinfo.index.map(lambda x: float(x)))
    stationfills.index.name='station id'

    #fills the dataframe with number of bikes at each station at each time
    for datetime in hourmarker.index:
        stationfills[datetime] = whereisbike.loc[datetime].value_counts()
        #if there are no bikes at the station then value_counts give nans, need to fill with 0
    return stationfills.T.fillna(0)

def bikeAR(df):
    #returns a df which tells you if a bike was added or removed from a station at a particular time
    startdf=df[['starttime','start station id','start station name']] # contains starting stations and times
    startdf.columns = ['time','station id','station name']
    startdf['bike']=-1 # start of trip removes a bike from station
    enddf=df[['stoptime','end station id','end station name']] # containts ending stations and times
    enddf.columns = ['time','station id','station name']
    enddf['bike']=1 #end of trip adds a bike to the station
    bikeAddRemove = pd.concat([startdf,enddf],ignore_index = True) #contains starting and ending stations
    bikeAddRemove = bikeAddRemove.sort('time').reset_index(drop=True)
    bikeAddRemove.set_index('time',inplace=True) # datetime is now the index
    return bikeAddRemove

def weekDayAvg(df):
    #this function takes a dataframe indexed by time (every hour for the time range)
    #it returns a dataframe with the averages by hour (0 to 23) for all weekdays.
    wdavg=df[df.index.dayofweek<5]
    wdavg['hour']=wdavg.index.hour
    return wdavg.groupby('hour').mean()

def bikeStationStays(whereisbike,stationinfo):
    #returns stationstays and bikestays dataframes
    #stationhours - number of hours a bike is at a station at a time
    #each row is a 'stay', gives the station id and number of hours a bike was at that station
    #station 0 is 'out on a ride', station -10 means the bike hasn't entered the system yet.
    #bikehours - number of trips of a certain length (rows are hour lenghts) for each bike (columns).

    stationstays=pd.DataFrame()
    bikestays=pd.DataFrame(index=np.arange(1,24*31))

    for bikeid in whereisbike.T.index:
        df1=pd.DataFrame()
        #at the begining of month some bikes may not have locations, fill with -10
        df=whereisbike[[bikeid]].fillna(-10)
        #block gives the block number associated with that bike stay
        df['block']=(df!=df.shift(1)).astype(int).cumsum()
        #hours tells the number of hours the bike was at that station at that stay
        df1['hours']=df.groupby(['block',bikeid]).size()
        df1=df1.reset_index().drop(['block'],axis=1)
    
        df1.rename(columns={bikeid:'station'},inplace=True)
        stationstays=stationstays.append(df1,ignore_index=True)
    
        df1.rename(columns={'hours':bikeid},inplace=True)
        bikestays[bikeid]=df1[df1['station']>0][bikeid].value_counts()
        
    #renaming station id to station new id which ranges from 0 to number of stations
    #-10 means the bike was out on a ride or not yet checked into the system
    stationstays['station new id']=stationstays['station'].map(stationinfo['new id']).fillna(-10)
        
    return stationstays, bikestays

def rebalanced(bikedf, startdate=dt.datetime(2014,5,1,0)):
    #when are the bikes being rebalanced?
    #determined by when a bike is picked up at a different station
    #from where it was last returned to
    #returns addbike, removebike
    #addbike gives the addition of bikes (ie to the station it was 
    #picked up from, using the time it was picked up as a proxy)
    #removebike gives the number of bikes removed from station, using
    #the time it was dropped off as a proxy.

    timerange=pd.date_range(startdate,periods=24*31,freq='H')
    hourmarker=pd.DataFrame(index=timerange)
    addbike=pd.DataFrame(index=timerange)
    removebike=pd.DataFrame(index=timerange)
    
    for bikeid in bikedf.index.levels[0]:#[:100]:

        df=bikedf.loc[bikeid].copy()
        df['start station id'] = df[['start station id']].shift(-1) #next station bike left from
        df['starttime'] = df[['starttime']].shift(-1) #next station bike left from

        #if stay=0 then the bike stayed at that station between the last drop off and next pickup 
        #if nonzero the bike was rebalanced
        df['moved']=df['start station id']!=df['end station id']
        df.dropna(inplace=True) #drops the last row which should have a nan from the shift
        df=df[df.moved] #only keeping rows where a bike has been rebalanced
        #df.drop(['starttime','moved'],axis=1,inplace=True)
    
        dfadd=df.set_index('starttime',inplace=False)
        dfadd=dfadd.join(hourmarker,how='outer')
        dfadd.fillna(method='ffill',limit=1,inplace=True)
        addbike=addbike.join(dfadd['start station id'],how='left')
        addbike.rename(columns={'start station id':bikeid},inplace=True)
    
        dfrm=df.set_index('stoptime',inplace=False)
        dfrm=dfrm.join(hourmarker,how='outer')
        dfrm.fillna(method='ffill',limit=1,inplace=True)
        removebike=removebike.join(dfrm['end station id'],how='left')
        removebike.rename(columns={'end station id':bikeid},inplace=True)

    return addbike, removebike



