import pandas as pd
import numpy as np
import collections as col
import  scipy as sp
import functools as funs

def ComputeVarianceOfBatteryTimings(file):
    df= pd.read_csv(file)
    #Splitting datetime in to date and time columns
    df['Date'], df['Time'] = zip(*df['start_time'].map(lambda x: x.split(' ')))

    #Removing rows with battery plugged status as o which is unplugged
    df= df[df.plugged !=0]
    df['Time'] =df['Time'].apply(ConvertTime)


    #Group by Time and
    grouping = df.groupby(['MappedID','Date'])

    #Get battery time for each day by taking the average for now
    batterychargeTime_perDay= [(key[0],np.average(value['Time'])) for (key,value) in grouping.__iter__()]
    tempdf= pd.DataFrame(batterychargeTime_perDay,columns=['ID','Time'])
    newgrouped = tempdf.groupby(['ID'])
    chargeTimingsVarianceList = [(key,np.var(value['Time'])) for (key,value) in newgrouped.__iter__()]
    outputdf= pd.DataFrame(chargeTimingsVarianceList,columns=['ID','VarChargeTime'])
    print(outputdf)
    return outputdf

def tempConvert(Values):
    return Values.get_values()
def ComputeChargerPluggedInTimeDailyV1(file):
    df= pd.read_csv(file)
    #Splitting datetime in to date and time columns
    df['Date'], df['Time'] = zip(*df['start_time'].map(lambda x: x.split(' ')))

    #Removing rows with battery plugged status as o which is unplugged
    df= df[df.plugged !=0]
    df['Time'] =df['Time'].apply(ConvertTime)


    #Group by Time and
    grouping = df.groupby(['MappedID','Date'])

    #Get battery time for each day by taking the average for now
    batterychargeTime_perDay= [(key[0],key[1],tempConvert(value['Time'])) for (key,value) in grouping.__iter__()]
    outputdf= pd.DataFrame(batterychargeTime_perDay,columns=['ID','Date','ChargerPlugInTime'])
    outputdf.to_csv(r'/home/naveen/Downloads/Data/BatteryOutPut1.csv')
    # print(outputdf)
    return outputdf



def ConvertToIntList(Values):
    return Values.get_values()

def ConvertToInt(value):

    return int(value)

def ConvertTime(Time):
    Times = Time.split(":")
    return int(Times[0])+((int)(Times[1])*(1/60))

def ComputeVariance_CampuseEntryLeaving_Timings(file):

    #Split Date and Time
    df = pd.read_csv(file)
    df_with_UofS_Wifi = df.loc[df.ssid.isin(['uofs-secure','uofs-public','uofs-guest'])]
    df_with_UofS_Wifi['Date'],df_with_UofS_Wifi['Time'] = zip(*df_with_UofS_Wifi['record_time'].map(lambda x:x.split(' ')))

    #Group by Id, Date
    grouped= df_with_UofS_Wifi.groupby(['user_id','Date'])


    #Get Campus Entry, Leaving Times
    campus_Entry_Leaving_Timingslist = [(key[0], min(value['Time']), max(value['Time'])) for (key, value) in grouped.__iter__()]
    # campus_Leaving_Timingslist = [(key[0], max(value['Time'])) for (key, value) in grouped.__iter__()]

    df = pd.DataFrame(campus_Entry_Leaving_Timingslist, columns=['ID', 'EntryTime','LeavingTime'])
    #Convertng the time and keeping in the new column
    df['convertedEntryTime']= df['EntryTime'].apply(ConvertTime)
    df['convertedLeavingTime']= df['LeavingTime'].apply(ConvertTime)
    newgrouped=df.groupby(['ID'])


    campus_entry_leaving_variance_list = [(key, np.var(value['convertedEntryTime']),np.var(value['convertedLeavingTime'])) for (key, value) in newgrouped.__iter__()]
    outputdf = pd.DataFrame(campus_entry_leaving_variance_list, columns=['ID', 'VarEntryTime','VarLeavingTime'])
    print(outputdf)

    return outputdf


def Compute_Campus_Entry_Leaving_Time_PerDay(file):

    #Split Date and Time
    df = pd.read_csv(file)
    df_with_UofS_Wifi = df.loc[df.ssid.isin(['uofs-secure','uofs-public','uofs-guest'])]
    df_with_UofS_Wifi['Date'],df_with_UofS_Wifi['Time'] = zip(*df_with_UofS_Wifi['record_time'].map(lambda x:x.split(' ')))

    #Group by Id, Date
    grouped= df_with_UofS_Wifi.groupby(['user_id','Date'])


    #Get Campus Entry, Leaving Times
    campus_Entry_Leaving_Timingslist = [(key[0],key[1], min(value['Time']), max(value['Time'])) for (key, value) in grouped.__iter__()]
    # campus_Leaving_Timingslist = [(key[0], max(value['Time'])) for (key, value) in grouped.__iter__()]

    df = pd.DataFrame(campus_Entry_Leaving_Timingslist, columns=['ID','Date', 'EntryTime','LeavingTime'])
    df['convertedEntryTime']= df['EntryTime'].apply(ConvertTime)
    df['convertedLeavingTime']= df['LeavingTime'].apply(ConvertTime)




    # print(df)

    return df

def Compute_TimeSpent_inSchool_PerDay(file):
    #Split Date and Time
    df = pd.read_csv(file)
    df_with_UofS_Wifi = df.loc[df.ssid.isin(['uofs-secure','uofs-public','uofs-guest'])]
    df_with_UofS_Wifi['Date'],df_with_UofS_Wifi['Time'] = zip(*df_with_UofS_Wifi['record_time'].map(lambda x:x.split(' ')))
    df_with_UofS_Wifi['ConvertedTime']= df_with_UofS_Wifi['Time'].apply(ConvertTime)
    #Group by Id, Date
    grouped= df_with_UofS_Wifi.groupby(['user_id','Date'])


    #Get Campus Entry, Leaving Times
    time_spent_inCampus = [(key[0],key[1],max(value['ConvertedTime']) -min(value['ConvertedTime'])) for (key, value) in grouped.__iter__()]
    #print(time_spent_inCampus)

    outputdf = pd.DataFrame(time_spent_inCampus, columns=['ID','Date','TimeinCampus'])
    #Convertng the time and keeping in the new column
    # print(outputdf)

    return outputdf

# Openness
def get_distinct_wifivisited_count_eachDay(file):
    #Split Date and Time
    df = pd.read_csv(file)
    df['record_time']= df['record_time'].astype(str)
    df['Date'],df['Time'] = zip(*df['record_time'].map(lambda x:x.split(' ')))
    # print(df)
    #Group by Id, Date
    grouped= df.groupby(['user_id','Date'])


    #Get Campus Entry, Leaving Times
    wifi_routers_visited_Daywise = [(key[0],key[1],CountDistinctStrings(value['ssid'])) for (key, value) in grouped.__iter__()]
    # print(wifi_routers_visited_Daywise)
    outputdf = pd.DataFrame(wifi_routers_visited_Daywise, columns=['ID','Date','WifiCountPerDay'])

    # print(outputdf)

    return outputdf

# Openness
def get_distinct_wifivisited_count_eachDay2(file):
    #Split Date and Time
    df = pd.read_csv(file)
    df['record_time']= df['record_time'].astype(str)
    df['Date'],df['Time'] = zip(*df['record_time'].map(lambda x:x.split(' ')))
    df_new= df[df.level > -40 ]
    # print(df)
    #Group by Id, Date
    grouped= df_new.groupby(['user_id','Date'])


    #Get Campus Entry, Leaving Times
    wifi_routers_visited_Daywise = [(key[0],key[1],CountDistinctStrings(value['ssid'])) for (key, value) in grouped.__iter__()]

    # print(wifi_routers_visited_Daywise)
    tempdf = pd.DataFrame(wifi_routers_visited_Daywise, columns=['ID','Date','ssids'])

    newgrouped = tempdf.groupby(['ID'])
    chargeTimingsVarianceList = [(key,np.average(value['ssids'])) for (key,value) in newgrouped.__iter__()]
    outputdf= pd.DataFrame(chargeTimingsVarianceList,columns=['ID','AvgWifiCount'])

    # print(outputdf)

    return outputdf

# Openness individual wifi counts per study
def get_distinct_wifivisited_count_eachDay3(file):
    #Split Date and Time
    df = pd.read_csv(file)
    df['record_time']= df['record_time'].astype(str)
    df['Date'],df['Time'] = zip(*df['record_time'].map(lambda x:x.split(' ')))

    # print(df)
    #Group by Id, Date
    grouped= df.groupby(['user_id'])


    #Get Campus Entry, Leaving Times
    wifi_routers_visited_Daywise = [(key, CountDistinctStrings(value['ssid'])) for (key, value) in grouped.__iter__()]

    # print(wifi_routers_visited_Daywise)
    tempdf = pd.DataFrame(wifi_routers_visited_Daywise, columns=['ID','Count'])


    # print(tempdf)

    return tempdf


def CountDistinctStrings(list):
    return np.count_nonzero(np.unique(list))

def Find_ContactRate_PerDay(file):
    df =pd.read_csv(file)

    df= pd.read_csv(file)
    smartphone_class_Id_List=['50020c','52020c','58020c','5a020c','62020c','70020c','72020c','78020c','7a020c']
    df_SmartPhones = df.loc[df.dev_class.isin(smartphone_class_Id_List)]
    df_NearBy_SmartPhones= df_SmartPhones[df_SmartPhones.rssi > -40 ]
    df_NearBy_SmartPhones['Date'],df_NearBy_SmartPhones['Time'] = zip(*df_NearBy_SmartPhones['record_time'].map(lambda x:x.split(' ')))

    #Grouping By ID, Date

    grouped = df_NearBy_SmartPhones.groupby(['user_id','Date'])

    #Get Contact Rate Each day
    ContactRateDailyList=[(key[0],key[1],CountDistinctStrings(value['mac'])) for (key,value) in grouped.__iter__()]
    outputdf= pd.DataFrame(ContactRateDailyList,columns=['ID','Date','ContactRatePerDay'])
    # print(outputdf)

def Find_ContactRate_PerDay2(file):
    df =pd.read_csv(file)
    df= pd.read_csv(file)
    smartphone_class_Id_List=['50020c','52020c','58020c','5a020c','62020c','70020c','72020c','78020c','7a020c']
    df_SmartPhones = df.loc[df.dev_class.isin(smartphone_class_Id_List)]
    df_NearBy_SmartPhones= df_SmartPhones[df_SmartPhones.rssi > -40 ]
    df_NearBy_SmartPhones['Date'],df_NearBy_SmartPhones['Time'] = zip(*df_NearBy_SmartPhones['record_time'].map(lambda x:x.split(' ')))

    #Grouping By ID, Date

    grouped = df_NearBy_SmartPhones.groupby(['user_id','Date'])

    #Get Contact Rate Each day
    ContactRateDailyList=[(key[0],key[1],CountDistinctStrings(value['mac'])) for (key,value) in grouped.__iter__()]
    tempdf= pd.DataFrame(ContactRateDailyList,columns=['ID','Date','ContactRatePerDay'])



    newgrouped = tempdf.groupby(['ID'])
    chargeTimingsVarianceList = [(key,np.sum(value['ContactRatePerDay'])) for (key,value) in newgrouped.__iter__()]
    outputdf= pd.DataFrame(chargeTimingsVarianceList,columns=['ID','AvgContactRate'])

    print(outputdf)

    return outputdf

def ComputeVariance_CampuseEntryLeaving_TimingsTemp(file):

    #Split Date and Time
    df = pd.read_csv(file)
    df_with_UofS_Wifi = df.loc[df.ssid.isin(['uofs-secure','uofs-public','uofs-guest'])]
    df_with_UofS_Wifi['Date'],df_with_UofS_Wifi['Time'] = zip(*df_with_UofS_Wifi['record_time'].map(lambda x:x.split(' ')))
    df_with_UofS_Wifi['Time'] = df_with_UofS_Wifi['Time'].apply(ConvertTime)
    #Group by Id, Date
    grouped= df_with_UofS_Wifi.groupby(['user_id','Time'])
    outputdf=[(key[0],np.average(value['Time'])) for (key,value) in grouped.__iter__()]
    print(outputdf)


    # return outputdf

def TakeMostProbableTimeInStudy(StudyValues,DayValues):

    if DayValues.count ==1 :
        return DayValues
    else:
        Time=0
        # print('Start')
        Studylst= StudyValues.Values.get_values()
        Daylst= DayValues.get_values()
        # print(type(Daylst))
        MostFreqTimeValue= 0
        MostFreqTimeKey = 0
        for index, x in np.ndenumerate(Daylst):
            for key,value in Studylst[0].most_common(len(Studylst[0])):
                if x == key:
                    if value > MostFreqTimeValue:
                        MostFreqTimeValue = value
                        MostFreqTimeKey=key

        # for key,value in lst[0].most_common(len(lst[0])):

        # (StudyValues.Values.get_values())
        # count= col.Counter(StudyValues.Values.get_values().tolist())
        # list =[1,2,3]
        # print(np.count_nonzero(studyValuesList=='10'))
        # counter = col.Counter(studyValuesList)
        # counter = col.Counter(StudyValues.Values.get_values())
        # print(counter)
        # print(StudyValues)
        return MostFreqTimeKey

def ComputeChargerPluggedInTimeDaily(file):
    df= pd.read_csv(file)
    #Splitting datetime in to date and time columns
    df['Date'], df['Time'] = zip(*df['start_time'].map(lambda x: x.split(' ')))

    #Removing rows with battery plugged status as o which is unplugged
    df= df[df.plugged !=0]
    df['Time'] =df['Time'].apply(ConvertTime)
    df['Time'] =df['Time'].apply(ConvertToInt)


    ##################################33
    tempdf = df
    tempgrouping = tempdf.groupby(['MappedID'])
    # batterychargeTimePerStudy={}
    batterychargeTimePerStudy= [(key,col.Counter(ConvertToIntList(value['Time']))) for (key,value) in tempgrouping.__iter__()]
    batterychargeTimePerStudydf= pd.DataFrame(batterychargeTimePerStudy,columns=['ID','Values'])
    batterychargeTimePerStudydf.to_csv(r'/home/naveen/Downloads/Data/wifi1.csv')
    # batteryTimingsCounter = col.Counter()
    # print(batterychargeTimePerStudydf)
    batterychargeTimePerStudydf.to_csv(r'/home/naveen/Downloads/Data/referenceforBatteryTimingsFrequency.csv')
    ####################################

    #Group by Time and
    grouping = df.groupby(['MappedID','Date'])

    #Get battery time for each day by taking the average for now
    batterychargeTime_perDay= [(key[0],key[1],TakeMostProbableTimeInStudy(batterychargeTimePerStudydf[batterychargeTimePerStudydf.ID ==key[0]],value['Time'])) for (key,value) in grouping.__iter__()]
    outputdf= pd.DataFrame(batterychargeTime_perDay,columns=['ID','Date','CharginTimeDaily'])
    # newgrouped = tempdf.groupby(['ID'])

    return outputdf

def main():

    #1.  Finding varicance for battery timings participant nos
    # battery_Data=ComputeChargerPluggedInTimeDaily(r'/home/naveen/Downloads/Data/BatteryEvents.csv')
    # battery_Data.to_csv(r'/home/naveen/Downloads/Data/FinalOutBatteryEvents.csv')
    # print(battery_Data)

    # 2,3 . Finding Campus enter,leaving Times
    # entryLeavedf=Compute_Campus_Entry_Leaving_Time_PerDay(r'/home/naveen/Downloads/Data/wifi.csv')
    # entryLeavedf.to_csv(r'/home/naveen/Downloads/Data/CampusEntryLeaveTime.csv')
    # print(entryLeavedf)

    #4. TimeSpent in Shcool
    # TimeSpentInSchool= Compute_TimeSpent_inSchool_PerDay(r'/home/naveen/Downloads/Data/wifi.csv')
    # TimeSpentInSchool.to_csv(r'/home/naveen/Downloads/Data/TimeSpentInSchool.csv')
    #5. Wifi
    # Wifi_visited_perDay= get_distinct_wifivisited_count_eachDay(r'/home/naveen/Downloads/Data/wifi.csv')
    # Wifi_visited_perDay.to_csv(r'/home/naveen/Downloads/Data/Wifi_visited_perDay.csv')

    # merged = pd.merge(battery_Data,entryLeavedf,on=['ID'])
    # merged.to_csv(r'/home/naveen/Downloads/Data/input.csv')
    # print(merged)

    ##################### Merging Dataframes
    df1= pd.read_csv(r'/home/naveen/Downloads/Data/ModifiedOutputs/CampusEntryLeaveTime.csv')
    df2= pd.read_csv(r'/home/naveen/Downloads/Data/ModifiedOutputs/FinalOutBatteryEvents.csv')
    df3= pd.read_csv(r'/home/naveen/Downloads/Data/ModifiedOutputs/TimeSpentInSchool.csv')
    df4= pd.read_csv(r'/home/naveen/Downloads/Data/ModifiedOutputs/Wifi_visited_perDay.csv')


    dfs = [df1,df2,df3,df4]
    df_final = funs.reduce(lambda left,right: pd.merge(left,right,on=['ID','Date']), dfs)
    df_final.to_csv(r'/home/naveen/Downloads/Data/Finalinput.csv')
    print(df_final)



    ###################################################################


    #Bluetooth : We dont have enough data to decide Finding the Contact patterens for each day   Mix of users ids and participant nos   Have very less no of records may be not suitable
    # contactRate_Data = Find_ContactRate_PerDay(r'/home/naveen/Downloads/Data/bluetooth.csv')


main()

