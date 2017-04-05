import pandas as pd
import numpy as np


def CountAppUsageTimes(AppNamesDf):
    # print((AppNamesDf))
    group = AppNamesDf.groupby(['app_name'])
    List =[(key,np.sum(value['fg_time_ms']))for (key,value) in group.__iter__()]
    return List
    # print(TimeSpent)



def ProcessAppUsage():
    df = pd.read_csv(r'907_2jrpSFEXxNsldKCnj0vL.csv')
    df['Date'],df['Time'] = zip(*df['record_time'].map(lambda x:x.split(' ')))
    grouped= df.groupby(['user_id','Date'])


    #Get Campus Entry, Leaving Times
    wifi_routers_visited_Daywise = [(key[0],key[1],CountAppUsageTimes(value[['app_name','fg_time_ms']])) for (key, value) in grouped.__iter__()]
    # print(wifi_routers_visited_Daywise)
    outputdf = pd.DataFrame(wifi_routers_visited_Daywise, columns=['ID','Date','AppUsage'])

    print(outputdf)

    return outputdf
    # print(df)

ProcessAppUsage()
