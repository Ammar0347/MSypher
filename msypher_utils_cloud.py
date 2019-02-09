# -*- coding: utf-8 -*-
"""
Created on Tue Oct 30 15:46:46 2018

@author: Ammar.Aamir
"""

import pandas as pd
from datetime import timedelta
import os
from xlrd import XLRDError
from sqlalchemy import create_engine

UPLOAD_FOLDER = 'processthesesheets/'

channel_list_budget = ["ARY DIGITAL", "HUM TV", "GEO ENTERTAINMENT", "TV ONE", "URDU 1", "PTV NEWS", 
                       "PTV SPORTS", "AAJ NEWS", "HNOW", "ARY ZINDAGI", "PLAY ENTERTAINMENT", 
                       "EXPRESS ENTERTAINMENT", "GEO KAHANI", "FILMAZIA", "WB", "HBO", "SILVER SCREEN",
                       "FILMAX", "FILM WORLD", "AXN", "RAAVI TV", "GEO NEWS", "EXPRESS NEWS", "DUNYA NEWS",
                       "SAMAA", "A PLUS", "CAPITAL TV", "NEWS ONE", "DIN NEWS TV", "AAJ ENTERTAINMENT",
                       "ARY NEWS", "DAWN NEWS", "ABB TAKK", "NEO TV", "HUM NEWS", "NINETY 2 NEWS", 
                       "TWENTYFOUR NEWS", "K21", "SEVEN NEWS", "CITY 42", "SINDH TV", "KASHISH", "APNA CHANNEL",
                       "WASEB", "VSH NEWS", "AVT KHYBER", "ARUJ TV", "PASHTO 1", "JALWA", "EIGHTXM", 
                       "CARTOON NETWORK", "NICKELODEON", "MASALA TV", "PUBLIC NEWS", "TEN SPORTS", "GEO SUPER", 
                       "PTV HOME", "SUCH TV", "MEHRAN TV", "KTN", "DHARTI TV", "AWAZ", "KIDS POP", "KIDS ZONE",
                       "ATV", "HUM SITARAY", "BOL NEWS", "KTN NEWS", "SINDH TV NEWS",
                       "PUNJAB TV", "KAY 2", "ARY MUSIK", "CINEMACHI KIDS", "GTV", "ROHI TV", "AFGHAN TV"]

tband_24hrs = ['00:00','01:00','02:00','03:00','04:00','05:00','06:00',
               '07:00', '08:00', '09:00', '10:00', '11:00', '12:00',
               '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', 
               '19:00', '20:00', '21:00', '22:00', '23:00']

#engine = create_engine('mysql+pymysql://ammarA:ammar@123@mindshare-domino-db.cxvf7pdkalcg.eu-central-1.rds.amazonaws.com:3306/ammar_all_purpose')
#engine = create_engine('mysql+pymysql://ammar:ammar123@msypher.cncilz0i4y2d.us-east-1.rds.amazonaws.com:3306/msypherdb')
engine = create_engine('mysql+pymysql://ammarA:ammar123@msyphercloud.cncilz0i4y2d.us-east-1.rds.amazonaws.com:3306/ammardb')

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)
 
def load_user(user,pwd):
    infodf = pd.read_sql_query('''select * from users where username="%s" AND password="%s";''' %(user, pwd), con=engine)
    return infodf
       
def load_optimizer(x, bands):
    # For inventory dataframe
    inventory_df = x.iloc[4:bands+4,12:19]
    inventory_df.columns = x.iloc[3,12:19]
    inventory_df = inventory_df.astype('float')
    
    # For rate dataframe
    rate_df = x.iloc[4:bands+4,20:27]
    rate_df.columns = x.iloc[3,20:27]
    rate_df = rate_df.astype('float')
    return rate_df, inventory_df

def load_budgetsheet(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    d_f = pd.read_sql_query("select * from "+monthm+"_budgetsheet;", con=engine)
#    d_f = d_f.reset_index()
    if('index' in d_f.columns):
        d_f.index = d_f['index']
        d_f = d_f.drop(['index'], axis=1)
#    if('Channel Name' in d_f.columns):
#        d_f.index = d_f['Channel Name']
#        d_f = d_f.drop(['Channel Name'], axis=1)
    #con.close()
    return d_f

def save_budgetsheet(df, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    df.to_sql(monthm+"_budgetsheet", con=engine, if_exists="replace")
    #con.close()
    
def load_paid_optimizer(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    d_f = pd.read_sql_query("select * from Paid_inventory_"+monthm+";", con=engine)
    if 'index' in d_f.columns:
        d_f = d_f.drop(['index'], axis=1)
    #con.close()
    return d_f

def save_paid_optimizer(df, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    df.to_sql("Paid_inventory_"+monthm, con=engine, if_exists="replace", index=False)
    #con.close()

def load_foc_optimizer(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    if engine.dialect.has_table(engine, "FOC_inventory_"+monthm):
        d_f = pd.read_sql_query("select * from FOC_inventory_"+monthm+";", con=engine)
        if 'index' in d_f.columns:
            d_f = d_f.drop(['index'], axis=1)
        #con.close()
        return d_f

def save_foc_optimizer(df, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    df.to_sql("FOC_inventory_"+monthm, con=engine, if_exists="replace")
    #con.close()

def load_cprp_optimizer(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    if engine.dialect.has_table(engine, "CPRP_inventory_"+monthm):
        d_f = pd.read_sql_query("select * from CPRP_inventory_"+monthm+";", con=engine)
        if 'index' in d_f.columns:
            d_f = d_f.drop(['index'], axis=1)
        #con.close()
        return d_f

def save_cprp_optimizer(df, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    df.to_sql("CPRP_inventory_"+monthm, con=engine, if_exists="replace")
    #con.close()

def load_spots_with_inventory(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    d_f = pd.read_sql_query("select * from "+monthm+"_fullplan_spotswithinventory;", con=engine)
    #con.close()
    return d_f

def save_spots_with_inventory(final_df, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    final_df.to_sql(monthm+"_fullplan_spotswithinventory", con=engine, if_exists="replace")
    #con.close()

def load_spots_only(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)  
    d_f = pd.read_sql_query("select * from "+monthm+"_fullplan_spots;", con=engine, index_col='index')
    #con.close()
    return d_f

def save_spots_only(spotsonly_df, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    spotsonly_df.to_sql(monthm+"_fullplan_spots", con=engine, if_exists="replace", chunksize=50)
    #con.close()

def save_entries(preprocess_df, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    preprocess_df.to_sql(monthm+"_entry", con=engine, if_exists="replace", index=False)
    #con.close()
    
def load_entries(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    preprocess_df = pd.read_sql_query("select * from "+monthm+"_entry;", con=engine)
    if 'index' in preprocess_df.columns:
        preprocess_df = preprocess_df.drop(['index'], axis=1)
#    preprocess_df.to_sql("November_APNA_Sunsilk", con=engine, if_exists="replace")
    #con.close()
    return preprocess_df

def load_converted_spots(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)  
    d_f = pd.read_sql_query("select * from Converted_"+monthm+"_Updated;", con=engine)
#    print(d_f)
#    d = pd.ExcelFile('Converted_Impulse4.xlsx')
#    d_f = d.parse('Sheet1')
#    d_f = d_f.drop(['Cost'], axis=1)
#    print(d_f)
    #con.close()
    return d_f

def generate_channel_summary(d_f):
    d_f = d_f[d_f['Time Band'] == "0"]
    summary_df = pd.DataFrame()
    summary_df["Plan"] = d_f["Plan"]
    summary_df["Channel"] = d_f["Channel"]
    summary_df["Brand"] = d_f["Brand"]
    summary_df["Caption"] = d_f["Caption"]
    summary_df["Duration"] = d_f["Duration"]
    summary_df["Total Spots"] = d_f["Total Spots"]
    summary_df["GRP"] = d_f["GRP"]
    summary_df["Spends"] = d_f["Spends"]
    summary_df["CPRP"] = d_f["CPRP"]
    return summary_df

def save_brand_splits():
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    brands_split = pd.ExcelFile('brand_splits.xlsx')
    splits_df = brands_split.parse('Cluster TG Working')
    splits_df = splits_df.iloc[:, 2:-1]
    splits_df.to_sql("Brand_splits", con=engine, if_exists="replace")
    #con.close()

def load_brand_splits():
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    splits_df = pd.read_sql_query("select * from Brand_splits;", con=engine)
    splits_df.index = splits_df["Brands"]
    splits_df = splits_df.drop(['index', 'Brands'], axis=1)
    #con.close()
    return splits_df

def load_ratings(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    ratings_df_consolidated = pd.read_sql_query("select * from Ratingsplan_"+monthm+";", con=engine)
    if 'index' in ratings_df_consolidated.columns:
        ratings_df_consolidated = ratings_df_consolidated.drop(['index'],axis=1)
    #con.close()
    return ratings_df_consolidated

def load_rates(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    rates_df_consolidated = pd.read_sql_query("select * from Paid_rates_"+monthm+";", con=engine)
    if 'index' in rates_df_consolidated.columns:
        rates_df_consolidated = rates_df_consolidated.drop(['index'],axis=1)
    #con.close()
    return rates_df_consolidated

def load_timebands(monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    timebands_consolidated = pd.read_sql_query("select * from Paid_timebands_"+monthm+";", con=engine)
    if 'index' in timebands_consolidated.columns:
        timebands_consolidated = timebands_consolidated.drop(['index'],axis=1)
    #con.close()
    return timebands_consolidated

def load_distribution(nameofplantype, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    dist = pd.read_sql_query("select * from "+nameofplantype+"_Minutes_Distribution_"+monthm+";", con=engine)
#    dist.index = dist["Channel Name"]
#    dist = dist.drop(["Channel Name"], axis=1)
    #con.close()
    return dist

def save_distribution(dist, nameofplantype, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    dist.to_sql(nameofplantype+"_Minutes_Distribution_"+monthm,con=engine, if_exists="replace")
    #con.close()    

def load_historical_data():
#    con = sqlite3.conect("channelsplan.db")
#    df = pd.read_sql_query("select Channel,Date,Day,AdType,AdStart,MidBreak,TransmissionHour from Historical_tracking_data where AdType like 'Spot/TVC' and MidBreak like 'Mid Break%';", con=engine)
#    df = pd.read_sql_query("select Channel,Date,Day,AdType,AdStart,MidBreak,TransmissionHour from Tracking_Data where AdType like 'Spot/TVC';", con=engine)
#    df = df[df.AdType == 'Spot/TVC']
#    df = df[(df.MidBreak != 'Opening') & (df.MidBreak != 'Closing') & (df.MidBreak != 'Casual') & (df.MidBreak != 'Prog. Part')]
    # Access data store
    data_store = pd.HDFStore('processed_data.h5')
    
    # Retrieve data using key
    df = data_store['preprocessed_df']
    data_store.close()

#    df = df.sort_values(by='Date')
    #con.close()
    return df
    
def load_pib_splits():
#    con = sqlite3.conect("channelsplan.db")
    merged_pib = pd.read_sql_query("select * from PIB_splits;", con=engine)
    merged_pib.index = merged_pib['index']
    merged_pib = merged_pib.drop(['index'], axis=1)
    #con.close()
    return merged_pib

def save_converted_file(converted_df, monthm):
#    con = sqlite3.conect("channelsplan.db")
    converted_df.to_sql("Converted_"+monthm+"_Updated", con=engine, if_exists="replace")    
    #con.close()

def load_splits():
    splits_df = pd.read_sql_query("select * from Brand_splits;", con=engine)
    return splits_df

def generate_budgetsheet_in_db(fname, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    budgetsheet = pd.ExcelFile(os.path.join(UPLOAD_FOLDER,fname))
    b_df = budgetsheet.parse(budgetsheet.sheet_names[1])
    b_df = b_df.iloc[1:,6:]
    b_df = b_df.fillna(0)

    b_df = b_df[(b_df['Channel Name'] != 0) & (b_df['Channel Name'] != 'TOTAL') 
    & (b_df['Channel Name'] != 'TOTAL TV') & (b_df['Channel Name'] != 'Sports Channels') & (b_df['Channel Name'] != 'Cooking')
    & (b_df['Channel Name'] != 'Kids Channels') & (b_df['Channel Name'] != 'Music Channels')
    & (b_df['Channel Name'] != 'Regional/Religious') & (b_df['Channel Name'] != 'News Channels')
    & (b_df['Channel Name'] != 'Movie Channels') & (b_df['Channel Name'] != 'Entertainment')
    & (b_df['Channel Name'] != 'Terr')]


    check_channels = b_df#[b_df['Channel Name'].isin(channel_list_budget)]
    
    check_channels.index = check_channels['Channel Name']
    get_brands_budget = check_channels.iloc[:,:-6]
    get_brands_budget.index = check_channels.loc[:,'Channel Name']
    percentage_df = pd.DataFrame(columns=get_brands_budget.columns.tolist(), index=get_brands_budget.index)
    
    budget_df = pd.DataFrame(columns=get_brands_budget.columns.tolist(), index=get_brands_budget.index)
    for i in range(len(channel_list_budget)):
        budget_df.loc[channel_list_budget[i],:] = get_brands_budget.loc[channel_list_budget[i],:]
        total = check_channels.loc[channel_list_budget[i],'TOTAL']
        percentage_df.loc[channel_list_budget[i],:] = (get_brands_budget.loc[channel_list_budget[i],:]/total)
    budget_df = budget_df.reset_index()
    percentage_df = percentage_df.reset_index()
    budget_df.to_sql(monthm+"_budgetsheet", con=engine, if_exists="replace")
    percentage_df.to_sql("Minutes_Distribution_"+monthm,con=engine, if_exists="replace")
    #con.close()
    
def generate_paid_optimizer_in_db(fname, monthm, startdt, enddt):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    inputsheet = pd.ExcelFile(os.path.join(UPLOAD_FOLDER,fname))
    sheet_names = inputsheet.sheet_names
    paid_channel_rates = []
    paid_channel_inventories = []
    paid_channel_timebands = []
    for s in sheet_names:
        if s == "ARY DIGITAL" or s == "PTV HOME":
            t = 26
        else:
            t = 24
        x = inputsheet.parse(s)
        RPM_df, inventory_df = load_optimizer(x, t)
        tband = x.iloc[4:t+4,2]
        t = x.iloc[4:t+4,2]
        tband.columns = x.iloc[1,2]
        time_band = pd.DataFrame(tband)
        mycolumns = ['Channel', 'Time Band']
        for dt in daterange(startdt, enddt):
            mycolumns.append(dt.strftime("%a_%e-%b"))
        invplan_df = pd.DataFrame(columns= mycolumns)
        rateplan_df = pd.DataFrame(columns=mycolumns)
        if(not inventory_df.empty):
            for col in mycolumns:
                if(col.split("_")[0] == "Sun"):
                    invplan_df.loc[:, col] = inventory_df.loc[:,col.split("_")[0]]
                    rateplan_df[col] = RPM_df[col.split("_")[0]].values
                if(col.split("_")[0] == "Mon"):
                    invplan_df.loc[:, col] = inventory_df.loc[:,col.split("_")[0]]
                    rateplan_df[col] = RPM_df[col.split("_")[0]].values
                if(col.split("_")[0] == "Tue"):
                    invplan_df.loc[:, col] = inventory_df.loc[:,col.split("_")[0]]
                    rateplan_df[col] = RPM_df[col.split("_")[0]].values
                if(col.split("_")[0] == "Wed"):
                    invplan_df.loc[:, col] = inventory_df.loc[:,col.split("_")[0]]
                    rateplan_df[col] = RPM_df[col.split("_")[0]].values
                if(col.split("_")[0] == "Thu"):
                    invplan_df.loc[:, col] = inventory_df.loc[:,col.split("_")[0]]
                    rateplan_df[col] = RPM_df[col.split("_")[0]].values
                if(col.split("_")[0] == "Fri"):
                    invplan_df.loc[:, col] = inventory_df.loc[:,col.split("_")[0]]
                    rateplan_df[col] = RPM_df[col.split("_")[0]].values
                if(col.split("_")[0] == "Sat"):
                    invplan_df.loc[:, col] = inventory_df.loc[:,col.split("_")[0]]
                    rateplan_df[col] = RPM_df[col.split("_")[0]].values
            invplan_df['Channel'] = s
            invplan_df['Time Band'] = [str(x)[:-3] for x in t]
            rateplan_df['Channel'] = s
            rateplan_df['Time Band'] = [str(x)[:-3] for x in t]
            time_band['Channel'] = s
            paid_channel_inventories.append(invplan_df)
            paid_channel_rates.append(rateplan_df)
            paid_channel_timebands.append(time_band)
    paidchannel_inventory = pd.concat(paid_channel_inventories)
    paidchannel_rates = pd.concat(paid_channel_rates)
    paidchannel_timeband = pd.concat(paid_channel_timebands)
    paidchannel_inventory.to_sql("Paid_inventory_"+monthm, con=engine, if_exists="replace")
    paidchannel_rates.to_sql("Paid_rates_"+monthm, con=engine, if_exists="replace")
    paidchannel_timeband.to_sql("Paid_timebands_"+monthm, con=engine, if_exists="replace")
    #con.close()
    
def generate_foc_optimizer_in_db(fname, monthm, startdt, enddt):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    focsheet = pd.ExcelFile(os.path.join(UPLOAD_FOLDER,fname))
    foc = focsheet.parse('FOC')
    percentage_df = pd.read_sql_query("select * from Minutes_Distribution_"+monthm+";", con=engine)
    if 'index' in percentage_df.columns:
        percentage_df.index = percentage_df['index']
        percentage_df = percentage_df.drop(['index'],axis=1)
#    if 'Channel Name' in percentage_df.columns:
#        percentage_df.index = percentage_df['Channel Name']
#        percentage_df = percentage_df.drop(['Channel Name'],axis=1)
    
    foc_inventories = []
    percentage_df = percentage_df.fillna(0)
    for r in channel_list_budget:
        mycolumns = ['Channel', 'Time Band']
        for dt in daterange(startdt, enddt):
            mycolumns.append(dt.strftime("%a_%e-%b")) 
        foc_df = pd.DataFrame(columns=mycolumns)   
        if r in foc.columns:
            total_mins = foc[r].sum()*len(mycolumns)
            percentage_df.loc[percentage_df["Channel Name"]==r,'Ponds White Beauty Cream':'Dove Bars'] = percentage_df.loc[percentage_df["Channel Name"]==r,'Ponds White Beauty Cream':'Dove Bars']*int(total_mins)
            for col in mycolumns:
                foc_df.loc[:,col] = foc[r]
            foc_df['Channel'] = r
            foc_df['Time Band'] = tband_24hrs
            foc_inventories.append(foc_df)
    focchannels_inventory = pd.concat(foc_inventories)
    focchannels_inventory = focchannels_inventory.reset_index()
    focchannels_inventory.to_sql("FOC_inventory_"+monthm,con=engine, if_exists="replace")
#    print(percentage_df)
    percentage_df.to_sql("FOC_Minutes_Distribution_"+monthm,con=engine, if_exists="replace")
    #con.close()
    
def generate_cprp_optimizer_in_db(fname, monthm, startdt, enddt):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    cprpsheet = pd.ExcelFile(os.path.join(UPLOAD_FOLDER,fname))
    cprp = cprpsheet.parse('CPRP')
    percentage_df = pd.read_sql_query("select * from Minutes_Distribution_"+monthm+";", con=engine)
    if 'index' in percentage_df.columns:
        percentage_df.index = percentage_df['index']
        percentage_df = percentage_df.drop(['index'],axis=1)
#    if 'Channel Name' in percentage_df.columns:
#        percentage_df.index = percentage_df['Channel Name']
#        percentage_df = percentage_df.drop(['Channel Name'],axis=1)
    cprp_inventories = []
    for r in channel_list_budget:
        mycolumns = ['Channel', 'Time Band']
        for dt in daterange(startdt, enddt):
            mycolumns.append(dt.strftime("%a_%e-%b")) 
        cprp_df = pd.DataFrame(columns=mycolumns)
        if r in cprp.columns:
            time_band_cprp = cprp['Time Band']
            total_mins = cprp[r].sum()*len(mycolumns)
            percentage_df.loc[percentage_df["Channel Name"]==r,'Ponds White Beauty Cream':'Dove Bars'] = percentage_df.loc[percentage_df["Channel Name"]==r,'Ponds White Beauty Cream':'Dove Bars']*int(total_mins)
            for col in mycolumns:
                cprp_df.loc[:,col] = cprp[r]
            cprp_df['Channel'] = r
            cprp_df['Time Band'] = tband_24hrs
            cprp_inventories.append(cprp_df)
    cprpchannels_inventory = pd.concat(cprp_inventories)
    cprpchannels_inventory = cprpchannels_inventory.reset_index()
    cprpchannels_inventory.to_sql("CPRP_inventory_"+monthm,con=engine, if_exists="replace")
    time_band_cprp = time_band_cprp.reset_index()
    time_band_cprp.to_sql("CPRP_timeband", con=engine, if_exists="replace")
#    print(percentage_df)
    percentage_df.to_sql("CPRP_Minutes_Distribution_"+monthm,con=engine, if_exists="replace")
    #con.close()
    
def generate_ratings_in_db(fname, mycolumns, monthm):
    #con = sqlite3.conect("channelsplan.db", timeout=20)
    ratingsheet = pd.ExcelFile(os.path.join(UPLOAD_FOLDER,fname))
    allratings = []
    timebands_consolidated = pd.read_sql_query("select * from Paid_timebands_"+monthm+";", con=engine)
    if 'index' in timebands_consolidated.columns:
        timebands_consolidated = timebands_consolidated.drop(['index'],axis=1)

    for r in channel_list_budget:
        mychanneltime = timebands_consolidated.loc[timebands_consolidated['Channel']==r,:]
        mychanneltime = mychanneltime.iloc[:,0]
        mychanneltime = pd.to_datetime(mychanneltime).dt.strftime("%H:%M")
        c = 'Periods('+r+')'
        try:
            ratings_df = ratingsheet.parse(c)
            ratings_df = ratings_df.iloc[2:,1:]
            ratings_df.columns = mycolumns
            ratings_df['Channel'] = r
            if(r == "ARY DIGITAL" or r == "PTV HOME"):
                ratings_df['Time Band'] = mychanneltime
            else:
                ratings_df['Time Band'] = tband_24hrs
            allratings.append(ratings_df)
        except XLRDError as e:
            print("No ratings found for "+r)
    all_ratings_df = pd.concat(allratings)
#    all_ratings_df = all_ratings_df.reset_index()
    all_ratings_df.to_sql("Ratingsplan_"+monthm, con=engine, if_exists="replace")
    #con.close()