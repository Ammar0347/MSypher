# -*- coding: utf-8 -*-
"""
Created on Fri May 25 14:01:59 2018

@author: Ammar.Aamir
"""
from flask import Flask, render_template, request, send_file, url_for, session
from flask_sqlalchemy import SQLAlchemy
import sqlite3
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from flask_cors import CORS
import os
import math
from tqdm import tqdm
import random
import time
import json
import msypher_utils_cloud

pd.options.display.float_format = '{:.2f}'.format

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

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

class ReverseProxied(object):
  def __init__(self, app):
      self.app = app
  def __call__(self, environ, start_response):
      script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
      if script_name:
          environ['SCRIPT_NAME'] = script_name
          path_info = environ['PATH_INFO']
          if path_info.startswith(script_name):
              environ['PATH_INFO'] = path_info[len(script_name):]
      return self.app(environ, start_response)


app = Flask(__name__)

CORS(app)
#app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///students.sqlite3'
app.wsgi_app = ReverseProxied(app.wsgi_app)
app.config['SECRET_KEY'] = "ammarbinaamir"
db = SQLAlchemy(app)

#ip = 'http://10.166.169.207:5000'
ip = 'http://10.143.71.132:8888'
monthm = "January_2019"
final_df = pd.DataFrame()
spotsonly_df = pd.DataFrame()
preprocess_df = pd.DataFrame(columns = ['Brand Name', 'Caption', 'Channel Name', 'Duration', 'Allocated Budget', 'Campaign Type', 'Launch Date', 'Specific Slot',  'Campaign Start Date', 'Campaign End Date', 'Others', 'Morning', 'Afternoon', 'Matinee', 'EPT', 'PT', 'LPT'])
rem_list = []
budget_list = []
cha_list= []
brand_list = []
launchspends_list = [] 
cyclespends_list = [] 
weekendspends_list = []
contentspends_list = [] 
spends_list = []
counter = 0 
action = ""


@app.route('/')
def basic_info():
    return render_template('login.html', mlogin=url_for("login"))

@app.route('/update_view_budget')
def viewbudget():
    d_f = msypher_utils_cloud.load_budgetsheet(monthm)
    df_columns =list(d_f.columns.values)
    return render_template('update_budgetsheet.html', dfcols = df_columns)

@app.route('/updatedbudget', methods=['GET', 'POST'])
def updatedbudget():
    data = request.get_json()
    keys = list(data[0].keys())
    df = pd.DataFrame(data, columns=keys)
    msypher_utils_cloud.save_budgetsheet(df, monthm)
    return "Got it!"

@app.route('/budget_json')
def getbudgetsheetinjson():
    d_f = msypher_utils_cloud.load_budgetsheet(monthm)
    data = { 'data': d_f.to_dict(orient='record')}
    return json.dumps(data)


@app.route('/update_view_optimizer')
def viewoptimizer():
    d_f = msypher_utils_cloud.load_paid_optimizer(monthm)
    df_columns =list(d_f.columns.values)
    return render_template('view_update_optimizer.html', dfcols = df_columns)

@app.route('/updatedoptimizer', methods=['GET', 'POST'])
def updatedoptimizer():
    data = request.get_json()
    keys = list(data[0].keys())
    df = pd.DataFrame(data, columns=keys)
    msypher_utils_cloud.save_paid_optimizer(df, monthm)
    return "Got it!"

@app.route('/optimizer_json')
def getoptimizerinjson():
    d_f = msypher_utils_cloud.load_paid_optimizer(monthm)
    d_f = d_f.fillna(0)
    data = { 'data': d_f.to_dict(orient='record')}
    return json.dumps(data)

@app.route('/update_view_foc')
def viewfoc():
    d_f = msypher_utils_cloud.load_foc_optimizer(monthm)
    df_columns =list(d_f.columns.values)
    return render_template('view_update_foc.html', dfcols = df_columns)

@app.route('/updatedfoc', methods=['GET', 'POST'])
def updatedfoc():
    data = request.get_json()
    keys = list(data[0].keys())
    df = pd.DataFrame(data, columns=keys)
    msypher_utils_cloud.save_foc_optimizer(df, monthm)
    return "Got it!"

@app.route('/foc_json')
def getfocinjson():
    d_f = msypher_utils_cloud.load_foc_optimizer(monthm)
    d_f = d_f.fillna(0)
    data = { 'data': d_f.to_dict(orient='record')}
    return json.dumps(data)

@app.route('/update_view_cprp')
def viewcprp():
    d_f = msypher_utils_cloud.load_cprp_optimizer(monthm)
    df_columns =list(d_f.columns.values)
    return render_template('view_update_cprp.html', dfcols = df_columns)

@app.route('/updatedcprp', methods=['GET', 'POST'])
def updatedcprp():
    data = request.get_json()
    keys = list(data[0].keys())
    df = pd.DataFrame(data, columns=keys)
    msypher_utils_cloud.save_cprp_optimizer(df, monthm)
    return "Got it!"

@app.route('/cprp_json')
def getcprpinjson():
    d_f = msypher_utils_cloud.load_cprp_optimizer(monthm)
    d_f = d_f.fillna(0)
    data = { 'data': d_f.to_dict(orient='record')}
    return json.dumps(data)

@app.route('/output')
def output():
#    list_of_files = glob.glob('Plans/With_Inventory/*') # * means all if need specific format then *.csv
#    latest_file = max(list_of_files, key=os.path.getctime)
#    d_f = pd.read_excel(latest_file)
    d_f = msypher_utils_cloud.load_spots_with_inventory(monthm)
    d_f = d_f.drop(['index'], axis=1)
    
    channels = d_f['Channel'].unique().tolist()
    brands = d_f['Brand'].unique().tolist()
    return render_template('summary.html',
                           ip = ip,
                           channels= channels, 
                           brands= brands, 
                           table= d_f.to_html(classes = 'table dataTable no-footer" id = "data-table" style = "display:block;overflow:auto', border=0, index=False))

@app.route('/withoutinventory')
def output1():
#    list_of_files = glob.glob('Plans/Only_Spots/*') # * means all if need specific format then *.csv
#    latest_file = max(list_of_files, key=os.path.getctime)
#    d_f = pd.read_excel(latest_file)
    d_f = msypher_utils_cloud.load_spots_only(monthm)
    if 'index' in d_f.columns:
        d_f = d_f.drop(['index'], axis=1)

    channels = d_f['Channel'].unique().tolist()
    brands = d_f['Brand'].unique().tolist()
    d_f = d_f[d_f.Brand.isin(session['brands'])]
    return render_template('summary1.html',
                           ip = ip,
                           channels= channels,
                           brands= brands, 
                           table= d_f.to_html(classes = 'table dataTable no-footer" id = "data-table" style = "display:block;overflow:auto', border=0, index=False))

@app.route('/channelwisesummary')
def output2():
#    list_of_files = glob.glob('Plans/Only_Spots/*') # * means all if need specific format then *.csv
#    latest_file = max(list_of_files, key=os.path.getctime)
#    d_f = pd.read_excel(latest_file)
    
    d_f = msypher_utils_cloud.load_spots_only(monthm)
    d_f = d_f.drop(['index'], axis=1)
    d_f = d_f[d_f.Brand.isin(session['brands'])]
    summary_df = msypher_utils_cloud.generate_channel_summary(d_f)
    channels = d_f['Channel'].unique().tolist()
    brands = d_f['Brand'].unique().tolist()
    
    return render_template('channelwise_summary.html', 
                           ip = ip,
                           channels= channels, 
                           brands= brands, 
                           table= summary_df.to_html(classes = 'table dataTable no-footer" id = "data-table" style = "display:block;overflow:auto', border=0, index=False))

@app.route('/convertedspots')
def output3_1():
#    conn = sqlite3.connect("channelsplan.db")  
    d_f = msypher_utils_cloud.load_converted_spots(monthm)#pd.read_sql_query("select * from Test_Convertor_January_Updated;", conn)
    if 'index' in d_f.columns:
        d_f = d_f.drop(['index'], axis=1)
    mydf = pd.DataFrame()
    channels = d_f['Channel'].unique().tolist()
    brands = d_f['Brand'].unique().tolist()
    
    mydf = d_f[d_f.Brand == brands[0]]
    mydf.columns = ['Date', 'Channels', 'Start Time', 'Length', 'Cost', 'Title']
    mydf.loc[:,'Cost'] = 1
    return render_template('convertedspots_summary.html', 
                           ip = ip,
                           channels= channels, 
                           brands= brands, 
                           table= mydf.to_html(classes = 'table dataTable no-footer" id = "data-table" style = "display:block;overflow:auto', border=0, index=False))

@app.route('/convertedspots1', methods = ['GET','POST'])
def output3():
    if request.method == 'POST':
#        conn = sqlite3.connect("channelsplan.db")    
#        d_f = pd.read_sql_query("select * from Test_Convertor_January_Updated;", con=engine)
        d_f = msypher_utils_cloud.load_converted_spots(monthm)
        if 'index' in d_f.columns:
            d_f = d_f.drop(['index'], axis=1)
        ccode = pd.ExcelFile('Channel Codes.xlsx')
        ccodes = ccode.parse(ccode.sheet_names[0], header=None)
        code = ccodes.iloc[:,0].str.split(",").tolist()
        codes = [x[0] for x in code]
        cnames = [x[1] for x in code]
        brand = request.form.get('brand')
        mydf = pd.DataFrame()
        mydf = d_f[d_f.Brand == brand]
        mydf.columns = ['Date', 'Channels', 'Start Time', 'Length', 'Cost', 'Title']
        mydf.Channels = [x.replace("NEO", "NEO TV") for x in mydf.Channels]
        mydf.Channels = [x.replace("EXPRESS ENT", "EXPRESS ENTERTAINMENT") for x in mydf.Channels]
        mydf.Channels = [x.replace("ARUJ","ARUJ TV") for x in mydf.Channels]
        mydf.Channels = [ch.replace("ABB TAKK NEWS","ABB TAKK") for ch in mydf.Channels]
        mydf.Channels = [ch.replace("H NOW","HEALTH TV") for ch in mydf.Channels]
        mydf.Channels = [ch.replace("24 NEWS","CHANNEL 24") for ch in mydf.Channels]
        for i in range(len(cnames)):
            mydf.Channels.loc[mydf.Channels == cnames[i]] = codes[i]
        mydf.loc[:,'Channels'] = mydf['Channels'].apply(lambda x: '{0:0>4}'.format(x))
        mydf.loc[:,'Cost'] = '0000000000' 
        a = pd.to_datetime(mydf.Date)
        mydf.loc[:, 'Date'] = a.dt.strftime("%Y%m%d")
        a = mydf['Start Time'].values
        xx = [x.astype('datetime64') for x in a]
        xx = pd.to_datetime(xx)
        xx = xx.time
        xlist = [str(b) for b in xx.tolist()]
        mydf.loc[:,'Start Time'] = [x.replace(":","") for x in xlist]
        a = mydf['Length']
        mydf.loc[:, 'Length'] = mydf['Length'].apply(lambda x: '{0:0>5}'.format(int(x)))
        mydf.to_csv(brand+'.txt', header=None, index=None, sep=' ', mode='w')
#        f= open(brand+".txt","w+")
#        f.close()
        return send_file(brand+'.txt', as_attachment=True)

@app.route('/uploader', methods = ['GET','POST'])
def uploader():
    if request.method == 'POST':
        msypher_utils_cloud.save_brand_splits()
        startdt = pd.to_datetime(request.form['startdate'])
        enddt = pd.to_datetime(request.form['enddate'])
        mycolumns = []
        for dt in daterange(startdt, enddt):
            mycolumns.append(dt.strftime("%a_%e-%b"))
        for f in request.files.getlist('file'):
            UPLOAD_FOLDER = 'processthesesheets/'
            if not os.path.exists(UPLOAD_FOLDER+'/'):
                os.makedirs(UPLOAD_FOLDER)
            f.save(os.path.join(UPLOAD_FOLDER,f.filename))
            if(f.filename.split(" ")[1] == "Budget"):
                msypher_utils_cloud.generate_budgetsheet_in_db(f.filename,
                                                         monthm)

            if(f.filename.split(" ")[1] == "Investment"):
                msypher_utils_cloud.generate_paid_optimizer_in_db(f.filename,
                                                            monthm,
                                                            startdt,
                                                            enddt)
                
            if(f.filename.split(" ")[0] == "FOC"):
                msypher_utils_cloud.generate_foc_optimizer_in_db(f.filename,
                                                           monthm,
                                                           startdt,
                                                           enddt)

            if(f.filename.split(" ")[0] == "CPRP"):
                msypher_utils_cloud.generate_cprp_optimizer_in_db(f.filename,
                                                            monthm,
                                                            startdt,
                                                            enddt)
            
            if(f.filename.split(" ")[1] == "Data"):
                msypher_utils_cloud.generate_ratings_in_db(f.filename,
                                                     mycolumns,
                                                     monthm)
        return "File uploaded successfully"

@app.route('/login', methods = ['GET','POST'])
def login():
    if request.method == 'POST':
        user = request.form['log']
        pwd = request.form['pwd']
        global role
        infodf = msypher_utils_cloud.load_user(user,pwd)
        brands_index = infodf.brands.values[0].split(',')
        brands_index = list(map(int, brands_index))
        session['role'] = infodf.role.values[0]
        session['user'] = infodf.username.values[0]
        if(session['role']=="Planner" or session['role']=="Admin"):
            splits_df = msypher_utils_cloud.load_splits()
            session['brands'] = splits_df.Brands[splits_df.index.isin(brands_index)].tolist()

            return render_template('index.html',
                                   ip = ip,
                                   username=session['user'],
                                   role=session['role'],
                                   splits=session['brands'])
        elif(session['role']=="Buying"):
            return render_template('upload.html', username=session['user'], ip = ip)
        else:
            return render_template('login.html', ip = ip)

@app.route('/initializer', methods = ['GET', 'POST'])
def initializer():
    global preprocess_df
    if request.method == 'POST':
        global action
        action = request.form.get('action')
        brand = request.form.get('brand')
        caption = request.form['caption']

        splits_df = msypher_utils_cloud.load_brand_splits()

        paid_df_consolidated = msypher_utils_cloud.load_paid_optimizer(monthm)
        cprp_df_consolidated = msypher_utils_cloud.load_cprp_optimizer(monthm)
        foc_df_consolidated = msypher_utils_cloud.load_foc_optimizer(monthm)

        budget_df = msypher_utils_cloud.load_budgetsheet(monthm)
#        chn = budget_df.loc[:,'Channel Name'][pd.notna(budget_df.loc[:,brand])]
#        chn = chn.iloc[chn.nonzero()[0]]
        chn = budget_df['Channel Name'][budget_df[brand] != 0]
        
        if(action == "Add" or action == "Update"):
            duration = float(request.form['duration'])
            plantype = request.form.get('plantype')
            startdt = pd.to_datetime(request.form['startdate'])
            mydt = startdt.replace(day=1)
            enddt = pd.to_datetime(request.form['enddate'])
            budgetsplit = float(request.form['budgetsplit'])/100
            launchdate = startdt
            if(plantype == "platinum" or plantype == "diamond" or plantype == "gold"
               or plantype == "silver"):
                launchdate = pd.to_datetime(request.form['launchdate'])
            content = '00:00'
            if(plantype == "content"):
                content = request.form['usr_time']
            
            plans = ["Paid", "CPRP", "FOC"]
            
            for plan in plans:
                mydf = pd.DataFrame(columns = ['Brand Name', 'Caption', 'Channel Name', 'Duration', 'Allocated Budget', 'Campaign Type', 'Launch Date', 'Specific Slot', 'Campaign Start Date', 'Campaign End Date', 'Others', 'Morning', 'Afternoon', 'Matinee', 'EPT', 'PT', 'LPT'])
                channel_select = chn.tolist()
#                channel_select = chn.iloc[:,0].tolist()
                if(plan=="Paid"):
                    # Removing channels on which inventory isn't available.
                    paid_channels = paid_df_consolidated.Channel.unique().tolist()
                    channel_select = [x for x in channel_select if x in paid_channels]
                elif(plan =="CPRP"):
                    # Removing channels on which inventory isn't available.
                    cprp_channels = cprp_df_consolidated.Channel.unique().tolist()
                    channel_select = [x for x in channel_select if x in cprp_channels]
                elif(plan == "FOC"):
                    # Removing channels on which inventory isn't available.
                    foc_channels = foc_df_consolidated.Channel.unique().tolist()
                    channel_select = [x for x in channel_select if x in foc_channels]
                print(channel_select)
                for i in range(len(channel_select)):
                    if(budget_df.loc[budget_df['Channel Name'] == channel_select[i],brand] is not None):
                        copybudget = budget_df.loc[budget_df['Channel Name'] == channel_select[i],brand] * budgetsplit
                        mydf = mydf.append({
                                'Allocated Budget': copybudget,
                                'Channel Name': channel_select[i],
                                'Others':splits_df.loc[brand, 'Others'],
                                'Morning':splits_df.loc[brand, 'Morning'],
                                'Afternoon':splits_df.loc[brand, 'Afternoon'],
                                'Matinee':splits_df.loc[brand, 'Matinee'],
                                'EPT':splits_df.loc[brand, 'EPT'],
                                'PT':splits_df.loc[brand, 'PT'],
                                'LPT':splits_df.loc[brand, 'LPT']
                                }, ignore_index=True)
#                        mydf.loc[i, 'Allocated Budget'] = copybudget
#                        mydf.loc[i, 'Channel Name'] = channel_select[i]        
#                        others = splits_df.loc[brand, 'Others']
#                        morning = splits_df.loc[brand, 'Morning']
#                        afternoon = splits_df.loc[brand, 'Afternoon']
#                        matinee = splits_df.loc[brand, 'Matinee']
#                        ept = splits_df.loc[brand, 'EPT']
#                        pt = splits_df.loc[brand, 'PT']
#                        lpt = splits_df.loc[brand, 'LPT']
#                        mydf.loc[i,'Others'] = others
#                        mydf.loc[i,'Morning'] = morning
#                        mydf.loc[i,'Afternoon'] = afternoon
#                        mydf.loc[i,'Matinee'] = matinee
#                        mydf.loc[i,'EPT'] = ept
#                        mydf.loc[i,'PT'] = pt
#                        mydf.loc[i,'LPT'] = lpt
                mydf['Plan'] = plan
                mydf['Brand Name'] = brand
                mydf['Budget Split'] = budgetsplit
                mydf['Caption'] = caption
                mydf['Duration'] = duration
                mydf['Campaign Type'] = plantype
                mydf['Launch Date'] = launchdate
                mydf['Campaign Start Date'] = startdt
                mydf['Campaign End Date'] = enddt
                mydf['mydate'] = mydt
                mydf['Specific Slot'] = content
                mydf = mydf[pd.notna(mydf['Allocated Budget'])]
                if(preprocess_df.empty):
                    preprocess_df = pd.DataFrame(columns=mydf.columns)
                preprocess_df = preprocess_df.append(mydf)
                preprocess_df = preprocess_df.reset_index(drop=True)
                preprocess_df['Campaign Type'] = pd.Categorical(preprocess_df['Campaign Type'], 
                                                 ["platinum", "diamond", "gold", "silver", 
                                                  "hawkeye", "cycle", "content", "effective",
                                                  "focspendwise", "cprpspendwise"])
                preprocess_df = preprocess_df.sort_values("Campaign Type")
        
#            print(preprocess_df)
            # Removing Channels on which Brand is not spending
#            preprocess_df = preprocess_df[preprocess_df['Allocated Budget'] != 0]
            global counter
            counter+=1
        if(action=="Delete"):            
            preprocess_df = preprocess_df[preprocess_df['Brand Name'] != brand]
            print(preprocess_df)
#        if(action=="Update"):
#            plantype = request.form.get('plantype')
#            duration = float(request.form['duration'])
#            
#            preprocess_df['Campaign Type'][preprocess_df['Caption'] == caption] = plantype 
#            preprocess_df['Duration'][preprocess_df['Caption'] == caption] = duration
        preprocess_df["Brand Name"] = preprocess_df["Brand Name"].astype(str)
        preprocess_df["Caption"] = preprocess_df["Caption"].astype(str)
        preprocess_df["Channel Name"] = preprocess_df["Channel Name"].astype(str)
        preprocess_df["Plan"] = preprocess_df["Plan"].astype(str)
        preprocess_df["Specific Slot"] = preprocess_df["Specific Slot"].astype(str)
        preprocess_df.Duration = preprocess_df.Duration.astype(np.float32)
        preprocess_df.Others = preprocess_df.Others.astype(np.float32)
        preprocess_df.Morning = preprocess_df.Morning.astype(np.float32)
        preprocess_df.Afternoon = preprocess_df.Afternoon.astype(np.float32)
        preprocess_df.Matinee = preprocess_df.Matinee.astype(np.float32)
        preprocess_df.EPT = preprocess_df.EPT.astype(np.float32)
        preprocess_df.PT = preprocess_df.PT.astype(np.float32)
        preprocess_df.LPT = preprocess_df.LPT.astype(np.float32)
        preprocess_df['Budget Split'] = preprocess_df['Budget Split'].astype(np.float32)
        preprocess_df['Allocated Budget'] = preprocess_df['Allocated Budget'].astype(np.float32)
#        
        
        msypher_utils_cloud.save_entries(preprocess_df, monthm)
        percent_progress = (counter/(55))*100
        return render_template('index.html',
                               brand=brand,
                               ip = ip,
                               splits=splits_df.index.tolist(),
                               role=role,
                               counter=counter,
                               progress = percent_progress,
                               table = preprocess_df.to_html(classes = 'table dataTable no-footer" id = "data-table" style = "display:block;overflow:auto', na_rep = "", index = False, border=0))

@app.route('/processit', methods = ['GET', 'POST'])
def process_file():
    global invplan_df, final_df, spotsonly_df
    global remaininglaunchbudget

    preprocess_df = msypher_utils_cloud.load_entries(monthm)
    
    invplan_df_consolidated = msypher_utils_cloud.load_paid_optimizer(monthm)

    rates_df_consolidated = msypher_utils_cloud.load_rates(monthm)

    budget_df = msypher_utils_cloud.load_budgetsheet(monthm)

    foc_df_consolidated = msypher_utils_cloud.load_foc_optimizer(monthm)
    
    cprp_df_consolidated = msypher_utils_cloud.load_cprp_optimizer(monthm)

    ratings_df_consolidated = msypher_utils_cloud.load_ratings(monthm)

    timebands_consolidated = msypher_utils_cloud.load_timebands(monthm)
    if request.method == 'POST':
        MAX_SPOTS_PER_HOUR = 4
        for index, r in preprocess_df.iterrows():
            plan = r['Plan']
            channel_select = r['Channel Name']
            c = channel_select.replace("_", " ")
            print(c)
            if((len(invplan_df_consolidated.loc[invplan_df_consolidated['Channel']==c, :])==0) & (len(foc_df_consolidated.loc[foc_df_consolidated['Channel']==c, :])==0) & (len(cprp_df_consolidated.loc[cprp_df_consolidated['Channel']==c, :])==0)):
                continue

            brand = r['Brand Name']
            caption = r['Caption']
            duration = float(r['Duration'])
            dur_min = duration/60
            
            r['Others'] = float(r['Others'])
            r['Morning'] = float(r['Morning'])
            r['Afternoon'] = float(r['Afternoon'])
            r['Matinee'] = float(r['Matinee'])
            r['EPT'] = float(r['EPT'])
            r['PT'] = float(r['PT'])
            r['LPT'] = float(r['LPT'])
            
            channel_budget = budget_df.loc[budget_df["Channel Name"]==c, brand].sum()
            totalbudget = float(r['Allocated Budget'])
            authorized_budget = totalbudget
            start_dt = pd.to_datetime(r['Campaign Start Date'])
            end_dt = pd.to_datetime(r['Campaign End Date'])
            my_dt = pd.to_datetime(r['mydate'])
            budgetsplit = float(r['Budget Split'])
            
            if(plan == "FOC"):
                plantype = "focspendwise"
            elif(plan == "CPRP"):
                plantype = "cprpspendwise"
            else:
                plantype = r['Campaign Type']
            try:
                cprp_channels = [ "ARY_ZINDAGI",	"PLAY_ENTERTAINMENT",	"EXPRESS_ENTERTAINMENT","GEO_KAHANI",	"FILMAZIA",	"HNOW"	,"EXPRESS_NEWS"	,"CAPITAL_TV" ,"AAJ_NEWS"	,"NEO_TV",	"TWENTYFOUR_NEWS"	,"SEVEN_NEWS"	 ,"NICKELODEON"	,"AAJ_ENTERTAINMENT"	,"SILVER_SCREEN"]
                if(channel_select not in cprp_channels):
                    plan_df = pd.DataFrame(index= invplan_df_consolidated.loc[invplan_df_consolidated['Channel']==c, :].index, columns=invplan_df_consolidated.columns[1:])
                    plan_df = plan_df.fillna(0)
                    spends_df = pd.DataFrame(index= invplan_df_consolidated.loc[invplan_df_consolidated['Channel']==c, :].index, columns=invplan_df_consolidated.columns[1:])
                    spends_df = spends_df.fillna(0)
                    tband = timebands_consolidated[timebands_consolidated.Channel == c].iloc[:,0]#.tolist()
                    tband = [str(x).split(" ")[2] for x in tband]
                    tband = pd.to_datetime(tband).strftime("%H:%M").tolist()
                    
                mycolumns = []
                weekends = []
                campaign_dates = []
                for dt in daterange(start_dt, end_dt):
                    campaign_dates.append(dt.strftime("%a_%e-%b"))
                    a = dt.weekday()
                    if(a >= 5):
                        weekends.append(dt.strftime("%a_%e-%b"))
                for dt in daterange(my_dt, end_dt):
                    mycolumns.append(dt.strftime("%a_%e-%b"))

                cycle_first = mycolumns[:8]
                cycle_last = mycolumns[:-8:-1]
                cycle = cycle_first + cycle_last
                
                
                launchspends = 0
                weekendspends = 0
                cyclespends = 0
                contentspends = 0
                launchsplit = 0
        ############################################################################################
        ############################################################################################
        ############################################################################################
        ######################################## LAUNCH ################################################        
                if(plantype=="platinum" or plantype=="diamond" or plantype=="gold" or plantype=="silver"):
                    if(plantype=="platinum"):
                        d = 6
                        launchsplit = 40
                    elif(plantype=="diamond"):
                        d = 4
                        launchsplit = 30
                    elif(plantype=="gold"):
                        d = 2
                        launchsplit = 20
                    elif(plantype=="silver"):
                        d = 1
                        launchsplit = 20
                    launchbudget = totalbudget*(launchsplit/100) #* (float(request.form['launchsplit'])/100)
                    totalbudget-=launchbudget
                    others = launchbudget*r['Others']#launchbudget * (float(request.form['others'])/100)
                    morning = launchbudget*r['Morning']#launchbudget * (float(request.form['morning'])/100)
                    afternoon = launchbudget*r['Afternoon']#launchbudget * (float(request.form['afternoon'])/100)
                    matinee = launchbudget*r['Matinee']#launchbudget * (float(request.form['matinee'])/100)
                    ept = launchbudget*r['EPT']#launchbudget * (float(request.form['ept'])/100)
                    pt = launchbudget*r['PT']#launchbudget * (float(request.form['pt'])/100)
                    lpt = launchbudget*r['LPT']#launchbudget * (float(request.form['lpt'])/100)
                    
                    remaininglaunchbudget = launchbudget

                    launchdatefrom = pd.to_datetime(r['Launch Date'])#pd.to_datetime(request.form['launchdate'])
                    launchdateto = launchdatefrom+pd.Timedelta(days=d)
                    launchdates = []
                    for dt in daterange(launchdatefrom, launchdateto):
                        launchdates.append(dt.strftime("%a_%e-%b"))
                        
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
                              if(plan_df.columns[col] in launchdates):
                                  for row in range(8,12):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
                                              launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
                                  for row in range(12,15):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
                                              launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
                                  for row in range(15,18):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Matinee Budget: " +str(matinee))
                                  for row in range(18,20):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining EPT Budget: " +str(ept))
                                  for row in range(20,22):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining PT Budget: " +str(pt))
                                  for row in range(22,len(tband)):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining LPT Budget: " +str(lpt))
                                  for row in range(0,8):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1

                    for col in range(0, len(plan_df.columns)):
                        if(plan_df.columns[col] in campaign_dates):
                            if(plan_df.columns[col] in launchdates):
                                for row in range(0,len(tband)):
                                    if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                       and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                        if(remaininglaunchbudget>= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                            plan_df.iloc[row,col]+=1
                                            invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                            invplan_df.iloc[row,col] -= dur_min
                                            launchspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            remaininglaunchbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
#                        print("Remaining Launch Budget: " +str(remaininglaunchbudget))
                    
                    
                    others = totalbudget * r['Others']#(float(request.form['others'])/100)
                    morning = totalbudget * r['Morning']#(float(request.form['morning'])/100)
                    afternoon = totalbudget * r['Afternoon']#(float(request.form['afternoon'])/100)
                    matinee = totalbudget * r['Matinee']#(float(request.form['matinee'])/100)
                    ept = totalbudget * r['EPT']#(float(request.form['ept'])/100)
                    pt = totalbudget * r['PT']#(float(request.form['pt'])/100)
                    lpt = totalbudget * r['LPT']#(float(request.form['lpt'])/100)
                    spends = 0
                    remainingbudget = remaininglaunchbudget+totalbudget
                    
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
#                              print("Day: "+str(col))
                              if(pd.to_datetime(plan_df.columns[col], format="%a_%d-%b") > pd.to_datetime(launchdates[-1], format= "%a_%d-%b")):
                                  for row in range(8,12):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Morning Budget: " +str(morning))
                                  for row in range(12,15):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Afternoon Budget: " +str(afternoon))            
                                  for row in range(15,18):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Matinee Budget: " +str(matinee))
                                  for row in range(18,20):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining EPT Budget: " +str(ept))
                                  for row in range(20,22):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining PT Budget: " +str(pt))
                                  for row in range(22,len(tband)):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining LPT Budget: " +str(lpt))
                                  for row in range(0,8):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
                    
                    remainingbudget = others+morning+afternoon+matinee+ept+pt+lpt
                    times=0
                    while(times!=15):
                        for col in range(len(plan_df.columns)-1, -1,-1):
                            if(plan_df.columns[col] in campaign_dates):
                                if(pd.to_datetime(plan_df.columns[col], format="%a_%d-%b") > pd.to_datetime(launchdates[-1], format= "%a_%d-%b")):
                                    for row in range(0,len(tband)):
                                        if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                           and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                            if(remainingbudget >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                                plan_df.iloc[row,col]+=1
                                                invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                                invplan_df.iloc[row,col] -= dur_min
                                                spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                remainingbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Budget: " +str(remainingbudget))
        ######################################## LAUNCH ############################################
        ############################################################################################
        ############################################################################################
        ############################################################################################
        ################################### EFFECTIVE VISIBILITY ###################################
                elif(plantype == "effective"):
                    others = totalbudget * r['Others']#(float(request.form['others'])/100)
                    morning = totalbudget * r['Morning']#(float(request.form['morning'])/100)
                    afternoon = totalbudget * r['Afternoon']#(float(request.form['afternoon'])/100)
                    matinee = totalbudget * r['Matinee']#(float(request.form['matinee'])/100)
                    ept = totalbudget * r['EPT']#(float(request.form['ept'])/100)
                    pt = totalbudget * r['PT']#(float(request.form['pt'])/100)
                    lpt = totalbudget * r['LPT']#(float(request.form['lpt'])/100)
                    spends = 0
                    remainingbudget = totalbudget
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
#                              print("Day: "+str(col))
                              
                              for row in range(8,12):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1                              
#                              print("Remaining Morning Budget: " +str(morning))
                              for row in range(12,15):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining Afternoon Budget: " +str(afternoon))            
                              for row in range(15,18):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining Matinee Budget: " +str(matinee))
                              for row in range(18,20):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining EPT Budget: " +str(ept))
                              for row in range(20,22):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining PT Budget: " +str(pt))
                              for row in range(22,len(tband)):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining LPT Budget: " +str(lpt))
                              for row in range(0,8):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
                    
                    remainingbudget = others+morning+afternoon+matinee+ept+pt+lpt
#                    print("Remaining Budget: " +str(remainingbudget))  
                    times = 0 
                    while(times != 15):
                        for col in range(len(plan_df.columns)-1, -1,-1):
                            if(plan_df.columns[col] in campaign_dates):
                                for row in range(0,len(tband)):
                                    if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min
                                       and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                        if(remainingbudget >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                            plan_df.iloc[row,col]+=1
                                            invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                            invplan_df.iloc[row,col] -= dur_min
                                            spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            remainingbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Budget: " +str(remainingbudget))
        ################################### EFFECTIVE VISIBILITY ###################################
        ############################################################################################
        ############################################################################################
        ############################################################################################
        ##################################### HAWKEYE WEEKEND ######################################
                if(plantype=="hawkeye"):
                    weekendbudget = totalbudget * 0.40#(float(request.form['launchsplit'])/100)
                    totalbudget-=weekendbudget
                    others = weekendbudget * r['Others']#(float(request.form['others'])/100)
                    morning = weekendbudget * r['Morning']#(float(request.form['morning'])/100)
                    afternoon = weekendbudget * r['Afternoon']#(float(request.form['afternoon'])/100)
                    matinee = weekendbudget * r['Matinee']#(float(request.form['matinee'])/100)
                    ept = weekendbudget * r['EPT']#(float(request.form['ept'])/100)
                    pt = weekendbudget * r['PT']#(float(request.form['pt'])/100)
                    lpt = weekendbudget * r['LPT']#(float(request.form['lpt'])/100)
                    
                    remaininglaunchbudget = weekendbudget
                        
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
#                              print("Day: "+str(col))
                              if(plan_df.columns[col] in weekends):
                                  for row in range(8,12):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Morning Budget: " +str(morning))
                                  for row in range(12,15):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Afternoon Budget: " +str(afternoon))            
                                  for row in range(15,18):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Matinee Budget: " +str(matinee))
                                  for row in range(18,20):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining EPT Budget: " +str(ept))
                                  for row in range(20,22):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining PT Budget: " +str(pt))
                                  for row in range(22,len(tband)):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining LPT Budget: " +str(lpt))
                                  for row in range(0,8):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
                    
#                    print("Remaining Weekend Budget: " +str(remaininglaunchbudget))
                    times=0
                    while(times!=15):
                        for col in range(len(plan_df.columns)-1, -1,-1):
                            if(plan_df.columns[col] in campaign_dates):
                                if(plan_df.columns[col] in weekends):
                                    for row in range(0,len(tband)):
                                        if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                           and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                            if(remaininglaunchbudget>= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                                plan_df.iloc[row,col]+=1
                                                invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                                invplan_df.iloc[row,col] -= dur_min
                                                weekendspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                remaininglaunchbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Weekend Budget: " +str(remaininglaunchbudget))
                
                    others = totalbudget * r['Others']#(float(request.form['others'])/100)
                    morning = totalbudget * r['Morning']#(float(request.form['morning'])/100)
                    afternoon = totalbudget * r['Afternoon']#(float(request.form['afternoon'])/100)
                    matinee = totalbudget * r['Matinee']#(float(request.form['matinee'])/100)
                    ept = totalbudget * r['EPT']#(float(request.form['ept'])/100)
                    pt = totalbudget * r['PT']#(float(request.form['pt'])/100)
                    lpt = totalbudget * r['LPT']#(float(request.form['lpt'])/100)
                    spends = 0
                    remainingbudget = remaininglaunchbudget+totalbudget
                    
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
#                              print("Day: "+str(col))
                              if(plan_df.columns[col] not in weekends):
                                  for row in range(8,12):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Morning Budget: " +str(morning))
                                  for row in range(12,15):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Afternoon Budget: " +str(afternoon))            
                                  for row in range(15,18):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Matinee Budget: " +str(matinee))
                                  for row in range(18,20):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining EPT Budget: " +str(ept))
                                  for row in range(20,22):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining PT Budget: " +str(pt))
                                  for row in range(22,len(tband)):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining LPT Budget: " +str(lpt))
                                  for row in range(0,8):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
                    
                    remainingbudget = others+morning+afternoon+matinee+ept+pt+lpt
#                    print("Remaining Budget: " +str(remainingbudget))  
                    times=0   
                    while(times!=15):
                        for col in range(len(plan_df.columns)-1, -1,-1):
                            if(plan_df.columns[col] in campaign_dates):
                                if(plan_df.columns[col] not in weekends):
                                    for row in range(0,len(tband)):
                                        if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                           and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                            if(remainingbudget >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                                plan_df.iloc[row,col]+=1
                                                invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                                invplan_df.iloc[row,col] -= dur_min
                                                spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                remainingbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Budget: " +str(remainingbudget))
        ##################################### HAWKEYE WEEKEND ######################################
        ############################################################################################
        ############################################################################################
        ############################################################################################
        ##################################### BUYING CYCLE #########################################
                elif(plantype=="cycle"):
                    cyclebudget = totalbudget * 0.60#(float(request.form['launchsplit'])/100)
                    totalbudget-=cyclebudget
                    others = cyclebudget * r['Others']#(float(request.form['others'])/100)
                    morning = cyclebudget * r['Morning']#(float(request.form['morning'])/100)
                    afternoon = cyclebudget * r['Afternoon']#(float(request.form['afternoon'])/100)
                    matinee = cyclebudget * r['Matinee']#(float(request.form['matinee'])/100)
                    ept = cyclebudget * r['EPT']#(float(request.form['ept'])/100)
                    pt = cyclebudget * r['PT']#(float(request.form['pt'])/100)
                    lpt = cyclebudget * r['LPT']#(float(request.form['lpt'])/100)
                    
                    remaininglaunchbudget = cyclebudget
                        
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
#                              print("Day: "+str(col))
                              if(plan_df.columns[col] in cycle):
                                  for row in range(8,12):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Morning Budget: " +str(morning))
                                  for row in range(12,15):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Afternoon Budget: " +str(afternoon))            
                                  for row in range(15,18):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Matinee Budget: " +str(matinee))
                                  for row in range(18,20):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining EPT Budget: " +str(ept))
                                  for row in range(20,22):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining PT Budget: " +str(pt))
                                  for row in range(22,len(tband)):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining LPT Budget: " +str(lpt))
                                  for row in range(0,8):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remaininglaunchbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
                    
#                    print("Remaining Buying Cycle Budget: " +str(remaininglaunchbudget))
                    times=0
                    while(times!=15):
                        for col in range(len(plan_df.columns)-1, -1,-1):
                            if(plan_df.columns[col] in campaign_dates):
                                if(plan_df.columns[col] in cycle):
                                    for row in range(0,len(tband)):
                                        if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                           and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                            if(remaininglaunchbudget>= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                                plan_df.iloc[row,col]+=1
                                                invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                                invplan_df.iloc[row,col] -= dur_min
                                                cyclespends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                remaininglaunchbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Buying Cycle Budget: " +str(remaininglaunchbudget))
                
                    others = totalbudget * r['Others']#(float(request.form['others'])/100)
                    morning = totalbudget * r['Morning']#(float(request.form['morning'])/100)
                    afternoon = totalbudget * r['Afternoon']#(float(request.form['afternoon'])/100)
                    matinee = totalbudget * r['Matinee']#(float(request.form['matinee'])/100)
                    ept = totalbudget * r['EPT']#(float(request.form['ept'])/100)
                    pt = totalbudget * r['PT']#(float(request.form['pt'])/100)
                    lpt = totalbudget * r['LPT']#(float(request.form['lpt'])/100)
                    spends = 0
                    remainingbudget = remaininglaunchbudget+totalbudget
                    
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
#                              print("Day: "+str(col))
                              if(plan_df.columns[col] not in cycle):
                                  for row in range(8,12):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Morning Budget: " +str(morning))
                                  for row in range(12,15):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Afternoon Budget: " +str(afternoon))            
                                  for row in range(15,18):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining Matinee Budget: " +str(matinee))
                                  for row in range(18,20):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining EPT Budget: " +str(ept))
                                  for row in range(20,22):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining PT Budget: " +str(pt))
                                  for row in range(22,len(tband)):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
#                                  print("Remaining LPT Budget: " +str(lpt))
                                  for row in range(0,8):
                                      if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                         and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                          if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                              plan_df.iloc[row,col]+=1
                                              invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                              invplan_df.iloc[row,col] -= dur_min
                                              spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                              spots+=1
                    
                    remainingbudget = others+morning+afternoon+matinee+ept+pt+lpt
#                    print("Remaining Budget: " +str(remainingbudget))  
                    times=0
                    while(times!=15):
                        for col in range(len(plan_df.columns)-1, -1,-1):
                            if(plan_df.columns[col] in campaign_dates):
                                if(plan_df.columns[col] not in cycle):
                                    for row in range(0,len(tband)):
                                        if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                           and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                            if(remainingbudget >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                                plan_df.iloc[row,col]+=1
                                                invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                                invplan_df.iloc[row,col] -= dur_min
                                                spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                remainingbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Budget: " +str(remainingbudget))
        ##################################### BUYING CYCLE #########################################
        ############################################################################################
        ############################################################################################
        ############################################################################################        
        #################################### CONTENT SPECIFIC #####################################
                elif(plantype=="content"):
                    timemy = r['Specific Slot']#request.form['usr_time']
                    timemyy = pd.to_datetime(timemy)
                    timem = timemyy.strftime("%H:%M:%S.%f")
                    contentbudget = totalbudget * 0.20#(float(request.form['launchsplit'])/100)
                    totalbudget-=contentbudget    
                    remaininglaunchbudget = contentbudget
                    times=0
                    while(times!=15):
                        for row in range(0,len(tband)):
                            if(timem == tband[row]):
                                for col in range(0,len(plan_df.columns)):
                                    if(plan_df.columns[col] in campaign_dates):
                                        if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                           and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                            if(remaininglaunchbudget>= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                                plan_df.iloc[row,col]+=1
                                                invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                                invplan_df.iloc[row,col] -= dur_min
                                                contentspends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                                remaininglaunchbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Content Specific Budget: " +str(remaininglaunchbudget))
                
                    others = totalbudget * r['Others']#(float(request.form['others'])/100)
                    morning = totalbudget * r['Morning']#(float(request.form['morning'])/100)
                    afternoon = totalbudget * r['Afternoon']#(float(request.form['afternoon'])/100)
                    matinee = totalbudget * r['Matinee']#(float(request.form['matinee'])/100)
                    ept = totalbudget * r['EPT']#(float(request.form['ept'])/100)
                    pt = totalbudget * r['PT']#(float(request.form['pt'])/100)
                    lpt = totalbudget *r['LPT']# (float(request.form['lpt'])/100)
                    spends = 0
                    remainingbudget = remaininglaunchbudget+totalbudget
                    
                    for col in range(0, len(plan_df.columns)):
                          if(plan_df.columns[col] in campaign_dates):
                              spots = 0
#                              print("Day: "+str(col))
                              for row in range(8,12):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(morning >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          morning-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining Morning Budget: " +str(morning))
                              for row in range(12,15):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(afternoon >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          afternoon-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining Afternoon Budget: " +str(afternoon))            
                              for row in range(15,18):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(matinee >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          matinee-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining Matinee Budget: " +str(matinee))
                              for row in range(18,20):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(ept >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          ept-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining EPT Budget: " +str(ept))
                              for row in range(20,22):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(pt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          pt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
#                              print("Remaining PT Budget: " +str(pt))
                              for row in range(22,len(tband)):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(lpt >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          lpt-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
                              print("Remaining LPT Budget: " +str(lpt))
                              for row in range(0,8):
                                  if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                     and plan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR):
                                      if(others >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                          plan_df.iloc[row,col]+=1
                                          invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                          invplan_df.iloc[row,col] -= dur_min
                                          spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          others-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          remainingbudget -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                          spots+=1
                    
                    remainingbudget = others+morning+afternoon+matinee+ept+pt+lpt
#                    print("Remaining Budget: " +str(remainingbudget))  
                    times=0
                    while(times!=15):
                        for col in range(len(plan_df.columns)-1, -1,-1):
                            if(plan_df.columns[col] in campaign_dates):
                                for row in range(0,len(tband)):
                                    if(invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]].values >= dur_min 
                                       and plan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR):
                                        if(remainingbudget >= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)):
                                            plan_df.iloc[row,col]+=1
                                            invplan_df_consolidated.loc[(invplan_df_consolidated['Channel']==c) & (invplan_df_consolidated['Time Band'] == tband[row][:-10]), invplan_df_consolidated.columns.values[col+1]] -= dur_min
#                                            invplan_df.iloc[row,col] -= dur_min
                                            spends += (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            spends_df.iloc[row,col]+=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            budget_df.loc[budget_df["Channel Name"]==c, brand] -= (rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                                            remainingbudget-=(rates_df_consolidated.loc[(rates_df_consolidated['Channel']==c) & (rates_df_consolidated['Time Band'] == tband[row][:-10]), rates_df_consolidated.columns.values[col+1]].values * dur_min)
                        times+=1
#                        print("Remaining Budget: " +str(remainingbudget))
#################################### CONTENT SPECIFIC ######################################
############################################################################################
############################################################################################
############################################################################################
#################################### FOC SPENDWISE DISTRIBUTION #####################################
                elif(plantype=="focspendwise"):
                    foc_dist = msypher_utils_cloud.load_distribution("FOC", monthm)
#                    foc_dist = pd.read_sql_query("select * from FOC_Minutes_Distribution_"+monthm+";", conn)
                    
                    tband_foc = ['00:00','01:00','02:00','03:00','04:00','05:00','06:00',
                                   '07:00', '08:00', '09:00', '10:00', '11:00', '12:00',
                                   '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', 
                                   '19:00', '20:00', '21:00', '22:00', '23:00']
                    
                    if 'index' in foc_dist.columns:
                        foc_dist.index = foc_dist['index']
                        foc_dist = foc_dist.drop(['index'], axis=1)
                    foc_dist_split = foc_dist
                    foc_dist_split.iloc[:, 1:] = foc_dist_split.iloc[:,1:]*budgetsplit
                    foc_in_secs = foc_dist_split.loc[foc_dist_split["Channel Name"]==c, brand]*60
                    foc_spots = math.ceil(foc_in_secs/duration)
                    spends = 0
                    remainingbudget = foc_spots
                    others = math.ceil(foc_spots * r['Others'])
                    morning = math.ceil(foc_spots * r['Morning'])
                    afternoon = math.ceil(foc_spots * r['Afternoon'])
                    matinee = math.ceil(foc_spots * r['Matinee'])
                    ept = math.ceil(foc_spots * r['EPT'])
                    pt = math.ceil(foc_spots * r['PT'])
                    lpt = math.ceil(foc_spots *r['LPT'])
                    
                    focplan_df = pd.DataFrame(index= foc_df_consolidated.loc[foc_df_consolidated['Channel']==c, :].index, columns=foc_df_consolidated.columns[1:])
                    focplan_df = focplan_df.fillna(0)
                   
                    for col in range(0, len(focplan_df.columns)):
                          if(focplan_df.columns[col] in campaign_dates):
                              spots = 0
                              for row in range(8,12):
#                                  print(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband[row][:-10]), foc_df_consolidated.columns.values[col+1]].values)
                                  if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     morning > 0 ):
                                      focplan_df.iloc[row,col]+=1
                                      foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                      foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      morning-=1
                                      spots+=1
                              for row in range(12,15):
                                  if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     afternoon > 0 ):
                                      focplan_df.iloc[row,col]+=1
                                      foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                      foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      afternoon-=1
                                      spots+=1
                              for row in range(15,18):
                                  if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     matinee > 0 ):
                                      focplan_df.iloc[row,col]+=1
                                      foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                      foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      matinee-=1
                                      spots+=1
                              for row in range(18,20):
                                  if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     ept > 0 ):
                                      focplan_df.iloc[row,col]+=1
                                      foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                      foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      ept-=1
                                      spots+=1
                              for row in range(20,22):
                                  if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     pt > 0 ):
                                      focplan_df.iloc[row,col]+=1
                                      foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                      foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      pt-=1
                                      spots+=1
                              for row in range(22,24):
                                  if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     lpt > 0 ):
                                      focplan_df.iloc[row,col]+=1
                                      foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                      foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      lpt-=1
                                      spots+=1
                              for row in range(0,8):
                                  if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     others > 0 ):
                                      focplan_df.iloc[row,col]+=1
                                      foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                      foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      others-=1
                                      spots+=1
                    times = 0
                    while(times!=15):
                        for col in range(len(focplan_df.columns)-1, -1,-1):
                            if(focplan_df.columns[col] in campaign_dates):
                                for row in range(0,24):
                                    if(foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]].values >= dur_min and
                                       remainingbudget > 0):
                                        focplan_df.iloc[row,col]+=1
                                        foc_df_consolidated.loc[(foc_df_consolidated['Channel']==c) & (foc_df_consolidated['Time Band'] == tband_foc[row]), foc_df_consolidated.columns.values[col+1]] -= dur_min
                                        foc_dist.loc[foc_dist_split["Channel Name"]==c, brand] -= dur_min
                                        remainingbudget-= 1
                        times+=1
                    msypher_utils_cloud.save_distribution(foc_dist, "FOC", monthm)
#################################### FOC SPENDWISE DISTRIBUTION ######################################
############################################################################################
############################################################################################
############################################################################################
#################################### CPRP SPENDWISE DISTRIBUTION #####################################
                elif(plantype=="cprpspendwise"):
                    cprp_dist = msypher_utils_cloud.load_distribution("CPRP", monthm)

                    tband_cprp = ['00:00','01:00','02:00','03:00','04:00','05:00','06:00',
                                   '07:00', '08:00', '09:00', '10:00', '11:00', '12:00',
                                   '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', 
                                   '19:00', '20:00', '21:00', '22:00', '23:00']
                    spends_df = pd.DataFrame(index= cprp_df_consolidated.loc[cprp_df_consolidated['Channel']==c, :].index, columns=cprp_df_consolidated.columns[1:])
                    spends_df = spends_df.fillna(0)
                    if 'index' in cprp_dist.columns:
                        cprp_dist.index = cprp_dist['index']
                        cprp_dist = cprp_dist.drop(['index'], axis=1)
                    
                    cprp_rpm = channel_budget/cprp_dist.loc[cprp_dist["Channel Name"]==c, brand].sum()#'Ponds White Beauty Cream':'Dove Bars'].sum()
                    rate_per_spot = cprp_rpm*dur_min
                    cprp_dist_split = cprp_dist
                    cprp_dist_split.iloc[:,1:] = cprp_dist.iloc[:,1:]*budgetsplit#100#(float(request.form['budgetsplit'])/100)
                    cprp_in_secs = cprp_dist_split.loc[cprp_dist_split["Channel Name"]==c, brand]*60
                    cprp_spots = math.ceil(cprp_in_secs/duration)
                    spends = 0
                    remainingbudget = cprp_spots
                    others = math.ceil(cprp_spots * r['Others'])
                    morning = math.ceil(cprp_spots * r['Morning'])
                    afternoon = math.ceil(cprp_spots * r['Afternoon'])
                    matinee = math.ceil(cprp_spots * r['Matinee'])
                    ept = math.ceil(cprp_spots * r['EPT'])
                    pt = math.ceil(cprp_spots * r['PT'])
                    lpt = math.ceil(cprp_spots * r['LPT'])

                    cprpplan_df = pd.DataFrame(index= cprp_df_consolidated.loc[cprp_df_consolidated['Channel']==c].index, columns=cprp_df_consolidated.columns[1:])
                    cprpplan_df = cprpplan_df.fillna(0)
                    for col in range(1, len(cprpplan_df.columns)):
                          if(cprpplan_df.columns[col] in campaign_dates):
                              spots = 0
                              for row in range(8,12):
                                  if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     cprpplan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR and
                                     morning > 0 ):
                                      cprpplan_df.iloc[row,col]+=1
                                      cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                      spends_df.iloc[row,col]+=rate_per_spot
                                      cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      morning-=1
                                      spots+=1
                              for row in range(12,15):
                                  if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     cprpplan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR and
                                     afternoon > 0 ):
                                      cprpplan_df.iloc[row,col]+=1
                                      cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                      spends_df.iloc[row,col]+=rate_per_spot
                                      cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      afternoon-=1
                                      spots+=1
                              for row in range(15,18):
                                  if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     cprpplan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR and
                                     matinee > 0 ):
                                      cprpplan_df.iloc[row,col]+=1
                                      cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                      spends_df.iloc[row,col]+=rate_per_spot
                                      cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      matinee-=1
                                      spots+=1
                              for row in range(18,20):
                                  if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     cprpplan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR and
                                     ept > 0 ):
                                      cprpplan_df.iloc[row,col]+=1
                                      cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                      spends_df.iloc[row,col]+=rate_per_spot
                                      cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      ept-=1
                                      spots+=1
                              for row in range(20,22):
                                  if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     cprpplan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR and
                                     pt > 0 ):
                                      cprpplan_df.iloc[row,col]+=1
                                      cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                      spends_df.iloc[row,col]+=rate_per_spot
                                      cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      pt-=1
                                      spots+=1
                              for row in range(22,24):
                                  if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     cprpplan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR and
                                     lpt > 0 ):
                                      cprpplan_df.iloc[row,col]+=1
                                      cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                      spends_df.iloc[row,col]+=rate_per_spot
                                      cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      lpt-=1
                                      spots+=1
                              for row in range(0,8):
                                  if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and 
                                     cprpplan_df.iloc[row,col] <= MAX_SPOTS_PER_HOUR and
                                     others > 0 ):
                                      cprpplan_df.iloc[row,col]+=1
                                      cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                      spends_df.iloc[row,col]+=rate_per_spot
                                      cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                      remainingbudget -= 1
                                      others-=1
                                      spots+=1
                    times=0
                    while(times!=15):
                        for col in range(len(cprpplan_df.columns)-1, -1,-1):
                            if(cprpplan_df.columns[col] in campaign_dates):
                                for row in range(0,24):
                                    if(cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]].values >= dur_min and
                                       cprpplan_df.iloc[row,col] < MAX_SPOTS_PER_HOUR and
                                       remainingbudget > 0):
                                        cprpplan_df.iloc[row,col]+=1
                                        cprp_df_consolidated.loc[(cprp_df_consolidated['Channel']==c) & (cprp_df_consolidated['Time Band'] == tband_cprp[row]), cprp_df_consolidated.columns.values[col+1]] -= dur_min
                                        spends_df.iloc[row,col]+=rate_per_spot
                                        cprp_dist.loc[cprp_dist["Channel Name"]==c, brand] -= dur_min
                                        remainingbudget-= 1
                        times+=1
                    msypher_utils_cloud.save_distribution(cprp_dist, "CPRP", monthm)
#################################### CPRP SPENDWISE DISTRIBUTION ######################################
############################################################################################
############################################################################################
############################################################################################ 
                
                global budget_list, rem_list, cha_list, brand_list, launchspends_list, cyclespends_list, weekendspends_list, contentspends_list, spends_list
                cha_list.append(channel_select)
                budget_list.append(authorized_budget)
                rem_list.append(remainingbudget)
                brand_list.append(brand)
                launchspends_list.append(launchspends)
                weekendspends_list.append(weekendspends)
                cyclespends_list.append(cyclespends)
                contentspends_list.append(contentspends)
                spends_list.append(spends)

                if(final_df.empty and spotsonly_df.empty):
                    global updated_cols
                    global finaldf
                    updated_cols = ['Plan','Channel','Brand', 'Caption', 'Duration', 'Time Band'] + mycolumns + ['Total Spots','GRP', 'Spends', 'CPRP']
                    final_df = pd.DataFrame(columns=updated_cols)
                    spotsonly_df = pd.DataFrame(columns=updated_cols)
                if(channel_select not in cprp_channels):
                    tim = pd.to_datetime(tband)
                    tband = tim.strftime("%H:%M")
                                
                tband_24hrs = ['00:00','01:00','02:00','03:00','04:00','05:00','06:00',
                               '07:00', '08:00', '09:00', '10:00', '11:00', '12:00',
                               '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', 
                               '19:00', '20:00', '21:00', '22:00', '23:00']
                tband_24hrs = pd.to_datetime(tband_24hrs)
                tband_24hrs = tband_24hrs.strftime("%H:%M")
                
                channel_ratings = ratings_df_consolidated.loc[ratings_df_consolidated['Channel'] == c, :]

                if(plantype=="focspendwise"):
                    channel_ratings_1 = channel_ratings.iloc[:,:-2].astype('float')
                    plan_df_1 = focplan_df.iloc[:,2:].astype('float')
                    if(c == "ARY DIGITAL" or c == "PTV HOME"):    
                        grpdf = pd.DataFrame(plan_df_1.values*channel_ratings_1.values[:-2], columns=plan_df_1.columns, index=plan_df_1.index)
                    else:
                        grpdf = pd.DataFrame(plan_df_1.values*channel_ratings_1.values, columns=plan_df_1.columns, index=plan_df_1.index)
                    focplan_df["Time Band"] = tband_24hrs
                    focplan_df["Total Spots"] = focplan_df.sum(axis=1)
                    focplan_df["GRP"] = grpdf.sum(axis=1)
                    focplan_df["Spends"] = spends_df.sum(axis=1)
                    focplan_df["CPRP"] = focplan_df["Spends"]/(focplan_df["GRP"]*duration/30)
                    final = pd.concat([focplan_df, foc_df_consolidated.loc[foc_df_consolidated['Channel']==c, :]], sort=False)
                    final = final.iloc[:,:-1]
                    msypher_utils_cloud.save_foc_optimizer(foc_df_consolidated, monthm)
                elif(plantype=="cprpspendwise"):
                    channel_ratings_1 = channel_ratings.iloc[:,:-2].astype('float')
                    plan_df_1 = cprpplan_df.iloc[:,2:].astype('float')
                    grpdf = pd.DataFrame(plan_df_1.values*channel_ratings_1.values, columns=plan_df_1.columns, index=plan_df_1.index)
                    cprpplan_df["Time Band"] = tband_24hrs
                    cprpplan_df["Total Spots"] = cprpplan_df.sum(axis=1)
                    cprpplan_df["GRP"] = grpdf.sum(axis=1)
                    cprpplan_df["Spends"] = spends_df.sum(axis=1)
                    cprpplan_df["CPRP"] = cprpplan_df["Spends"]/(cprpplan_df["GRP"]*duration/30)
                    
                    final = pd.concat([cprpplan_df, cprp_df_consolidated.loc[cprp_df_consolidated['Channel']==c, :]], sort=False)
                    final = final.iloc[:,:-1]
                    msypher_utils_cloud.save_cprp_optimizer(cprp_df_consolidated, monthm)
                    
                else:
                    channel_ratings_1 = channel_ratings.iloc[:,:-2].astype('float')
                    plan_df_1 = plan_df.iloc[:,1:].astype('float')
                    grpdf = pd.DataFrame(plan_df_1.values*channel_ratings_1.values, columns=plan_df_1.columns, index=plan_df_1.index)
                    plan_df["Time Band"] = tband
                    plan_df["Total Spots"] = plan_df.sum(axis=1)
                    plan_df["GRP"] = grpdf.sum(axis=1)
                    plan_df["Spends"] = spends_df.sum(axis=1)
                    plan_df["CPRP"] = plan_df["Spends"]/(plan_df["GRP"]*duration/30)
                    final = pd.concat([plan_df, invplan_df_consolidated.loc[invplan_df_consolidated['Channel']==c, :]], sort=False)
                    msypher_utils_cloud.save_paid_optimizer(invplan_df_consolidated, monthm)

                final = final.sort_index(kind='mergesort')
                final["Plan"] = plan
                final["Channel"] = channel_select
                final["Brand"] = brand
                final["Caption"] = caption
                final["Duration"] = duration
                final = final[updated_cols]
                final.loc['Total'] = final.iloc[:,6:].sum()
                final.loc['Total', 'CPRP'] = final.loc['Total', 'Spends']/(final.loc['Total', 'GRP']*duration/30)
                final.iloc[:,:5] = [plan, channel_select, brand, caption, duration] 
                final_df = final_df.append(final)
                final_df = final_df.reset_index(drop=True)
                final_df = final_df.fillna(0)
                if(plantype == "focspendwise"):
                    spotsonly = focplan_df
                elif(plantype == "cprpspendwise"):
                    spotsonly = cprpplan_df
                else:
                    spotsonly = plan_df
                spotsonly["Plan"] = plan
                spotsonly["Channel"] = channel_select
                spotsonly["Brand"] = brand
                spotsonly["Caption"] = caption
                spotsonly["Duration"] = duration
                spotsonly = spotsonly[updated_cols]
                spotsonly.loc['Total'] = spotsonly.iloc[:,6:].sum()
                spotsonly.loc['Total', 'CPRP'] = spotsonly.loc['Total', 'Spends']/(spotsonly.loc['Total', 'GRP']*duration/30)
                spotsonly.iloc[:,:5] = [plan, channel_select, brand, caption, duration]
                spotsonly_df = spotsonly_df.append(spotsonly)
                spotsonly_df = spotsonly_df.reset_index(drop=True)
                spotsonly_df = spotsonly_df.fillna(0)
                
            except sqlite3.OperationalError as e:
                print("\nAn OperationalError occurred. Error number {0}: {1}.".format(e.args[0],e.args[1]))
            except pd.io.sql.DatabaseError as e:
                print(e)
            msypher_utils_cloud.save_budgetsheet(budget_df, monthm)
        global action
#        if(action == "Update"):    
#            fullplanspotsdf = pd.read_sql_query("select * from "+monthm+"_fullplan_spots;", conn)
#            fullplanspotsdf = fullplanspotsdf.drop(['index'], axis=1)
#            for col in fullplanspotsdf.columns:
#                if col in campaign_dates:
#                    fullplanspotsdf.loc[fullplanspotsdf.Brand == brand,col] = spotsonly_df[col]
#            fullplanspotsdf.loc[fullplanspotsdf.Brand == brand, 'Total Spots'] += spotsonly_df['Total Spots']
#            fullplanspotsdf.loc[fullplanspotsdf.Brand == brand, 'Spends'] += spotsonly_df['Spends']
#            fullplanspotsdf.to_sql(monthm+"_fullplan_spots", conn, if_exists="replace")
#        else:
        #mylist = [fullplanspotsdf, spotsonly_df]
        #mydf= pd.concat(mylist)
#        spotsonly_df.to_sql(monthm+"_fullplan_spots", conn, if_exists="replace")
        
        spotsonly_df["Plan"] = spotsonly_df["Plan"].astype(str)
        spotsonly_df["Channel"] = spotsonly_df["Channel"].astype(str)
        spotsonly_df["Brand"] = spotsonly_df["Brand"].astype(str)
        spotsonly_df["Caption"] = spotsonly_df["Caption"].astype(str)
        spotsonly_df["Time Band"] = spotsonly_df["Time Band"].astype(str)
        spotsonly_df['CPRP'] = spotsonly_df.CPRP.replace(np.inf, 0)
        msypher_utils_cloud.save_spots_only(spotsonly_df, monthm)
#        msypher_utils_cloud.save_spots_with_inventory(final_df, monthm)
#        final_df.to_sql(monthm+"_fullplan_spotswithinventory", conn, if_exists="replace")        
        
#        filename = "Full_plan"
#        writer = pd.ExcelWriter('Plans/With_Inventory/'+filename+'.xlsx',  engine='openpyxl')
#        final_df.to_excel(writer, 'Plan')
#        writer.close()
#        writer1 = pd.ExcelWriter('Plans/Only_Spots/'+filename+'.xlsx',  engine='openpyxl')
#        spotsonly_df.to_excel(writer1, 'Plan')
#        writer1.close()
        return render_template('success.html') 
#                               ip = ip
#                               brand=brand,
#                               caption=caption, 
#                               duration=duration, 
#                               splits=splits_df.index.tolist())
                               #table= view_df.to_html(index=False))

@app.route('/convertor', methods = ['GET', 'POST'])
def convertor():
    if request.method == 'GET':
        return render_template('convertor.html')
    if request.method == 'POST':
        df = msypher_utils_cloud.load_historical_data()
        print(len(df))
        print("Loaded tracking data.")
        
#        plandf = pd.ExcelFile('Full_plan.xlsx')
#        plandf = plandf.parse(plandf.sheet_names[0], parse_dates=True)
        plandf = msypher_utils_cloud.load_spots_only(monthm)
        
        plandf.Channel = [ch.replace("_"," ") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("TWENTYFOUR NEWS","24 NEWS") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("SEVEN NEWS","7 NEWS") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("EIGHTXM","8XM") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("ABB TAKK","ABB TAKK NEWS") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("ARUJ TV","ARUJ") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("HNOW","H NOW") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("EXPRESS ENTERTAINMENT","EXPRESS ENT") for ch in plandf.Channel]
        plandf.Channel = [ch.replace("NEO TV","NEO") for ch in plandf.Channel]

        # FOR PIB
        total_spots = plandf.filter(['Plan','Channel', 'Time Band', 'Total Spots'])
        total_spots = total_spots[total_spots['Time Band'] == 0]
        
        plandf = plandf[(plandf['Time Band'] != '0')]
        print("Total rows in plan: {0}".format(len(plandf)))
        
        date1 = pd.to_datetime(request.form['startdate'])
        date2 = pd.to_datetime(request.form['enddate'])
        week = int(request.form['backdate'])
        mycolumns = []
        mydateformat = []
        for dt in daterange(date1, date2):
            mycolumns.append(dt.date())
            mydateformat.append(dt.strftime("%a_%e-%b"))

        df['Date'] = pd.to_datetime(df['Date'])
        df = df.reset_index()
        
        merged_pib = msypher_utils_cloud.load_pib_splits() 
        print("Loaded PIB Spots.")
        
        start = time.time()
        converted_df = pd.DataFrame(columns=['Date', 'Channel', 'Start Time', 'Length', 'Brand'])
        date_list = []
        channel_list = []
        adstarttime_list = []
        adduration_list = []
        brand_list = []
        plan_list = []
        days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        mpib = [0,-1, 1]
        counter=0
        for col in tqdm(range(6,len(plandf.columns)-4)):
#        for col in tqdm(range(len(mycolumns))):
            if(plandf.columns[col] in mydateformat):
                print("Date: "+plandf.columns[col])
                first = mycolumns[counter] - pd.Timedelta(weeks=week)
                present_spots = plandf[plandf.iloc[:,col] != 0]
                thatday = df[df.Date.dt.date == first]
                for row in present_spots.index:
                    thatchannel = thatday[thatday.Channel == present_spots.at[row,'Channel']]
                    thathour = thatchannel[thatchannel.TransmissionHour == pd.to_datetime(present_spots.at[row,'Time Band']).time().hour]
                    if(thathour.empty):
                        thathour = df[(df.Channel == present_spots.at[row,'Channel']) & (df.TransmissionHour == pd.to_datetime(present_spots.at[row,'Time Band']).time().hour)
                                     & (df.Day == days[first.weekday()])]
                    if(thathour.empty):
                        thathour = df[(df.Channel == present_spots.at[row,'Channel']) & (df.TransmissionHour == pd.to_datetime(present_spots.at[row,'Time Band']).time().hour)]
                    if(thathour.empty):
                        thathour = df[(df.Channel == present_spots.at[row,'Channel']) & (df.Day == days[first.weekday()])]
                    for count in range(int(present_spots.at[row,plandf.columns[col]])):
                        if(len(thathour.MidBreak.unique()) >= present_spots.at[row,plandf.columns[col]]):
                            thatmidbreak = thathour[thathour.MidBreak == thathour.MidBreak.unique()[count]]
                        else:
                            if(len(thathour) > 0):
                                thatmidbreak = thathour[thathour.MidBreak == thathour.MidBreak.unique()[len(thathour.MidBreak.unique())-1]]
            
                        if(merged_pib.PIBs[(merged_pib.index == present_spots.at[row,'Channel'])].values > 0):
                            merged_pib.PIBs[merged_pib.index == present_spots.at[row,'Channel']]-=1
                            if(len(thatmidbreak) > 1):
                                thatspot = thatmidbreak.iloc[random.choice(mpib),:]
                            else:
                                thatspot = thatmidbreak.iloc[random.choice(mpib[:2]),:]
                        else:
                            thatspot = thatmidbreak.iloc[int(len(thatmidbreak)/2),:]
                        
                        date_list.append(first)
                        channel_list.append(present_spots.at[row,'Channel'])
                        adstarttime_list.append(thatspot.AdStart)
                        adduration_list.append(present_spots.at[row,'Duration'])
                        brand_list.append(present_spots.at[row,'Brand'])
                        plan_list.append(present_spots.at[row,'Plan'])
                        
                counter+=1
        end = time.time()
        elapsed = end - start
        print("Time taken for whole process is "+str(elapsed)+" seconds.")
        
        print("Preparing converted data...")
        
        date_list = [d.strftime('%#d-%b-%y') for d in date_list]
        converted_df['Date'] = date_list
        converted_df['Plan'] = plan_list
        converted_df['Channel'] = channel_list
        converted_df['Start Time'] = adstarttime_list
        converted_df['Length'] = adduration_list
        converted_df['Brand'] = brand_list
        
        converted_df = converted_df.sort_values(by=['Date','Start Time'])
        converted_df.to_excel('Converted_'+monthm+'.xlsx')
        msypher_utils_cloud.save_converted_file(converted_df, monthm)
        return render_template('success.html')

@app.route('/updateplan', methods = ['GET', 'POST'])
def updateplan():
    if(request.method=='GET'):
        conn = sqlite3.connect("channelsplan.db")
        plandf = pd.read_sql_query("select * from "+monthm+"_fullplan_spots;", conn)
        plandf = plandf.drop(['index'], axis=1)
        brands = plandf.Brand.unique()
        return render_template('update_inventory.html', splits=brands, ip = ip)
    if(request.method=='POST'):
        conn = sqlite3.connect("channelsplan.db")
        plandf = pd.read_sql_query("select * from "+monthm+"_fullplan_spots;", conn)
        plandf = plandf.drop(['index'], axis=1)

        minutes_dist = pd.read_sql_query("select * from Minutes_Distribution_"+monthm+";", conn)
        print(minutes_dist)
        if 'index' in minutes_dist.columns:
            minutes_dist.index = minutes_dist['index']
            minutes_dist = minutes_dist.drop(['index'], axis=1)
        if 'Channel Name' in minutes_dist.columns:
            minutes_dist.index = minutes_dist['Channel Name']
            minutes_dist = minutes_dist.drop(['Channel Name'], axis=1)        
        
        brand = request.form.get('brand')
        start_dt = pd.to_datetime(request.form['startdate'])
        end_dt = pd.to_datetime(request.form['enddate'])
        mycolumns=[]
        for dt in daterange(start_dt, end_dt):
            mycolumns.append(dt.strftime("%a_%e-%b"))
        
        timebands_consolidated = msypher_utils_cloud.load_timebands(monthm)
        mydf = plandf[(plandf.Brand == brand) & (plandf['Time Band'] != '0')]
        
        for index, r in mydf.iterrows():
#            time_band = pd.read_sql_query("select * from "+r['Channel']+"_timeband;", conn)
            tbands = timebands_consolidated[timebands_consolidated.Channel == r["Channel"]].iloc[:,0].tolist()
#            tband = [datetime.strptime(x,"%H:%M:%S.%f") for x in time_band.iloc[:,1].tolist()]
#            tbands = [x.strftime("%H:%M") for x in tband]
            
            tband_24hrs = ['00:00','01:00','02:00','03:00','04:00','05:00','06:00',
               '07:00', '08:00', '09:00', '10:00', '11:00', '12:00',
               '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', 
               '19:00', '20:00', '21:00', '22:00', '23:00']

            if(r['Plan'] == "Paid"):
                invplan_df = pd.read_sql_query("select * from "+r['Channel']+"_invplan_"+monthm+";", conn)
                invplan_df.index = tbands
                rate_df = pd.read_sql_query("select * from "+r['Channel']+"_rateplan_"+monthm+";", conn)
                rate_df = rate_df.drop(['index'], axis=1)
                rate_df.index = tbands

            if(r['Plan'] == "FOC"):
                invplan_df = pd.read_sql_query("select * from "+r['Channel']+"_focinvplan_"+monthm+";", conn)
                invplan_df.index = tband_24hrs

            if(r['Plan'] == "CPRP"):
                invplan_df = pd.read_sql_query("select * from "+r['Channel']+"_cprpinvplan_"+monthm+";", conn)
                invplan_df.index = tband_24hrs
             
            if('index' in invplan_df.columns):
                invplan_df = invplan_df.drop(['index'], axis=1)

            budget_df = pd.read_sql_query("select * from "+monthm+"_budgetsheet;", conn)

            chan = [x.replace(" ", "_") for x in budget_df.iloc[:,0].tolist()]
            budget_df.index = chan
            if('index' in budget_df.columns):
                budget_df = budget_df.drop(['index'], axis=1)
            for col in mydf.columns:
                if(col in mycolumns):
                    if(r[col] > 0):
                        dur = (r[col]*r['Duration'])/60.0
                        invplan_df.loc[r['Time Band'], col] += dur
                        plandf.loc[index, col] = 0
                        if(r['Plan'] == "Paid"):
                            budget_df.loc[r['Channel'], brand] += (rate_df.loc[r['Time Band'], col]*dur)
                            plandf.loc[index, 'Total Spots'] -= r[col]
                            plandf.loc[index, 'Spends'] -= (rate_df.loc[r['Time Band'], col]*dur)
                        else:
                            print(minutes_dist.loc[minutes_dist.index == r['Channel'], brand])
                            minutes_dist.loc[r['Channel'], brand] += dur
                            print(minutes_dist.loc[minutes_dist.index == r['Channel'], brand])
                            
            if(r['Plan'] == "Paid"):
                invplan_df.to_sql(r['Channel']+"_invplan_"+monthm, conn, if_exists="replace")
                budget_df.to_sql(monthm+"_budgetsheet", conn, if_exists="replace")
            if(r['Plan'] == "FOC"):
                invplan_df.to_sql(r['Channel']+"_focinvplan_"+monthm, conn, if_exists="replace")
            if(r['Plan'] == "CPRP"):
                invplan_df.to_sql(r['Channel']+"_cprpinvplan_"+monthm, conn, if_exists="replace")
        
        preprocessed_df = pd.read_sql_query("select * from "+monthm+"_entry;", conn)
        preprocessed_df = preprocessed_df.drop(['index'], axis=1)
        #preprocess_df = preprocess_df[preprocess_df['Brand Name'] != brand]
        preprocessed_df = pd.DataFrame(columns=preprocessed_df.columns)
        preprocessed_df.to_sql(monthm+"_entry", conn, if_exists="replace")
        global preprocess_df, spotsonly_df
        preprocess_df = pd.DataFrame(columns=preprocessed_df)
        #spotsonly_df = pd.DataFrame(columns=plandf.columns)
        plandf.to_sql(monthm+"_fullplan_spots", conn, if_exists="replace")
        return "Updated!"#render_template('update_inventory.html')

if __name__ == '__main__':
    #run_server()
#    app.secret_key = 'super secret key'
    app.run(host='0.0.0.0', port=5000, debug = False)
    #app.run()