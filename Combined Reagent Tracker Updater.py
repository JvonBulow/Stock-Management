import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from collections import Counter
import mysql.connector
import snowflake.connector

# # *********************************************************************** Library Prep ***********************************************************************

# ***** Reagent Tracker Connection *****

mydb = mysql.connector.connect(
    host = 'mysql-ro.ils.illumina.com',
    user = 'tableau_ro',
    password = 'ILSLetsGatherSomeMetrics',
    port = 3306,
    database = 'reagent_tracker_prod'
    
    )


Output = pd.read_sql(f"""SELECT *
FROM reagent_tracker_prod.RecordComponent
JOIN reagent_tracker_prod.RecordKit ON RecordKit.id = RecordComponent.kit_id
JOIN reagent_tracker_prod.Record ON Record.id = RecordKit.record_id
JOIN reagent_tracker_prod.Lab ON Lab.id = Record.lab_id
JOIN reagent_tracker_prod.Process ON Process.id = Record.process_id
WHERE RecordComponent.id > 470178
and Record.created_by like 'Lucy Carver' or Record.created_by like 'Mari-Anne Salisbury' or Record.created_by like 'Christine Alberti-Segui'""", con = mydb)


Output.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Lib_Prep_Output.csv', index= False )


# ***** REAGENT TRACKER UPDATER *****

df = pd.read_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Lib_Prep_Output.csv')



# ***** Purification *****

# Generate purification lot and id dictionary

purification_df = df[(df['type'] == 'Illumina DNA PCR-free Purification Kit, 96 samples') & (df['name'] == 'Box Lot Number')]

pur_dict = dict(zip(purification_df['id'].to_list(), purification_df['lot_no'].to_list()))




# Check and update reagent tracker

rt = pd.read_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Reagent_Tracker.csv')

rt_lots = list(rt['Lot'])

# Iterate through purification lots from output file and remove those not in the reagent tracker

for ele in list(pur_dict.values()):
    if ele not in rt_lots:
        print ('Purification Kit not in Reagent Tracker used ', ele)


# Open the processed Reagents 

pi = pd.read_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Processed_Reagents.csv')

# Iterate through purification ids from output file and remove those already in processed reagents

pi_ids = list(pi['id'])

for ele in list(pur_dict.keys()):
    if str(ele) in pi_ids:
        pur_dict = {key:val for key, val in pur_dict.items() if key != ele}


# Reduce quantity in reagent tracker by 1

i=0
for ele in list(pur_dict.values()):
    index = rt.index[rt['Lot']==ele].tolist()
    rt.loc[index, 'Quantity'] = ((rt.loc[index, 'Quantity'].astype(int))-1)



# ***** TAGMENTATION *****

# Generate tagmentation lot and id dictionary

tagmentation_df = df[(df['type'] == 'Illumina DNA PCR-free Tagmentation Kit, 96 samples') & (df['name'] == 'Box Lot Number')]
tag_dict = dict(zip(tagmentation_df['id'].to_list(), tagmentation_df['lot_no'].to_list()))


# Iterate through tagmentation lots from output file and remove those not in the reagent tracker

for ele in list(tag_dict.values()):
    if str(ele) not in rt_lots:
        print ('Tagmentation Kit not in Reagent Tracker used ', ele)


# Iterate through tagmentation ids from output file and remove those already in processed ids

for ele in list(tag_dict.keys()):
    if str(ele) in pi_ids:
        tag_dict = {key:val for key, val in tag_dict.items() if key != ele}


# Reduce quantity in reagent tracker by 1

i=0
for ele in list(tag_dict.values()):
    index = rt.index[rt['Lot']==ele].tolist()
    rt.loc[index, 'Quantity'] = (rt.loc[index, 'Quantity'].astype(int))-1
    new_pi_row = {'id': list(tag_dict.keys())[i]}
    pi.loc[len(pi)] = new_pi_row
    pi = pi.reset_index(drop=True)
    i+=1


# ***** UDI PLATE *****

# Generate UDI lot and id dictionary

UDI_df = df[(df['type'] == 'IUO, 32 INDEX UDI, PCR-FREE') & (df['name'] == 'UDI Barcode')]
UDI_dict = dict(zip(UDI_df['id'].to_list(), UDI_df['lot_no'].to_list()))


# Iterate through UDI lots from output file and remove those not in the reagent tracker

for ele in list(UDI_dict.values()):
    if str(ele) not in rt_lots:
        print('UDI IP barcode used instead of lot! ', ele)


# Iterate through UDI ids from output file and remove those already in processed ids

for ele in list(UDI_dict.keys()):
    if str(ele) in pi_ids:
        UDI_dict = {key:val for key, val in UDI_dict.items() if key != ele}


# Reduce quantity in reagent tracker by 1 and add to processed ID

i=0
for ele in list(UDI_dict.values()):
    index = rt.index[rt['Lot']==ele].tolist()
    rt.loc[index, 'Quantity'] = (rt.loc[index, 'Quantity'].astype(int))-1
    new_pi_row = {'id': list(UDI_dict.keys())[i]}
    pi.loc[len(pi)] = new_pi_row
    pi = pi.reset_index(drop=True)
    i+=1


# Overall Reagent Stock Count:
    
reagent_list = ['Illumina DNA PCR-free Purification Kit, 96 samples', 'Illumina DNA PCR-free Tagmentation Kit, 96 samples', 'IUO, 32 INDEX UDI, PCR-FREE']

for reagent in reagent_list:

    total_quantities = list(rt.query(f'Reagent=="{reagent}"')['Quantity'])
    sum = int()
    for ele in total_quantities:
        sum+=ele

    if sum < 10:
        print('\nLow Stock of', reagent,': only ', sum, 'remaining')


# Final csv outputs

pi.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Processed_Reagents.csv', index = False)
rt.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Reagent_Tracker.csv', index= False)











# *********************************************************************** SEQUENCING ***********************************************************************


username = "jvonbulow"

con = snowflake.connector.connect(
    user=username,
    authenticator='externalbrowser',
    account= 'nlhadkr-ilmnupaeuw2')
cur = con.cursor()

try:
    cur.execute("select current_date")
    one_row=cur.fetchone()
    cur.execute("SELECT current_version()")
    one_row = cur.fetchone()
    
finally:
    cur.close()
cur.close()

today = datetime.date.today()
last_month = today - relativedelta(months =1)

# open connection and select warehouse
cur = con.cursor()
cur.execute("USE WAREHOUSE ILS_TEAM_EUW2")

cur.execute(f"""SELECT EXPERIMENTNAME,
    SBSLOTNUMBER,
    CLUSTERLOTNUMBER,
    BUFFERLOTNUMBER,
    FLOWCELLLOTNUMBER,
    INSTRUMENT
    FROM
    ILS_PRVW_DB.ILS_UNCONTROLLED.PRVW_BSSH_TRENDING_SS
    WHERE
    INSTRUMENTSTARTEDON >= '{last_month}' AND INSTRUMENTSTARTEDON <= '{today}'
""")

df = cur.fetch_pandas_all()

df = df.drop_duplicates()


output = df.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Sequencing_Output.csv', index=False)

df = pd.read_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Sequencing_Output.csv')

# Make a combined dictionary {EXPERIMENTNAME: [SBS, CLUSTER, BUFFER, FC]}

exp_names = list(df['EXPERIMENTNAME'])
SBS_lots = list(df['SBSLOTNUMBER'])
CLUSTER_lots = list(df['CLUSTERLOTNUMBER'])
BUFFER_lots = list(df['BUFFERLOTNUMBER'])
FC_lots = list(df['FLOWCELLLOTNUMBER'])
asset_nos = list(df['INSTRUMENT'])



combined_lot_dict = {}

i = 0
for ele in exp_names:
    combined_lot_dict[exp_names[i]] = [SBS_lots[i], CLUSTER_lots[i], BUFFER_lots[i], FC_lots[i]]
    i+=1


# Remove EXPERIMENTNAMES already in processed runs

pr = pd.read_csv("C:\Python Projects\Stock Management\Combined Reagent Tracker\Processed_Reagents.csv")

pr_names = list(pr['id'])

for ele in list(combined_lot_dict.keys()):
    if ele in pr_names:
        combined_lot_dict.pop(ele)



# Make count of each reagent and lot

# SBS
SBS_Lot_List = []
for ele in combined_lot_dict.values():
    SBS_Lot_List.append(ele[0])

SBS_Lot_Dict = dict(Counter(SBS_Lot_List))



# Cluster
Cluster_Lot_List = []
for ele in combined_lot_dict.values():
    Cluster_Lot_List.append(ele[1])

Cluster_Lot_Dict = dict(Counter(Cluster_Lot_List))

# Buffer
Buffer_Lot_List = []
for ele in combined_lot_dict.values():
    Buffer_Lot_List.append(ele[2])

Buffer_Lot_Dict = dict(Counter(Buffer_Lot_List))

# FC
FC_Lot_List = []
for ele in combined_lot_dict.values():
    FC_Lot_List.append(ele[3])

FC_Lot_Dict = dict(Counter(FC_Lot_List))
    


# ***** Check and update reagent tracker *****

rt = pd.read_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Reagent_Tracker.csv')

# Remove 0s
rt = rt[rt['Quantity'] > 0]

# SBS
i=0
for ele in list(SBS_Lot_Dict.keys()):
    index = rt.index[rt['Lot'].astype('string') == str(ele)].tolist()
    if rt.iloc[index, 0].tolist() == ['SBS']:
        rt.loc[index, 'Quantity'] = (rt.loc[index, 'Quantity'].astype(int))-list(Cluster_Lot_Dict.values())[i]
    i+=1


# Cluster
i=0
for ele in Cluster_Lot_Dict.keys():
    index = rt.index[rt['Lot'].astype('string') == str(ele)].tolist()
    if rt.iloc[index, 0].tolist() == ['Cluster']:
        rt.loc[index, 'Quantity'] = (rt.loc[index, 'Quantity'].astype(int))-list(Cluster_Lot_Dict.values())[i]
    i+=1

# Buffer
i=0
for ele in Buffer_Lot_Dict.keys():
    index = rt.index[rt['Lot'].astype('string') == str(ele)].tolist()
    if rt.iloc[index, 0].tolist() == ['Buffer']:
        rt.loc[index, 'Quantity'] = (rt.loc[index, 'Quantity'])-list(Buffer_Lot_Dict.values())[i]
    i+=1

# FC
i=0
for ele in FC_Lot_Dict.keys():
    index = rt.index[rt['Lot'].astype('string') == str(ele)].tolist()
    if rt.iloc[index, 0].tolist() == ['FC']:
        rt.loc[index, 'Quantity'] = (rt.loc[index, 'Quantity'])-list(FC_Lot_Dict.values())[i]
    i+=1


# Expiration date check and warning
today = datetime.datetime.today()
today_dt = datetime.date.today()

tendayshence = today + relativedelta(days = 10)
tendayshence = tendayshence.date()

i=0
for date in rt["Expiry Date"]:
    exp_date = datetime.datetime.strptime(date, '%d/%m/%Y').date()
    if exp_date < today_dt:
        Expiring_lot = str(rt.loc[i,"Lot"])
        Expiring_reagent = str(rt.loc[i,"Reagent"])
        print('\nLot ' + Expiring_lot, 'of the reagent "' + Expiring_reagent +'" has expired!' + ' (' + str(exp_date)+')')
    
    elif exp_date < tendayshence:
        Expiring_lot = str(rt.loc[i,"Lot"])
        Expiring_reagent = str(rt.loc[i,"Reagent"])
        print('\nLot ' + Expiring_lot, 'of the reagent "' + Expiring_reagent +'" expires in less than 10 days!' + ' (' + str(exp_date)+')')
    i+=1



# Overall Reagent Stock Count:


    
reagent_list = ['SBS', 'Cluster', 'Buffer', 'FC']

for reagent in reagent_list:

    total_quantities = list(rt.query(f'Reagent=="{reagent}"')['Quantity'])
    sum = int()
    for ele in total_quantities:
        sum+=ele

    if sum < 10:
        print('\nLow Stock of', reagent,': only ', sum, 'remaining')



rt.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Reagent_Tracker.csv', index=False)



# Processed runs updater

i=0
for ele in combined_lot_dict.keys():
    new_pr_row = {'id': ele}
    pr.loc[len(pr)] = new_pr_row
    pr = pr.reset_index(drop = True)
    i+=1

pr.to_csv('C:\Python Projects\Stock Management\Combined Reagent Tracker\Processed_Reagents.csv', index=False)