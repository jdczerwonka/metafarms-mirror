SERVER = ""
USERNAME = ""
PASSWORD = ""
DATABASE = ""

##bcp DietIngredientDB.dbo.[TABLE_NAME] in CSV_PATH -c -U jdczerwonka@wsf-db-server -S wsf-db-server.database.windows.net -t \t -P U2,6d2s5

from tables import *
from schemas import *

from flask import jsonify
from sqlalchemy import create_engine, func, Table
from sqlalchemy.orm import sessionmaker

import numpy
import pandas
import simplejson
import datetime
import time
import math

DB_URI = 'mssql+pyodbc://' + USERNAME + ':' + PASSWORD + '@' + SERVER + '/' + DATABASE + '?driver=SQL+Server+Native+Client+11.0'


##app = Flask(__name__)

def CreateSession():
    engine = create_engine(DB_URI)
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session

def DropTableAll():
    engine = create_engine(DB_URI)
    Base.metadata.drop_all(engine)

def DeleteRowsAll(table):
    session = CreateSession()
    session.query(table).delete()
    session.commit()

def DeleteRows(table, filterArr, filterCritArr):
    session = CreateSession()

def CreateTable(table):
    engine = create_engine(DB_URI)
    Base.metadata.tables[table.__tablename__].create(bind=engine)

def CreateTableAll():
    engine = create_engine(DB_URI)
    Base.metadata.create_all(engine)

def DietsAdd():
    EXCEL_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\Initialize Diets.xlsx"
    SAVE_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\diets.csv"
    
    df = pandas.read_excel(EXCEL_PATH, sheetname="Ingredient Quantities", skiprows=18, parse_cols=(0, 3, 6, 9, 10, 11, 14))
    df.columns = ['group_num', 'delivery_date', 'diet', 'quantity', 'mill', 'order_id', 'cost']

    df['year'] = pandas.DatetimeIndex(df['delivery_date']).year
    df['month'] = pandas.DatetimeIndex(df['delivery_date']).month
    df['week'] = pandas.DatetimeIndex(df['delivery_date']).week
    df['delivery_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
    df['delivery_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

    df = df[['order_id', 'group_num', 'delivery_date', 'delivery_month', 'delivery_week', 'year', 'month', 'week', 'mill', 'diet', 'quantity', 'cost']]

    df.to_csv(SAVE_PATH, sep='\t', index=False)
    
##    engine = create_engine(DB_URI)
##    df.to_sql('diets', engine, , if_exists='append', index=False)

def DietIngredientsAdd():
    EXCEL_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\Initialize Diets.xlsx"
    SAVE_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\diet_ingredients.csv"
    
    wb = openpyxl.load_workbook(EXCEL_PATH)
    ws = wb['Ingredient Quantities']
    max_col = ws.max_column

    col_arr = range(20, max_col)
    col_arr.insert(0, 11)

    qty_df = pandas.read_excel(EXCEL_PATH, sheetname="Ingredient Quantities", skiprows=18, parse_cols=col_arr)
    col_names = range(0, max_col - 20 + 1)

    col_names[0] = qty_df.columns[0]
    for x in range(1, max_col - 20 + 1):
        s = qty_df.columns[x]
        col_names[x] = s[:s.find('(') - 1]

    qty_df.columns = col_names
    qty_df_melt = pandas.melt(qty_df, id_vars = 'Order ID', var_name = 'ingredient')



    cost_df = pandas.read_excel(EXCEL_PATH, sheetname="Ingredient Cost", skiprows=18, parse_cols=col_arr)
    col_names = range(0, max_col - 20 + 1)

    col_names[0] = cost_df.columns[0]
    for x in range(1, max_col - 20 + 1):
        s = cost_df.columns[x]
        col_names[x] = s[:s.find('(') - 1]

    cost_df.columns = col_names
    cost_df_melt = pandas.melt(cost_df, id_vars = 'Order ID', var_name = 'ingredient')

    qty_df_melt['cost'] = cost_df_melt['value']
    qty_df_melt.columns = ('order_id', 'ingredient', 'quantity', 'cost')

    final_df = qty_df_melt.dropna()
    final_df['id'] = ""
    final_df = final_df[['id', 'order_id', 'ingredient', 'quantity', 'cost']]

    final_df.to_csv(SAVE_PATH, sep='\t', index=False)
##    engine = create_engine(DB_URI)
##    final_df.to_sql('diet_ingredients', engine, flavor='mssql', if_exists='append', index=False)

def GroupsAdd():
    EXCEL_PATH = "C:\Users\Jonathan.WSF\Downloads\Group_List03012016093834.xls"
    SAVE_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\groups.csv"
    
    df = pandas.read_excel(EXCEL_PATH, sheetname="Report", skiprows=9, parse_cols=(4, 5, 6, 9, 13, 14, 15, 16))
    df.columns = ['producer', 'site', 'barn', 'group_num', 'group_type', 'status', 'open_date', 'close_date']
    df = df[['group_num', 'group_type', 'status', 'producer', 'site', 'barn', 'open_date', 'close_date']]

    df.to_csv(SAVE_PATH, sep='\t', index=False)

##    engine = create_engine(DB_URI)
##    df.to_sql('groups', engine, flavor='mssql', if_exists='append', index=False)

def DeathsAdd():
    EXCEL_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\Initialize Deaths.xlsx"
    SAVE_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\deaths.csv"

    df = pandas.read_excel(EXCEL_PATH, sheetname="Mortality Report", skiprows=7, parse_cols=(8, 11, 13, 14, 15, 16, 19))
    df.columns = ['group_num', 'death_date', 'quantity', 'weight', 'death_type', 'reason', 'comments']

    df['year'] = pandas.DatetimeIndex(df['death_date']).year
    df['month'] = pandas.DatetimeIndex(df['death_date']).month
    df['week'] = pandas.DatetimeIndex(df['death_date']).week
    df['death_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
    df['death_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

    df['id'] = ""
    df = df[['id', 'group_num', 'death_date', 'death_month', 'death_week', 'year', 'month', 'week', 'death_type', 'reason', 'comments', 'quantity', 'weight']]
    
    df.to_csv(SAVE_PATH, sep='\t', index=False)

def MovementsAdd():
    EXCEL_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\Initialize Movements.xlsx"
    SAVE_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\movements.csv"    

    df = pandas.read_excel(EXCEL_PATH, sheetname="Summary", skiprows=9, parse_cols=(2, 6, 10, 12, 15, 20, 21, 23, 26, 28, 29, 31))
    df.columns = ['sow_unit', 'group', 'supplier', 'customer', 'plant', 'event_category', 'event_code', 'movement_id', 'movement_date', 'quantity', 'weight', 'cost']

    df['bool'] = df['group'].isnull()

    def func(row):
        if row['bool']:
            return row['sow_unit']
        else:
            return float('NaN')
        
    df['sow_unit'] = df.apply(func, axis=1)
    df.pop('bool')
    df_melt = pandas.melt(df, id_vars = ['event_category', 'event_code', 'movement_id', 'movement_date', 'quantity', 'weight', 'cost'], var_name = 'location_type', value_name='location_id')
    df_melt_drop = df_melt.dropna()

    df_melt_drop['year'] = pandas.DatetimeIndex(df_melt_drop['movement_date']).year
    df_melt_drop['month'] = pandas.DatetimeIndex(df_melt_drop['movement_date']).month
    df_melt_drop['week'] = pandas.DatetimeIndex(df_melt_drop['movement_date']).week
    df_melt_drop['movement_month'] = df_melt_drop['year'].astype(str) + " " + df_melt_drop['month'].astype(str)
    df_melt_drop['movement_week'] = (df_melt_drop['year'] - ((df_melt_drop['week'] == 53) & (df_melt_drop['month'] == 1))).astype(str) + " " + df_melt_drop['week'].astype(str)

    df_melt_drop['id'] = ""
    df_final = df_melt_drop[['id', 'movement_id', 'location_id', 'location_type', 'movement_date', 'movement_month', 'movement_week', 'year', 'month', 'week', 'event_category', 'event_code', 'quantity', 'weight', 'cost']]

    df_final.sort(columns=['movement_date', 'movement_id']).to_csv(SAVE_PATH, sep='\t', index=False)

def SalesAdd():
    EXCEL_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\Initialize Sales test.xls"
    SAVE_PATH = "C:\Users\Jonathan.WSF\Desktop\wsf databases\deaths.csv"

    carcass_df = pandas.read_excel(EXCEL_PATH, sheetname="Carcass Details", skiprows=9, parse_cols=(1, 4, 6, 7, 8, 9, 10, 14, 15, 16, 20, 21, 22, 23, 24, 31))
    carcass_df.columns = ['plant', 'tattoo', 'kill_date', 'quantity', 'avg_live_weight', 'avg_carcass_weight', 'base_price_cwt', 'value_cwt', 'vob_cwt', 'base_price', 'value', 'vob', 'back_fat', 'loin_depth', 'lean', 'yield']

    load_df = pandas.read_excel(EXCEL_PATH, sheetname="Sales", skiprows=21, parse_cols=(20, 28, 34, 37, 50, 51, 53, 54, 55, 56, 57))
    load_df.columns = ['group_num', 'kill_date', 'tattoo', 'plant', 'avg_live_weight', 'quantity', 'avg_carcass_weight', 'back_fat', 'loin_depth', 'yield', 'lean']
    
    carcass_df['group_id'] = carcass_df['tattoo'].astype(str) + ".0" + carcass_df['kill_date'].astype(str)
    load_df['group_id'] = load_df['tattoo'].astype(str) + load_df['kill_date'].astype(str)

    df = pandas.merge(carcass_df, load_df[['group_num', 'group_id']], on='group_id')

    load_dupl_df = load_df[load_df.duplicated(subset='group_id', keep=False)]
    group_id_dupl = load_dupl_df[~ load_dupl_df.duplicated(subset='group_id')]['group_id']

    df_reduced_dupl = df[df['group_id'].isin(group_id_dupl)]
    df_reduced = df[~ df['group_id'].isin(group_id_dupl)]

    pivot_df = pandas.pivot_table(df_reduced_dupl, index='group_id', values=['base_price_cwt', 'base_price', 'vob_cwt', 'vob', 'value_cwt', 'value'])
    pivot_df['group_id'] = pivot_df.index.tolist()

    load_dupl_full_df = pandas.merge(load_dupl_df, pivot_df, on='group_id')

    df_reduced['id'] = ""
    df_final = df_reduced[['id', 'group_num', 'plant', 'tattoo', 'kill_date', 'base_price_cwt', 'vob_cwt', 'value_cwt', 'base_price', 'vob', 'value', 'back_fat', 'loin_depth', 'yield', 'lean']]

    load_dupl_full_df['id'] = ""
    df_final_dupl = load_dupl_full_df[['id', 'group_num', 'plant', 'tattoo', 'kill_date', 'base_price_cwt', 'vob_cwt', 'value_cwt', 'base_price', 'vob', 'value', 'back_fat', 'loin_depth', 'yield', 'lean']]

    print df_reduced.head()
    print load_dupl_full_df.head()

##    print df_final.head()
##    print df_final_dupl.head()

def DietIngredientsQuery():
    session = CreateSession()
    a_query = session.query(Ingredients.ingredient, func.sum(Ingredients.quantity).label('quantity'), func.sum(Ingredients.cost).label('cost'))
##    a_query = a_query.join(Diets).filter(Diets.delivery_date.between(datetime.date(2016,1,1), datetime.date(2016,1,31)))
    a_query = a_query.join(Diets).filter(Diets.delivery_month == '2016 1')
    print a_query
    ingredients = a_query.group_by(Ingredients.ingredient).order_by(Ingredients.ingredient.asc()).all()
    schema = IngredientsSchema(many=True)
    result = schema.dump(ingredients)
    print result.data
    print simplejson.dumps({'ingredients' : result.data})

def GroupsQuery():
    session = CreateSession()
##    groups = session.query(func.sum(Diets.quantity).label('quantity'), func.sum(Diets.cost).label('cost'), Groups.group_num).join(Groups).group_by(Groups.group_num).order_by(Groups.group_num.desc()).all()

##    a_query = session.query(func.sum(Movements.quantity).label('quantity'), func.sum(Movements.weight).label('weight'), func.sum(Movements.cost).label('cost'), Movements.location_id.label('group_num'))
##    a_query = a_query.filter( (Movements.event_code == 'DIS') | (Movements.event_code=='PWP')).group_by(Movements.location_id)
    a_query = session.query(func.abs(func.sum(Movements.quantity)).label('quantity'), func.sum(Movements.weight).label('weight'), func.sum(Movements.cost).label('cost'), Movements.location_id.label('group_num'))
    a_query = a_query.filter( (Movements.location_type == 'group') | (Movements.location_type == 'sow_unit')).group_by(Movements.location_id)

    groups = a_query.order_by(Movements.location_id.desc()).all()

    schema = GroupsSchema(many=True)
    result = schema.dump(groups)
    print result.data
##    print jsonify({'groups' : result.data})



SalesAdd()
print "Done"

##CreateTable(Movements)
##print "Done"
    
##GroupsAdd()
##print "Added Groups"
##DeathsAdd()
##print "Added Deaths"
##DietsAdd()
##print "Added Diets"
##DietIngredientsAdd()
##print "Added Diet Ingredients"
##MovementsAdd()
##print "Added Movements"

##https://nsamteladze.wordpress.com/2015/07/26/bulk-insert-csv-into-azure-sql-database/
##https://msdn.microsoft.com/en-us/library/ms162802.aspx
