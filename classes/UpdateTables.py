import os
import datetime
import time
import math
import subprocess

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import numpy
import pandas
import openpyxl

from MetaFarms import MetaFarms as mf
from Tables import *

class UpdateTables():
    def __init__(self, Server, Database, Username, Password, CFID, BasePath, DownloadDict):
        self.server = Server
        self.database = Database
        self.username = Username
        self.password = Password

        self.db_uri = 'mssql+pyodbc://' + self.username + ':' + self.password + '@' + self.server + '/' + self.database + '?driver=SQL+Server+Native+Client+11.0'

        self.cfid = CFID

        self.base_path = BasePath
        self.upload_path = self.base_path + "\uploads"
        self.download_path = self.base_path + "\downloads"
        self.error_path = self.base_path + "\errors"

        self.save_path_dict = { 'diets' : '\diets.csv',
                                'ingredients' : '\ingredients.csv',
                                'groups' : '\groups.csv',
                                'deaths' : '\deaths.csv',
                                'movements' : '\movements.csv',
                                'sales' : '\sales.csv'}

        self.error_path_dict = {'diets' : '\diets-error.txt',
                                'ingredients' : '\ingredients-error.txt',
                                'groups' : '\groups-error.txt',
                                'deaths' : '\deaths-error.txt',
                                'movements' : '\movements-error.txt',
                                'sales' : '\sales-error.txt'}

        self.open_path_dict = { 'diets' : '\diets.xlsx',
                                'ingredients' : '\diets.xlsx',
                                'groups' : '\groups.xls',
                                'deaths' : '\deaths.xls',
                                'movements' : '\movements.xls',
                                'sales' : '\sales.xls'}

        self.mf = mf(self.cfid, self.download_path)
        self.download_dict = DownloadDict
        self.end_date = datetime.date.today()
        self.start_date = {}

        self.download_files
        self.create_uploads
        self.update_tables

        time.sleep(10)

        self.delete_files

    @property
    def download_files(self):
        for key in self.download_dict:
            self.start_date[key] = (self.end_date - datetime.timedelta(days=self.download_dict[key]))
            if key == 'diets':
                self.mf.getDietIngredientDetail(self.start_date[key].strftime("%m/%d/%Y"), self.end_date.strftime("%m/%d/%Y"), 'Home Mill')
                print 'diets successfully downloaded'

            elif key == 'groups':
                self.mf.getGroupList('producer', 'All Producers')
                print 'groups successfully downloaded'

            elif key == 'movements':
                self.mf.getMovementReportSingleRow('producer', 'All Producers', self.start_date[key].strftime("%m/%d/%Y"), self.end_date.strftime("%m/%d/%Y"))
                print 'movements successfully downloaded'

                self.mf.getMortalityList('producer', 'All Producers', self.start_date[key].strftime("%m/%d/%Y"), self.end_date.strftime("%m/%d/%Y"))
                print 'deaths successfully downloaded'

            elif key == 'sales':
                self.mf.getMarketSalesSummary('producer', 'All Producers', self.start_date[key].strftime("%m/%d/%Y"), self.end_date.strftime("%m/%d/%Y"), ['jbs', 'ipc'])
                print 'sales successfully downloaded'

        self.mf.close()

    @property
    def create_uploads(self):
        for key in self.download_dict:
            if key == 'diets':
                self.diets()
                print 'diets successfully parsed'

                self.ingredients()
                print 'ingredients successfully parsed'

            elif key == 'groups':
                self.groups()
                print 'groups successfully parsed'

            elif key == 'movements':
                self.movements()
                print 'movements successfully parsed'

                self.deaths()
                print 'deaths successfully parsed'

            elif key == 'sales':
                self.sales()
                print 'sales successfully parsed'

    @property
    def update_tables(self):
        session = self.create_session()

        for key in self.download_dict:
            if key == 'diets':
                session.query(Ingredients).filter(Ingredients.delivery_date >= self.start_date[key]).delete()
                session.commit()
                print 'ingredients successfully deleted'

                subprocess.call(self.bcp_str('ingredients', 'ingredients'))
                print 'ingredients successfully updated'

                session.query(Diets).filter(Diets.delivery_date >= self.start_date[key].strftime("%m/%d/%Y")).delete()
                session.commit()
                print 'diets successfully deleted'

                subprocess.call(self.bcp_str('diets', 'diets'))
                print 'diets successfully updated'


            elif key == 'groups':
                print 'groups successfully deleted'
                print 'groups successfully updated'

            elif key == 'movements':
                session.query(Movements).filter(Movements.movement_date >= self.start_date[key].strftime("%m/%d/%Y")).delete()
                session.commit()
                print 'movements successfully deleted'
                
                subprocess.call(self.bcp_str('movements', 'movements'))
                print 'movements successfully updated'

                subprocess.call(self.bcp_str('movements', 'deaths'))
                print 'deaths successfully updated'

            elif key == 'sales':
                session.query(Sales).filter(Sales.kill_date >= self.start_date[key].strftime("%m/%d/%Y")).delete()
                session.commit()
                print 'sales successfully deleted'
                
                subprocess.call(self.bcp_str('sales', 'sales'))
                print 'sales successfully updated'

        session.close()

    @property
    def delete_files(self):
        for file in os.listdir(self.download_path):
            os.remove(self.download_path + "\\" + file)

        print 'downloads deleted'

        for file in os.listdir(self.upload_path):
            os.remove(self.upload_path + "\\" + file)

        print 'uploads deleted'

    def create_session(self):
        engine = create_engine(self.db_uri)
        Base.metadata.bind = engine
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        return session

    def bcp_str(self, TableStr, ReportStr):
        # str_arr = ["bcp ", self.database, ".dbo.", TableStr, " in ", self.save_path(ReportStr), " -c -U ", self.username, " -S ", self.server, " -t \\t -P ", self.password, self.error_file_path(ReportStr)]
        # return ''.join(str_arr)
        return "bcp " + self.database + ".dbo." + TableStr + " in " + self.save_path(ReportStr) + " -c -U " + self.username + " -S " + self.server + " -t \\t -P " + self.password + " -e " + self.error_file_path(ReportStr)

    def save_path(self, ReportStr):
        return self.upload_path + self.save_path_dict[ReportStr]

    def open_path(self, ReportStr):
        return self.download_path + self.open_path_dict[ReportStr]

    def error_file_path(self, ReportStr):
        return self.error_path + self.error_path_dict[ReportStr]

    def diets(self):
        df = pandas.read_excel(self.open_path('diets'), sheetname="Ingredient Quantities", skiprows=18, parse_cols=(0, 3, 6, 9, 10, 11, 14))
        df.columns = ['group_num', 'delivery_date', 'diet', 'quantity', 'mill', 'order_id', 'cost']

        df['year'] = pandas.DatetimeIndex(df['delivery_date']).year
        df['month'] = pandas.DatetimeIndex(df['delivery_date']).month
        df['week'] = pandas.DatetimeIndex(df['delivery_date']).week
        df['delivery_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
        df['delivery_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

        df['id'] = ""
    
        df = df[['id', 'order_id', 'group_num', 'mill', 'delivery_date', 'delivery_month', 'delivery_week', 'year', 'month', 'week', 'diet', 'quantity', 'cost']]

        df.to_csv(self.save_path('diets'), sep='\t', index=False, header=False)

    def ingredients(self):
        ADD_COL = 4

        wb = openpyxl.load_workbook(self.open_path('ingredients'), read_only=True, use_iterators=True)
        ws = wb['Ingredient Quantities']
        max_col = ws.max_column
        wb._archive.close()

        col_arr = range(20, max_col)
        col_arr.insert(0, 11)
        col_arr.insert(0, 10)
        col_arr.insert(0, 3)
        col_arr.insert(0, 0)
        

        qty_df = pandas.read_excel(self.open_path('ingredients'), sheetname="Ingredient Quantities", skiprows=18, parse_cols=col_arr)
        col_names = range(0, max_col - 20 + ADD_COL)

        col_names[0:ADD_COL] = qty_df.columns[0:ADD_COL]
        for x in range(ADD_COL, max_col - 20 + ADD_COL):
            s = qty_df.columns[x]
            col_names[x] = s[:s.find('(') - 1]

        qty_df.columns = col_names
        qty_df.rename(columns={"Order ID" : "order_id", "Group" : "group_num", "Delivery Date" : "delivery_date", "Mill" : "mill"}, inplace=True)
        qty_df_melt = pandas.melt(qty_df, id_vars = ['order_id', 'group_num', 'delivery_date', 'mill'], var_name = 'ingredient', value_name='quantity')
        qty_df_melt = qty_df_melt.dropna()
        qty_df_melt['id'] = qty_df_melt['order_id'] + qty_df_melt['ingredient']

        

        cost_df = pandas.read_excel(self.open_path('ingredients'), sheetname="Ingredient Cost", skiprows=18, parse_cols=col_arr)
        col_names = range(0, max_col - 20 + ADD_COL)

        col_names[0:ADD_COL] = cost_df.columns[0:ADD_COL]
        for x in range(ADD_COL, max_col - 20 + ADD_COL):
            s = cost_df.columns[x]
            if s[len(s) - 2:] == '.1':
                col_names[x] = s[:len(s) - 2]
            else:
                col_names[x] = s

        cost_df.columns = col_names
        cost_df.rename(columns={"Order ID" : "order_id"}, inplace=True)
        cost_df_melt = pandas.melt(cost_df, id_vars = 'order_id', var_name = 'ingredient', value_name='cost')
        cost_df_melt = cost_df_melt.dropna()
        cost_df_melt['id'] = cost_df_melt['order_id'] + cost_df_melt['ingredient']

        

        df = pandas.merge(qty_df_melt, cost_df_melt[['id', 'cost']], on='id')

        df['year'] = pandas.DatetimeIndex(df['delivery_date']).year
        df['month'] = pandas.DatetimeIndex(df['delivery_date']).month
        df['week'] = pandas.DatetimeIndex(df['delivery_date']).week
        df['delivery_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
        df['delivery_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

        df['quantity'] = numpy.round(df['quantity'].astype(float), 2)
        df['cost'] = numpy.round(df['cost'].astype(float), 2)
        
        df['id'] = ""
        df = df[['id', 'order_id', 'group_num', 'mill', 'delivery_date', 'delivery_month', 'delivery_week', 'year', 'month', 'week', 'ingredient', 'quantity', 'cost']]

        df.to_csv(self.save_path('ingredients'), sep='\t', index=False, header=False)

    def groups(self):
        excel_df = pandas.read_excel(self.open_path('groups'), sheetname="Report", skiprows=9, parse_cols=(4, 5, 6, 9, 13, 14, 15, 16))
        excel_df.columns = ['producer', 'site', 'barn', 'group_num', 'group_type', 'status', 'open_date', 'close_date']
        excel_df = excel_df[['group_num', 'group_type', 'status', 'producer', 'site', 'barn', 'open_date', 'close_date']]

        engine = create_engine(DB_URI)
        sql_df = pandas.read_sql('groups', engine)

        df_comb = excel_df.append(sql_df)
        group_num_unique = df_comb[~df_comb.duplicated(subset='group_num', keep=False)]['group_num']

        group_num_remove = sql_df[sql_df['group_num'].isin(group_num_unique)]['group_num']
        add_df = excel_df[excel_df['group_num'].isin(group_num_unique)]

        

        group_num_change_df = df_comb[~df_comb.duplicated(subset=['group_num', 'status', 'open_date', 'close_date'], keep=False)]
        group_num_change = group_num_change_df[group_num_change_df.duplicated(subset='group_num')]['group_num']

        change_df = excel_df[excel_df['group_num'].isin(group_num_change)][['group_num', 'status', 'open_date', 'close_date']]
        change_df = change_df.fillna("")


        
        session = self.create_session()
        
        if not group_num_unique.empty:
            session.query(Groups).filter(Groups.group_num.in_(group_num_remove)).delete(synchronize_session=False)
            session.commit()

        if not change_df.empty:
            for index, row in change_df.iterrows():
                print row['group_num']
                session.query(Groups).filter(Groups.group_num == row['group_num']).update({'status' : row['status'], 'open_date' : row['open_date'], 'close_date' : row['close_date']})

            session.commit() 

        if not add_df.empty:
            engine = create_engine(self.db_uri)
            add_df.to_sql('groups', engine, flavor='mssql', if_exists='append', index=False)

        session.close()

    def deaths(self):
        df = pandas.read_excel(self.open_path('deaths'), sheetname="Mortality Report", skiprows=7, parse_cols=(8, 11, 13, 14, 15, 16))
        df.columns = ['entity_from', 'movement_date', 'quantity', 'weight', 'event_category_from', 'event_name_from']

        df['year'] = pandas.DatetimeIndex(df['movement_date']).year
        df['month'] = pandas.DatetimeIndex(df['movement_date']).month
        df['week'] = pandas.DatetimeIndex(df['movement_date']).week
        df['movement_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
        df['movement_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

        df['entity_to'] = ""
        df['entity_type_to'] = ""
        df['event_category_to'] = ""
        df['event_code_to'] = ""
        df['event_name_to'] = ""
        df['entity_type_from'] = 'group'
        df['event_code_from'] = ""
        df['cost'] = 0
        df['id'] = ""

        df = df[['id', 'entity_to', 'entity_type_to', 'event_category_to', 'event_code_to', 'event_name_to', 'entity_from', 'entity_type_from', 'event_category_from', 'event_code_from', 'event_name_from', 'movement_date', 'movement_month', 'movement_week', 'year', 'month', 'week', 'quantity', 'weight', 'cost']]

        df.to_csv(self.save_path('deaths'), sep='\t', index=False, header=False)

    def movements(self):
        df = pandas.read_excel(self.open_path('movements'), sheetname="Summary", skiprows=9, parse_cols=(2, 6, 10, 12, 15, 20, 21, 22, 23, 26, 28, 29, 31))
        df.columns = ['sow_unit', 'group', 'supplier', 'customer', 'plant', 'event_category', 'event_code', 'event_name', 'movement_id', 'movement_date', 'quantity', 'weight', 'cost']

        df_non_sow = df[df['sow_unit'].isnull()]
        df_sow = df[~df['sow_unit'].isnull()]
        df_group = df_sow[~df_sow['group'].isnull()]
        df_sow = df_sow[df_sow['group'].isnull()]

        df_group['sow_unit'] = float('NaN')

        df = df_non_sow.append(df_sow, ignore_index=True)
        df = df.append(df_group, ignore_index=True)

        df_melt = pandas.melt(df, id_vars=['event_category', 'event_code', 'event_name', 'movement_id', 'movement_date', 'quantity', 'weight', 'cost'], value_vars=['sow_unit', 'group', 'supplier', 'customer', 'plant'], var_name='entity_type', value_name='entity')
        df_melt = df_melt[~df_melt['entity'].isnull()]

        df_pos = df_melt[df_melt['quantity'] > 0]
        df_neg = df_melt[df_melt['quantity'] < 0]

        df_pos.rename(columns={"event_category" : "event_category_to", "event_code" : "event_code_to", "event_name" : "event_name_to", "entity" : "entity_to", "entity_type" : "entity_type_to"}, inplace=True)
        df_neg.rename(columns={"event_category" : "event_category_from", "event_code" : "event_code_from", "event_name" : "event_name_from", "entity" : "entity_from", "entity_type" : "entity_type_from"}, inplace=True)

        df = pandas.merge(df_pos, df_neg[['movement_id', 'event_category_from', 'event_code_from', 'event_name_from', 'entity_from', 'entity_type_from']], on='movement_id')

        df['year'] = pandas.DatetimeIndex(df['movement_date']).year
        df['month'] = pandas.DatetimeIndex(df['movement_date']).month
        df['week'] = pandas.DatetimeIndex(df['movement_date']).week
        df['movement_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
        df['movement_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

        df['id'] = ""

        df = df[['id', 'entity_to', 'entity_type_to', 'event_category_to', 'event_code_to', 'event_name_to', 'entity_from', 'entity_type_from', 'event_category_from', 'event_code_from', 'event_name_from', 'movement_date', 'movement_month', 'movement_week', 'year', 'month', 'week', 'quantity', 'weight', 'cost']]

        df.to_csv(self.save_path('movements'), sep='\t', index=False, header=False)

    def sales(self):
        pandas.options.mode.chained_assignment = None  # default='warn'

        carcass_df = pandas.read_excel(self.open_path('sales'), sheetname="Carcass Details", skiprows=9, parse_cols=(1, 4, 6, 7, 8, 9, 10, 14, 15, 16, 20, 21, 22, 23, 24, 31))
        carcass_df.columns = ['plant', 'tattoo', 'kill_date', 'quantity', 'avg_live_wt', 'avg_carcass_wt', 'base_price_cwt', 'value_cwt', 'vob_cwt', 'base_price', 'value', 'vob', 'back_fat', 'loin_depth', 'lean', 'yield']

        load_df = pandas.read_excel(self.open_path('sales'), sheetname="Sales", skiprows=21, parse_cols=(20, 28, 34, 37))
        load_df.columns = ['group_num', 'kill_date', 'plant', 'tattoo']
        
        carcass_df['group_id'] = carcass_df['tattoo'].astype(str) + ".0" + carcass_df['kill_date'].astype(str)
        load_df['group_id'] = load_df['tattoo'].astype(str) + load_df['kill_date'].astype(str)
        group_id_dupl = load_df[load_df.duplicated(subset='group_id', keep=False)]['group_id']

        df = pandas.merge(carcass_df, load_df[['group_num', 'group_id']], on='group_id')
        df_final = df[~ df['group_id'].isin(group_id_dupl)]

        df_final['avg_carcass_wt'] = numpy.round(df_final['avg_carcass_wt'], 0)
        df_final['avg_live_wt'] = numpy.round(df_final['avg_live_wt'], 2)
        df_final['lean'] = df_final['lean'] * 100
        df_final['base_price_cwt'] = numpy.round(df_final['base_price_cwt'], 2)
        df_final['vob_cwt'] = numpy.round(df_final['vob_cwt'], 2)
        df_final['value_cwt'] = numpy.round(df_final['value_cwt'], 2)
        df_final['base_price'] = numpy.round(df_final['base_price'], 2)
        df_final['vob'] = numpy.round(df_final['vob'], 2)
        df_final['value'] = numpy.round(df_final['value'], 2)

        df_final['year'] = pandas.DatetimeIndex(df_final['kill_date']).year
        df_final['month'] = pandas.DatetimeIndex(df_final['kill_date']).month
        df_final['week'] = pandas.DatetimeIndex(df_final['kill_date']).week
        df_final['kill_month'] = df_final['year'].astype(str) + " " + df_final['month'].astype(str)
        df_final['kill_week'] = (df_final['year'] - ((df_final['week'] == 53) & (df_final['month'] == 1))).astype(str) + " " + df_final['week'].astype(str)

        df_final['id'] = ""
        df_final = df_final[['id', 'group_num', 'plant', 'tattoo', 'kill_date', 'kill_month', 'kill_week', 'year', 'month', 'week', 'quantity', 'avg_live_wt', 'avg_carcass_wt', 'base_price_cwt', 'vob_cwt', 'value_cwt', 'base_price', 'vob', 'value', 'back_fat', 'loin_depth', 'yield', 'lean']]

        df_final.to_csv(self.save_path('sales'), sep='\t', index=False, header=False)
