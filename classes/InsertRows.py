import datetime
import time
import math

import numpy
import pandas

from MetaFarms import MetaFarms as mf

class InsertRows():
    def __init__(self, Server = "", Database = "", Username = "", Password = ""):
        self.server = Server
        self.database = Database
        self.username = Username
        self.password = Password


        self.path_base = "C:\Users\Jonathan.WSF\Desktop\wsf databases"
        self.path_dict = {  'diets' : '\diets.csv',
                            'diet_ingredients' : '\diet_ingredients.csv',
                            'groups' : '\groups.csv',
                            'deaths' : '\deaths.csv',
                            'movements' : '\movements.csv',
                            'sales' : '\sales.csv'}

    @property
    def bcp_str(self):
        return 'bcp ' + self.database + '.dbo.' + self.curr_table + ' in ' + self.curr_save_path + ' -c -U ' +
            self.username + ' -S ' + self.server + ' -t \t -P ' + self.password 

    @property
    def curr_save_path(self):
        return self.path_base + self.path_dict[self.curr_table]

    def insert(self, TableStr):
        self.curr_table = TableStr
        if self.curr_table == 'diets':
            self.diets()
        elif self.curr_table == 'diet_ingredients':
            self.diet_ingredients()
        elif self.curr_table == 'groups':
            self.groups()
        elif self.curr_table == 'deaths':
            self.deaths()
        elif self.curr_table == 'movements':
            self.movements()
        elif self.curr_table == 'sales':
            self.sales()

        subprocess.call(self.bcp_str)

    def diets(self):       
        df = pandas.read_excel(EXCEL_PATH, sheetname="Ingredient Quantities", skiprows=18, parse_cols=(0, 3, 6, 9, 10, 11, 14))
        df.columns = ['group_num', 'delivery_date', 'diet', 'quantity', 'mill', 'order_id', 'cost']

        df['year'] = pandas.DatetimeIndex(df['delivery_date']).year
        df['month'] = pandas.DatetimeIndex(df['delivery_date']).month
        df['week'] = pandas.DatetimeIndex(df['delivery_date']).week
        df['delivery_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
        df['delivery_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

        df = df[['order_id', 'group_num', 'delivery_date', 'delivery_month', 'delivery_week', 'year', 'month', 'week', 'mill', 'diet', 'quantity', 'cost']]

        df.to_csv(self.curr_save_path, sep='\t', index=False)

    def diet_ingredients(self):      
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

        final_df.to_csv(self.curr_save_path, sep='\t', index=False)

    def groups(self):
        
        df = pandas.read_excel(EXCEL_PATH, sheetname="Report", skiprows=9, parse_cols=(4, 5, 6, 9, 13, 14, 15, 16))
        df.columns = ['producer', 'site', 'barn', 'group_num', 'group_type', 'status', 'open_date', 'close_date']
        df = df[['group_num', 'group_type', 'status', 'producer', 'site', 'barn', 'open_date', 'close_date']]

        df.to_csv(self.curr_save_path, sep='\t', index=False)

    def deaths(self):
        df = pandas.read_excel(EXCEL_PATH, sheetname="Mortality Report", skiprows=7, parse_cols=(8, 11, 13, 14, 15, 16, 19))
        df.columns = ['group_num', 'death_date', 'quantity', 'weight', 'death_type', 'reason', 'comments']

        df['year'] = pandas.DatetimeIndex(df['death_date']).year
        df['month'] = pandas.DatetimeIndex(df['death_date']).month
        df['week'] = pandas.DatetimeIndex(df['death_date']).week
        df['death_month'] = df['year'].astype(str) + " " + df['month'].astype(str)
        df['death_week'] = (df['year'] - ((df['week'] == 53) & (df['month'] == 1))).astype(str) + " " + df['week'].astype(str)

        df['id'] = ""
        df = df[['id', 'group_num', 'death_date', 'death_month', 'death_week', 'year', 'month', 'week', 'death_type', 'reason', 'comments', 'quantity', 'weight']]
        
        df.to_csv(self.curr_save_path, sep='\t', index=False)

    def movements(self):
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

        df_final.sort(columns=['movement_date', 'movement_id']).to_csv(self.curr_save_path, sep='\t', index=False)

    def sales(self):

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
