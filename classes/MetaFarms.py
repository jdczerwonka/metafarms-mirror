URL_MF = "http://iprod2.metadesk.com/EnterpriseManager/"

JSON_PRODUCERS = "\\json\\producers.txt"
JSON_SITES = "\\json\\sites.txt"
JSON_FEED_MILLS = "\\json\\feed_mills.txt"
JSON_WEBSITE = "\\json\\website.txt"
JSON_REPORT_FIELDS = "\\json\\report_fields.txt"

import time
import os
import simplejson as json

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

class MetaFarms():
    def __init__(self, ID, download_path, github_path):
        self.download_path = download_path
        self.github_path = github_path

        self.json_producers = self.github_path + JSON_PRODUCERS
        self.json_sites = self.github_path + JSON_SITES
        self.json_feed_mills = self.github_path + JSON_FEED_MILLS
        self.json_web_site = self.github_path + JSON_WEBSITE
        self.json_report_fields = self.github_path + JSON_REPORT_FIELDS

        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        fp.set_preference("browser.download.dir", self.download_path)
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel")

        self.menu = json.load(open(self.json_web_site))
        self.report_field = json.load(open(self.json_report_fields))
        self.report_field['producer']['options'] = json.load(open(self.json_producers))
        self.report_field['site']['options'] = json.load(open(self.json_sites))
        self.report_field['feed_mill']['options'] = json.load(open(self.json_feed_mills))

        self.ID = ID
        self.driver = webdriver.Firefox(firefox_profile=fp)
        self.wait = WebDriverWait(self.driver, 10)

        self.driver.get(URL_MF + "Form_FM_Main_Menu_Select.aspx?cfid=" + self.ID)
        element = self.validateElement("companyname")

    def close(self):
        self.driver.close()

    def selectReportCheckbox(self, value, check_all = True):
        if check_all:
            element = self.validateElement( value, By.CLASS_NAME )
            element.click()
        else:
            x = "//li[@id='" + value + "']/label/input[@type='checkbox']"
            element = self.validateElement( x , By.XPATH )
            element.click()

    def selectReportDropdown(self, ID_key, value_key):
        element = self.validateElement( self.report_field[ID_key]["value"] )
        Select( element ).select_by_value( str(self.report_field[ID_key]["options"][value_key]["value"]) )

    def selectReportOption(self, value):
        element = self.validateElement( self.getElementByValue( value ) , By.XPATH )
        element.click()

    def selectReportButton(self, ID):
        element = self.validateElement( ID )
        element.click()    

    def inputReportText(self, ID, value, wait = True):
        element = self.validateElement( ID )
        element.clear()
        element.send_keys(value, Keys.ENTER)
        if wait:
            element = self.validateElement( self.getElementByValue( value , "input" ) , By.XPATH )

    def inputReportDates(self, start_date, end_date, selected_dates = "start_end_date"):
        if selected_dates == "start_end_date":
            self.inputReportText( self.report_field["start_date"]["value"] , start_date, False )
            self.inputReportText( self.report_field["end_date"]["value"] , end_date, False )
        else:
            self.selectReportDropdown( self.report_field["start_production_week"]["value"], start_date )
            self.selectReportDropdown( self.report_field["end_production_week"]["value"], end_date )        
    
    def validateElement(self, validation_str, validation_type = By.ID):
        return self.wait.until(EC.presence_of_element_located((validation_type, validation_str)))

    def getElementByValue(self, value, element_type = "option"):
        return "//" + element_type + "[@value='" + value + "']"
        
    def navigateMenu(self, menu_item, *arguments):
        self.driver.get(URL_MF + self.menu[menu_item]["value"])

        a_dict = self.menu[menu_item]["options"]
        option_type = self.menu[menu_item]["option_type"]
        for arg in arguments:
            if option_type == "option":
                self.selectReportOption( str(a_dict[arg]["value"]) )
            elif option_type == "button":
                self.selectReportButton( str(a_dict[arg]["value"]) )
                
            if "options" in a_dict[arg]:
                option_type = a_dict[arg]["option_type"]
                a_dict = a_dict[arg]["options"]

    def navigateToMenu(self):
        self.driver.get(URL_MF + "Form_FM_Main_Menu_Select.aspx")

    def waitForDownload(self, SearchStr):
        exist_bool = False
        while not exist_bool:
            for file in os.listdir(self.download_path):
                if file.find(SearchStr) != -1:
                    exist_bool = True
                    break

        part_bool = True
        while part_bool:
            part_bool = False
            for file in os.listdir(self.download_path):
                if file.find('.part') != -1:
                    part_bool = True
                    break

    def renameDownload(self, SearchStr, ReplaceStr):
        exist_bool = False
        while not exist_bool:
            for file in os.listdir(self.download_path):
                if file.find(SearchStr) != -1:
                    exist_bool = True
                    break

            if not exist_bool:
                time.sleep(1)

        time.sleep(2)

        part_bool = True
        while part_bool:
            part_bool = False
            for file in os.listdir(self.download_path):
                if file.find('.part') != -1:
                    part_bool = True
                    time.sleep(1)
                    break

        time.sleep(2)

        for file in os.listdir(self.download_path):
            if file.find(SearchStr) != -1:
                os.rename(self.download_path + "\\" + file, self.download_path + "\\" + ReplaceStr)
                exist_bool = True
                break


    def getData(self):
        self.getFeedMills
        self.getProducers
        self.getSites

    @property
    def getFeedMills(self):
        self.navigateMenu("feed_mills")
        
        elements = self.driver.find_elements(By.XPATH , "//tbody/tr[@valign='middle']")
        count = len(elements) - 1

        dict_feed_mill = {"All Feed Mills" : { "value" : "0" }}
        for num in range(1, count):
            self.driver.switch_to_window( self.driver.window_handles[0] )
            element = self.validateElement("ghxFeedMillMore_Data_" + str(num) )
            element.click()

            self.driver.switch_to_window( self.driver.window_handles[1] )
            
            element_name = self.validateElement("ctl00_MainContent_txtFeed_Mill")
            element_id = self.validateElement("ctl00_MainContent_lblFMID")
            dict_feed_mill[ str( element_name.get_attribute("value") ) ] = {"value" : str(element_id.text) }

            self.driver.close()

        self.driver.switch_to_window( self.driver.window_handles[0] )
        open(self.json_feed_mills, 'w').close()
        json.dump(dict_feed_mill, open(self.json_feed_mills, "w"))

        self.navigateToMenu()
    
    @property
    def getProducers(self):
        self.navigateMenu("producers_sites_barns", "search_producer")
        
        elements = self.driver.find_elements(By.XPATH , "//tbody/tr[@valign='middle']")
        count = len(elements) - 1

        dict_producer = {"All Producers" : { "value" : "0" }}
        for num in range(1, count):
            self.driver.switch_to_window( self.driver.window_handles[0] )
            element = self.validateElement("ghlLocMore_Data_" + str(num) )
            element.click()

            self.driver.switch_to_window( self.driver.window_handles[1] )
            
            element_name = self.validateElement("ctl00_MainContent_txtName")
            element_id = self.validateElement("ctl00_MainContent_lblFarmID")
            dict_producer[ str( element_name.get_attribute("value") ) ] = {"value" : str(element_id.text) }

            self.driver.close()

        self.driver.switch_to_window( self.driver.window_handles[0] )
        open(self.json_producers, 'w').close()
        json.dump(dict_producer, open(self.json_producers, "w"))

        self.navigateToMenu()

    @property
    def getSites(self):
        self.navigateMenu("producers_sites_barns", "search_site")

        self.selectReportDropdown("UI_DataNavigator1_ddPageSize", "100")
        time.sleep(.5)
        elements = self.driver.find_elements(By.XPATH , "//tbody/tr[@valign='middle']")
        count = len(elements) - 2

        dict_site = {"All Sites" : { "value" : "0" }}
        for num in range(1, count):
            self.driver.switch_to_window( self.driver.window_handles[0] )
            element = self.validateElement("ghlLocMore_Data_" + str(num) )
            element.click()

            self.driver.switch_to_window( self.driver.window_handles[1] )
            
            element_name = self.validateElement("ctl00_MainContent_txtName")
            element_id = self.validateElement("ctl00_MainContent_lblSiteID")
            dict_site[ str( element_name.get_attribute("value") ) ] = {"value" : str(element_id.text) }

            self.driver.close()

        self.driver.switch_to_window( self.driver.window_handles[0] )
        open(self.json_sites, 'w').close()
        json.dump(dict_site, open(self.json_sites, "w"))

        self.navigateToMenu()
        
    def getGroupDetailCloseout(self, GroupArr):
        self.navigateMenu("reports", "finish", "group_detail_closeout")
        
        for group in GroupArr:
            self.inputReportText( "ctl00_MainContent_UI_FARM_SITE1_txtUI_FARM_SITE_GroupMask" , group )
            self.selectReportButton( self.report_field["run_report"]["value"] )

        self.navigateToMenu()

    def getGroupList(self, report_by, report_by_value, group_type = "all_types", status = "all", report_layout = "metafarms_summary"):
        self.navigateMenu("reports", "finish", "group_list")

        self.selectReportDropdown( "group_type" , group_type )  
        self.selectReportDropdown( "status" , status )
        # self.selectReportDropdown( "report_layout" , report_layout )

        self.selectReportDropdown( "report_by" , report_by )
        self.selectReportDropdown( report_by , report_by_value )

        self.selectReportButton( self.report_field["run_report"]["value"] )

        self.renameDownload('Group_List', 'groups.xls')
        self.navigateToMenu()

    def getMortalityList(self, report_by, report_by_value, start_date, end_date, report_layout = "metafarms_summary"):
        self.navigateMenu("reports", "finish", "mortality_list")

        # self.selectReportDropdown( "report_layout" , report_layout )

        self.selectReportDropdown( "report_by" , report_by )
        self.selectReportDropdown( report_by , report_by_value )

        self.inputReportDates(start_date, end_date)

        # self.selectReportButton( self.report_field["run_report"]["value"] )

        self.renameDownload('Mortality_List', 'deaths.xls')
        self.navigateToMenu()       

    def getMovementReportSingleRow(self, report_by, report_by_value, start_date, end_date, date_type = "event_date", report_layout = "metafarms_summary"):
        self.navigateMenu("reports", "finish", "movement_report_single_row")

        self.selectReportDropdown( "date_type" , date_type )
        # self.selectReportDropdown( "report_layout" , report_layout )

        self.selectReportDropdown( "report_by" , report_by )
        self.selectReportDropdown( report_by , report_by_value )

        self.inputReportDates(start_date, end_date)

        # self.selectReportButton( self.report_field["run_report"]["value"] )

        self.renameDownload('Movement_Report_Single_Row', 'movements.xls')
        self.navigateToMenu()

    def getDietIngredientDetail(self, start_date, end_date, feed_mill = "All Feed Mills", diet_type = "all_diet_types", producer = "All Producers", site = "All Sites", group_type = "all_types", status = "all", report_layout = "metafarms_summary"):
        self.navigateMenu("reports", "finish", "diet_ingredient_detail")

        self.selectReportDropdown( "feed_mill" , feed_mill )
        self.selectReportDropdown( "diet_type" , diet_type )
        self.selectReportDropdown( "producer" , producer )
        self.selectReportDropdown( "site" , site )
        self.selectReportDropdown( "group_type" , group_type )
        # self.selectReportDropdown( "report_layout" , report_layout )

        self.inputReportDates(start_date, end_date)

        # self.selectReportButton( self.report_field["run_report"]["value"] )

        self.renameDownload('Diet_Ingredient_Detail', 'diets.xlsx')
        self.navigateToMenu()

    def getFeedUsageReport(self, start_date, end_date, report_by, report_by_value, feed_mill_check, selected_dates = "start_end_date"):
        self.navigateMenu("reports", "finish", "feed_usage_report")

        self.selectReportDropdown( "selected_dates" , selected_dates )  
        self.inputReportDates(start_date, end_date, selected_dates)
        
        self.selectReportDropdown( "report_by" , report_by )
        self.selectReportDropdown( report_by , report_by_value )

        time.sleep(.5)

        self.selectReportCheckbox( self.report_field["feed_mill_check"]["options"]["check_all"]["value"] )
        for feed_mill in feed_mill_check:
            self.selectReportCheckbox( self.report_field["feed_mill_check"]["options"][feed_mill]["value"] , False )

        self.selectReportButton( self.report_field["run_report"]["value"] )

        self.navigateToMenu()

    def getMarketSalesSummary(self, report_by, report_by_value, start_date, end_date, packer_check, report_layout = "metafarms_summary"):
        self.navigateMenu("reports", "sales", "market_sales_summary")

        self.selectReportDropdown( "report_by" , report_by )
        self.selectReportDropdown( report_by , report_by_value )

        self.selectReportCheckbox( self.report_field["packer_check"]["options"]["check_all"]["value"] )
        for feed_mill in packer_check:
            self.selectReportCheckbox( self.report_field["packer_check"]["options"][feed_mill]["value"] , False )

        # self.selectReportDropdown( "report_layout" , report_layout )
        self.inputReportDates(start_date, end_date)

        self.renameDownload('Market_Sales_Summary', 'sales.xls')
        self.navigateToMenu()