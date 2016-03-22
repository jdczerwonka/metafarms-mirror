URL_MF = "http://iprod2.metadesk.com/EnterpriseManager/"

JSON_PRODUCERS = "json/producers.txt"
JSON_SITES = "json/sites.txt"

import time
import os
import simplejson as json

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from dict import mf_dict, report_field_dict

class MetaFarms():
    def __init__(self, ID, download_path):
        self.download_path = download_path
        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        fp.set_preference("browser.download.dir", self.download_path)
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/vnd.ms-excel")

        self.menu = mf_dict
        self.report_field = report_field_dict

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

    def selectReportDropdown(self, ID, value):
        element = self.validateElement( ID )
        Select( element ).select_by_value( value )

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

    def inputReportDates(self, selected_dates, start_date, end_date):
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
        open(JSON_PRODUCERS, 'w').close()
        json.dump(dict_producer, open(JSON_PRODUCERS, "w"))

        self.navigateToMenu()

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
        open(JSON_SITES, 'w').close()
        json.dump(dict_site, open(JSON_SITES, "w"))

        self.navigateToMenu()
        
    def getGroupDetailCloseout(self, GroupArr):
        self.navigateMenu("reports", "finish", "group_detail_closeout")
        
        for group in GroupArr:
            self.inputReportText( "ctl00_MainContent_UI_FARM_SITE1_txtUI_FARM_SITE_GroupMask" , group )
            self.selectReportButton( self.report_field["run_report"]["value"] )

        self.navigateToMenu()

        x = 0
        for file in os.listdir(self.download_path):
            if os.path.isfile(file):
                os.rename(file, GroupArr[x] + ".xls")
                x = x + 1

    def getGroupList(self, report_by, report_by_value, group_type = "all_types", status = "all", report_layout = "metafarms_summary"):
        self.navigateMenu("reports", "finish", "group_list")

        self.selectReportDropdown( self.report_field["group_type"]["value"] , self.report_field["group_type"]["options"][group_type]["value"] )  
        self.selectReportDropdown( self.report_field["status"]["value"] , self.report_field["status"]["options"][status]["value"] )
##        self.selectReportDropdown( self.report_field["report_layout"]["value"] , self.report_field["report_layout"]["options"][report_layout]["value"] )

        self.selectReportDropdown( self.report_field["report_by"]["value"] , self.report_field["report_by"]["options"][report_by]["value"] )
        self.selectReportDropdown( self.report_field[ report_by ]["value"] , self.report_field[ report_by ]["options"][report_by_value]["value"] )

        self.selectReportButton( self.report_field["run_report"]["value"] )

        self.navigateToMenu()

    def getMovementReport(self, start_date, end_date, report_by, report_by_value, selected_dates = "start_end_date", date_type = "event_date", report_layout = "metafarms_summary"):
        self.selectReportDropdown( self.report_field["selected_dates"]["value"] , self.report_field["selected_dates"]["options"][selected_dates]["value"] )  
        self.selectReportDropdown( self.report_field["date_type"]["value"] , self.report_field["date_type"]["options"][date_type]["value"] )
        self.selectReportDropdown( self.report_field["report_layout"]["value"] , self.report_field["report_layout"]["options"][report_layout]["value"] )

        self.selectReportDropdown( self.report_field["report_by"]["value"] , self.report_field["report_by"]["options"][report_by]["value"] )
        self.selectReportDropdown( self.report_field[ report_by ]["value"] , self.report_field[ report_by ]["options"][report_by_value]["value"] )

        self.inputReportDates(selected_dates, start_date, end_date)

        self.selectReportButton( self.report_field["run_report"]["value"] )

        self.navigateToMenu()

    def getFeedUsageReport(self, start_date, end_date, report_by, report_by_value, feed_mill_check, selected_dates = "start_end_date"):
        self.navigateMenu("reports", "finish", "feed_usage_report")

        self.selectReportDropdown( self.report_field["selected_dates"]["value"] , self.report_field["selected_dates"]["options"][selected_dates]["value"] )  
        self.inputReportDates(selected_dates, start_date, end_date)
        
        self.selectReportDropdown( self.report_field["report_by"]["value"] , self.report_field["report_by"]["options"][report_by]["value"] )
        self.selectReportDropdown( self.report_field[ report_by ]["value"] , self.report_field[ report_by ]["options"][report_by_value]["value"] )

        time.sleep(.5)

        self.selectReportCheckbox( self.report_field["feed_mill_check"]["options"]["check_all"]["value"] )
        for feed_mill in feed_mill_check:
            self.selectReportCheckbox( self.report_field["feed_mill_check"]["options"][feed_mill]["value"] , False )

        self.selectReportButton( self.report_field["run_report"]["value"] )

        self.navigateToMenu()
