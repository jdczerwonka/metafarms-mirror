import simplejson as json

producers_dict = json.load(open("json/producers.txt"))
sites_dict = json.load(open("json/sites.txt"))

mf_dict = {   "reports" :
                    {   "value" : "Form_SS_Report_Control_Edit.aspx",
                        "option_type" : "option",
                        "options" :
                            {   "enterprise" :
                                    { "value": "A" },
                                "exports" :
                                    { "value" : "DT" },
                                "finish" :
                                    {   "value" : "FM",
                                        "option_type" : "option",
                                        "options" :
                                            {   "active_group_summary" : 43467,
                                                "active_groups_by_week_on_feed" : 43468,
                                                "assurance_site_history" : 53544,
                                                "closeout_performance_monitor" : 43469,
                                                "closeout_summary" : 43470,
                                                "combined_group_closeout" : 43471,
                                                "continuous_flow_performance_Report" : 43472,
                                                "current_group_mortality" : 43473,
                                                "diet_ingredient_detail" : 43474,
                                                "diet_price_comparison_by_feed_mill" : 43475,
                                                "feed_budget_comparison" : 43476,
                                                "feed_deliveries" : 43477,
                                                "feed_deliveries_detail" : 43478,
                                                "feed_ingredient_bid_pricing" : 43479,
                                                "feed_performance" : 43480,
                                                "feed_usage_report" : { "value" : 43481 },
                                                "finish_physical_count" : 43482,
                                                "grain_bank_perpetual_report" : 43483,
                                                "group_cost_valueation" : 43484,
                                                "group_cumulative_mortality" : 43485,
                                                "group_detail_closeout" : { "value" : 43486 },
                                                "group_diet_report" : 43487,
                                                "group_exceptions" : 43488,
                                                "group_inventory_distribution" : 43491,
                                                "group_inventory_distribution_monthly" : 43492,
                                                "group_list" : { "value" : 43493 },
                                                "group_movement" : 43494,
                                                "group_sources" : 43495,
                                                "ingredient_price" : 43496,
                                                "inventory_by_production_week" : 43497,
                                                "inventory_flow_age_on_feed" : 43498,
                                                "inventory_flow_weeks_on_feed" : 43499,
                                                "invoice_balancing_report" : 43500,
                                                "location_history" : 43501,
                                                "mortality_analysis" : 43502,
                                                "mortality_by_calendar_week" : 43503,
                                                "mortality_by_production_week" : 43504,
                                                "mortality_by_week_on_feed" : 43505,
                                                "mortality_list" : 43506,
                                                "movement_report" : { "value" : 43507 },
                                                "movement_report_single_row" : 43508,
                                                "recalc_active_group_active_group_summary" : 43509,
                                                "scheduled_groups" : 43510
                                            }
                                    },
                                "sales" : { "value" : "CR" },
                                "sow_analytics" : { "value" : "SSMART" },
                                "sow" : { "value" : "SWM" },
                                "supervisor" : { "value" : "SU" },
                                "system" : { "value" : "SM" }
                            }
                    },
                "general_attributes" : { "value" : "Form_FM_Table_Entry.aspx" },
                "producers_sites_barns" :
                    {   "value" : "form_fm_producersitesetup_entry.aspx",
                        "option_type" : "button", 
                        "options" :
                            {   "search_producer" : { "value" : "btnFarm" },
                                "search_site" : { "value" : "btnAllSite" }
                            }
                    }
            }

report_field_dict = {   "report_by" :
                            {   "value" : "ctl00_MainContent_drpReportBy",
                                "options":
                                    {   "producer" : { "value" : "4" },
                                        "site" : { "value" : "5" },
                                        "busines_unit" : { "value" : 3 },
                                        "feed_mill" : { "value" : 9 },
                                        "pod" : { "value" : 11 },
                                        "supervisor" : { "value" : "9001" },
                                        "pig_flow" : { "value" : "2" },
                                        "pig_owner" : { "value" : "34" },
                                        "event_code" : { "value" : "16" },
                                        "event_category" : {"value" : "17" }
                                    }
                            },
                        "producer" :
                            {   "value" : "ctl00_MainContent_UI_FARM_SITE1_drpFarm",
                                "options" : producers_dict
                            },
                        "site" :
                            {   "value" : "ctl00_MainContent_UI_FARM_SITE1_drpSite",
                                "options" : sites_dict
                            },
                        "feed_mill_check" :
                            {   "value" : "ctl00_MainContent_lblGeneric1Name" ,
                                "options" :
                                {   "check_all" : { "value" : "rlbCheckAllItemsCheckBox" },
                                    "graymont_coop_association" : { "value" : "ctl00_MainContent_lbGeneric1_i0" },
                                    "home_mill" : { "value" : "ctl00_MainContent_lbGeneric1_i1" },
                                    "south_central_fs" : { "value" : "ctl00_MainContent_lbGeneric1_i2" }
                                }
                            },
                        "selected_dates" :
                            {   "value" : "ctl00_MainContent_drpNewDateRange",
                                "options" :
                                    {   "start_end_date" : { "value" : "1" },
                                        "start_end_week" : { "value" : "2" }
                                    }
                            },
                        "date_type" :
                            {   "value" : "ctl00_MainContent_drpDateType",
                                "options" :
                                    {   "event_date" : { "value" : "1" },
                                         "applied_date" : { "value" : "2" }
                                    }
                            },
                        "group_type" :
                            {   "value" : "ctl00_MainContent_drpType",
                                "options" :
                                    {   "all_types" : { "value" : "0" },
                                        "nursery" : { "value" : "1" },
                                        "finish" : { "value" : "2" },
                                        "wean_to_finish" : { "value" : "3" },
                                        "gdu" : { "value" : "4" }
                                    }
                            },
                        "status" :
                            {   "value" : "ctl00_MainContent_drpGroupStatus",
                                "options" :
                                    {   "all" : { "value" : "0" },
                                        "active" : { "value" : "1" },
                                        "closed" : { "value" : "2" },
                                        "schedule" : { "value" : "3" },
                                        "inactive" : { "value" : "4" }
                                    }
                            },
                        "start_date" : { "value" : "ctl00_MainContent_txtStartDate" },
                        "end_date" : { "value" : "ctl00_MainContent_txtEndDate" },
                        "start_production_week" : { "value" : "ctl00_MainContent_drpStartYearWeek" },
                        "end_production_week" : { "value" : "ctl00_MainContent_drpEndYearWeek" },
                        "report_layout" :
                            {   "value" : "ctl00_MainContent_drpCustomReport",
                                "options" :
                                    {   "metafarms_summary" : { "value" : "-12" },
                                        "metafarms_full" : { "value" : "0" }
                                    }
                            },
                        "run_report" : { "value" : "ctl00_MainContent_btnRunReport_input" }
                    }
