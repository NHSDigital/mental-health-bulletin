# Databricks notebook source
# DBTITLE 1,Permanent Lists
unsup_breakdowns = [
  "England",                     
  "England; Age Group (Higher Level)", 
  "England; Age Group (Lower Level)",                     
  "England; Gender", 
  "Gender; Age Group (Lower Level)",                     
  "England; Ethnicity (Lower Level)", 
  "England; Ethnicity (Higher Level)",                     
  "Ethnicity (Higher Level); Age Group (Higher Level)", 
  "Ethnicity (Higher Level); Age Group (Lower Level)", 
  "Ethnicity (Lower Level); Age Group (Lower Level)",
  "England; IMD Decile", 
  "England; IMD Quintile", 
  "IMD Decile; Age Group (Lower Level)", 
  "IMD Decile; Ethnicity (Higher Level)", 
  "IMD Decile; Gender; Age Group (Lower Level)",
  "England; Intervention Type", 
  "Intervention Type; Gender", 
  "Intervention Type; Age Group (Lower Level)", 
  "Intervention Type; Gender; Age Group (Lower Level)",
  "England; Service or Team Type", 
  "Service Type as Reported to MHSDS; Age Group (Higher Level)", 
  "Service Type as Reported to MHSDS; Ethnicity (Higher Level)", 
  "Service Type as Reported to MHSDS; Gender", 
  "Service Type as Reported to MHSDS; IMD Quintile", 
  "Service or Team Type; Age Group (Higher Level)", 
  "Service or Team Type; Ethnicity (Higher Level)", 
  "Service or Team Type; Gender", 
  "Service or Team Type; IMD Quintile",
  "England; Attendance Code", 
  "Attendance Code; Service or Team Type",
  "England; Bed Type as Reported to MHSDS", 
  "Bed Type as Reported to MHSDS; Age Group (Higher Level)", 
  "Bed Type as Reported to MHSDS; Ethnicity (Higher Level)", 
  "Bed Type as Reported to MHSDS; Gender", 
  "Bed Type as Reported to MHSDS; IMD Quintile",
  "England; Service Type as Reported to MHSDS",
  "Age Group (Higher Level); Provider Type",
  "Age Group (Lower Level); Provider Type",
  "Gender; Age Group (Lower Level); Provider Type",
  "Gender; Provider Type",
  "Provider Type"
]

geog_breakdowns = ["England", "Provider", "CCG - Registration or Residence", "STP of Residence", "Commissioning Region", "LAD/UA"]

activity_higher_than_england_totals = [
  "England; Bed Type", "Provider; Bed Type", "Sub ICB - GP Practice or Residence; Bed Type", 
  "Bed Type as Reported to MHSDS; Age Group (Higher Level)", "Bed Type as Reported to MHSDS; Ethnicity (Higher Level)", "Bed Type as Reported to MHSDS; Gender", "Bed Type as Reported to MHSDS; IMD Quintile",
  "England; Service or Team Type", 
  "Service Type as Reported to MHSDS; Age Group (Higher Level)", "Service Type as Reported to MHSDS; Ethnicity (Higher Level)", "Service Type as Reported to MHSDS; Gender", "Service Type as Reported to MHSDS; IMD Quintile",
  "Attendance Code; Service or Team Type", "England; Attendance Code"  
]

prov_prep_tables_metrics = ["1a", "1b", "1c", "1d", "1e", "4a"]

cyp_crude_rate_metrics = ["17b", "16k", "16l", "9b", "15o", "15r", "15u"] #aged 0 to 17
adu_crude_rate_metrics = ["16g", "16h", "10g", "10h", "10i", "10j", "10k", "15p", "15s", "15v"] #aged 18 to 64
oap_crude_rate_metrics = ["13e", "16i", "16j", "15q", "15t", "15w"] #aged 65 and over
peri_crude_rate_metrics = ["11b"] #females aged 15 to 54

csv_lookup_columns = ["REPORTING_PERIOD_START", "REPORTING_PERIOD_END", "STATUS", "BREAKDOWN", "LEVEL_ONE", "LEVEL_ONE_DESCRIPTION", "LEVEL_TWO", "LEVEL_TWO_DESCRIPTION", "LEVEL_THREE", "LEVEL_THREE_DESCRIPTION", "LEVEL_FOUR", "LEVEL_FOUR_DESCRIPTION", "METRIC", "METRIC_NAME", "SOURCE_DB"]

output_columns = ["REPORTING_PERIOD_START", "REPORTING_PERIOD_END", "STATUS", "BREAKDOWN", "LEVEL_ONE", "LEVEL_ONE_DESCRIPTION", "LEVEL_TWO", "LEVEL_TWO_DESCRIPTION", "LEVEL_THREE", "LEVEL_THREE_DESCRIPTION", "LEVEL_FOUR", "LEVEL_FOUR_DESCRIPTION", "METRIC", "METRIC_NAME", "METRIC_VALUE", "SOURCE_DB"]