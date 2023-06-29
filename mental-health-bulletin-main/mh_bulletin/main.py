# Databricks notebook source
#initialise widgets
dbutils.widgets.text("db_output", "output_database") #output database
dbutils.widgets.text("db_source", "mh_database") #mhsds database
dbutils.widgets.text("month_id_end", "1452") #uniqmonthid in mhs000header for end month of financial year
dbutils.widgets.text("month_id_start", "1441", "month_id_start") #uniqmonthid in mhs000header for start month of financial year
dbutils.widgets.text("rp_enddate", "2021-03-31") #end date of the financial year
dbutils.widgets.text("rp_startdate", "2020-04-01", "rp_startdate") #start date of the financial year
dbutils.widgets.text("populationyear", "2020") #year of count for population data
dbutils.widgets.text("status", "Final") #mhsds submission window
dbutils.widgets.text("IMD_year", "2019", "IMD_year") #year for deprivation data (usually in 2 year cycles)
dbutils.widgets.text("LAD_published_date", "2020-03-27") #date at which latest local authority reference data was published

#get widgets as python variables
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id_end = dbutils.widgets.get("month_id_end")
month_id_start = dbutils.widgets.get("month_id_start")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
populationyear  = dbutils.widgets.get("populationyear")
status  = dbutils.widgets.get("status")
IMD_year  = dbutils.widgets.get("IMD_year")
LAD_published_date  = dbutils.widgets.get("LAD_published_date")

#create custom parameter json for mh bulletin run
mh_bulletin_params = {
  'db_source': db_source, 
  'db_output': db_output, 
  'rp_enddate': rp_enddate, 
  'rp_startdate': rp_startdate, 
  'month_id_end': month_id_end, 
  'status': status, 
  'IMD_year': IMD_year, 
  'populationyear': populationyear, 
  'month_id_start': month_id_start, 
  'LAD_published_date': LAD_published_date
}

# COMMAND ----------

#fix for table ownership bug
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation','true')

#create needed permanent tables for mh bulletin run
dbutils.notebook.run('setup/create_tables', 0, mh_bulletin_params)
dbutils.notebook.run('setup/mh_bulletin_assets', 0, mh_bulletin_params)
dbutils.notebook.run('setup/csv_master', 0, mh_bulletin_params)
dbutils.notebook.run('setup/population', 0, mh_bulletin_params)

# COMMAND ----------

#generic prep for all chapters such as creating an mhs001mpi table with derived fields
dbutils.notebook.run('prep/01.generic_prep', 0, mh_bulletin_params)

#chapter 1 preparation and aggregation - people in contact with mh services measures in the financial year
dbutils.notebook.run('prep/02.chapter_1_prep', 0, mh_bulletin_params)
dbutils.notebook.run('prep/03.chapter_1_standardisation', 0, mh_bulletin_params)
dbutils.notebook.run('agg/01.chapter_1_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg_phase_2/01.chapter_1_phase_2', 0, mh_bulletin_params)

#chapter 2 removed from 2016/17 mh bulletin onwards (now published separetely in mental health act statistics)
#chapter 3 removed from 2021/22 mh bulletin onwards

#chapter 4 preparation and aggregation - bed days measures in the financial year
dbutils.notebook.run('prep/04.chapter_4_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/02.chapter_4_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg_phase_2/02.chapter_4_phase_2', 0, mh_bulletin_params)

#chapter 5 preparation and aggregation - admissions and discharges measures in the financial year
dbutils.notebook.run('prep/05.chapter_5_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/03.chapter_5_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg_phase_2/03.chapter_5_phase_2', 0, mh_bulletin_params)

#chapter 6 preparation and aggregation - care contacts measures in the financial year
dbutils.notebook.run('prep/06.chapter_6_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/04.chapter_6_agg', 0, mh_bulletin_params)

#chapter 7 preparation and aggregation - restrictive interventions measures in the financial year
dbutils.notebook.run('prep/07.chapter_7_standardisation', 0, mh_bulletin_params)
dbutils.notebook.run('agg/05.chapter_7_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg_phase_2/04.chapter_7_phase_2', 0, mh_bulletin_params)

#chapter 8 removed from 2021/22 mh bulletin onwards (replaced by chapter 17 in mh bulletin)

#chapter 9 preparation and aggregation - children and young people measures in the financial year
dbutils.notebook.run('prep/08.chapter_9_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/06.chapter_9_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg_phase_2/05.chapter_9_phase_2', 0, mh_bulletin_params)

#chapter 10 preparation and aggregation - early intervention for psychosis measures in the financial year
dbutils.notebook.run('prep/09.chapter_10_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/07.chapter_10_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg_phase_2/06.chapter_10_phase_2', 0, mh_bulletin_params)

#chapter 11 preparation and aggregation - perinatal measures in the financial year
dbutils.notebook.run('prep/10.chapter_11_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/08.chapter_11_agg', 0, mh_bulletin_params)

#chapter 12 preparation and aggregation - 72 hour follow up measures in the financial year
dbutils.notebook.run('prep/11.chapter_12_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/09.chapter_12_agg', 0, mh_bulletin_params)

#chapter 13 preparation and aggregation - memory services measures in the financial year
dbutils.notebook.run('prep/12.chapter_13_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/10.chapter_13_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg_phase_2/07.chapter_13_phase_2', 0, mh_bulletin_params)

#chapter 14 and 15 preparation and aggregation - community access and admissions measures in the financial year
dbutils.notebook.run('prep/13.chapter_14_15_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/11.chapter_14_agg', 0, mh_bulletin_params)
dbutils.notebook.run('agg/12.chapter_15_agg', 0, mh_bulletin_params)

#chapter 16 preparation and aggregation - length of stay measures in the financial year
dbutils.notebook.run('prep/14.chapter_16_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/13.chapter_16_agg', 0, mh_bulletin_params)

#chapter 17 preparation and aggregation - children and young people measures in the financial year
dbutils.notebook.run('prep/15.chapter_17_prep', 0, mh_bulletin_params)
dbutils.notebook.run('agg/14.chapter_17_agg', 0, mh_bulletin_params)

# COMMAND ----------

#combine aggregate counts and rates into a single output using the 'skeleton' table created in setup/csv_master and create suppressed output
dbutils.notebook.run('setup/outputs_and_suppression', 0, params)

# COMMAND ----------

#import mhb_output_tests and mh_bulletin_measure_params

# COMMAND ----------

 %run ../tests/mhb_output_tests

# COMMAND ----------

 %run ./setup/mh_bulletin_measure_params

# COMMAND ----------

#get unsuppressed and suppressed final outputs as pyspark dataframes (to be used for testing)
output_unsup = spark.table(f"{db_output}.output_unsuppressed")
output_sup = spark.table(f"{db_output}.output_suppressed_final_1")

# COMMAND ----------

#run tests for unsuppressed output
name = "output_unsup"
print('Schema tests')
test_NumberOfColumnsUnsup(output_unsup)
test_SameColumnNamesUnsup(output_unsup)
print('RP tests')
test_SingleReportingPeriodEnd(output_unsup)
test_ReportingPeriodEndParamMatch(output_unsup, params)
print('Measure tests')
test_measure_reporting_periods(output_unsup, measure_metadata)
test_measure_breakdown_figures(output_unsup, measure_metadata)

# COMMAND ----------

#run tests for suppressed output
name = "output_sup"
print('Schema tests')
test_NumberOfColumnsSup(output_sup)
test_SameColumnNamesSup(output_sup)
print('RP tests')
test_SingleReportingPeriodEnd(output_sup)
test_ReportingPeriodEndParamMatch(output_sup, params)
print('Measure tests')
test_measure_reporting_periods(output_sup, measure_metadata)
test_measure_breakdown_figures(output_sup, measure_metadata)

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.error_log

# COMMAND ----------

 %sql
 ---display error log
 select *
 from $db_output.error_log
 where RUNDATE = current_date()
   and Pass = False
 order by 2,4,5

# COMMAND ----------

 %sql
 ---if figure above 1 million it will need to be split
 select COUNT(*) from $db_output.output_unsuppressed 

# COMMAND ----------

 %sql
 ---if figure above 1 million it will need to be split
 select COUNT(*) from $db_output.output_suppressed_final_1 

# COMMAND ----------

 %sql
 ---final main unsuppressed output
 select distinct * from $db_output.output_unsuppressed 
 WHERE Metric NOT LIKE '10%'
 UNION ALL
 select distinct * from $db_output.output_unsuppressed 
 WHERE Metric LIKE '10%' AND LEVEL_ONE <> 'RW5'
 order by metric, breakdown, level_one, level_two

# COMMAND ----------

 %sql
 ---final main suppressed output
 select distinct * 
 from $db_output.output_suppressed_final_1
 WHERE METRIC NOT LIKE '10%'
 UNION ALL
 select distinct * 
 from $db_output.output_suppressed_final_1
 WHERE (METRIC LIKE '10%' AND LEVEL_ONE <> 'RW5')
 order by metric, breakdown, level_one, level_two

# COMMAND ----------

#dq output prep and output (excel) is ran separately in prep/16.dq_prep as the output is exported to AWS unlike the outputs above

# COMMAND ----------

#dq tables prep (showcased in published metadata file)
# dbutils.notebook.run('prep/16.dq_prep', 0, mh_bulletin_params)