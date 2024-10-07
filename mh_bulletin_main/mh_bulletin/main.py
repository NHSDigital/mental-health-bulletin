# Databricks notebook source
# DBTITLE 1,Removes all widgets
# dbutils.widgets.removeAll();

# COMMAND ----------

# DBTITLE 1,Creates and sets widgets
# _tootle1_100261

dbutils.widgets.text("db_output", "mark_wagner1_100394", "db_output")
db_output  = dbutils.widgets.get("db_output")
assert db_output

dbutils.widgets.text("db_source", "mh_pre_pseudo_d1", "db_source")
db_source = dbutils.widgets.get("db_source")
assert db_source

dbutils.widgets.text("rp_enddate", "2021-03-31", "rp_enddate")
rp_enddate = dbutils.widgets.get("rp_enddate")
assert rp_enddate

dbutils.widgets.text("status", "Final", "status")
status  = dbutils.widgets.get("status")
assert status

chapters = ["ALL", "CHAP1", "CHAP4", "CHAP5", "CHAP6", "CHAP7", "CHAP9", "CHAP10", "CHAP11", "CHAP12", "CHAP13", "CHAP14", "CHAP15", "CHAP16", "CHAP17", "CHAP18", "CHAP19", "CUSTOM_RUN"]
dbutils.widgets.dropdown("chapter", "ALL", chapters)
product = dbutils.widgets.get("chapter")

# COMMAND ----------

# DBTITLE 1,Run this if you want to drop all tables in your chosen database
# tables_meta_df = spark.sql(f"show tables in {db_output}")
# actual_tables = [r['tableName'] for r in tables_meta_df.collect()]
# for exp_table in actual_tables:
#   print(exp_table)
#   if exp_table != "automated_output_unsuppressed":
#     spark.sql(f"drop table {db_output}.{exp_table}")

# COMMAND ----------

 %run ./parameters

# COMMAND ----------

 %run ./mhsds_functions

# COMMAND ----------

 %run ./metric_metadata

# COMMAND ----------

# DBTITLE 1,Set up MHBulletinParameter Data Class
rp_enddate_firstday = dt2str(first_day(str2dt(rp_enddate)))
mh_run_params = MHBulletinParameters(db_output, db_source, status, rp_enddate_firstday, product)
params = mh_run_params.as_dict()
params

# COMMAND ----------

# DBTITLE 1,Set-up
dbutils.notebook.run('setup/Create Tables', 0, params)
dbutils.notebook.run('setup/Create MH Bulletin Asset', 0, params)
dbutils.notebook.run('setup/Reference Data', 0, params)
dbutils.notebook.run('setup/Population_v3_rebased', 0, params)
# This section takes approximately 25 minutes to run

# COMMAND ----------

# DBTITLE 1,Prep
dbutils.notebook.run('prep/0. Generic_prep', 0, params) #Generic Prep    <<< 39 mins:
dbutils.notebook.run('prep/1. Chapter_1_prep', 0, params) #People in contact       << 13 mins:  people_in_contact 7m, people_in_contact_prov 6m
dbutils.notebook.run('prep/2. Chapter_1_Standardisation_code_prep', 0, params) #People in contact standardised rates  <<< 3 mins
dbutils.notebook.run('prep/4. Chapter_4_prep', 0, params) #Bed Days
dbutils.notebook.run('prep/5. Chapter_5_prep', 0, params) #Admissions and Discharges
dbutils.notebook.run('prep/6. Chapter_6_prep', 0, params) #Care Contacts
dbutils.notebook.run('prep/7. Chapter_7_Standardisation_restraint_prep', 0, params) #Restraints
dbutils.notebook.run('prep/8. Chapter_8_9_prep', 0, params) #CYP 2nd Contact
dbutils.notebook.run('prep/9. Chapter_10_prep', 0, params) #EIP    <<<  MHS001MPI_PATMRECINRP_FIX 6.22 mins, 
dbutils.notebook.run('prep/10. Chapter_11_prep', 0, params) #Perinatal
dbutils.notebook.run('prep/11. Chapter_12_prep', 0, params) #72 hour follow up
dbutils.notebook.run('prep/12. Chapter_13_prep', 0, params) #Memory services
dbutils.notebook.run('prep/13. Chapter_14_15_prep (Community Access and Admissions)', 0, params) #CMH Admissions and Access
dbutils.notebook.run('prep/14. Chapter_16_prep (LOS)', 0, params) #LOS
dbutils.notebook.run('prep/15. Chapter_17_prep (CYP Access)', 0, params) #CYP Access
dbutils.notebook.run('prep/16. Chapter_18_prep (IPS)', 0, params) #IPS
dbutils.notebook.run('prep/17. Chapter_19_prep (CYP Outcomes)', 0, params) #CYP Outcomes
# This section takes approximately 90 minutes to run

# COMMAND ----------

# DBTITLE 1,Prep Table Tests
dbutils.notebook.run('../tests/test_prep_tables', 0, params)

# COMMAND ----------

 %sql
 DELETE FROM $db_output.automated_output_suppressed WHERE metric not in ("1f", "1fa", "1fb", "1fc", "1fd");
 DELETE FROM $db_output.automated_output_unsuppressed1 WHERE metric not in ("1f", "1fa", "1fb", "1fc", "1fd");

# COMMAND ----------

# DBTITLE 1,Aggregation
dbutils.notebook.run('agg', 0, params)
# This section takes approximately 300 minutes to run

# COMMAND ----------

# DBTITLE 1,Create CSV Skeleton and join to aggregated data
dbutils.notebook.run('csv_skeleton', 0, params)

# COMMAND ----------

import pandas as pd
unsup_breakdowns_data = {"breakdown": unsup_breakdowns} #unsup_breakdowns list is located in parameters
unsup_breakdowns_pdf = pd.DataFrame(unsup_breakdowns_data)
unsup_breakdowns_df = spark.createDataFrame(unsup_breakdowns_pdf)
unsup_breakdowns_df.createOrReplaceTempView("unsup_breakdowns")

# COMMAND ----------

# DBTITLE 1,automated_bulletin_output_final
 %sql
 create or replace table $db_output.automated_bulletin_output_final 
 select * from $db_output.auto_final_supp_output
 where LEVEL_ONE_DESCRIPTION NOT IN ('Gypsy', 'Arab', 'Roma')
   and LEVEL_TWO_DESCRIPTION NOT IN ('Gypsy', 'Arab', 'Roma')

# COMMAND ----------

# DBTITLE 1,automated_output_unsuppressed1_final
 %sql
 create or replace table $db_output.automated_output_unsuppressed1_final
 select distinct 
 REPORTING_PERIOD_START, REPORTING_PERIOD_END,
 STATUS, BREAKDOWN,
 LEVEL_ONE, LEVEL_ONE_DESCRIPTION,
 LEVEL_TWO, LEVEL_TWO_DESCRIPTION,
 LEVEL_THREE, LEVEL_THREE_DESCRIPTION,
 LEVEL_FOUR, LEVEL_FOUR_DESCRIPTION,
 METRIC, METRIC_NAME, METRIC_VALUE
 from $db_output.automated_output_unsuppressed1
 ORDER BY METRIC, BREAKDOWN, LEVEL_ONE, LEVEL_TWO, LEVEL_THREE

# COMMAND ----------

 %sql
 --IF THIS IS OVER 1 MILLION, THE OUTPUT WILL NEED TO BE SPLIT
 select COUNT(*) from $db_output.automated_output_unsuppressed1_final

# COMMAND ----------

 %sql
 --IF THIS IS OVER 1 MILLION, THE OUTPUT WILL NEED TO BE SPLIT
 select COUNT(*) from $db_output.automated_bulletin_output_final

# COMMAND ----------

# DBTITLE 1,Replace CCG with Sub ICB and replace STP with ICB
if rp_enddate > "2022-06-30":  # the last month of CCGs is June 2022
  #unsuppressed output
  sqlContext.sql(f"UPDATE {db_output}.automated_output_unsuppressed1_final SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.automated_output_unsuppressed1_final SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  #suppressed output
  sqlContext.sql(f"UPDATE {db_output}.automated_bulletin_output_final SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.automated_bulletin_output_final SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  
  #suppressed output
  sqlContext.sql(f"UPDATE {db_output}.auto_final_supp_output SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.auto_final_supp_output SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB 

# COMMAND ----------

# DBTITLE 1,Aggregation Output Tests
dbutils.notebook.run('../tests/test_aggregation_outputs', 0, params)

# COMMAND ----------

 %sql
 select *
 from $db_output.error_log
 where RUNDATE = current_date()
   and Pass = False
 order by 2,4,5

# COMMAND ----------

import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)

bbrb_final_df = spark.table(f"{db_output}.automated_bulletin_output_final").filter((F.col("REPORTING_PERIOD_END") == rp_enddate) & (F.col("STATUS") == status) & (F.col("SOURCE_DB") == db_source))
bbrb_final_pdf = bbrb_final_df.toPandas()
supp_data_df = spark.table(f"{db_output}.auto_final_supp_output").filter((F.col("REPORTING_PERIOD_END") == rp_enddate) & (F.col("STATUS") == status) & (F.col("SOURCE_DB") == db_source))
supp_data_pdf = supp_data_df.toPandas()

test_df = pd.merge(supp_data_pdf, bbrb_final_pdf, on=output_columns, how="left", indicator="exists")
test_df["exists"] = np.where(test_df.exists == "both", True, False)

if test_df.exists.eq(True).all():
  notes = "No errors"
else:
  notes = "Rows in auto_final_supp_output do not exist in automated_bulletin_output_final"
print(notes)

# COMMAND ----------

# DBTITLE 1,Check if row exists in suppressed that does not appear in final output
test_df[(test_df["exists"] == False)]

# COMMAND ----------

# DBTITLE 1,Final Unsuppressed Output
 %sql
 select distinct 
 REPORTING_PERIOD_START, REPORTING_PERIOD_END,
 STATUS, BREAKDOWN,
 LEVEL_ONE, LEVEL_ONE_DESCRIPTION,
 LEVEL_TWO, LEVEL_TWO_DESCRIPTION,
 LEVEL_THREE, LEVEL_THREE_DESCRIPTION,
 LEVEL_FOUR, LEVEL_FOUR_DESCRIPTION,
 METRIC, METRIC_NAME, METRIC_VALUE
 from $db_output.automated_output_unsuppressed1_final
 ORDER BY METRIC, BREAKDOWN, LEVEL_ONE, LEVEL_TWO, LEVEL_THREE

# COMMAND ----------

# DBTITLE 1,Final Suppressed Output
 %sql
 select distinct 
 REPORTING_PERIOD_START, REPORTING_PERIOD_END,
 STATUS, BREAKDOWN,
 LEVEL_ONE, LEVEL_ONE_DESCRIPTION,
 LEVEL_TWO, LEVEL_TWO_DESCRIPTION,
 LEVEL_THREE, LEVEL_THREE_DESCRIPTION,
 LEVEL_FOUR, LEVEL_FOUR_DESCRIPTION,
 METRIC, METRIC_NAME, METRIC_VALUE
 from $db_output.automated_bulletin_output_final

 ORDER BY METRIC, BREAKDOWN, LEVEL_ONE, LEVEL_TWO, LEVEL_THREE