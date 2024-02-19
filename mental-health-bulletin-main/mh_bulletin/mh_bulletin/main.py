# Databricks notebook source
# DBTITLE 1,Creates and sets widgets
# personal_db

dbutils.widgets.text("db_output", "personal_db", "db_output")
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
dbutils.notebook.run('setup/Population_v2', 0, params)

# COMMAND ----------

# DBTITLE 1,Prep
dbutils.notebook.run('prep/0. Generic_prep', 0, params) 
dbutils.notebook.run('prep/1. Chapter_1_prep', 0, params) 
dbutils.notebook.run('prep/2. Chapter_1_Standardisation_code_prep', 0, params) 
dbutils.notebook.run('prep/4. Chapter_4_prep', 0, params) 
dbutils.notebook.run('prep/5. Chapter_5_prep', 0, params) 
dbutils.notebook.run('prep/6. Chapter_6_prep', 0, params) 
dbutils.notebook.run('prep/7. Chapter_7_Standardisation_restraint_prep', 0, params) 
dbutils.notebook.run('prep/8. Chapter_8_9_prep_v2', 0, params) 
dbutils.notebook.run('prep/9. Chapter_10a_prep', 0, params) 
dbutils.notebook.run('prep/10. Chapter_11_prep', 0, params) 
dbutils.notebook.run('prep/11. Chapter_12_prep', 0, params) 
dbutils.notebook.run('prep/12. Chapter_13_prep', 0, params) 
dbutils.notebook.run('prep/13. Chapter_14_15_prep (Community Access and Admissions)', 0, params) 
dbutils.notebook.run('prep/14. Chapter_16_prep (LOS)', 0, params) 
dbutils.notebook.run('prep/15. Chapter_17_prep (CYP Access)', 0, params) 
dbutils.notebook.run('prep/16. Chapter_18_prep (IPS)', 0, params) 
dbutils.notebook.run('prep/17. Chapter_19_prep (CYP Outcomes)', 0, params) 

# COMMAND ----------

# DBTITLE 1,Prep Table Tests
dbutils.notebook.run('../tests/test_prep_tables', 0, params)

# COMMAND ----------

# DBTITLE 1,Aggregation
x1 = f"create table if not exists {db_output}.log1 (log1 string)"
spark.sql(x1)


dbutils.notebook.run('agg_v1', 0, params)

# COMMAND ----------

# DBTITLE 1,Reconciliation
 %sql
 -- every measure_id breakdown combination in agg_supp_df should appear in automated_output_unsuppressed
 -- CountOut is number of rows in output for that combination
 
 select t1.breakdown, t1.Chapter, t1.measure_id, t2.METRIC, t2.RowCount from 
 (select breakdown, Chapter, measure_id from $db_output.mapDenNum) t1
 
 left join 
 
 (select BREAKDOWN, METRIC, count(*) as RowCount
 from $db_output.automated_output_unsuppressed1
 group by BREAKDOWN, METRIC) t2
 on t1.breakdown = t2.BREAKDOWN
 and t1.measure_id = t2.METRIC
 
 where RowCount is null 
 order by measure_id, breakdown

# COMMAND ----------

dbutils.notebook.run('./CrossJoin', 0, params)

# COMMAND ----------

# DBTITLE 1,Create Bulletin CSV Lookup
chapter_metric_metadata = automated_metric_metadata[product]
insert_bulletin_lookup_values(mh_run_params, chapter_metric_metadata, csv_lookup_columns)

# COMMAND ----------

import pandas as pd
unsup_breakdowns_data = {"breakdown": unsup_breakdowns}
unsup_breakdowns_pdf = pd.DataFrame(unsup_breakdowns_data)
unsup_breakdowns_df = spark.createDataFrame(unsup_breakdowns_pdf)
unsup_breakdowns_df.createOrReplaceTempView("unsup_breakdowns")

# COMMAND ----------

# DBTITLE 1,Create Automated Bulletin Final Output
 %sql
 DROP TABLE IF EXISTS $db_output.automated_bulletin_output_final;
 CREATE TABLE IF NOT EXISTS $db_output.automated_bulletin_output_final AS
 SELECT
 c.REPORTING_PERIOD_START,
 c.REPORTING_PERIOD_END,
 c.STATUS,
 c.BREAKDOWN,
 c.LEVEL_ONE,
 c.LEVEL_ONE_DESCRIPTION,
 c.LEVEL_TWO,
 c.LEVEL_TWO_DESCRIPTION,
 c.LEVEL_THREE,
 c.LEVEL_THREE_DESCRIPTION,
 c.LEVEL_FOUR,
 c.LEVEL_FOUR_DESCRIPTION,
 c.METRIC,
 c.METRIC_NAME,
 CASE 
   WHEN u.Breakdown is not null THEN COALESCE(b.METRIC_VALUE, 0) 
   ELSE COALESCE(b.METRIC_VALUE, "*") 
   END as METRIC_VALUE
 FROM $db_output.bulletin_csv_lookup c
 LEFT JOIN $db_output.automated_output_suppressed b
 ON c.REPORTING_PERIOD_END = b.REPORTING_PERIOD_END
 AND lower(c.STATUS) = lower(b.STATUS)
 AND lower(c.BREAKDOWN) = lower(b.BREAKDOWN)
 AND lower(c.LEVEL_ONE) = lower(b.LEVEL_ONE)
 AND lower(c.LEVEL_TWO) = lower(b.LEVEL_TWO)
 AND lower(c.LEVEL_THREE) = lower(b.LEVEL_THREE)
 AND lower(c.LEVEL_FOUR) = lower(b.LEVEL_FOUR)
 AND lower(c.METRIC) = lower(b.METRIC)
 AND lower(c.SOURCE_DB) = lower(b.SOURCE_DB)
 LEFT JOIN unsup_breakdowns U ON lower(C.BREAKDOWN) = lower(U.BREAKDOWN)
 UNION ALL
 SELECT
 c.REPORTING_PERIOD_START,
 c.REPORTING_PERIOD_END,
 c.STATUS,
 c.BREAKDOWN,
 c.LEVEL_ONE,
 c.LEVEL_ONE_DESCRIPTION,
 c.LEVEL_TWO,
 c.LEVEL_TWO_DESCRIPTION,
 c.LEVEL_THREE,
 c.LEVEL_THREE_DESCRIPTION,
 c.LEVEL_FOUR,
 c.LEVEL_FOUR_DESCRIPTION,
 c.METRIC,
 c.METRIC_NAME,
 c.METRIC_VALUE
 FROM $db_output.automated_output_unsuppressed c
 WHERE METRIC like "1f%"
 and LEVEL_TWO_DESCRIPTION NOT IN ('Gypsy', 'Arab', 'Roma')

# COMMAND ----------

# DBTITLE 1,Error Log
 %sql
 select *
 from $db_output.error_log
 where RUNDATE = current_date()
   and Pass = False
 order by 2,4,5

# COMMAND ----------

 %sql
 --IF THIS IS OVER 1 MILLION, THE OUTPUT WILL NEED TO BE SPLIT
 select COUNT(*) from $db_output.automated_output_unsuppressed1

# COMMAND ----------

 %sql
 --IF THIS IS OVER 1 MILLION, THE OUTPUT WILL NEED TO BE SPLIT
 select COUNT(*) from $db_output.automated_bulletin_output_final

# COMMAND ----------

if rp_enddate > "2022-06-30":  # the last month of CCGs is June 2022
  #unsuppressed output
  sqlContext.sql(f"UPDATE {db_output}.automated_output_unsuppressed1 SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.automated_output_unsuppressed1 SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB
  #suppressed output
  sqlContext.sql(f"UPDATE {db_output}.automated_bulletin_output_final SET BREAKDOWN = REPLACE(BREAKDOWN,'CCG','Sub ICB') WHERE BREAKDOWN LIKE '%CCG%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update CCG to Sub ICB
  sqlContext.sql(f"UPDATE {db_output}.automated_bulletin_output_final SET BREAKDOWN = REPLACE(BREAKDOWN,'STP','ICB') WHERE BREAKDOWN LIKE '%STP%' AND REPORTING_PERIOD_END = '{rp_enddate}'") #update STP to ICB

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
 from $db_output.automated_output_unsuppressed1
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

# COMMAND ----------

