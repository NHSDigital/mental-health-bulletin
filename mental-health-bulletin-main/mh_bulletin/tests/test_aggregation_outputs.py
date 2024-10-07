# Databricks notebook source
dbutils.widgets.text("db_output", "_tootle1_100261")
dbutils.widgets.text("db_source", "mh_v5_pre_clear")
dbutils.widgets.text("rp_enddate", "2024-03-31")
dbutils.widgets.text("rp_startdate", "2023-04-01")
dbutils.widgets.text("product", "ALL")
db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
product = dbutils.widgets.get("product")

# COMMAND ----------

 %run ../mh_bulletin/parameters

# COMMAND ----------

 %run ../mh_bulletin/metric_metadata

# COMMAND ----------

chapter_metric_metadata = automated_metric_metadata[product]

# COMMAND ----------

 %run ./test_functions

# COMMAND ----------

output_unsup = spark.table(f"{db_output}.automated_output_unsuppressed1_final")
output_sup = spark.table(f"{db_output}.automated_bulletin_output_final")

# COMMAND ----------

 %sql
 DELETE FROM $db_output.error_log WHERE RUNDATE = current_date()

# COMMAND ----------

run_unsuppressed_aggregation_output_tests(output_unsup, chapter_metric_metadata, test_output_columns, activity_higher_than_england_totals)

# COMMAND ----------

run_suppressed_aggregation_output_tests(output_sup, chapter_metric_metadata, test_output_columns, unsup_breakdowns)