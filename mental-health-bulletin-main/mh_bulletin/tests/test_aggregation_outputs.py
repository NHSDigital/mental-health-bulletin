# Databricks notebook source
dbutils.widgets.text("db_output", "personal_db")
dbutils.widgets.text("db_source", "mhsds_database")
dbutils.widgets.text("rp_enddate", "2022-03-31")
dbutils.widgets.text("rp_startdate", "2021-04-01")
dbutils.widgets.text("year", "2021/22")
db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
year = dbutils.widgets.get("year") # year widget is exclusive to MHA Annual Publication

# COMMAND ----------

 %run ../mh_bulletin/metric_metadata

# COMMAND ----------

 %run ./test_functions

# COMMAND ----------

output_unsup = spark.table(f"{db_output}.automated_output_unsuppressed")
output_sup = spark.table(f"{db_output}.automated_bulletin_output_final")

# COMMAND ----------

test_NumberOfColumnsUnsup(output_unsup)
test_SameColumnNamesUnsup(output_unsup)
test_SingleReportingPeriodEnd(output_unsup, rp_enddate)
test_ReportingPeriodEndParamMatch(output_unsup, rp_enddate)
test_measure_reporting_periods(output_unsup, metric_metadata)
test_measure_breakdown_figures(output_unsup, metric_metadata)
test_breakdown_unknowns(output_unsup, output_unsup)

# COMMAND ----------

test_NumberOfColumnsSup(output_sup)
test_SameColumnNamesSup(output_sup)
test_SingleReportingPeriodEnd(output_sup, rp_enddate)
test_ReportingPeriodEndParamMatch(output_sup, rp_enddate)
test_suppression_stars(output_sup)
test_suppression_zeros(output_sup)
test_suppression_dashes(output_sup)
test_suppression_counts_less_than_5(output_sup, metric_metadata)
test_suppression_counts_divide_5(output_sup, metric_metadata)