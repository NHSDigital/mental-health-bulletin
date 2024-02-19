# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
IMD_year = dbutils.widgets.get("IMD_year")
status = dbutils.widgets.get("status")
product = dbutils.widgets.get("product")

# COMMAND ----------

 %run ./parameters

# COMMAND ----------

 %run ./metric_metadata

# COMMAND ----------

 %sql
 DELETE FROM $db_output.automated_output_unsuppressed1 WHERE metric not in ("1f", "1fa", "1fb", "1fc", "1fd");
 DELETE FROM $db_output.automated_output_suppressed WHERE metric not in ("1f", "1fa", "1fb", "1fc", "1fd")

# COMMAND ----------

spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", False)

# COMMAND ----------

from pyspark.sql import DataFrame
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def chapter(metric):
  alphabet = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
             "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
             "u", "v", "w", "x", "y", "z"]
  for x in alphabet: metric = metric.replace(x, "")
  return int(metric)

# COMMAND ----------

# DBTITLE 1,Create left hand side of reconciliation table
amd = automated_metric_metadata["ALL"]

l2 = [[amd[measure_id]["suppression"],
       chapter(measure_id),
       measure_id,
       amd[measure_id]["numerator_id"],
       amd[measure_id]["crude_rate"],
       breakdown["breakdown_name"],
       amd[measure_id]["name"]
      ] for measure_id in amd 
       for breakdown in amd[measure_id]["breakdowns"]]
 
print(f"{timenow()} create den_num_df")
for x in l2: pass
schema = "suppression string, Chapter int, measure_id string, numerator_id string, crude_rate int, breakdown string, measure_name string"
den_num_df = spark.createDataFrame(l2, schema = schema)
spark.sql(f"drop table if exists {db_output}.mapDenNum")
den_num_df.write.saveAsTable(f"{db_output}.mapDenNum", mode = 'overwrite')

# COMMAND ----------

# DBTITLE 1,Create automated_output_unsuppressed
chapter_metric_metadata = automated_metric_metadata[product]
for measure_id in chapter_metric_metadata:
  print(timenow(), f">>>>> {measure_id} >>>>>")
  list_agg_df = []
  list_supp_df = []
  
  measure_name = chapter_metric_metadata[measure_id]["name"]
  measure_freq = chapter_metric_metadata[measure_id]["freq"]
  measure_rp_startdate = rp_startdate
  numerator_id = chapter_metric_metadata[measure_id]["numerator_id"]
  crude_rate = chapter_metric_metadata[measure_id]["crude_rate"]
  suppression_type = chapter_metric_metadata[measure_id]["suppression"]
  measure_breakdowns = chapter_metric_metadata[measure_id]["breakdowns"]
  for breakdown in measure_breakdowns:
    breakdown_name = breakdown["breakdown_name"]
    source_table = chapter_metric_metadata[measure_id]["source_table"]
    filter_clause = chapter_metric_metadata[measure_id]["filter_clause"]
    if "Provider" in breakdown_name and measure_id in prov_prep_tables_metrics:
      source_table = f"{source_table}_prov"
    aggregate_field = chapter_metric_metadata[measure_id]["aggregate_field"]
    aggregate_function = chapter_metric_metadata[measure_id]["aggregate_function"]
    
    print(timenow(), measure_id, breakdown_name)
    try:
      if crude_rate == 1:
        primary_level = F.col("LEVEL_ONE")
        primary_level_desc = F.col("LEVEL_ONE_DESCRIPTION")
        secondary_level = F.col("LEVEL_TWO")
        secondary_level_desc = F.col("LEVEL_TWO_DESCRIPTION")
        third_level = F.col("LEVEL_THREE")
        third_level_desc = F.col("LEVEL_THREE_DESCRIPTION")
        fourth_level = F.col("LEVEL_FOUR")
        fourth_level_desc = F.col("LEVEL_FOUR_DESCRIPTION")
        agg_df = aggregate_function(
        db_output, db_source, "automated_output_unsuppressed1", filter_clause,
        measure_rp_startdate, rp_enddate, primary_level, primary_level_desc, secondary_level, secondary_level_desc,
        third_level, third_level_desc, fourth_level, fourth_level_desc,
        aggregate_field, breakdown_name, status, measure_id, numerator_id, measure_name, output_columns
      )
      else: ###measure is not a crude rate:
        primary_level = breakdown["primary_level"]
        primary_level_desc = breakdown["primary_level_desc"]
        secondary_level = breakdown["secondary_level"]
        secondary_level_desc = breakdown["secondary_level_desc"]
        third_level = breakdown["third_level"]
        third_level_desc = breakdown["third_level_desc"]
        fourth_level = breakdown["fourth_level"]
        fourth_level_desc = breakdown["fourth_level_desc"]
        #create agg df
        agg_df = aggregate_function(
          db_output, db_source, source_table, filter_clause,
          measure_rp_startdate, rp_enddate, primary_level, primary_level_desc, secondary_level, secondary_level_desc,
          third_level, third_level_desc, fourth_level, fourth_level_desc,
          aggregate_field, breakdown_name, status, measure_id, numerator_id, measure_name, output_columns
        )
      list_agg_df += [agg_df]
    except:
      writelog(f"Failed {measure_id}, {breakdown_name}")
      print("failed")
    
    if breakdown_name in unsup_breakdowns:
      supp_df = agg_df.filter(F.col("BREAKDOWN") == breakdown_name)
    else:
      insert_df = spark.table(f"{db_output}.automated_output_unsuppressed1")
      supp_df = mhsds_suppression(insert_df, suppression_type, breakdown_name, measure_id, rp_enddate, status, numerator_id, db_source)

    sup_agg_df = (supp_df.withColumn("METRIC_NAME", F.lit(measure_name)).select(*output_columns))
    list_supp_df += [sup_agg_df]
    
  print(timenow(), "Combine and insert")
  if len(list_agg_df) > 0:
    
    agg_df = unionAll(*[df for df in list_agg_df])
    agg_df = agg_df.distinct()
    r1 = agg_df.count()
    print(timenow(), f"Combine and insert unsuppressed {r1} rows")
    insert_unsup_agg(agg_df, db_output, output_columns, "automated_output_unsuppressed1") 
    list_agg_df = []
    supp_df = unionAll(*[df for df in list_supp_df])
    supp_df = supp_df.distinct()
    r1 = supp_df.count()
    print(timenow(), f"Combine and insert suppressed {r1} rows")
    insert_sup_agg(supp_df, db_output, measure_name, output_columns, "automated_output_suppressed")
    list_supp_df = []

print(timenow(), "Complete")

# COMMAND ----------

# DBTITLE 1,automated_output_suppressed
import pandas as pd
import numpy as np

def suppress(list1):  # [suppression, metric_value, num_val, breakdown]
  try:
    suppression = list1[0].lower()
    metric_value = list1[1]
    num_val = list1[2]
    breakdown = list1[3]
    metric_value = float(metric_value)
    num_val = float(num_val)
    if breakdown in unsup_breakdowns:
      if metric_value == int(metric_value):
        s1 = int(metric_value)
      else:
        s1 = round(metric_value, 3)
    elif suppression == "count":
      if num_val < 5:
        s1 = str("*")
      else:
        s1 = int(5 * round(num_val / 5, 0))
    else:
      if num_val < 5:
        s1 = str("*")
      else:
        s1 = round(metric_value, 3)
    return str(s1)
  except:
    return str("*")
    
population_metrics = ["1f", "1fa", "1fb", "1fc", "1fd", "1fe"]
# for breakdown in list(set(unsup_breakdowns)): print(breakdown)

sdf1 = spark.sql(f"select * from {db_output}.automated_output_unsuppressed1")
df1 = sdf1.select("*").toPandas()
sdf2 = spark.sql(f"select * from {db_output}.mapDenNum")
df2 = sdf2.select("*").toPandas()

df2 = df2[["suppression","measure_id","numerator_id", "breakdown", "crude_rate"]]
df2 = df2.rename(columns={"measure_id": "METRIC", "breakdown": "BREAKDOWN"})
df2 = df2.drop_duplicates()

df1 = df1[~df1["METRIC"].isin(population_metrics)]
df3 = df1.merge(df2, left_on = ["METRIC", "BREAKDOWN"],
               right_on = ["METRIC", "BREAKDOWN"], how = "left")

left_cols = ["REPORTING_PERIOD_END", "METRIC", "BREAKDOWN", "LEVEL_ONE", "LEVEL_TWO", 
             "LEVEL_THREE", "LEVEL_FOUR"]
right_cols = ["REPORTING_PERIOD_END", "numerator_id", "BREAKDOWN", "LEVEL_ONE", "LEVEL_TWO", 
             "LEVEL_THREE", "LEVEL_FOUR", "METRIC_VALUE"]
df4 = df1[["REPORTING_PERIOD_END", "METRIC", "BREAKDOWN", "LEVEL_ONE", "LEVEL_TWO", 
           "LEVEL_THREE", "LEVEL_FOUR", "METRIC_VALUE"]].rename(
    columns = {"METRIC": "numerator_id", "METRIC_VALUE": "NUMERATOR_VALUE"})

cols = ["REPORTING_PERIOD_END", "numerator_id", "BREAKDOWN", "LEVEL_ONE", "LEVEL_TWO", 
        "LEVEL_THREE", "LEVEL_FOUR"]

df5 = df3.merge(df4, left_on = cols, right_on = cols, how = 'left')
df5["SuppressedValue"] = df5[["suppression", "METRIC_VALUE", "NUMERATOR_VALUE", "BREAKDOWN"]].apply(suppress, axis = 1)

for col in df5.columns:
   if ((df5[col].dtypes != np.int64) & 
      (df5[col].dtypes != np.float64)):
    df5[col] = df5[col].fillna(0)
    
cols = list(df5)
cols1 = [col for col in cols if "VALUE" not in col]
df5 = df5.astype({col: "string" for col in cols1})
df5["NUMERATOR_VALUE"] = pd.to_numeric(df5["NUMERATOR_VALUE"], errors = 'coerce')
df5["METRIC_VALUE"] = pd.to_numeric(df5["METRIC_VALUE"], errors = 'coerce')

sdf5 = spark.createDataFrame(df5)
spark.sql(f"drop table if exists {db_output}.Suppression")
sdf5.write.saveAsTable(f"{db_output}.Suppression", mode = 'overwrite')
display(sdf5)