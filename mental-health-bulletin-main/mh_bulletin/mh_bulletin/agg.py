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
 -- DELETE FROM $db_output.automated_output_unsuppressed1 WHERE metric not in ("1f", "1fa", "1fb", "1fc", "1fd");
 -- DELETE FROM $db_output.automated_output_suppressed WHERE metric not in ("1f", "1fa", "1fb", "1fc", "1fd")

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

l2 = [["E" if breakdown["breakdown_name"] in unsup_breakdowns
       else "C" if (amd[measure_id]["suppression"] == "count" or amd[measure_id]["suppression"] == "Count")
       else "P", 
       amd[measure_id]["suppression"],
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
schema = "SupType string, suppression string, Chapter int, measure_id string, numerator_id string, crude_rate int, breakdown string, measure_name string"
den_num_df = spark.createDataFrame(l2, schema = schema)
spark.sql(f"drop table if exists {db_output}.mapDenNum")
den_num_df.write.saveAsTable(f"{db_output}.mapDenNum", mode = 'overwrite')
display(den_num_df)

# COMMAND ----------

# DBTITLE 1,Create automated_output_unsuppressed
chapter_metric_metadata = automated_metric_metadata[product]
for measure_id in chapter_metric_metadata:
# for measure_id in ['5a', '5b', '5c']:
#   spark.sql(f"OPTIMIZE {db_output}.automated_output_unsuppressed ZORDER BY METRIC, BREAKDOWN")
#   spark.sql(f"OPTIMIZE {db_output}.automated_output_suppressed ZORDER BY METRIC, BREAKDOWN")  
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
    if breakdown_name in prov_breakdowns and measure_id in prov_prep_tables_metrics:
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
      # add agg_df to list for insert
      list_agg_df += [agg_df]
    except:
      writelog(f"Failed {measure_id}, {breakdown_name}")
      print("failed")
    
    #''' #TURN THIS ON TO GET SUPPRESSION BACK IN
#     if breakdown_name in unsup_breakdowns:
#       supp_df = agg_df.filter(F.col("BREAKDOWN") == breakdown_name)
#     else:
#       insert_df = spark.table(f"{db_output}.automated_output_unsuppressed1")
#       supp_df = mhsds_suppression(insert_df, suppression_type, breakdown_name, measure_id, rp_enddate, status, numerator_id, db_source)

#     sup_agg_df = (supp_df.withColumn("METRIC_NAME", F.lit(measure_name)).select(*output_columns))
    
#    list_supp_df += [sup_agg_df]
    # add supp_df to list for insert
    #'''
    
  print(timenow(), f"Combine {len(list_agg_df)} dataframes")
  if len(list_agg_df) > 0:
    
    agg_df = unionAll(*[df for df in list_agg_df])
    agg_df = agg_df.distinct()
    r1 = agg_df.count()
    print(timenow(), f"Insert unsuppressed {r1} rows")
    insert_unsup_agg(agg_df, db_output, output_columns, "automated_output_unsuppressed1") 
    list_agg_df = []
    
  #''' #TURN THIS ON TO GET SUPPRESSION BACK IN

#     supp_df = unionAll(*[df for df in list_supp_df])
#     supp_df = supp_df.distinct()
#     r1 = supp_df.count()
#     print(timenow(), f"Combine and insert suppressed {r1} rows")
#     insert_sup_agg(supp_df, db_output, measure_name, output_columns, "automated_output_suppressed")
#     list_supp_df = []
  #'''

print(timenow(), "Complete")