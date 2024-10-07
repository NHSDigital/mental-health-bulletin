# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
print("db_output", db_output)
assert db_output

db_source = dbutils.widgets.get("db_source")
print("db_source", db_source)
assert db_source

rp_enddate = dbutils.widgets.get("rp_enddate")
print("rp_enddate", rp_enddate)
assert rp_enddate

status  = dbutils.widgets.get("status")
assert status

product = dbutils.widgets.get("product")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

 %run ./mhsds_functions

# COMMAND ----------

 %run ./parameters

# COMMAND ----------

 %run ./metric_metadata

# COMMAND ----------

rp_enddate_firstday = dt2str(first_day(str2dt(rp_enddate)))
mh_run_params = MHBulletinParameters(db_output, db_source, status, rp_enddate_firstday, product)
params = mh_run_params.as_dict()
print("params >>>\n", params)

# COMMAND ----------

# DBTITLE 1,mapDenNum
amd = automated_metric_metadata["ALL"]

du = {x: "" for x in unsup_breakdowns}
l2 = []

for measure_id in amd:
  for breakdown in amd[measure_id]["breakdowns"]:
    try:
      x1 = unsup_breakdowns.index(breakdown["breakdown_name"])
      SupType = "Nat"
    except:
      SupType = "SubNat"
                                  
    l2 += [[SupType, amd[measure_id]["suppression"],
       chapter(measure_id), measure_id, 
       amd[measure_id]["numerator_id"],
       amd[measure_id]["crude_rate"],
       breakdown["breakdown_name"],
       amd[measure_id]["name"]     
      ]]

print(f"{timenow()} create den_num_df")
for x in l2: pass
schema = "SupType string, suppression string, Chapter int, measure_id string, numerator_id string, crude_rate int, breakdown string, measure_name string"
den_num_df = spark.createDataFrame(l2, schema = schema)
spark.sql(f"drop table if exists {db_output}.mapDenNum")
den_num_df.write.saveAsTable(f"{db_output}.mapDenNum", mode = 'overwrite')
display(den_num_df)

# COMMAND ----------

# DBTITLE 1,UnsuppressedLink1
 %sql
 create or replace table $db_output.UnsuppressedLink1
 select u.*, n.SupType, n.suppression, n.numerator_id
 from $db_output.automated_output_unsuppressed1 u
 left join $db_output.mapDenNum n
 on n.measure_id = u.metric
 and n.BREAKDOWN = u.BREAKDOWN
 where metric not in ('1f', '1fa', '1fb', '1fc', '1fd');

 select * from $db_output.UnsuppressedLink1
 where level_one = '00L'

# COMMAND ----------

# DBTITLE 1,UnsuppressedLink2
 %sql
 create or replace table $db_output.UnsuppressedLink2
 select l.*, 
 u.metric_value as Numerator_value,
 case when l.SupType = 'Nat' then l.METRIC_VALUE
      when l.SupType = 'SubNat' and l.suppression = "count" and u.metric_value >= 5 then 5 * round(l.metric_value / 5, 0)
      when l.SupType = 'SubNat' and l.suppression = "count" and u.metric_value < 5 then '*'
      when l.SupType = 'SubNat' and l.suppression = "percent" and u.metric_value >= 5 then round(l.metric_value, 0)
      when l.SupType = 'SubNat' and l.suppression = "percent" and u.metric_value <  5 then '*'
      else '*' end as Suppressed_Value
   from $db_output.UnsuppressedLink1 l
   left join $db_output.automated_output_unsuppressed1 u
     on l.numerator_id = u.metric
     and l.BREAKDOWN = u.BREAKDOWN
     and l.Reporting_period_end = u.Reporting_period_end
     and l.Level_One = u.Level_One
     and l.Level_Two = u.Level_Two
     and l.Level_Three = u.Level_Three
     and l.Level_Four = u.Level_Four;

 select * from $db_output.UnsuppressedLink2
 where suppression = 'percent'

# COMMAND ----------

# DBTITLE 1,Create link between crude rates and 1f(a b c d)
d1 = {}
for x in cyp_crude_rate_metrics: d1[x] = "1fa"
for x in adu_crude_rate_metrics: d1[x] = "1fb"
for x in oap_crude_rate_metrics: d1[x] = "1fc"
for x in peri_crude_rate_metrics: d1[x] = "1fd"

l1 = []
x1 = f'''select distinct(metric) from {db_output}.UnsuppressedLink2
  where METRIC <> numerator_id and 
  (metric_name like "Crude rate%" or metric_name like "Crude Rate%" or metric_name like "crude rate%")'''
df1 = spark.sql(x1)
for x in df1.collect():
  if x[0] in d1: 
    l1 += [[x[0], d1[x[0]]]]
  else:
    l1 += [[x[0], "1f"]]

schema = "CrudeRateMetric string, PopMetric string"
df1 = spark.createDataFrame(l1, schema = schema)
spark.sql(f"drop table if exists {db_output}.CrudeRateLinkPop")
df1.write.saveAsTable(f"{db_output}.CrudeRateLinkPop", mode = 'overwrite')
display(df1)

# COMMAND ----------

# DBTITLE 1,CrudeRateLink1 CrudeRateLink2  - link crude rate to numerator and pop value
 %sql
 create or replace table $db_output.CrudeRateLink1
 select l2.*, cr.PopMetric from 
   (
   select * from $db_output.UnsuppressedLink2 
    where metric_name like "Crude rate%" or metric_name like "Crude Rate%" or metric_name like "crude rate%"
   ) l2
 left join $db_output.CrudeRateLinkPop cr
 on l2.METRIC = cr.CrudeRateMetric;

 create or replace table $db_output.CrudeRateLink2
 select cr.*, u.METRIC_VALUE as PopValue, f.metric_value as Pop_1f from $db_output.CrudeRateLink1 cr
 left join $db_output.automated_output_unsuppressed1 u
 on cr.Breakdown = u.Breakdown
 and cr.PopMetric = u.METRIC
 and cr.LEVEL_ONE = u.LEVEL_ONE
 and cr.LEVEL_TWO = u.LEVEL_TWO
 and cr.LEVEL_THREE = u.LEVEL_THREE
 and cr.LEVEL_FOUR = u.LEVEL_FOUR

 left join 
   (select * from $db_output.automated_output_unsuppressed1
   where metric = '1f'
 ) f
 on cr.Breakdown = f.Breakdown
 and cr.LEVEL_ONE = f.LEVEL_ONE
 and cr.LEVEL_TWO = f.LEVEL_TWO
 and cr.LEVEL_THREE = f.LEVEL_THREE
 and cr.LEVEL_FOUR = f.LEVEL_FOUR;

 select * from $db_output.CrudeRateLink2

# COMMAND ----------

# DBTITLE 1,automated_output_suppressed
 %sql
 create or replace table $db_output.automated_output_suppressed
 select REPORTING_PERIOD_START, REPORTING_PERIOD_END, STATUS, BREAKDOWN,
 LEVEL_ONE, LEVEL_ONE_DESCRIPTION, LEVEL_TWO, LEVEL_TWO_DESCRIPTION,
 LEVEL_THREE, LEVEL_THREE_DESCRIPTION, LEVEL_FOUR, LEVEL_FOUR_DESCRIPTION,
 METRIC, METRIC_NAME, Suppressed_Value as METRIC_VALUE, SOURCE_DB
 from $db_output.UnsuppressedLink2

# COMMAND ----------

# DBTITLE 1,Test that for SupType C metric is same as numerator_id
 %sql
 -- no data should appear here
 select * from $db_output.UnsuppressedLink2
 where SupType = 'SubNat' and suppression = "count" and metric != numerator_id

# COMMAND ----------

# DBTITLE 1,Test for SupType P metric is not same as numerator_id
 %sql
 -- no data should appear here
 select * from $db_output.UnsuppressedLink2
 where SupType = 'SubNat' and suppression = "percent" and metric = numerator_id

# COMMAND ----------

# DBTITLE 1,Create and combine Breakdown dataframes into bd_df
# this cell loops through the metadata to create the Breakdown dataframe, then stores it in a dictionary, avoiding repest creation

bd_dict = {}
warnings = []
          
for measure_id in amd:
  breakdowns = amd[measure_id]["breakdowns"]
  listdf1 = []
  for breakdown in breakdowns:
    bd_name = get_var_name(breakdown)
    if "_bd" not in bd_name: 
      print("***** check if this breakdown exists in output", measure_id, bd_name, breakdown["breakdown_name"])
      warnings += [f'***** Warning: Please check if this breakdown exists in output {measure_id} {bd_name} {breakdown["breakdown_name"]}']
      
    if bd_name not in bd_dict:
      try:
        print(timenow(), len(bd_dict), "Create dataframe for", bd_name)
        df1 = createbreakdowndf(breakdown)
        bd_dict[bd_name] = createbreakdowndf(breakdown)
      except:
        print("Failed ", bd_name)

for x in warnings: print(x)
bd_df = unionAll(*[bd_dict[bd_name] for bd_name in bd_dict])
display(bd_df)

# COMMAND ----------

# DBTITLE 1,Create metric-breakdown met_bd_df
lmtbd = []
for measure_id in amd:
  breakdowns = amd[measure_id]["breakdowns"]
  for breakdown in breakdowns:
    bd_name = get_var_name(breakdown)
    freq = amd[measure_id]["freq"]
    # placeholder for monthly publications
    # rp_startdate = mh_freq_to_rp_startdate(mh_run_params, freq)
    rp_startdate = params["rp_startdate"]
    # SupType = amd[measure_id]["suppression"][:1].upper()
    SupType = "SubNat"
    if breakdown["breakdown_name"] in unsup_breakdowns: SupType = "Nat"
    lmtbd += [[bd_name, breakdown["breakdown_name"], freq, SupType, status, db_source, rp_startdate, rp_enddate, 
               chapter(measure_id), measure_id, amd[measure_id]["name"]]]
    
met_bd_df = spark.createDataFrame(lmtbd, schema = "bd_name string, breakdown string, freq string, SupType string, status string, db_source string, rp_startdate string, rp_enddate string, chapter string, metric string, METRIC_NAME string")
display(met_bd_df)

# COMMAND ----------

# DBTITLE 1,bd_df left outer join met_bd_df to create bulletin_csv_link1
# for each row in bd_df bring in all matching rows from df1 as
df2 = bd_df.join(met_bd_df, how = 'left', on = ['bd_name', 'breakdown'])
df2 = df2.distinct()

spark.sql(f"drop table if exists {db_output}.bulletin_csv_link1")
df2.write.saveAsTable(f"{db_output}.bulletin_csv_link1", mode = "overwrite")
display(df2)

# COMMAND ----------

# DBTITLE 1,bulletin_csv_link2
 %sql
 create or replace table $db_output.bulletin_csv_link2
 select bd_name, chapter, SupType, 
        rp_startdate as REPORTING_PERIOD_START, rp_enddate as REPORTING_PERIOD_END,
        status as STATUS, 
        breakdown as BREAKDOWN,
        primary_level as LEVEL_ONE,
        primary_level_desc as LEVEL_ONE_DESCRIPTION,
        secondary_level as LEVEL_TWO,
        secondary_level_desc as LEVEL_TWO_DESCRIPTION,
        third_level as LEVEL_THREE,
        third_level_desc as LEVEL_THREE_DESCRIPTION,
        fourth_level as LEVEL_FOUR,
        fourth_level_desc as LEVEL_FOUR_DESCRIPTION,
        metric as METRIC,
        METRIC_NAME,
        db_source as SOURCE_DB
        from $db_output.bulletin_csv_link1;
        
 select * from $db_output.bulletin_csv_link2

# COMMAND ----------

 %sql
 create or replace table $db_output.auto_final_supp_output
 select 
   c.REPORTING_PERIOD_START, c.REPORTING_PERIOD_END,
   c.STATUS, c.BREAKDOWN,
   c.LEVEL_ONE, c.LEVEL_ONE_DESCRIPTION,
   c.LEVEL_TWO, c.LEVEL_TWO_DESCRIPTION,
   c.LEVEL_THREE, c.LEVEL_THREE_DESCRIPTION,
   c.LEVEL_FOUR, c.LEVEL_FOUR_DESCRIPTION,
   c.METRIC, c.METRIC_NAME,
   CASE WHEN c.SupType = 'Nat' THEN COALESCE(b.METRIC_VALUE, 0) 
     ELSE COALESCE(b.METRIC_VALUE, "*") 
     END as METRIC_VALUE,
   c.SOURCE_DB

 from $db_output.bulletin_csv_link2 c
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

 union 
 (select * from $db_output.automated_output_unsuppressed1
   where METRIC like "1f%"
   and LEVEL_TWO_DESCRIPTION NOT IN ('Gypsy', 'Arab', 'Roma'))