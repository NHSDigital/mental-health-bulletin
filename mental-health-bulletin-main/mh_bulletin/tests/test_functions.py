# Databricks notebook source
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as df

# COMMAND ----------

 %run ../mh_bulletin/parameters

# COMMAND ----------

def test_log_errors(db_output, df, test, metric, breakdown, test_pass):
  """
  This function populates the error_log with the results of the test
  """
  spark.sql(f"INSERT INTO {db_output}.error_log VALUES(current_date(), '{df}', '{test}', '{metric}', '{breakdown}', {test_pass})")

# COMMAND ----------

def test_values_in_year(input_df: df, val_col: str, rp_startdate: str, rp_enddate: str) -> None:
  test_df = (
    input_df
    .filter(
      (~F.col(val_col).between(rp_startdate, rp_enddate))
    )
  )
  
  assert test_df.count() == 0, f"Table contains {val_col} values which occured outside of the financial year"
  print(f"{test_values_in_year.__name__}: PASSED")

# COMMAND ----------

def test_null_values(input_df: df, col_names: list) -> None:
  for col in col_names:
    test_df = (
      input_df
      .filter(F.col(col).isNull()
      )
    )

    assert test_df.count() == 0, f"{col} contains null values"
  print(f"{test_null_values.__name__}: PASSED")

# COMMAND ----------

def test_duplicate_breakdown_values(input_df: df, breakdown_cols: list, value_col: str) -> None:
  for breakdown_col in breakdown_cols:
    test_df = (
      input_df
      .groupBy(F.col(value_col))
      .agg(F.count_distinct(F.col(breakdown_col)))
      .where(F.count_distinct(F.col(breakdown_col)) > 1)
    )

    assert test_df.count() == 0, f"Mulitple distinct {breakdown_col} values for each {value_col}"
  print(f"{test_duplicate_breakdown_values.__name__}: PASSED")

# COMMAND ----------

def test_percent_unknown_geog_values(input_df: df, code_col: str, value_col: str) -> None:
  test_df = (
  input_df
  .groupBy(F.col(code_col))
  .agg(F.count_distinct(F.col(value_col)).alias("VALUE"))
  )
  
  total_value = test_df.select(F.sum("VALUE")).collect()[0][0]
  unknown_value = test_df.filter(
    (F.col(code_col).isNull())
    | (F.col(code_col) == "UNKNOWN")
  ).select(F.coalesce(F.sum("VALUE"), F.lit(0))).collect()[0][0]
    
  perc_unknown = unknown_value / total_value

  assert perc_unknown < 0.25, f"null/UNKNOWN values of {code_col} make up more than 25% of the total count of {value_col}: {perc_unknown}"
  print(f"{test_percent_unknown_geog_values.__name__}: PASSED ({round(perc_unknown, 2)})")

# COMMAND ----------

def test_ReportingPeriodEndParamMatch(df, rp_enddate):
  """
  This tests that the reporting period end in the output match in the parameter JSON
  """
  #Output ReportingPeriodEndDate list
  end_rp = list(df.select('REPORTING_PERIOD_END').distinct().toPandas()['REPORTING_PERIOD_END'])
  output_rp_enddate = str(end_rp[0])
  
  assert rp_enddate in output_rp_enddate, "Reporting Period End does not match to Parameter"
  print(f"{test_ReportingPeriodEndParamMatch.__name__}: PASSED")

# COMMAND ----------

def test_NumberOfColumnsUnsup(df):
  """
  This tests whether the number of columns in the unsuppressed output is equal to 12 (PRIMARY, SECONDARY)
  """  
  if len(df.columns) == 13:
    test_pass = True
    print(f"{test_NumberOfColumnsUnsup.__name__}: PASSED")
  else:
    test_pass = False
    print("Unsuppressed Output does not contain expected amount of columns")
  test_log_errors(db_output, df, test_NumberOfColumnsUnsup.__name__, "All", "All", test_pass)
  
def test_NumberOfColumnsSup(df):
  """
  This tests whether the number of columns in the suppressed output is equal to 13 (PRIMARY, SECONDARY)
  """
  if len(df.columns) == 13:
    test_pass = True
    print(f"{test_NumberOfColumnsSup.__name__}: PASSED")
  else:
    test_pass = False
    print("Suppressed Output does not contain expected amount of columns")
  test_log_errors(db_output, df, test_NumberOfColumnsSup.__name__, "All", "All", test_pass)
  
def test_SameColumnNamesUnsup(df, output_columns):
  """
  This asserts that all columns in the dataframe contain the expected column names for an MHSDS unsuppressed output
  """  
  if output_columns == df.columns:
    test_pass = True
    print(f"{test_SameColumnNamesUnsup.__name__}: PASSED")
  else:
    test_pass = False
    print("Unsuppressed Output does not contain expected column names")
  test_log_errors(db_output, df, test_SameColumnNamesUnsup.__name__, "All", "All", test_pass)
  
def test_SameColumnNamesSup(df, output_columns):
  """
  This asserts that all columns in the dataframe contain the expected column names for an MHSDS suppressed output
  """  
  if output_columns == df.columns:
    test_pass = True
    print(f"{test_SameColumnNamesSup.__name__}: PASSED")
  else:
    test_pass = False
    print("Suppressed Output does not contain expected column names")
  test_log_errors(db_output, df, test_SameColumnNamesSup.__name__, "All", "All", test_pass)

def test_SingleReportingPeriodEnd(df, rp_enddate):
  """
  This tests that there is only 1 reporting period end month in the output
  """
  end_rp = df.select('REPORTING_PERIOD_END').distinct().toPandas()['REPORTING_PERIOD_END']
  
  if len(end_rp) == 1:
    test_pass = True
    print(f"{test_SingleReportingPeriodEnd.__name__}: PASSED")
  else:
    test_pass = False
    print("More than a single month in ReportingPeriodEndDate")
  test_log_errors(db_output, df, test_SingleReportingPeriodEnd.__name__, "All", "All", test_pass)

# COMMAND ----------

def test_breakdown_unknowns(df1, df2):
  '''
  This function tests that all totals for counts of apeople at each breakdown are greater than or equal to the National total
  '''
  
  metric = list(df2.select("metric").dropDuplicates().toPandas()["metric"])[0]
  unknowns = df2.join(df1, (df1.BREAKDOWN == df2.BREAKDOWN) & (df1.metric == df2.metric), "inner").select(df2["*"],df1["TOTAL"])
  unknowns = unknowns.withColumn("Prop_Unknown", col("UNKNOWN")/col("TOTAL"))
  breakdowns = list(df2.select("BREAKDOWN").toPandas()["BREAKDOWN"])
  
  for value in breakdowns:
    prop_unknown = unknowns[unknowns["BREAKDOWN"]==value].select("Prop_Unknown").toPandas()["Prop_Unknown"]
    breakdown = value
    for prop in prop_unknown:
      if prop is None:
        test_pass = True
        print(f"{test_breakdown_unknowns.__name__}: PASSED")
      elif prop < 0.25:
        test_pass = True
        print(f"{test_breakdown_unknowns.__name__}: PASSED")
      else:
        test_pass = False
        print(f"WARNING: The proportion of unknowns for {breakdown} is high")
      test_log_errors(db_output, df, test_breakdown_unknowns.__name__, metric, breakdown, test_pass)
  

# COMMAND ----------

def test_suppression_stars(df):
  '''
  This function tests that stars are present at sub-national level in the suppressed output
  '''
  subnational = df[~df["BREAKDOWN"].isin(unsup_breakdowns)] # ~ means not in
  val_star = subnational[subnational["metric_VALUE"] == "*"]
  
  if val_star.count() > 0:
    test_pass = True
  else:
    test_pass = False
  test_log_errors(db_output, df, test_suppression_stars.__name__, metric, breakdown, test_pass)

def test_suppression_zeros(df):
  '''
  This function tests that zeros are not present at sub-national level in the suppressed output
  '''
  subnational = df[~df["BREAKDOWN"].isin(unsup_breakdowns)] # ~ means not in
  val_zero = subnational[subnational["metric_VALUE"] == "0"]
  
  assert val_zero.count() == 0, "Zero values in output"
  print(f"{test_suppression_zeros.__name__}: PASSED")

def test_suppression_dashes(df):
  '''
  This function tests that dashes are not present at sub-national level in the suppressed output
  '''
  subnational = df[~df["BREAKDOWN"].isin(unsup_breakdowns)] # ~ means not in
  val_dash = subnational[subnational["metric_VALUE"] == "-"]
  
  assert val_dash.count() == 0, "Dash values in output"
  print(f"{test_suppression_dashes.__name__}: PASSED")

def test_suppression_counts_less_than_5(df,metric_metadata):
  '''
  This function tests that sub-national level count metrics are suppressed correctly in the suppressed output
  '''
  
   #This limits the list of metrics to only be counts
  count_metrics = []
  for k in metric_metadata:
    if metric_metadata[k]["denominator"] == 0:
      count_metrics.append(k)
  
  subnational = df[~df["BREAKDOWN"].isin(unsup_breakdowns)] # ~ means not in
  subnational_counts = subnational[subnational["metric"].isin(count_metrics)]
  subnational_counts_int = subnational_counts[subnational_counts["metric_VALUE"] != "*"]
  val_less_than_5 = subnational_counts_int[subnational_counts_int["metric_VALUE"] < 5]
  
  assert val_less_than_5.count() == 0, "Values less than 5 in output"
  print(f"{test_suppression_counts_less_than_5.__name__}: PASSED")

def test_suppression_counts_divide_5(df,metric_metadata):
  '''
  This function tests that sub-national level count metrics are suppressed correctly in the suppressed output
  '''
  
   #This limits the list of metrics to only be counts
  count_metrics = []
  for k in metric_metadata:
    if metric_metadata[k]["denominator"] == 0:
      count_metrics.append(k)      
  
  subnational = df[~df["BREAKDOWN"].isin(unsup_breakdowns)] # ~ means not in
  subnational_counts = subnational[subnational["metric"].isin(count_metrics)] #limit to count metrics
  subnational_counts2 = subnational_counts[subnational_counts["metric_VALUE"] != "*"] #remove stars
  subnational_counts_int = subnational_counts2.withColumn("metric_VALUE",subnational_counts2.metric_VALUE.cast("int")) #make metric_value column integer
  subnational_counts_int = subnational_counts_int.withColumn("metric_VALUE_CHECK",subnational_counts_int.metric_VALUE / 5) #new column metric_value_check = mewasure_value / 5
  subnational_counts_int = subnational_counts_int.withColumn("metric_VALUE_CHECK",subnational_counts_int.metric_VALUE_CHECK.cast("int")) #round metric_value_check
  subnational_counts_int = subnational_counts_int.withColumn("metric_VALUE_CHECK",subnational_counts_int.metric_VALUE_CHECK * 5) #multiply metric_value_check by 5
  val_not_rounded = subnational_counts_int[subnational_counts_int["metric_VALUE"] != subnational_counts_int["metric_VALUE_CHECK"]] #get rows where metric_value_check != metric_value

  assert val_not_rounded.count() == 0, "Not all count metric values are rounded to nearest 5 in output"
  print(f"{test_suppression_counts_divide_5.__name__}: PASSED")

# COMMAND ----------

def single_metric_df(df, metric):
  '''
  This function filters a dataframe to a single metric
  '''
  df_metric = df[df["metric"] == metric]
  
  return df_metric

def get_distinct_metrics(df):
  '''
  This function returns a distinct list of all the metrics in a dataframe
  '''
  metrics = list(df.select('metric').distinct().toPandas()['metric'])
  
  return sorted(metrics)

def single_metric_breakdown_df(df, metric, BREAKDOWN_VALUE):
  '''
  This function filters a dataframe to a single metric and LEVEL_ONE value
  '''
  df_metric_BREAKDOWN = df[(df["metric"] == metric) & (df["LEVEL_ONE"] == BREAKDOWN_VALUE)]
  
  return df_metric_BREAKDOWN

# COMMAND ----------

def get_metric_reporting_periods(df, metric):
  '''
  This function returns a dataframe of distinct REPORTING_PERIOD_START and REPORTING_PERIOD_END for a single metric
  '''
  df_metric = single_metric_df(df, metric)
  rps = df_metric.select("metric", "REPORTING_PERIOD_START", "REPORTING_PERIOD_END").distinct()
  
  return rps

def assess_metric_reporting_periods(df, metric_metadata):
  '''
  This function assess the distinct reporting periods based on the frequency key in the metric metadata
  '''
  metric_value = df.first()["metric"]
  if metric_metadata[metric_value]["freq"] == "Q":
    print(metric_value)
    test_SingleReportingPeriodEnd(df)
    test_Quart_ReportingPeriod(df)
  elif metric_metadata[metric_value]["freq"] == "12M":
    print(metric_value)
    test_SingleReportingPeriodEnd(df)
    test_12month_ReportingPeriod(df)
  elif metric_metadata[metric_value]["freq"] == "M":
    print(metric_value)
    test_SingleReportingPeriodEnd(df)
    test_1month_ReportingPeriod(df)
  else:
    print(f"{metric_value}: Invalid code")
    
def test_measure_reporting_periods(df, metric_metadata):
  '''
  This function combines the reporting period functions into one test for better readability
  '''
  metrics = get_distinct_metrics(df)
  for metric in metrics:
    rps = get_metric_reporting_periods(df, metric)
    assess_metric_reporting_periods(rps, metric_metadata)

# COMMAND ----------

def get_metric_breakdown_totals(df, metric, metric_metadata):
  '''
  This function gets an aggregate figure for metric VALUE at each breakdown for a single metric
  '''
  df_metric = single_metric_df(df, metric)
  if metric_metadata[metric]["denominator"] == 0:
    breakdowns = df_metric.groupBy("metric", "BREAKDOWN").agg(sum("metric_VALUE").alias("TOTAL"))  ###sum of a count of activity across all breakdowns should equal the same total
  else:
    data = [{"metric": metric, "BREAKDOWN": "NO_BREAKDOWNS_TESTED", "TOTAL": 0}]
    breakdowns = spark.createDataFrame(data)
  
  return breakdowns

def assess_metric_breakdown_totals(df, metric_metadata):
  '''
  This function assess the distinct breakdown totals
  '''
  metric_value = df.first()["metric"]
  metric_breakdowns = metric_metadata[metric_value]["breakdowns"]
  metric_breakdown_names = [breakdown["breakdown_name"] for breakdown in metric_breakdowns]
  if any(breakdown in metric_breakdown_names for breakdown in ["England"]): 
    ### count of referrals, hospital spells, care contact related metrics
    if (metric_metadata[metric_value]["related_to"] != "people") & (metric_metadata[metric_value]["denominator"] == 0):
      print(metric_value)      
      if any(breakdown in metric_breakdown_names for breakdown in activity_higher_than_england_totals):      
          test_breakdown_people_count(df)
      else:      
          test_breakdown_activity_count(df)
    ### count of people related metrics
    elif (metric_metadata[metric_value]["related_to"] == "people") & (metric_metadata[metric_value]["denominator"] == 0):
      print(metric_value)
      test_breakdown_people_count(df)
    else:
      print(f"Breakdown Test for {metric_value} is not yet implemented")
  else:
    print(f"Breakdown Test for {metric_value} is not done as no England breakdown to compare with")
    
def get_metric_unknowns(df, metric, metric_metadata):
  '''
  This function gets an aggregate figure for metric VALUE at each breakdown for a single metric
  '''
  df_metric = single_metric_breakdown_df(df, metric, "Unknown")
  if metric_metadata[metric]["denominator"] == 0:
#     df_metric_UNKNOWN = df_metric[(df_metric["level_one_description"] == 'UNKNOWN')|(df_metric["level_two_description"] == 'UNKNOWN')]
    breakdowns = df_metric.groupBy("metric", "BREAKDOWN").agg(sum("metric_VALUE").alias("UNKNOWN"))  ###sum of a count of activity across all breakdowns should equal the same total
  else:
    data = [{"metric": metric, "BREAKDOWN": "NO_BREAKDOWNS_TESTED", "UNKNOWN": 0}]
    breakdowns = spark.createDataFrame(data)
  
  return breakdowns

def assess_metric_unknowns(df1, df2, metric_metadata):
  '''
  This function assess the distinct breakdown proportion of unknowns
  '''
  metric_value = df1.first()["metric"]
  test_breakdown_unknowns(df1, df2)
    
def test_measure_breakdown_figures(df, metric_metadata):
  '''
  This function combines the reporting period functions into one test for better readability
  '''
  metrics = get_distinct_metrics(df)
  for metric in metrics:
    breakdowns = get_metric_breakdown_totals(df, metric, metric_metadata)
    unknowns = get_metric_unknowns(df, metric, metric_metadata)
    assess_metric_breakdown_totals(breakdowns, metric_metadata)
    assess_metric_unknowns(breakdowns, unknowns, metric_metadata)

# COMMAND ----------

def get_distinct_provider_codes(df, metric, metric_metadata):
  '''
  This function returns a list of distinct Provider codes if the Provider breakdown exists for the metric 
  else it returns a blank list
  '''
  df_metric = single_metric_df(df, metric)
  metric_breakdowns = metric_metadata[df_metric]["breakdowns"]
  breakdown_list = [breakdown["breakdown_name"] for breakdown in metric_breakdowns]
  if breakdown_list.count("Provider") == 1:
    prov_df = df_metric[df_metric['BREAKDOWN'].startswith('Provider')]
    df_prov_list  = list(prov_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  else:
    df_prov_list = []
    
  return df_prov_list

def get_expected_provider_codes(params_json):
  '''
  This function returns a list of expected distinct Provider codes based on the rp_enddate
  '''
  rp_enddate = params_json['rp_enddate']
  exp_prov_df = spark.sql(f"""
  SELECT DISTINCT ORG_CODE
           FROM $reference_db.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('{rp_enddate}', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('{rp_enddate}', 1)	
                AND (ORG_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(ORG_CLOSE_DATE))              
                AND ORG_OPEN_DATE <= '{rp_enddate}' 
  """)
  exp_prov_list = list(exp_prov_df.select('ORG_CODE').toPandas()['ORG_CODE'])
  
  return exp_prov_list

def test_ProviderCodes(df, metric_metadata, params_json):
  """
  This tests Provider codes in output against the latest valid codes 
  """
  metrics = get_distinct_metrics(df)
  exp_prov_list = get_expected_provider_codes(params_json)
  for metric in metrics:
    prov_codes = get_distinct_provider_codes(df, metric, metric_metadata)
    if len(prov_codes) == 0:
      print(metric)
      print("Provider breakdown does not exist for this metric")
    else:
      check = all(item in exp_prov_list for item in prov_codes)
  
  if check is True:
    print(f"{test_ProviderCodes.__name__}: PASSED")    
  else :
    ValueError(print("provider codes in output are not in expected provider codes")) 

# COMMAND ----------

def get_distinct_subicb_codes(df, metric, metric_metadata):
  '''
  This function returns a list of distinct Sub-ICB codes if the Sub-ICB breakdown exists for the metric 
  else it returns a blank list
  '''
  df_metric = single_metric_df(df, metric)
  metric_breakdowns = metric_metadata[df_metric]["breakdowns"]
  breakdown_list = [breakdown["breakdown_name"] for breakdown in metric_breakdowns]
  if breakdown_list.count("SubICB") == 1:
    subicb_df = df_metric[df_metric['BREAKDOWN'].startswith('Sub-ICB')]
    df_subicb_list  = list(ccg_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  else:
    df_subicb_list = []
    
  return df_subicb_list

def get_expected_subicb_codes(params_json):
  '''
  This function returns a list of expected distinct Provider codes based on the rp_enddate
  '''
  rp_enddate = params_json['rp_enddate']  

  exp_ccg_df = spark.sql(f"""
  SELECT DISTINCT ORG_CODE
           FROM $reference_db.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('{rp_enddate}', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('{rp_enddate}', 1)	
                AND ORG_TYPE_CODE = 'CC'
                AND (ORG_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(ORG_CLOSE_DATE))
                AND ORG_OPEN_DATE <= '{rp_enddate}'
                AND NAME NOT LIKE '%HUB'
                AND NAME NOT LIKE '%NATIONAL%'
  """)
  exp_subicb_list = list(exp_subicb_df.select('ORG_CODE').toPandas()['ORG_CODE'])
  
  return exp_subicb_list

def test_SubICBCodes(df, metric_metadata, params_json):
  """
  This tests Sub-ICB codes in output against the latest valid codes 
  """
  metrics = get_distinct_metrics(df)
  exp_subicb_list = get_expected_subicb_codes(params_json)
  for metric in metrics:
    subicb_codes = get_distinct_subicb_codes(df, metric, metric_metadata)
    if len(subicb_codes) == 0:
      print(metric)
      print("Sub-ICB breakdown does not exist for this metric")
    else:
      assert set(exp_subicb_list) == set(subicb_codes), "Sub-ICB codes in output do not match expected sub-icb codes"
      print(f"{test_SubICBCodes.__name__}: PASSED")

# COMMAND ----------

def get_distinct_icb_codes(df, metric, metric_metadata):
  '''
  This function returns a list of distinct ICB codes if the ICB breakdown exists for the metric 
  else it returns a blank list
  '''
  df_metric = single_metric_df(df, metric)
  metric_breakdowns = metric_metadata[df_metric]["breakdowns"]
  breakdown_list = [breakdown["breakdown_name"] for breakdown in metric_breakdowns]
  if breakdown_list.count("ICB") == 1:
    icb_df = metric_df[metric_df['BREAKDOWN'].startswith('ICB')]
    df_icb_list  = list(icb_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  else:
    df_icb_list = []
    
  return df_icb_list

def get_expected_icb_codes(params_json):
  '''
  This function returns a list of expected distinct Provider codes based on the rp_enddate
  '''
  rp_enddate = params_json['rp_enddate']  

  exp_icb_df1 = spark.sql(f"""
  SELECT DISTINCT 
                ORG_CODE,
                NAME,
                ORG_TYPE_CODE,
                ORG_OPEN_DATE, 
                ORG_CLOSE_DATE, 
                BUSINESS_START_DATE, 
                BUSINESS_END_DATE
           FROM $reference_db.org_daily
          WHERE (BUSINESS_END_DATE >= add_months('{rp_enddate}', 1) OR ISNULL(BUSINESS_END_DATE))
                AND BUSINESS_START_DATE <= add_months('{rp_enddate}', 1)	
                AND (ORG_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(ORG_CLOSE_DATE))              
                AND ORG_OPEN_DATE <= '{rp_enddate}'
  """)
  exp_icb_df1.createOrReplaceGlobalTempView("exp_org_daily")
  exp_icb_df2 = spark.sql(f"""
  SELECT 
                REL_TYPE_CODE,
                REL_FROM_ORG_CODE,
                REL_TO_ORG_CODE, 
                REL_OPEN_DATE,
                REL_CLOSE_DATE
            FROM $reference_db.org_relationship_daily
            WHERE (REL_CLOSE_DATE >= '{rp_enddate}' OR ISNULL(REL_CLOSE_DATE))              
                   AND REL_OPEN_DATE <= '{rp_enddate}'
  """)
  exp_icb_df2.createOrReplaceGlobalTempView("exp_org_relationship_daily")
  
  exp_icb_df = spark.sql(f"""
  SELECT 
                A.ORG_CODE as ICB_CODE, 
                A.NAME as ICB_NAME, 
                C.ORG_CODE as SUB-ICB_CODE, 
                C.NAME as SUB-ICB_NAME,
                E.ORG_CODE as REGION_CODE,
                E.NAME as REGION_NAME
            FROM global_temp.exp_org_daily A
            LEFT JOIN global_temp.exp_org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
            LEFT JOIN global_temp.exp_org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
            LEFT JOIN global_temp.exp_org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
            LEFT JOIN global_temp.exp_org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
            WHERE A.ORG_TYPE_CODE = 'ST' AND B.REL_TYPE_CODE is not null
  """)
  
  exp_icb_list = list(exp_icb_df.select('ICB_CODE').toPandas()['ICB_CODE'])
  
  return exp_subicb_list

def test_subicbCodes(df, metric_metadata, params_json):
  """
  This tests Sub-ICB codes in output against the latest valid codes 
  """
  metrics = get_distinct_metrics(df)
  exp_subicb_list = get_expected_subicb_codes(params_json)
  for metric in metrics:
    subicb_codes = get_distinct_subicb_codes(df, metric, metric_metadata)
    if len(subicb_codes) == 0:
      print(metric)
      print("Sub-ICB breakdown does not exist for this metric")
    else:
      assert set(exp_subicb_list) == set(subicb_codes), "Sub-ICB codes in output do not match expected sub-icb codes"
      print(f"{test_SubICBCodes.__name__}: PASSED")

# COMMAND ----------

def test_RegionCodes(df):
  """
  This tests Commissioning Region codes in output against the latest valid codes 
  """
  #df commissioning region codes excluding UNKNOWN value
  comm_region_df = df[(df['BREAKDOWN'].endswith('Region')) & (df['PRIMARY_LEVEL'] != 'UNKNOWN')]
  df_regions_list  = list(comm_region_df.select('PRIMARY_LEVEL').distinct().toPandas()['PRIMARY_LEVEL'])
  
  #expected commissioning region codes
  comm_region_od = spark.sql("select * from $reference_db.org_daily where org_type_code = 'CE'")
  latest_comm_region = comm_region_od.filter("ORG_CLOSE_DATE is null and BUSINESS_END_DATE is null")
  expt_regions_list = list(latest_comm_region.select('ORG_CODE').toPandas()['ORG_CODE'])  
 
  assert set(df_regions_list) == set(expt_regions_list), "region codes in output do not match expected region codes"
  print(f"{test_RegionCodes.__name__}: PASSED")