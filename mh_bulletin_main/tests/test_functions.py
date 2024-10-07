# Databricks notebook source
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as df
from pyspark.sql import Column as col

# COMMAND ----------

 %run ../mh_bulletin/parameters

# COMMAND ----------

# DBTITLE 1,Common Functions
def test_log_errors(db_output: str, df: df, test: str, metric: str, breakdown: str, test_pass: bool) -> None:
  """
  This function populates the error_log with the results of the test
  """
  spark.sql(f"""INSERT INTO {db_output}.error_log VALUES
  (current_date(), '{df}', '{test}', '{metric}', '{breakdown}', {test_pass})
  """)
  
def dfcol2list(df: df, column: str, datatype: str) -> list:
  '''
  This function gets all values in a given dataframe column and returns then as a list
  '''
  if datatype == "str":
    dfcollist = [str(r[0]) for r in df.select(F.col(column)).collect()]
  if datatype == "int":
    dfcollist = [int(r[0]) for r in df.select(F.col(column)).collect()]
  if datatype == "float":
    dfcollist = [float(r[0]) for r in df.select(F.col(column)).collect()]
    
  return dfcollist

def dfcol2value(df: df, column: str) -> str:
  '''
  This function gets the first value in a given dataframe column
  NOTE should only be used for a dataframe with a single row
  '''
  
  return df.select(F.col(column)).collect()[0][0]

def get_count_metrics(metric_metadata: dict) -> list:
  '''
  This function gets a list of all metrics in the metadata that use count suppression
  '''
  #This limits the list of metrics to only be counts
  count_metrics = []
  for k in metric_metadata:
    if metric_metadata[k]["suppression"] == "count":
      count_metrics.append(k)
  
  return count_metrics

# COMMAND ----------

# DBTITLE 1,Common Test Functions
def test_number_of_columns(df: df) -> None:
  '''
  This tests whether the number of columns in the unsuppressed output is equal to 15 (PRIMARY, SECONDARY)
  ''' 
  if len(df.columns) == 15:
    test_pass = True
    test_log_errors(db_output, df, test_number_of_columns.__name__, "All", "All", test_pass)
  else:
    test_pass = False
    test_log_errors(db_output, df, test_number_of_columns.__name__, "All", "All", test_pass)
    
def test_column_names(df: df, output_columns: list) -> None:
  """
  This asserts that all columns in the dataframe contain the expected column names for an MHSDS suppressed output
  """  
  if output_columns == df.columns:
    test_pass = True
    test_log_errors(db_output, df, test_column_names.__name__, "All", "All", test_pass)
  else:
    test_pass = False
    test_log_errors(db_output, df, test_column_names.__name__, "All", "All", test_pass)
    
def test_single_reporting_period_end(df):
  """
  This tests that there is only 1 reporting period end month in the output
  """
  end_rp = dfcol2list(df, "REPORTING_PERIOD_END", "str")
  
  if len(set(end_rp)) == 1:
    test_pass = True
    test_log_errors(db_output, df, test_single_reporting_period_end.__name__, "All", "All", test_pass)
  else:
    test_pass = False
    test_log_errors(db_output, df, test_single_reporting_period_end.__name__, "All", "All", test_pass)
    
def test_single_reporting_period_start(df):
  """
  This tests that there is only 1 reporting period start month in the output
  """
  start_rp = dfcol2list(df, "REPORTING_PERIOD_START", "str")
  
  if len(set(start_rp)) == 1:
    test_pass = True
    test_log_errors(db_output, df, test_single_reporting_period_start.__name__, "All", "All", test_pass)
  else:
    test_pass = False
    test_log_errors(db_output, df, test_single_reporting_period_start.__name__, "All", "All", test_pass)

# COMMAND ----------

# DBTITLE 1,Prep Table Functions
def test_null_values(input_df: df, col_names: list) -> None:
  for col in col_names:
    test_df = (
      input_df
      .filter(F.col(col).isNull()
      )
    )

    assert test_df.count() == 0, f"{col} contains null values"
  print(f"{test_null_values.__name__}: PASSED")
  
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

# DBTITLE 1,Unsuppressed Test Functions
def get_single_metric_df(df: df, metric: str) -> df:
  '''
  This function filters a dataframe to a single metric
  '''
  df_metric = df.filter(F.col("METRIC") == metric)
  
  return df_metric

def get_single_metric_breakdown_df(df: df, metric: str, breakdown: str) -> df:
  '''
  This function filters a dataframe to a single metric
  '''
  df_metric_breakdown = (
    df
    .filter(
      (F.col("METRIC") == metric) & (F.col("BREAKDOWN") == breakdown)
      )
  )
  
  return df_metric_breakdown

def get_distinct_metrics(df: df) -> list:
  '''
  This function returns a distinct list of all the metrics in a dataframe
  '''
  metrics = [str(r[0]) for r in df.select(F.col("METRIC")).distinct().collect()]
  
  return sorted(metrics)

def get_single_metric_breakdown_level_df(df: df, metric: str, breakdown_level: str) -> str:
  '''
  This function filters a dataframe to a single metric and LEVEL_ONE value
  '''
  df = (
    df
    .filter(
      (F.col("METRIC") == metric) & 
      (F.col("LEVEL_ONE") == breakdown_level)
    )
  )
  
  metric_breakdown_level_df = (
    df
    .groupBy(F.col("METRIC"), F.col("BREAKDOWN"))
    .agg(
      F.sum(F.col("METRIC_VALUE")).cast("int").alias("METRIC_BREAKDOWN_LEVEL_TOTAL")
    )
    .select(F.col("METRIC"), F.col("BREAKDOWN"), F.col("METRIC_BREAKDOWN_LEVEL_TOTAL"))  
  )
  
  return metric_breakdown_level_df
    
def get_breakdown_total_df(df: df, metric: str) -> df:
    '''
    This function gets breakdown totals for a given metric
    '''
    metric_df = get_single_metric_df(df, metric)
    breakdown_total_df = (    
      metric_df    
      .groupBy(F.col("METRIC"), F.col("BREAKDOWN"))    
      .agg(      
        F.sum(F.col("METRIC_VALUE")).cast("int").alias("METRIC_BREAKDOWN_TOTAL")    
      )    
      .select(F.col("METRIC"), F.col("BREAKDOWN"), F.col("METRIC_BREAKDOWN_TOTAL"))
    )   
    
    return breakdown_total_df
  
def get_breakdown_max_df(df: df, metric: str) -> df:
    '''
    This function gets breakdown max values for a given metric
    '''
    metric_df = get_single_metric_df(df, metric)
    breakdown_max_df = (    
      metric_df    
      .groupBy(F.col("METRIC"), F.col("BREAKDOWN"))    
      .agg(      
        F.sum(F.col("METRIC_VALUE")).cast("int").alias("METRIC_BREAKDOWN_MAX")    
      )    
      .select(F.col("METRIC"), F.col("BREAKDOWN"), F.col("METRIC_BREAKDOWN_MAX"))
    )  
    
    return breakdown_max_df
  
def get_unknown_breakdown_total_percentage_df(df: df, metric: str) -> df:
  '''
  This function gets the percentages of UNKNOWN total value to breakdown totals for a given metric
  '''
  unknown_breakdown_df = get_single_metric_breakdown_level_df(df, metric, "UNKNOWN")
  breakdown_total_df = get_breakdown_total_df(df, metric)
  
  join_df = (
    breakdown_total_df
    .join(
      unknown_breakdown_df,
      (breakdown_total_df.METRIC == unknown_breakdown_df.METRIC) &
      (breakdown_total_df.BREAKDOWN == unknown_breakdown_df.BREAKDOWN)    
    )
    .withColumn("UNKNOWN_VALUE_PERCENTAGE", (F.col("METRIC_BREAKDOWN_LEVEL_TOTAL") / F.col("METRIC_BREAKDOWN_TOTAL")))
    .select(breakdown_total_df.METRIC, breakdown_total_df.BREAKDOWN, unknown_breakdown_df.METRIC_BREAKDOWN_LEVEL_TOTAL, breakdown_total_df.METRIC_BREAKDOWN_TOTAL, F.col("UNKNOWN_VALUE_PERCENTAGE"))
  )
  
  return join_df
  
def test_activity_breakdown_total_df(df: df, metric: str, activity_higher_than_england_totals: list) -> None:
  '''
  This function tests that every value for an activity breakdown total (i.e. referrals, hospital spells, care contacts etc.) is the same
  '''
  breakdown_total_df = get_breakdown_total_df(df, metric)
  filt_breakdown_total = breakdown_total_df.filter(~F.col("BREAKDOWN").isin(activity_higher_than_england_totals))
  test_total_df = (
    filt_breakdown_total       
    .select(F.stddev(F.col("METRIC_BREAKDOWN_TOTAL")).alias("STDDEV"))    
  )
  std_value = test_total_df.select(F.col("STDDEV")).collect()[0][0]
  
  if int(std_value) == 0:
    test_pass = True
  else:
    test_pass = False
    
  test_log_errors(db_output, df, test_activity_breakdown_total_df.__name__, metric, "All", test_pass)
  
  return test_pass

def test_people_breakdown_total_df(df: df, metric: str) -> None:
  '''
  This function tests that every value for a people breakdown total is greater than or equal to the England total
  '''
  breakdown_total_df = get_breakdown_total_df(df, metric)
  breakdown_total_values = dfcol2list(breakdown_total_df, "METRIC_BREAKDOWN_TOTAL", "int")
  
  england_breakdown_total_df = get_single_metric_breakdown_df(breakdown_total_df, metric, "England")
  england_total_value = dfcol2value(england_breakdown_total_df, "METRIC_BREAKDOWN_TOTAL")
  
  test_people_breakdown_total_list = [value for value in breakdown_total_values if value >= england_total_value]
  
  if len(breakdown_total_values) == len(test_people_breakdown_total_list):
    test_pass = True
  else:
    test_pass = False
  
  test_log_errors(db_output, df, test_people_breakdown_total_df.__name__, metric, "All", test_pass)
  
  return test_pass

def test_breakdown_max_df(df: df, metric: str) -> None:
  '''
  This function tests that every value for a breakdown total is not equal to zero
  '''
  breakdown_max_df = get_breakdown_max_df(df, metric)
  breakdown_max_values = dfcol2list(breakdown_max_df, "METRIC_BREAKDOWN_MAX", "int")
  
  if any(value for value in breakdown_max_values) == 0:
    test_pass = False
  else:
    test_pass = True
    
  test_log_errors(db_output, df, test_breakdown_max_df.__name__, metric, "All", test_pass)
  
  return test_pass

def test_unknown_breakdown_total_percentage_df(df: df, metric: str, threshold: float) -> None:
  '''
  This function tests that every unknown breakdown total percentage is no greater than a given threshold
  '''
  unknown_breakdown_total_percentage_df = get_unknown_breakdown_total_percentage_df(df, metric)
  percentage_values = dfcol2list(unknown_breakdown_total_percentage_df, "UNKNOWN_VALUE_PERCENTAGE", "float")
  
  test_percentage_values_list = [value for value in percentage_values if value < threshold]
  
  if len(percentage_values) == len(test_percentage_values_list):
    test_pass = True
  else:
    test_pass = False
    
  test_log_errors(db_output, df, test_unknown_breakdown_total_percentage_df.__name__, metric, "All", test_pass)

def run_unsuppressed_aggregation_output_tests(df: df, metric_metadata: dict, output_columns: list, activity_higher_than_england_totals: list) -> None:
  '''
  This function runs all tests for the MH Bulletin Unsuppressed Output. 
  Current tests are:
  - test_number_of_columns
  - test_column_names
  - test_single_reporting_period_end
  - test_single_reporting_period_start
  - test_activity_breakdown_total_df
  - test_people_breakdown_total_df
  - test_breakdown_max_df
  - test_unknown_breakdown_total_percentage_df
  '''
  #non-metric-specific tests
  test_number_of_columns(df)
  print("test_number_of_columns: Test Complete")
  test_column_names(df, output_columns)
  print("test_column_names: Test Complete")
  test_single_reporting_period_end(df)
  print("test_single_reporting_period_end: Test Complete")
  test_single_reporting_period_start(df)
  print("test_single_reporting_period_start: Test Complete")
  
  for metric in metric_metadata:
    print(metric)
    related_to = metric_metadata[metric]["related_to"]
    suppression = metric_metadata[metric]["suppression"]
    
    if suppression == "percent":    
      test_breakdown_max_df(df, metric)
      print("test_breakdown_max_df: Test Complete")
    else:
      if related_to == "people":
        test_people_breakdown_total_df(df, metric)
        print("test_people_breakdown_total_df: Test Complete")
      else:
        test_activity_breakdown_total_df(df, metric, activity_higher_than_england_totals)
        print("test_activity_breakdown_total_df: Test Complete")
        
      test_unknown_breakdown_total_percentage_df(df, metric, 0.25)
      print("test_unknown_breakdown_total_percentage_df: Test Complete")

# COMMAND ----------

# DBTITLE 1,Suppressed Output Tests
def test_suppression_stars(df: df, unsup_breakdowns: list) -> None:
  '''
  This function tests that stars are present at sub-national level in the suppressed output
  '''
  subnational = df.filter(~F.col("BREAKDOWN").isin(unsup_breakdowns)) # ~ means not in
  val_star = subnational.filter(F.col("METRIC_VALUE") == "*")
  
  if val_star.count() > 0:
    test_pass = True
  else:
    test_pass = False
  
  test_log_errors(db_output, df, test_suppression_stars.__name__, "All", "All", test_pass)
  
def test_suppression_zeros(df: df, unsup_breakdowns: list) -> None:
  '''
  This function tests that zeros are not present at sub-national level in the suppressed output
  '''
  subnational = df.filter(~F.col("BREAKDOWN").isin(unsup_breakdowns)) # ~ means not in
  val_zero = subnational.filter(F.col("METRIC_VALUE") == 0)
  
  if val_zero.count() == 0:
    test_pass = True
  else:
    test_pass = False
    
  test_log_errors(db_output, df, test_suppression_zeros.__name__, "All", "All", test_pass)

def test_suppression_dashes(df: df, unsup_breakdowns: list) -> None:
  '''
  This function tests that dashes are not present at sub-national level in the suppressed output
  '''
  subnational = df.filter(~F.col("BREAKDOWN").isin(unsup_breakdowns)) # ~ means not in
  val_dash = subnational.filter(F.col("METRIC_VALUE") == "-")
  
  if val_dash.count() == 0:
    test_pass = True
  else:
    test_pass = False
    
  test_log_errors(db_output, df, test_suppression_dashes.__name__, "All", "All", test_pass)
  
def test_suppression_counts_less_than_5(df: df, metric_metadata: dict, unsup_breakdowns: list) -> None:
  '''
  This function tests that sub-national level count metrics are suppressed correctly in the suppressed output
  '''  
  count_metrics = get_count_metrics(metric_metadata)
  
  subnational = df.filter(~F.col("BREAKDOWN").isin(unsup_breakdowns)) # ~ means not in
  subnational_counts = subnational.filter(F.col("METRIC").isin(count_metrics))
  subnational_counts_int = subnational_counts.filter(F.col("METRIC_VALUE") != "*")
  val_less_than_5 = subnational_counts_int.filter(F.col("METRIC_VALUE") < 5)
  
  if val_less_than_5.count() == 0:
    test_pass = True
  else:
    test_pass = False
    
  test_log_errors(db_output, df, test_suppression_counts_less_than_5.__name__, "All", "All", test_pass)
  
def test_suppression_counts_divide_5(df: df, metric_metadata: dict, unsup_breakdowns: list):
  '''
  This function tests that sub-national level count metrics are suppressed correctly in the suppressed output
  '''
  count_metrics = get_count_metrics(metric_metadata)      
  
  subnational = df.filter(~F.col("BREAKDOWN").isin(unsup_breakdowns)) # ~ means not in
  subnational_counts = subnational.filter(F.col("METRIC").isin(count_metrics))
  subnational_counts_int = subnational_counts.filter(F.col("METRIC_VALUE") != "*")
  subnational_counts_int = subnational_counts_int.withColumn("METRIC_VALUE", subnational_counts_int.METRIC_VALUE.cast("int")) #make metric_value column integer
  subnational_counts_int = subnational_counts_int.withColumn("METRIC_VALUE_CHECK",subnational_counts_int.METRIC_VALUE / 5) #new column metric_value_check = mewasure_value / 5
  subnational_counts_int = subnational_counts_int.withColumn("METRIC_VALUE_CHECK",subnational_counts_int.METRIC_VALUE_CHECK.cast("int")) #round metric_value_check
  subnational_counts_int = subnational_counts_int.withColumn("METRIC_VALUE_CHECK",subnational_counts_int.METRIC_VALUE_CHECK * 5) #multiply metric_value_check by 5
  val_not_rounded = subnational_counts_int.filter(F.col("METRIC_VALUE") != F.col("METRIC_VALUE_CHECK")) #get rows where metric_value_check != metric_value
 
  if val_not_rounded.count() == 0:
    test_pass = True
  else:
    test_pass = False
    
  test_log_errors(db_output, df, test_suppression_counts_divide_5.__name__, "All", "All", test_pass)

def run_suppressed_aggregation_output_tests(df: df, metric_metadata: dict, output_columns: list, unsup_breakdowns: list) -> None:
  '''
  This function runs all tests for the MH Bulletin Unsuppressed Output. 
  Current tests are:
  - test_single_reporting_period_end
  - test_single_reporting_period_start
  - test_suppression_stars(df: df, unsup_breakdowns: list)
  - test_people_breakdown_total_df
  - test_breakdown_max_df
  - test_unknown_breakdown_total_percentage_df
  '''
  #non-metric-specific tests
  test_single_reporting_period_end(df)
  print("test_single_reporting_period_end: Test Complete")
  test_single_reporting_period_start(df)
  print("test_single_reporting_period_start: Test Complete")
  test_suppression_stars(df, unsup_breakdowns)
  print("test_suppression_stars: Test Complete")
  test_suppression_dashes(df, unsup_breakdowns)
  print("test_suppression_dashes: Test Complete")
  test_suppression_counts_less_than_5(df, metric_metadata, unsup_breakdowns)
  print("test_suppression_counts_less_than_5: Test Complete")
  test_suppression_counts_divide_5(df, metric_metadata, unsup_breakdowns)
  print("test_suppression_counts_divide_5: Test Complete")