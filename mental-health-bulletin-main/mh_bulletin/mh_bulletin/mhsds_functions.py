# Databricks notebook source
from dataclasses import dataclass, field
import pyspark.sql.types as T
import pyspark.sql.functions as F
import json
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta
from pyspark.sql.column import Column
from pyspark.sql import DataFrame as df
import pyspark.pandas as ps
import pandas as pd
import numpy as np
from functools import reduce  #for combining pyspark dataframes

# COMMAND ----------

 %run ./parameters

# COMMAND ----------

# DBTITLE 1,Common Functions
def timenow():
  import datetime
  return datetime.datetime.now().strftime("%Y%m%d %X")

def writelog(txt1):
  x1 = f"{timenow()} {txt1}"
  x1 = f"insert into table {db_output}.log1 values ('{x1}')"
  spark.sql(x1)

def str2dt(date: str) -> T.DateType():
  """  This function converts a string into a datetime in the 
  format datetime(YYYY, M, D, H, M)
  
  Example:
  str2dt("2021-09-01")
  >> datetime.datetime(2021, 9, 1, 0, 0)
  """
  date_format = '%Y-%m-%d'
  date_conv = datetime.strptime(date, date_format)
  
  return date_conv
 
def dt2str(date: T.DateType()) -> str:
  """
  This function converts a datetime to a string in the 
  format "YYYY-MM-DD"
  
  Example:
  dt2str(datetime(2022, 1, 1, 0, 0))
  >> "2022-01-01"
  """ 
  date_format = '%Y-%m-%d'
  date_conv = date.strftime(date_format)
  
  return date_conv
 
def first_day(date: T.DateType()) -> T.DateType():
  """  This function gets the first day of the month
  for any given date
  
  Example:
  get_first_day(datetime(2021, 1, 15, 0, 0))
  >> datetime(2021, 1, 1, 0, 0)
  """
  first_day = date.replace(day=1) ##first day
  
  return first_day

def last_day(date: T.DateType()) -> T.DateType():
  """  This function gets the last day of the month
  for any given date
  
  Example:
  get_last_day(datetime(2021, 1, 15, 0, 0))
  >> datetime(2021, 1, 31, 0, 0)
  """
  last_day_params = calendar.monthrange(date.year, date.month)[1]
  last_day = date.replace(day=last_day_params) ##last day
  
  return last_day
 
def add_months(date: T.DateType(), rp_length_num: int) -> T.DateType():
  """ This functions adds required amount of 
  months from a given date  
  
  Example:
  add_months(datetime(2021, 1, 15, 0, 0), 3)
  >> datetime(2020, 10, 15, 0, 0)
  """
  new_month = date + relativedelta(months=rp_length_num) ##minus rp_length_num months
  
  return new_month
 
def minus_months(date: T.DateType(), rp_length_num: int) -> T.DateType():
  """ This functions minuses required amount of 
  months from a given date  
  
  Example:
  minus_months(datetime(2021, 1, 15, 0, 0), 3)
  >> datetime(2020, 10, 15, 0, 0)
  """
  new_month = date - relativedelta(months=rp_length_num) ##minus rp_length_num months
  
  return new_month

def parent_breakdown(whole_breakdown: str) -> str:
  return whole_breakdown.split(";")[0]

def int_type_parent_breakdown(whole_breakdown: str) -> str:  

  l1=whole_breakdown.split(";")
  if len(l1) == 2 and l1[0] == "Intervention Type":
    breakdown = whole_breakdown.replace("Intervention Type","England")
    #print("a",len(l1), l1[0])
  elif len(l1)== 3 and l1[0] == "Intervention Type":
    breakdown = whole_breakdown.replace("Intervention Type; ","")
    #print("b",len(l1), l1[0])
  elif whole_breakdown == "England; Intervention Type": 
    breakdown = "England"
    #print("c",len(l1), l1[0])
  elif whole_breakdown not in ["Intervention Type; Age Group (Lower Level)", "Intervention Type; Gender"] and l1[0] == "Intervention Type":
    breakdown = whole_breakdown.split(";")[0]
    #print("d",len(l1), l1[0])
  elif l1[0] == "Intervention Type":
    breakdown = whole_breakdown.replace("Intervention Type","England")
    #print("e",len(l1), l1[0])
  else: breakdown = whole_breakdown
  
  breakdown = breakdown.replace("; Intervention Type", "")
  return breakdown

def is_numeric(s):
    try:
        float(s)
        return 1
    except ValueError:
        return 0
    except TypeError:
        return 0
spark.udf.register("is_numeric", is_numeric)

def get_df_name(df):
  name = [x for x in globals() if globals()[x] is df][0]
  return name

def get_provider_type_code(OrgIDProv: str) -> str:
  if OrgIDProv[0] == 'R' or OrgIDProv[0] == 'T':
    return 'NHS'
  return 'Non NHS'
spark.udf.register("get_provider_type_code", get_provider_type_code)

def get_provider_type_name(OrgIDProv: str) -> str:
  if OrgIDProv[0] == 'R' or OrgIDProv[0] == 'T':
    return 'NHS Providers'
  return 'Non NHS Providers'
spark.udf.register("get_provider_type_name", get_provider_type_name)

# COMMAND ----------

# DBTITLE 1,Parameter Functions
def get_rp_enddate(rp_startdate: str) -> str:
  """ This function gets the end of the month from
  the reporting period start date
  
  Example:
  get_rp_enddate("2021-10-01")
  >> "2021-10-31"
  """
  rp_startdate_dt = str2dt(rp_startdate)
  rp_enddate_dt = last_day(rp_startdate_dt)
  rp_enddate = dt2str(rp_enddate_dt)
  
  return rp_enddate  

def get_pub_month(rp_startdate: str, status: str) -> str:
  """ This function gets the Publication year and month
  in the format YYYYMM from the reporting period start date 
  and submission window
  
  Example:
  get_pub_month("2021-10-01", "Performance")
  >> "202201"
  """ 
  pub_month = str2dt(rp_startdate)
  
  if status == "Provisional":
    pub_month = add_months(pub_month, 2)
  elif status in ["Performance", "Final", "Adhoc"]:
    pub_month = add_months(pub_month, 3)
  else:
    return ValueError("Invalid submission window name inputted")

  pub_month = dt2str(pub_month)
  pub_month = pub_month[0:4] + pub_month[5:7]
  return pub_month
 
def get_qtr_startdate(rp_startdate: str) -> str:
  """  This functions gets the ReportingPeriodStartDate of a
  Quarterly Reporting Period
  
  Example:
  get_qtr_startdate("2020-03-01")
  >> "2020-01-01"
  """
  rp_startdate_dt = str2dt(rp_startdate) ##to datetime
  rp_qtr_startdate_dt = minus_months(rp_startdate_dt, 2) ##minus 2 months
  rp_qtr_startdate = dt2str(rp_qtr_startdate_dt) ##to string
  
  return rp_qtr_startdate
 
def get_12m_startdate(rp_startdate: str) -> str:
  """  This functions gets the ReportingPeriodStartDate of a
  12-month Reporting Period
  
  Example:
  get_12m_startdate("2020-03-01")
  >> "2019-04-01"
  """
  rp_startdate_dt = str2dt(rp_startdate) ##to datetime
  rp_12m_startdate_dt = minus_months(rp_startdate_dt, 11) ##minus 11 months
  rp_12m_startdate = dt2str(rp_12m_startdate_dt) ##to string
  
  return rp_12m_startdate

def get_month_ids(rp_startdate: str) -> int:
  """  This function gets the end_month_id and start_month_id 
  parameters from the rp_startdate. This assumes a reporting 
  period of 12 months maximum
  
  Example:
  get_month_ids("2021-09-01")
  >> 1458, 1447
  """
  rp_startdate_dt = str2dt(rp_startdate)
  start_date_dt = datetime(1900, 4, 1)
  time_diff = relativedelta(rp_startdate_dt, start_date_dt)
  end_month_id = int(time_diff.years * 12 + time_diff.months + 1)
  start_month_id = end_month_id - 11
  
  return end_month_id, start_month_id
 
def get_financial_yr_start(rp_startdate: str) -> str:
    """ This function returns the date of the start
    of the financial year using the start of the
    reporting period
    
    Example:
    get_financial_yr_start("2022-05-01")
    >> "2022-04-01"
    """
    rp_startdate_dt = str2dt(rp_startdate)
    if rp_startdate_dt.month > 3:
      financial_year_start = datetime(rp_startdate_dt.year,4,1)
    else:
      financial_year_start = datetime(rp_startdate_dt.year-1,4,1)
      
    return dt2str(financial_year_start)

def get_year_of_count(rp_startdate):
  '''
  This function returns the year_of_count which should be used to extract data from $reference_db.ONS_POPULATION_V2.  
  If the financial_yr_start is greater than the existing max(current_year) in $reference_db.ONS_POPULATION_V2 then use
  current_year = max(current_year).
  '''
  current_year = get_financial_yr_start(rp_startdate)[0:4]
  max_year_of_count = spark.sql(f"select max(year_of_count) AS year_of_count from $reference_db.ONS_POPULATION_V2 where GEOGRAPHIC_GROUP_CODE = 'E38'")
  max_year_of_count_value = max_year_of_count.first()["year_of_count"]
  year_of_count = current_year
  if (year_of_count > max_year_of_count_value):
    year_of_count = max_year_of_count_value
 
  return year_of_count 

def get_imd_year():
  imd_year = spark.sql("select max(IMD_YEAR) from $reference_db.english_indices_of_dep_v02").collect()[0][0]
  
  return imd_year

def get_lad_published_date():
  lad_published_date = spark.sql("select max(DSS_ONS_PUBLISHED_DATE) from $reference_db.ons_lsoa_ccg_stp_lad_v01").collect()[0][0]
  
  return dt2str(lad_published_date)

def get_population_year():
  population_year = spark.sql("select max(year_of_count) from $reference_db.ons_population_v2 WHERE geographic_group_code='E12'").collect()[0][0]
  
  return population_year

# COMMAND ----------

# DBTITLE 1,Parameter Data Class
@dataclass
class MHBulletinParameters:
  db_output: str
  db_source: str
  status: str  
  rp_enddate_firstday: str
  product: str
  pub_month: str = field(init=False) 
  rp_enddate: str = field(init=False)   
  end_month_id: int = field(init=False)
  start_month_id: int = field(init=False)  
  rp_startdate_1m: str = field(init=False)  
  rp_startdate_qtr: str = field(init=False)  
  rp_startdate: str = field(init=False)
  financial_year_start: str = field(init=False)
  IMD_year: int = field(init=False)
  lad_published_date: str = field(init=False)
  populationyear: int = field(init=False)
  $reference_db: str = "$reference_db"
   
  def __post_init__(self):
    self.pub_month = get_pub_month(self.rp_enddate_firstday, self.status)
    self.rp_enddate = get_rp_enddate(self.rp_enddate_firstday)
    self.end_month_id, self.start_month_id = get_month_ids(self.rp_enddate_firstday)
    self.rp_startdate_1m = self.rp_enddate_firstday
    self.rp_startdate_qtr = get_qtr_startdate(self.rp_enddate_firstday)
    self.rp_startdate = get_12m_startdate(self.rp_enddate_firstday)
    self.financial_year_start = get_financial_yr_start(self.rp_enddate_firstday)
    self.IMD_year = get_imd_year()
    self.lad_published_date = get_lad_published_date()
    self.populationyear = get_population_year()
  
  def as_dict(self):
    json_dump = json.dumps(self, sort_keys=False, default=lambda o: o.__dict__)
    return json.loads(json_dump)
  
  def run_pub():
    return None

# COMMAND ----------

# DBTITLE 1,Aggregation Functions
def mh_freq_to_rp_startdate(mh_run_params: object, freq: str) -> str:
  """  This function gets the corresponding rp_startdate for a measure_id
  depending on the "freq" key value in the metadata
  
  Current Frequency values:
  ["M", "Q", "12M"]
  
  Example:
  mh_freq_to_rp_startdate("2022-03-31", "M")
  >> "2022-01-01"
  """
  if freq == "12M":
    rp_startdate = mh_run_params.rp_startdate_12m
  elif freq == "Q":
    rp_startdate = mh_run_params.rp_startdate_qtr
  else: ##Monthly most common frequency
    rp_startdate = mh_run_params.rp_startdate_1m
    
  return rp_startdate

def create_agg_df(
  df: df,
  db_source: str,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  third_level: Column, 
  third_level_desc: Column, 
  fourth_level: Column, 
  fourth_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  measure_name: str,
  column_order: list) -> df:
  """
  
  """
  agg_df = (
            df
            .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc, third_level, third_level_desc, fourth_level, fourth_level_desc)
            .agg(aggregation_field)
            .select(
              "*",
              F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
              F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
              F.lit(breakdown).alias("BREAKDOWN"),
              F.lit(status).alias("STATUS"),
              primary_level.alias("LEVEL_ONE"),
              primary_level_desc.alias("LEVEL_ONE_DESCRIPTION"),
              secondary_level.alias("LEVEL_TWO"),
              secondary_level_desc.alias("LEVEL_TWO_DESCRIPTION"),
              third_level.alias("LEVEL_THREE"),
              third_level_desc.alias("LEVEL_THREE_DESCRIPTION",),
              fourth_level.alias("LEVEL_FOUR"),
              fourth_level_desc.alias("LEVEL_FOUR_DESCRIPTION"),
              F.lit(measure_id).alias("METRIC"),
              F.lit(measure_name).alias("METRIC_NAME"),
              F.lit(db_source).alias("SOURCE_DB"),
            )
            .select(*column_order)
  )
  
  return agg_df 

def produce_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  third_level: Column, 
  third_level_desc: Column, 
  fourth_level: Column, 
  fourth_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    agg_df = create_agg_df(prep_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           third_level, third_level_desc, fourth_level, fourth_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df  

def access_filter_for_breakdown(prep_df: df, breakdown_name: str) -> df:
  """  This function filters the access-related prep table with the relevant
  access ROW_NUMBER() field depending on the breakdown being aggregated i.e. AccessEngRN
  to be equal to 1
  
  NOTE: This function is only currently required for the CMH and CYP Access prep tables
  """ 
  if breakdown_name == "Provider" or parent_breakdown(breakdown_name) == "Provider":
    rn_field = "AccessProvRN"
  elif (breakdown_name == "CCG of Residence" or parent_breakdown(breakdown_name) == "CCG of Residence") and breakdown_name != "CCG of Residence; Provider":
    rn_field = "AccessCCGRN"
  elif (breakdown_name == "CCG - Registration or Residence" or parent_breakdown(breakdown_name) == "CCG - Registration or Residence") and breakdown_name != "CCG - Registration or Residence; Provider":
    rn_field = "AccessCCGRN"
  elif breakdown_name == "STP of Residence" or parent_breakdown(breakdown_name) == "STP of Residence":
    rn_field = "AccessSTPRN"
  elif breakdown_name == "Commissioning Region" or parent_breakdown(breakdown_name) == "Commissioning Region":
    rn_field = "AccessRegionRN"
  elif breakdown_name == "LA/UA":
    rn_field = "AccessLARN"
  elif breakdown_name == "CCG of Residence; Provider":
    rn_field = "AccessCCGProvRN"  
  else:
    rn_field = "AccessEngRN"
    
  filt_df = (
    prep_df
    .filter(F.col(rn_field) == 1)
  )
  
  return filt_df

def produce_filter_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  third_level: Column, 
  third_level_desc: Column, 
  fourth_level: Column, 
  fourth_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
#    display(prep_df)
    
    prep_filter_df = (
      prep_df.filter(filter_clause)
    )
    
#    display(prep_filter_df)
    
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           third_level, third_level_desc, fourth_level, fourth_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
#    display(agg_df)
    
    return agg_df
  
def produce_access_filter_agg_df(
  db_output: str,  
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  third_level: Column, 
  third_level_desc: Column, 
  fourth_level: Column, 
  fourth_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    """     
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    prep_access_df = access_filter_for_breakdown(prep_df, breakdown)
    
    prep_filter_df = (
      prep_access_df.filter(filter_clause)
    )
    
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           third_level, third_level_desc, fourth_level, fourth_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df
 
 
def get_numerator_df(df, numerator_id, breakdown, status, db_source):
  numerator_df = (
    df
    .filter(
      (F.col("METRIC") == numerator_id)
      & (F.col("BREAKDOWN") == breakdown)
      & (F.col("STATUS") == status)
      & (F.col("SOURCE_DB") == db_source)
    )
    .select(
      df.BREAKDOWN, 
      df.LEVEL_ONE, df.LEVEL_ONE_DESCRIPTION,
      df.LEVEL_TWO, df.LEVEL_TWO_DESCRIPTION,
      df.LEVEL_THREE, df.LEVEL_THREE_DESCRIPTION,
      df.LEVEL_FOUR, df.LEVEL_FOUR_DESCRIPTION,
      df.METRIC_VALUE.alias("NUMERATOR_COUNT")    
    )
  )
  
  return numerator_df

def get_bed_days_df(df, whole_breakdown, status, db_source):
  if "Intervention Type" in whole_breakdown:
    breakdown = int_type_parent_breakdown(whole_breakdown)
  else: breakdown = whole_breakdown
    
  bed_days_df = (
    df
    .filter(
      (F.col("METRIC") == '4a')
      & (F.col("BREAKDOWN") == breakdown)
      & (F.col("STATUS") == status)
    )
    .select(
      df.BREAKDOWN, 
      df.LEVEL_ONE, df.LEVEL_ONE_DESCRIPTION,
      df.LEVEL_TWO, df.LEVEL_TWO_DESCRIPTION,
      df.LEVEL_THREE, df.LEVEL_THREE_DESCRIPTION,
      df.LEVEL_FOUR, df.LEVEL_FOUR_DESCRIPTION,
      df.METRIC_VALUE.alias("BED_DAYS_COUNT")    
    )
  )
  
  return bed_days_df
  
def get_pop_df(df: df, measure_id: str, breakdown: str, status: str) -> df:  
  
  #the following lists are taken from the parameters notebook
  if measure_id in cyp_crude_rate_metrics:
    pop_id = "1fa" #use children and young people population metric
  elif measure_id in adu_crude_rate_metrics:
    pop_id = "1fb" #use adults 18-64 population metric
  elif measure_id in oap_crude_rate_metrics:
    pop_id = "1fc" #use adults aged 65 and over population metric
  elif measure_id in peri_crude_rate_metrics:
    pop_id = "1fd" #use women aged 15 to 54 population metric
  else:
    pop_id = "1f" #use all population metric
    
  pop_df = (
      df
      .filter(
        (F.col("METRIC") == pop_id)
        & (F.col("BREAKDOWN") == breakdown)
        & (F.col("STATUS") == status)
      )
      .select(
        df.BREAKDOWN, 
        df.LEVEL_ONE, df.LEVEL_ONE_DESCRIPTION,
        df.LEVEL_TWO, df.LEVEL_TWO_DESCRIPTION,
        df.LEVEL_THREE, df.LEVEL_THREE_DESCRIPTION,
        df.LEVEL_FOUR, df.LEVEL_FOUR_DESCRIPTION,
        df.METRIC_VALUE.alias("POPULATION_COUNT")    
      ).distinct()
    )
  
  return pop_df

def create_crude_rate_prep_df(numerator_df: df, pop_df: df, measure_id: str, breakdown: str):    
  crude_rate_prep_df = (
    numerator_df
    .join(pop_df,
                  (numerator_df.BREAKDOWN == pop_df.BREAKDOWN) &
                  (numerator_df.LEVEL_ONE == pop_df.LEVEL_ONE) &   
                  (numerator_df.LEVEL_TWO == pop_df.LEVEL_TWO) &
                  (numerator_df.LEVEL_THREE == pop_df.LEVEL_THREE) &
                  (numerator_df.LEVEL_FOUR == pop_df.LEVEL_FOUR),
                  how="left")
    .select(
      numerator_df.BREAKDOWN, 
      numerator_df.LEVEL_ONE, numerator_df.LEVEL_ONE_DESCRIPTION,
      numerator_df.LEVEL_TWO, numerator_df.LEVEL_TWO_DESCRIPTION,
      numerator_df.LEVEL_THREE, numerator_df.LEVEL_THREE_DESCRIPTION,
      numerator_df.LEVEL_FOUR, numerator_df.LEVEL_FOUR_DESCRIPTION,
      numerator_df.NUMERATOR_COUNT,
      pop_df.POPULATION_COUNT
    )
  )  
  
  return crude_rate_prep_df
  
def produce_crude_rate_agg_df(
  db_output: str,
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  third_level: Column, 
  third_level_desc: Column,
  fourth_level: Column, 
  fourth_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    insert_df = spark.table(f"{db_output}.{table_name}")
    
    numerator_df = get_numerator_df(insert_df, numerator_id, breakdown, status, db_source)

    pop_df = get_pop_df(insert_df, measure_id, breakdown, status)

    crude_rate_prep_df = create_crude_rate_prep_df(numerator_df, pop_df, measure_id, breakdown)
    
    agg_df = (
        crude_rate_prep_df
        .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc, third_level, third_level_desc, fourth_level, fourth_level_desc)
        .agg(aggregation_field)
        .select(
              "*",
              F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
              F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
              F.lit(breakdown).alias("BREAKDOWN"),
              F.lit(status).alias("STATUS"),             
              F.lit(measure_id).alias("METRIC"),
              F.lit(measure_name).alias("METRIC_NAME"),
              F.lit(db_source).alias("SOURCE_DB"),
            )
        .select(*column_order)
    )
    
    return agg_df
  
def produce_std_rate_agg_df(
  db_output: str,
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  third_level: Column, 
  third_level_desc: Column,
  fourth_level: Column, 
  fourth_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    prep_df = spark.table(f"{db_output}.{table_name}")
    
    prep_filter_df = (
      prep_df
      .filter(F.col("Ethnic_level") == breakdown)
    )
    
    agg_df = create_agg_df(prep_filter_df, db_source, rp_startdate, rp_enddate, 
                           primary_level, primary_level_desc, secondary_level, secondary_level_desc,
                           third_level, third_level_desc, fourth_level, fourth_level_desc,
                           aggregation_field, breakdown, status, measure_id, measure_name, column_order)
    
    return agg_df


def create_restr_per_bed_days_prep_df(numerator_df: df, bed_days_df: df, whole_breakdown: str) -> df: 
  """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
  import numpy as np
  import pandas as pd
  geog_breakdown_check = [br for br in geog_breakdowns if br in whole_breakdown]
  
  if len(geog_breakdown_check) > 0 and "Intervention Type" not in whole_breakdown: # if breakdown contains an item in geog_breakdowns and not England; Intervention Type
    restr_per_bed_days_prep_df = (
    numerator_df
    .join(bed_days_df,   
                  (numerator_df.LEVEL_ONE == bed_days_df.LEVEL_ONE),
                  how="left")
    .select(
      numerator_df.BREAKDOWN, 
      numerator_df.LEVEL_ONE, numerator_df.LEVEL_ONE_DESCRIPTION,
      numerator_df.LEVEL_TWO, numerator_df.LEVEL_TWO_DESCRIPTION,
      numerator_df.LEVEL_THREE, numerator_df.LEVEL_THREE_DESCRIPTION,
      numerator_df.LEVEL_FOUR, numerator_df.LEVEL_FOUR_DESCRIPTION,
      numerator_df.NUMERATOR_COUNT,
      bed_days_df.BED_DAYS_COUNT
    )
  )

  elif "Intervention Type" in whole_breakdown and whole_breakdown != "England; Intervention Type":
    num_pdf = numerator_df.select("*").toPandas()
    bed_days_pdf = bed_days_df.select("*").toPandas()
    l1=list(num_pdf)+["BED_DAYS_COUNT"]
    breakdown_split = whole_breakdown.split(";")
    if breakdown_split[0] == "Intervention Type":  
      #correct logic for when Intervention Type is used as the level_one
      pdf = num_pdf.merge(bed_days_pdf, left_on = ["LEVEL_TWO", "LEVEL_THREE", "LEVEL_FOUR"],
      right_on = ["LEVEL_ONE", "LEVEL_TWO", "LEVEL_THREE"], how = 'left')
    else:      
      #correct logic for when Intervention Type is used as the level_one
      pdf = num_pdf.merge(bed_days_pdf, left_on = ["LEVEL_ONE"],
      right_on = ["LEVEL_ONE"], how = 'left')
    l2=list(pdf)
    d2={x:x.replace("_x","") for x in l2}
    l2=[x for x in d2 if "_y" not in x]
    pdf=pdf[l2]
    pdf=pdf.rename(columns=d2)

    restr_per_bed_days_prep_df=spark.createDataFrame(pdf)
  elif whole_breakdown == "England; Intervention Type": #if just England Intervention Type
    restr_per_bed_days_prep_df = (
    numerator_df
    .crossJoin(bed_days_df)
    .select(
      numerator_df.BREAKDOWN, 
      numerator_df.LEVEL_ONE, numerator_df.LEVEL_ONE_DESCRIPTION,
      numerator_df.LEVEL_TWO, numerator_df.LEVEL_TWO_DESCRIPTION,
      numerator_df.LEVEL_THREE, numerator_df.LEVEL_THREE_DESCRIPTION,
      numerator_df.LEVEL_FOUR, numerator_df.LEVEL_FOUR_DESCRIPTION,
      numerator_df.NUMERATOR_COUNT,
      bed_days_df.BED_DAYS_COUNT
    )
  )
  else:  #not an intervention type breakdown
    restr_per_bed_days_prep_df = (
      numerator_df
      .join(bed_days_df,
                    (numerator_df.BREAKDOWN == bed_days_df.BREAKDOWN) &
                    (numerator_df.LEVEL_ONE == bed_days_df.LEVEL_ONE) &   
                    (numerator_df.LEVEL_TWO == bed_days_df.LEVEL_TWO) &
                    (numerator_df.LEVEL_THREE == bed_days_df.LEVEL_THREE) &
                    (numerator_df.LEVEL_FOUR == bed_days_df.LEVEL_FOUR),
                    how="left")
      .select(
        numerator_df.BREAKDOWN, 
        numerator_df.LEVEL_ONE, numerator_df.LEVEL_ONE_DESCRIPTION,
        numerator_df.LEVEL_TWO, numerator_df.LEVEL_TWO_DESCRIPTION,
        numerator_df.LEVEL_THREE, numerator_df.LEVEL_THREE_DESCRIPTION,
        numerator_df.LEVEL_FOUR, numerator_df.LEVEL_FOUR_DESCRIPTION,
        numerator_df.NUMERATOR_COUNT,
        bed_days_df.BED_DAYS_COUNT
      )
    )   
  
  return restr_per_bed_days_prep_df
  
def produce_restr_per_bed_days_agg_df(
  db_output: str,
  db_source: str,
  table_name: str,
  filter_clause: Column,
  rp_startdate: str, 
  rp_enddate: str, 
  primary_level: Column, 
  primary_level_desc: Column, 
  secondary_level: Column, 
  secondary_level_desc: Column,
  third_level: Column, 
  third_level_desc: Column,
  fourth_level: Column, 
  fourth_level_desc: Column,
  aggregation_field: Column,    
  breakdown: str,
  status: str,
  measure_id: str, 
  numerator_id: str, 
  measure_name: str,
  column_order: list
) -> df:
    """  This function produces the aggregation output dataframe from a defined preparation table
    for all measures and breakdowns according to the mhsds_measure_metadata dictionary 
    
    NOTE: All required filtering should be done on the final prep table
    """  
    insert_df = spark.table(f"{db_output}.{table_name}")
    
    numerator_df = get_numerator_df(insert_df, numerator_id, breakdown, status, db_source)

    bed_days_df = get_bed_days_df(insert_df, breakdown, status, db_source)

    restr_per_bd_prep_df = create_restr_per_bed_days_prep_df(numerator_df, bed_days_df, breakdown)
    
    agg_df = (
        restr_per_bd_prep_df
        .groupBy(primary_level, primary_level_desc, secondary_level, secondary_level_desc, third_level, third_level_desc, fourth_level, fourth_level_desc)
        .agg(aggregation_field)
        .select(
              "*",
              F.lit(rp_startdate).alias("REPORTING_PERIOD_START"),
              F.lit(rp_enddate).alias("REPORTING_PERIOD_END"),
              F.lit(breakdown).alias("BREAKDOWN"),
              F.lit(status).alias("STATUS"),             
              F.lit(measure_id).alias("METRIC"),
              F.lit(measure_name).alias("METRIC_NAME"),
              F.lit(db_source).alias("SOURCE_DB"),
            )
        .select(*column_order)
    )
    
    return agg_df

# COMMAND ----------

# DBTITLE 1,Insert into Table Functions
def insert_unsup_agg(agg_df: df, db_output: str, unsup_columns: list, output_table: str) -> None:
  """
  This function uses the aggregation dataframe produced in the different aggregation functions 
  and selects certain columns and inserts them into the required unsuppressed table in measure metadata
  
  Example:
  
  """
  unsup_agg_df = (
    agg_df
    .withColumn("METRIC_VALUE", F.coalesce(F.col("METRIC_VALUE"), F.lit(0)))
    .select(*unsup_columns)
  )
  
  unsup_agg_df.write.insertInto(f"{db_output}.{output_table}")
  
def insert_sup_agg(agg_df: df, db_output: str, measure_name: str, sup_columns: list, output_table: str) -> None:
  """
  This function uses the aggregation dataframe produced in the different aggregation functions 
  and selects certain columns and inserts them into the required suppressed table in measure metadata
  
  Example:
  
  """
  sup_agg_df = (
    agg_df
    .withColumn("METRIC_NAME", F.lit(measure_name))
    .select(*sup_columns)
  )
  
  sup_agg_df.write.insertInto(f"{db_output}.{output_table}")

# COMMAND ----------

# DBTITLE 1,Suppression Functions
def count_suppression(x: int, base=5) -> str:  
  """  The function has the logic for suppression of MHSDS count measures
  i.e. "denominator" == 0 in measure_metadata
  
  Examples:
  count_suppression(254)
  >> 255
  count_suppression(3)
  >> *
  """
  if x < 5:
    return '*'
  else:
    return str(int(base * round(float(x)/base)))
  
  
def mhsds_suppression(df: df, suppression_type: str, breakdown: str, measure_id: str, rp_enddate: str, status: str, numerator_id: str, db_source: str) -> df:
  """  The function has the logic for suppression of MHSDS count and percent measures
  if numerator_id value of percentage = "*" then percentage value = "*"
  else round to nearest whole number
  
  Example:
  """
  supp_method = F.udf(lambda z: count_suppression(z))
  
  if suppression_type == "count":
    supp_df = (
      df
      .filter(
        (F.col("METRIC") == measure_id)
        & (F.col("BREAKDOWN") == breakdown)
        & (F.col("REPORTING_PERIOD_END") == rp_enddate)
        & (F.col("STATUS") == status)
        & (F.col("SOURCE_DB") == db_source)
      )
      .withColumn("METRIC_VALUE", supp_method(F.col("METRIC_VALUE")))
    )  
  
  else:
    perc_values = (
      df
      .filter(
        (F.col("METRIC") == measure_id)
        & (F.col("BREAKDOWN") == breakdown)
        & (F.col("REPORTING_PERIOD_END") == rp_enddate)
        & (F.col("STATUS") == status)
        & (F.col("SOURCE_DB") == db_source)
      )
    )

    num_values = (
      df
      .filter(
        (F.col("METRIC") == numerator_id)
        & (F.col("BREAKDOWN") == breakdown)
        & (F.col("STATUS") == status)
        & (F.col("SOURCE_DB") == db_source)
      )
      .select(
        F.col("BREAKDOWN"), F.col("LEVEL_ONE"), F.col("LEVEL_TWO"), F.col("LEVEL_THREE"), F.col("LEVEL_FOUR"), F.col("METRIC").alias("NUMERATOR_ID"), F.col("METRIC_VALUE").alias("NUMERATOR_VALUE")
      )
    )

    perc_num_comb = (
      num_values
      .join(perc_values, ["BREAKDOWN", "LEVEL_ONE", "LEVEL_TWO", "LEVEL_THREE", "LEVEL_FOUR"])
    )

    #percentage suppression logic
    perc_supp_logic = (
    F.when(F.col("NUMERATOR_VALUE_SUPP") == "*", F.lit("*"))
     .otherwise(F.round(F.col("METRIC_VALUE"), 0)) #round to nearest whole number (0dp)
    )

    supp_df = (
      perc_num_comb
      .withColumn("NUMERATOR_VALUE_SUPP", supp_method(F.col("NUMERATOR_VALUE")))
      .withColumn("METRIC_VALUE", perc_supp_logic)
    )
    
  return supp_df

# COMMAND ----------

# DBTITLE 1,Breakdown Functions
def create_level_dataframe(level_list: list, lookup_columns: list) -> df:
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each necessary measure_id    
    """
    level_df = (
    spark.createDataFrame(
      level_list,
      lookup_columns
    )
    )
    
    return level_df
  
def combine_breakdowns_dict_to_df(bd1: dict, bd2: dict) -> df:
  bd1_df = create_level_dataframe(bd1["level_list"], bd1["lookup_col"])
  bd2_df = create_level_dataframe(bd2["level_list"], bd2["lookup_col"])
  cross_df = bd1_df.crossJoin(bd2_df)
  
  return cross_df

def three_way_cross_join_df(df1: df, df2: df, df3: df) -> df:
  cross2_df = df1.crossJoin(df2)
  cross2_df_a = cross2_df.withColumn("tag", F.lit("a"))
  bd3_df_a = df3.withColumn("tag", F.lit("a"))
  
  cross3_df = cross2_df_a.join(bd3_df_a, cross2_df_a.tag == bd3_df_a.tag, "outer")
  
  return cross3_df

def combine_breakdowns_dict_to_level_three_df(bd1: dict, bd2: dict, bd3: dict) -> df:
  bd1_df = create_level_dataframe(bd1["level_list"], bd1["lookup_col"])
  bd2_df = create_level_dataframe(bd2["level_list"], bd2["lookup_col"])
  bd3_df = create_level_dataframe(bd3["level_list"], bd3["lookup_col"])
  
  cross3_df = three_way_cross_join_df(bd1_df, bd2_df, bd3_df)
  
  return cross3_df

def combine_org_ref_and_breakdown_dict_to_df(org_df: df, bd: dict) -> df:
  bd_df = create_level_dataframe(bd["level_list"], bd["lookup_col"])
  cross_df = org_df.crossJoin(bd_df)
  
  return cross_df

def get_str_or_dict_df(db_output: str, table) -> df:
  if type(table) == dict:
    df = create_level_dataframe(table["level_list"], table["lookup_col"])
  elif type(table) == str:
    df = spark.table(f"{db_output}.{table}")
    
  return df

# COMMAND ----------

# DBTITLE 1,CSV Lookup Table Data Class and Functions
@dataclass
class MHMeasureLevel():
  db_output: str
  measure_id: str
  breakdown: str
  level_tables: list
  level_list: list
  level_fields: list
  lookup_columns: list
  
  def create_level_dataframe(self):
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each necessary measure_id    
    """
    if len(self.level_tables) > 0:
      level_one_df = get_str_or_dict_df(self.db_output, self.level_tables[0])
      if len(self.level_tables) == 1:
        level_df = level_one_df
      elif len(self.level_tables) == 2:
        level_two_df = get_str_or_dict_df(self.db_output, self.level_tables[1])        
        level_df = level_one_df.crossJoin(level_two_df)
      elif len(self.level_tables) == 3:        
        level_two_df = get_str_or_dict_df(self.db_output, self.level_tables[1])        
        level_three_df = get_str_or_dict_df(self.db_output, self.level_tables[2])        
        level_df = three_way_cross_join_df(level_one_df, level_two_df, level_three_df)      
    else:
      level_df = (
      spark.createDataFrame(
        self.level_list,
        self.lookup_columns
      )
    )
    
    return level_df
  
  def insert_level_dataframe(self):
    """
    This function creates the necessary "level" dataframe for each breakdown
    This is then scaled to each measure_id using the create_level_dataframe() function
    and then inserts into the $db_output.bbrb_level_values table
    """
    level_df = self.create_level_dataframe()
    insert_df = level_df.select(self.level_fields).distinct()
    
    insert_df.write.insertInto(f"{self.db_output}.bulletin_level_values")

def insert_bulletin_lookup_values(mh_run_params: object, metadata: dict, column_order: list):
  """  This function is the main function which creates the 
  main mh_monthly_level_values table for each measure_id and breakdown.
  
  This table is then used as the "skeleton" in which mh_raw values are joined.
  This is to ensure that we have every possible breakdown value for each
  measure and breakdown in the final output.
  i.e. a provider may have submitted to the MHS000Header Table but not the MHS101Referral Table in
  month, therefore, they wouldn't appear in the MHS01 "Provider" breakdown raw output.
  The "skeleton" table ensures cases like this aren't missed in the final output
  
  Example:
  jan22perf = MHRunParameters("mh_clear_collab", "mhsds_database", "2022-01-31")
  insert_mh_monthly_lookup_values(jan22perf.db_output, jan22perf.db_source, jan22perf.rp_enddate, jan22perf.end_month_id, common_measure_ids)
  >> all breakdown and primary/secondary level for each measure inserted into the mh_monthly_level_values table
  """
  spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.bulletin_csv_lookup")
  for measure_id in metadata:    
    measure_name = metadata[measure_id]["name"]
    breakdowns = metadata[measure_id]["breakdowns"]
    
    for breakdown in breakdowns:        
      breakdown_name = breakdown["breakdown_name"]
      lvl_tables = breakdown["level_tables"]
      lvl_list = breakdown["level_list"]
      lvl_fields = breakdown["level_fields"]
      lvl_cols = breakdown["lookup_col"]
      print(timenow(), f"{measure_id}: {breakdown_name}")
      #set up MHMeasureLevel class
      lvl = MHMeasureLevel(mh_run_params.db_output, measure_id, breakdown_name, lvl_tables, lvl_list, lvl_fields, lvl_cols)
      #create level_df to insert into mh_monthly_level_values
      lvl.insert_level_dataframe() #all breakdowns and primary levels added for the measure_id
      #add measure_id to dataframe and insert into mh_monthly_lookup
      lvl_df = spark.table(f"{db_output}.bulletin_level_values")
      lvl_measure_df = (
        lvl_df
        .select(
          "*",
          F.lit(mh_run_params.rp_startdate).alias("REPORTING_PERIOD_START"),
          F.lit(mh_run_params.rp_enddate).alias("REPORTING_PERIOD_END"),
          F.lit(mh_run_params.status).alias("STATUS"),
          F.lit(measure_id).alias("METRIC"),
          F.lit(measure_name).alias("METRIC_NAME"),
          F.lit(mh_run_params.db_source).alias("SOURCE_DB")
        )
        .select(*column_order)
      )
      #write into final ref table
      lvl_measure_df.write.insertInto(f"{mh_run_params.db_output}.bulletin_csv_lookup")
      #reset level_values table for next measure_id
      spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.bulletin_level_values")                              
      #restart loop
    ##loop ends with full populated levels and breakdowns for each measure id in metadata

def unionAll(*dfs):
  from pyspark.sql import DataFrame
  return reduce(DataFrame.unionAll, dfs)    
        
def insert_bulletin_lookup_values_v2(mh_run_params: object, metadata: dict, column_order: list):
  """  This function is an alternative to insert_bulletin_lookup_values
  """
  spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.bulletin_csv_lookup_v2")
  for measure_id in metadata:    
    measure_name = metadata[measure_id]["name"]
    breakdowns = metadata[measure_id]["breakdowns"]
    list_lvl_measure_df = []
    
    for breakdown in breakdowns:        
      breakdown_name = breakdown["breakdown_name"]
      lvl_tables = breakdown["level_tables"]
      lvl_list = breakdown["level_list"]
      lvl_fields = breakdown["level_fields"]
      lvl_cols = breakdown["lookup_col"]
      print(timenow(), f"{measure_id}: {breakdown_name}")
      #set up MHMeasureLevel class
      lvl = MHMeasureLevel(mh_run_params.db_output, measure_id, breakdown_name, lvl_tables, lvl_list, lvl_fields, lvl_cols)
      #create level_df to insert into mh_monthly_level_values
      lvl.insert_level_dataframe() #all breakdowns and primary levels added for the measure_id
      #add measure_id to dataframe and insert into mh_monthly_lookup
      lvl_df = spark.table(f"{db_output}.bulletin_level_values")
      lvl_measure_df = (
        lvl_df
        .withColumns({"REPORTING_PERIOD_START": F.lit(mh_run_params.rp_startdate),
                      "REPORTING_PERIOD_END": F.lit(mh_run_params.rp_enddate),
                      "STATUS": F.lit(mh_run_params.status),
                      "METRIC": F.lit(measure_id),
                      "METRIC_NAME": F.lit(measure_name),
                      "SOURCE_DB": F.lit(mh_run_params.db_source)})
      )      
      lvl_measure_df = (
        lvl_measure_df
        .select(*column_order)
      )

      #add to list
      list_lvl_measure_df += [lvl_measure_df]
     
      #reset level_values table for next measure_id
      spark.sql(f"TRUNCATE TABLE {mh_run_params.db_output}.bulletin_level_values")   
    
    # combine and insert after every metric instead of after every breakdown
    print(timenow(), "Insert bulletin_csv_lookup_v2")
    lvl_measure_df = unionAll([df for df in list_lvl_measure_df])
    list_lvl_measure_df = []
    lvl_measure_df.write.saveAsTable(f"{mh_run_params.db_output}.bulletin_csv_lookup_v2", mode = 'Append')
    # lvl_measure_df.write.insertInto(f"{mh_run_params.db_output}.bulletin_csv_lookup")
      
    #restart loop
    ##loop ends with full populated levels and breakdowns for each measure id in metadata