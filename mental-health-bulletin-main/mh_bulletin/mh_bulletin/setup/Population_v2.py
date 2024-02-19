# Databricks notebook source
# DBTITLE 1,Create Widgets - for standlone use
# please comment out when calling from another notebpok

# '''
dbutils.widgets.removeAll()

dbutils.widgets.text("db_output", "personal_db", "db_output")
db_output  = dbutils.widgets.get("db_output")
assert db_output

dbutils.widgets.text("db_source", "mhsds_database", "db_source")
db_source = dbutils.widgets.get("db_source")
assert db_source

dbutils.widgets.text("rp_enddate", "2023-03-31", "rp_enddate")
rp_enddate = dbutils.widgets.get("rp_enddate")
assert rp_enddate

dbutils.widgets.text("rp_startdate", "2023-03-31", "rp_startdate")
rp_startdate = dbutils.widgets.get("rp_startdate")
assert rp_startdate

dbutils.widgets.text("status", "Final", "status")
status  = dbutils.widgets.get("status")
assert status

chapters = ["ALL", "CHAP1", "CHAP4", "CHAP5", "CHAP6", "CHAP7", "CHAP9", "CHAP10", "CHAP11", "CHAP12", "CHAP13", "CHAP14", "CHAP15", "CHAP16", "CHAP17", "CHAP18", "CHAP19"]
dbutils.widgets.dropdown("chapter", "ALL", chapters)
product = dbutils.widgets.get("chapter")
# '''
#

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %run ../parameters

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
# end_month_id = dbutils.widgets.get("end_month_id")
# start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status  = dbutils.widgets.get("status")

# COMMAND ----------

 %sql
 DELETE FROM $db_output.automated_output_unsuppressed1 WHERE METRIC in ("1f", "1fa", "1fb", "1fc", "1fd", "1fe");
 DELETE FROM $db_output.automated_output_suppressed WHERE METRIC in ("1f", "1fa", "1fb", "1fc", "1fd", "1fe");

# COMMAND ----------

 %md  
 
 
 -- replaced by census_2021_national_derived 
 DROP TABLE IF EXISTS $db_output.MHB_ons_population_v2;
 CREATE TABLE         $db_output.MHB_ons_population_v2 USING DELTA AS
 SELECT *
 FROM $reference_db.ons_population_v2
 WHERE geographic_group_code='E12' and year_of_count = '$populationyear' ;
   
 OPTIMIZE $db_output.MHB_ons_population_v2
 ZORDER BY ons_release_date;

# COMMAND ----------

# DBTITLE 1,Age reference data used in the two census tables
# used in census_2021_national_derived  and census_2021_sub_icb_derived and pop_health

from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql import DataFrame as df

def mapage(x):
  x = x.lower()
  lx = x.split(" ")
  if len(lx) == 1 or " to " in x:
      ly = [i for i in range(int(lx[0]), 1 + int(lx[-1]))]
  elif "under " in x:
      ly = [i for i in range(int(lx[1]))]
  elif "over " in x:
      ly = [i for i in range(int(lx[1]), 125)]
  elif " or over" in x or " and over" in x:
      ly = [i for i in range(int(lx[0]), 125)]
  elif " or under" in x or " and under" in x:
      ly = [i for i in range(1 + int(lx[0]))]
  try:
    return ly
  except:
    print(">>>>> Warning >>>>> \n", x)
    return x

AgeCat = {"age_group_higher_level": ["Under 18", "18 and over"],
          "age_group_perinatal": ["Under 15", "15 to 55", "56 and over"],
          "age_group_lower_common": ["Under 18", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84", "85 to 89", "90 or over"],
          "age_group_lower_chap1": ["0 to 5", "6 to 10", "11 to 15", "16", "17", "18", "19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69",  "70 to 74", "75 to 79", "80 to 84", "85 to 89", "90 or over"],
          "age_group_lower_chap45": ["Under 14", "14 to 15", "16 to 17", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84", "85 to 89", "90 or over"],
          "age_group_lower_chap10": ["Under 14", "14 to 15", "16 to 17", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 or over"],
          "age_group_lower_chap10a": ["Under 14", "14 to 17", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59",
                                      "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84", "85 to 89", "90 or over"],
          "age_group_lower_chap11": ["Under 15", "15 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 or over"],
          "imd_base": ["0 to 2", "3 to 4", "5 to 7", "8 to 9", "10 to 14", "15", "16 to 17", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65", "66 to 69", "70 to 74", "75 to 79", "80 to 84", "85 or over"],
         "imd_split_15_65": ["Under 15", "15 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 or over"],
          "imd_split_15_19_65": ["Under 15", "15 to 19", "20 to 64", "65 or over"],
          "imd_decile_age_lower": ["Under 18", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84", "85 or over"],
          "imd_decile_gender_age_lower": ["0 to 4", "5 to 9", "10 to 15", "16 to 17", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84", "85 or over"]
}

AgeCat1 = {key: {x1: x for x in AgeCat[key] for x1 in mapage(x)} for key in AgeCat}
AgeData = [[i] + [AgeCat1[key][i] if i in AgeCat1[key] else "NA" for key in AgeCat1] for i in range(125)]

lh = ["Age int"] + [f"{x} string" for x in AgeCat]
schema1 = ', '.join(lh)
df1 = spark.createDataFrame(AgeData, schema = schema1)
spark.sql(f"drop table if exists {db_output}.MapAge")
df1.write.saveAsTable(f"{db_output}.MapAge", mode = 'overwrite')
display(df1)

# COMMAND ----------

# DBTITLE 1,Ethnicity lookup by ethniccode - used in the two census tables
# used in census_2021_national_derived  and census_2021_sub_icb_derived

def mapethniccode(NHSDEthnicity, EthCat):  #maps ethnicity to upper / lower / name 
  if EthCat == "UpperEthnicity":
    d = {'White': ('A', 'B', 'C'), 'Mixed': ('D', 'E', 'F', 'G'), 'Asian or Asian British': ('H', 'J', 'K', 'L'), 
         'Black or Black British': ('M', 'N', 'P'), 'Other Ethnic Groups': ('R', 'S'), 'Not Stated': 'Z', 
         'Not Known': '99'}
    l1 = [d1 for d1 in d if NHSDEthnicity in d[d1]]
    if len(l1) == 1: 
      return l1[0]
    if len(l1) != 1: 
      return "Unknown"
    
  if EthCat == "LowerEthnicityCode":
    d = {'Z': 'Not Stated', '99': 'Not Known'}
    for k1 in ("A", "B", "C", "D", "E", "F", "G", "H", "J", "K", "L", "M", "N", "P", "R", "S"):
      d[k1] = k1
    if NHSDEthnicity in d: 
      return d[NHSDEthnicity]
    else: 
      return 'Unknown'
  
  if EthCat == "LowerEthnicityName":
    d = {'A': 'British', 'B': 'Irish', 'C': 'Any Other White Background', 'D': 'White and Black Caribbean', 
         'E': 'White and Black African', 'F': 'White and Asian', 'G': 'Any Other Mixed Background', 'H': 'Indian', 
         'J': 'Pakistani', 'K': 'Bangladeshi', 'L': 'Any Other Asian Background', 'M': 'Caribbean', 'N': 'African', 
         'P': 'Any Other Black Background', 'R': 'Chinese', 'S': 'Any Other Ethnic Group', 'Z': 'Not Stated', 
         '99': 'Not Known'}
    if NHSDEthnicity in d: 
      return d[NHSDEthnicity]
    else: 
      return 'Unknown'
  
lec1 = ["LowerEthnicityCode", "UpperEthnicity", "LowerEthnicityName"]
lne1 = ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'Z', '99')
lnec1 = [[x] + [mapethniccode(x, y) for y in lec1] for x in lne1]
schema1 = ' '.join(["NHSDEthnicity string"] + [f", {x} string" for x in lec1])
df1 = spark.createDataFrame(lnec1, schema = schema1)
spark.sql(f"drop table if exists {db_output}.mapethniccode")
df1.write.saveAsTable(f"{db_output}.mapethniccode", mode = 'overwrite')
display(df1)

# COMMAND ----------

# DBTITLE 1,Ethnicity lookup table by ethnic_group_code - used to replace case when
# used in census_2021_national_derived and census_2021_sub_icb_derived

x1 = """select code as ethnic_group_code, description as Ethnic_group from $reference_db.ONS_2021_census_lookup
where field = 'ethnic_group_code' and code != -8"""
df1 = spark.sql(x1)

# ethnic_group_formatted
d1 = {13: 'British', 14: 'Irish', 15: 'Gypsy', 16: 'Roma', 17: 'Any Other White Background', 
      11: 'White and Black Caribbean', 10: 'White and Black African', 9: 'White and Asian', 
      12: 'Any Other Mixed Background', 3: 'Indian', 4: 'Pakistani', 1: 'Bangladeshi', 
      5: 'Any Other Asian Background', 7: 'Caribbean', 6: 'African', 8: 'Any Other Black Background', 
      2:'Chinese', 18: 'Arab', 19: 'Any Other Ethnic Group'}  

# LowerEthnicityCode
d2 = {13: 'A', 14: 'B', 17: 'C', 11: 'D', 10: 'E', 9: 'F', 12: 'G', 3: 'H', 4: 'J', 1: 'K', 5: 'L', 7: 'M', 6: 'N', 
      8: 'P', 2: 'R', 19: 'S'}

# UpperEthnicity
l3 = [[(13, 14, 17, 15, 16), 'White'],
     [(11, 10, 9, 12), 'Mixed'],
     [(3, 4, 1, 5), 'Asian or Asian British'],
     [(7, 6, 8), 'Black or Black British'],
     [(2, 19, 18), 'Other Ethnic Groups']]
d3 = dict(zip([x1 for x in l3 for x1 in x[0]], [x[1] for x in l3 for x1 in x[0]]))

set1 = set([key for key in d1] + [key for key in d2] + [key for key in d3])
l1 = [[key, d1[key] if key in d1 else "Unknown", d2[key] if key in d2 else "Unknown", 
       d3[key] if key in d3 else "Unknown"] for key in set1]
df1a = spark.createDataFrame(l1, schema = "ethnic_group_code int, LowerEthnicityName string, LowerEthnicityCode string, UpperEthnicity string")

df1 = (df1.join(df1a, on='ethnic_group_code', how='left'))
spark.sql(f"drop table if exists {db_output}.mapethnicgroupcode")
df1.write.saveAsTable(f"{db_output}.mapethnicgroupcode", mode = 'overwrite')
display(df1)

# COMMAND ----------

# DBTITLE 1,IMD lookup
l1 = [[10, '10 Least deprived', '05 Least deprived'],
      [9, '09 Less deprived', '05 Least deprived'],
      [8, '08 Less deprived', '04'],
      [7, '07 Less deprived', '04'],
      [6, '06 Less deprived', '03'],
      [5, '05 More deprived', '03'],
      [4, '04 More deprived', '02'],
      [3, '03 More deprived', '02'],
      [2, '02 More deprived', '01 Most deprived'],
      [1, '01 Most deprived', '01 Most deprived']]
schema1 = "IMD int, IMD_Decile string, IMD_Quintile string"
df1 = spark.createDataFrame(l1, schema = schema1)
spark.sql(f"drop table if exists {db_output}.mapimd")
df1.write.saveAsTable(f"{db_output}.mapimd", mode = 'overwrite')

IMD_year = spark.sql("select max(IMD_YEAR) from $reference_db.english_indices_of_dep_v02").collect()[0][0]
display(df1)

# COMMAND ----------

 %sql
 create or replace temporary view census_2021_national_derived as
 select 
 ethnic_group_code,
 sex_code,
 
 case when age_code between 0 and 5 then '0 to 5'
 	  when age_code between 6 and 10 then '6 to 10'
 	  when age_code between 11 and 15 then '11 to 15'
 	  when age_code = 16 then '16'
 	  when age_code = 17 then '17'
 	  when age_code = 18 then '18'
 	  when age_code = 19 then '19'
 	  when age_code between 20 and 24 then '20 to 24'
       when age_code between 25 and 29 then '25 to 29'
 	  when age_code between 30 and 34 then '30 to 34'
       when age_code between 35 and 39 then '35 to 39'
 	  when age_code between 40 and 44 then '40 to 44'
       when age_code between 45 and 49 then '45 to 49'
 	  when age_code between 50 and 54 then '50 to 54'
       when age_code between 55 and 59 then '55 to 59'
 	  when age_code between 60 and 64 then '60 to 64'
       when age_code between 65 and 69 then '65 to 69'
 	  when age_code between 70 and 74 then '70 to 74'
       when age_code between 75 and 79 then '75 to 79'
 	  when age_code between 80 and 84 then '80 to 84'
       when age_code between 85 and 89 then '85 to 89'
 	  when age_code >= '90' then '90 or over' else 'Unknown' end as Age_Group,
      sum(observation) as observation
 from $reference_db.ons_2021_census
 
 where area_type_group_code = "E92" ---England grouping only
 and ons_date = (select max(ons_date) from $reference_db.ons_2021_census where area_type_group_code = "E92") ---most recent data
 group by ethnic_group_code,
 sex_code,
 case when age_code between 0 and 5 then '0 to 5'
 	  when age_code between 6 and 10 then '6 to 10'
 	  when age_code between 11 and 15 then '11 to 15'
 	  when age_code = 16 then '16'
 	  when age_code = 17 then '17'
 	  when age_code = 18 then '18'
 	  when age_code = 19 then '19'
 	  when age_code between 20 and 24 then '20 to 24'
       when age_code between 25 and 29 then '25 to 29'
 	  when age_code between 30 and 34 then '30 to 34'
       when age_code between 35 and 39 then '35 to 39'
 	  when age_code between 40 and 44 then '40 to 44'
       when age_code between 45 and 49 then '45 to 49'
 	  when age_code between 50 and 54 then '50 to 54'
       when age_code between 55 and 59 then '55 to 59'
 	  when age_code between 60 and 64 then '60 to 64'
       when age_code between 65 and 69 then '65 to 69'
 	  when age_code between 70 and 74 then '70 to 74'
       when age_code between 75 and 79 then '75 to 79'
 	  when age_code between 80 and 84 then '80 to 84'
       when age_code between 85 and 89 then '85 to 89'
 	  when age_code >= '90' then '90 or over' else 'Unknown' end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.pop_health;
 CREATE TABLE IF NOT EXISTS $db_output.pop_health AS
 select 
 CASE WHEN ethnic_group_code = 13 THEN 'British'
 WHEN ethnic_group_code = 14 THEN 'Irish'
 WHEN ethnic_group_code = 15 THEN 'Gypsy'
 WHEN ethnic_group_code = 16 THEN 'Roma'
 WHEN ethnic_group_code = 17 THEN 'Any Other White Background'
 WHEN ethnic_group_code = 11 THEN 'White and Black Caribbean'
 WHEN ethnic_group_code = 10 THEN 'White and Black African'
 WHEN ethnic_group_code = 9 THEN 'White and Asian'
 WHEN ethnic_group_code = 12 THEN 'Any Other Mixed Background'
 WHEN ethnic_group_code = 3 THEN 'Indian'
 WHEN ethnic_group_code = 4 THEN 'Pakistani'
 WHEN ethnic_group_code = 1 THEN 'Bangladeshi'
 WHEN ethnic_group_code = 5 THEN 'Any Other Asian Background'
 WHEN ethnic_group_code = 7 THEN 'Caribbean'
 WHEN ethnic_group_code = 6 THEN 'African'
 WHEN ethnic_group_code = 8 THEN 'Any Other Black Background'
 WHEN ethnic_group_code = 2 THEN 'Chinese'
 WHEN ethnic_group_code = 18 THEN 'Arab'
 WHEN ethnic_group_code = 19 THEN 'Any Other Ethnic Group'
 ELSE 'Unknown' END as ethnic_group_formatted, ---get everything after : in description (i.e. lower ethnic group)
 a.description as Ethnic_group,
 ethnic_group_code,
 CASE WHEN ethnic_group_code = 13 THEN 'A'
 WHEN ethnic_group_code = 14 THEN 'B'
 WHEN ethnic_group_code = 17 THEN 'C'
 WHEN ethnic_group_code = 11 THEN 'D'
 WHEN ethnic_group_code = 10 THEN 'E'
 WHEN ethnic_group_code = 9 THEN 'F'
 WHEN ethnic_group_code = 12 THEN 'G'
 WHEN ethnic_group_code = 3 THEN 'H'
 WHEN ethnic_group_code = 4 THEN 'J'
 WHEN ethnic_group_code = 1 THEN 'K'
 WHEN ethnic_group_code = 5 THEN 'L'
 WHEN ethnic_group_code = 7 THEN 'M'
 WHEN ethnic_group_code = 6 THEN 'N'
 WHEN ethnic_group_code = 8 THEN 'P'
 WHEN ethnic_group_code = 2 THEN 'R' 
 WHEN ethnic_group_code = 19 THEN 'S'
 ELSE 'Unknown' END AS LowerEthnicityCode,
 CASE WHEN ethnic_group_code IN (13, 14, 17, 15, 16) THEN 'White'
      WHEN ethnic_group_code IN (11, 10, 9, 12) THEN 'Mixed'
      WHEN ethnic_group_code IN (3, 4, 1, 5) THEN 'Asian or Asian British'
      WHEN ethnic_group_code IN (7, 6, 8) THEN 'Black or Black British'
      WHEN ethnic_group_code IN (2, 19, 18) THEN 'Other Ethnic Groups'
      ELSE 'Unknown' END AS UpperEthnicity,
 sex_code as Der_Gender,
 Age_Group,
 observation as Population
 from census_2021_national_derived c
 left join $reference_db.ONS_2021_census_lookup a on c.ethnic_group_code = a.code and a.field = "ethnic_group_code"
 where ethnic_group_code != -8 ---exclude does not apply ethnicity
 order by ethnic_group_formatted, der_gender desc, Age_Group

# COMMAND ----------

# DBTITLE 1,census_2021_national_derived
 %sql
 
 -- drop table if exists $db_output.ons_2021_census;
 create or replace table $db_output.ons_2021_census -- using delta as
 select ethnic_group_code, sex_code, age_code as Age, observation
 from $reference_db.ons_2021_census
 where area_type_group_code = "E92" ---England grouping only
 and ons_date = (select max(ons_date) from $reference_db.ons_2021_census where area_type_group_code = "E92") ---most recent data
 and ethnic_group_code != -8;
 
 create or replace temporary view vw_ons_2021_census as
 SELECT c1.Age as Age1, c1.GenderCode, c1.Der_Gender, 
   c1.ethnic_group_code, c1.Ethnic_group, c1.LowerEthnicityCode,  c1.LowerEthnicityName, c1.UpperEthnicity,
   m.*, COALESCE(c1.Population, 0) AS Population 
   FROM $db_output.MapAge AS m
   LEFT JOIN 
     (select Age, c.sex_code as GenderCode,
     case when c.sex_code = 1 then 'Male' when c.sex_code = 2 then 'Female' else 'Unknown' end as Der_Gender,
     a.ethnic_group_code, a.Ethnic_group, a.LowerEthnicityCode,  a.LowerEthnicityName, a.UpperEthnicity,
     sum(observation) as Population
     from $db_output.ons_2021_census c
     left join $db_output.mapethnicgroupcode a on c.ethnic_group_code = a.ethnic_group_code
     group by c.Age, GenderCode, Der_Gender, a.ethnic_group_code, a.Ethnic_group, a.LowerEthnicityCode,  a.LowerEthnicityName, a.UpperEthnicity
     order by c.Age, der_gender, ethnic_group_code) c1
   ON m.Age = c1.Age
 order by m.Age, c1.der_gender, c1.ethnic_group_code;
 
 DROP TABLE IF EXISTS $db_output.census_2021_national_derived;
 CREATE TABLE         $db_output.census_2021_national_derived USING DELTA AS
 select * from vw_ons_2021_census
 where Population > 0;
 
 drop view if exists vw_ons_2021_census;
 drop table if exists $db_output.ons_2021_census;
 
 -- select sum(Population) from $db_output.census_2021_national_derived
 select * from $db_output.census_2021_national_derived

# COMMAND ----------

# DBTITLE 1,census_2021_sub_icb_derived
 %sql
 
 --drop table if exists $db_output.ons_2021_census;
 create or replace table $db_output.ons_2021_census using delta as
 select area_type_group_code, area_type_code, Age_code as Age, sex_code, ethnic_group_code, observation 
 from $reference_db.ons_2021_census
 where area_type_group_code = "E38" ---Sub ICB grouping only
 and ons_date = (select max(ons_date) from $reference_db.ons_2021_census where area_type_group_code = "E38")
 and ethnic_group_code != -8 ---exclude does not apply ethnicity;
 
 create or replace temporary view vw_ons_2021_census as
 SELECT c1.Age as Age1, c1.GenderCode, c1.Der_Gender, 
   c1.ethnic_group_code, c1.Ethnic_group, c1.LowerEthnicityCode,  c1.LowerEthnicityName, c1.UpperEthnicity,
   c1.CCG_Code, c1.CCG_Name, c1.STP_Code, c1.STP_Name, c1.Region_Code, c1.Region_Name,
   
   c1.DH_GEOGRAPHY_CODE,
   c1.area_type_code,
   
   m.*, COALESCE(c1.Population, 0) AS Population 
   FROM $db_output.MapAge AS m
   LEFT JOIN (
     select c.Age, c.sex_code as GenderCode,
     coalesce(o.DH_GEOGRAPHY_CODE, od.DH_GEOGRAPHY_CODE) as DH_GEOGRAPHY_CODE, 
     area_type_code,
     
     case when c.sex_code = 1 then 'Male' when c.sex_code = 2 then 'Female' else 'Unknown' end as Der_Gender,
     a.ethnic_group_code, a.Ethnic_group, a.LowerEthnicityCode,  a.LowerEthnicityName, a.UpperEthnicity, 
     stp.CCG_Code, stp.CCG_Name, stp.STP_Code, stp.STP_Name, stp.Region_Code, stp.Region_Name,
     sum(observation) as Population
     from $db_output.ons_2021_census c
     left join $reference_db.ONS_CHD_GEO_EQUIVALENTS o on c.area_type_code = o.GEOGRAPHY_CODE and area_type_group_code = "E38" and is_current = 1
     -- workaround as ccg no longer 92a and 70f included
     left join $reference_db.ONS_CHD_GEO_EQUIVALENTS od on c.area_type_code = od.GEOGRAPHY_CODE and area_type_code in ("E38000246", 'E38000248')
     
     left join $db_output.mapethnicgroupcode a on c.ethnic_group_code = a.ethnic_group_code
     left join $db_output.STP_Region_Mapping stp on coalesce(o.DH_GEOGRAPHY_CODE, od.DH_GEOGRAPHY_CODE) = stp.ccg_code
     group by coalesce(o.DH_GEOGRAPHY_CODE, od.DH_GEOGRAPHY_CODE), 
     area_type_code, c.Age, GenderCode, Der_Gender, a.ethnic_group_code, a.Ethnic_group, a.LowerEthnicityCode,  a.LowerEthnicityName, a.UpperEthnicity, stp.CCG_Code, stp.CCG_Name, stp.STP_Code, stp.STP_Name, stp.Region_Code, stp.Region_Name
     order by c.Age, der_gender, stp.CCG_Code, ethnic_group_code) c1 
 ON m.Age = c1.Age
 order by m.Age, c1.der_gender, c1.CCG_Code, c1.ethnic_group_code;
 
 drop table if exists $db_output.census_2021_sub_icb_derived;
 create table $db_output.census_2021_sub_icb_derived using delta as
 select * from vw_ons_2021_census where CCG_Code is not null;
 
 drop view if exists vw_ons_2021_census;
 drop table if exists $db_output.ons_2021_census;

# COMMAND ----------

# DBTITLE 1,mhb_lsoatoccg
 %sql
 create or replace temporary view mhb_lsoatoccg as
 select LSOA11, ccg_code, stp_code from
 (select *, row_number() over (partition by LSOA11 order by RECORD_START_DATE desc, RECORD_END_DATE asc) as row_num from
 (select distinct(*) from 
 (select LSOA11, ccg as CCG_Code, stp as STP_Code, RECORD_START_DATE,
 case when RECORD_END_DATE is null then '31-12-9999' else RECORD_END_DATE end as RECORD_END_DATE
 from $reference_db.postcode
 where LEFT(LSOA11, 3) = "E01")))
 where row_num = 1;
 
 --drop table if exists $db_output.mhb_lsoatoccg;
 
 create or replace table $db_output.mhb_lsoatoccg as
 select * from mhb_lsoatoccg;
 
 drop view if exists mhb_lsoatoccg;

# COMMAND ----------

# DBTITLE 1,Filter ons_2021_census_lsoa_age_sex
 %sql
 create or replace table $db_output.ons_2021_census_lsoa_age_sex as
 select *, 
 case when sex_code = 1 then 2 else 1 end as GenderCode,
 case when age_code = 1 then "0 to 2"
   when age_code = 2 then "3 to 4"
   when age_code = 3 then "5 to 7"
   when age_code = 4 then "8 to 9"
   when age_code = 5 then "10 to 14"
   when age_code = 6 then "15"
   when age_code = 7 then "16 to 17"
   when age_code = 8 then "18 to 19"
   when age_code = 9 then "20 to 24"
   when age_code = 10 then "25 to 29"
   when age_code = 11 then "30 to 34"
   when age_code = 12 then "35 to 39"
   when age_code = 13 then "40 to 44"
   when age_code = 14 then "45 to 49"
   when age_code = 15 then "50 to 54"
   when age_code = 16 then "55 to 59"
   when age_code = 17 then "60 to 64"
   when age_code = 18 then "65"
   when age_code = 19 then "66 to 69"
   when age_code = 20 then "70 to 74"
   when age_code = 21 then "75 to 79"
   when age_code = 22 then "80 to 84"
   when age_code = 23 then "85 or over"
 end as imd_age_base,
 case  when age_code in (1,2,3,4,5,6,7) then "Under 18"
   when age_code in (8) then "18 to 19"
   when age_code in (9) then "20 to 24"
   when age_code in (10) then "25 to 29"
   when age_code in (11) then "30 to 34"
   when age_code in (12) then "35 to 39"
   when age_code in (13) then "40 to 44"
   when age_code in (14) then "45 to 49"
   when age_code in (15) then "50 to 54"
   when age_code in (16) then "55 to 59"
   when age_code in (17) then "60 to 64"
   when age_code in (18,19) then "65 to 69"
   when age_code in (20) then "70 to 74"
   when age_code in (21) then "75 to 79"
   when age_code in (22) then "80 to 84"
   when age_code in (23) then "85 or over"
 end as imd_decile_age_lower,
 case when age_code in (1,2) then "0 to 4"
   when age_code in (3,4) then "5 to 9"
   when age_code in (5,6) then "10 to 15"
   when age_code in (7) then "16 to 17"
   when age_code in (8) then "18 to 19"
   when age_code in (9) then "20 to 24"
   when age_code in (10) then "25 to 29"
   when age_code in (11) then "30 to 34"
   when age_code in (12) then "35 to 39"
   when age_code in (13) then "40 to 44"
   when age_code in (14) then "45 to 49"
   when age_code in (15) then "50 to 54"
   when age_code in (16) then "55 to 59"
   when age_code in (17) then "60 to 64"
   when age_code in (18, 19) then "65 to 69"
   when age_code in (20) then "70 to 74"
   when age_code in (21) then "75 to 79"
   when age_code in (22) then "80 to 84"
   when age_code in (23) then "85 or over"
 end as imd_decile_gender_age_lower,
 case when age_code in (1,2,3,4,5) then "Under 15"
   when age_code in (6,7,8) then "15 to 19"
   when age_code in (9,10,11,12,13,14,15,16,17) then "20 to 64"
   when age_code in (18,19,20,21,22,23) then "65 or over"
 end as imd_split_15_19_65,
 observation as Population
 FROM $reference_db.ons_2021_census_lsoa_age_sex
 where area_type_code like '%E01%';
 
 select area_type_code, age_code, imd_age_base, imd_decile_age_lower, imd_decile_gender_age_lower, GenderCode, Population
 from $db_output.ons_2021_census_lsoa_age_sex

# COMMAND ----------

# DBTITLE 1,mhb_imd_pop
 %sql
 create or replace temporary view vw_mhb_imd_pop as
 select * from
   (select area_type_code, Age_Code, imd_age_base, imd_decile_age_lower, imd_decile_gender_age_lower, imd_split_15_19_65,
   GenderCode, Population
    from $db_output.ons_2021_census_lsoa_age_sex) c1  
 
 left join
   (select DECI_IMD, IMD_YEAR, LSOA_CODE_2011, LSOA_NAME_2011
   from $reference_db.ENGLISH_INDICES_OF_DEP_V02
   where LSOA_CODE_2011 like '%E01%'
   and IMD_YEAR = (select max(IMD_YEAR) from $reference_db.english_indices_of_dep_v02)) r
 on c1.area_type_code = r.LSOA_CODE_2011
 
 left join $db_output.mapIMD m 
 on r.DECI_IMD = m.IMD
 
 left join $db_output.mhb_lsoatoccg lc
 on c1.area_type_code = lc.LSOA11
 
 left join 
 (select CCG_Code as CCG_Code1, CCG_Name, STP_Name, Region_Code, Region_name from $db_output.STP_Region_Mapping) stp
 on stp.ccg_code1 = lc.ccg_code
 ;
 
 --drop table if exists $db_output.mhb_imd_pop1;
 create or replace table $db_output.mhb_imd_pop as
 select Age_Code, imd_age_base, imd_decile_age_lower, imd_decile_gender_age_lower, imd_split_15_19_65, 
   GenderCode, IMD, 
   case when IMD is null then 'Unknown' else IMD_Decile end as IMD_Decile,
   case when IMD is null then 'Unknown' else IMD_Quintile end as IMD_Quintile,
   --ethnic_group_code, Ethnic_group, LowerEthnicityCode, LowerEthnicity, UpperEthnicity, 
   case when lsoa11 is null then 'Unknown' else lsoa11 end as lsoa11, 
   case when LSOA_NAME_2011 is null then 'Unknown' else LSOA_NAME_2011 end as LSOA_NAME_2011, 
   case when ccg_code is null then 'Unknown' else ccg_code end as ccg_code, 
   case when CCG_Name is null then 'Unknown' else CCG_Name end as CCG_Name, 
   case when stp_code is null then 'Unknown' else stp_code end as stp_code, 
   case when STP_Name is null then 'Unknown' else STP_Name end as STP_Name, 
   case when Region_Code is null then 'Unknown' else Region_Code end as Region_Code, 
   case when Region_name is null then 'Unknown' else Region_name end as Region_name,  
   Population 
 from vw_mhb_imd_pop;
 
 select * from  $db_output.mhb_imd_pop

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Populations that include age in one of the 4 levels - not IMD
# age type and breakdown label - these combine to create a list for aggregation
# please only list those that have age column, all others go into the next cell


age_types = {"age_group_lower_common": "Age Group (Lower Level)",
             "age_group_lower_chap1":  "Age Group (Lower Level)",
             "age_group_higher_level": "Age Group (Higher Level)",
             "age_group_lower_chap45": "Age Group (Lower Level)",
             "age_group_lower_chap10": "Age Group (Lower Level)",
             "age_group_lower_chap10a": "Age Group (Lower Level)",
             "age_group_lower_chap11": "Age Group (Lower Level)"}


list_age_dicts = []
for age_type in age_types:
  age_lower_pop = {
    "breakdown_name": f"England; {age_types[age_type]}", 
    "source_table": "census_2021_national_derived", 
    "primary_level": F.col(age_type), 
    "primary_level_desc": F.col(age_type), 
    "secondary_level": F.lit("NONE"), 
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(age_lower_pop)
  
  eth_higher_age_pop = {
    "breakdown_name": f"Ethnicity (Higher Level); {age_types[age_type]}", 
    "source_table": "census_2021_national_derived", 
    "primary_level": F.col("UpperEthnicity"), 
    "primary_level_desc": F.col("UpperEthnicity"), 
    "secondary_level": F.col(age_type), 
    "secondary_level_desc": F.col(age_type), 
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(eth_higher_age_pop)
  
  eth_lower_age_pop = {
    "breakdown_name": f"Ethnicity (Lower Level); {age_types[age_type]}", 
    "source_table": "census_2021_national_derived", 
    "primary_level": F.col("LowerEthnicityCode"), 
    "primary_level_desc": F.col("LowerEthnicityName"), 
    "secondary_level": F.col(age_type), 
    "secondary_level_desc": F.col(age_type), 
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(eth_lower_age_pop)
  
  der_gender_age_pop = {
    "breakdown_name": f"Gender; {age_types[age_type]}", 
    "source_table": "census_2021_national_derived", 
    "primary_level": F.col("GenderCode"), 
    "primary_level_desc": F.col("Der_Gender"), 
    "secondary_level": F.col(age_type), 
    "secondary_level_desc": F.col(age_type), 
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(der_gender_age_pop)
  
  ccg_prac_res_age_lower_pop = {
    "breakdown_name": f"CCG - Registration or Residence; {age_types[age_type]}", 
    "source_table": "census_2021_sub_icb_derived", 
    "primary_level": F.col("CCG_Code"), 
    "primary_level_desc": F.col("CCG_Name"), 
    "secondary_level": F.col(age_type), 
    "secondary_level_desc": F.col(age_type), 
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(ccg_prac_res_age_lower_pop)
  
  ccg_res_age_lower_pop = {
    "breakdown_name": f"CCG of Residence; {age_types[age_type]}", 
    "source_table": "census_2021_sub_icb_derived", 
    "primary_level": F.col("CCG_Code"), 
    "primary_level_desc": F.col("CCG_Name"), 
    "secondary_level": F.col(age_type), 
    "secondary_level_desc": F.col(age_type), 
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(ccg_res_age_lower_pop)

  ccg_prac_res_der_gender_age_lower_pop = {
    "breakdown_name": "CCG - Registration or Residence; Gender; Age Group (Lower Level)", 
    "source_table": "census_2021_sub_icb_derived", 
    "primary_level": F.col("CCG_Code"), 
    "primary_level_desc": F.col("CCG_Name"), 
    "secondary_level": F.col("GenderCode"), 
    "secondary_level_desc": F.col("Der_Gender"), 
    "third_level": F.col(age_type),
    "third_level_desc": F.col(age_type),
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(ccg_prac_res_der_gender_age_lower_pop)
  
  ccg_res_der_gender_age_lower_pop = {
    "breakdown_name": "CCG of Residence; Gender; Age Group (Lower Level)", 
    "source_table": "census_2021_sub_icb_derived", 
    "primary_level": F.col("CCG_Code"), 
    "primary_level_desc": F.col("CCG_Name"), 
    "secondary_level": F.col("GenderCode"), 
    "secondary_level_desc": F.col("Der_Gender"), 
    "third_level": F.col(age_type),
    "third_level_desc": F.col(age_type),
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(ccg_res_der_gender_age_lower_pop)
  
  stp_res_age_lower_pop = {
    "breakdown_name": f"STP of Residence; {age_types[age_type]}", 
    "source_table": "census_2021_sub_icb_derived", 
    "primary_level": F.col("STP_Code"), 
    "primary_level_desc": F.col("STP_Name"), 
    "secondary_level": F.col(age_type),
    "secondary_level_desc": F.col(age_type),
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(stp_res_age_lower_pop)
  
  comm_region_age_lower_pop = {
    "breakdown_name": f"Commissioning Region; {age_types[age_type]}", 
    "source_table": "census_2021_sub_icb_derived", 
    "primary_level": F.col("Region_Code"), 
    "primary_level_desc": F.col("Region_Name"), 
    "secondary_level": F.col(age_type),
    "secondary_level_desc": F.col(age_type),
    "third_level": F.lit("NONE"), 
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"), 
    "fourth_level_desc": F.lit("NONE"), 
    "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
  }
  list_age_dicts.append(comm_region_age_lower_pop)


#for x in list_age_dicts:
#  print(f"\n>>>>>>\n", x)

# COMMAND ----------

# DBTITLE 1,IMD age dictionaries
age_types = {"imd_age_base": "Age Group (Lower Level)",
             "imd_decile_age_lower":  "Age Group (Lower Level)",
             "imd_decile_gender_age_lower": "Gender; Age Group (Lower Level)",
             "imd_split_15_19_65": "Age Group (Lower Level)"}

imd_age_dicts = []
for age_type in age_types:
  imd_decile_age_pop = {
    "breakdown_name": f"IMD Decile; {age_types[age_type]}", 
    "source_table": "mhb_imd_pop", 
    "primary_level": F.col("IMD_Decile"),    
    "primary_level_desc": F.col("IMD_Decile"),
    "secondary_level": F.col(age_type),
    "secondary_level_desc": F.col(age_type),
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"),
    "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
  }
  imd_age_dicts.append(imd_decile_age_pop)
  
  imd_quintile_age_pop = {
    "breakdown_name": f"IMD Quintile; {age_types[age_type]}", 
    "source_table": "mhb_imd_pop", 
    "primary_level": F.col("IMD_Quintile"),    
    "primary_level_desc": F.col("IMD_Quintile"),
    "secondary_level": F.col(age_type),
    "secondary_level_desc": F.col(age_type),
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"),
    "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
  }
  imd_age_dicts.append(imd_quintile_age_pop)
    
# print(imd_age_dicts)

# COMMAND ----------

# DBTITLE 1,Base Population metadata - not including Age in any level
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql import DataFrame as df

eng_pop = {
  "breakdown_name": "England", 
  "source_table": "census_2021_national_derived", 
  "primary_level": F.lit("England"), 
  "primary_level_desc": F.lit("England"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}

imd_decile_eth_higher_pop = {
  "breakdown_name": "IMD Decile; Ethnicity (Higher Level)", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("IMD_Decile"),
  "primary_level_desc": F.col("IMD_Decile"),
  "secondary_level": F.col("UpperEthnicity"),
  "secondary_level_desc": F.col("UpperEthnicity"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}

imd_quintile_eth_higher_pop = {
  "breakdown_name": "IMD Quintile; Ethnicity (Higher Level)", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("IMD_Quintile"),
  "primary_level_desc": F.col("IMD_Quintile"),
  "secondary_level": F.col("UpperEthnicity"),
  "secondary_level_desc": F.col("UpperEthnicity"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}

eth_higher_pop = {
  "breakdown_name": "England; Ethnicity (Higher Level)", 
  "source_table": "census_2021_national_derived", 
  "primary_level": F.col("UpperEthnicity"), 
  "primary_level_desc": F.col("UpperEthnicity"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
eth_lower_pop = {
  "breakdown_name": "England; Ethnicity (Lower Level)", 
  "source_table": "census_2021_national_derived", 
  "primary_level": F.col("LowerEthnicityCode"), 
  "primary_level_desc": F.col("LowerEthnicityName"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
der_gender_pop = {
  "breakdown_name": "England; Gender", 
  "source_table": "census_2021_national_derived", 
  "primary_level": F.col("GenderCode"), 
  "primary_level_desc": F.col("Der_Gender"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}

imd_decile_pop = {
  "breakdown_name": "England; IMD Decile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("IMD_Decile"),    
  "primary_level_desc": F.col("IMD_Decile"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

imd_quintile_pop = {
  "breakdown_name": "England; IMD Quintile", 
  "source_table": "mhb_imd_pop",
  "primary_level": F.col("IMD_Quintile"),    
  "primary_level_desc": F.col("IMD_Quintile"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

ccg_prac_res_pop = {
  "breakdown_name": "CCG - Registration or Residence", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
ccg_prac_res_eth_higher_pop = {
  "breakdown_name": "CCG - Registration or Residence; Ethnicity (Higher Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.col("UpperEthnicity"), 
  "secondary_level_desc": F.col("UpperEthnicity"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
ccg_prac_res_eth_lower_pop = {
  "breakdown_name": "CCG - Registration or Residence; Ethnicity (Lower Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.col("LowerEthnicityCode"), 
  "secondary_level_desc": F.col("LowerEthnicityName"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
ccg_prac_res_der_gender_pop = {
  "breakdown_name": "CCG - Registration or Residence; Gender", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.col("GenderCode"), 
  "secondary_level_desc": F.col("Der_Gender"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
ccg_prac_res_imd_decile_pop = {
  "breakdown_name": "CCG - Registration or Residence; IMD Decile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("CCG_Code"),    
  "primary_level_desc": F.col("CCG_Name"),
  "secondary_level": F.col("IMD_Decile"),
  "secondary_level_desc": F.col("IMD_Decile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

ccg_prac_res_imd_quintile_pop = {
  "breakdown_name": "CCG - Registration or Residence; IMD Quintile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("CCG_Code"),    
  "primary_level_desc": F.col("CCG_Name"),
  "secondary_level": F.col("IMD_Quintile"),
  "secondary_level_desc": F.col("IMD_Quintile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

ccg_res_pop = {
  "breakdown_name": "CCG of Residence", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
ccg_res_eth_higher_pop = {
  "breakdown_name": "CCG of Residence; Ethnicity (Higher Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.col("UpperEthnicity"), 
  "secondary_level_desc": F.col("UpperEthnicity"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
ccg_res_eth_lower_pop = {
  "breakdown_name": "CCG of Residence; Ethnicity (Lower Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.col("LowerEthnicityCode"), 
  "secondary_level_desc": F.col("LowerEthnicityName"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
ccg_res_der_gender_pop = {
  "breakdown_name": "CCG of Residence; Gender", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("CCG_Code"), 
  "primary_level_desc": F.col("CCG_Name"), 
  "secondary_level": F.col("GenderCode"), 
  "secondary_level_desc": F.col("Der_Gender"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}

ccg_res_imd_decile_pop = {
  "breakdown_name": "CCG of Residence; IMD Decile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("CCG_Code"),    
  "primary_level_desc": F.col("CCG_Name"),
  "secondary_level": F.col("IMD_Decile"),
  "secondary_level_desc": F.col("IMD_Decile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

ccg_res_imd_quintile_pop = {
  "breakdown_name": "CCG of Residence; IMD Quintile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("CCG_Code"),    
  "primary_level_desc": F.col("CCG_Name"),
  "secondary_level": F.col("IMD_Quintile"),
  "secondary_level_desc": F.col("IMD_Quintile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

comm_region_pop = {
  "breakdown_name": "Commissioning Region", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("Region_Code"), 
  "primary_level_desc": F.col("Region_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}

comm_region_eth_higher_pop = {
  "breakdown_name": "Commissioning Region; Ethnicity (Higher Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("Region_Code"), 
  "primary_level_desc": F.col("Region_Name"), 
  "secondary_level": F.col("UpperEthnicity"), 
  "secondary_level_desc": F.col("UpperEthnicity"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
comm_region_eth_lower_pop = {
  "breakdown_name": "Commissioning Region; Ethnicity (Lower Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("Region_Code"), 
  "primary_level_desc": F.col("Region_Name"), 
  "secondary_level": F.col("LowerEthnicityCode"), 
  "secondary_level_desc": F.col("LowerEthnicityName"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
comm_region_der_gender_pop = {
  "breakdown_name": "Commissioning Region; Gender", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("Region_Code"), 
  "primary_level_desc": F.col("Region_Name"), 
  "secondary_level": F.col("GenderCode"), 
  "secondary_level_desc": F.col("Der_Gender"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
comm_region_imd_decile_pop = {
  "breakdown_name": "Commissioning Region; IMD Decile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("Region_Code"), 
  "primary_level_desc": F.col("Region_Name"), 
  "secondary_level": F.col("IMD_Decile"),
  "secondary_level_desc": F.col("IMD_Decile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

comm_region_imd_quintile_pop = {
  "breakdown_name": "Commissioning Region; IMD Quintile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("Region_Code"), 
  "primary_level_desc": F.col("Region_Name"), 
  "secondary_level": F.col("IMD_Quintile"),
  "secondary_level_desc": F.col("IMD_Quintile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

stp_res_pop = {
  "breakdown_name": "STP of Residence", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("STP_Code"), 
  "primary_level_desc": F.col("STP_Name"), 
  "secondary_level": F.lit("NONE"), 
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
stp_res_eth_higher_pop = {
  "breakdown_name": "STP of Residence; Ethnicity (Higher Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("STP_Code"), 
  "primary_level_desc": F.col("STP_Name"), 
  "secondary_level": F.col("UpperEthnicity"), 
  "secondary_level_desc": F.col("UpperEthnicity"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
stp_res_eth_lower_pop = {
  "breakdown_name": "STP of Residence; Ethnicity (Lower Level)", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("STP_Code"), 
  "primary_level_desc": F.col("STP_Name"), 
  "secondary_level": F.col("LowerEthnicityCode"), 
  "secondary_level_desc": F.col("LowerEthnicityName"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}
stp_res_der_gender_pop = {
  "breakdown_name": "STP of Residence; Gender", 
  "source_table": "census_2021_sub_icb_derived", 
  "primary_level": F.col("STP_Code"), 
  "primary_level_desc": F.col("STP_Name"), 
  "secondary_level": F.col("GenderCode"), 
  "secondary_level_desc": F.col("Der_Gender"), 
  "third_level": F.lit("NONE"), 
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"), 
  "fourth_level_desc": F.lit("NONE"), 
  "aggregation_field": (F.expr("SUM(Population)").cast("String").alias("METRIC_VALUE"))
}

stp_res_imd_decile_pop = {
  "breakdown_name": "STP of Residence; IMD Decile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("STP_Code"),    
  "primary_level_desc": F.col("STP_Name"),
  "secondary_level": F.col("IMD_Decile"),
  "secondary_level_desc": F.col("IMD_Decile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

stp_res_imd_quintile_pop = {
  "breakdown_name": "STP of Residence; IMD Quintile", 
  "source_table": "mhb_imd_pop", 
  "primary_level": F.col("STP_Code"),    
  "primary_level_desc": F.col("STP_Name"),
  "secondary_level": F.col("IMD_Quintile"),
  "secondary_level_desc": F.col("IMD_Quintile"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "aggregation_field": (F.expr("SUM(Population)").alias("METRIC_VALUE"))
}

# COMMAND ----------

# DBTITLE 1,Populations and SubPopulations incl parameters for 1f, 1fa, 1fb, 1fc, 1fd, 1fe
#Non age breakdowns

Populations = [
  # ccg practice or residence populations
  ccg_prac_res_pop, ccg_prac_res_eth_higher_pop, ccg_prac_res_eth_lower_pop, ccg_prac_res_der_gender_pop, 
  ccg_prac_res_imd_decile_pop, ccg_prac_res_imd_quintile_pop, 
  
  # ccg residence populations
  ccg_res_pop, ccg_res_eth_higher_pop, ccg_res_eth_lower_pop, ccg_res_der_gender_pop, 
  ccg_res_imd_decile_pop, ccg_res_imd_quintile_pop, 
  
  # stp residence populations
  stp_res_pop, stp_res_eth_higher_pop, stp_res_eth_lower_pop, stp_res_der_gender_pop, 
  stp_res_imd_decile_pop, stp_res_imd_quintile_pop,
  
  # commissiong region populations
  comm_region_pop, comm_region_eth_higher_pop, comm_region_eth_lower_pop, comm_region_der_gender_pop, 
  comm_region_imd_decile_pop, comm_region_imd_quintile_pop, 
  
  # national populations
  eng_pop, eth_higher_pop, eth_lower_pop, der_gender_pop,
  imd_quintile_pop, imd_decile_pop  
]

# add age dictionaries to this list
Populations += list_age_dicts
Populations += imd_age_dicts

# metric_id: metric_name, filter
SubPop = {
  "1f": {"name": "Census 2021 Population", "filter": (F.col("Age").isNotNull())},
  "1fa": {"name": "Census 2021 Children and Young People Population", "filter": (F.col("Age").between(0,17))},
  "1fb": {"name": "Census 2021 Adults aged 18 to 64 Population", "filter": (F.col("Age").between(18,64))},
  "1fc": {"name": "Census 2021 Adults aged 65 and over Population", "filter": (F.col("Age") >= 65)},
  "1fd": {"name": "Census 2021 Female aged 15 to 54 Population", "filter": ((F.col("Age").between(15,54)) & (F.col("GenderCode") == 2))},
#  "1fe": {"name": "Census 2021 Persons aged 14 to 65 Population", "filter": (F.col("Age").between(14,65))},
}

IMDPop = {
  "1f": {"name": "Census 2021 Population", "filter": (F.col("Age_Code").isNotNull())},
  "1fa": {"name": "Census 2021 Children and Young People Population", "filter": (F.col("Age_Code").between(0,7))},
  "1fb": {"name": "Census 2021 Adults aged 18 to 64 Population", "filter": (F.col("Age_Code").between(8,17))},
  "1fc": {"name": "Census 2021 Adults aged 65 and over Population", "filter": (F.col("Age_Code") >= 18)},
  "1fd": {"name": "Census 2021 Female aged 15 to 54 Population", "filter": ((F.col("Age_Code").between(6,15)) & (F.col("GenderCode") == 2))},
#  "1fe": {"name": "Census 2021 Persons aged 14 to 65 Population", "filter": (F.col("Age_Code").between(6,18))}
}

# COMMAND ----------

# DBTITLE 1,Non IMD populations
from pyspark.sql import DataFrame
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)
  
spark.conf.set("spark.databricks.delta.state.corruptionIsFatal", False)

source_tables = ["census_2021_sub_icb_derived", "census_2021_national_derived"]
for source_table in source_tables:
  list1 = [pop for pop in Populations if pop["source_table"] == source_table]
  print(timenow(), source_table, f"{len(list1) } aggregations")
  
  pop_table_df = spark.table(f"{db_output}.{source_table}")

  for pop_measure_id in SubPop:
      filt = SubPop[pop_measure_id]["filter"]
      pop_table_filt_df = pop_table_df.filter(filt)
        
      print(timenow(), "create agg_df", pop_measure_id, source_table,  f"{len(list1) } aggregations")
      agg_df = unionAll(*[create_agg_df(
        pop_table_filt_df, db_source, rp_startdate, rp_enddate, 
        pop["primary_level"], pop["primary_level_desc"], 
        pop["secondary_level"], pop["secondary_level_desc"],  
        pop["third_level"], pop["third_level_desc"], 
        pop["fourth_level"], pop["fourth_level_desc"],  
        pop["aggregation_field"], #aggregation_field, 
        pop["breakdown_name"], # breakdown_name, 
        status,  
        pop_measure_id, 
        SubPop[pop_measure_id]["name"], 
        output_columns)
        for pop in list1])
      
      agg_df = agg_df.withColumn("REPORTING_PERIOD_START", F.col("REPORTING_PERIOD_START").cast("string"))
      agg_df = agg_df.withColumn("REPORTING_PERIOD_END", F.col("REPORTING_PERIOD_END").cast("string"))
      agg_df = agg_df.withColumn("METRIC_VALUE", F.col("METRIC_VALUE").cast("decimal(10,2)"))
      agg_df = agg_df.distinct()
      
      # print(agg_df.schema)
      print(timenow(), "insert into unsuppressed output table", source_table, pop_measure_id)
      try:
        agg_df.write.saveAsTable(f"{db_output}.automated_output_unsuppressed1", mode = 'Append')
      except:
        agg_df.write.saveAsTable(f"{db_output}.automated_output_unsuppressed1", mode = 'Overwrite')
      # insert_unsup_agg(agg_df, db_output, output_columns, "automated_output_unsuppressed1") 

# COMMAND ----------

# DBTITLE 1,IMD Populations
source_table = "mhb_imd_pop"

list1 = [pop for pop in Populations if pop["source_table"] == source_table]

print(timenow(), source_table, f"{len(list1) } aggregations")

pop_table_df = spark.table(f"{db_output}.{source_table}")

for pop_measure_id in IMDPop:
    filt = IMDPop[pop_measure_id]["filter"]
    pop_table_filt_df = pop_table_df.filter(filt)

    print(timenow(), "create agg_df", pop_measure_id, source_table,  f"{len(list1) } aggregations")
    agg_df = unionAll(*[create_agg_df(
      pop_table_filt_df, db_source, rp_startdate, rp_enddate, 
      pop["primary_level"], pop["primary_level_desc"], 
      pop["secondary_level"], pop["secondary_level_desc"],  
      pop["third_level"], pop["third_level_desc"], 
      pop["fourth_level"], pop["fourth_level_desc"],  
      pop["aggregation_field"], #aggregation_field, 
      pop["breakdown_name"], # breakdown_name, 
      status,  
      pop_measure_id, 
      IMDPop[pop_measure_id]["name"], 
      output_columns)
      for pop in list1])

    agg_df = agg_df.withColumn("REPORTING_PERIOD_START", F.col("REPORTING_PERIOD_START").cast("string"))
    agg_df = agg_df.withColumn("REPORTING_PERIOD_END", F.col("REPORTING_PERIOD_END").cast("string"))
    agg_df = agg_df.withColumn("METRIC_VALUE", F.col("METRIC_VALUE").cast("decimal(10,2)"))
    agg_df = agg_df.distinct()
    
    # print(agg_df.schema)
    print(timenow(), "insert into unsuppressed output table", source_table, pop_measure_id)
    try:
      agg_df.write.saveAsTable(f"{db_output}.automated_output_unsuppressed1", mode = 'Append')
    except:
      agg_df.write.saveAsTable(f"{db_output}.automated_output_unsuppressed1", mode = 'Overwrite')
    # insert_unsup_agg(agg_df, db_output, output_columns, "automated_output_unsuppressed") 