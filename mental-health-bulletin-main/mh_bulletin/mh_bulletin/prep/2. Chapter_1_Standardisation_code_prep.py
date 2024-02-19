# Databricks notebook source
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

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
# end_month_id = dbutils.widgets.get("end_month_id")
# start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status  = dbutils.widgets.get("status")
# populationyear = dbutils.widgets.get("populationyear")
# IMD_year = dbutils.widgets.get("IMD_year")

# COMMAND ----------

# DBTITLE 1,Gets the total population sum
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Population AS
 SELECT  SUM (POP.Population) as Population
 FROM $db_output.pop_health AS POP ---uses hard coded ethnicity figures from load_eth_2011_figures notebook
   WHERE POP.Der_Gender IN ("1","2")
   AND POP.UpperEthnicity IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 ; 
 SELECT * FROM global_temp.Population

# COMMAND ----------

# DBTITLE 1,ONS populations tabulation
 %sql 
 --CREATES ONS TEMP TABLE WITH COUNT OF PEOPLE GROUPED BY ETHNICITY, GENDER AND AGE GROUP
  
 CREATE OR REPLACE GLOBAL TEMP VIEW ONS_Population AS
 SELECT CASE 
   WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
   WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
   WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
   WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
   WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background', 'Roma') THEN 'White'
   ELSE NULL
 END AS ethnic_group_formatted,
 POP.Der_Gender,
 CASE WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
   WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
   WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
   WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
   WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
   WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
   ELSE 'Unknown'
 END AS AGE_GROUP,
 SUM (POP.Population) AS Population
 FROM $db_output.pop_health AS POP
 WHERE POP.Der_Gender IN ("1", "2")
 group by CASE WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background', 'Roma') THEN 'White'
 ELSE NULL
 END
 ,POP.Der_Gender
 ,CASE  WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
   WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
   WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
   WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
   WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
   WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
   ELSE 'Unknown'
 END
  
 union all
  
 SELECT POP.ethnic_group_formatted
 ,POP.Der_Gender
 ,CASE WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
   WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
   WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
   WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
   WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
   WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
   ELSE 'Unknown'
 END AS AGE_GROUP
 ,SUM (POP.Population) AS Population
 FROM  $db_output.pop_health AS POP
 WHERE POP.Der_Gender IN ("1", "2")
 -- AND POP.ethnic_group_formatted NOT IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 group by POP.ethnic_group_formatted
 ,POP.Der_Gender
 ,CASE WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
   WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
   WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
   WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
   WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
   WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
   ELSE 'Unknown'
 END;
 
 SELECT * FROM global_temp.ONS_Population

# COMMAND ----------

 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MHSDS_Annual_PeopleInContact AS 
 
 SELECT MHS.LowerEthnicityName AS Ethnicity
 ,MHS.Der_Gender
 ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
   WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
   WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
   WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
   WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
   WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
   ELSE 'Unknown'
 END AS AGE_GROUP
 ,COUNT(distinct person_id)  ---people in contact where Age, Upper Ethnicity and Der_Gender are known
 AS PEOPLE
 FROM $db_output.people_in_contact AS MHS
 LEFT JOIN $db_output.NHSDEthnicityDim ETH on mhs.NHSDEthnicity = ETH.key
 WHERE ETH.id IS NOT NULL
   AND AgeRepPeriodEnd IS NOT NULL
   AND MHS.Der_Gender IN ('1', '2')
 
 GROUP BY CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
   WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
   WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
   WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
   WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
   WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
   ELSE 'Unknown'
 END
 ,MHS.LowerEthnicityName
 ,MHS.Der_Gender
             
 --new code added below to incldue both sub-ethnicity and lower level ethnicity
 
 union all
 
 SELECT ETH.Description AS Ethnicity,
 MHS.Der_Gender
 ,CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
   WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
   WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
   WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
   WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
   WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
   ELSE 'Unknown'
 END AS AGE_GROUP
 ,COUNT(distinct person_id) AS PEOPLE
 FROM $db_output.people_in_contact AS MHS
 LEFT JOIN $db_output.NHSDEthnicityDim ETH on mhs.NHSDEthnicity = ETH.key
 WHERE ETH.Description IS NOT NULL
   AND AgeRepPeriodEnd IS NOT NULL
   AND MHS.Der_Gender IN ('1', '2')
 
 GROUP BY CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
   WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
   WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
   WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
   WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
   WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
   ELSE 'Unknown'
 END
 ,ETH.Description
 ,MHS.Der_Gender;
 SELECT * FROM global_temp.MHSDS_Annual_PeopleInContact

# COMMAND ----------

# DBTITLE 1,CREATES CRUDE RATES FOR EACH ETHNIC / GENDER / AGE GROUP
 %sql
 
 --CREATES CRUDE RATES FOR EACH ETHNIC / Der_Gender / AGE GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW CrudeRatesBySubGroup AS
 SELECT POP.ethnic_group_formatted
 ,POP.Der_Gender
 ,POP.AGE_GROUP
 ,COALESCE (PIC.PEOPLE, 0) AS PEOPLE
 ,POP.Population
 ,COALESCE (CAST (PIC.PEOPLE AS float), 0) / CAST (POP.Population AS float) AS CRUDE_RATE
 FROM global_temp.ONS_Population AS POP
 LEFT OUTER JOIN global_temp.MHSDS_Annual_PeopleInContact AS PIC
   ON PIC.Ethnicity = POP.ethnic_group_formatted 
   AND PIC.Der_Gender = POP.Der_Gender 
   AND PIC.AGE_GROUP = POP.AGE_GROUP
 where pic.age_group <> 'Unknown';
 
 SELECT * FROM global_temp.CrudeRatesBySubGroup

# COMMAND ----------

# DBTITLE 1,CALCULATE THE PROPORTION OF THE OVERALL POPULATION IN EACH SUB GROUP
 %sql
 
 -- CALCULATE THE PROPORTION OF THE OVERALL POPULATION IN EACH SUB GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW PopPrcntSubGroup AS
 SELECT POP.Der_Gender
 ,POP.AGE_GROUP
 --,CAST (SUM(POP.Population) AS FLOAT) / CAST (select Population from global_temp.Population AS FLOAT) AS POP_PRCNT
 ,cast(SUM(POP.Population) as float) / cast((select sum(Population) from global_temp.Population) as float) AS POP_PRCNT
 FROM global_temp.ONS_Population AS POP
 where           ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 GROUP BY POP.Der_Gender
 ,POP.AGE_GROUP;
 select * from global_temp.PopPrcntSubGroup

# COMMAND ----------

# DBTITLE 1,CREATES STANDARDISED RATES FOR EACH ETHNIC / GENDER / AGE GROUP
 %sql
 
 --CREATES STANDARDISED RATES FOR EACH ETHNIC / Der_Gender / AGE GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesBySubGroup AS
 
 SELECT CRS.AGE_GROUP
 ,CRS.Der_Gender
 ,CRS.ethnic_group_formatted
 ,CRS.CRUDE_RATE
 ,PPS.POP_PRCNT
 ,cast(CRS.CRUDE_RATE*PPS.POP_PRCNT as float) AS STND_RATE
 FROM Global_temp.CrudeRatesBySubGroup AS CRS
 LEFT OUTER JOIN Global_temp.PopPrcntSubGroup AS PPS
   ON CRS.Der_Gender = PPS.Der_Gender 
   AND CRS.AGE_GROUP = PPS.AGE_GROUP;
 select * from global_temp.StandRatesBySubGroup

# COMMAND ----------

# DBTITLE 1,CREATES STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP
 %sql
 
 --CREATES STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP
 
 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesByEthGroup AS
 SELECT			SRS.ethnic_group_formatted
 				,SUM (SRS.STND_RATE) * 100000 AS STANDARDISED_RATE_PER_100000
 FROM			global_temp.StandRatesBySubGroup AS SRS
 GROUP BY		SRS.ethnic_group_formatted;
 select * from global_temp.StandRatesByEthGroup

# COMMAND ----------

# DBTITLE 1,CREATES CONFIDENCE INTERVALS FOR STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP
 %sql
 
 --CREATES CONFIDENCE INTERVALS FOR STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroupIntermediate AS
 SELECT POP.ethnic_group_formatted
 ,PGA.Der_Gender
 ,PGA.AGE_GROUP
 ,PGA.Population_AgeGroup_Der_Gender
 ,CRS.CRUDE_RATE
 ,POP.Population
 ,(	(
 CAST (COALESCE (PGA.Population_AgeGroup_Der_Gender, 0) AS FLOAT) * CAST (COALESCE (PGA.Population_AgeGroup_Der_Gender, 0) AS FLOAT)
 )
 * CRS.CRUDE_RATE * (1 - CRS.CRUDE_RATE)
 ) / CAST (SUM (POP.Population) AS FLOAT) AS CI_RATE
 FROM (
   SELECT POP.Der_Gender ,POP.AGE_GROUP ,SUM (POP.Population) AS Population_AgeGroup_Der_Gender
   FROM global_temp.ONS_Population AS POP
   where ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
   GROUP BY POP.Der_Gender, POP.AGE_GROUP
   ) AS PGA 
 LEFT OUTER JOIN global_temp.CrudeRatesBySubGroup AS CRS
   ON CRS.Der_Gender = PGA.Der_Gender 
   AND CRS.AGE_GROUP = PGA.AGE_GROUP
 LEFT OUTER JOIN global_temp.ONS_Population AS POP
   ON CRS.ethnic_group_formatted = POP.ethnic_group_formatted 
   AND CRS.Der_Gender = POP.Der_Gender 
   AND CRS.AGE_GROUP = POP.AGE_GROUP
 GROUP BY POP.ethnic_group_formatted
   ,PGA.Der_Gender
   ,PGA.AGE_GROUP
   ,PGA.Population_AgeGroup_Der_Gender
   ,CRS.CRUDE_RATE
   ,POP.Population;
   
 select * from global_temp.CIByEthGroupIntermediate

# COMMAND ----------

# DBTITLE 1,Pulls together confidence intervals for each Ethncity
 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroup AS
 SELECT CIINT.ethnic_group_formatted
   ,SUM (CIINT.CI_RATE) AS CI_RATE
   ,1.96*SQRT(cast(1.0/(cast((select sum(Population) from global_temp.Population) as FLOAT)*cast((select sum(Population) from global_temp.Population) as FLOAT)) as FLOAT)* SUM(CI_rate))*100000 as CONFIDENCE_INTERVAL_95
 FROM global_temp.CIByEthGroupIntermediate AS CIINT
 GROUP BY CIINT.ethnic_group_formatted;
 select * from global_temp.CIByEthGroup

# COMMAND ----------

 %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW StandardisedRateEthnicOutput AS
 SELECT SRE.ethnic_group_formatted AS EthnicGroup
 ,SRE.STANDARDISED_RATE_PER_100000
 ,CIE.CONFIDENCE_INTERVAL_95
 FROM global_temp.StandRatesByEthGroup AS SRE
 LEFT OUTER JOIN global_temp.CIByEthGroup AS CIE
   ON SRE.ethnic_group_formatted = CIE.ethnic_group_formatted;
 select * from global_temp.StandardisedRateEthnicOutput

# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.standardisation; 
 CREATE TABLE $db_output.standardisation USING DELTA AS
 select case when EthnicGroup IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups') 
   then 'England; Ethnicity (Higher Level)' 
   else 'England; Ethnicity (Lower Level)' 
 end as Ethnic_level
 ,coalesce(e.LowerEthnicityCode, s.EthnicGroup) as EthnicGroupCode                   
 ,EthnicGroup as EthnicGroupName
 ,STANDARDISED_RATE_PER_100000
 ,CONFIDENCE_INTERVAL_95
 from global_temp.StandardisedRateEthnicOutput s
 left join (select distinct LowerEthnicityCode, LowerEthnicityName from $db_output.mpi) e on s.EthnicGroup = e.LowerEthnicityName
 where EthnicGroup not in ('Arab','Gypsy', 'Roma');
 select * from $db_output.standardisation

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.standardisation