# Databricks notebook source
# dbutils.widgets.removeAll();

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

import os

dbutils.widgets.text("db_output", "mark_wagner1_100394", "db_output")
db_output  = dbutils.widgets.get("db_output")
assert db_output

dbutils.widgets.text("db_source", "mh_v5_pre_clear", "db_source")
db_source = dbutils.widgets.get("db_source")
assert db_source

dbutils.widgets.text("end_month_id", "1464", "end_month_id")
end_month_id = dbutils.widgets.get("end_month_id")
assert end_month_id

dbutils.widgets.text("start_month_id", "1453", "start_month_id")
start_month_id = dbutils.widgets.get("start_month_id")
assert start_month_id

dbutils.widgets.text("rp_enddate", "2022-03-31", "rp_enddate")
rp_enddate = dbutils.widgets.get("rp_enddate")
assert rp_enddate

dbutils.widgets.text("rp_startdate", "2021-04-01", "rp_startdate")
rp_startdate = dbutils.widgets.get("rp_startdate")
assert rp_startdate

dbutils.widgets.text("populationyear", "2020", "populationyear")
populationyear  = dbutils.widgets.get("populationyear")
assert rp_startdate

dbutils.widgets.text("status", "Final", "status")
status  = dbutils.widgets.get("status")
assert status

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Population AS
 SELECT         SUM (POP.Population) as Population
 FROM        $db_output.pop_health AS POP ---uses hard coded ethnicity figures from load_eth_2011_figures notebook
 WHERE        POP.Der_Gender IN ("1","2")
             AND POP.UpperEthnicity IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RI_Prep;
 CREATE TABLE $db_output.RI_Prep USING DELTA AS
 SELECT DISTINCT
 inc.Person_ID,
 inc.OrgIDProv,
 o.NAME as Provider_Name,
 get_provider_type_code(inc.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(inc.OrgIDProv) as ProvTypeName,
 mpi.AgeRepPeriodEnd,
 mpi.age_group_higher_level,
 mpi.age_group_lower_common,
 mpi.Der_Gender,
 mpi.Der_Gender_Desc,
 mpi.NHSDEthnicity,
 mpi.UpperEthnicity,
 mpi.LowerEthnicityCode,
 mpi.LowerEthnicityName,
 mpi.LADistrictAuthCode as LADistrictAuth,
 mpi.LADistrictAuthName,                    
 mpi.IMD_Decile,
 mpi.IMD_Quintile,
 COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
 COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
 COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
 COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
 COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
 COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME,
 coalesce(int.key, "UNKNOWN") as IntTypeCode,
 coalesce(int.description, "UNKNOWN") as IntTypeName,
 inc.UniqRestrictiveIntIncID,
 type.UniqRestrictiveIntTypeID,
 concat(inc.UniqRestrictiveIntIncID, type.UniqRestrictiveIntTypeID) as UniqRestraintID
 FROM			    $db_output.MHB_MHS505RestrictiveinterventInc as inc
 INNER JOIN          $db_output.MHB_MHS515RestrictiveInterventType as type on inc.UniqRestrictiveIntIncID = type.UniqRestrictiveIntIncID and inc.UniqMonthID = type.UniqMonthID
 LEFT JOIN           $db_output.MPI as mpi on inc.Person_ID = mpi.Person_ID
 LEFT JOIN           menh_publications.RestrictiveIntTypeDim_Extended as int on type.restrictiveinttype = int.key
 LEFT JOIN           $db_output.STP_Region_mapping stp on mpi.OrgIDCCGRes = stp.CCG_code ---OrgIDCCGRes/OrgIDSubICBLocResidence CASE statement already done in Create MH Bulletin Asset
 LEFT JOIN           $db_output.MHB_ORG_DAILY o on inc.OrgIDProv = o.ORG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.RI_Prep

# COMMAND ----------

# DBTITLE 1,CREATES MH TEMP TABLE WITH COUNT OF PEOPLE GROUPED BY ETHNICITY, GENDER AND AGE GROUP
 %sql

 --CREATES MH TEMP TABLE WITH COUNT OF PEOPLE GROUPED BY ETHNICITY, GENDER AND AGE GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW MHSDS_Annual_PeopleInContact_Restraint AS
 SELECT				 MPI.LowerEthnicityName as Ethnicity
 					,Der_Gender
 					,CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
 							WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
 							WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
 							WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
 							WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
 							WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
 							ELSE "UNKNOWN"
 							END AS AGE_GROUP
 					,COUNT(distinct INC.Person_ID) AS PEOPLE
 FROM			    $db_output.MHB_MHS505RestrictiveinterventInc as INC
 INNER JOIN          $db_output.MHB_MHS515RestrictiveInterventType as TYPE on INC.UniqRestrictiveIntIncID = TYPE.UniqRestrictiveIntIncID and INC.UniqMonthID = TYPE.UniqMonthID
 left join           $db_output.MPI as MPI on INC.Person_ID = MPI.Person_ID --count of people so joining on person_id
 left join           $db_output.NHSDEthnicityDim ETH on MPI.NHSDEthnicity = ETH.key
 WHERE				MPI.NHSDEthnicity IS NOT NULL
 				    AND AgeRepPeriodEnd IS NOT NULL
 				    AND MPI.Der_Gender IN ('1', '2')
 GROUP BY	        CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
                             WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
                             WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
                             WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
                             WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
                             WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
                             ELSE "UNKNOWN" END
                     ,MPI.LowerEthnicityName
                     ,Der_Gender  
                                                        
   --new code added below to incldue both Sub_Ethnicity and lower level ethnicity                          
 union all

 SELECT				 ETH.upper_description as Ethnicity
 					,Der_Gender   
 					,CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
 							WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
 							WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
 							WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
 							WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
 							WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
 							ELSE "UNKNOWN"
 							END AS AGE_GROUP
 					,COUNT(distinct INC.Person_ID) AS PEOPLE
 FROM			    $db_output.MHB_MHS505RestrictiveinterventInc as INC
 INNER JOIN          $db_output.MHB_MHS515RestrictiveInterventType as TYPE on INC.UniqRestrictiveIntIncID = TYPE.UniqRestrictiveIntIncID and INC.UniqMonthID = TYPE.UniqMonthID
 left join           $db_output.MPI as MPI on INC.Person_ID = MPI.Person_ID
 left join           $db_output.NHSDEthnicityDim ETH on MPI.NHSDEthnicity = ETH.key
 WHERE				ETH.upper_description IS NOT NULL
 				    AND AgeRepPeriodEnd IS NOT NULL
 				    AND MPI.Der_Gender IN ('1', '2')
 GROUP BY	        CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
                             WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
                             WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
                             WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
                             WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
                             WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
                             ELSE "UNKNOWN"
                             END
                     ,ETH.upper_description
                     ,Der_Gender

# COMMAND ----------

# DBTITLE 1,CREATES ONS TEMP TABLE WITH COUNT OF PEOPLE GROUPED BY ETHNICITY, GENDER AND AGE GROUP
 %sql

 --CREATES ONS TEMP TABLE WITH COUNT OF PEOPLE GROUPED BY ETHNICITY, GENDER AND AGE GROUP

  CREATE OR REPLACE GLOBAL TEMP VIEW ONS_Population_Restraint AS
 SELECT				  CASE	WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background', 'Roma') THEN 'White'
 							ELSE NULL
 							END AS ethnic_group_formatted
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
 							WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
 							WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
 							ELSE "UNKNOWN"
 							END AS AGE_GROUP
 					,SUM (POP.Population) AS Population
 FROM				$db_output.pop_health AS POP
 WHERE				POP.Der_Gender IN ("1", "2")
 group by            CASE	WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background', 'Roma') THEN 'White'
 							ELSE NULL
 							END
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
 							WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
 							WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
 							ELSE "UNKNOWN"
 							END
  
 union all

 SELECT				 POP.ethnic_group_formatted
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
 							WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
 							WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
 							ELSE "UNKNOWN"
 							END AS AGE_GROUP
 					,SUM (POP.Population) AS Population
 FROM				$db_output.pop_health AS POP
 WHERE				POP.Der_Gender IN ("1", "2")
 -- 				    AND POP.ethnic_group_formatted NOT IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 group by            POP.ethnic_group_formatted
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 5', '6 to 10', '11 to 15') THEN '15 or under'
 							WHEN POP.Age_group IN ('16', '17') THEN '16 to 17'
 							WHEN POP.Age_group IN ('18', '19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 to 89', '90 or over') THEN '65 or over'
 							ELSE "UNKNOWN"
 							END

# COMMAND ----------

# DBTITLE 1,CREATES CRUDE RATES FOR EACH ETHNIC / GENDER / AGE GROUP
 %sql

 --CREATES CRUDE RATES FOR EACH ETHNIC / GENDER / AGE GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW CrudeRatesBySubGroup_Restraint AS
 SELECT			POP.ethnic_group_formatted
 				,POP.Der_Gender
 				,POP.AGE_GROUP
 				,COALESCE (PIC.PEOPLE, 0) AS PEOPLE
 				,POP.Population
 				,COALESCE (CAST (PIC.PEOPLE AS float), 0) / CAST (POP.Population AS float) AS CRUDE_RATE ---crude rate = people/population
 FROM			global_temp.ONS_Population_Restraint AS POP
 				LEFT OUTER JOIN global_temp.MHSDS_Annual_PeopleInContact_Restraint AS PIC --gathering number of people information
 				ON PIC.Ethnicity = POP.ethnic_group_formatted 
                 AND PIC.Der_Gender = POP.Der_Gender 
                 AND PIC.AGE_GROUP = POP.AGE_GROUP
 where           pic.age_group <> "UNKNOWN"

# COMMAND ----------

# DBTITLE 1,CALCULATE THE PROPORTION OF THE OVERALL POPULATION IN EACH SUB GROUP
 %sql

 -- CALCULATE THE PROPORTION OF THE OVERALL POPULATION IN EACH SUB GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW PopPrcntSubGroup_Restraint AS
 SELECT			POP.Der_Gender
 				,POP.AGE_GROUP
 				--,CAST (SUM(POP.Population) AS FLOAT) / CAST (select Population from global_temp.Population AS FLOAT) AS POP_PRCNT
                 ,cast(SUM(POP.Population) as float) / cast((select sum(Population) from global_temp.Population) as float) AS POP_PRCNT  --- population proportion = sub_group_population/overall_population
 FROM			global_temp.ONS_Population_Restraint AS POP
 where           ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 GROUP BY		POP.Der_Gender
 				,POP.AGE_GROUP

# COMMAND ----------

# DBTITLE 1,CREATES STANDARDISED RATES FOR EACH ETHNIC / GENDER / AGE GROUP
 %sql

 --CREATES STANDARDISED RATES FOR EACH ETHNIC / GENDER / AGE GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesBySubGroup_Restraint AS

 SELECT			CRS.AGE_GROUP
 				,CRS.Der_Gender
 				,CRS.ethnic_group_formatted
 				,CRS.CRUDE_RATE
 				,PPS.POP_PRCNT
 				,cast(CRS.CRUDE_RATE*PPS.POP_PRCNT as float) AS STND_RATE  --standard_rate = crude_rate * population_proportion 
 FROM			Global_temp.CrudeRatesBySubGroup_Restraint AS CRS
 LEFT OUTER JOIN Global_temp.PopPrcntSubGroup_Restraint AS PPS
 			    ON CRS.Der_Gender = PPS.Der_Gender 
                 AND CRS.AGE_GROUP = PPS.AGE_GROUP

# COMMAND ----------

# DBTITLE 1,CREATES STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP
 %sql

 --CREATES STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP

 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesByEthGroup_Restraint AS
 SELECT			SRS.ethnic_group_formatted
 				,SUM (SRS.STND_RATE) * 100000 AS STANDARDISED_RATE_PER_100000 ---standard_rate per 100,000
 FROM			global_temp.StandRatesBySubGroup_Restraint AS SRS
 GROUP BY		SRS.ethnic_group_formatted


# COMMAND ----------

# DBTITLE 1,CREATES CONFIDENCE INTERVALS FOR STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP
 %sql

 --CREATES CONFIDENCE INTERVALS FOR STANDARDISED RATES PER 100,000 POPULATION FOR EACH ETHNIC GROUP
 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroupIntermediate_Restraint AS
 SELECT			POP.ethnic_group_formatted
 				,PGA.Der_Gender
 				,PGA.AGE_GROUP
 				,PGA.Population_AgeGroup_Gender
 				,CRS.CRUDE_RATE
 				,POP.Population
 				,(	(
 					CAST (COALESCE (PGA.Population_AgeGroup_Gender, 0) AS FLOAT) * CAST (COALESCE (PGA.Population_AgeGroup_Gender, 0) AS FLOAT) --squaring average
 					)
 				 * CRS.CRUDE_RATE * (1 - CRS.CRUDE_RATE)  ----STDEV = SE = STDEV /SQRT(n)
 				 ) / CAST (SUM (POP.Population) AS FLOAT) AS CI_RATE --Ask AH about methodology
 FROM			(---Population figures broken down by Age and Gender
                     SELECT		POP.Der_Gender 
                                 ,POP.AGE_GROUP
                                 ,SUM (POP.Population) AS Population_AgeGroup_Gender
                     FROM		global_temp.ONS_Population_Restraint AS POP
 --                     where       ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
                     GROUP BY	POP.Der_Gender
                                 ,POP.AGE_GROUP
 				) AS PGA 
 LEFT OUTER JOIN global_temp.CrudeRatesBySubGroup_Restraint AS CRS
 				ON CRS.Der_Gender = PGA.Der_Gender 
                 AND CRS.AGE_GROUP = PGA.AGE_GROUP
 LEFT OUTER JOIN global_temp.ONS_Population_Restraint AS POP
 				ON CRS.ethnic_group_formatted = POP.ethnic_group_formatted 
                 AND CRS.Der_Gender = POP.Der_Gender 
                 AND CRS.AGE_GROUP = POP.AGE_GROUP
 GROUP BY		POP.ethnic_group_formatted
 				,PGA.Der_Gender
 				,PGA.AGE_GROUP
 				,PGA.Population_AgeGroup_Gender
 				,CRS.CRUDE_RATE
 				,POP.Population

# COMMAND ----------

 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroup_Restraint AS
 SELECT			CIINT.ethnic_group_formatted
 				,SUM (CIINT.CI_RATE) AS CI_RATE
 				--,1.96 * SQRT (CAST (1 / ((select sum(Population) from global_temp.Population) * (select sum(Population) from global_temp.Population)) AS FLOAT) * SUM(CIINT.CI_RATE)) * 100000 AS CONFIDENCE_INTERVAL_95
                 ,1.96*SQRT(cast(1.0/(cast((select sum(Population) from global_temp.Population) as FLOAT)*cast((select sum(Population) from global_temp.Population) as FLOAT)) as FLOAT)* SUM(CI_rate))*100000 as CONFIDENCE_INTERVAL_95
 FROM			global_temp.CIByEthGroupIntermediate_Restraint AS CIINT
 GROUP BY		CIINT.ethnic_group_formatted

# COMMAND ----------

 %sql

 CREATE OR REPLACE GLOBAL TEMP VIEW StandardisedRateEthnicOutput_Restraint AS
 SELECT			SRE.ethnic_group_formatted AS EthnicGroup
 				,SRE.STANDARDISED_RATE_PER_100000
 				,CIE.CONFIDENCE_INTERVAL_95
 FROM			global_temp.StandRatesByEthGroup_Restraint AS SRE
 LEFT OUTER JOIN global_temp.CIByEthGroup_Restraint AS CIE
 			    ON SRE.ethnic_group_formatted = CIE.ethnic_group_formatted

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Standardisation_Restraint;
 CREATE TABLE $db_output.Standardisation_Restraint USING DELTA AS

 -- CREATE OR REPLACE GLOBAL TEMP VIEW standardisation_Restraint AS
 select        case when EthnicGroup IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups') 
                    then 'England; Ethnicity (Higher Level)' 
                    else 'England; Ethnicity (Lower Level)' end as Ethnic_level
               ,coalesce(e.LowerEthnicityCode, s.EthnicGroup) as EthnicGroupCode                   
               ,EthnicGroup as EthnicGroupName
               ,STANDARDISED_RATE_PER_100000
               ,CONFIDENCE_INTERVAL_95
 from          global_temp.StandardisedRateEthnicOutput_Restraint s
 left join (select distinct LowerEthnicityCode, LowerEthnicityName from $db_output.mpi) e on s.EthnicGroup = e.LowerEthnicityName
 where         EthnicGroup not in ('Arab','Gypsy', 'Roma')

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Standardisation_Restraint