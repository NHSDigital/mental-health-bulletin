# Databricks notebook source
 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Population AS
 ---getting the sum of population where Gender and Ethncity are known
 SELECT SUM(population) as population
 from   $db_output.pop_health ---from hard coded lower ethnicity table in setup/population
 WHERE  gender in ('1','2')
        AND ethnic_group_formatted in ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')--upper ethnicity only (avoids over-counting)

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ONS_Population AS 
 ---get population counts by higher ethnicity, gender and age group
 SELECT				  CASE	WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background') THEN 'White'
 							ELSE NULL
 							END AS ethnic_group_formatted ---Upper ethnicity grouping
 					,POP.Der_Gender AS GENDER
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14')
 								THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17'
 								THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34')
 								THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49')
 								THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64')
 								THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over')
 								THEN '65 or over'
 								ELSE 'Unknown'
 							END
 						AS AGE_GROUP
 					,SUM (POP.Population) AS Population
 FROM				$db_output.pop_health AS POP
 WHERE				POP.Der_Gender IN ('1', '2')
 group by 
  CASE WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background') THEN 'White'
 							ELSE NULL
 							END 
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14')
 								THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17'
 								THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34')
 								THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49')
 								THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64')
 								THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over')
 								THEN '65 or over'
 								ELSE 'Unknown'
 							END                            
 union all
 ---get population counts by lower ethnicity, gender and age group
 SELECT				 ethnic_group_formatted ---lower ethnicity grouping
 					,Der_Gender AS GENDER
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14')
 								THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17'
 								THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34')
 								THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49')
 								THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64')
 								THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over')
 								THEN '65 or over'
 								ELSE 'Unknown'
 							END
 						AS AGE_GROUP
 					,SUM (POP.Population) AS Population
 FROM				$db_output.pop_health AS POP
 WHERE				POP.Der_Gender IN ('1', '2') AND POP.ethnic_group_formatted NOT IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 group by 
  POP.ethnic_group_formatted
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14')
 								THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17'
 								THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34')
 								THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49')
 								THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64')
 								THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over')
 								THEN '65 or over'
 								ELSE 'Unknown'
 							END 

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MHSDS_Annual_PeopleInContact AS 
 ---get count of people in contact by upper ethnicity, gender and age band where age, upper ethnicity and gender are known
 SELECT				 ETH.Ethnicity AS Ethnicity
 					,MHS.Gender
 					,CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15
 								THEN '15 or under'
 							WHEN AgeRepPeriodEnd BETWEEN 16 AND 17
 								THEN '16 to 17'
 							WHEN AgeRepPeriodEnd BETWEEN 18 AND 34
 								THEN '18 to 34'
 							WHEN AgeRepPeriodEnd BETWEEN 35 AND 49
 								THEN '35 to 49'
 							WHEN AgeRepPeriodEnd BETWEEN 50 AND 64
 								THEN '50 to 64'
 							WHEN AgeRepPeriodEnd >= 65
 								THEN '65 or over'
 								ELSE 'Unknown'
 							END
 						AS AGE_GROUP
 					,COUNT(distinct person_id) AS PEOPLE ---people in contact where Age, Upper Ethnicity and Gender are known						
 FROM				$db_output.people_in_contact AS MHS
 LEFT JOIN           $db_output.RD_Ethnicity ETH on mhs.NHSDEthnicity = ETH.code
 WHERE				Ethnicity IS NOT NULL AND AgeRepPeriodEnd IS NOT NULL AND MHS.Gender IN ('1', '2')
 GROUP BY	CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15
 						THEN '15 or under'
 					WHEN AgeRepPeriodEnd BETWEEN 16 AND 17
 						THEN '16 to 17'
 					WHEN AgeRepPeriodEnd BETWEEN 18 AND 34
 						THEN '18 to 34'
 					WHEN AgeRepPeriodEnd BETWEEN 35 AND 49
 						THEN '35 to 49'
 					WHEN AgeRepPeriodEnd BETWEEN 50 AND 64
 						THEN '50 to 64'
 					WHEN AgeRepPeriodEnd >= 65
 						THEN '65 or over'
 						ELSE 'Unknown'
 					END
 			,ETH.Ethnicity
 			,MHS.Gender
 union all
 --new code added below to include both sub-ethnicity and lower level ethnicity
 SELECT				 ETH.Sub_Ethnicity AS Ethnicity
 					,MHS.Gender
 					,CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15
 								THEN '15 or under'
 							WHEN AgeRepPeriodEnd BETWEEN 16 AND 17
 								THEN '16 to 17'
 							WHEN AgeRepPeriodEnd BETWEEN 18 AND 34
 								THEN '18 to 34'
 							WHEN AgeRepPeriodEnd BETWEEN 35 AND 49
 								THEN '35 to 49'
 							WHEN AgeRepPeriodEnd BETWEEN 50 AND 64
 								THEN '50 to 64'
 							WHEN AgeRepPeriodEnd >= 65
 								THEN '65 or over'
 								ELSE 'Unknown'
 							END
 						AS AGE_GROUP
 					,COUNT(distinct person_id) AS PEOPLE
 FROM				$db_output.people_in_contact AS MHS
 LEFT JOIN           $db_output.RD_Ethnicity ETH on mhs.NHSDEthnicity = ETH.code
 WHERE				Ethnicity IS NOT NULL AND AgeRepPeriodEnd IS NOT NULL AND MHS.Gender IN ('1', '2') ---people in contact where Age, Lower Ethnicity and Gender are known
 GROUP BY	CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15
 						THEN '15 or under'
 					WHEN AgeRepPeriodEnd BETWEEN 16 AND 17
 						THEN '16 to 17'
 					WHEN AgeRepPeriodEnd BETWEEN 18 AND 34
 						THEN '18 to 34'
 					WHEN AgeRepPeriodEnd BETWEEN 35 AND 49
 						THEN '35 to 49'
 					WHEN AgeRepPeriodEnd BETWEEN 50 AND 64
 						THEN '50 to 64'
 					WHEN AgeRepPeriodEnd >= 65
 						THEN '65 or over'
 						ELSE 'Unknown'
 					END
 			,ETH.Sub_Ethnicity
 			,MHS.Gender  

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CrudeRatesBySubGroup AS
 ---get crude rate for higher ethnicity, gender and age group
 SELECT			POP.ethnic_group_formatted
 				,POP.Gender
 				,POP.AGE_GROUP
 				,COALESCE (PIC.PEOPLE, 0) AS PEOPLE
 				,POP.Population
 				,COALESCE (CAST (PIC.PEOPLE AS float), 0) / CAST (POP.Population AS float) AS CRUDE_RATE
 FROM			global_temp.ONS_Population AS POP
 				LEFT OUTER JOIN global_temp.MHSDS_Annual_PeopleInContact AS PIC
 				ON PIC.Ethnicity = POP.ethnic_group_formatted 
                 AND PIC.Gender = POP.Gender 
                 AND PIC.AGE_GROUP = POP.AGE_GROUP
 where           pic.age_group <> 'Unknown'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW PopPrcntSubGroup AS
 ---get proportion of the overall population in each gender and age group sub group
 SELECT			POP.Gender
 				,POP.AGE_GROUP
                 ,cast(SUM(POP.Population) as float) / cast((select sum(Population) from global_temp.Population) as float) AS POP_PRCNT
 FROM			global_temp.ONS_Population AS POP
 where           ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 GROUP BY		POP.Gender,POP.AGE_GROUP

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesBySubGroup AS
 ---get standardised rate for each ethnicity (higher and lower), gender and age group combination
 SELECT			CRS.AGE_GROUP
 				,CRS.Gender
 				,CRS.ethnic_group_formatted
 				,CRS.CRUDE_RATE
 				,PPS.POP_PRCNT
 				,cast(CRS.CRUDE_RATE*PPS.POP_PRCNT as float) AS STND_RATE
 FROM			Global_temp.CrudeRatesBySubGroup AS CRS
 LEFT OUTER JOIN Global_temp.PopPrcntSubGroup AS PPS
 			    ON CRS.Gender = PPS.Gender 
                 AND CRS.AGE_GROUP = PPS.AGE_GROUP

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesByEthGroup AS
 ---get standardised rate per 100,000 population by ethnicity (higher and lower)
 SELECT			SRS.ethnic_group_formatted
 				,SUM (SRS.STND_RATE) * 100000 AS STANDARDISED_RATE_PER_100000
 FROM			global_temp.StandRatesBySubGroup AS SRS
 GROUP BY		SRS.ethnic_group_formatted

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroupIntermediate AS
 ---get confidence interval rate for standardised rates per 100,000 population for each ethnicity (higher and lower), gender and age group
 SELECT			POP.ethnic_group_formatted
 				,PGA.Gender
 				,PGA.AGE_GROUP
 				,PGA.Population_AgeGroup_Gender
 				,CRS.CRUDE_RATE
 				,POP.Population
 				,(	(
 					CAST (COALESCE (PGA.Population_AgeGroup_Gender, 0) AS FLOAT) * CAST (COALESCE (PGA.Population_AgeGroup_Gender, 0) AS FLOAT)
 					)
 				 * CRS.CRUDE_RATE * (1 - CRS.CRUDE_RATE)
 				 ) / CAST (SUM (POP.Population) AS FLOAT) AS CI_RATE
 FROM			(
                     SELECT		POP.Gender
                                 ,POP.AGE_GROUP
                                 ,SUM (POP.Population) AS Population_AgeGroup_Gender
                     FROM		global_temp.ONS_Population AS POP
                     where       ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
                     GROUP BY	POP.Gender
                                 ,POP.AGE_GROUP
 				) AS PGA 
 LEFT OUTER JOIN global_temp.CrudeRatesBySubGroup AS CRS
 				ON CRS.Gender = PGA.Gender 
                 AND CRS.AGE_GROUP = PGA.AGE_GROUP
 LEFT OUTER JOIN global_temp.ONS_Population AS POP
 				ON CRS.ethnic_group_formatted = POP.ethnic_group_formatted 
                 AND CRS.Gender = POP.Gender 
                 AND CRS.AGE_GROUP = POP.AGE_GROUP
 GROUP BY		POP.ethnic_group_formatted
 				,PGA.Gender
 				,PGA.AGE_GROUP
 				,PGA.Population_AgeGroup_Gender
 				,CRS.CRUDE_RATE
 				,POP.Population

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroup AS
 ---get 95% confidence intervals for each ethnicity (higher and lower)
 SELECT			CIINT.ethnic_group_formatted
 				,SUM (CIINT.CI_RATE) AS CI_RATE
                 ,1.96*SQRT(cast(1.0/(cast((select sum(Population) from global_temp.Population) as FLOAT)*cast((select sum(Population) from global_temp.Population) as FLOAT)) as FLOAT)* SUM(CI_rate))*100000 as CONFIDENCE_INTERVAL_95
 FROM			global_temp.CIByEthGroupIntermediate AS CIINT
 GROUP BY		CIINT.ethnic_group_formatted

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW StandardisedRateEthnicOutput AS
 ---combine standardised rate and 95% confidence interval together for each ethnicity (higher and lower)
 SELECT			SRE.ethnic_group_formatted AS EthnicGroup
 				,SRE.STANDARDISED_RATE_PER_100000
 				,CIE.CONFIDENCE_INTERVAL_95
 FROM			global_temp.StandRatesByEthGroup AS SRE
 LEFT OUTER JOIN global_temp.CIByEthGroup AS CIE
 			    ON SRE.ethnic_group_formatted = CIE.ethnic_group_formatted

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.standardisation; 
 CREATE TABLE IF NOT EXISTS $db_output.standardisation USING DELTA AS
 ---final prep table for standardisation - figures aggregated later
 select        case when EthnicGroup IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups') 
                    then 'Sub_ethnicity' 
                    else 'Lower_Ethnicity' end as Ethnic_level ---separate ethnicity (higher and lower) into grouping
               ,EthnicGroup
               ,STANDARDISED_RATE_PER_100000
               ,CONFIDENCE_INTERVAL_95
 from          global_temp.StandardisedRateEthnicOutput
 where         EthnicGroup not in ('Arab','Gypsy') ---no mhsds data for these ethnicities so would be blank rows in final output therefore excluded
 
 OPTIMIZE $db_output.standardisation;