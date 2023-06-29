# Databricks notebook source
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Population AS
 SELECT		 SUM (POP.Population) as Population
 FROM		$db_output.pop_health AS POP ---uses hard coded ethnicity figures from population notebook
 WHERE		POP.Gender IN ('MALE','FEMALE')
             AND POP.ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MHSDS_Annual_PeopleInContact_Restraint AS
 ---creates temporary mpi table with count of people grouped by ethnicity, gender and age group where ethnicity and age group
 SELECT				 ETH.Ethnicity
 					,Der_Gender as Gender
 					,CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
 							WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
 							WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
 							WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
 							WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
 							WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
 							ELSE 'Unknown'
 							END AS AGE_GROUP
 					,COUNT(distinct RES.Person_ID) AS PEOPLE
 FROM			    $db_output.MHB_MHS505RestrictiveIntervention as RES
 left join           $db_output.MPI as mhs on res.Person_ID = mhs.Person_ID --count of people so joining on person_id
 left join           $db_output.RD_Ethnicity ETH on mhs.NHSDEthnicity = ETH.code
 WHERE				Ethnicity IS NOT NULL
 				    AND AgeRepPeriodEnd IS NOT NULL
 				    AND MHS.Der_Gender IN ('1', '2')
                     ---as per standardisation methodology, count at ethnicity needs to be where all other demographics are known or have values that can be standardised (such as Male and Female only)
                     and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 GROUP BY	        CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
                             WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
                             WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
                             WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
                             WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
                             WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
                             ELSE 'Unknown' END
                     ,ETH.Ethnicity
                     ,Der_Gender  
                                                        
 ---new code added below to incldue both higher level ethnicity and lower level ethnicity                          
 union all
 
 SELECT				 ETH.Sub_Ethnicity
 					,Der_Gender as Gender   
 					,CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
 							WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
 							WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
 							WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
 							WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
 							WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
 							ELSE 'Unknown'
 							END AS AGE_GROUP
 					,COUNT(distinct RES.Person_ID) AS PEOPLE
 FROM			    $db_output.MHB_MHS505RestrictiveIntervention as RES
 left join           $db_output.MPI as mhs on res.Person_ID = mhs.Person_ID
 left join           $db_output.RD_Ethnicity ETH on mhs.NHSDEthnicity = ETH.code
 WHERE				Ethnicity IS NOT NULL
 				    AND AgeRepPeriodEnd IS NOT NULL
 				    AND MHS.Der_Gender IN ('1', '2')
                     ---as per standardisation methodology, count at ethnicity needs to be where all other demographics are known or have values that can be standardised (such as Male and Female only)
                     and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 GROUP BY	        CASE	WHEN AgeRepPeriodEnd BETWEEN 0 AND 15 THEN '15 or under'
                             WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
                             WHEN AgeRepPeriodEnd BETWEEN 18 AND 34 THEN '18 to 34'
                             WHEN AgeRepPeriodEnd BETWEEN 35 AND 49 THEN '35 to 49'
                             WHEN AgeRepPeriodEnd BETWEEN 50 AND 64 THEN '50 to 64'
                             WHEN AgeRepPeriodEnd >= 65 THEN '65 or over'
                             ELSE 'Unknown'
                             END
                     ,ETH.Sub_Ethnicity
                     ,Der_Gender  

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ONS_Population_Restraint AS
 ---get population count grouped by ethnicity, gender and age group where ethnicity and age group
 SELECT				  CASE	WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background') THEN 'White'
 							ELSE NULL
 							END AS ethnic_group_formatted
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14') THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17' THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over') THEN '65 or over'
 							ELSE 'Unknown'
 							END AS AGE_GROUP
 					,SUM (POP.Population) AS Population
 FROM				$db_output.pop_health AS POP
 WHERE				POP.Der_Gender IN ('1', '2')
 group by            CASE	WHEN POP.ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN POP.ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN POP.ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN POP.ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN POP.ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background') THEN 'White'
 							ELSE NULL
 							END
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14') THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17' THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over') THEN '65 or over'
 							ELSE 'Unknown'
 							END
 ---new code added below to incldue both higher level ethnicity and lower level ethnicity   
 union all
 SELECT				 POP.ethnic_group_formatted
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14') THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17' THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over') THEN '65 or over'
 							ELSE 'Unknown'
 							END AS AGE_GROUP
 					,SUM (POP.Population) AS Population
 FROM				$db_output.pop_health AS POP
 WHERE				POP.Gender IN ('1', '2')
 				    AND POP.ethnic_group_formatted NOT IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 group by            POP.ethnic_group_formatted
 					,POP.Der_Gender
 					,CASE	WHEN POP.Age_group IN ('0 to 4', '5 to 7', '8 to 9', '10 to 14') THEN '15 or under'
 							WHEN POP.Age_group = '16 to 17' THEN '16 to 17'
 							WHEN POP.Age_group IN ('18 to 19', '20 to 24', '25 to 29', '30 to 34') THEN '18 to 34'
 							WHEN POP.Age_group IN ('35 to 39', '40 to 44', '45 to 49') THEN '35 to 49'
 							WHEN POP.Age_group IN ('50 to 54', '55 to 59', '60 to 64') THEN '50 to 64'
 							WHEN POP.Age_group IN ('65 to 69', '70 to 74', '75 to 79', '80 to 84', '85 and over') THEN '65 or over'
 							ELSE 'Unknown'
 							END

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CrudeRatesBySubGroup_Restraint AS
 ---get crude rate for higher ethnicity, gender and age group
 SELECT			POP.ethnic_group_formatted
 				,POP.Gender
 				,POP.AGE_GROUP
 				,COALESCE (PIC.PEOPLE, 0) AS PEOPLE
 				,POP.Population
 				,COALESCE (CAST (PIC.PEOPLE AS float), 0) / CAST (POP.Population AS float) AS CRUDE_RATE ---crude rate = people/population
 FROM			global_temp.ONS_Population_Restraint AS POP
 				LEFT OUTER JOIN global_temp.MHSDS_Annual_PeopleInContact_Restraint AS PIC --gathering number of people information
 				ON PIC.Ethnicity = POP.ethnic_group_formatted 
                 AND PIC.Gender = POP.Gender 
                 AND PIC.AGE_GROUP = POP.AGE_GROUP
 where           pic.age_group <> 'Unknown'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW PopPrcntSubGroup_Restraint AS
 ---get proportion of the overall population in each gender and age group sub group
 SELECT			POP.Gender
 				,POP.AGE_GROUP
                 ,cast(SUM(POP.Population) as float) / cast((select sum(Population) from global_temp.Population) as float) AS POP_PRCNT  --- population proportion = sub_group_population/overall_population
 FROM			global_temp.ONS_Population_Restraint AS POP
 where           ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
 GROUP BY		POP.Gender
 				,POP.AGE_GROUP

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesBySubGroup_Restraint AS
 ---get standardised rate for each ethnicity (higher and lower), gender and age group combination
 SELECT			CRS.AGE_GROUP
 				,CRS.Gender
 				,CRS.ethnic_group_formatted
 				,CRS.CRUDE_RATE
 				,PPS.POP_PRCNT
 				,cast(CRS.CRUDE_RATE*PPS.POP_PRCNT as float) AS STND_RATE  --standard_rate = crude_rate * population_proportion 
 FROM			Global_temp.CrudeRatesBySubGroup_Restraint AS CRS
 LEFT OUTER JOIN Global_temp.PopPrcntSubGroup_Restraint AS PPS
 			    ON CRS.Gender = PPS.Gender 
                 AND CRS.AGE_GROUP = PPS.AGE_GROUP

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW StandRatesByEthGroup_Restraint AS
 ---get standardised rate per 100,000 population by ethnicity (higher and lower)
 SELECT			SRS.ethnic_group_formatted
 				,SUM (SRS.STND_RATE) * 100000 AS STANDARDISED_RATE_PER_100000 ---standard_rate per 100,000
 FROM			global_temp.StandRatesBySubGroup_Restraint AS SRS
 GROUP BY		SRS.ethnic_group_formatted

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroupIntermediate_Restraint AS
 ---get confidence interval rate for standardised rates per 100,000 population for each ethnicity (higher and lower), gender and age group
 SELECT			POP.ethnic_group_formatted
 				,PGA.Gender
 				,PGA.AGE_GROUP
 				,PGA.Population_AgeGroup_Gender
 				,CRS.CRUDE_RATE
 				,POP.Population
 				,(	(
 					CAST (COALESCE (PGA.Population_AgeGroup_Gender, 0) AS FLOAT) * CAST (COALESCE (PGA.Population_AgeGroup_Gender, 0) AS FLOAT) ---squaring average
 					)
 				 * CRS.CRUDE_RATE * (1 - CRS.CRUDE_RATE)  ---STDEV = SE = STDEV /SQRT(n)
 				 ) / CAST (SUM (POP.Population) AS FLOAT) AS CI_RATE
 FROM			(---Population figures broken down by Age and Gender
                     SELECT		POP.Gender 
                                 ,POP.AGE_GROUP
                                 ,SUM (POP.Population) AS Population_AgeGroup_Gender
                     FROM		global_temp.ONS_Population_Restraint AS POP
                     where       ethnic_group_formatted IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups')
                     GROUP BY	POP.Gender
                                 ,POP.AGE_GROUP
 				) AS PGA 
 LEFT OUTER JOIN global_temp.CrudeRatesBySubGroup_Restraint AS CRS
 				ON CRS.Gender = PGA.Gender 
                 AND CRS.AGE_GROUP = PGA.AGE_GROUP
 LEFT OUTER JOIN global_temp.ONS_Population_Restraint AS POP
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
 CREATE OR REPLACE GLOBAL TEMP VIEW CIByEthGroup_Restraint AS
 ---get 95% confidence intervals for each ethnicity (higher and lower)
 SELECT			CIINT.ethnic_group_formatted
 				,SUM (CIINT.CI_RATE) AS CI_RATE
                 ,1.96*SQRT(cast(1.0/(cast((select sum(Population) from global_temp.Population) as FLOAT)*cast((select sum(Population) from global_temp.Population) as FLOAT)) as FLOAT)* SUM(CI_rate))*100000 as CONFIDENCE_INTERVAL_95
 FROM			global_temp.CIByEthGroupIntermediate_Restraint AS CIINT
 GROUP BY		CIINT.ethnic_group_formatted

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW StandardisedRateEthnicOutput_Restraint AS
 ---combine standardised rate and 95% confidence interval together for each ethnicity (higher and lower)
 SELECT			SRE.ethnic_group_formatted AS EthnicGroup
 				,SRE.STANDARDISED_RATE_PER_100000
 				,CIE.CONFIDENCE_INTERVAL_95
 FROM			global_temp.StandRatesByEthGroup_Restraint AS SRE
 LEFT OUTER JOIN global_temp.CIByEthGroup_Restraint AS CIE
 			    ON SRE.ethnic_group_formatted = CIE.ethnic_group_formatted

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW standardisation_Restraint AS
 ---final prep table for standardisation - figures aggregated later
 select        case when EthnicGroup IN ('White', 'Mixed', 'Asian or Asian British', 'Black or Black British', 'Other Ethnic Groups') 
                    then 'Sub_ethnicity' 
                    else 'Lower_Ethnicity' end as Ethnic_level
               ,EthnicGroup
               ,STANDARDISED_RATE_PER_100000
               ,CONFIDENCE_INTERVAL_95
 from          global_temp.StandardisedRateEthnicOutput_Restraint 
 where         EthnicGroup not in ('Arab','Gypsy')

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 1m_england AS
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'England' as Breakdown
 ,'England' as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1m' as Metric
 ,SUM(population) as Metric_value
 from $db_output.pop_health
 where ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 1m_ethnicity AS
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Lower Level)' as Breakdown
 ,case when ethnic_group_formatted = 'British' then 'A'
       when ethnic_group_formatted = 'Irish' then 'B'
       when ethnic_group_formatted = 'Any Other White Background' then 'C'
       when ethnic_group_formatted = 'White and Black Caribbean' then 'D'
       when ethnic_group_formatted = 'White and Black African' then 'E'
       when ethnic_group_formatted = 'White and Asian' then 'F'
       when ethnic_group_formatted = 'Any Other Mixed Background' then 'G'
       when ethnic_group_formatted = 'Indian' then 'H'
       when ethnic_group_formatted = 'Pakistani' then 'J'
       when ethnic_group_formatted = 'Bangladeshi' then 'K'
       when ethnic_group_formatted = 'Any Other Asian Background' then 'L'
       when ethnic_group_formatted = 'Caribbean' then 'M'
       when ethnic_group_formatted = 'African' then 'N'
       when ethnic_group_formatted = 'Any Other Black Background' then 'P'
       when ethnic_group_formatted = 'Chinese' then 'R'
       when ethnic_group_formatted = 'Any Other Ethnic Group' then 'S' else '99' end as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1m' as Metric
 ,SUM(population) as Metric_value
 from $db_output.pop_health
 where ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')
 group by case when ethnic_group_formatted = 'British' then 'A'
       when ethnic_group_formatted = 'Irish' then 'B'
       when ethnic_group_formatted = 'Any Other White Background' then 'C'
       when ethnic_group_formatted = 'White and Black Caribbean' then 'D'
       when ethnic_group_formatted = 'White and Black African' then 'E'
       when ethnic_group_formatted = 'White and Asian' then 'F'
       when ethnic_group_formatted = 'Any Other Mixed Background' then 'G'
       when ethnic_group_formatted = 'Indian' then 'H'
       when ethnic_group_formatted = 'Pakistani' then 'J'
       when ethnic_group_formatted = 'Bangladeshi' then 'K'
       when ethnic_group_formatted = 'Any Other Asian Background' then 'L'
       when ethnic_group_formatted = 'Caribbean' then 'M'
       when ethnic_group_formatted = 'African' then 'N'
       when ethnic_group_formatted = 'Any Other Black Background' then 'P'
       when ethnic_group_formatted = 'Chinese' then 'R'
       when ethnic_group_formatted = 'Any Other Ethnic Group' then 'S' else '99' end

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 1m_sub_ethnicity AS
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level)' as Breakdown
 ,CASE	WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background') THEN 'White'
 								ELSE NULL END AS Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1m' as Metric
 ,SUM(population) as Metric_value
 from $db_output.pop_health
 group by CASE WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
 							WHEN ethnic_group_formatted IN ('African', 'Caribbean', 'Any Other Black Background') THEN 'Black or Black British'
 							WHEN ethnic_group_formatted IN ('Any Other Mixed Background', 'White and Asian', 'White and Black African', 'White and Black Caribbean') THEN 'Mixed'
 							WHEN ethnic_group_formatted IN ('Chinese', 'Any Other Ethnic Group', 'Arab') THEN 'Other Ethnic Groups'
 							WHEN ethnic_group_formatted IN ('British', 'Gypsy', 'Irish', 'Any Other White Background') THEN 'White'
 								ELSE NULL END