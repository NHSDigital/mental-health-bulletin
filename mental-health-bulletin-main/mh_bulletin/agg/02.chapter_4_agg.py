# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---national occupied bed days count
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
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group (lower level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 , Gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap45 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by gender, age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider type and age group (lower level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level); Provider Type' as Breakdown
 ,case when AgeRepPeriodEnd < 14 then 'Under 14'
       when AgeRepPeriodEnd between 14 and 15 then '14 to 15'
       when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 	  when AgeRepPeriodEnd between 18 and 19 then '18 to 19'
 	  when AgeRepPeriodEnd between 20 and 24 then '20 to 24'
       when AgeRepPeriodEnd between 25 and 29 then '25 to 29'
 	  when AgeRepPeriodEnd between 30 and 34 then '30 to 34'
       when AgeRepPeriodEnd between 35 and 39 then '35 to 39'
 	  when AgeRepPeriodEnd between 40 and 44 then '40 to 44'
       when AgeRepPeriodEnd between 45 and 49 then '45 to 49'
 	  when AgeRepPeriodEnd between 50 and 54 then '50 to 54'
       when AgeRepPeriodEnd between 55 and 59 then '55 to 59'
 	  when AgeRepPeriodEnd between 60 and 64 then '60 to 64'
       when AgeRepPeriodEnd between 65 and 69 then '65 to 69'
 	  when AgeRepPeriodEnd between 70 and 74 then '70 to 74'
       when AgeRepPeriodEnd between 75 and 79 then '75 to 79'
 	  when AgeRepPeriodEnd between 80 and 84 then '80 to 84'
       when AgeRepPeriodEnd between 85 and 89 then '85 to 89'
 	  when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as Level_1
 ,'NULL' as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_provider_type
 group by 
 case when AgeRepPeriodEnd < 14 then 'Under 14'
       when AgeRepPeriodEnd between 14 and 15 then '14 to 15'
       when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 	  when AgeRepPeriodEnd between 18 and 19 then '18 to 19'
 	  when AgeRepPeriodEnd between 20 and 24 then '20 to 24'
       when AgeRepPeriodEnd between 25 and 29 then '25 to 29'
 	  when AgeRepPeriodEnd between 30 and 34 then '30 to 34'
       when AgeRepPeriodEnd between 35 and 39 then '35 to 39'
 	  when AgeRepPeriodEnd between 40 and 44 then '40 to 44'
       when AgeRepPeriodEnd between 45 and 49 then '45 to 49'
 	  when AgeRepPeriodEnd between 50 and 54 then '50 to 54'
       when AgeRepPeriodEnd between 55 and 59 then '55 to 59'
 	  when AgeRepPeriodEnd between 60 and 64 then '60 to 64'
       when AgeRepPeriodEnd between 65 and 69 then '65 to 69'
 	  when AgeRepPeriodEnd between 70 and 74 then '70 to 74'
       when AgeRepPeriodEnd between 75 and 79 then '75 to 79'
 	  when AgeRepPeriodEnd between 80 and 84 then '80 to 84'
       when AgeRepPeriodEnd between 85 and 89 then '85 to 89'
 	  when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender, age group (lower level) and provider type occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level); Provider Type' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,case when AgeRepPeriodEnd < 14 then 'Under 14'
       when AgeRepPeriodEnd between 14 and 15 then '14 to 15'
       when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 	  when AgeRepPeriodEnd between 18 and 19 then '18 to 19'
 	  when AgeRepPeriodEnd between 20 and 24 then '20 to 24'
       when AgeRepPeriodEnd between 25 and 29 then '25 to 29'
 	  when AgeRepPeriodEnd between 30 and 34 then '30 to 34'
       when AgeRepPeriodEnd between 35 and 39 then '35 to 39'
 	  when AgeRepPeriodEnd between 40 and 44 then '40 to 44'
       when AgeRepPeriodEnd between 45 and 49 then '45 to 49'
 	  when AgeRepPeriodEnd between 50 and 54 then '50 to 54'
       when AgeRepPeriodEnd between 55 and 59 then '55 to 59'
 	  when AgeRepPeriodEnd between 60 and 64 then '60 to 64'
       when AgeRepPeriodEnd between 65 and 69 then '65 to 69'
 	  when AgeRepPeriodEnd between 70 and 74 then '70 to 74'
       when AgeRepPeriodEnd between 75 and 79 then '75 to 79'
 	  when AgeRepPeriodEnd between 80 and 84 then '80 to 84'
       when AgeRepPeriodEnd between 85 and 89 then '85 to 89'
 	  when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as Level_2
 ,'NULL' as Level_2_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_provider_type
 group by
 Gender 
 ,case when AgeRepPeriodEnd < 14 then 'Under 14'
       when AgeRepPeriodEnd between 14 and 15 then '14 to 15'
       when AgeRepPeriodEnd between 16 and 17 then '16 to 17'
 	  when AgeRepPeriodEnd between 18 and 19 then '18 to 19'
 	  when AgeRepPeriodEnd between 20 and 24 then '20 to 24'
       when AgeRepPeriodEnd between 25 and 29 then '25 to 29'
 	  when AgeRepPeriodEnd between 30 and 34 then '30 to 34'
       when AgeRepPeriodEnd between 35 and 39 then '35 to 39'
 	  when AgeRepPeriodEnd between 40 and 44 then '40 to 44'
       when AgeRepPeriodEnd between 45 and 49 then '45 to 49'
 	  when AgeRepPeriodEnd between 50 and 54 then '50 to 54'
       when AgeRepPeriodEnd between 55 and 59 then '55 to 59'
 	  when AgeRepPeriodEnd between 60 and 64 then '60 to 64'
       when AgeRepPeriodEnd between 65 and 69 then '65 to 69'
 	  when AgeRepPeriodEnd between 70 and 74 then '70 to 74'
       when AgeRepPeriodEnd between 75 and 79 then '75 to 79'
 	  when AgeRepPeriodEnd between 80 and 84 then '80 to 84'
       when AgeRepPeriodEnd between 85 and 89 then '85 to 89'
 	  when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and provider type occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Provider Type' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_provider_type
 group by Gender, case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_chap45 as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Higher Level)' as Breakdown
 ,age_group_higher_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider type occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider Type' as Breakdown
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_provider_type
 group by case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_prov
 group by OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region' as Breakdown
 ,case when b.Region_code IS Null then 'Unknown' else b.Region_code end as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP' as Breakdown
 ,case when b.STP_code IS null then 'Unknown' else b.STP_code end as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA' as Breakdown
 ,coalesce(LADistrictAuth, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by coalesce(LADistrictAuth, 'Unknown')