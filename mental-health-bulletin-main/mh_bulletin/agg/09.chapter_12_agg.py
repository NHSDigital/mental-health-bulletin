# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---national discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS Breakdown, 
 'England' AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS Breakdown, 
 'England' AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS Breakdown, 
 'England' AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group lower discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Lower Level)' AS Breakdown, 
 age_group_lower_chap12 AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group lower discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Lower Level)' AS Breakdown, 
 age_group_lower_chap12 AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group lower discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Lower Level)' AS Breakdown, 
 age_group_lower_chap12 AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Higher Level)' AS Breakdown, 
 age_group_higher_level AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group higher discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Higher Level)' AS Breakdown, 
 age_group_higher_level AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by age_group_higher_level 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group higher discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Higher Level)' AS Breakdown, 
 age_group_higher_Level AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by age_group_higher_Level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and age group higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Age Group (Higher Level)' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and age group higher discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Age Group (Higher Level)' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and age group higher discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Age Group (Higher Level)' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and ethnicity higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Ethnicity (Higher Level)' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and ethnicity higher discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Ethnicity (Higher Level)' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and ethnicity higher discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Ethnicity (Higher Level)' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and gender higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Gender' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 Gender as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and gender discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Gender' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 Gender as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and gender discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Gender' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 Gender as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and deprivation quintile higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; IMD Quintiles' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and deprivation quintile discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; IMD Quintiles' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and deprivation quintile discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; IMD Quintiles' AS Breakdown, 
 CCG AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by CCG, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS Breakdown, 
 Region_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Region_Code

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS Breakdown, 
 Region_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Region_Code

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS Breakdown, 
 Region_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Region_Code

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Higher Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group higher discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Higher Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group higher discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Higher Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group lower discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Lower Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group lower discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Lower Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group lower discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Lower Level)' AS Breakdown, 
 UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by UpperEthnicity, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level)' AS Breakdown, 
 LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level)' AS Breakdown, 
 LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level)' AS Breakdown, 
 LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower and age group lower discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level); Age Group (Lower Level)' AS Breakdown, 
 LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LowerEthnicity, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower and age group lower discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level); Age Group (Lower Level)' AS Breakdown, 
 LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LowerEthnicity, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower and age group lower discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level); Age Group (Lower Level)' AS Breakdown, 
 LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LowerEthnicity, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender' AS Breakdown, 
 Gender AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender' AS Breakdown, 
 Gender AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender' AS Breakdown, 
 Gender AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group lower discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender; Age Group (Lower Level)' AS Breakdown, 
 Gender AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Gender, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group lower discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender; Age Group (Lower Level)' AS Breakdown, 
 Gender AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Gender, age_Group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group lower discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender; Age Group (Lower Level)' AS Breakdown, 
 Gender AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by Gender, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD' AS Breakdown, 
 IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD' AS Breakdown, 
 IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD' AS Breakdown, 
 IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD Quintiles' AS Breakdown, 
 IMD_Quintile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD Quintiles' AS Breakdown, 
 IMD_Quintile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD Quintiles' AS Breakdown, 
 IMD_Quintile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile and age group lower discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD; Age Group (Lower Level)' AS Breakdown, 
 IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Decile, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile and age group lower discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD; Age Group (Lower Level)' AS Breakdown, 
 IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Decile, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile and age group lower discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD; Age Group (Lower Level)' AS Breakdown, 
 IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 age_group_lower_chap12 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by IMD_Decile, age_group_lower_chap12

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'LAD/UA' AS Breakdown, 
 LADistrictAuth AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LADistrictAuth

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'LAD/UA' AS Breakdown, 
 LADistrictAuth AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LADistrictAuth

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district and age group lower discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'LAD/UA' AS Breakdown, 
 LADistrictAuth AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by LADistrictAuth

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and age group higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Age Group (Higher Level)' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, age_group_higher_Level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and age group higher discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Age Group (Higher Level)' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, age_group_higher_Level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and age group higher discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Age Group (Higher Level)' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_Level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, age_group_higher_Level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity higher discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity higher discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and gender higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Gender' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 Gender as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and gender discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Gender' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 Gender as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and gender discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Gender' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 Gender as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile higher discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; IMD Quintiles' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; IMD Quintiles' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; IMD Quintiles' AS Breakdown, 
 ResponsibleProv AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by ResponsibleProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership discharges from adult acute beds eligible for 72 hour follow up count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP' AS Breakdown, 
 STP_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12a' AS Metric,
 SUM(ElgibleDischFlag_Modified) as metric_value       
 FROM $db_output.72h_follow_Master
 group by STP_Code

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership discharges from adult acute beds followed up within 72 hours count
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP' AS Breakdown, 
 STP_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12b' AS Metric,
 SUM(FollowedUp3Days) as metric_value       
 FROM $db_output.72h_follow_Master
 group by STP_Code

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership discharges from adult acute beds followed up within 72 hours proportion
 SELECT 
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP' AS Breakdown, 
 STP_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '12c' AS Metric,
 coalesce(((SUM(FollowedUp3Days) / SUM(ElgibleDischFlag_Modified)) * 100),0) as metric_value       
 FROM $db_output.72h_follow_Master
 group by STP_Code