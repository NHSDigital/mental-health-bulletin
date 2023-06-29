# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---bed type and age group (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Bed Type as Reported to MHSDS; Age Group (Higher Level)' as Breakdown
 ,bed_type as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by bed_type, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and ethnicity (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Bed Type as Reported to MHSDS; Ethnicity (Higher Level)' as Breakdown
 ,bed_type as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by bed_type, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and deprivation quintile occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Bed Type as Reported to MHSDS; IMD Quintiles' as Breakdown
 ,bed_type as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by bed_type, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and gender occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Bed Type as Reported to MHSDS; Gender' as Breakdown
 ,bed_type as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by bed_type, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Bed Type as Reported to MHSDS' as Breakdown
 ,bed_type as Level_1
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
 group by bed_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and age group (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Age Group (Higher Level)' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by coalesce(IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and bed type occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Bed Type Group' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,bed_type as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by coalesce(IC_Rec_CCG, 'Unknown'), bed_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and ethnicity (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by coalesce(IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and gender occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Gender' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by coalesce(IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and deprivation quintile occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; IMD Quintiles' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays
 group by coalesce(IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level)' as Breakdown
 ,UpperEthnicity as Level_1
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
 group by UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) and age group (lower level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
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
 group by UpperEthnicity, age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
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
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'IMD' as Breakdown
 ,IMD_Decile as Level_1
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
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'IMD Quintiles' as Breakdown
 ,IMD_Quintile as Level_1
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
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and age group (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Age Group (Higher Level)' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_prov
 group by OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and bed type occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Bed Type Group' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,bed_type as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_prov
 group by OrgIDProv, bed_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity (higher level) occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Ethnicity (Higher Level)' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_prov
 group by OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and gender occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Gender' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_prov
 group by OrgIDProv, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile occupied bed days count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; IMD Quintiles' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'4a' as Metric
 ,SUM(beddays) as Metric_value
 from $db_output.beddays_prov
 group by OrgIDProv, IMD_Quintile