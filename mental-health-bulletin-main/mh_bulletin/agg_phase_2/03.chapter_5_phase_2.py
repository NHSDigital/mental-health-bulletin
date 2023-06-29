# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and age group (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and ethnicity (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and gender admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and deprivation quintile admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by UpperEthnicity

# COMMAND ----------

# DBTITLE 0, 
 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) and age group (lower level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by UpperEthnicity, age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and age group (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges_prov
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges_prov
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and gender admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges_prov
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by OrgIDProv, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges_prov
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by bed_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and age group (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by bed_type, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and deprivation quintile admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by bed_type, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and ethnicity (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by bed_type, UpperEthnicity