# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and gender people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Gender' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,Gender as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(IC_Rec_CCG, 'Unknown'), Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and service type people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Service Type Group' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,service_type as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(IC_Rec_CCG, 'Unknown'), service_type

# COMMAND ----------

# DBTITLE 0, 
 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) and age group (lower level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap1 as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by UpperEthnicity, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) and provider people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Ethnicity (Higher Level)' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 group by OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and provider people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Gender' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 group by OrgIDProv, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation and provider people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; IMD Quintiles' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 group by OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and provider people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Service Type Group' as Breakdown
 ,OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,service_type as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 group by OrgIDProv, service_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by service_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and age group (higher level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; Age Group (Higher Level)' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by service_type, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and ethnicity (higher level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; Ethnicity (Higher Level)' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by service_type, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and gender people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; Gender' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by service_type, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and deprivation quintile people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; IMD Quintiles' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by service_type, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by bed_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and age group (higher level) people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by bed_type, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and ethnicity (higher level) people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by bed_type, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and depriavtion quintile people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by bed_type, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---bed type and gender admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by bed_type, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and bed type people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), bed_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and ethnicity (higher level) people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and gender people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and deprivation quintile people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) and ethnicity (higher level) people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap1 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by UpperEthnicity, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and bed type people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Admitted'
 group by OrgIDProv, bed_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity (higher level) people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Admitted'
 group by OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and gender people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Admitted'
 group by OrgIDProv, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Admitted'
 group by OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and ethnicity (higher level) people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and gender people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and deprivation decile people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; IMD' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Decile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and service type people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Service Type Group' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,service_type as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), service_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) and age group (lower level) people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap1 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by UpperEthnicity, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity (higher level) people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Non_Admitted'
 group by OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and gender people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Non_Admitted'
 group by OrgIDProv, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Non_Admitted'
 group by OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by service_type

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and age group (higher level) people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; Age Group (Higher Level)' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by service_type, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and ethnicity (higher level) people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; Ethnicity (Higher Level)' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by service_type, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and gender people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; Gender' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by service_type, gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type and deprivation quintile people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Service Type as Reported to MHSDS; IMD Quintiles' as Breakdown
 ,service_type as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by service_type, IMD_Quintile

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---all breakdowns for proportion of people in contact with NHS funded secondary mental health, learning disabilities and autism services not admitted as an inpatient
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,a.Breakdown
 ,a.Level_1
 ,a.Level_1_description
 ,a.Level_2
 ,a.Level_2_description
 ,a.Level_3
 ,a.Level_3_description
 ,a.Level_4
 ,a.Level_4_description
 ,'1e' as Metric
 ,cast(b.Metric_value as float) / cast(a.Metric_value as float) * 100 as Metric_value
 
 from $db_output.output1 a
 left join $db_output.output1 b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3 and b.metric = '1c'
 where a.breakdown in ('England', 
                       'Age Group (Higher Level)',
                       'Age Group (Lower Level)',
                       'CCG - Registration or Residence',
                       'CCG - Registration or Residence; Age Group (Higher Level)',
                       'CCG - Registration or Residence; Ethnicity (Higher Level)',
                       'CCG - Registration or Residence; Gender',
                       'CCG - Registration or Residence; IMD',
                       'CCG - Registration or Residence; Service Type Group',
                       'Ethnicity (Higher Level)',
                       'Ethnicity (Higher Level); Age Group (Lower Level)', 
                       'Ethnicity (Lower Level)',
                       'Gender',
                       'Gender; Age Group (Lower Level)',
                       'Gender; Provider Type',
                       'IMD',
                       'IMD Quintiles',
                       'Provider',
                       'Provider Type',
                       'Provider; Age Group (Higher Level)',
                       'Provider; Ethnicity (Higher Level)', 
                       'Provider; Gender',
                       'Provider; IMD Quintiles',                      
                       'Service Type as Reported to MHSDS',
                       'Service Type as Reported to MHSDS; Age Group (Higher Level)',
                       'Service Type as Reported to MHSDS; Ethnicity (Higher Level)',
                       'Service Type as Reported to MHSDS; Gender',
                       'Service Type as Reported to MHSDS; IMD Quintiles'                      
                       ) and a.metric = '1a'

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric)