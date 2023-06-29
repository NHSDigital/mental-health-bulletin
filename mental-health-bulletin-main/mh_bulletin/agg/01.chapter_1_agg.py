# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---national people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider type people in contact count
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
 ,'1a' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 group by case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider type and gender people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Provider Type' as Breakdown
 ,Gender as Level_1
 ,case when Gender = '1' then 'MALE' when Gender = '2' then 'FEMALE' else 'UNKNOWN' end as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'Null' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 group by Gender, case when Gender = '1' then 'MALE' when Gender = '2' then 'FEMALE' else 'UNKNOWN' end,
 case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and provider type people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Higher Level); Provider Type' as Breakdown
 ,case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
 	  when AgeRepPeriodEnd >= 18 then '18 and over' 
 	  else 'Unknown' end as Level_1
 ,'NULL' as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 group by case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
 	  when AgeRepPeriodEnd >= 18 then '18 and over' 
 	  else 'Unknown' end
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) and gender people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap1 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by Gender, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_chap1 as Level_1
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
 group by age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 group by OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping as b on a.IC_Rec_CCG = b.CCG_code
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and age group (higher level) people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 group by OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and age group (higher level) people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region and age group (higher level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region; Age Group (Higher Level)' as Breakdown
 ,case when b.Region_code IS Null then 'Unknown' else b.Region_code end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and age group (higher level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP; Age Group (Higher Level)' as Breakdown
 ,case when b.STP_code IS null then 'Unknown' else b.STP_code end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district and age group (higher level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA; Age Group (Higher Level)' as Breakdown
 ,coalesce(LADistrictAuth, 'Unknown') as Level_1
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
 group by coalesce(LADistrictAuth, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence), gender and age group (lower level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Gender; Age Group (Lower Level)' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,Gender as Level_2
 ,'NULL' as Level_2_description
 ,age_group_lower_chap1 as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(IC_Rec_CCG, 'Unknown'), gender, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and ethnicity (higher level) people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group (registration or residence) and deprivation people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by coalesce(IC_Rec_CCG, 'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile people in contact count
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
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile, gender and age group (lower level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'IMD; Gender; Age Group (Lower Level)' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,Gender as Level_2
 ,'NULL' as Level_2_description
 ,age_group_lower_chap1 as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1a' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 group by IMD_Decile,Gender, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile and ethnicity (higher level) people in contact count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'IMD; Ethnicity (Higher Level)' as Breakdown
 ,IMD_Decile as Level_1
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
 group by IMD_Decile, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and provider type people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Provider Type' as Breakdown
 ,Gender as Level_1
 ,case when Gender = '1' then 'MALE' when Gender = '2' then 'FEMALE' else 'UNKNOWN' end as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1b' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Admitted'
 group by Gender, case when Gender = '1' then 'MALE' when Gender = '2' then 'FEMALE' else 'UNKNOWN' end, case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and provider type people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level); Provider Type' as Breakdown
 ,age_group_lower_chap1 as Level_1
 ,'NULL' as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1b' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Admitted'
 group by age_group_lower_chap1
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender, age group (lower level) and provider type people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level); Provider Type' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap1 as Level_2
 ,'NULL' as Level_2_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1b' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Admitted'
 group by Gender, age_group_lower_chap1
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group (lower level) people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,Gender as Level_1
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
 group by Gender, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_chap1 as Level_1
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
 group by age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider type people admitted as an inpatient count
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
 ,'1b' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Admitted'
 group by case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and provider type people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Higher Level); Provider Type' as Breakdown
 ,age_group_higher_level as Level_1
 ,'NULL' as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1b' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Admitted'
 group by age_group_higher_level
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Admitted'
 group by OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code and a.Admitted = 'Admitted'
 where a.Admitted = 'Admitted'
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code  and a.Admitted = 'Admitted'
 where a.Admitted = 'Admitted'
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and provider people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Admitted'
 group by OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and clinical commissioning group people admitted as an inpatient count
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
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and commissioning region people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region; Age Group (Higher Level)' as Breakdown
 ,case when b.Region_code IS Null then 'Unknown' else b.Region_code end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping as b on a.IC_Rec_CCG = b.CCG_code and a.Admitted = 'Admitted'
 where a.Admitted = 'Admitted'
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and sustainability and transformation partnership people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP; Age Group (Higher Level)' as Breakdown
 ,case when b.STP_code IS null then 'Unknown' else b.STP_code end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1b' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping as b on a.IC_Rec_CCG = b.CCG_code and a.Admitted = 'Admitted'
 where a.Admitted = 'Admitted'
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and local authority district people admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA; Age Group (Higher Level)' as Breakdown
 ,coalesce(LADistrictAuth, 'Unknown') as Level_1
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
 group by coalesce(LADistrictAuth, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and provider type people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Non_Admitted'
 group by Gender, case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and provider type people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level); Provider Type' as Breakdown
 ,age_group_lower_chap1 as Level_1
 ,'NULL' as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Non_Admitted'
 group by age_group_lower_chap1, case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender, age group (lower level) and provider type people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level); Provider Type' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap1 as Level_2
 ,'NULL' as Level_2_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Non_Admitted'
 group by gender, age_group_lower_chap1, case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group (lower level) people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,Gender as Level_1
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
 group by gender, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_chap1 as Level_1
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
 group by age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider type people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Non_Admitted'
 group by case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and provider type people not admitted as an inpatient count
 select        
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Higher Level); Provider Type' as Breakdown
 ,age_group_higher_level as Level_1
 ,'NULL' as Level_1_description
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,SUM(people) as Metric_value
 from $db_output.people_in_contact_prov_type
 where Admitted = 'Non_Admitted'
 group by age_group_higher_level
 ,case when Provider_type = 'NHS Providers' then 'NHS' else 'Non NHS' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Non_Admitted'
 group by OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 select 
 ---clinical commissioning group people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping as b on a.IC_Rec_CCG = b.CCG_code and a.Admitted = 'Non_Admitted'
 where a.Admitted = 'Non_Admitted'
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping as b on a.IC_Rec_CCG = b.CCG_code and a.Admitted = 'Non_Admitted'
 where a.Admitted = 'Non_Admitted'
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and provider people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact_prov
 where Admitted = 'Non_Admitted'
 group by OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and clinical commissioning group people not admitted as an inpatient count
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
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Admitted = 'Non_Admitted'
 group by coalesce(IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and commissioning region people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region; Age Group (Higher Level)' as Breakdown
 ,case when b.Region_code IS Null then 'Unknown' else b.Region_code end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping as b on a.IC_Rec_CCG = b.CCG_code and a.Admitted = 'Non_Admitted'
 where a.Admitted = 'Non_Admitted'
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and sustainability and transformation partnership people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP; Age Group (Higher Level)' as Breakdown
 ,case when b.STP_code IS null then 'Unknown' else b.STP_code end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1c' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact a
 left join $db_output.STP_Region_mapping as b on a.IC_Rec_CCG = b.CCG_code and a.Admitted = 'Non_Admitted'
 where a.Admitted = 'Non_Admitted'
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) and local authority district people not admitted as an inpatient count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA; Age Group (Higher Level)' as Breakdown
 ,coalesce(LADistrictAuth, 'Unknown') as Level_1
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
 group by coalesce(LADistrictAuth, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 ---improves performance for query below
 OPTIMIZE $db_output.output1 ZORDER BY (Metric)

# COMMAND ----------

 %sql
 ---all breakdowns for proportion of people admitted as an inpatient
 INSERT INTO $db_output.output1
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
 ,'1d' as Metric
 ,cast(b.Metric_value as float) / cast(a.Metric_value as float) * 100 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown in ('Age Group (Lower Level)'
                                 ,'Age Group (Higher Level)'
                                 ,'Age Group (Higher Level); Provider Type'
                                 ,'CCG - Registration or Residence'
                                 ,'CCG - Registration or Residence; Age Group (Higher Level)'
                                 ,'Commissioning Region'
                                 ,'Commissioning Region; Age Group (Higher Level)'
                                 ,'England'
                                 ,'Gender'
                                 ,'Gender; Age Group (Lower Level)'
                                 ,'Gender; Provider Type'
                                 ,'LAD/UA'
                                 ,'LAD/UA; Age Group (Higher Level)'
                                 ,'Provider'
                                 ,'Provider Type'
                                 ,'Provider; Age Group (Higher Level)'
                                 ,'STP'
                                 ,'STP; Age Group (Higher Level)'
                               )
           and metric = '1a') a
 left join (  select *
              from $db_output.output1 b
              where b.metric = '1b') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national census 2020 population count
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
 ,'1f' as Metric
 ,SUM(population_count) as Metric_value
 from (select * from $db_output.MHB_ons_population_v2 where geographic_group_code='E12' and year_of_count = '$populationyear') a
 inner join (select max(ons_release_date) as ons_release_date from $db_output.MHB_ons_population_v2 where year_of_count='$populationyear' and geographic_group_code='E12') b on a.ons_release_date = b.ons_release_date

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender census 2020 population count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender' as Breakdown
 ,case when Gender = 'M' then '1'
       when Gender = 'F' then '2' end as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1f' as Metric
 ,SUM(population_count) as Metric_value
 from ( select * from $db_output.MHB_ons_population_v2 where geographic_group_code='E12' and year_of_count = '$populationyear') a
 inner join (select max(ons_release_date) as ons_release_date from $db_output.MHB_ons_population_v2 where year_of_count='$populationyear' and geographic_group_code='E12') b on a.ons_release_date = b.ons_release_date
 group by case when Gender = 'M' then '1' when Gender = 'F' then '2' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) census 2020 population count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level)' as Breakdown
 ,case when AGE_LOWER between 0 and 5 then '0 to 5'
 	  when AGE_LOWER between 6 and 10 then '6 to 10'
 	  when AGE_LOWER between 11 and 15 then '11 to 15'
 	  when AGE_LOWER = 16 then '16'
 	  when AGE_LOWER = 17 then '17'
 	  when AGE_LOWER = 18 then '18'
 	  when AGE_LOWER = 19 then '19'
 	  when AGE_LOWER between 20 and 24 then '20 to 24'
       when AGE_LOWER between 25 and 29 then '25 to 29'
 	  when AGE_LOWER between 30 and 34 then '30 to 34'
       when AGE_LOWER between 35 and 39 then '35 to 39'
 	  when AGE_LOWER between 40 and 44 then '40 to 44'
       when AGE_LOWER between 45 and 49 then '45 to 49'
 	  when AGE_LOWER between 50 and 54 then '50 to 54'
       when AGE_LOWER between 55 and 59 then '55 to 59'
 	  when AGE_LOWER between 60 and 64 then '60 to 64'
       when AGE_LOWER between 65 and 69 then '65 to 69'
 	  when AGE_LOWER between 70 and 74 then '70 to 74'
       when AGE_LOWER between 75 and 79 then '75 to 79'
 	  when AGE_LOWER between 80 and 84 then '80 to 84'
       when AGE_LOWER between 85 and 89 then '85 to 89'
 	  when AGE_LOWER >= '90' then '90 or over' else 'Unknown' end as Level_1   
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1f' as Metric
 ,SUM(population_count) as Metric_value
 from ( select * from $db_output.MHB_ons_population_v2 where geographic_group_code='E12' and year_of_count = '$populationyear') a
 inner join (select max(ons_release_date) as ons_release_date from $db_output.MHB_ons_population_v2 where year_of_count='$populationyear' and geographic_group_code='E12') b on a.ons_release_date = b.ons_release_date
 group by 
 case when AGE_LOWER between 0 and 5 then '0 to 5'
 	  when AGE_LOWER between 6 and 10 then '6 to 10'
 	  when AGE_LOWER between 11 and 15 then '11 to 15'
 	  when AGE_LOWER = 16 then '16'
 	  when AGE_LOWER = 17 then '17'
 	  when AGE_LOWER = 18 then '18'
 	  when AGE_LOWER = 19 then '19'
 	  when AGE_LOWER between 20 and 24 then '20 to 24'
       when AGE_LOWER between 25 and 29 then '25 to 29'
 	  when AGE_LOWER between 30 and 34 then '30 to 34'
       when AGE_LOWER between 35 and 39 then '35 to 39'
 	  when AGE_LOWER between 40 and 44 then '40 to 44'
       when AGE_LOWER between 45 and 49 then '45 to 49'
 	  when AGE_LOWER between 50 and 54 then '50 to 54'
       when AGE_LOWER between 55 and 59 then '55 to 59'
 	  when AGE_LOWER between 60 and 64 then '60 to 64'
       when AGE_LOWER between 65 and 69 then '65 to 69'
 	  when AGE_LOWER between 70 and 74 then '70 to 74'
       when AGE_LOWER between 75 and 79 then '75 to 79'
 	  when AGE_LOWER between 80 and 84 then '80 to 84'
       when AGE_LOWER between 85 and 89 then '85 to 89'
 	  when AGE_LOWER >= '90' then '90 or over' else 'Unknown' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group (lower level) census 2020 population count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,case when Gender = 'M' then '1'
       when Gender = 'F' then '2' end as Level_1
 ,'NULL' as Level_1_description
 ,case when AGE_LOWER between 0 and 5 then '0 to 5'
 	  when AGE_LOWER between 6 and 10 then '6 to 10'
 	  when AGE_LOWER between 11 and 15 then '11 to 15'
 	  when AGE_LOWER = 16 then '16'
 	  when AGE_LOWER = 17 then '17'
 	  when AGE_LOWER = 18 then '18'
 	  when AGE_LOWER = 19 then '19'
 	  when AGE_LOWER between 20 and 24 then '20 to 24'
       when AGE_LOWER between 25 and 29 then '25 to 29'
 	  when AGE_LOWER between 30 and 34 then '30 to 34'
       when AGE_LOWER between 35 and 39 then '35 to 39'
 	  when AGE_LOWER between 40 and 44 then '40 to 44'
       when AGE_LOWER between 45 and 49 then '45 to 49'
 	  when AGE_LOWER between 50 and 54 then '50 to 54'
       when AGE_LOWER between 55 and 59 then '55 to 59'
 	  when AGE_LOWER between 60 and 64 then '60 to 64'
       when AGE_LOWER between 65 and 69 then '65 to 69'
 	  when AGE_LOWER between 70 and 74 then '70 to 74'
       when AGE_LOWER between 75 and 79 then '75 to 79'
 	  when AGE_LOWER between 80 and 84 then '80 to 84'
       when AGE_LOWER between 85 and 89 then '85 to 89'
 	  when AGE_LOWER >= '90' then '90 or over' else 'Unknown' end as Level_2  
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1f' as Metric
 ,SUM(population_count) as Metric_value
 from ( select * from $db_output.MHB_ons_population_v2 where geographic_group_code='E12' and year_of_count = '$populationyear') a
 inner join (select max(ons_release_date) as ons_release_date from $db_output.MHB_ons_population_v2 where year_of_count='$populationyear' and geographic_group_code='E12') b on a.ons_release_date = b.ons_release_date
 group by case when Gender = 'M' then '1'
       when Gender = 'F' then '2' end
 ,case when AGE_LOWER between 0 and 5 then '0 to 5'
 	  when AGE_LOWER between 6 and 10 then '6 to 10'
 	  when AGE_LOWER between 11 and 15 then '11 to 15'
 	  when AGE_LOWER = 16 then '16'
 	  when AGE_LOWER = 17 then '17'
 	  when AGE_LOWER = 18 then '18'
 	  when AGE_LOWER = 19 then '19'
 	  when AGE_LOWER between 20 and 24 then '20 to 24'
       when AGE_LOWER between 25 and 29 then '25 to 29'
 	  when AGE_LOWER between 30 and 34 then '30 to 34'
       when AGE_LOWER between 35 and 39 then '35 to 39'
 	  when AGE_LOWER between 40 and 44 then '40 to 44'
       when AGE_LOWER between 45 and 49 then '45 to 49'
 	  when AGE_LOWER between 50 and 54 then '50 to 54'
       when AGE_LOWER between 55 and 59 then '55 to 59'
 	  when AGE_LOWER between 60 and 64 then '60 to 64'
       when AGE_LOWER between 65 and 69 then '65 to 69'
 	  when AGE_LOWER between 70 and 74 then '70 to 74'
       when AGE_LOWER between 75 and 79 then '75 to 79'
 	  when AGE_LOWER between 80 and 84 then '80 to 84'
       when AGE_LOWER between 85 and 89 then '85 to 89'
 	  when AGE_LOWER >= '90' then '90 or over' else 'Unknown' end

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---all breakdowns for proportion of the population in contact with NHS funded secondary mental health, learning disabilities and autism services
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
 ,'1g' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown in ('England'
                           ,'Gender; Age Group (Lower Level)'
                           ,'Gender'
                           ,'Age Group (Lower Level)')
       and metric = '1a') a
 left join (select * from $db_output.output1 where metric = '1f') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) census 2011 population
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
 INSERT INTO $db_output.output1
 ---national census 2011 population
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
 ,CAST(SUM(population) as int) as Metric_value ---casting as int here because large floats can be converted to scientific notation
 from $db_output.pop_health
 where ethnic_group_formatted not in ('White','Mixed','Asian or Asian British','Black or Black British','Other Ethnic Groups')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) census 2011 population
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level)' as Breakdown
 ,CASE WHEN ethnic_group_formatted IN ('Bangladeshi', 'Indian', 'Any Other Asian Background', 'Pakistani') THEN 'Asian or Asian British'
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

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national number of people in contact with a known gender, age and ethnicity
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
 ,'1h' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Gender in ('1','2')
 and AgeRepPeriodEnd is not null
 and NHSDEthnicity is not null and NHSDEthnicity not in ('-1','99','Z')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) people in contact with a known gender, age and ethnicity
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
 ,'1h' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Gender in ('1','2')
 and AgeRepPeriodEnd is not null
 and NHSDEthnicity is not null and NHSDEthnicity not in ('-1','99','Z')
 group by LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) people in contact with a known gender, age and ethnicity
 select distinct
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
 ,'1h' as Metric
 ,COUNT(distinct person_id) as Metric_value
 from $db_output.people_in_contact
 where Gender in ('1','2')
 and AgeRepPeriodEnd is not null
 and NHSDEthnicity is not null and NHSDEthnicity not in ('-1','99','Z')
 group by UpperEthnicity

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---all breakdowns for crude rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population
 select distinct
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
 ,'1i' as Metric
 ,cast(b.Metric_value as float) / cast(a.Metric_value as float) * 100000 as Metric_value
 from (  select *
         from $db_output.output1
         where breakdown in ('England'
                               ,'Ethnicity (Higher Level)'
                               ,'Ethnicity (Lower Level)')
           and metric = '1m') a
 left join (select * from $db_output.output1 where metric = '1h') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (higher level) standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level)' as Breakdown
 ,EthnicGroup as Level_1
 ,EthnicGroup as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1j' as Metric
 ,STANDARDISED_RATE_PER_100000 as Metric_value
 from $db_output.standardisation a
 where Ethnic_level = 'Sub_ethnicity'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population
 select  
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Lower Level)' as Breakdown
 ,case when EthnicGroup = 'British' then 'A'
       when EthnicGroup = 'Irish' then 'B'
      when EthnicGroup = 'Any Other White Background' then 'C'
      when EthnicGroup = 'White and Black Caribbean' then 'D'
      when EthnicGroup = 'White and Black African' then 'E'
      when EthnicGroup = 'White and Asian' then 'F'
      when EthnicGroup = 'Any Other Mixed Background' then 'G'
      when EthnicGroup = 'Indian' then 'H'
      when EthnicGroup = 'Pakistani' then 'J'
      when EthnicGroup = 'Bangladeshi' then 'K'
      when EthnicGroup = 'Any Other Asian Background' then 'L'
      when EthnicGroup = 'Caribbean' then 'M'
      when EthnicGroup = 'African' then 'N'
      when EthnicGroup = 'Any Other Black Background' then 'P'
      when EthnicGroup = 'Chinese' then 'R'
      when EthnicGroup = 'Any Other Ethnic Group' then 'S' else '99' end as Level_1
 ,EthnicGroup as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1j' as Metric
 ,STANDARDISED_RATE_PER_100000 as Metric_value
 from $db_output.standardisation a
 where Ethnic_level = 'Lower_Ethnicity'

# COMMAND ----------

 %sql
 ---ethnicity (higher level) confidence interval for the standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population
 INSERT INTO $db_output.output1
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level)' as Breakdown
 ,EthnicGroup as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1k' as Metric
 ,CONFIDENCE_INTERVAL_95 as Metric_value
 from $db_output.standardisation a
 where Ethnic_level = 'Sub_ethnicity'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity (lower level) confidence interval for the standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population
 select  
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Lower Level)' as Breakdown
 ,case when EthnicGroup = 'British' then 'A'
       when EthnicGroup = 'Irish' then 'B'
      when EthnicGroup = 'Any Other White Background' then 'C'
      when EthnicGroup = 'White and Black Caribbean' then 'D'
      when EthnicGroup = 'White and Black African' then 'E'
      when EthnicGroup = 'White and Asian' then 'F'
      when EthnicGroup = 'Any Other Mixed Background' then 'G'
      when EthnicGroup = 'Indian' then 'H'
      when EthnicGroup = 'Pakistani' then 'J'
      when EthnicGroup = 'Bangladeshi' then 'K'
      when EthnicGroup = 'Any Other Asian Background' then 'L'
      when EthnicGroup = 'Caribbean' then 'M'
      when EthnicGroup = 'African' then 'N'
      when EthnicGroup = 'Any Other Black Background' then 'P'
      when EthnicGroup = 'Chinese' then 'R'
      when EthnicGroup = 'Any Other Ethnic Group' then 'S' else '99' end as Level_1
 ,EthnicGroup as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'1k' as Metric
 ,CONFIDENCE_INTERVAL_95 as Metric_value
 from $db_output.standardisation a
 where Ethnic_level = 'Lower_Ethnicity'

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric)