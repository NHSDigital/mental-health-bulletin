# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---national admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group (lower level) admissions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,Gender as Level_1
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
 group by Gender, age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges_prov
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' 
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges a 
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate' 
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district admissions count
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
 ,'5a' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where StartDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group (lower level) discharges count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap45 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by Gender, age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (lower level) discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by age_group_lower_chap45

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group (higher level) discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges_prov
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate' 
 group by case when b.Region_code IS Null then 'Unknown' else b.Region_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges a
 left join $db_output.STP_Region_mapping b on a.IC_Rec_CCG = b.CCG_code
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate' 
 group by case when b.STP_code IS null then 'Unknown' else b.STP_code end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district discharges count
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
 ,'5b' as Metric
 ,COUNT(distinct UniqHospProvSpellID) as Metric_value
 from $db_output.admissions_discharges
 where DischDateHospProvSpell between '$rp_startdate' and '$rp_enddate'
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

# DBTITLE 1,All breakdowns
 %sql
 ---all breakdowns for average number of daily occupied beds
 ---average number of daily occupied beds uses occupied bed days as the numerator so chapter_4_agg script must be run first for this to work
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
 ,'5c' as Metric
 ,cast(Metric_value as float) / 365 as Metric_value
 from $db_output.output1 a
 where Metric = '4a'
 and a.Breakdown in ('England','Provider','Commissioning Region','STP','CCG - Registration or Residence','LAD/UA')