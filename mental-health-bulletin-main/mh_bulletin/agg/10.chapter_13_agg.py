# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---national open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' as Breakdown
 ,'England' as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---national contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' as Breakdown
 ,'England' as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---national attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' as Breakdown
 ,'England' as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by orgidprov

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by orgidprov

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' as Breakdown
 ,Orgidprov as Level_1
 ,'England' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by Orgidprov

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(IC_Rec_CCG, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---sustainability and transformation partnership open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' as Breakdown
 ,coalesce(STP_code, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(STP_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---sustainability and transformation partnership contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' as Breakdown
 ,coalesce(STP_code, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(STP_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---sustainability and transformation partnership attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' as Breakdown
 ,coalesce(STP_code, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(STP_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---commissioning region open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' as Breakdown
 ,coalesce(Region_code, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(Region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---commissioning region contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' as Breakdown
 ,coalesce(Region_code, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(Region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---commissioning region attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' as Breakdown
 ,coalesce(Region_code, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(Region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' as Breakdown
 ,coalesce(LADistrictAuth, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' as Breakdown
 ,coalesce(LADistrictAuth, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' as Breakdown
 ,coalesce(LADistrictAuth, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---age group lower contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 group by age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 where AttendOrDNACode IN ('5', '6')
 group by age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---age group higher open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' as Breakdown
 ,age_group_higher_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---age group higher contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' as Breakdown
 ,age_group_higher_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 group by age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---age group higher attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' as Breakdown
 ,age_group_higher_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 where AttendOrDNACode IN ('5', '6')
 group by age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' as Breakdown
 ,gender as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 group by Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' as Breakdown
 ,Gender as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 where AttendOrDNACode IN ('5', '6')
 group by Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 group by IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 where AttendOrDNACode IN ('5', '6')
 group by IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation quintile open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' as Breakdown
 ,IMD_Quintile as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation quintile contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' as Breakdown
 ,IMD_Quintile as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 group by IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation quintile attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' as Breakdown
 ,IMD_Quintile as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 where AttendOrDNACode IN ('5', '6')
 group by IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by gender, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 group by gender, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 where AttendOrDNACode IN ('5', '6')
 group by gender, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher and age group lower open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by UpperEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher and age group lower contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by UpperEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher and age group lower attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by UpperEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group loweropen referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep
 group by LowerEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group lower contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by LowerEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group lower attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by LowerEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile and age group lower open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by IMD_Decile, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile and age group lower contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 group by IMD_Decile, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile and age group lower attended contacts with memory services team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep a
 where AttendOrDNACode IN ('5', '6')
 group by IMD_Decile, age_group_lower_level

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric);