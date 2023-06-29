# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' as Breakdown
 ,coalesce(IC_REC_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by coalesce(IC_REC_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and age group higher contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and age group higher attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(IC_Rec_CCG, 'Unknown'), age_group_higher_level 

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and ethnicity higher open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' as Breakdown
 ,coalesce(IC_REC_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by coalesce(IC_REC_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and ethnicity higher contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and ethnicity higher attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and gender open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' as Breakdown
 ,coalesce(IC_REC_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by coalesce(IC_REC_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and gender contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and ethnicity higher attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' as Breakdown
 ,coalesce(IC_REC_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by coalesce(IC_REC_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---clinical commissioning group and deprivation quintile contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by coalesce(IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' as Breakdown
 ,coalesce(IC_Rec_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by coalesce(IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by Orgidprov, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and age group higher contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by orgidprov, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and age group higher attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by Orgidprov, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by Orgidprov, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by orgidprov, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by Orgidprov, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and gender open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by Orgidprov, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and gender contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by orgidprov, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and gender attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by Orgidprov, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and age group higher open referrals to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13a' as Metric
 ,Count(distinct UniqServReqID) as Metric_value
 from $db_output.Dementia_prep a
 group by Orgidprov, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and deprivation quintile contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13b' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 group by orgidprov, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and deprivation quintile attended contacts to memory services team at the end of the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'13c' as Metric
 ,Count(distinct UniqCareContID) as Metric_value
 from $db_output.Dementia_prep
 where AttendOrDNACode IN ('5', '6')
 group by Orgidprov, IMD_Quintile

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric);