# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23d_common as  A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP64abc_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher eip referrals on pathway in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIPRef_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23d_common as  A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP64abc_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher eip referrals on pathway in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIPRef_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23d_common as  A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender higher referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP64abc_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender eip referrals on pathway in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Gender' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIPRef_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23d_common as  A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP64abc_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile higher eip referrals on pathway in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; IMD Quintiles' AS BREAKDOWN
 ,COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIPRef_common as  A
 GROUP BY COALESCE(A.IC_Rec_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 GROUP BY A.OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' AS BREAKDOWN,
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common_Prov A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and age group higher referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY A.OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common_Prov A
 GROUP BY A.OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 GROUP BY A.OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common_Prov A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
      SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY A.OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and ethnicity higher eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Ethnicity (Higher Level)' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common_Prov A
 GROUP BY A.OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and ethnicity higher eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 GROUP BY A.OrgIDProv, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and gender eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.OrgIDProv, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and gender open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common_Prov A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.OrgIDProv, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and gender referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY A.OrgIDProv, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and gender eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Gender' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common_Prov A
 GROUP BY A.OrgIDProv, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and deprivation quintile eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 GROUP BY A.OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and deprivation quintile eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and deprivation quintile open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common_Prov A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and deprivation quintile referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY A.OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider and deprivation quintile eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; IMD Quintiles' AS BREAKDOWN
 ,A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common_Prov A
 GROUP BY A.OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric, Breakdown);

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---all breakdowns for proportion of referrals eip pathway that waited 2 weeks or less to enter treatment
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,a.breakdown AS BREAKDOWN
 ,a.level_1 as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,a.level_2 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10c' AS METRIC
 ,(CAST(B.METRIC_VALUE AS FLOAT)/CAST(A.METRIC_VALUE AS FLOAT))*100 AS METRIC_VALUE
 FROM (select * from $db_output.output1 where metric = '10a') A
 left join $db_output.output1 b on a.breakdown = b.breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and b.metric = '10b'
 GROUP BY a.breakdown, a.level_1, a.level_2, b.metric_value, a.metric_value
 ORDER BY a.breakdown

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric, Breakdown);

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---england, gender, age group and gender; age group lower breakdowns for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown in ('England','Gender')
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select *
             from $db_output.output1
             where metric = '1f') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group lower breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'Age Group (Lower Level)'                              
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (SELECT CASE WHEN AGE_LOWER BETWEEN 0 AND 13 THEN '0 to 13'
                        WHEN AGE_LOWER BETWEEN 14 AND 17 THEN '14 to 17'
                        WHEN AGE_LOWER BETWEEN 18 AND 19 THEN '18 to 19'
                        WHEN AGE_LOWER BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AGE_LOWER BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AGE_LOWER BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AGE_LOWER BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AGE_LOWER BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AGE_LOWER BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AGE_LOWER BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AGE_LOWER BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '65 to 69'
                        WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '70 to 74'
                        WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '75 to 79'
                        WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '80 to 84'
                        WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '85 to 89'
                        WHEN AGE_LOWER > 90 THEN '90 or over' ELSE 'Unknown' END as AGE,
             SUM(POPULATION_COUNT) AS Metric_Value
             FROM $db_output.MHB_ons_population_v2
             GROUP BY CASE WHEN AGE_LOWER BETWEEN 0 AND 13 THEN '0 to 13'
                            WHEN AGE_LOWER BETWEEN 14 AND 17 THEN '14 to 17'
                            WHEN AGE_LOWER BETWEEN 18 AND 19 THEN '18 to 19'
                            WHEN AGE_LOWER BETWEEN 20 AND 24 THEN '20 to 24'
                            WHEN AGE_LOWER BETWEEN 25 AND 29 THEN '25 to 29'
                            WHEN AGE_LOWER BETWEEN 30 AND 34 THEN '30 to 34'
                            WHEN AGE_LOWER BETWEEN 35 AND 39 THEN '35 to 39'
                            WHEN AGE_LOWER BETWEEN 40 AND 44 THEN '40 to 44'
                            WHEN AGE_LOWER BETWEEN 45 AND 49 THEN '45 to 49'
                            WHEN AGE_LOWER BETWEEN 50 AND 54 THEN '50 to 54'
                            WHEN AGE_LOWER BETWEEN 55 AND 59 THEN '55 to 59'
                            WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '60 to 64'
                            WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '65 to 69'
                            WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '70 to 74'
                            WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '75 to 79'
                            WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '80 to 84'
                            WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '85 to 89'
                            WHEN AGE_LOWER > 90 THEN '90 or over' ELSE 'Unknown' END) b on a.level_1 = b.AGE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender; age group lower breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'Gender; Age Group (Lower Level)'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (SELECT CASE WHEN AGE_LOWER BETWEEN 0 AND 13 THEN '0 to 13'
                        WHEN AGE_LOWER BETWEEN 14 AND 17 THEN '14 to 17'
                        WHEN AGE_LOWER BETWEEN 18 AND 19 THEN '18 to 19'
                        WHEN AGE_LOWER BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AGE_LOWER BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AGE_LOWER BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AGE_LOWER BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AGE_LOWER BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AGE_LOWER BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AGE_LOWER BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AGE_LOWER BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '65 to 69'
                        WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '70 to 74'
                        WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '75 to 79'
                        WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '80 to 84'
                        WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '85 to 89'
                        WHEN AGE_LOWER > 90 THEN '90 or over' ELSE 'Unknown' END as AGE,
                    CASE WHEN Gender = 'M' THEN '1'
                         WHEN Gender = 'F' THEN '2'
                         ELSE 'OTHER' END as Gender,
             SUM(POPULATION_COUNT) AS Metric_Value
             FROM $db_output.MHB_ons_population_v2
             GROUP BY CASE WHEN AGE_LOWER BETWEEN 0 AND 13 THEN '0 to 13'
                            WHEN AGE_LOWER BETWEEN 14 AND 17 THEN '14 to 17'
                            WHEN AGE_LOWER BETWEEN 18 AND 19 THEN '18 to 19'
                            WHEN AGE_LOWER BETWEEN 20 AND 24 THEN '20 to 24'
                            WHEN AGE_LOWER BETWEEN 25 AND 29 THEN '25 to 29'
                            WHEN AGE_LOWER BETWEEN 30 AND 34 THEN '30 to 34'
                            WHEN AGE_LOWER BETWEEN 35 AND 39 THEN '35 to 39'
                            WHEN AGE_LOWER BETWEEN 40 AND 44 THEN '40 to 44'
                            WHEN AGE_LOWER BETWEEN 45 AND 49 THEN '45 to 49'
                            WHEN AGE_LOWER BETWEEN 50 AND 54 THEN '50 to 54'
                            WHEN AGE_LOWER BETWEEN 55 AND 59 THEN '55 to 59'
                            WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '60 to 64'
                            WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '65 to 69'
                            WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '70 to 74'
                            WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '75 to 79'
                            WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '80 to 84'
                            WHEN AGE_LOWER BETWEEN 65 AND 69 THEN '85 to 89'
                            WHEN AGE_LOWER > 90 THEN '90 or over' ELSE 'Unknown' END,
                        CASE WHEN Gender = 'M' THEN '1'
                             WHEN Gender = 'F' THEN '2'
                             ELSE 'OTHER' END) b on a.level_1 = b.Gender and a.level_2 = b.AGE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and ethnicity lower breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown in ('Ethnicity (Higher Level)','Ethnicity (Lower Level)')
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.output1 where metric = '1m') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'IMD'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.MHB_imd_pop) b on a.level_1 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group breakdowns for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown like 'CCG%'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.mhb_ccg_pop) b on a.level_1 = b.CCG_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.mhb_stp_pop) b on a.level_1 = b.STP_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and age group lower breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Age Group (Lower Level)'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.stp_age_pop_adults) b on a.level_1 = b.STP_CODE and a.level_2 = b.Der_Age_Group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and gender breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Gender'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.stp_gender_pop) b on a.level_1 = b.STP_CODE and a.level_2 = b.Sex

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and deprivation decile breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; IMD'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.stp_imd_pop) b on a.level_1 = b.STP and a.level_2 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and ethnicity higher breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Ethnicity (Higher Level)'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.stp_eth_pop) b on a.level_1 = b.STP_CODE
                                                   and a.level_2 = CASE WHEN b.Ethnic_group = 'Black' THEN 'Black or Black British'
                                                                        WHEN b.Ethnic_group = 'Asian' THEN 'Asian or Asian British'
                                                                        WHEN b.Ethnic_group = 'Other' THEN 'Other Ethnic Groups'
                                                                        ELSE b.Ethnic_group END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region breakdown for crude rate values of 10a, 10b, 10d, 10e, 10f
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
 ,CASE WHEN a.metric = '10a' THEN '10g'
       WHEN a.metric = '10b' THEN '10h'
       WHEN a.metric = '10d' THEN '10i'
       WHEN a.metric = '10e' THEN '10j'
       WHEN a.metric = '10f' THEN '10k'
       END  as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown ='Commissioning Region'
       and metric in ('10a','10b','10d','10e','10f')) a
 left join (select * from $db_output.mhb_region_pop) b on a.level_1 = b.REGION_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric, Breakdown);