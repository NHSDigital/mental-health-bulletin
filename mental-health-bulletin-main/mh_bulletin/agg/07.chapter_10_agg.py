# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---national eip referrals on pathway that entered treatment at any time in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' AS BREAKDOWN 
 ,'England' AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23a_common A

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---national eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' AS BREAKDOWN
 ,'England' AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---national open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START 
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' AS BREAKDOWN, 
 'England' AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---national referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' AS BREAKDOWN, 
 'England' AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP64abc_common A

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---national eip referrals on pathway in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'England' AS BREAKDOWN 
 ,'England' AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC
 ,COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIPRef_common A

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower eip referrals on pathway that entered treatment at any time in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' AS BREAKDOWN 
 ,AGE_GROUP as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23a_common A
 GROUP BY A.AGE_GROUP

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' AS BREAKDOWN 
 ,AGE_GROUP AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.AGE_GROUP   

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' AS BREAKDOWN 
 ,AGE_GROUP AS LEVEL_1 
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.AGE_GROUP

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' AS BREAKDOWN 
 ,AGE_GROUP AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP64abc_common A
 GROUP BY A.AGE_GROUP

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower eip referrals on pathway in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Lower Level)' AS BREAKDOWN 
 ,AGE_GROUP as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIPRef_common A
 GROUP BY A.AGE_GROUP

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher eip referrals on pathway that entered treatment at any time in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' AS BREAKDOWN 
 ,A.age_group_higher_level as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23a_common A
 GROUP BY A.age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' AS BREAKDOWN 
 ,A.age_group_higher_level AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' AS BREAKDOWN 
 ,A.age_group_higher_level AS LEVEL_1 
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' AS BREAKDOWN 
 ,A.age_group_higher_level AS LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIP64abc_common A
 GROUP BY A.age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher eip referrals on pathway in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Age Group (Higher Level)' AS BREAKDOWN 
 ,A.age_group_higher_level as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) METRIC_VALUE
 FROM  $db_output.EIPRef_common A
 GROUP BY A.age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' AS BREAKDOWN,
 COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 GROUP BY A.IC_Rec_CCG

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' AS BREAKDOWN,
 COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23a_common as  A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.IC_Rec_CCG

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' AS BREAKDOWN,
 COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP23d_common as  A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.IC_Rec_CCG

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT			
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' AS BREAKDOWN,
 COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIP64abc_common as  A
 GROUP BY A.IC_Rec_CCG

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group eip referrals on pathway in the financial year count
 SELECT			
 				'$rp_startdate' as REPORTING_PERIOD_START
                 ,'$rp_enddate' as REPORTING_PERIOD_END
                 ,'Final' as Status
                 ,'CCG - Registration or Residence' AS BREAKDOWN,
 				COALESCE(A.IC_Rec_CCG, 'Unknown') as LEVEL_1
                 ,'NULL' as LEVEL_1_DESCRIPTION
                 ,'NULL' as Level_2
                 ,'NULL' as Level_2_description
                 ,'NULL' as Level_3
                 ,'NULL' as Level_3_description
                 ,'NULL' as Level_4
                 ,'NULL' as Level_4_description
                 ,'10f' AS METRIC,
                 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) as METRIC_VALUE
 FROM $db_output.EIPRef_common as  A
 GROUP BY A.IC_Rec_CCG

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' AS BREAKDOWN,
 A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 GROUP BY A.OrgIDProv

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' AS BREAKDOWN,
 A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common_Prov A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY A.OrgIDProv

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' AS BREAKDOWN,
 A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common_Prov A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY A.OrgIDProv

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---provider referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' AS BREAKDOWN,
 A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY A.OrgIDProv

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider' AS BREAKDOWN,
 A.OrgIDProv as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common_Prov A
 GROUP BY A.OrgIDProv

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partership eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' AS BREAKDOWN,
 coalesce(stp_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY coalesce(stp_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---sustainability and transformation partnership eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' AS BREAKDOWN,
 coalesce(stp_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY coalesce(stp_code, 'Unknown')   

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---sustainability and transformation partnership open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' AS BREAKDOWN,
 coalesce(stp_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY coalesce(stp_code, 'Unknown')   

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---sustainability and transformation partnership referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' AS BREAKDOWN,
 coalesce(stp_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY coalesce(stp_code, 'Unknown')   

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'STP' AS BREAKDOWN,
 coalesce(stp_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY coalesce(stp_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---commissioning region eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' AS BREAKDOWN,
 coalesce(region_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY coalesce(region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---commissioning region eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' AS BREAKDOWN,
 coalesce(region_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY coalesce(region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---commissioning region open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' AS BREAKDOWN,
 coalesce(region_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY coalesce(region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' AS BREAKDOWN,
 coalesce(region_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY coalesce(region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---commissioning region eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Commissioning Region' AS BREAKDOWN,
 coalesce(region_code, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY coalesce(region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' AS BREAKDOWN,
 coalesce(LADistrictAuth, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' AS BREAKDOWN,
 coalesce(LADistrictAuth, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' AS BREAKDOWN,
 coalesce(LADistrictAuth, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' AS BREAKDOWN,
 coalesce(LADistrictAuth, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---local authority district eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'LAD/UA' AS BREAKDOWN,
 coalesce(LADistrictAuth, 'Unknown') as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A   
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' AS BREAKDOWN,
 Gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' AS BREAKDOWN,
 Gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' AS BREAKDOWN,
 Gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' AS BREAKDOWN,
 Gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender higher eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender' AS BREAKDOWN,
 Gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile higher eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation quintile eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' AS BREAKDOWN,
 IMD_Quintile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation quintile eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' AS BREAKDOWN,
 IMD_Quintile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation quintile open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' AS BREAKDOWN,
 IMD_Quintile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation quintile referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' AS BREAKDOWN,
 IMD_Quintile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation quintile eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD Quintiles' AS BREAKDOWN,
 IMD_Quintile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' AS BREAKDOWN,
 gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY gender, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' AS BREAKDOWN,
 gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY gender, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' AS BREAKDOWN,
 Gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY Gender, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' AS BREAKDOWN,
 gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY gender, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---gender and age group lower eip referrals on pathway in the financial year count
 SELECT
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Gender; Age Group (Lower Level)' AS BREAKDOWN,
 gender as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY gender, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher and age group lower eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY UpperEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher and age group lower eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY UpperEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher and age group lower open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY UpperEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher and age group lower referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY UpperEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity higher and age group lower eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' AS BREAKDOWN,
 UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY UpperEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group lower eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY LowerEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group lower eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY LowerEthnicity, age_group

# COMMAND ----------

# DBTITLE 0,  
 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group lower open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY LowerEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group lower referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY LowerEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---ethnicity lower and age group lower eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Ethnicity (Lower Level); Age Group (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY LowerEthnicity, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile and age group lower eip referrals on pathway that entered treatment at any time in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10a' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 GROUP BY IMD_Decile, age_group

# COMMAND ----------

 %sql
 Insert into $db_output.Output1 
 ---deprivation decile and age group lower eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10b' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23a_common A
 where A.days_between_ReferralRequestReceivedDate <= 14
 GROUP BY IMD_Decile, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile and age group lower open eip referrals on pathway that entered treatment within 2 weeks in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10d' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP23d_common A
 WHERE A.days_between_endate_ReferralRequestReceivedDate <= 14 
 GROUP BY IMD_Decile, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile and age group lower referrals not on pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10e' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIP64abc_common A
 GROUP BY IMD_Decile, age_group

# COMMAND ----------

 %sql
 insert into $db_output.Output1 
 ---deprivation decile and age group eip referrals on pathway in the financial year count
 SELECT 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'IMD; Age Group (Lower Level)' AS BREAKDOWN,
 IMD_Decile as LEVEL_1
 ,'NULL' as LEVEL_1_DESCRIPTION
 ,age_group as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'10f' AS METRIC,
 COALESCE (COUNT (DISTINCT A.UniqServReqID), 0) AS METRIC_VALUE
 FROM $db_output.EIPRef_common A
 GROUP BY IMD_Decile, age_group

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric, breakdown);