# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group lower children and young people with atleast 2 contacts in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence; Age Group (Lower Level)' as Breakdown
 ,coalesce(IC_REC_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(IC_REC_CCG, 'Unknown'), age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(IC_REC_CCG, 'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(IC_REC_CCG, 'Unknown'), gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(IC_REC_CCG, 'Unknown'), IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group lower children and young people with atleast 2 contacts in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'Provider; Age Group (Lower Level)' as Breakdown
 ,Orgidprov as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by Orgidprov, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and ethnicity higher children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by Orgidprov, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and gender children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by Orgidprov, gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and deprivation quintile children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by Orgidprov, IMD_Quintile