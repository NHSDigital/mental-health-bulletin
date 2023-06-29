# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---national children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by Orgidprov

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group children and young people with atleast 2 contacts in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status
 ,'CCG - Registration or Residence' as Breakdown
 ,coalesce(IC_REC_CCG, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(IC_REC_CCG, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(STP_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(Region_code, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---local authority district children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by coalesce(LADistrictAuth, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second
 group by UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second a
 group by LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second a
 group by age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---gender children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second a
 group by Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second a
 group by IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation quintile children and young people with atleast 2 contacts in the financial year count
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
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second a
 group by IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---gender and age group lower children and young people with atleast 2 contacts in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status,
 'Gender; Age Group (Lower Level)' as Breakdown
 ,gender as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second a
 group by gender, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher and age group lower children and young people with atleast 2 contacts in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status,
 'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
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
 group by UpperEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower and age group lower children and young people with atleast 2 contacts in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status,
 'Ethnicity (Lower Level); Age Group (Lower Level)' as Breakdown
 ,LowerEthnicity as Level_1
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
 group by LowerEthnicity, age_group_lower_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile and age group lower children and young people with atleast 2 contacts in the financial year count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'Final' as Status,
 'IMD; Age Group (Lower Level)' as Breakdown
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'9a' as Metric
 ,count (distinct 2_contact) as Metric_value
 from $db_output.Cont_prep_table_second a
 group by IMD_Decile, age_group_lower_level

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Output1 ZORDER BY (Metric, Breakdown)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and ethnicity lower children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in 
 ('Ethnicity (Higher Level)'
 ,'Ethnicity (Lower Level)')
 and metric = '9a') a
 left join (select * from $db_output.output1 where metric = '1m') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('England')
 and metric = '9a') a
 left join (select 'England' as Breakdown, SUM(Metric_Value) as Metric_Value from $db_output.output1 where metric = '1f' and Breakdown = 'Age Group (Lower Level)' and Level_1 in ('0 to 5','11 to 15','16','17')) b on a.Breakdown = b.Breakdown

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group higher and age group lower children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in 
 ('Gender; Age Group (Lower Level)'
 ,'Age Group (Lower Level)')
 and metric = '9a') a
 left join (select * from $db_output.output1 where metric = '1f') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group lower children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('Gender')
 and metric = '9a') a
 left join (select  'Gender' as Breakdown,
 level_1,
 SUM(Metric_Value) AS Metric_Value
 from $db_output.output1
 where metric = '1f'
 and Breakdown = 'Gender; Age Group (Lower Level)'
 and Level_2 in ('0 to 5','11 to 15','16','17')
 GROUP BY Breakdown,
 Level_1) b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('IMD')
 and metric = '9a') a
 left join (select * from $db_output.MHB_imd_pop) b on a.level_1 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('CCG - Registration or Residence')
 and metric = '9a') a
 left join (select * from $db_output.mhb_ccg_cyp_pop) b on a.level_1 = b.CCG_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('STP')
 and metric = '9a') a
 left join (select * from $db_output.mhb_stp_cyp_pop) b on a.level_1 = b.STP_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and age group lower children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('STP; Age Group (Lower Level)')
 and metric = '9a') a
 left join (select * from $db_output.stp_age_pop_contacts) b on a.level_1 = b.STP_CODE and a.level_2 = b.Der_Age_Group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and gender children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('STP; Gender')
 and metric = '9a') a
 left join (select * from $db_output.stp_cyp_gender_pop) b on a.level_1 = b.STP_CODE and a.level_2 = b.Sex

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and deprivation decile children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('STP; IMD')
 and metric = '9a') a
 left join (select * from $db_output.stp_imd_pop) b on a.level_1 = b.STP and a.level_2 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and ethnicity higher children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('STP; Ethnicity (Higher Level)')
 and metric = '9a') a
 left join (select * from $db_output.stp_cyp_eth_pop) b on a.level_1 = b.STP_CODE
 and a.level_2 = CASE WHEN b.Ethnic_group = 'Black' THEN 'Black or Black British'
                      WHEN b.Ethnic_group = 'Asian' THEN 'Asian or Asian British'
                      WHEN b.Ethnic_group = 'Other' THEN 'Other Ethnic Groups'
                      ELSE b.Ethnic_group END

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region children and young people with atleast 2 contacts in the financial year crude rate
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
 ,'9b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
 from $db_output.output1
 where breakdown in ('Commissioning Region')
 and metric = '9a') a
 left join (select * from $db_output.mhb_region_cyp_pop) b on a.level_1 = b.REGION_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Output1 ZORDER BY (Metric, Breakdown);