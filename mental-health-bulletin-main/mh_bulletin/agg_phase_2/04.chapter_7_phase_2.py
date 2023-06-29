# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Age Group (Higher Level)' as Breakdown
 ,case when ccg.ORG_CODE is null then 'Unknown'
       else ccg.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST ccg on p.IC_Rec_CCG = ccg.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when ccg.ORG_CODE is null then 'Unknown'
      else ccg.ORG_CODE end, age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Age Group (Higher Level)' as Breakdown
 ,case when b.ORG_CODE is null then 'Unknown'
       else b.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST b on p.IC_Rec_CCG = b.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when b.ORG_CODE is null then 'Unknown'
      else b.ORG_CODE end, age_group_higher_level 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' as Breakdown
 ,case when ccg.ORG_CODE is null then 'Unknown'
       else ccg.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST ccg on p.IC_Rec_CCG = ccg.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when ccg.ORG_CODE is null then 'Unknown'
      else ccg.ORG_CODE end, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Ethnicity (Higher Level)' as Breakdown
 ,case when b.ORG_CODE is null then 'Unknown'
       else b.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST b on p.IC_Rec_CCG = b.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when b.ORG_CODE is null then 'Unknown'
      else b.ORG_CODE end, UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Gender' as Breakdown
 ,case when ccg.ORG_CODE is null then 'Unknown'
       else ccg.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST ccg on p.IC_Rec_CCG = ccg.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when ccg.ORG_CODE is null then 'Unknown'
      else ccg.ORG_CODE end, 
 case when Gender = '1' then '1'
      when Gender = '2' then '2'
      else 'Unknown'  end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Gender' as Breakdown
 ,case when b.ORG_CODE is null then 'Unknown'
       else b.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST b on p.IC_Rec_CCG = b.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when b.ORG_CODE is null then 'Unknown'
          else b.ORG_CODE end, 
 case when Gender = '1' then '1'
      when Gender = '2' then '2'
      else 'Unknown'  end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; IMD Quintiles' as Breakdown
 ,case when ccg.ORG_CODE is null then 'Unknown'
       else ccg.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST ccg on p.IC_Rec_CCG = ccg.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when ccg.ORG_CODE is null then 'Unknown'
      else ccg.ORG_CODE end, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation quintile restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; IMD Quintiles' as Breakdown
 ,case when b.ORG_CODE is null then 'Unknown'
        else b.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,IMD_Quintile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST b on p.IC_Rec_CCG = b.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when b.ORG_CODE is null then 'Unknown'
        else b.ORG_CODE end, IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher and age group lower people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level
 ,'NULL' as Level_1_description
 ,age_group_lower_chap7 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by UpperEthnicity, age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher and age group lower restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level); Age Group (Lower Level)' as Breakdown
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,age_group_lower_chap7 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST b on p.IC_Rec_CCG = b.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by UpperEthnicity, age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile people subject to restrictive intervention count
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
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile restrictive interventions count
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
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation quintile people subject to restrictive intervention count
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
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation quintile restrictive interventions count
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
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by IMD_Quintile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Age Group (Higher Level)' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(age_group_higher_level, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, coalesce(age_group_higher_level, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher restrictive interventions count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Age Group (Higher Level)' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(age_group_higher_level, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, coalesce(age_group_higher_level, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and ethnicity higher people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Ethnicity (Higher Level)' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(UpperEthnicity, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, coalesce(UpperEthnicity, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher restrictive interventions count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Ethnicity (Higher Level)' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(UpperEthnicity, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
                             or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
                             or (res.Enddaterestrictiveint is null))
                             and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, coalesce(UpperEthnicity, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and gender people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Gender' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, 
 case when Gender = '1' then '1'
      when Gender = '2' then '2'
      else 'Unknown'  end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and gender restrictive interventions count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Gender' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, 
 case when Gender = '1' then '1'
      when Gender = '2' then '2'
      else 'Unknown'  end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and deprivation quintile people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; IMD Quintiles' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(IMD_Quintile, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, coalesce(IMD_Quintile, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and deprivation quintile restrictive interventions count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; IMD Quintiles' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(IMD_Quintile, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv, coalesce(IMD_Quintile, 'Unknown')