# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---national people subject to restrictive intervention count
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
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.person_id = mpi.person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null)) 
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---higher ethnicity people subject to restrictive intervention count
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
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.RD_Ethnicity b on mpi.NHSDEthnicity = b.code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null)) 
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'                    
 group by UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---lower ethnicity people subject to restrictive intervention count
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
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.RD_Ethnicity b on mpi.NHSDEthnicity = b.code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null)) 
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---gender people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender' as Breakdown
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_1
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
 group by 
 case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown'  end
 ,case when Gender = '1' then 'Male' when Gender = '2' then 'Female' else 'Unknown' end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_chap7 as Level_1
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
 group by age_group_lower_chap7 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher people subject to restrictive intervention count
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
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---gender and age group lower people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_1
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
 group by 
 case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown' end
 ,age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type' as Breakdown
 ,coalesce(int.key, 'Unknown') as Level_1
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
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type and gender people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type; Gender' as Breakdown
 ,coalesce(int.key,'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by coalesce(int.key,'Unknown'),
 case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown'  end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type and age group lower people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type; Age Group (Lower Level)' as Breakdown
 ,coalesce(int.key,'Unknown') as Level_1
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
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'                            
 group by coalesce(int.key,'Unknown')
 ,age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type, gender and age group lower people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type; Gender; Age Group (Lower Level)' as Breakdown
 ,coalesce(int.key,'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_2
 ,'NULL' as Level_2_description
 ,age_group_lower_chap7 as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by coalesce(int.key,'Unknown')
 ,case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown'  end
 ,age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence' as Breakdown
 ,case when ccg.ORG_CODE is null then 'Unknown'
       else ccg.ORG_CODE end as Level_1
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
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST ccg on p.IC_Rec_CCG = ccg.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when ccg.ORG_CODE is null then 'Unknown'
        else ccg.ORG_CODE end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and intervention type people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Intervention Type' as Breakdown
 ,case when ccg.ORG_CODE is null then 'Unknown'
       else ccg.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key,'Unknown') as Level_2
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
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when ccg.ORG_CODE is null then 'Unknown'
      else ccg.ORG_CODE end 
 ,coalesce(int.key,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region' as Breakdown
 ,case when Region_code IS Null then 'Unknown' else Region_code end as Level_1
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
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when Region_code IS Null then 'Unknown' else Region_code end 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and intervention type people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region; Intervention Type' as Breakdown
 ,case when Region_code IS Null then 'Unknown' else Region_code end as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
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
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when Region_code IS Null then 'Unknown' else Region_code end  
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP' as Breakdown
 ,case when STP_code IS null then 'Unknown' else STP_code end as Level_1
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
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when STP_code IS null then 'Unknown' else STP_code end 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership and intervention type people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP; Intervention Type' as Breakdown
 ,case when STP_code IS null then 'Unknown' else STP_code end as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
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
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when STP_code IS null then 'Unknown' else STP_code end 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---local authority district people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA' as Breakdown
 ,case when LEFT(LADistrictAuth,1) = 'S' then 'S'
 	  when LEFT(LADistrictAuth,1) = 'N' then 'N'
 	  when LEFT(LADistrictAuth,1) = 'W' then 'W'
 	  when LADistrictAuth is null then 'Unknown'
       when LADistrictAuth = '' then 'Unknown'
 	  else LADistrictAuth end as Level_1 
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
 group by 
 case when LEFT(LADistrictAuth,1) = 'S' then 'S'
      when LEFT(LADistrictAuth,1) = 'N' then 'N'
      when LEFT(LADistrictAuth,1) = 'W' then 'W'
      when LADistrictAuth is null then 'Unknown'
      when LADistrictAuth = '' then 'Unknown'
      else LADistrictAuth end 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---local authority district and intervention type people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA; Intervention Type' as Breakdown
 ,case when LEFT(LADistrictAuth,1) = 'S' then 'S'
 	  when LEFT(LADistrictAuth,1) = 'N' then 'N'
 	  when LEFT(LADistrictAuth,1) = 'W' then 'W'
 	  when LADistrictAuth is null then 'Unknown'
       when LADistrictAuth = '' then 'Unknown'
 	  else LADistrictAuth end as Level_1 
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when LEFT(LADistrictAuth,1) = 'S' then 'S'
 	 when LEFT(LADistrictAuth,1) = 'N' then 'N'
 	 when LEFT(LADistrictAuth,1) = 'W' then 'W'
 	 when LADistrictAuth is null then 'Unknown'
      when LADistrictAuth = '' then 'Unknown'
 	 else LADistrictAuth end 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider' as Breakdown
 ,res.OrgIDProv as Level_1
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
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and intervention type people subject to restrictive intervention count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Intervention Type' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7a' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---national restrictive interventions count
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
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
                             or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
                             or (res.Enddaterestrictiveint is null))
                             and res.uniqmonthid between '$month_id_start' and '$month_id_end'

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---gender restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender' as Breakdown
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_1
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
 group by 
 case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown'  end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Age Group (Lower Level)' as Breakdown
 ,age_group_lower_chap7 as Level_1
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
 group by age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher restrictive interventions count
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
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---gender and age group lower restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Gender; Age Group (Lower Level)' as Breakdown
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_1
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
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown'  end
 ,age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher restrictive interventions count
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
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower restrictive interventions count
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
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type' as Breakdown
 ,coalesce(int.key, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL'  as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type and gender restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type; Gender' as Breakdown
 ,coalesce(int.key, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by coalesce(int.key, 'Unknown'),
 case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown'  end

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type and age group lower restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type; Age Group (Lower Level)' as Breakdown
 ,coalesce(int.key, 'Unknown') as Level_1
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
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by coalesce(int.key, 'Unknown')
 ,age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---intervention type, gender and age group lower restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Intervention Type; Gender; Age Group (Lower Level)' as Breakdown
 ,coalesce(int.key, 'Unknown') as Level_1
 ,'NULL' as Level_1_description
 ,case when Gender = '1' then '1'
       when Gender = '2' then '2'
       else 'Unknown'  end  as Level_2
 ,'NULL' as Level_2_description
 ,age_group_lower_chap7 as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by coalesce(int.key, 'Unknown')
 ,case when Gender = '1' then '1' when Gender = '2' then '2' else 'Unknown' end
 ,age_group_lower_chap7

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence' as Breakdown
 ,case when b.ORG_CODE is null then 'Unknown'
       else b.ORG_CODE end as Level_1
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
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.MHB_RD_CCG_LATEST b on p.IC_Rec_CCG = b.ORG_CODE
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when b.ORG_CODE is null then 'Unknown'
      else b.ORG_CODE end 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and intervention type restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'CCG - Registration or Residence; Intervention Type' as Breakdown
 ,case when b.ORG_CODE is null then 'Unknown'
       else b.ORG_CODE end as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
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
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when b.ORG_CODE is null then 'Unknown'
      else b.ORG_CODE end 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region' as Breakdown
 ,case when Region_code IS Null then 'Unknown' else Region_code end as Level_1
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
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when Region_code IS Null then 'Unknown' else Region_code end 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and intervention type restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Commissioning Region; Intervention Type' as Breakdown
 ,case when Region_code IS Null then 'Unknown' else Region_code end as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
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
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when Region_code IS Null then 'Unknown' else Region_code end 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP' as Breakdown
 ,case when STP_code IS null then 'Unknown' else STP_code end as Level_1
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
 left join $db_output.CCG_final p on mpi.Person_id = p.Person_id
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when STP_code IS null then 'Unknown' else STP_code end 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership an intervention type restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'STP; Intervention Type' as Breakdown
 ,case when STP_code IS null then 'Unknown' else STP_code end as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
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
 left join $db_output.STP_Region_mapping as stp on p.IC_Rec_CCG = stp.CCG_code
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by case when STP_code IS null then 'Unknown' else STP_code end 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---local authority district restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA' as Breakdown
 ,case when LEFT(LADistrictAuth,1) = 'S' then 'S'
 	  when LEFT(LADistrictAuth,1) = 'N' then 'N'
 	  when LEFT(LADistrictAuth,1) = 'W' then 'W'
 	  when LADistrictAuth is null then 'Unknown'
       when LADistrictAuth = '' then 'Unknown'
 	  else LADistrictAuth end as Level_1 
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
 group by 
 case when LEFT(LADistrictAuth,1) = 'S' then 'S'
 	 when LEFT(LADistrictAuth,1) = 'N' then 'N'
 	 when LEFT(LADistrictAuth,1) = 'W' then 'W'
 	 when LADistrictAuth is null then 'Unknown'
      when LADistrictAuth = '' then 'Unknown'
 	 else LADistrictAuth end 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---local authority district and intervention type restrictive interventions count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'LAD/UA; Intervention Type' as Breakdown
 ,case when LEFT(LADistrictAuth,1) = 'S' then 'S'
 	  when LEFT(LADistrictAuth,1) = 'N' then 'N'
 	  when LEFT(LADistrictAuth,1) = 'W' then 'W'
 	  when LADistrictAuth is null then 'Unknown'
       when LADistrictAuth = '' then 'Unknown'
 	  else LADistrictAuth end as Level_1 
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by 
 case when LEFT(LADistrictAuth,1) = 'S' then 'S'
 	 when LEFT(LADistrictAuth,1) = 'N' then 'N'
 	 when LEFT(LADistrictAuth,1) = 'W' then 'W'
 	 when LADistrictAuth is null then 'Unknown'
      when LADistrictAuth = '' then 'Unknown'
 	 else LADistrictAuth end 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider restrictive interventions count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider' as Breakdown
 ,res.OrgIDProv as Level_1
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
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv 

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and intervention type restrictive interventions count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Provider; Intervention Type' as Breakdown
 ,res.OrgIDProv as Level_1
 ,'NULL' as Level_1_description
 ,coalesce(int.key, 'Unknown') as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7b' as Metric
 ,COUNT(distinct res.MHS505UniqID) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi_PROV as mpi on res.Person_id = mpi.Person_id and res.OrgIDProv = mpi.OrgIDProv
 left join menh_publications.RestrictiveIntTypeDim_Extended as int on res.restrictiveinttype = int.key
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 group by res.OrgIDProv 
 ,coalesce(int.key, 'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---national people subject to restrictive intervention where age, gender and ethnicity are known count
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
 ,'7c' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 and Gender in ('1','2')
 and AgeRepPeriodEnd is not null
 and NHSDEthnicity is not null 
 and NHSDEthnicity not in ('-1','99','Z')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher people subject to restrictive intervention where age, gender and ethnicity are known count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Higher Level)' as Breakdown
 ,coalesce(b.Sub_Ethnicity, 'Not Known') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7c' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.RD_Ethnicity b on mpi.NHSDEthnicity = b.code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null)) 
 and res.uniqmonthid between '$month_id_start' and '$month_id_end' 
 and Gender in ('1','2')
 and AgeRepPeriodEnd is not null
 and NHSDEthnicity is not null 
 and NHSDEthnicity not in ('-1','99','Z')
 group by coalesce(b.Sub_Ethnicity, 'Not Known')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower people subject to restrictive intervention where age, gender and ethnicity are known count
 select 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Lower Level)' as Breakdown
 ,coalesce(b.code, '99') as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7c' as Metric
 ,COUNT(distinct res.Person_id) as Metric_value
 from $db_output.MHB_MHS505RestrictiveIntervention as res
 left join $db_output.mpi as mpi on res.Person_id = mpi.Person_id
 left join $db_output.RD_Ethnicity b on mpi.NHSDEthnicity = b.code
 where ((res.Startdaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate') 
 or (res.Enddaterestrictiveint BETWEEN '$rp_startdate' AND '$rp_enddate')
 or (res.Enddaterestrictiveint is null))
 and res.uniqmonthid between '$month_id_start' and '$month_id_end'
 and Gender in ('1','2')
 and AgeRepPeriodEnd is not null
 and NHSDEthnicity is not null 
 and NHSDEthnicity not in ('-1','99','Z')
 group by coalesce(b.code, '99')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---national people subject to restrictive intervention where age, gender and ethnicity are known crude rate
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
 ,'7d' as Metric
 ,(cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000) as Metric_value
 from $db_output.Output1 a
 left join global_temp.1m_england b on a.Breakdown = b.Breakdown 
 where a.metric = '7c'
 and a.breakdown = 'England'

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher people subject to restrictive intervention where age, gender and ethnicity are known crude rate
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
 ,'7d' as Metric
 ,(cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000) as Metric_value
 from $db_output.Output1 a
 left join global_temp.1m_sub_ethnicity b on a.Breakdown = b.Breakdown and a.Level_1 = b.Level_1 and b.metric = '1m'
 where a.metric = '7c'
 and a.breakdown = 'Ethnicity (Higher Level)'
 and a.level_1 in ('Mixed','White','Other Ethnic Groups','Black or Black British','Asian or Asian British')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower people subject to restrictive intervention where age, gender and ethnicity are known crude rate
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
 ,'7d' as Metric
 ,(cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000) as Metric_value
 from $db_output.Output1 a
 left join global_temp.1m_ethnicity b on a.Breakdown = b.Breakdown and a.Level_1 = b.Level_1 and b.metric = '1m'
 where a.metric = '7c'
 and a.breakdown = 'Ethnicity (Lower Level)'
 and (a.level_1_description not in ('Not Known','Not Stated') or a.level_1_description is not null)

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher people subject to restrictive intervention where age, gender and ethnicity are known standardised rate
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
 ,'7e' as Metric
 ,STANDARDISED_RATE_PER_100000
 from global_temp.standardisation_Restraint a
 where Ethnic_level = 'Sub_ethnicity'

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower people subject to restrictive intervention where age, gender and ethnicity are known standardised rate
 select  
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Lower Level)' as Breakdown
 ,b.code as Level_1
 ,EthnicGroup as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7e' as Metric
 ,STANDARDISED_RATE_PER_100000
 from global_temp.standardisation_Restraint a
 left join $db_output.RD_Ethnicity b on a.EthnicGroup = b.ethnicity
 where Ethnic_level = 'Lower_Ethnicity'

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher people subject to restrictive intervention where age, gender and ethnicity are known confidence interval
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
 ,'7f' as Metric
 ,CONFIDENCE_INTERVAL_95
 from global_temp.standardisation_Restraint a
 where Ethnic_level = 'Sub_ethnicity'

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower people subject to restrictive intervention where age, gender and ethnicity are known confidence interval
 select  
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Ethnicity (Lower Level)' as Breakdown
 ,b.code as Level_1
 ,EthnicGroup as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'7f' as Metric
 ,CONFIDENCE_INTERVAL_95
 from global_temp.standardisation_Restraint a
 left join $db_output.RD_Ethnicity b on a.EthnicGroup = b.ethnicity
 where Ethnic_level = 'Lower_Ethnicity'