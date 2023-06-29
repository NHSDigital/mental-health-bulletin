# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---national care contacts count
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
 ,'6a' as Metric
 ,Count(MHS201UniqID) as Metric_value
 from $db_output.MHB_MHS201CareContact

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---attendance code care contacts count
 select 
 distinct 
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,'Attendance Code' as Breakdown
 ,case when AttendOrDNACode IN ('2','3','4','5','6','7') then AttendOrDNACode else 'Unknown' end as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6a' as Metric
 ,Count(distinct MHS201UniqID) as Metric_value
 from $db_output.MHB_MHS201CareContact a
 group by 
 case when AttendOrDNACode IN ('2','3','4','5','6','7') then AttendOrDNACode else 'Unknown' end

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type care contacts count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,*
 from global_temp.6a_teamtype

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider care contacts count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,*
 from global_temp.6a_provider

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and service type care contacts count
 select
 '$rp_startdate' as REPORTING_PERIOD_START
 ,'$rp_enddate' as REPORTING_PERIOD_END
 ,'$status' as STATUS
 ,*
 from global_temp.6a_provider_teamtype

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---attendance code and service type care contacts count
 select 
               '$rp_startdate' as REPORTING_PERIOD_START
               ,'$rp_enddate' as REPORTING_PERIOD_END
               ,'$status' as STATUS
               ,'Attendance Code; Service or Team Type' as Breakdown
               ,case when AttendOrDNACode IN ('2','3','4','5','6','7') then AttendOrDNACode else 'Unknown' end as Level_1
               ,'NULL' as Level_1_description
               ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
                           ,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
                           ,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end as Level_2
               ,'NULL' as Level_2_description
               ,'NULL' as Level_3
               ,'NULL' as Level_3_description
               ,'NULL' as Level_4
               ,'NULL' as Level_4_description
               ,'6a' as Metric
               ,Count(MHS201UniqID) as Metric_value             
 from          $db_output.MHB_MHS201CareContact a
 left join     $db_output.MHB_MHS102ServiceTypeReferredTo b on a.UniqServReqID = b.UniqServReqID and a.UniqCareProfTeamID = b.UniqCareProfTeamID
 left join     $ref_database.hes_op_attended as d on a.AttendOrDNACode = d.HES_OP_ATTENDED_CODE
 group by      case when AttendOrDNACode IN ('2','3','4','5','6','7') then AttendOrDNACode else 'Unknown' end
              ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
                         ,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
                         ,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end 

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national attended care contacts proportion
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
 ,'6b' as Metric
 ,cast((select Count(MHS201UniqID) from $db_output.MHB_MHS201CareContact where AttendOrDNACode in ('5','6')) as float) / cast(Count(MHS201UniqID) as float) * 100 as Metric_value
 from $db_output.MHB_MHS201CareContact

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type breakdowns for attended care contacts proportion
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
 ,'6b' as Metric
 ,(cast( b.Metric_value as float) / cast(a.Metric_value as float)) * 100 as Metric_value
 from global_temp.6a_teamtype a
 left join global_temp.6b_teamtype b on a.Level_1 = b.Level_1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider breakdowns for attended care contacts proportion
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
 ,'6b' as Metric
 ,(cast( b.Metric_value as float) / cast(a.Metric_value as float)) * 100 as Metric_value
 from global_temp.6a_provider a
 left join global_temp.6b_provider b on a.Level_1 = b.Level_1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and service type breakdowns for attended care contacts proportion
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
 ,'6b' as Metric
 ,(cast( b.Metric_value as float) / cast(a.Metric_value as float)) * 100 as Metric_value
 from global_temp.6a_provider_teamtype a
 left join $db_output.6b_provider_teamtype b on a.Level_1 = b.Level_1 and a.Level_2 = b.Level_2

# COMMAND ----------

# DBTITLE 0,England
 %sql
 INSERT INTO $db_output.output1
 ---national non-attended care contacts proportion
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
 ,'6c' as Metric
 ,cast((select Count(MHS201UniqID) from $db_output.MHB_MHS201CareContact where AttendOrDNACode in ('7','2','3')) as float) / cast(Count(MHS201UniqID) as float) * 100 as Metric_value
 from $db_output.MHB_MHS201CareContact

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type breakdowns for non-attended care contacts proportion
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
 ,'6c' as Metric
 ,(cast( b.Metric_value as float) / cast(a.Metric_value as float)) * 100 as Metric_value
 from global_temp.6a_teamtype a
 left join global_temp.6c_teamtype b on a.Level_1 = b.Level_1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national provider cancelled care contacts proportion
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
 ,'6d' as Metric
 ,cast((select Count(MHS201UniqID) from $db_output.MHB_MHS201CareContact where AttendOrDNACode = '4') as float) / cast(Count(MHS201UniqID) as float) * 100 as Metric_value
 from $db_output.MHB_MHS201CareContact

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type breakdowns for provider cancelled attended care contacts proportion
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
 ,'6d' as Metric
 ,(cast( b.Metric_value as float) / cast(a.Metric_value as float)) * 100 as Metric_value
 from global_temp.6a_teamtype a
 left join global_temp.6d_teamtype b on a.Level_1 = b.Level_1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national invalid or missing attendance code care contacts proportion
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
 ,'6e' as Metric
 ,cast((select COUNT(MHS201UniqID) from $db_output.MHB_MHS201CareContact where (AttendOrDNACode not in ('4','2','3','5','6','7') or AttendOrDNACode is null)) as float) / cast(Count(MHS201UniqID) as float) * 100 as Metric_value
 from $db_output.MHB_MHS201CareContact

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---service type invalid or missing attendance code care contacts proportion
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
 ,'6e' as Metric
 ,(cast( b.Metric_value as float) / cast(a.Metric_value as float)) * 100 as Metric_value
 from global_temp.6a_teamtype a
 left join global_temp.6e_teamtype b on a.Level_1 = b.Level_1