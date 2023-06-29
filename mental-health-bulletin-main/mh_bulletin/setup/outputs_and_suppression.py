# Databricks notebook source
 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric);

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output_v2;
 CREATE TABLE         $db_output.output_v2 USING DELTA AS
 ---Change all NULL cells to NONE in aggregate output
 select Breakdown
       ,level_1
       ,case when level_2 = 'NULL' then 'NONE' else level_2 end as level_2
       ,case when level_3 = 'NULL' then 'NONE' else level_3 end as level_3
       ,metric
       ,metric_value
 from  $db_output.output1;
 
 OPTIMIZE $db_output.output_v2;

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output_unsuppressed;
 CREATE TABLE         $db_output.output_unsuppressed USING DELTA AS
 ---add counts and rates onto the csv_master reference data
 select distinct a.*
                 ,coalesce(b.metric_value, 0) as METRIC_VALUE
 from $db_output.csv_master a
 left join $db_output.output_v2 b
       on a.breakdown = b.breakdown 
       and a.level_one = b.level_1
       and a.level_two = b.level_2 
       and a.level_three = b.level_3 
       and a.metric = b.metric
 OPTIMIZE $db_output.output_unsuppressed;

# COMMAND ----------

 %sql
 ---count metric suppression
 DROP TABLE IF EXISTS $db_output.output_v2_suppressed_pt1;
 CREATE TABLE         $db_output.output_v2_suppressed_pt1 USING DELTA AS
 select 
 REPORTING_PERIOD_START 
 ,REPORTING_PERIOD_END
 ,STATUS
 ,BREAKDOWN
 ,LEVEL_ONE
 ,LEVEL_ONE_DESCRIPTION
 ,LEVEL_TWO
 ,LEVEL_TWO_DESCRIPTION
 ,LEVEL_THREE
 ,LEVEL_THREE_DESCRIPTION
 ,METRIC
 ,cast(case when cast(Metric_value as float) < 5 then '9999999' else cast(round(metric_value/5,0)*5 as float) end as string) as  METRIC_VALUE
 from $db_output.output_unsuppressed
 where BREAKDOWN in ('CCG - Registration or Residence'
 						    ,'AMH Cluster Super Class; CCG - Registration or Residence'
 							,'LAD/UA'
 							,'LAD/UA; Age Group (Higher Level)'
 							,'LAD/UA; Intervention Type'
 							,'AMH Cluster Super Class; LAD/UA'
 							,'Provider'
                             ,'Provider; Age Group (Higher Level)'
                             ,'Provider; Age Group (Lower Level)'
                             ,'Provider; Gender'
                             ,'Provider; Ethnicity (Higher Level)'
                             ,'Provider; IMD Quintiles'
                             ,'Provider; Service Type Group'
                             ,'Provider; Bed Type Group'
 							,'STP; Age Group (Higher Level)'							
 							,'Commissioning Region'
 							,'Commissioning Region; Age Group (Higher Level)'
 							,'STP'
 							,'AMH Cluster Super Class; Commissioning Region'
 							,'AMH Cluster Super Class; STP'
 							,'Provider; Service or Team Type'
 							,'AMH Cluster Super Class; Provider'
 							,'STP; Intervention Type'
 							,'Provider; Intervention Type'
 							,'Commissioning Region; Intervention Type'
 							,'CCG - Registration or Residence; Gender'
                             ,'CCG - Registration or Residence; Age Group (Higher Level)'
                             ,'CCG - Registration or Residence; Age Group (Lower Level)'
 							,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                             ,'CCG - Registration or Residence; IMD'
 							,'CCG - Registration or Residence; IMD Quintiles'
                             ,'CCG - Registration or Residence; Intervention Type'
 							,'CCG - Registration or Residence; Service Type Group'
                             ,'CCG - Registration or Residence; Bed Type Group'
                             ,'CCG - Registration or Residence; Gender; Age Group (Lower Level)'
                             ,'CCG of Residence'
 						    ,'CCG of Residence; Gender'
                             ,'CCG of Residence; Age Group (Higher Level)'
                             ,'CCG of Residence; Age Group (Lower Level)'
 							,'CCG of Residence; Ethnicity (Higher Level)'
 							,'CCG of Residence; Ethnicity (Lower Level)'
                             ,'CCG of Residence; IMD'
                             ,'Provider; Ethnicity (Lower Level)'
                             ,'Provider; IMD'
                             ,'STP; Gender'
                             ,'STP; Age Group (Higher Level)'
                             ,'STP; Age Group (Lower Level)'
 							,'STP; Ethnicity (Higher Level)'
 							,'STP; Ethnicity (Lower Level)'
                             ,'STP; IMD'
                             ,'Region'
                             ,'Region; Gender'
                             ,'Region; Age Group (Higher Level)'
                             ,'Region; Age Group (Lower Level)'
 							,'Region; Ethnicity (Higher Level)'
 							,'Region; Ethnicity (Lower Level)'
                             ,'Region; IMD'
                             ,'Commissioning Region'
                             ,'Commissioning Region; Gender'
                             ,'Commissioning Region; Age Group (Higher Level)'
                             ,'Commissioning Region; Age Group (Lower Level)'
 							,'Commissioning Region; Ethnicity (Higher Level)'
 							,'Commissioning Region; Ethnicity (Lower Level)'
                             ,'Commissioning Region; IMD')
 
 and metric not in ('1e','1d','1g',
                    '5c',
                    '6b','6c','6d','6e',
                    '10c','10g','10h','10i','10j','10k',
                    '12c',
                    '14b',
                    '15d','15e',
                    '16g','16h','16i','16j','16k','16l',
                    '17b')
 
 union all
 --adding in breakdowns which don't need to be suppressed
 select 
 REPORTING_PERIOD_START 
 ,REPORTING_PERIOD_END
 ,STATUS
 ,BREAKDOWN
 ,LEVEL_ONE
 ,LEVEL_ONE_DESCRIPTION
 ,LEVEL_TWO
 ,LEVEL_TWO_DESCRIPTION
 ,LEVEL_THREE
 ,LEVEL_THREE_DESCRIPTION
 ,METRIC
 ,metric_value
 from $db_output.output_unsuppressed
 where BREAKDOWN not in ('CCG - Registration or Residence'
 						    ,'CCG - Registration or Residence; Age Group (Higher Level)'
 							,'CCG - Registration or Residence; Age Group (Lower Level)'
 							,'CCG - Registration or Residence; Intervention Type'
 							,'AMH Cluster Super Class; CCG - Registration or Residence'
 							,'LAD/UA'
 							,'LAD/UA; Age Group (Higher Level)'
 							,'LAD/UA; Intervention Type'
 							,'AMH Cluster Super Class; LAD/UA'
 							,'Provider; Age Group (Higher Level)'
                             ,'Provider'
                             ,'Provider; Age Group (Higher Level)'
                             ,'Provider; Age Group (Lower Level)'
                             ,'Provider; Gender'
                             ,'Provider; Ethnicity (Higher Level)'
                             ,'Provider; IMD Quintiles'
                             ,'Provider; Service Type Group'
                             ,'Provider; Bed Type Group'
 							,'STP; Age Group (Higher Level)'							
 							,'Commissioning Region'
 							,'Commissioning Region; Age Group (Higher Level)'
 							,'STP'
 							,'AMH Cluster Super Class; Commissioning Region'
 							,'AMH Cluster Super Class; STP'
 							,'Provider; Service or Team Type'
 							,'AMH Cluster Super Class; Provider'
 							,'STP; Intervention Type'
 							,'Provider; Intervention Type'
 							,'Commissioning Region; Intervention Type'
 							,'CCG - Registration or Residence; Gender'
                             ,'CCG - Registration or Residence; Age Group (Higher Level)'
                             ,'CCG - Registration or Residence; Age Group (Lower Level)'
 							,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                             ,'CCG - Registration or Residence; IMD'
 							,'CCG - Registration or Residence; IMD Quintiles'
                             ,'CCG - Registration or Residence; Service Type Group'
                             ,'CCG - Registration or Residence; Bed Type Group'
                             ,'CCG - Registration or Residence; Gender; Age Group (Lower Level)'
                             ,'CCG of Residence'
 						    ,'CCG of Residence; Gender'
                             ,'CCG of Residence; Age Group (Higher Level)'
                             ,'CCG of Residence; Age Group (Lower Level)'
 							,'CCG of Residence; Ethnicity (Higher Level)'
 							,'CCG of Residence; Ethnicity (Lower Level)'
                             ,'CCG of Residence; IMD'
                             ,'Provider; Ethnicity (Lower Level)'
                             ,'Provider; IMD'
                             ,'STP; Gender'
                             ,'STP; Age Group (Higher Level)'
                             ,'STP; Age Group (Lower Level)'
 							,'STP; Ethnicity (Higher Level)'
 							,'STP; Ethnicity (Lower Level)'
                             ,'STP; IMD'
                             ,'Region'
                             ,'Region; Gender'
                             ,'Region; Age Group (Higher Level)'
                             ,'Region; Age Group (Lower Level)'
 							,'Region; Ethnicity (Higher Level)'
 							,'Region; Ethnicity (Lower Level)'
                             ,'Region; IMD'
                             ,'Commissioning Region'
                             ,'Commissioning Region; Gender'
                             ,'Commissioning Region; Age Group (Higher Level)'
                             ,'Commissioning Region; Age Group (Lower Level)'
 							,'Commissioning Region; Ethnicity (Higher Level)'
 							,'Commissioning Region; Ethnicity (Lower Level)'
                             ,'Commissioning Region; IMD')
                             
  union all
 --1e suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC = '1a' and b.METRIC_VALUE <5 then '9999999' 
 	  when c.METRIC = '1c' and c.METRIC_VALUE <5 then '9999999' else cast(round(a.metric_value,0) as float) end as Metric_Value
 from (   select *
          from $db_output.output_unsuppressed a
          where a.BREAKDOWN in ('CCG - Registration or Residence'
                                       ,'CCG - Registration or Residence; Age Group (Higher Level)'
                                       ,'CCG - Registration or Residence; Age Group (Lower Level)'
                                       ,'CCG - Registration or Residence; Intervention Type'
                                       ,'AMH Cluster Super Class; CCG - Registration or Residence'
                                       ,'LAD/UA'
                                       ,'LAD/UA; Age Group (Higher Level)'
                                       ,'LAD/UA; Intervention Type'
                                       ,'AMH Cluster Super Class; LAD/UA'
                                       ,'Provider; Age Group (Higher Level)'
                                       ,'Provider'
                                       ,'Provider; Age Group (Higher Level)'
                                       ,'Provider; Age Group (Lower Level)'
                                       ,'Provider; Gender'
                                       ,'Provider; Ethnicity (Higher Level)'
                                       ,'Provider; IMD Quintiles'
                                       ,'Provider; Service Type Group'
                                       ,'Provider; Bed Type Group'
                                       ,'STP; Age Group (Higher Level)'							
                                       ,'Commissioning Region'
                                       ,'Commissioning Region; Age Group (Higher Level)'
                                       ,'STP'
                                       ,'AMH Cluster Super Class; Commissioning Region'
                                       ,'AMH Cluster Super Class; STP'
                                       ,'Provider; Service or Team Type'
                                       ,'AMH Cluster Super Class; Provider'
                                       ,'STP; Intervention Type'
                                       ,'Provider; Intervention Type'
                                       ,'Commissioning Region; Intervention Type'
                                       ,'CCG - Registration or Residence; Gender'
                                       ,'CCG - Registration or Residence; Age Group (Higher Level)'
                                       ,'CCG - Registration or Residence; Age Group (Lower Level)'
                                       ,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                                       ,'CCG - Registration or Residence; IMD'
                                       ,'CCG - Registration or Residence; IMD Quintiles'
                                       ,'CCG - Registration or Residence; Service Type Group'
                                       ,'CCG - Registration or Residence; Bed Type Group') 
           and a.METRIC = '1e') a
 left join ( select * from $db_output.output_unsuppressed where metric = '1a') b on a.BREAKDOWN = b.BREAKDOWN 
                                                     and a.LEVEL_ONE = b.LEVEL_ONE 
                                                     and a.LEVEL_TWO = b.LEVEL_TWO 
                                                     and a.LEVEL_THREE = b.LEVEL_THREE 
                                                     ----metric changed from 1d to 1e for MHB part 2
 left join ( select * from $db_output.output_unsuppressed where metric = '1c') c on a.BREAKDOWN = c.BREAKDOWN
                                                     and a.LEVEL_ONE = c.LEVEL_ONE 
                                                     and a.LEVEL_TWO = c.LEVEL_TWO 
                                                     and a.LEVEL_THREE = c.LEVEL_THREE 
                                                     ----metric changed from 1d to 1e for MHB part 2
 
 --1d suppression
 union all
 
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC = '1a' and b.METRIC_VALUE <5 then '9999999' 
 	  when c.METRIC = '1b' and c.METRIC_VALUE <5 then '9999999' else cast(round(a.metric_value,0) as float) end as Metric_Value
 from (   select *
          from $db_output.output_unsuppressed a
          where a.BREAKDOWN in ('CCG - Registration or Residence'
                                       ,'CCG - Registration or Residence; Age Group (Higher Level)'
                                       ,'CCG - Registration or Residence; Age Group (Lower Level)'
                                       ,'CCG - Registration or Residence; Intervention Type'
                                       ,'AMH Cluster Super Class; CCG - Registration or Residence'
                                       ,'LAD/UA'
                                       ,'LAD/UA; Age Group (Higher Level)'
                                       ,'LAD/UA; Intervention Type'
                                       ,'AMH Cluster Super Class; LAD/UA'
                                       ,'Provider; Age Group (Higher Level)'
                                       ,'Provider'
                                       ,'Provider; Age Group (Higher Level)'
                                       ,'Provider; Age Group (Lower Level)'
                                       ,'Provider; Gender'
                                       ,'Provider; Ethnicity (Higher Level)'
                                       ,'Provider; IMD Quintiles'
                                       ,'Provider; Service Type Group'
                                       ,'Provider; Bed Type Group'
                                       ,'STP; Age Group (Higher Level)'							
                                       ,'Commissioning Region'
                                       ,'Commissioning Region; Age Group (Higher Level)'
                                       ,'STP'
                                       ,'AMH Cluster Super Class; Commissioning Region'
                                       ,'AMH Cluster Super Class; STP'
                                       ,'Provider; Service or Team Type'
                                       ,'AMH Cluster Super Class; Provider'
                                       ,'STP; Intervention Type'
                                       ,'Provider; Intervention Type'
                                       ,'Commissioning Region; Intervention Type'
                                       ,'CCG - Registration or Residence; Gender'
                                       ,'CCG - Registration or Residence; Age Group (Higher Level)'
                                       ,'CCG - Registration or Residence; Age Group (Lower Level)'
                                       ,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                                       ,'CCG - Registration or Residence; IMD'
                                       ,'CCG - Registration or Residence; IMD Quintiles'
                                       ,'CCG - Registration or Residence; Service Type Group'
                                       ,'CCG - Registration or Residence; Bed Type Group') 
           and a.METRIC = '1d') a
 left join ( select * from $db_output.output_unsuppressed where metric = '1a') b on a.BREAKDOWN = b.BREAKDOWN 
                                                     and a.LEVEL_ONE = b.LEVEL_ONE 
                                                     and a.LEVEL_TWO = b.LEVEL_TWO 
                                                     and a.LEVEL_THREE = b.LEVEL_THREE 
 left join ( select * from $db_output.output_unsuppressed where metric = '1b') c on a.BREAKDOWN = c.BREAKDOWN
                                                     and a.LEVEL_ONE = c.LEVEL_ONE 
                                                     and a.LEVEL_TWO = c.LEVEL_TWO 
                                                     and a.LEVEL_THREE = c.LEVEL_THREE 
 
 union all
 ---10c suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC = '10b' and b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join $db_output.output_unsuppressed b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE 
 						and b.METRIC = '10b'	 
 where a.METRIC = '10c'
 and a.BREAKDOWN in ('Provider; Age Group (Higher Level)'
                     ,'Provider; Gender'
                     ,'Provider; Ethnicity (Higher Level)'
                     ,'Provider; IMD Quintiles'
                     ,'CCG - Registration or Residence; Gender'
                     ,'CCG - Registration or Residence; Age Group (Higher Level)'
 					,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                     ,'CCG - Registration or Residence; IMD Quintiles'
                     ,'LAD/UA'
                     ,'CCG - Registration or Residence'
                     ,'STP'
                     ,'Provider'
                     ,'Commissioning Region')
 
 union all
 ---10g-k suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where metric = '10a') b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE
 where a.METRIC = '10g'
 and a.BREAKDOWN in ('Provider; Age Group (Higher Level)'
                     ,'Provider; Gender'
                     ,'Provider; Ethnicity (Higher Level)'
                     ,'Provider; IMD Quintiles'
                     ,'CCG - Registration or Residence; Gender'
                     ,'CCG - Registration or Residence; Age Group (Higher Level)'
 					,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                     ,'CCG - Registration or Residence; IMD Quintiles'
                     ,'LAD/UA'
                     ,'CCG - Registration or Residence'
                     ,'STP'
                     ,'Provider'
                     ,'Commissioning Region')
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where metric = '10b') b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE
 where a.METRIC = '10h'
 and a.BREAKDOWN in ('Provider; Age Group (Higher Level)'
                     ,'Provider; Gender'
                     ,'Provider; Ethnicity (Higher Level)'
                     ,'Provider; IMD Quintiles'
                     ,'CCG - Registration or Residence; Gender'
                     ,'CCG - Registration or Residence; Age Group (Higher Level)'
 					,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                     ,'CCG - Registration or Residence; IMD Quintiles'
                     ,'LAD/UA'
                     ,'CCG - Registration or Residence'
                     ,'STP'
                     ,'Provider'
                     ,'Commissioning Region')
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where metric = '10d') b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE
 where a.METRIC = '10i'
 and a.BREAKDOWN in ('Provider; Age Group (Higher Level)'
                     ,'Provider; Gender'
                     ,'Provider; Ethnicity (Higher Level)'
                     ,'Provider; IMD Quintiles'
                     ,'CCG - Registration or Residence; Gender'
                     ,'CCG - Registration or Residence; Age Group (Higher Level)'
 					,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                     ,'CCG - Registration or Residence; IMD Quintiles'
                     ,'LAD/UA'
                     ,'CCG - Registration or Residence'
                     ,'STP'
                     ,'Provider'
                     ,'Commissioning Region')
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where metric = '10e') b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE
 where a.METRIC = '10j'
 and a.BREAKDOWN in ('Provider; Age Group (Higher Level)'
                     ,'Provider; Gender'
                     ,'Provider; Ethnicity (Higher Level)'
                     ,'Provider; IMD Quintiles'
                     ,'CCG - Registration or Residence; Gender'
                     ,'CCG - Registration or Residence; Age Group (Higher Level)'
 					,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                     ,'CCG - Registration or Residence; IMD Quintiles'
                     ,'LAD/UA'
                     ,'CCG - Registration or Residence'
                     ,'STP'
                     ,'Provider'
                     ,'Commissioning Region')
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where metric = '10f') b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE
 where a.METRIC = '10k'
 and a.BREAKDOWN in ('Provider; Age Group (Higher Level)'
                     ,'Provider; Gender'
                     ,'Provider; Ethnicity (Higher Level)'
                     ,'Provider; IMD Quintiles'
                     ,'CCG - Registration or Residence; Gender'
                     ,'CCG - Registration or Residence; Age Group (Higher Level)'
 					,'CCG - Registration or Residence; Ethnicity (Higher Level)'
                     ,'CCG - Registration or Residence; IMD Quintiles'
                     ,'LAD/UA'
                     ,'CCG - Registration or Residence'
                     ,'STP'
                     ,'Provider'
                     ,'Commissioning Region')
   
                     
 union all
 
 ---6c suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC = '6a' and b.METRIC_VALUE <5 then '9999999' 
 	  when c.METRIC_VALUE <5 then '9999999' else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join $db_output.output_unsuppressed b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE 
 						and a.METRIC = '6b'
 						and b.METRIC = '6a'
 left join $db_output.6b_provider_teamtype c on a.BREAKDOWN = c.Breakdown 
 						and a.LEVEL_ONE = c.LEVEL_1
 						and a.LEVEL_TWO = c.LEVEL_2
 						--and a.LEVEL_THREE = c.LEVEL_3
 						and a.METRIC = '6b'
 where a.breakdown in ('Provider; Service or Team Type', 'Provider', 'LAD/UA','CCG - Registration or Residence','STP','Provider', 'Commissioning Region')					 
 and a.METRIC = '6b'
 
 
 union all
 ---5c suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC = '4a' and b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,1) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join $db_output.output_unsuppressed b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE 
 						and b.METRIC = '4a'	 
 where a.METRIC = '5c'
 and a.breakdown in ('LAD/UA','CCG - Registration or Residence','STP','Provider', 'Commissioning Region')
 
 union all
 ---12c suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC = '12b' and b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join $db_output.output_unsuppressed b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE 
 						and b.METRIC = '12b'	 
 where a.METRIC = '12c'
 and a.breakdown in ('CCG - Registration or Residence',
                     'CCG - Registration or Residence; Age Group (Higher Level)',
                     'CCG - Registration or Residence; Ethnicity (Higher Level)',
                     'CCG - Registration or Residence; Gender',
                     'CCG - Registration or Residence; IMD Quintiles',
                     'Commissioning Region',
                     'LAD/UA',
                     'Provider', 
                     'Provider; Age Group (Higher Level)',
                     'Provider; Ethnicity (Higher Level)', 
                     'Provider; Gender',
                     'Provider; IMD Quintiles',
                     'STP')
 
 
 union all
 ---14b suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '14a')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('14b')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         )
 
 union all
 ---15d&e suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '15b')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE
 where a.METRIC in ('15d')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'CCG of Residence; Gender'
                         ,'CCG of Residence; Age Group (Higher Level)'
                         ,'CCG of Residence; Age Group (Lower Level)'
                         ,'CCG of Residence; Ethnicity (Higher Level)'
                         ,'CCG of Residence; Ethnicity (Lower Level)'
                         ,'CCG of Residence; IMD'
                         ,'Provider'
                         ,'Provider; Gender'
                         ,'Provider; Age Group (Higher Level)'
                         ,'Provider; Age Group (Lower Level)'
                         ,'Provider; Ethnicity (Higher Level)'
                         ,'Provider; Ethnicity (Lower Level)'
                         ,'Provider; IMD'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         ,'Commissioning Region; Gender'
                         ,'Commissioning Region; Age Group (Higher Level)'
                         ,'Commissioning Region; Age Group (Lower Level)'
                         ,'Commissioning Region; Ethnicity (Higher Level)'
                         ,'Commissioning Region; Ethnicity (Lower Level)'
                         ,'Commissioning Region; IMD'
                         )
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '15c')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE
 where a.METRIC in ('15e')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'CCG of Residence; Gender'
                         ,'CCG of Residence; Age Group (Higher Level)'
                         ,'CCG of Residence; Age Group (Lower Level)'
                         ,'CCG of Residence; Ethnicity (Higher Level)'
                         ,'CCG of Residence; Ethnicity (Lower Level)'
                         ,'CCG of Residence; IMD'
                         ,'Provider'
                         ,'Provider; Gender'
                         ,'Provider; Age Group (Higher Level)'
                         ,'Provider; Age Group (Lower Level)'
                         ,'Provider; Ethnicity (Higher Level)'
                         ,'Provider; Ethnicity (Lower Level)'
                         ,'Provider; IMD'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         ,'Commissioning Region; Gender'
                         ,'Commissioning Region; Age Group (Higher Level)'
                         ,'Commissioning Region; Age Group (Lower Level)'
                         ,'Commissioning Region; Ethnicity (Higher Level)'
                         ,'Commissioning Region; Ethnicity (Lower Level)'
                         ,'Commissioning Region; IMD'
                         )
 
 union all
 ---16g-l suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '16a')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('16g')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         )
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '16b')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('16h')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         )
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '16c')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('16i')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         )
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '16d')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('16j')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         )
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '16e')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('16k')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         )
 
 union all
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '16f')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('16l')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         )
 
 union all
 ---17b suppression
 select 
 a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,case when b.METRIC_VALUE < 5 then '9999999' 
 	  else cast(round(a.metric_value,0) as float) end as Metric_Value
 from $db_output.output_unsuppressed a
 left join (select * from $db_output.output_unsuppressed where Metric = '17a')  b on a.BREAKDOWN = b.Breakdown 
 						and a.LEVEL_ONE = b.LEVEL_ONE 
 						and a.LEVEL_TWO = b.LEVEL_TWO 
 						and a.LEVEL_THREE = b.LEVEL_THREE  
 where a.METRIC in ('17b')
   and a.BREAKDOWN in (   'CCG of Residence'
                         ,'STP'
                         ,'STP; Gender'
                         ,'STP; Age Group (Higher Level)'
                         ,'STP; Age Group (Lower Level)'
                         ,'STP; Ethnicity (Higher Level)'
                         ,'STP; Ethnicity (Lower Level)'
                         ,'STP; IMD'
                         ,'Commissioning Region'
                         );
 
 OPTIMIZE $db_output.output_v2_suppressed_pt1;

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")

# COMMAND ----------

mhb_unsup_row_df = spark.sql(f"SELECT COUNT(distinct *) FROM {db_output}.output_unsuppressed")
mhb_unsup_row_val = mhb_unsup_row_df.collect()[0][0]

mhb_sup_row_df = spark.sql(f"SELECT COUNT(distinct *) FROM {db_output}.output_v2_suppressed_pt1")
mhb_sup_row_val = mhb_sup_row_df.collect()[0][0]

#assert rows in unsuppressed output and suppressed output are equal
assert mhb_unsup_row_val == mhb_sup_row_val, "unsuppressed and suppressed mental health bulletin main output row counts are not equal"

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output_suppressed_final_1;
 CREATE TABLE         $db_output.output_suppressed_final_1 USING DELTA AS
 ---replace final 9999999 values with * as per mhsds suppression methodology
 Select a.REPORTING_PERIOD_START 
 ,a.REPORTING_PERIOD_END
 ,a.STATUS
 ,a.BREAKDOWN
 ,a.LEVEL_ONE
 ,a.LEVEL_ONE_DESCRIPTION
 ,a.LEVEL_TWO
 ,a.LEVEL_TWO_DESCRIPTION
 ,a.LEVEL_THREE
 ,a.LEVEL_THREE_DESCRIPTION
 ,a.METRIC
 ,b.METRIC_DESCRIPTION
 ,case when METRIC_VALUE = '9999999' then '*' else METRIC_VALUE end as METRIC_VALUE
 from $db_output.output_v2_suppressed_pt1 a
 inner join $db_output.metric_name_lookup b
    on a.METRIC = b.METRIC;
 
 OPTIMIZE $db_output.output_suppressed_final_1