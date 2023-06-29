# Databricks notebook source
 %sql
 INSERT INTO $db_output.output1
 ---national people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS Breakdown, 
 'England' AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessEngRN = 1 THEN Person_ID END) AS metric_value
 from $db_output.Perinatal_Master

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group lower people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Lower Level)' AS Breakdown, 
 m.age_group_lower_chap11 AS Level_1,
 m.age_group_lower_chap11 as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessEngRN = 1 THEN p.Person_ID END) AS metric_value
 from $db_output.Perinatal_Master p
 inner join $db_output.MPI m on p.person_id = m.person_id
 group by m.age_group_lower_chap11

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Higher Level)' AS Breakdown, 
 m.age_group_higher_level AS Level_1,
 m.age_group_higher_level as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessEngRN = 1 THEN p.Person_ID END) AS metric_value
 from $db_output.Perinatal_Master p
 inner join $db_output.MPI m on p.person_id = m.person_id
 group by m.age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence' AS Breakdown, 
 OrgIDCCGRes AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessCCGRN = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master 
 GROUP BY OrgIDCCGRes

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and age group higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Age Group (Higher Level)' AS Breakdown, 
 OrgIDCCGRes AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 age_group_higher_level as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessCCGRN = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master m
 GROUP BY OrgIDCCGRes, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and ethnicity higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Ethnicity (Higher Level)' AS Breakdown, 
 OrgIDCCGRes AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessCCGRN = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master 
 GROUP BY OrgIDCCGRes, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and gender people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; Gender' AS Breakdown, 
 OrgIDCCGRes AS Level_1,
 'NULL' as Level_1_description,
 Gender as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessCCGRN = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master
 GROUP BY OrgIDCCGRes, Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group and deprivation quintile higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG - Registration or Residence; IMD Quintiles' AS Breakdown, 
 OrgIDCCGRes AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessCCGRN = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master
 GROUP BY OrgIDCCGRes, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS Breakdown, 
 Region_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessRegionRN = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master
 GROUP BY Region_Code

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level)' AS Breakdown, 
 m.UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Higher Level)' AS Breakdown, 
 m.UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 m.age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.UpperEthnicity, m.age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and age group lower people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Lower Level)' AS Breakdown, 
 m.UpperEthnicity AS Level_1,
 'NULL' as Level_1_description,
 m.age_group_lower_chap11 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.UpperEthnicity, m.age_group_lower_chap11

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level)' AS Breakdown, 
 m.LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower and age group lower people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level); Age Group (Lower Level)' AS Breakdown, 
 m.LowerEthnicity AS Level_1,
 'NULL' as Level_1_description,
 m.age_group_lower_chap11 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.LowerEthnicity, m.age_group_lower_chap11

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender' AS Breakdown, 
 p.Gender AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY p.Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group lower people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender; Age Group (Lower Level)' AS Breakdown, 
 p.Gender AS Level_1,
 'NULL' as Level_1_description,
 m.age_group_lower_chap11 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY p.Gender, m.age_group_lower_chap11

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD' AS Breakdown, 
 m.IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation quintile people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD Quintiles' AS Breakdown, 
 m.IMD_Quintile AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile and age group lower people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD; Age Group (Lower Level)' AS Breakdown, 
 m.IMD_Decile AS Level_1,
 'NULL' as Level_1_description,
 m.age_group_lower_chap11 as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY m.IMD_Decile, m.age_group_lower_chap11

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---local authority district people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'LAD/UA' AS Breakdown, 
 p.LADistrictAuth AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN p.AttContacts > 0 AND p.FYAccessEngRN = 1 THEN p.Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master p 
 inner join $db_output.MPI m on p.person_id = m.person_id
 GROUP BY p.LADistrictAuth

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS Breakdown, 
 OrgIDProv AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessRNProv = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master
 GROUP BY OrgIDProv

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and age group higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Age Group (Higher Level)' AS Breakdown, 
 OrgIDProv AS Level_1,
 'NULL' as Level_1_description,
 age_group_higher_level as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessRNProv = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master 
 GROUP BY OrgIDProv, age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and ethnicity higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS Breakdown, 
 OrgIDProv AS Level_1,
 'NULL' as Level_1_description,
 UpperEthnicity as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessRNProv = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master 
 GROUP BY OrgIDProv, UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---provider and deprivation quintile higher people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; IMD Quintiles' AS Breakdown, 
 OrgIDProv AS Level_1,
 'NULL' as Level_1_description,
 IMD_Quintile as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessRNProv = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master 
 GROUP BY OrgIDProv, IMD_Quintile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership people in contact with specialist perinatal mental health community services count
 select
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP' AS Breakdown, 
 STP_Code AS Level_1,
 'NULL' as Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '11a' AS Metric,
 coalesce(COUNT(DISTINCT CASE WHEN AttContacts > 0 AND FYAccessSTPRN = 1 THEN Person_ID END),0) AS metric_value
 FROM $db_output.Perinatal_Master
 GROUP BY STP_Code

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Output1 ZORDER BY (Metric, Breakdown);

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national people in contact with specialist perinatal mental health community services crude rate - limited to female popluation
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
 ,'11b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'England'
       and metric = '11a') a
 left join (select 
           'England' as Breakdown, 
           SUM(Metric_Value) as Metric_Value 
           from $db_output.output1 
           where metric = '1f' and Breakdown = 'Gender' and Level_1 = '2'
           ) b on a.Breakdown = b.Breakdown

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---age group lower people in contact with specialist perinatal mental health community services crude rate - limited to female popluation
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
 ,'11b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'Age Group (Lower Level)'
       and metric = '11a') a
 left join (SELECT CASE WHEN AGE_LOWER BETWEEN 0 AND 14 THEN 'Under 15'
                        WHEN AGE_LOWER BETWEEN 15 AND 19 THEN '15 to 19'
                        WHEN AGE_LOWER BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AGE_LOWER BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AGE_LOWER BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AGE_LOWER BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AGE_LOWER BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AGE_LOWER BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AGE_LOWER BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AGE_LOWER BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AGE_LOWER > 64 THEN '65 and over' ELSE 'Unknown' END as AGE,
                   SUM(POPULATION_COUNT) AS Metric_Value
             FROM $db_output.MHB_ons_population_v2
             WHERE GENDER = 'F'
             GROUP BY CASE WHEN AGE_LOWER BETWEEN 0 AND 14 THEN 'Under 15'
                            WHEN AGE_LOWER BETWEEN 15 AND 19 THEN '15 to 19'
                            WHEN AGE_LOWER BETWEEN 20 AND 24 THEN '20 to 24'
                            WHEN AGE_LOWER BETWEEN 25 AND 29 THEN '25 to 29'
                            WHEN AGE_LOWER BETWEEN 30 AND 34 THEN '30 to 34'
                            WHEN AGE_LOWER BETWEEN 35 AND 39 THEN '35 to 39'
                            WHEN AGE_LOWER BETWEEN 40 AND 44 THEN '40 to 44'
                            WHEN AGE_LOWER BETWEEN 45 AND 49 THEN '45 to 49'
                            WHEN AGE_LOWER BETWEEN 50 AND 54 THEN '50 to 54'
                            WHEN AGE_LOWER BETWEEN 55 AND 59 THEN '55 to 59'
                            WHEN AGE_LOWER BETWEEN 60 AND 64 THEN '60 to 64'
                            WHEN AGE_LOWER > 64 THEN '65 and over' ELSE 'Unknown' END) b on a.level_1 = b.Age

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower people in contact with specialist perinatal mental health community services crude rate - limited to female popluation
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
 ,'11b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'Ethnicity (Lower Level)'
       and metric = '11a') a
 left join (select   CASE WHEN ethnic_group_formatted = 'British' THEN 'A'
                          WHEN ethnic_group_formatted = 'Irish' THEN 'B'
                          WHEN ethnic_group_formatted = 'Any Other White Background' THEN 'C'
                          WHEN ethnic_group_formatted = 'White and Black Caribbean' THEN 'D'
                          WHEN ethnic_group_formatted = 'White and Black African' THEN 'E'
                          WHEN ethnic_group_formatted = 'White and Asian' THEN 'F'
                          WHEN ethnic_group_formatted = 'Any Other Mixed Background' THEN 'G'
                          WHEN ethnic_group_formatted = 'Indian' THEN 'H'
                          WHEN ethnic_group_formatted = 'Pakistani' THEN 'J'
                          WHEN ethnic_group_formatted = 'Bangladeshi' THEN 'K'
                          WHEN ethnic_group_formatted = 'Any Other Asian Background' THEN 'L'
                          WHEN ethnic_group_formatted = 'Caribbean' THEN 'M'
                          WHEN ethnic_group_formatted = 'African' THEN 'N'
                          WHEN ethnic_group_formatted = 'Any Other Black Background' THEN 'P'
                          WHEN ethnic_group_formatted = 'Chinese' THEN 'R'
                          WHEN ethnic_group_formatted = 'Other Ethnic Groups' THEN 'S'
                          END as LEVEL_1,
                     SUM(Population) as Metric_value
             from $db_output.pop_health
             where Der_Gender = '2'
             GROUP BY 
                     CASE WHEN ethnic_group_formatted = 'British' THEN 'A'
                          WHEN ethnic_group_formatted = 'Irish' THEN 'B'
                          WHEN ethnic_group_formatted = 'Any Other White Background' THEN 'C'
                          WHEN ethnic_group_formatted = 'White and Black Caribbean' THEN 'D'
                          WHEN ethnic_group_formatted = 'White and Black African' THEN 'E'
                          WHEN ethnic_group_formatted = 'White and Asian' THEN 'F'
                          WHEN ethnic_group_formatted = 'Any Other Mixed Background' THEN 'G'
                          WHEN ethnic_group_formatted = 'Indian' THEN 'H'
                          WHEN ethnic_group_formatted = 'Pakistani' THEN 'J'
                          WHEN ethnic_group_formatted = 'Bangladeshi' THEN 'K'
                          WHEN ethnic_group_formatted = 'Any Other Asian Background' THEN 'L'
                          WHEN ethnic_group_formatted = 'Caribbean' THEN 'M'
                          WHEN ethnic_group_formatted = 'African' THEN 'N'
                          WHEN ethnic_group_formatted = 'Any Other Black Background' THEN 'P'
                          WHEN ethnic_group_formatted = 'Chinese' THEN 'R'
                          WHEN ethnic_group_formatted = 'Other Ethnic Groups' THEN 'S'
                          END
                          ) b on a.level_1 = b.LEVEL_1

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Output1 ZORDER BY (Metric, Breakdown);