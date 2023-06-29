# Databricks notebook source
 %sql
 INSERT INTO $db_output.Output1
 ---national children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS PRIMARY_LEVEL,
 'England' AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER H
         LEFT JOIN $db_output.mhb_org_daily b ON H.ORGIDPROVIDER = B.ORG_CODE 
         WHERE
         UNIQMONTHID BETWEEN $month_id_end -11 and $month_id_end)
         h
 LEFT JOIN $db_output.MHB_FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
 GROUP BY 
 h.ORGIDPROVIDER,
 h.NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence' AS BREAKDOWN, 
 ccg21CDH AS PRIMARY_LEVEL,
 ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 ccg21CDH,
 ccg21nm

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and provider children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence; Provider' AS BREAKDOWN,
 SubICB_Code AS PRIMARY_LEVEL,
 SubICB_Name AS PRIMARY_LEVEL_DESCRIPTION,
 OrgIDProv AS SECONDARY_LEVEL,
 PROV_NAME AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessSubICBProvRN = '1'
 AND Metric = 'MHS95'
 GROUP BY 
 SubICB_Code,
 SubICB_Name,
 OrgIDProv,
 PROV_NAME

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnershipchildren and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP' AS BREAKDOWN,
 STP21CDH AS PRIMARY_LEVEL,
 STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 STP21CDH,
 STP21nm

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 NHSER21CDH AS PRIMARY_LEVEL,
 NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 NHSER21CDH,
 NHSER21nm

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 UpperEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER H
         LEFT JOIN $db_output.mhb_org_daily b ON H.ORGIDPROVIDER = B.ORG_CODE 
         WHERE
         UNIQMONTHID BETWEEN $month_id_end -11 and $month_id_end)
         h
 LEFT JOIN $db_output.MHB_FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
 GROUP BY 
 h.ORGIDPROVIDER,
 h.NAME,
 UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence; Ethnicity (Higher Level)' AS BREAKDOWN,
 ccg21CDH AS PRIMARY_LEVEL,
 ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
 UpperEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 ccg21CDH,
 ccg21nm,
 UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP; Ethnicity (Higher Level)' AS BREAKDOWN,
 STP21CDH AS PRIMARY_LEVEL,
 STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
 UpperEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 STP21CDH,
 STP21nm,
 UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region; Ethnicity (Higher Level)' AS BREAKDOWN,
 NHSER21CDH AS PRIMARY_LEVEL,
 NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
 UpperEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 NHSER21CDH,
 NHSER21nm,
 UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity higher and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Higher Level); Age Group (Lower Level)' AS BREAKDOWN,
 UpperEthnicity AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY UpperEthnicity, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level)' AS BREAKDOWN,
 LowerEthnicity AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Ethnicity (Lower Level)' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 LowerEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER H
         LEFT JOIN $db_output.mhb_org_daily b ON H.ORGIDPROVIDER = B.ORG_CODE 
         WHERE
         UNIQMONTHID BETWEEN $month_id_end -11 and $month_id_end)
         h
 LEFT JOIN $db_output.MHB_FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
 GROUP BY 
 h.ORGIDPROVIDER,
 h.NAME,
 LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence; Ethnicity (Lower Level)' AS BREAKDOWN,
 ccg21CDH AS PRIMARY_LEVEL,
 ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
 LowerEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 ccg21CDH,
 ccg21nm,
 LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP; Ethnicity (Lower Level)' AS BREAKDOWN,
 STP21CDH AS PRIMARY_LEVEL,
 STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
 LowerEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 STP21CDH,
 STP21nm,
 LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region; Ethnicity (Lower Level)' AS BREAKDOWN,
 NHSER21CDH AS PRIMARY_LEVEL,
 NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
 LowerEthnicity AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 NHSER21CDH,
 NHSER21nm,
 LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity lower and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Ethnicity (Lower Level); Age Group (Lower Level)' AS BREAKDOWN,
 LowerEthnicity AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY LowerEthnicity, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Lower Level)' AS BREAKDOWN,
 age_group_lower_chap1 AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Age Group (Lower Level)' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER H
         LEFT JOIN $db_output.mhb_org_daily b ON H.ORGIDPROVIDER = B.ORG_CODE 
         WHERE
         UNIQMONTHID BETWEEN $month_id_end -11 and $month_id_end)
         h
 LEFT JOIN $db_output.MHB_FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
 GROUP BY 
 h.ORGIDPROVIDER,
 h.NAME,
 age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence; Age Group (Lower Level)' AS BREAKDOWN, 
 ccg21CDH AS PRIMARY_LEVEL,
 ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 ccg21CDH,
 ccg21nm,
 age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP; Age Group (Lower Level)' AS BREAKDOWN,
 STP21CDH AS PRIMARY_LEVEL,
 STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 STP21CDH,
 STP21nm,
 age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region; Age Group (Lower Level)' AS BREAKDOWN,
 NHSER21CDH AS PRIMARY_LEVEL,
 NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 NHSER21CDH,
 NHSER21nm,
 age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Age Group (Higher Level)' AS BREAKDOWN,
 age_group_higher_level AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Age Group (Higher Level)' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_higher_level AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER H
         LEFT JOIN $db_output.mhb_org_daily b ON H.ORGIDPROVIDER = B.ORG_CODE 
         WHERE
         UNIQMONTHID BETWEEN $month_id_end -11 and $month_id_end)
         h
 LEFT JOIN $db_output.MHB_FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
 GROUP BY 
 h.ORGIDPROVIDER,
 h.NAME,
 age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence; Age Group (Higher Level)' AS BREAKDOWN,
 ccg21CDH AS PRIMARY_LEVEL,
 ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_higher_level AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 ccg21CDH,
 ccg21nm,
 age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP; Age Group (Higher Level)' AS BREAKDOWN,
 STP21CDH AS PRIMARY_LEVEL,
 STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_higher_level AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 STP21CDH,
 STP21nm,
 age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group higher children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region; Age Group (Higher Level)' AS BREAKDOWN,
 NHSER21CDH AS PRIMARY_LEVEL,
 NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_higher_level AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 NHSER21CDH,
 NHSER21nm,
 age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---gender children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender' AS BREAKDOWN,
 Der_Gender AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and gender children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; Gender' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 Der_Gender AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER H
         LEFT JOIN $db_output.mhb_org_daily b ON H.ORGIDPROVIDER = B.ORG_CODE 
         WHERE
         UNIQMONTHID BETWEEN $month_id_end -11 and $month_id_end)
         h
 LEFT JOIN $db_output.MHB_FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
 GROUP BY 
 h.ORGIDPROVIDER,
 h.NAME,
 Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and gender children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence; Gender' AS BREAKDOWN,
 ccg21CDH AS PRIMARY_LEVEL,
 ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
 Der_Gender AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 ccg21CDH,
 ccg21nm,
 Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and gender children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP; Gender' AS BREAKDOWN,
 STP21CDH AS PRIMARY_LEVEL,
 STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
 Der_Gender AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 STP21CDH,
 STP21nm,
 Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and gender children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region; Gender' AS BREAKDOWN,
 NHSER21CDH AS PRIMARY_LEVEL,
 NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
 Der_Gender AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 NHSER21CDH,
 NHSER21nm,
 Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---gender and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Gender; Age Group (Lower Level)' AS BREAKDOWN,
 Der_Gender AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY Der_Gender, age_group_lower_chap1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---deprivation decile children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD' AS BREAKDOWN,
 IMD_Decile AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 'NULL' AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and deprivation decile children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Provider; IMD' AS BREAKDOWN,
 h.ORGIDPROVIDER AS PRIMARY_LEVEL,
 h.NAME AS PRIMARY_LEVEL_DESCRIPTION,
 IMD_Decile AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER H
         LEFT JOIN $db_output.mhb_org_daily b ON H.ORGIDPROVIDER = B.ORG_CODE 
         WHERE
         UNIQMONTHID BETWEEN $month_id_end -11 and $month_id_end)
         h
 LEFT JOIN $db_output.MHB_FirstCont_Final f on f.orgidprov = h.orgidprovider AND AccessRNProv = '1' AND Metric = 'MHS95'
 GROUP BY 
 h.ORGIDPROVIDER,
 h.NAME,
 IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and deprivation decile children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'CCG of Residence; IMD' AS BREAKDOWN,
 ccg21CDH AS PRIMARY_LEVEL,
 ccg21nm AS PRIMARY_LEVEL_DESCRIPTION,
 IMD_Decile AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.SubICB_Code = h.ccg21CDH AND A.AccessSubICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 ccg21CDH,
 ccg21nm,
 IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and deprivation decile children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'STP; IMD' AS BREAKDOWN,
 STP21CDH AS PRIMARY_LEVEL,
 STP21nm AS PRIMARY_LEVEL_DESCRIPTION,
 IMD_Decile AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.ICB_Code = h.STP21CDH and a.AccessICBRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 STP21CDH,
 STP21nm,
 IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and deprivation decile children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'Commissioning Region; IMD' AS BREAKDOWN,
 NHSER21CDH AS PRIMARY_LEVEL,
 NHSER21nm AS PRIMARY_LEVEL_DESCRIPTION,
 IMD_Decile AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 LEFT JOIN $db_output.MHB_FirstCont_Final a on a.Region_Code = h.NHSER21CDH and a.AccessRegionRN = '1' AND Metric = 'MHS95'
 GROUP BY 
 NHSER21CDH,
 NHSER21nm,
 IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---deprivation decile and age group lower children and young people aged under 18 with at least one contact count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 '$status' AS STATUS,
 'IMD; Age Group (Lower Level)' AS BREAKDOWN,
 IMD_Decile AS PRIMARY_LEVEL,
 'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
 age_group_lower_chap1 AS SECONDARY_LEVEL,
 'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '17a' AS MEASURE_ID,
 COUNT(DISTINCT PERSON_ID) AS MEASURE_VALUE
 FROM
 $db_output.MHB_FirstCont_Final
 WHERE
 AccessEngRN = '1' AND Metric = 'MHS95'
 GROUP BY IMD_Decile, age_group_lower_chap1

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Output1 ZORDER BY (Metric, Breakdown);

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown in ('England')
         and metric = '17a') a
 left join (select 'England' as Breakdown, SUM(Metric_Value) as Metric_Value from $db_output.output1 where metric = '1f' and Breakdown = 'Age Group (Lower Level)' and Level_1 in ('0 to 5','11 to 15','16','17')) b on a.Breakdown = b.Breakdown

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity lower and ethncity higher children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
         from $db_output.output1
         where breakdown in ('Ethnicity (Higher Level)', 'Ethnicity (Lower Level)')
         and metric = '17a') a
 left join (select * from $db_output.output1 where metric = '1m') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender and age group lower children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown in ('Gender; Age Group (Lower Level)', 'Age Group (Lower Level)')
       and metric = '17a') a
 left join (select  *
             from $db_output.output1
             where metric = '1f') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---gender children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'Gender'
       and metric = '17a') a
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
 ---deprivation decile children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'IMD'
       and metric = '17a') a
 left join (select * from $db_output.MHB_imd_pop) b on a.level_1 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'CCG of Residence'
       and metric = '17a') a
 left join (select * from $db_output.mhb_ccg_cyp_pop) b on a.level_1 = b.CCG_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP'
       and metric = '17a') a
 left join (select * from $db_output.mhb_stp_cyp_pop) b on a.level_1 = b.STP_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and age group lower children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Age Group (Lower Level)'
       and metric = '17a') a
 left join (select * from $db_output.stp_age_pop_contacts) b on a.level_1 = b.STP_CODE and a.level_2 = b.Der_Age_Group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and gender children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Gender'
       and metric = '17a') a
 left join (select * from $db_output.stp_cyp_gender_pop) b on a.level_1 = b.STP_CODE and a.level_2 = b.Sex

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and deprivation decile children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; IMD'
       and metric = '17a') a
 left join (select * from $db_output.stp_imd_pop) b on a.level_1 = b.STP and a.level_2 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and ethnicity higher children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Ethnicity (Higher Level)'
       and metric = '17a') a
 left join (select * from $db_output.stp_cyp_eth_pop) b on a.level_1 = b.STP_CODE
                                                   and a.level_2 = b.Ethnic_group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region children and young people aged under 18 with at least one contact crude rate
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
 ,'17b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'Commissioning Region'
       and metric = '17a') a
 left join (select * from $db_output.mhb_region_cyp_pop) b on a.level_1 = b.REGION_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Output1 ZORDER BY (Metric, Breakdown);