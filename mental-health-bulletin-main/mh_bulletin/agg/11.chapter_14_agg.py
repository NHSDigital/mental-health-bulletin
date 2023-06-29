# Databricks notebook source
 %sql
 insert into $db_output.Output1
 ---national people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'England' AS BREAKDOWN
 ,'England' as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider' AS BREAKDOWN
 ,COALESCE(h.ORGIDPROVIDER,'Unknown') as Level_1
 ,COALESCE(h.NAME,'Unknown') as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov
 WHERE AccessProvRN = 1
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence' AS BREAKDOWN
 ,COALESCE(h.CCG21CDH,'Unknown') as Level_1
 ,COALESCE(h.CCG21NM,'Unknown') as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = c.CCG_Code
 WHERE AccessCCGRN = 1
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown')
 ORDER BY COALESCE(h.CCG21CDH,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP' AS BREAKDOWN
 ,COALESCE(h.STP21CDH,'Unknown') as Level_1
 ,COALESCE(h.STP21NM,'Unknown') as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = c.STP_Code
 WHERE AccessSTPRN = 1
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region' AS BREAKDOWN
 ,COALESCE(h.NHSER21CDH,'Unknown') as Level_1
 ,COALESCE(h.NHSER21NM,'Unknown') as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = c.Region_Code
 WHERE AccessRegionRN = 1
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Higher Level)' AS BREAKDOWN
 ,UpperEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and ethnicity higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.ORGIDPROVIDER,'Unknown') as Level_1
 ,COALESCE(h.NAME,'Unknown') as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov
 WHERE AccessProvRN = 1
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.CCG21CDH,'Unknown') as Level_1
 ,COALESCE(h.CCG21NM,'Unknown') as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = c.CCG_Code
 WHERE AccessCCGRN = 1
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), UpperEthnicity
 ORDER BY COALESCE(h.CCG21CDH,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership and ethnicity higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.STP21CDH,'Unknown') as Level_1
 ,COALESCE(h.STP21NM,'Unknown') as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = c.STP_Code
 WHERE AccessSTPRN = 1
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and ethnicity higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.NHSER21CDH,'Unknown') as Level_1
 ,COALESCE(h.NHSER21NM,'Unknown') as Level_1_description
 ,UpperEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = c.Region_Code
 WHERE AccessRegionRN = 1
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---ethnicity lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Lower Level)' AS BREAKDOWN
 ,LowerEthnicity as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and ethnicity lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.ORGIDPROVIDER,'Unknown') as Level_1
 ,COALESCE(h.NAME,'Unknown') as Level_1_description
 ,LowerEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov
 WHERE AccessProvRN = 1
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and ethnicity lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.CCG21CDH,'Unknown') as Level_1
 ,COALESCE(h.CCG21NM,'Unknown') as Level_1_description
 ,LowerEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = c.CCG_Code
 WHERE AccessCCGRN = 1
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), LowerEthnicity
 ORDER BY COALESCE(h.CCG21CDH,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership and ethnicity lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.STP21CDH,'Unknown') as Level_1
 ,COALESCE(h.STP21NM,'Unknown') as Level_1_description
 ,LowerEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = c.STP_Code
 WHERE AccessSTPRN = 1
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and ethnicity lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.NHSER21CDH,'Unknown') as Level_1
 ,COALESCE(h.NHSER21NM,'Unknown') as Level_1_description
 ,LowerEthnicity as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = c.Region_Code
 WHERE AccessRegionRN = 1
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Lower Level)' AS BREAKDOWN
 ,age_group_lower_chap12 as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1
 GROUP BY age_group_lower_chap12

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.ORGIDPROVIDER,'Unknown') as Level_1
 ,COALESCE(h.NAME,'Unknown') as Level_1_description
 ,age_group_lower_chap12 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov
 WHERE AccessProvRN = 1
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), age_group_lower_chap12

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.CCG21CDH,'Unknown') as Level_1
 ,COALESCE(h.CCG21NM,'Unknown') as Level_1_description
 ,age_group_lower_chap12 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = c.CCG_Code
 WHERE AccessCCGRN = 1
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), age_group_lower_chap12
 ORDER BY COALESCE(h.CCG21CDH,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership and age group lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.STP21CDH,'Unknown') as Level_1
 ,COALESCE(h.STP21NM,'Unknown') as Level_1_description
 ,age_group_lower_chap12 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = c.STP_Code
 WHERE AccessSTPRN = 1
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_lower_chap12
 ORDER BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_lower_chap12

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and age group lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Lower Level)' AS BREAKDOWN
 ,COALESCE(h.NHSER21CDH,'Unknown') as Level_1
 ,COALESCE(h.NHSER21NM,'Unknown') as Level_1_description
 ,age_group_lower_chap12 as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = c.Region_Code
 WHERE AccessRegionRN = 1
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), age_group_lower_chap12

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---age group higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Higher Level)' AS BREAKDOWN
 ,age_group_higher_level as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1
 GROUP BY age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and age group higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.ORGIDPROVIDER,'Unknown') as Level_1
 ,COALESCE(h.NAME,'Unknown') as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov
 WHERE AccessProvRN = 1
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and age group higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.CCG21CDH,'Unknown') as Level_1
 ,COALESCE(h.CCG21NM,'Unknown') as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = c.CCG_Code
 WHERE AccessCCGRN = 1
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), age_group_higher_level
 ORDER BY COALESCE(h.CCG21CDH,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation partnership and age group higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.STP21CDH,'Unknown') as Level_1
 ,COALESCE(h.STP21NM,'Unknown') as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = c.STP_Code
 WHERE AccessSTPRN = 1
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and age group higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Higher Level)' AS BREAKDOWN
 ,COALESCE(h.NHSER21CDH,'Unknown') as Level_1
 ,COALESCE(h.NHSER21NM,'Unknown') as Level_1_description
 ,age_group_higher_level as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = c.Region_Code
 WHERE AccessRegionRN = 1
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---gender people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Gender' AS BREAKDOWN
 ,Der_Gender as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and gender people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Gender' AS BREAKDOWN
 ,COALESCE(h.ORGIDPROVIDER,'Unknown') as Level_1
 ,COALESCE(h.NAME,'Unknown') as Level_1_description
 ,Der_Gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov
 WHERE AccessProvRN = 1
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and gender people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Gender' AS BREAKDOWN
 ,COALESCE(h.CCG21CDH,'Unknown') as Level_1
 ,COALESCE(h.CCG21NM,'Unknown') as Level_1_description
 ,Der_Gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = c.CCG_Code
 WHERE AccessCCGRN = 1
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), Der_Gender
 ORDER BY COALESCE(h.CCG21CDH,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation and gender people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Gender' AS BREAKDOWN
 ,COALESCE(h.STP21CDH,'Unknown') as Level_1
 ,COALESCE(h.STP21NM,'Unknown') as Level_1_description
 ,Der_Gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = c.STP_Code
 WHERE AccessSTPRN = 1
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and gender people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Gender' AS BREAKDOWN
 ,COALESCE(h.NHSER21CDH,'Unknown') as Level_1
 ,COALESCE(h.NHSER21NM,'Unknown') as Level_1_description
 ,Der_Gender as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = c.Region_Code
 WHERE AccessRegionRN = 1
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---deprivation decile people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'IMD' AS BREAKDOWN
 ,IMD_Decile as Level_1
 ,'NULL' as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 $db_output.CMH_Rolling_Activity
 WHERE AccessEngRN = 1
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---provider and deprivation decile people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; IMD' AS BREAKDOWN
 ,COALESCE(h.ORGIDPROVIDER,'Unknown') as Level_1
 ,COALESCE(h.NAME,'Unknown') as Level_1_description
 ,IMD_Decile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_output.MHB_MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o on o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Rolling_Activity c on h.OrgIDProvider = c.orgidprov
 WHERE AccessProvRN = 1
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---clinical commissioning group and deprivation decile people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; IMD' AS BREAKDOWN
 ,COALESCE(h.CCG21CDH,'Unknown') as Level_1
 ,COALESCE(h.CCG21NM,'Unknown') as Level_1_description
 ,IMD_Decile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.CCG21CDH = c.CCG_Code
 WHERE AccessCCGRN = 1
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), IMD_Decile
 ORDER BY COALESCE(h.CCG21CDH,'Unknown')

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---sustainability and transformation and deprivation decile people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; IMD' AS BREAKDOWN
 ,COALESCE(h.STP21CDH,'Unknown') as Level_1
 ,COALESCE(h.STP21NM,'Unknown') as Level_1_description
 ,IMD_Decile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.STP21CDH = c.STP_Code
 WHERE AccessSTPRN = 1
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 insert into $db_output.Output1
 ---commissioning region and deprivation decile people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; IMD' AS BREAKDOWN
 ,COALESCE(h.NHSER21CDH,'Unknown') as Level_1
 ,COALESCE(h.NHSER21NM,'Unknown') as Level_1_description
 ,IMD_Decile as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'14a' AS Metric,
 COUNT(DISTINCT Person_ID) AS Metric_value
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Rolling_Activity c ON h.NHSER21CDH = c.Region_Code
 WHERE AccessRegionRN = 1
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric, Breakdown);

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---national, gender, age group lower and gender; age group lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (  select *
         from $db_output.output1
         where breakdown in ('England'
                               ,'Gender; Age Group (Lower Level)'
                               ,'Gender'
                               ,'Age Group (Lower Level)'
                               )
         and metric = '14a') a
 left join (select  Breakdown,
                     CASE WHEN Level_1 in ('18','19') THEN '18 to 19' ELSE Level_1 END AS level_1,
                     level_2,
                     level_3,
                     SUM(Metric_Value) AS Metric_Value
             from $db_output.output1
             where metric = '1f'
             GROUP BY Breakdown,
                     CASE WHEN Level_1 in ('18','19') THEN '18 to 19' ELSE Level_1 END,
                     level_2,
                     level_3) b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---ethnicity higher and ethnicity lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown in ('Ethnicity (Higher Level)'
                               ,'Ethnicity (Lower Level)'
                               )
         and metric = '14a') a
 left join (select * from $db_output.output1 where metric = '1m') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---deprivation decile people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'IMD'
         and metric = '14a') a
 left join (select * from $db_output.MHB_imd_pop) b on a.level_1 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---clinical commissioning group people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'CCG of Residence'
         and metric = '14a') a
 left join (select * from $db_output.mhb_ccg_pop) b on a.level_1 = b.CCG_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'STP'
         and metric = '14a') a
 left join (select * from $db_output.mhb_stp_pop) b on a.level_1 = b.STP_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and age group lower people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'STP; Age Group (Lower Level)'
         and metric = '14a') a
 left join (select * from $db_output.stp_age_pop_adults) b on a.level_1 = b.STP_CODE and a.level_2 = b.Der_Age_Group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and gender people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'STP; Gender'
         and metric = '14a') a
 left join (select * from $db_output.stp_gender_pop) b on a.level_1 = b.STP_CODE and a.level_2 = b.Sex

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and deprivation decile people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'STP; IMD'
         and metric = '14a') a
 left join (select * from $db_output.stp_imd_pop) b on a.level_1 = b.STP and a.level_2 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---sustainability and transformation partnership and ethnicity higher people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'STP; Ethnicity (Higher Level)'
         and metric = '14a') a
 left join (select * from $db_output.stp_eth_pop) b on a.level_1 = b.STP_CODE
                                                   and a.level_2 = b.Ethnic_group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---commissioning region people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts in the financial year crude rate
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
 ,'14b' as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 
 from (  select *
         from $db_output.output1
         where breakdown = 'Commissioning Region'
         and metric = '14a') a
 left join (select * from $db_output.mhb_region_pop) b on a.level_1 = b.REGION_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric, Breakdown);