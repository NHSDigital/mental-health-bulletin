# Databricks notebook source
 %sql
 INSERT INTO $db_output.Output1
 ---national adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS Level_1,
 'England' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---national adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS Level_1,
 'England' AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---national adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS Level_1,
 'England' AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---national adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS Level_1,
 'England' AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---national adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'England' AS BREAKDOWN,
 'England' AS Level_1,
 'England' AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and gender adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Gender' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Gender' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Gender' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Gender' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; Gender' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and deprivation decile adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; IMD' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and deprivation decile higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; IMD' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and deprivation decile higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; IMD' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and deprivation decile higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; IMD' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---clinical commissioning group and deprivation decile higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'CCG of Residence; IMD' AS BREAKDOWN,
 COALESCE(h.CCG21CDH,'Unknown') AS Level_1,
 COALESCE(h.CCG21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT CCG21CDH, CCG21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.CCG21CDH = c.CCG_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.CCG21CDH,'Unknown'), COALESCE(h.CCG21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and gender adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Gender' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Gender' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Gender' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Gender' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; Gender' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and deprivation decile adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; IMD' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and deprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; IMD' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and deprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; IMD' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
       DISTINCT STP21CDH, STP21NM
       FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and deprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; IMD' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---sustainability and transformation partnership and deprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'STP; IMD' AS BREAKDOWN,
 COALESCE(h.STP21CDH,'Unknown') AS Level_1,
 COALESCE(h.STP21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT STP21CDH, STP21NM
        FROM $db_output.ccg_mapping_2021) h
 FULL JOIN $db_output.CMH_Agg c ON h.STP21CDH = c.STP_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.STP21CDH,'Unknown'), COALESCE(h.STP21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUEFROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and gender adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Gender' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Gender' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Gender' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Gender' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; Gender' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and deprivation decile adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; IMD' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and deprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; IMD' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and deprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; IMD' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and deprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; IMD' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---provider and deprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Provider; IMD' AS BREAKDOWN,
 COALESCE(h.ORGIDPROVIDER,'Unknown') AS Level_1,
 COALESCE(h.NAME,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
         DISTINCT ORGIDPROVIDER, NAME
         FROM $db_source.MHS000HEADER h
         LEFT JOIN $db_output.mhb_org_daily o ON o.ORG_CODE = h.ORGIDPROVIDER
         WHERE
         UNIQMONTHID BETWEEN $month_id_start and $month_id_end)
         h
 FULL JOIN $db_output.CMH_Agg c on COALESCE(h.ORGIDPROVIDER,'Unknown') = c.orgidprov AND ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.ORGIDPROVIDER,'Unknown'), COALESCE(h.NAME,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 'NULL' AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 UpperEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Ethnicity (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 LowerEthnicity AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Higher Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 age_group_higher_level AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and gender adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Gender' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Gender' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Gender' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Gender' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; Gender' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 Der_Gender AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and deprivation decile adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; IMD' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and deprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; IMD' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and deprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; IMD' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and deprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; IMD' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---commissioning region and deprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Commissioning Region; IMD' AS BREAKDOWN,
 COALESCE(h.NHSER21CDH,'Unknown') AS Level_1,
 COALESCE(h.NHSER21NM,'Unknown') AS Level_1_DESCRIPTION,
 IMD_Decile AS Level_2,
 'NULL' AS Level_2_DESCRIPTION,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 (SELECT 
        DISTINCT NHSER21CDH, NHSER21NM
        FROM $db_output.ccg_mapping_2021) h 
 FULL JOIN $db_output.CMH_Agg c ON h.NHSER21CDH = c.Region_Code
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(h.NHSER21CDH,'Unknown'), COALESCE(h.NHSER21NM,'Unknown'), IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Higher Level)' AS BREAKDOWN,
 UpperEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY UpperEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Lower Level)' AS BREAKDOWN,
 LowerEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Lower Level)' AS BREAKDOWN,
 LowerEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Lower Level)' AS BREAKDOWN,
 LowerEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Lower Level)' AS BREAKDOWN,
 LowerEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---ethnicity lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Ethnicity (Lower Level)' AS BREAKDOWN,
 LowerEthnicity AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY LowerEthnicity

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group lower adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group lower adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group lower adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Lower Level)' AS BREAKDOWN,
 COALESCE(age_group_lower_chap12,'Unknown') AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY COALESCE(age_group_lower_chap12,'Unknown')

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group higher adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Higher Level)' AS BREAKDOWN,
 age_group_higher_level AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Higher Level)' AS BREAKDOWN,
 age_group_higher_level AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Higher Level)' AS BREAKDOWN,
 age_group_higher_level AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group higher adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Higher Level)' AS BREAKDOWN,
 age_group_higher_level AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---age group higher adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Age Group (Higher Level)' AS BREAKDOWN,
 age_group_higher_level AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY age_group_higher_level

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---gender adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Gender' AS BREAKDOWN,
 Der_Gender AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Gender' AS BREAKDOWN,
 Der_Gender AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Gender' AS BREAKDOWN,
 Der_Gender AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---gender adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Gender' AS BREAKDOWN,
 Der_Gender AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---gender adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'Gender' AS BREAKDOWN,
 Der_Gender AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY Der_Gender

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---deprivation decile adult and older adult acute admissions in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'IMD' AS BREAKDOWN,
 IMD_Decile AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15a' AS MEASURE_ID,
 ROUND(SUM(Admissions), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---deprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'IMD' AS BREAKDOWN,
 IMD_Decile AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15b' AS MEASURE_ID,
 ROUND(SUM(Contact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---adeprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year count
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'IMD' AS BREAKDOWN,
 IMD_Decile AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15c' AS MEASURE_ID,
 ROUND(SUM(NoContact), 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---adeprivation decile adult and older adult acute admissions with contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'IMD' AS BREAKDOWN,
 IMD_Decile AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15d' AS MEASURE_ID,
 ROUND((SUM(Contact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.Output1
 ---adeprivation decile adult and older adult acute admissions with no contact in the prior year with mental health services in the financial year crude rate
 SELECT
 '$rp_startdate' AS REPORTING_PERIOD_START_DATE,
 '$rp_enddate' AS REPORTING_PERIOD_END_DATE,
 'Final' AS STATUS,
 'IMD' AS BREAKDOWN,
 IMD_Decile AS Level_1,
 'NULL' AS Level_1_description,
 'NULL' as Level_2,
 'NULL' as Level_2_description,
 'NULL' as Level_3,
 'NULL' as Level_3_description,
 'NULL' as Level_4,
 'NULL' as Level_4_description,
 '15e' AS MEASURE_ID,
 ROUND((SUM(NoContact)/SUM(Admissions))*100, 0) AS MEASURE_VALUE
 FROM
 $db_output.CMH_Agg
 WHERE ReportingPeriodStartDate >= '$rp_startdate' AND last_day(ReportingPeriodStartDate) <= '$rp_enddate'
 GROUP BY IMD_Decile

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Output1