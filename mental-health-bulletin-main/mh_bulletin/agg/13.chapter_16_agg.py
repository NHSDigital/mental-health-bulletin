# Databricks notebook source
#metadata relating to the relevant filters needed for the aggregation of each length of stay measure
counts_metadata = {'16a': {'HOSPITALBEDTYPEMH': '10',
                            'HOSP_LOS': 60,
                            'AGEREPPERIODEND_start': 18,
                            'AGEREPPERIODEND_end': 64},
                   '16b': {'HOSPITALBEDTYPEMH': '10',
                            'HOSP_LOS': 90,
                            'AGEREPPERIODEND_start': 18,
                            'AGEREPPERIODEND_end': 64},
                   '16c': {'HOSPITALBEDTYPEMH': '11',
                            'HOSP_LOS': 60,
                            'AGEREPPERIODEND_start': 65,
                            'AGEREPPERIODEND_end': 999},
                   '16d': {'HOSPITALBEDTYPEMH': '11',
                            'HOSP_LOS': 90,
                            'AGEREPPERIODEND_start': 65,
                            'AGEREPPERIODEND_end': 999},
                   '16e': {'HOSPITALBEDTYPEMH': '',
                            'HOSP_LOS': 60,
                            'AGEREPPERIODEND_start': 0,
                            'AGEREPPERIODEND_end': 17},
                   '16f': {'HOSPITALBEDTYPEMH': '',
                            'HOSP_LOS': 90,
                            'AGEREPPERIODEND_start': 0,
                            'AGEREPPERIODEND_end': 17}}

# COMMAND ----------

#metadata relating to the national breakdown name and column used for each breakdown (or group by)
breakdowns_metdata = { 'England' :{'name':'England',
                                   'column':'"England"'},
                       'AGE_LL'  :{'name':'Age Group (Lower Level)',
                                   'column':'age_group_lower_chap12',
                                   'column_cyp':'age_group_lower_chap1'},
                       'ETH_LL'  :{'name':'Ethnicity (Lower Level)',
                                   'column':'LowerEthnicity'},
                       'ETH_HL'  :{'name':'Ethnicity (Higher Level)',
                                   'column':'UpperEthnicity'},
                       'GENDER'  :{'name':'Gender',
                                   'column':'Der_Gender'},
                       'IMD'     :{'name':'IMD',
                                   'column':'IMD_Decile'}
                       }

# COMMAND ----------

#metadata relating to the sub-national breakdown name and column used for each breakdown (or group by)
geog_breakdown_metadata = {'CCG'     : {'name':'CCG of Residence',
                                        'codecol':'CCG_CODE',
                                        'namecol':'CCG_NAME',
                                        'spellscol':'IC_REC_CCG'},
                           'STP'     : {'name':'STP',
                                        'codecol':'STP_CODE',
                                        'namecol':'STP_NAME',
                                        'spellscol':'STP_CODE'},
                           'REGION'  : {'name':'Commissioning Region',
                                        'codecol':'REGION_CODE',
                                        'namecol':'REGION_NAME',
                                        'spellscol':'REGION_CODE'}
                          }

# COMMAND ----------

for metric in counts_metadata:
  HOSPITALBEDTYPEMH = counts_metadata[metric]['HOSPITALBEDTYPEMH'] #get bed type filter
  HOSP_LOS = counts_metadata[metric]['HOSP_LOS'] #get length of stay filter
  AGEREPPERIODEND_start = counts_metadata[metric]['AGEREPPERIODEND_start'] #get min age range
  AGEREPPERIODEND_end = counts_metadata[metric]['AGEREPPERIODEND_end'] #get max age range
  spark.sql(f"DROP TABLE IF EXISTS {db_output}.SPELLS_{metric}")
  if HOSPITALBEDTYPEMH != '': #if no bed type filter
    #create prep table for the metric with filters applied (without bed type)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.SPELLS_{metric} USING DELTA AS
                  SELECT *
                  FROM 
                  {db_output}.SPELLS
                  WHERE
                  HOSPITALBEDTYPEMH = {HOSPITALBEDTYPEMH}
                  AND HOSP_LOS >= {HOSP_LOS}
                  AND AGEREPPERIODEND between {AGEREPPERIODEND_start} and {AGEREPPERIODEND_end}""")
  else:
    #create prep table for the metric with filters applied
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.SPELLS_{metric} USING DELTA AS
                  SELECT *
                  FROM 
                  {db_output}.SPELLS
                  WHERE
                  HOSP_LOS >= {HOSP_LOS}
                  AND AGEREPPERIODEND between {AGEREPPERIODEND_start} and {AGEREPPERIODEND_end}""")

# COMMAND ----------

#national level breakdowns for count metrics
for metric in counts_metadata:
  for breakdown in breakdowns_metdata:
    name = breakdowns_metdata[breakdown]['name'] #get breakdown name
    if (breakdown == 'AGE_LL') & (metric in ('16e','16f')): #metrics 16e and 16f use a different aggregate field for age group (lower level) breakdown
      col = breakdowns_metdata[breakdown]['column_cyp']
    else:
      col = breakdowns_metdata[breakdown]['column']
    spark.sql(f"""  INSERT INTO {db_output}.output1

                    SELECT 
                    '{rp_startdate}' AS REPORTING_PERIOD_START,
                    '{rp_enddate}' AS REPORTING_PERIOD_END,
                    'Final' AS STATUS,
                    '{name}' AS BREAKDOWN,
                    COALESCE({col},'Unknown') AS PRIMARY_LEVEL,
                    'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
                    'NULL' AS SECONDARY_LEVEL,
                    'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
                    'NULL' AS Level_3,
                    'NULL' AS Level_3_DESCRIPTION,
                    'NULL' AS Level_4,
                    'NULL' AS Level_4_DESCRIPTION,
                    '{metric}' AS Metric,
                    COALESCE(COUNT(DISTINCT PERSON_ID),0) AS Metric_Value
                    FROM 
                    {db_output}.SPELLS_{metric} ---use metric specific prep table
                    GROUP BY COALESCE({col},'Unknown')""") #group by column

# COMMAND ----------

#provider level breakdowns for count metrics
for metric in counts_metadata:
  for breakdown in breakdowns_metdata:
    if breakdown != 'England': #national breakdowns which are not england (i.e demorgaphics)
        name = 'Provider; '+breakdowns_metdata[breakdown]['name']
        if (breakdown == 'AGE_LL') & (metric in ('16e','16f')): #metrics 16e and 16f use a different aggregate field for age group (lower level) breakdown
          col = breakdowns_metdata[breakdown]['column_cyp']
        else:
          col = breakdowns_metdata[breakdown]['column']
    else:
        name = 'Provider'
        col = '"NULL"'
    spark.sql(f"""  INSERT INTO {db_output}.output1

                    SELECT 
                    '{rp_startdate}' AS REPORTING_PERIOD_START,
                    '{rp_enddate}' AS REPORTING_PERIOD_END,
                    'Final' AS STATUS,
                    '{name}' AS BREAKDOWN,
                    A.OrgIDProvider AS PRIMARY_LEVEL,
                    A.Prov_Name AS PRIMARY_LEVEL_DESCRIPTION,
                    COALESCE({col},'Unknown') AS SECONDARY_LEVEL,
                    'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
                    'NULL' AS Level_3,
                    'NULL' AS Level_3_DESCRIPTION,
                    'NULL' AS Level_4,
                    'NULL' AS Level_4_DESCRIPTION,
                    '{metric}' AS Metric,
                    COALESCE(COUNT(DISTINCT PERSON_ID),0) AS Metric_Value
                    FROM 
                    ( SELECT 
                      DISTINCT 
                      A.ORGIDPROVIDER, 
                      B.NAME AS PROV_NAME
                      FROM {db_output}.MHB_MHS000HEADER A
                      LEFT JOIN {db_output}.MHB_LOS_RD_ORG_DAILY_LATEST B
                            ON A.ORGIDPROVIDER = B.ORG_CODE
                      WHERE 
                      REPORTINGPERIODENDDATE BETWEEN '{rp_startdate}' and '{rp_enddate}') as A
                    LEFT JOIN {db_output}.SPELLS_{metric} B ON A.ORGIDPROVIDER = B.ORGIDPROV 
                    GROUP BY 
                        A.OrgIDProvider,
                        A.Prov_Name,
                        COALESCE({col},'Unknown')""")
spark.sql(f"OPTIMIZE {db_output}.output1 ZORDER BY (Metric, Breakdown)")

# COMMAND ----------

#sub-national breakdowns for count metrics
for metric in counts_metadata:
  for geog_breakdown in geog_breakdown_metadata:
    geogname = geog_breakdown_metadata[geog_breakdown]['name'] #get breakdown name
    codecol = geog_breakdown_metadata[geog_breakdown]['codecol'] #get group by code
    namecol = geog_breakdown_metadata[geog_breakdown]['namecol'] #get group by name
    spellscol = geog_breakdown_metadata[geog_breakdown]['spellscol'] #get group by column in prep table
    for breakdown in breakdowns_metdata:
      if breakdown != 'England':
        name = geogname+'; '+breakdowns_metdata[breakdown]['name']
        if (breakdown == 'AGE_LL') & (metric in ('16e','16f')):
          col = breakdowns_metdata[breakdown]['column_cyp']
        else:
          col = breakdowns_metdata[breakdown]['column']
      else:
        name = geogname
        col = '"NULL"'
      spark.sql(f"""  INSERT INTO {db_output}.output1

                      SELECT 
                      '{rp_startdate}' AS REPORTING_PERIOD_START,
                      '{rp_enddate}' AS REPORTING_PERIOD_END,
                      'Final' AS STATUS,
                      '{name}' AS BREAKDOWN,
                      COALESCE(A.CODE,'Unknown') AS PRIMARY_LEVEL,
                      COALESCE(A.NAME,'Unknown') AS PRIMARY_LEVEL_DESCRIPTION,
                      COALESCE({col},'Unknown') AS SECONDARY_LEVEL,
                      'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
                      'NULL' AS Level_3,
                      'NULL' AS Level_3_DESCRIPTION,
                      'NULL' AS Level_4,
                      'NULL' AS Level_4_DESCRIPTION,
                      '{metric}' AS Metric,
                      COALESCE(COUNT(DISTINCT PERSON_ID),0) AS Metric_Value
                      FROM 
                      ( SELECT DISTINCT 
                        {codecol} AS CODE,
                        {namecol} AS NAME
                        FROM {db_output}.LoS_stp_mapping) A
                      FULL JOIN {db_output}.SPELLS_{metric} B ON A.CODE = B.{spellscol}
                      GROUP BY 
                      COALESCE(A.CODE,'Unknown'),
                      COALESCE(A.NAME,'Unknown'),
                      COALESCE({col},'Unknown')""")
spark.sql(f"OPTIMIZE {db_output}.output1 ZORDER BY (Metric, Breakdown)")

# COMMAND ----------

 %sql
 ---calculate crude rates for all the counts for national, clinical commissioning group and sustainability and transformation partnership
 INSERT INTO $db_output.output1
 SELECT 
 a.REPORTING_PERIOD_START,
 a.REPORTING_PERIOD_END,
 a.STATUS,
 a.BREAKDOWN,
 a.Level_1,
 a.Level_1_DESCRIPTION,
 a.Level_2,
 a.Level_2_DESCRIPTION,
 a.Level_3,
 a.Level_3_DESCRIPTION,
 a.Level_4,
 a.Level_4_DESCRIPTION,
 CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END 
   AS Metric,
 COALESCE(CASE 
   WHEN A.Metric = '16a' AND A.BREAKDOWN = 'England' THEN (A.Metric_Value / C.Aged_18_to_64)*100000
   WHEN A.Metric = '16b' AND A.BREAKDOWN = 'England' THEN (A.Metric_Value / C.Aged_18_to_64)*100000
   WHEN A.Metric = '16a' AND A.BREAKDOWN = 'CCG of Residence' THEN (A.Metric_Value / B.Aged_18_to_64)*100000
   WHEN A.Metric = '16b' AND A.BREAKDOWN = 'CCG of Residence' THEN (A.Metric_Value / B.Aged_18_to_64)*100000
   WHEN A.Metric = '16a' AND A.BREAKDOWN = 'STP' THEN (A.Metric_Value / D.Aged_18_to_64)*100000
   WHEN A.Metric = '16b' AND A.BREAKDOWN = 'STP' THEN (A.Metric_Value / D.Aged_18_to_64)*100000
   WHEN A.Metric = '16a' AND A.BREAKDOWN = 'Commissioning Region' THEN (A.Metric_Value / E.Aged_18_to_64)*100000
   WHEN A.Metric = '16b' AND A.BREAKDOWN = 'Commissioning Region' THEN (A.Metric_Value / E.Aged_18_to_64)*100000
   WHEN A.Metric = '16c' AND A.BREAKDOWN = 'England' THEN (A.Metric_Value / C.Aged_65_OVER)*100000
   WHEN A.Metric = '16d' AND A.BREAKDOWN = 'England' THEN (A.Metric_Value / C.Aged_65_OVER)*100000
   WHEN A.Metric = '16c' AND A.BREAKDOWN = 'CCG of Residence'THEN (A.Metric_Value / B.Aged_65_OVER)*100000
   WHEN A.Metric = '16d' AND A.BREAKDOWN = 'CCG of Residence'THEN (A.Metric_Value / B.Aged_65_OVER)*100000
   WHEN A.Metric = '16c' AND A.BREAKDOWN = 'STP' THEN (A.Metric_Value / D.Aged_65_OVER)*100000
   WHEN A.Metric = '16d' AND A.BREAKDOWN = 'STP' THEN (A.Metric_Value / D.Aged_65_OVER)*100000
   WHEN A.Metric = '16c' AND A.BREAKDOWN = 'Commissioning Region' THEN (A.Metric_Value / E.Aged_65_OVER)*100000
   WHEN A.Metric = '16d' AND A.BREAKDOWN = 'Commissioning Region' THEN (A.Metric_Value / E.Aged_65_OVER)*100000
   WHEN A.Metric = '16e' AND A.BREAKDOWN = 'England' THEN (A.Metric_Value / C.Aged_0_to_17)*100000
   WHEN A.Metric = '16f' AND A.BREAKDOWN = 'England' THEN (A.Metric_Value / C.Aged_0_to_17)*100000
   WHEN A.Metric = '16e' AND A.BREAKDOWN = 'CCG of Residence'THEN (A.Metric_Value / B.Aged_0_to_17)*100000
   WHEN A.Metric = '16f' AND A.BREAKDOWN = 'CCG of Residence'THEN (A.Metric_Value / B.Aged_0_to_17)*100000
   WHEN A.Metric = '16e' AND A.BREAKDOWN = 'STP' THEN (A.Metric_Value / D.Aged_0_to_17)*100000
   WHEN A.Metric = '16f' AND A.BREAKDOWN = 'STP' THEN (A.Metric_Value / D.Aged_0_to_17)*100000
   WHEN A.Metric = '16e' AND A.BREAKDOWN = 'Commissioning Region' THEN (A.Metric_Value / E.Aged_0_to_17)*100000
   WHEN A.Metric = '16f' AND A.BREAKDOWN = 'Commissioning Region' THEN (A.Metric_Value / E.Aged_0_to_17)*100000
   END,0) 
   AS Metric_Value
 FROM (  SELECT *
         FROM $db_output.output1
         WHERE Metric in ('16a','16b','16c','16d','16e','16f')
           AND BREAKDOWN in ('England','CCG of Residence','STP','Commissioning Region')) A 
 LEFT JOIN (SELECT ---bring in ccg population figures
             CCG_2021_CODE,
             CCG_2021_NAME,
             SUM(Aged_18_to_64) as Aged_18_to_64,
             SUM(Aged_65_OVER) as Aged_65_OVER,
             SUM(Aged_0_to_17) as Aged_0_to_17
             FROM
             $db_output.mhb_los_ccg_pop
             GROUP BY
             CCG_2021_CODE,
             CCG_2021_NAME) 
             as B
             ON A.Level_1 = B.CCG_2021_CODE
 LEFT JOIN (SELECT ---national population figures using sum of $db_output.mhb_los_ccg_pop
             'England' as England, 
             SUM(Aged_18_to_64) AS Aged_18_to_64, 
             SUM(Aged_65_OVER) AS Aged_65_OVER ,
             SUM(Aged_0_to_17) AS Aged_0_to_17
             FROM $db_output.mhb_los_ccg_pop) 
             AS C 
             ON A.BREAKDOWN = C.England
 LEFT JOIN (SELECT ---bring in ccg population figures and aggregate to stp level using $db_output.stp_region_mapping
             STP_CODE,
             STP_NAME,
             SUM(Aged_18_to_64) as Aged_18_to_64,
             SUM(Aged_65_OVER) as Aged_65_OVER,
             SUM(Aged_0_to_17) as Aged_0_to_17
             FROM $db_output.mhb_los_ccg_pop as A
             INNER JOIN $db_output.stp_region_mapping as B
                ON A.CCG_2021_CODE = B.CCG_CODE
             GROUP BY STP_CODE,
                       STP_NAME)
             AS D
             ON A.LEVEL_1 = D.STP_CODE
 LEFT JOIN (SELECT ---bring in ccg population figures and aggregate to commissioning region level using $db_output.stp_region_mapping
             REGION_CODE,
             REGION_NAME,
             SUM(Aged_18_to_64) as Aged_18_to_64,
             SUM(Aged_65_OVER) as Aged_65_OVER,
             SUM(Aged_0_to_17) as Aged_0_to_17
             FROM $db_output.mhb_los_ccg_pop as A
             INNER JOIN $db_output.stp_region_mapping as B
                ON A.CCG_2021_CODE = B.CCG_CODE
             GROUP BY REGION_CODE,
                       REGION_NAME)
             AS E
             ON A.LEVEL_1 = E.REGION_CODE

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for all the counts for ethnicity higher and ethnicity lower
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown in ('Ethnicity (Higher Level)', 'Ethnicity (Lower Level)')
       and metric in ('16a','16b','16c','16d','16e','16f')) a
 left join (select * from $db_output.output1 where metric = '1m') b on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3 ---get ethncity population using data from 1m

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for all the counts for gender and age group breakdowns
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Metric_value as float) * 100000 as Metric_value
 from (  select *
         from $db_output.output1
         where breakdown in ('Gender; Age Group (Lower Level)', 'Gender', 'Age Group (Lower Level)')
         and metric in ('16a','16b','16c','16d','16e','16f')) a
 left join ( select  Breakdown,
                     CASE WHEN Level_1 in ('18','19') THEN '18 to 19' ELSE Level_1 END AS level_1,
                     level_2,
                     level_3,
                     SUM(Metric_Value) AS Metric_Value
             from $db_output.output1
             where metric = '1f'
             GROUP BY Breakdown,
                     CASE WHEN Level_1 in ('18','19') THEN '18 to 19' ELSE Level_1 END,
                     level_2,
                     level_3) b
    on a.Breakdown = b.Breakdown and a.level_1 = b.level_1 and a.level_2 = b.level_2 and a.level_3 = b.level_3

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for all the counts for deprivation breakdown
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'IMD'
       and metric in ('16a','16b','16c','16d','16e','16f')) a
 left join (select * from $db_output.MHB_imd_pop) b on a.level_1 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for 16e and 16f counts for sustainability and transformation partnership and age group lower breakdown
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Age Group (Lower Level)'
       and metric in ('16e','16f')) a
 left join (select * from $db_output.stp_age_pop_contacts) b on a.level_1 = b.STP_CODE and a.level_2 = b.Der_Age_Group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for 16a, 16b, 16c and 16d counts for sustainability and transformation partnership and age group lower breakdown
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Age Group (Lower Level)'
       and metric in ('16a','16b','16c','16d')) a
 left join (select * from $db_output.stp_age_pop_adults) b on a.level_1 = b.STP_CODE and a.level_2 = b.Der_Age_Group

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for all counts for sustainability and transformation partnership and gender breakdown
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Gender'
       and metric in ('16a','16b','16c','16d','16e','16f')) a
 left join (select * from $db_output.stp_gender_pop) b on a.level_1 = b.STP_CODE and a.level_2 = b.Sex

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for all counts for sustainability and transformation partnership and deprivation breakdown
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Count as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; IMD'
       and metric in ('16a','16b','16c','16d','16e','16f')) a
 left join (select * from $db_output.stp_imd_pop) b on a.level_1 = b.STP and a.level_2 = b.IMD_Decile

# COMMAND ----------

 %sql
 INSERT INTO $db_output.output1
 ---calculate crude rates for all counts for sustainability and transformation partnership and ethnicity higher breakdown
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
 ,CASE 
   WHEN A.Metric = '16a' THEN '16g' 
   WHEN A.Metric = '16b' THEN '16h' 
   WHEN A.Metric = '16c' THEN '16i' 
   WHEN A.Metric = '16d' THEN '16j'
   WHEN A.Metric = '16e' THEN '16k'
   WHEN A.Metric = '16f' THEN '16l'
   END as Metric
 ,cast(a.Metric_value as float) / cast(b.Pop as float) * 100000 as Metric_value
 from (select *
       from $db_output.output1
       where breakdown = 'STP; Ethnicity (Higher Level)'
       and metric in ('16a','16b','16c','16d','16e','16f')) a
 left join (select * from $db_output.stp_eth_pop) b on a.level_1 = b.STP_CODE
                                                   and a.level_2 = b.Ethnic_group

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.output1 ZORDER BY (Metric, Breakdown);