# Databricks notebook source
db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status  = dbutils.widgets.get("status")
populationyear = dbutils.widgets.get("populationyear")
IMD_year = dbutils.widgets.get("IMD_year")

# COMMAND ----------

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

 %sql
 ---pulling person_id and hospitalbedtype straight from MHS502 to match Chapter 4 methodology used in Part1
 CREATE OR REPLACE GLOBAL TEMP VIEW bedtype AS
 select            distinct Person_id
                   ,HospitalBedTypeMH
                   ,sum(datediff(CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                                       WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                                       ELSE EndDateWardStay END
                                       ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                         ELSE StartDateWardStay END)) as Beddays                                        
 from              $db_output.MHB_MHS502WardStay
 group by          Person_id, HospitalBedTypeMH

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 --final bed days table. As this activity there is no need to join on referral/people table
 DROP TABLE IF EXISTS $db_output.beddays;
 CREATE TABLE        $db_output.beddays AS
 select              
 a.Person_ID,
 a.OrgIDProv,
 o.NAME as Provider_Name,
 get_provider_type_code(a.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(a.OrgIDProv) as ProvTypeName,
 a.AgeRepPeriodEnd,
 a.age_group_higher_level,
 a.age_group_lower_common,
 a.age_group_lower_chap45,
 a.Der_Gender,
 a.Der_Gender_Desc,
 a.NHSDEthnicity,
 a.UpperEthnicity,
 a.LowerEthnicityCode,
 a.LowerEthnicityName,
 case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                     when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                     when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                     when a.LADistrictAuth = 'L99999999' then 'L99999999'
                     when a.LADistrictAuth = 'M99999999' then 'M99999999'
                     when a.LADistrictAuth = 'X99999998' then 'X99999998'
                     when la.level is null then 'Unknown'
                     when a.LADistrictAuth = '' then 'Unknown'
                     else la.level end as LADistrictAuth,
 la.level_description as LADistrictAuthName,                    
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,'Unknown') AS CCG_CODE,
 COALESCE(stp.CCG_NAME, 'Unknown') as CCG_NAME,
 COALESCE(stp.STP_CODE, 'Unknown') as STP_CODE,
 COALESCE(stp.STP_NAME, 'Unknown') as STP_NAME, 
 COALESCE(stp.REGION_CODE, 'Unknown') as REGION_CODE,
 COALESCE(stp.REGION_NAME, 'Unknown') as REGION_NAME,                  
 CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
       WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
       ELSE 'Invalid' END AS Bed_Type,
                     sum(Beddays) as Beddays                                            
 from                $db_output.MPI a
 left join           global_temp.bedtype bd on bd.person_id = a.person_id 
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         else 'Unknown' end 
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 left join           $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE
 GROUP BY            
 a.Person_ID,
 a.OrgIDProv,
 o.NAME,
 a.AgeRepPeriodEnd,
 a.age_group_higher_level,
 a.age_group_lower_common,
 a.age_group_lower_chap45,
 a.Der_Gender,
 a.Der_Gender_Desc,
 a.NHSDEthnicity,
 a.UpperEthnicity,
 a.LowerEthnicityCode,
 a.LowerEthnicityName,
 case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                     when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                     when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                     when a.LADistrictAuth = 'L99999999' then 'L99999999'
                     when a.LADistrictAuth = 'M99999999' then 'M99999999'
                     when a.LADistrictAuth = 'X99999998' then 'X99999998'
                     when la.level is null then 'Unknown'
                     when a.LADistrictAuth = '' then 'Unknown'
                     else la.level end,
 la.level_description,                    
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,'Unknown'),
 COALESCE(stp.CCG_NAME, 'Unknown'),
 COALESCE(stp.STP_CODE, 'Unknown'),
 COALESCE(stp.STP_NAME, 'Unknown'), 
 COALESCE(stp.REGION_CODE, 'Unknown'),
 COALESCE(stp.REGION_NAME, 'Unknown'),                  
 CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
       WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
       ELSE 'Invalid' END

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.beddays

# COMMAND ----------

 %sql
 ---pulling person_id and hospitalbedtype straight from MHS502 to match Chapter 4 methodology used in Part1
 CREATE OR REPLACE GLOBAL TEMP VIEW bedtype_prov AS
 select            distinct Person_id
                   ,OrgIDProv
                   ,HospitalBedTypeMH
                   ,sum(datediff(CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                                       WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                                       ELSE EndDateWardStay END
                                       ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                         ELSE StartDateWardStay END)) as Beddays                                        
 from              $db_output.MHB_MHS502WardStay
 group by          Person_id, OrgIDProv, HospitalBedTypeMH

# COMMAND ----------

 %sql
 --final bed days table. As this activity there is no need to join on referral/people table
 DROP TABLE IF EXISTS $db_output.beddays_prov;
 CREATE TABLE        $db_output.beddays_prov AS
 select              
 a.Person_ID,
 a.OrgIDProv,
 o.NAME as Provider_Name,
 get_provider_type_code(a.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(a.OrgIDProv) as ProvTypeName,
 a.AgeRepPeriodEnd,
 a.age_group_higher_level,
 a.age_group_lower_common,
 a.age_group_lower_chap45,
 a.Der_Gender,
 a.Der_Gender_Desc,
 a.NHSDEthnicity,
 a.UpperEthnicity,
 a.LowerEthnicityCode,
 a.LowerEthnicityName,
 case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                     when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                     when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                     when a.LADistrictAuth = 'L99999999' then 'L99999999'
                     when a.LADistrictAuth = 'M99999999' then 'M99999999'
                     when a.LADistrictAuth = 'X99999998' then 'X99999998'
                     when la.level is null then 'Unknown'
                     when a.LADistrictAuth = '' then 'Unknown'
                     else la.level end as LADistrictAuth,
 la.level_description as LADistrictAuthName,                    
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,'Unknown') AS CCG_CODE,
 COALESCE(stp.CCG_NAME, 'Unknown') as CCG_NAME,
 COALESCE(stp.STP_CODE, 'Unknown') as STP_CODE,
 COALESCE(stp.STP_NAME, 'Unknown') as STP_NAME, 
 COALESCE(stp.REGION_CODE, 'Unknown') as REGION_CODE,
 COALESCE(stp.REGION_NAME, 'Unknown') as REGION_NAME,                  
 CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
       WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
       ELSE 'Invalid' END AS Bed_Type,
                     sum(Beddays) as Beddays                                            
 from                $db_output.MPI_PROV a
 left join           global_temp.bedtype_prov bd on a.person_id = bd.person_id  and a.OrgIDProv = bd.OrgIDProv
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.la la on la.level = a.LADistrictAuth
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 left join           $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE
 GROUP BY            
 a.Person_ID,
 a.OrgIDProv,
 o.NAME,
 a.AgeRepPeriodEnd,
 a.age_group_higher_level,
 a.age_group_lower_common,
 a.age_group_lower_chap45,
 a.Der_Gender,
 a.Der_Gender_Desc,
 a.NHSDEthnicity,
 a.UpperEthnicity,
 a.LowerEthnicityCode,
 a.LowerEthnicityName,
 case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                     when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                     when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                     when a.LADistrictAuth = 'L99999999' then 'L99999999'
                     when a.LADistrictAuth = 'M99999999' then 'M99999999'
                     when a.LADistrictAuth = 'X99999998' then 'X99999998'
                     when la.level is null then 'Unknown'
                     when a.LADistrictAuth = '' then 'Unknown'
                     else la.level end,
 la.level_description,                    
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,'Unknown'),
 COALESCE(stp.CCG_NAME, 'Unknown'),
 COALESCE(stp.STP_CODE, 'Unknown'),
 COALESCE(stp.STP_NAME, 'Unknown'), 
 COALESCE(stp.REGION_CODE, 'Unknown'),
 COALESCE(stp.REGION_NAME, 'Unknown'),                  
 CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
       WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
       ELSE 'Invalid' END

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.beddays_prov