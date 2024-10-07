# Databricks notebook source
# dbutils.widgets.text("db_output", "mark_wagner1_100394", "db_output")
# dbutils.widgets.text("db_source", "mh_pre_pseudo_d1", "db_source")
# dbutils.widgets.text("end_month_id", "1452", "end_month_id")
# dbutils.widgets.text("start_month_id", "1441", "start_month_id")
# dbutils.widgets.text("rp_enddate", "2021-03-31", "rp_enddate")
# dbutils.widgets.text("rp_startdate", "2020-04-01", "rp_startdate")
# dbutils.widgets.text("populationyear", "2020", "populationyear")

# COMMAND ----------

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
                   ,orgidprov
                   ,HospitalBedTypeMH
                   ,sum(datediff(CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                                       WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                                       ELSE EndDateWardStay END
                                       ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                         ELSE StartDateWardStay END)) as Beddays                                        
 from              $db_output.MHB_MHS502WardStay
 group by          Person_id, HospitalBedTypeMH, orgidprov

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 --final bed days table. As this activity there is no need to join on referral/people table
 DROP TABLE IF EXISTS $db_output.beddays;
 CREATE TABLE        $db_output.beddays AS
 select              
 a.Person_ID,
 bd.OrgIDProv,
 o.NAME as Provider_Name,
 get_provider_type_code(bd.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(bd.OrgIDProv) as ProvTypeName,
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

 a.LADistrictAuthCode as LADistrictAuth,
 a.LADistrictAuthName,                    
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
 COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
 COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
 COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
 COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
 COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME,                  
 COALESCE(bdd.HigherMHAdmittedPatientClassName, "Invalid") AS Bed_Type,
 sum(Beddays) as Beddays 
                     
 from                global_temp.bedtype bd
 left join           $db_output.MPI a  on bd.person_id = a.person_id 
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 left join           $db_output.MHB_ORG_DAILY o on bd.OrgIDProv = o.ORG_CODE
 left join           $db_output.hosp_bed_desc bdd on bd.HospitalBedTypeMH = bdd.MHAdmittedPatientClass
 GROUP BY            
 a.Person_ID,
 bd.OrgIDProv,
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
 a.LADistrictAuthCode,
 a.LADistrictAuthName,                 
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,"UNKNOWN"),
 COALESCE(stp.CCG_NAME, "UNKNOWN"),
 COALESCE(stp.STP_CODE, "UNKNOWN"),
 COALESCE(stp.STP_NAME, "UNKNOWN"), 
 COALESCE(stp.REGION_CODE, "UNKNOWN"),
 COALESCE(stp.REGION_NAME, "UNKNOWN"),                  
 COALESCE(bdd.HigherMHAdmittedPatientClassName, "Invalid")

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
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
 COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
 COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
 COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
 COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
 COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME,                  
 COALESCE(bdd.HigherMHAdmittedPatientClassName, "Invalid") AS Bed_Type,
 sum(Beddays) as Beddays       

 from                $db_output.MPI_PROV a
 left join           global_temp.bedtype_prov bd on a.person_id = bd.person_id  and a.OrgIDProv = bd.OrgIDProv
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 left join           $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE
 left join           $db_output.hosp_bed_desc bdd on bd.HospitalBedTypeMH = bdd.MHAdmittedPatientClass
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
 a.IMD_Decile,
 a.IMD_Quintile,
 COALESCE(stp.CCG_CODE,"UNKNOWN"),
 COALESCE(stp.CCG_NAME, "UNKNOWN"),
 COALESCE(stp.STP_CODE, "UNKNOWN"),
 COALESCE(stp.STP_NAME, "UNKNOWN"), 
 COALESCE(stp.REGION_CODE, "UNKNOWN"),
 COALESCE(stp.REGION_NAME, "UNKNOWN"),                  
 COALESCE(bdd.HigherMHAdmittedPatientClassName, "Invalid")

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.beddays_prov