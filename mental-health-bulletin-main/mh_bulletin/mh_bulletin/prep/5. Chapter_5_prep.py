# Databricks notebook source
# %sql
# create widget text end_month_id default "1452";
# create widget text start_month_id default "1441";
# create widget text db_source default "mh_pre_pseudo_d1";
# create widget text rp_enddate default "2021-03-31";
# create widget text rp_startdate default "2020-04-01";
# create widget text status default "Final";
# create widget text db_output default "_tootle1_100261";
# create widget text populationyear default 2020;
# create widget text IMD_year default 2019;

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
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

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 ---final table for admissions with required breakdowns apart from bed type. Provider can be included in this table as it is activity rather than a count of people
 DROP TABLE IF EXISTS $db_output.admissions_discharges;
 CREATE TABLE        $db_output.admissions_discharges AS
 SELECT DISTINCT
 a.Person_ID,
 a.OrgIDProv,
 o.NAME as Provider_Name,
 get_provider_type_code(a.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(a.OrgIDProv) as ProvTypeName,
 a.AgeRepPeriodEnd,
 a.age_group_higher_level,
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
 COALESCE(bd.HigherMHAdmittedPatientClassName, "Invalid") AS Bed_Type,
 hs.StartDateHospProvSpell, 
 hs.DischDateHospProvSpell, 
 hs.UniqHospProvSpellID  --count will then be taken in aggregation phase with where clause on StartDateHospProvSpell and DischDateHospProvSpell to disgtinguish between admissions and discharges
 from $db_output.MHB_MHS501HospProvSpell hs
 left join $db_output.mpi a on a.person_id = hs.person_id --join on person_id for demographic information
 left join $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code
 left join $db_output.MHB_MHS502wardstay ws on hs.UniqHospProvSpellID = ws.UniqHospProvSpellID --as this is an activity, join on UniqHospProvSpellNum
 left join $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE
 left join $db_output.hosp_bed_desc bd on ws.HospitalBedTypeMH = bd.MHAdmittedPatientClass

# COMMAND ----------

 %sql
 ---final table for admissions with required breakdowns apart from bed type. Provider can be included in this table as it is activity rather than a count of people
 DROP TABLE IF EXISTS $db_output.admissions_discharges_prov;
 CREATE TABLE        $db_output.admissions_discharges_prov AS
 SELECT DISTINCT
 a.Person_ID,
 a.OrgIDProv,
 o.NAME as Provider_Name,
 a.AgeRepPeriodEnd,
 a.age_group_higher_level,
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
 COALESCE(bd.HigherMHAdmittedPatientClassName, "Invalid") AS Bed_Type,
 hs.StartDateHospProvSpell, 
 hs.DischDateHospProvSpell, 
 hs.UniqHospProvSpellID  --count will then be taken in aggregation phase with where clause on StartDateHospProvSpell and DischDateHospProvSpell to disgtinguish between admissions and discharges
 from $db_output.MHB_MHS501HospProvSpell hs
 left join $db_output.mpi_prov a on a.person_id = hs.person_id and a.orgidprov = hs.orgidprov --join on person_id for demographic information
 left join $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code
 left join $db_output.MHB_MHS502wardstay ws on hs.UniqHospProvSpellID = ws.UniqHospProvSpellID --as this is an activity, join on UniqHospProvSpellNum
 left join $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE
 left join $db_output.hosp_bed_desc bd on ws.HospitalBedTypeMH = bd.MHAdmittedPatientClass