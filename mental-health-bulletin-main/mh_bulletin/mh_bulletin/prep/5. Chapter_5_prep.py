# Databricks notebook source
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
 hs.StartDateHospProvSpell, 
 hs.DischDateHospProvSpell, 
 hs.UniqHospProvSpellID  --count will then be taken in aggregation phase with where clause on StartDateHospProvSpell and DischDateHospProvSpell to disgtinguish between admissions and discharges
 from $db_output.MHB_MHS501HospProvSpell hs
 left join $db_output.mpi a on a.person_id = hs.person_id --join on person_id for demographic information
 left join $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join $db_output.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         else 'Unknown' end 
 left join $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code
 left join $db_output.MHB_MHS502wardstay ws on hs.UniqHospProvSpellID = ws.UniqHospProvSpellID --as this is an activity, join on UniqHospProvSpellNum
 left join $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE

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
 hs.StartDateHospProvSpell, 
 hs.DischDateHospProvSpell, 
 hs.UniqHospProvSpellID  --count will then be taken in aggregation phase with where clause on StartDateHospProvSpell and DischDateHospProvSpell to disgtinguish between admissions and discharges
 from $db_output.MHB_MHS501HospProvSpell hs
 left join $db_output.mpi_prov a on a.person_id = hs.person_id and a.orgidprov = hs.orgidprov --join on person_id for demographic information
 left join $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join $db_output.la la on la.level = a.LADistrictAuth
 left join $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code
 left join $db_output.MHB_MHS502wardstay ws on hs.UniqHospProvSpellID = ws.UniqHospProvSpellID --as this is an activity, join on UniqHospProvSpellNum
 left join $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE