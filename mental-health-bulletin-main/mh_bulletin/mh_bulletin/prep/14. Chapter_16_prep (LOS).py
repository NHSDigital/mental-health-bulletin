# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("db_output", "_tootle1_100261", "db_output")
dbutils.widgets.text("db_source", "mh_v5_pre_clear", "db_source")
dbutils.widgets.text("start_month_id", "1465")
dbutils.widgets.text("end_month_id", "1476")
dbutils.widgets.text("rp_enddate", "2023-03-31", "rp_enddate")
dbutils.widgets.text("rp_startdate", "2022-04-01", "rp_startdate")
dbutils.widgets.text("IMD_year", "2019", "IMD_year")

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
IMD_year = dbutils.widgets.get("IMD_year")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHB_LOS_RD_CCG_LATEST;

 CREATE TABLE IF NOT EXISTS $db_output.MHB_LOS_RD_CCG_LATEST USING DELTA AS
 SELECT DISTINCT ORG_CODE,
                 NAME
            FROM reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%'
                 
 UNION

 SELECT "UNKNOWN" AS ORG_CODE,
        "UNKNOWN" AS NAME;

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHB_LOS_rd_org_daily_latest;

 CREATE TABLE IF NOT EXISTS $db_output.MHB_LOS_RD_ORG_DAILY_LATEST USING DELTA AS
 SELECT DISTINCT ORG_CODE, 
                 NAME
            FROM reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.LoS_org_relationship_daily;
 CREATE TABLE $db_output.LoS_ORG_RELATIONSHIP_DAILY USING DELTA AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 reference_data.org_relationship_daily
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.LoS_stp_mapping;
 CREATE TABLE $db_output.LoS_stp_mapping USING DELTA AS 
 SELECT 
 A.ORG_CODE as STP_CODE, 
 A.NAME as STP_NAME, 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 $db_output.MHB_LOS_RD_ORG_DAILY_LATEST A
 LEFT JOIN $db_output.LoS_ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN $db_output.MHB_LOS_RD_ORG_DAILY_LATEST C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN $db_output.LoS_ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN $db_output.MHB_LOS_RD_ORG_DAILY_LATEST E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE B.REL_TYPE_CODE is not null
 ORDER BY 1

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.los_mpi;
 CREATE TABLE $db_output.los_mpi USING DELTA AS 

 select distinct
 Person_ID,
 RecordNumber,
 Case 
   when c.UniqMonthID <= 1467 then c.OrgIDCCGRes -- Case When added to add CCG/SubICB derivation according to month
   when c.UniqMonthID > 1467 then c.OrgIDSubICBLocResidence
   else "UNKNOWN" end as OrgIDCCGRes,
 LSOA2011,
 CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
      WHEN Gender IN ('1','2','9') THEN Gender
      ELSE "UNKNOWN" END AS Der_Gender,
 coalesce(g.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
 AgeRepPeriodEnd,
 coalesce(ab.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
 coalesce(ab.age_group_lower_common, "UNKNOWN") as age_group_lower_common,
 coalesce(ab.age_group_lower_chap1, "UNKNOWN") as age_group_lower_chap1,
 coalesce(eth.upper_description, "UNKNOWN") as UpperEthnicity,
 coalesce(eth.key, "UNKNOWN") as LowerEthnicityCode,
 coalesce(eth.lower_description, "UNKNOWN") as LowerEthnicityName,
 coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,
 coalesce(imd.IMD_Quintile, "UNKNOWN") as IMD_Quintile,
 COALESCE(F.ORG_CODE,"UNKNOWN") AS CCG_CODE,
 COALESCE(F.NAME, "UNKNOWN") as CCG_NAME,
 COALESCE(S.STP_CODE, "UNKNOWN") as STP_CODE, 
 COALESCE(S.STP_NAME, "UNKNOWN") as STP_NAME, 
 COALESCE(S.REGION_CODE, "UNKNOWN") as REGION_CODE,
 COALESCE(S.REGION_NAME, "UNKNOWN") as REGION_NAME

 FROM $db_source.MHS001MPI c
 LEFT JOIN $db_output.MHB_LOS_RD_CCG_LATEST F ON CASE when c.UniqMonthID <= 1467 then c.OrgIDCCGRes -- Case When added to add CCG/SubICB derivation according to month
                                                      when c.UniqMonthID > 1467 then c.OrgIDSubICBLocResidence
                                                      else "UNKNOWN" end = F.ORG_CODE
 LEFT JOIN $db_output.LoS_stp_mapping s ON COALESCE(F.ORG_CODE,"UNKNOWN") = S.CCG_CODE
 left join $db_output.Der_Gender g on CASE WHEN c.GenderIDCode IN ('1','2','3','4') THEN c.GenderIDCode
                                           WHEN c.Gender IN ('1','2','9') THEN c.Gender
                                           ELSE "UNKNOWN" END = g.Der_Gender 
 left join $db_output.age_band_desc ab on c.AgeRepPeriodEnd = ab.Age
 left join $db_output.NHSDEthnicityDim eth on c.NHSDEthnicity = eth.key
 left join reference_data.ENGLISH_INDICES_OF_DEP_V02 r 
           on c.LSOA2011 = r.LSOA_CODE_2011 
           and r.imd_year = '$IMD_year'
 left join $db_output.imd_desc imd on r.DECI_IMD = imd.IMD_Number  
 where UniqMonthID between $start_month_id - 11 and $end_month_id                    

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.hosp_los;
 CREATE TABLE $db_output.hosp_los USING DELTA AS 
 SELECT 
 A.PERSON_ID,
 A.RecordNumber,
 A.UniqHospProvSpellID, -- renamed A.UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
 A.OrgIDProv,
 A.StartDateHospProvSpell,
 A.DischDateHospProvSpell,
 DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
 B.UniqWardStayID,
 B.HospitalBedTypeMH,
 B.StartDateWardStay,
 B.EndDateWardStay,
 DATEDIFF(B.EndDateWardStay, B.StartDateWardStay) as WARD_LOS
 FROM $db_output.MHB_MHS501HospProvSpell a 
 LEFT JOIN $db_output.MHB_MHS502WardStay B on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.Recordnumber = b.RecordNumber -- renamed UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
 WHERE A.DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.spells;
 CREATE TABLE $db_output.spells USING DELTA AS 

 SELECT 
 A.PERSON_ID, 
 A.RecordNumber,
 A.UniqHospProvSpellID, -- renamed A.UNIQHOSPPROVSPELLNUM to UniqHospProvSpellID HL 11/1/22
 A.OrgIDProv,
 E.NAME AS Provider_Name,
 get_provider_type_code(A.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(A.OrgIDProv) as ProvTypeName,
 A.StartDateHospProvSpell,
 A.DischDateHospProvSpell,
 DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
 A.UniqWardStayID,
 A.StartDateWardStay,
 A.EndDateWardStay,
 A.HospitalBedTypeMH,
 C.AgeRepPeriodEnd,
 CCG_CODE,
 CCG_NAME,
 STP_CODE, 
 STP_NAME, 
 REGION_CODE,
 REGION_NAME,
 c.Der_Gender,
 g.Der_Gender_Desc,
 age_group_higher_level, 
 age_group_lower_common,
 age_group_lower_chap1,
 UpperEthnicity,
 LowerEthnicityCode,
 LowerEthnicityName, 
 IMD_Decile
 FROM $db_output.hosp_los A
 LEFT JOIN $db_output.los_mpi c ON A.RecordNumber = C.RecordNumber --AND c.PatMRecInRP = TRUE - Removed this as using RecordNumber (so will use demographics of person in that provider at time of discharge)
 LEFT JOIN $db_output.MHB_LOS_RD_ORG_DAILY_LATEST E ON A.ORGIDPROV = E.ORG_CODE
 LEFT JOIN $db_output.Der_Gender g on c.Der_Gender = g.Der_Gender

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.spells