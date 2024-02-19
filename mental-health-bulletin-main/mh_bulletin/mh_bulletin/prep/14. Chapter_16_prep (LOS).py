# Databricks notebook source
# dbutils.widgets.removeAll()

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
            FROM $reference_db.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
                 AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 AND NAME NOT LIKE '%HUB'
                 AND NAME NOT LIKE '%NATIONAL%'
                 
 UNION
 
 SELECT 'Unknown' AS ORG_CODE,
        'Unknown' AS NAME;

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHB_LOS_rd_org_daily_latest;
 
 CREATE TABLE IF NOT EXISTS $db_output.MHB_LOS_RD_ORG_DAILY_LATEST USING DELTA AS
 SELECT DISTINCT ORG_CODE, 
                 NAME
            FROM $reference_db.org_daily
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
 $reference_db.org_relationship_daily
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
 OrgIDCCGRes,
 LSOA2011,
 CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
             WHEN Gender IN ('1','2','9') THEN Gender
             ELSE 'Unknown' END AS Der_Gender,
             AgeRepPeriodEnd,
         case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
              when AgeRepPeriodEnd >= 18 then '18 and over' 
              else 'Unknown' end as age_group_higher_level,
         case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                      when AgeRepPeriodEnd between 18 and 19 then '18 to 19'
                      when AgeRepPeriodEnd between 20 and 24 then '20 to 24'
                      when AgeRepPeriodEnd between 25 and 29 then '25 to 29'
                      when AgeRepPeriodEnd between 30 and 34 then '30 to 34'
                      when AgeRepPeriodEnd between 35 and 39 then '35 to 39'
                      when AgeRepPeriodEnd between 40 and 44 then '40 to 44'
                      when AgeRepPeriodEnd between 45 and 49 then '45 to 49'
                      when AgeRepPeriodEnd between 50 and 54 then '50 to 54'
                      when AgeRepPeriodEnd between 55 and 59 then '55 to 59'
                      when AgeRepPeriodEnd between 60 and 64 then '60 to 64'
                      when AgeRepPeriodEnd between 65 and 69 then '65 to 69'
                      when AgeRepPeriodEnd between 70 and 74 then '70 to 74'
                      when AgeRepPeriodEnd between 75 and 79 then '75 to 79'
                      when AgeRepPeriodEnd between 80 and 84 then '80 to 84'
                      when AgeRepPeriodEnd between 85 and 89 then '85 to 89'
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_common,  
                      
                       case when AgeRepPeriodEnd between 0 and 5 then '0 to 5'
                      when AgeRepPeriodEnd between 6 and 10 then '6 to 10'
                      when AgeRepPeriodEnd between 11 and 15 then '11 to 15'
                      when AgeRepPeriodEnd = 16 then '16'
                      when AgeRepPeriodEnd = 17 then '17'
                      when AgeRepPeriodEnd = 18 then '18'
                      when AgeRepPeriodEnd = 19 then '19'
                      when AgeRepPeriodEnd between 20 and 24 then '20 to 24'
                      when AgeRepPeriodEnd between 25 and 29 then '25 to 29'
                      when AgeRepPeriodEnd between 30 and 34 then '30 to 34'
                      when AgeRepPeriodEnd between 35 and 39 then '35 to 39'
                      when AgeRepPeriodEnd between 40 and 44 then '40 to 44'
                      when AgeRepPeriodEnd between 45 and 49 then '45 to 49'
                      when AgeRepPeriodEnd between 50 and 54 then '50 to 54'
                      when AgeRepPeriodEnd between 55 and 59 then '55 to 59'
                      when AgeRepPeriodEnd between 60 and 64 then '60 to 64'
                      when AgeRepPeriodEnd between 65 and 69 then '65 to 69'
                      when AgeRepPeriodEnd between 70 and 74 then '70 to 74'
                      when AgeRepPeriodEnd between 75 and 79 then '75 to 79'
                      when AgeRepPeriodEnd between 80 and 84 then '80 to 84'
                      when AgeRepPeriodEnd between 85 and 89 then '85 to 89'
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_chap1,
                      
            CASE WHEN NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                 WHEN NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                 WHEN NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                 WHEN NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                 WHEN NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                 WHEN NHSDEthnicity = 'Z' THEN 'Not Stated'
                 WHEN NHSDEthnicity = '99' THEN 'Not Known'
                 ELSE 'Unknown' END AS UpperEthnicity,
            CASE WHEN NHSDEthnicity = 'A' THEN 'A'
                  WHEN NHSDEthnicity = 'B' THEN 'B'
                  WHEN NHSDEthnicity = 'C' THEN 'C'
                  WHEN NHSDEthnicity = 'D' THEN 'D'
                  WHEN NHSDEthnicity = 'E' THEN 'E'
                  WHEN NHSDEthnicity = 'F' THEN 'F'
                  WHEN NHSDEthnicity = 'G' THEN 'G'
                  WHEN NHSDEthnicity = 'H' THEN 'H'
                  WHEN NHSDEthnicity = 'J' THEN 'J'
                  WHEN NHSDEthnicity = 'K' THEN 'K'
                  WHEN NHSDEthnicity = 'L' THEN 'L'
                  WHEN NHSDEthnicity = 'M' THEN 'M'
                  WHEN NHSDEthnicity = 'N' THEN 'N'
                  WHEN NHSDEthnicity = 'P' THEN 'P'
                  WHEN NHSDEthnicity = 'R' THEN 'R'
                  WHEN NHSDEthnicity = 'S' THEN 'S'
                  WHEN NHSDEthnicity = 'Z' THEN 'Not Stated'
                  WHEN NHSDEthnicity = '99' THEN 'Not Known'
                          ELSE 'Unknown' END AS LowerEthnicityCode,
           CASE WHEN NHSDEthnicity = 'A' THEN 'British'
                     WHEN NHSDEthnicity = 'B' THEN 'Irish'
                     WHEN NHSDEthnicity = 'C' THEN 'Any Other White Background'
                     WHEN NHSDEthnicity = 'D' THEN 'White and Black Caribbean' 
                     WHEN NHSDEthnicity = 'E' THEN 'White and Black African'
                     WHEN NHSDEthnicity = 'F' THEN 'White and Asian'
                     WHEN NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
                     WHEN NHSDEthnicity = 'H' THEN 'Indian'
                     WHEN NHSDEthnicity = 'J' THEN 'Pakistani'
                     WHEN NHSDEthnicity = 'K' THEN 'Bangladeshi'
                     WHEN NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
                     WHEN NHSDEthnicity = 'M' THEN 'Caribbean'
                     WHEN NHSDEthnicity = 'N' THEN 'African'
                     WHEN NHSDEthnicity = 'P' THEN 'Any Other Black Background'
                     WHEN NHSDEthnicity = 'R' THEN 'Chinese'
                     WHEN NHSDEthnicity = 'S' THEN 'Any Other Ethnic Group'
                     WHEN NHSDEthnicity = 'Z' THEN 'Not Stated'
                     WHEN NHSDEthnicity = '99' THEN 'Not Known'
                     ELSE 'Unknown' END AS LowerEthnicityName,
               CASE
               WHEN r.DECI_IMD = 10 THEN '10 Least deprived'
               WHEN r.DECI_IMD = 9 THEN '09 Less deprived'
               WHEN r.DECI_IMD = 8 THEN '08 Less deprived'
               WHEN r.DECI_IMD = 7 THEN '07 Less deprived'
               WHEN r.DECI_IMD = 6 THEN '06 Less deprived'
               WHEN r.DECI_IMD = 5 THEN '05 More deprived'
               WHEN r.DECI_IMD = 4 THEN '04 More deprived'
               WHEN r.DECI_IMD = 3 THEN '03 More deprived'
               WHEN r.DECI_IMD = 2 THEN '02 More deprived'
               WHEN r.DECI_IMD = 1 THEN '01 Most deprived'
               ELSE 'Unknown'
               END AS IMD_Decile,
               CASE 
               WHEN r.DECI_IMD IN (9, 10) THEN '05 Least deprived'
               WHEN r.DECI_IMD IN (7, 8) THEN '04'
               WHEN r.DECI_IMD IN (5, 6) THEN '03'
               WHEN r.DECI_IMD IN (3, 4) THEN '02'
               WHEN r.DECI_IMD IN (1, 2) THEN '01 Most deprived'                          
               ELSE 'Unknown' 
               END AS IMD_Quintile,
 COALESCE(F.ORG_CODE,'Unknown') AS CCG_CODE,
 COALESCE(F.NAME, 'Unknown') as CCG_NAME,
 COALESCE(S.STP_CODE, 'Unknown') as STP_CODE, 
 COALESCE(S.STP_NAME, 'Unknown') as STP_NAME, 
 COALESCE(S.REGION_CODE, 'Unknown') as REGION_CODE,
 COALESCE(S.REGION_NAME, 'Unknown') as REGION_NAME
 FROM $db_source.MHS001MPI c
 LEFT JOIN $db_output.MHB_LOS_RD_CCG_LATEST F ON C.ORGIDCCGRES = F.ORG_CODE
 LEFT JOIN $db_output.LoS_stp_mapping s ON COALESCE(F.ORG_CODE,'Unknown') = S.CCG_CODE
 left join       $reference_db.ENGLISH_INDICES_OF_DEP_V02 r 
                     on c.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
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