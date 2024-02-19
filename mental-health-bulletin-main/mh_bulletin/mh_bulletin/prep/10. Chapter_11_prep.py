# Databricks notebook source
 %md
 DO NOT USE THE BULLETIN ASSET TABLES FOR PERINATAL. THIS LIMITS THE NUMBER OF CONTACTS 

# COMMAND ----------

 %sql
 create widget text end_month_id default "1464";
 create widget text start_month_id default "1453";
 create widget text db_source default "mhsds_database";
 create widget text rp_enddate default "2022-03-31";
 create widget text rp_startdate default "2021-04-01";
 create widget text status default "Final";
 create widget text db_output default "personal_db";
 create widget text populationyear default '2020';
 create widget text IMD_year default '2019';

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

 %sql
 CREATE OR REPLACE TEMPORARY VIEW refs AS
 
 SELECT DISTINCT
    r.UniqMonthID,
    r.RecordNumber,
    r.Person_ID,
    r.ServiceRequestId,
    r.UniqServReqID,
    r.OrgIDProv,
    m.OrgIDCCGRes,
    r.ReferralRequestReceivedDate,
    o.Name as Provider_Name,
    CASE
     WHEN GENDERIDCODE = '1' THEN '1'
     WHEN GENDERIDCODE = '2' THEN '2'
     WHEN GENDERIDCODE = '3' THEN '3'
     WHEN GENDERIDCODE = '4' THEN '4'
     WHEN GENDER = '1' THEN '1'
     WHEN GENDER = '2' THEN '2'
     WHEN GENDER = '9' THEN '9'
     ELSE 'Unknown' END AS Der_Gender,
    COALESCE(mp.CCG_CODE,'Unknown') AS CCG_CODE,
    COALESCE(mp.CCG_NAME, 'Unknown') as CCG_NAME,
    COALESCE(mp.STP_CODE, 'Unknown') as STP_CODE,
    COALESCE(mp.STP_NAME, 'Unknown') as STP_NAME, 
    COALESCE(mp.REGION_CODE, 'Unknown') as REGION_CODE,
    COALESCE(mp.REGION_NAME, 'Unknown') as REGION_NAME,
    case when LEFT(m.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(m.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(m.LADistrictAuth,1) = 'W' then 'W'
                           when m.LADistrictAuth = 'L99999999' then 'L99999999'
                           when m.LADistrictAuth = 'M99999999' then 'M99999999'
                           when m.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when m.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth,
    la.level_description as LADistrictAuthName,
    case when m.AgeRepPeriodEnd between 0 and 17 then 'Under 18'
         when m.AgeRepPeriodEnd >= 18 then '18 and over' 
         else 'Unknown' end as age_group_higher_level,
    CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 14 THEN 'Under 15'
                        WHEN AgeRepPeriodEnd BETWEEN 15 AND 19 THEN '15 to 19'
                        WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeRepPeriodEnd > 64 THEN '65 or over' ELSE 'Unknown' END as age_group_lower_chap11,
    CASE WHEN m.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
       WHEN m.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
       WHEN m.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
       WHEN m.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
       WHEN m.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
       WHEN m.NHSDEthnicity = 'Z' THEN 'Not Stated'
       WHEN m.NHSDEthnicity = '99' THEN 'Not Known'
       ELSE 'Unknown' END AS UpperEthnicity,
    CASE WHEN m.NHSDEthnicity = 'A' THEN 'A'
          WHEN m.NHSDEthnicity = 'B' THEN 'B'
          WHEN m.NHSDEthnicity = 'C' THEN 'C'
          WHEN m.NHSDEthnicity = 'D' THEN 'D'
          WHEN m.NHSDEthnicity = 'E' THEN 'E'
          WHEN m.NHSDEthnicity = 'F' THEN 'F'
          WHEN m.NHSDEthnicity = 'G' THEN 'G'
          WHEN m.NHSDEthnicity = 'H' THEN 'H'
          WHEN m.NHSDEthnicity = 'J' THEN 'J'
          WHEN m.NHSDEthnicity = 'K' THEN 'K'
          WHEN m.NHSDEthnicity = 'L' THEN 'L'
          WHEN m.NHSDEthnicity = 'M' THEN 'M'
          WHEN m.NHSDEthnicity = 'N' THEN 'N'
          WHEN m.NHSDEthnicity = 'P' THEN 'P'
          WHEN m.NHSDEthnicity = 'R' THEN 'R'
          WHEN m.NHSDEthnicity = 'S' THEN 'S'
          WHEN m.NHSDEthnicity = 'Z' THEN 'Not Stated'
          WHEN m.NHSDEthnicity = '99' THEN 'Not Known'
                  ELSE 'Unknown' END AS LowerEthnicityCode,
    CASE WHEN m.NHSDEthnicity = 'A' THEN 'British'
         WHEN m.NHSDEthnicity = 'B' THEN 'Irish'
         WHEN m.NHSDEthnicity = 'C' THEN 'Any Other White Background'
         WHEN m.NHSDEthnicity = 'D' THEN 'White and Black Caribbean' 
         WHEN m.NHSDEthnicity = 'E' THEN 'White and Black African'
         WHEN m.NHSDEthnicity = 'F' THEN 'White and Asian'
         WHEN m.NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
         WHEN m.NHSDEthnicity = 'H' THEN 'Indian'
         WHEN m.NHSDEthnicity = 'J' THEN 'Pakistani'
         WHEN m.NHSDEthnicity = 'K' THEN 'Bangladeshi'
         WHEN m.NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
         WHEN m.NHSDEthnicity = 'M' THEN 'Caribbean'
         WHEN m.NHSDEthnicity = 'N' THEN 'African'
         WHEN m.NHSDEthnicity = 'P' THEN 'Any Other Black Background'
         WHEN m.NHSDEthnicity = 'R' THEN 'Chinese'
         WHEN m.NHSDEthnicity = 'S' THEN 'Any Other Ethnic group'
         WHEN m.NHSDEthnicity = 'Z' THEN 'Not Stated'
         WHEN m.NHSDEthnicity = '99' THEN 'Not Known'
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
     END AS IMD_Quintile 
    
 FROM $db_source.MHS101Referral r
 
 INNER JOIN 
     $db_source.MHS102ServiceTypeReferredTo s 
                 ON s.RecordNumber = r.RecordNumber 
                 AND s.UniqServReqID = r.UniqServReqID 
                 AND s.ServTeamTypeRefToMH IN ('C02','F02')
                 and s.uniqmonthid between '$end_month_id'-11 AND '$end_month_id'
 
 INNER JOIN 
     $db_source.MHS001MPI m
                 ON m.recordnumber = r.recordnumber 
                 AND CASE WHEN m.GenderIDCode IN ('1','2','3','4','X','Z') THEN m.GenderIDCode ELSE m.Gender END = '2' 
                 --AND m.DER_GENDER = '2'
                 AND (m.LADistrictAuth IS NULL OR m.LADistrictAuth LIKE ('E%') or ladistrictauth = '')
                 and m.uniqmonthid between '$end_month_id'-11 AND '$end_month_id'
 LEFT JOIN $db_output.MHB_ORG_DAILY o on r.OrgIDProv = o.ORG_CODE
 left join $db_output.la la on la.level = case when LEFT(m.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(m.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(m.LADistrictAuth,1) = 'W' then 'W'
                                         when m.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when m.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when m.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when m.LADistrictAuth = '' then 'Unknown'
                                         when m.LADistrictAuth is not null then m.LADistrictAuth
                                         else 'Unknown' end 
 LEFT JOIN $db_output.STP_Region_mapping as mp on m.OrgIDCCGRes = mp.CCG_code
 left join $reference_db.ENGLISH_INDICES_OF_DEP_V02 r 
                     on m.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 WHERE  r.UniqMonthID BETWEEN '$end_month_id'-11 AND '$end_month_id'

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW conts12 AS
 
 SELECT
 r.UniqMonthID,
 r.Person_ID,
 r.RecordNumber,
 r.LADistrictAuth,
 r.OrgIDProv,
 r.OrgIDCCGRes,
 r.UniqServReqID,
 c.UniqCareContID,
 c.CareContDate AS Der_ContactDate,
 r.STP_Code,
 r.Region_Code
 
 FROM Refs r
 
 INNER JOIN $db_source.MHS201CareContact c 
        ON 
        r.RecordNumber = c.RecordNumber AND
        r.UniqServReqID = c.UniqServReqID 
        --AND r.Person_id = c.Person_ID -- Revised method
        AND c.AttendOrDNACode IN ('5','6')
        AND ((c.ConsMechanismMH IN ('01','03') and c.UNiqMonthID <= 1458) ---v4.1 data
             or
              (c.ConsMechanismMH IN ('01','11') and c.UNiqMonthID > 1458)) ---v5 data
        --AND c.UniqMonthID between '$start_month_id' and '$end_month_id' -- Revised method

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW contYTD AS
 
 SELECT
        c.UniqMonthID,
        c.Person_ID,
        c.RecordNumber,
        c.LADistrictAuth,
        c.OrgIDProv,
        c.OrgIDCCGRes,
        c.UniqServReqID,
        c.UniqCareContID,
        c.STP_Code,
        c.Region_Code,
        
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.LADistrictAuth ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS AccessLARN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDCCGRes ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS AccessCCGRN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDProv ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS AccessProvRN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS AccessEngRN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.STP_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS AccessSTPRN,
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.Region_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS AccessRegionRN
 
 FROM Conts12 c
 
 WHERE c.UniqMonthID BETWEEN '$end_month_id'-11 AND '$end_month_id'

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Perinatal_Master;
 CREATE TABLE        $db_output.Perinatal_Master USING DELTA AS
 
 SELECT DISTINCT
 r.UniqMonthID,
 r.Person_ID,
 r.UniqServReqID,
 r.OrgIDProv,
 r.Provider_Name,
 get_provider_type_code(r.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(r.OrgIDProv) as ProvTypeName,
 r.CCG_CODE,
 r.CCG_NAME,
 r.STP_CODE,
 r.STP_NAME, 
 r.REGION_CODE,
 r.REGION_NAME,
 r.LADistrictAuth,
 r.LADistrictAuthName,
 r.age_group_lower_chap11,
 r.age_group_higher_level,
 r.UpperEthnicity,
 r.LowerEthnicityCode,
 r.LowerEthnicityName,
 r.Der_Gender,
 g.Der_Gender_Desc,
 r.IMD_Quintile,
 r.IMD_Decile,
 CASE WHEN c1.Contacts >0 THEN 1 ELSE NULL END AS AttContacts,
 
 CASE WHEN c1.Contacts = 0 THEN 1 ELSE NULL END AS NotAttContacts,
 
 c2.AccessLARN, 
 c2.AccessProvRN,
 c2.AccessCCGRN,
 c2.AccessSTPRN,
 c2.AccessRegionRN,
 c2.AccessEngRN
 
 FROM Refs r    
 
 LEFT JOIN 
        (SELECT
              c.UniqMonthID,
              c.RecordNumber,
              c.UniqServReqID,
              COUNT(c.UniqCareContID) AS Contacts
        FROM Conts12 c
        GROUP BY c.UniqMonthID, c.RecordNumber, c.UniqServReqID) c1
              ON r.RecordNumber = c1.RecordNumber
              AND r.UniqServReqID = c1.UniqServReqID
 
 LEFT JOIN ContYTD c2 
        ON r.RecordNumber = c2.RecordNumber
        AND r.UniqServReqID = c2.UniqServReqID
         AND (c2.AccessProvRN = 1 
               OR c2.AccessCCGRN = 1
               OR c2.AccessSTPRN = 1
               OR c2.AccessRegionRN = 1)   
 
 LEFT JOIN $db_output.Der_Gender g on r.Der_Gender = g.Der_Gender

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Perinatal_Master