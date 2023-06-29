# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY VIEW refs AS
 ---mh bulletin asset tables not used for perinatal referrals and contacts as this limits the number of contacts. Base MHSDS tables used instead
 ---get referrals to specialist perinatal mental health community service or maternal mental health service team where GenderIDCode or Gender field is female in the financial year
 SELECT DISTINCT
    r.UniqMonthID,
    r.RecordNumber,
    r.Person_ID,
    r.ServiceRequestId,
    r.UniqServReqID,
    r.OrgIDProv,
    r.ReferralRequestReceivedDate,
    COALESCE(mp.CCG_CODE, 'Unknown') AS OrgIDCCGRes,
  --a-nnual version
  CASE
    WHEN GENDERIDCODE = '1' THEN '1'
    WHEN GENDERIDCODE = '2' THEN '2'
    WHEN GENDERIDCODE = '3' THEN '3'
    WHEN GENDERIDCODE = '4' THEN '4'
    WHEN GENDER = '1' THEN '1'
    WHEN GENDER = '2' THEN '2'
    WHEN GENDER = '9' THEN '9'
    ELSE 'UNKNOWN' END AS GENDER,
    case when LEFT(m.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(m.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(m.LADistrictAuth,1) = 'W' then 'W'
                           when m.LADistrictAuth = 'L99999999' then 'L99999999'
                           when m.LADistrictAuth = 'M99999999' then 'M99999999'
                           when m.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when m.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth,
    COALESCE(mp.STP_code, 'Unknown') AS STP_Code,
    COALESCE(mp.Region_code, 'Unknown') AS Region_Code,
    case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                      when AgeRepPeriodEnd >= 18 then '18 and over' 
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
                        WHEN AgeRepPeriodEnd > 64 THEN '65 and over' ELSE 'Unknown' END as age_group_lower_chap11, 
                    CASE WHEN NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                         WHEN NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                         WHEN NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                         WHEN NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                         WHEN NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                         WHEN NHSDEthnicity = 'Z' THEN 'Not Stated'
                         WHEN NHSDEthnicity = '99' THEN 'Not Known'
                         ELSE 'Unknown' END AS UpperEthnicity,
              CASE  WHEN NHSDEthnicity = 'A' THEN 'A'
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
                            ELSE 'Unknown' END AS LowerEthnicity,
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
 INNER JOIN $db_source.MHS102ServiceTypeReferredTo s
                 ON s.RecordNumber = r.RecordNumber 
                 AND s.UniqServReqID = r.UniqServReqID 
                 AND s.ServTeamTypeRefToMH IN ('C02','F02') ---specialist perinatal mental health community service or maternal mental health service team
                 and s.uniqmonthid between '$month_id_end'-11 AND '$month_id_end'
 
 INNER JOIN $db_source.MHS001MPI m
                 ON m.recordnumber = r.recordnumber 
                 AND CASE WHEN m.GenderIDCode IN ('1','2','3','4','X','Z') THEN m.GenderIDCode ELSE m.Gender END = '2' ---genderidcode / gender field is female
                 AND (m.LADistrictAuth IS NULL OR m.LADistrictAuth LIKE ('E%') or ladistrictauth = '')
                 and m.uniqmonthid between '$month_id_end'-11 AND '$month_id_end'
 
 left join $db_output.mhb_la la on la.level = m.LADistrictAuth
 LEFT JOIN $db_output.stp_Region_mapping as mp on m.OrgIDCCGRes = mp.CCG_code
 left join [DATABASE].[DEPRIVATION_REF]
                     on m.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 WHERE  r.UniqMonthID BETWEEN '$month_id_end'-11 AND '$month_id_end' ---referrals in finacial year only

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW conts12 AS
 ---mh bulletin asset tables not used for perinatal referrals and contacts as this limits the number of contacts. Base MHSDS tables used instead
 ---get attend direct contacts relating to referrals to specialist perinatal mental health community service or maternal mental health service team where GenderIDCode or Gender field is female in the financial year
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
 INNER JOIN $db_source.MHS201CareContact c ---direct contacts
        ON r.RecordNumber = c.RecordNumber 
        AND r.UniqServReqID = c.UniqServReqID 
        AND c.AttendOrDNACode IN ('5','6') ---attended
        AND ((c.ConsMechanismMH IN ('01','03') and c.UNiqMonthID <= 1458) ---v4.1 data
        OR  (c.ConsMechanismMH IN ('01','11') and c.UNiqMonthID > 1458)) ---v5 data

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW contYTD AS
 ---create ranking for each Person_ID and geographic level by the earliest month, contact date and contact identifier. As a patient may have had multiple contacts at different providers or have different geographic levels in the year
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
        
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.LADistrictAuth ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessLARN, ---local authority ranking
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDCCGRes ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessCCGRN, ---ccg ranking
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.OrgIDProv ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessRNProv, ---provider ranking
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessEngRN, ---national ranking
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.STP_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessSTPRN, ---stp ranking
        ROW_NUMBER () OVER(PARTITION BY c.Person_ID, c.Region_Code ORDER BY c.UniqMonthID ASC, c.Der_ContactDate ASC, c.UniqCareContID ASC) AS FYAccessRegionRN ---region ranking
 
 FROM Conts12 c
 
 WHERE c.UniqMonthID BETWEEN '$month_id_end'-11 AND '$month_id_end' ---care contact submitted in the financial year

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Perinatal_Master;
 CREATE TABLE        $db_output.Perinatal_Master USING DELTA AS
 ---brings all perinatal referrals and first contacts in financial year (for each geography) together
 SELECT DISTINCT
 r.UniqMonthID,
 r.Person_ID,
 r.UniqServReqID,
 r.OrgIDProv,
 r.OrgIDCCGRes,
 r.STP_Code,
 r.REGION_CODE,
 r.LADistrictAuth,
 r.Gender,
 r.age_group_higher_level,
 r.age_group_lower_chap11,
 r.UpperEthnicity,
 r.LowerEthnicity,
 r.IMD_Decile,
 r.IMD_Quintile,
 CASE WHEN c1.Contacts >0 THEN 1 ELSE NULL END AS AttContacts,
 CASE WHEN c1.Contacts = 0 THEN 1 ELSE NULL END AS NotAttContacts,
 
 c2.FYAccessLARN, 
 c2.FYAccessRNProv,
 c2.FYAccessCCGRN,
 c2.FYAccessSTPRN,
 c2.FYAccessRegionRN,
 c2.FYAccessEngRN
 
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
         AND (c2.FYAccessRNProv = 1 
         OR c2.FYAccessCCGRN = 1
         OR c2.FYAccessSTPRN = 1
         OR c2.FYAccessRegionRN = 1)   