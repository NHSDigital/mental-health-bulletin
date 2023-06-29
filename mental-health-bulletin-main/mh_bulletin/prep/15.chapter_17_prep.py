# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMPORARY VIEW Ref AS 
 ---get children and young people referrals in financial year and add in geographic and demographic details
 SELECT
 	r.UniqMonthID,
 	r.OrgIDProv,
 	CASE 
       WHEN r.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
       ELSE r.Person_ID
       END AS Person_ID,
 	r.RecordNumber,
 	r.UniqServReqID,
 	Case when r.OrgIDProv = 'DFC' then r.OrgIDComm
          when (r.UniqMonthID <= 1467 and r.OrgIDProv <> "DFC") then m.OrgIDCCGRes -- Case When added to add CCG/SubICB derivation according to month
     else "UNKNOWN" end as Der_OrgComm,
 	m.LADistrictAuth,
 	r.AgeServReferRecDate,
 	m.AgeRepPeriodEnd,
         CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
             WHEN Gender IN ('1','2','9') THEN Gender
             ELSE 'Unknown' END AS Der_Gender,
         case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
              when AgeRepPeriodEnd >= 18 then '18 and over' 
              else 'Unknown' end as age_group_higher_level,
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
                          ELSE 'Unknown' END AS LowerEthnicity,
               CASE
               WHEN i.DECI_IMD = 10 THEN '10 Least deprived'
               WHEN i.DECI_IMD = 9 THEN '09 Less deprived'
               WHEN i.DECI_IMD = 8 THEN '08 Less deprived'
               WHEN i.DECI_IMD = 7 THEN '07 Less deprived'
               WHEN i.DECI_IMD = 6 THEN '06 Less deprived'
               WHEN i.DECI_IMD = 5 THEN '05 More deprived'
               WHEN i.DECI_IMD = 4 THEN '04 More deprived'
               WHEN i.DECI_IMD = 3 THEN '03 More deprived'
               WHEN i.DECI_IMD = 2 THEN '02 More deprived'
               WHEN i.DECI_IMD = 1 THEN '01 Most deprived'
               ELSE 'Unknown'
               END AS IMD_Decile,
               CASE 
               WHEN i.DECI_IMD IN (9, 10) THEN '05 Least deprived'
               WHEN i.DECI_IMD IN (7, 8) THEN '04'
               WHEN i.DECI_IMD IN (5, 6) THEN '03'
               WHEN i.DECI_IMD IN (3, 4) THEN '02'
               WHEN i.DECI_IMD IN (1, 2) THEN '01 Most deprived'                          
               ELSE 'Unknown' 
               END AS IMD_Quintile
 
 FROM $db_source.mhs101referral r
 INNER JOIN $db_source.mhs001mpi m ON r.RecordNumber = m.RecordNumber
 left join [DATABASE].[DEPRIVATION_REF] i 
                     on m.LSOA2011 = i.LSOA_CODE_2011 
                     and i.imd_year = '$IMD_year'
 
 WHERE 
 r.AgeServReferRecDate BETWEEN 0 AND 17 AND ---age at referral between 0 and 17
 r.UniqMonthID BETWEEN '$month_id_end' -11 AND '$month_id_end' ---referrals in financial year
 AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR LADistrictAuth = '')

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Comb AS
 ---get all direct and attended care contacts and combine with indirect care contacts ever
 SELECT
     CASE 
       WHEN c.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
       ELSE c.Person_ID
       END AS Person_ID,
 	c.RecordNumber,
 	c.UniqServReqID,
 	c.CareContDate AS Der_ContactDate,
 	c.AgeCareContDate
 
 FROM $db_source.MHS201CareContact c
 LEFT JOIN $db_source.MHS001MPI m ON c.RecordNumber = m.RecordNumber
 
 WHERE 
   (
     ( c.AttendOrDNACode IN ('5', '6') and ((c.ConsMechanismMH NOT IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('01', '02', '04', '11') and c.UniqMonthID >= '1459')))
     or 
     ( ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')) and c.OrgIdProv = 'DFC') 
 UNION ALL
 
 SELECT
 	CASE 
       WHEN i.OrgIDProv = 'DFC' THEN CONCAT(m.OrgIDProv, m.LocalPatientID)
       ELSE i.Person_ID
       END AS Person_ID,
 	i.RecordNumber,
 	i.UniqServReqID,
 	i.IndirectActDate AS Der_ContactDate,
 	NULL AS AgeCareContDate
 
 FROM $db_source.MHS204IndirectActivity i
 LEFT JOIN $db_source.MHS001MPI m ON i.RecordNumber = m.RecordNumber

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Act AS 
 ---combine referrals in the financial year with all their associated contacts (where the age at contact was between 0 and 17)
 SELECT
 	r.UniqMonthID,
 	r.OrgIDProv,
 	r.Der_OrgComm,
 	r.LADistrictAuth,
 	r.Person_ID,
 	r.RecordNumber,
 	r.UniqServReqID,
 	COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) AS Der_ContAge,
 	a.Der_ContactDate,
     r.Der_Gender,
     r.age_group_higher_level,
     r.age_group_lower_chap1,
     r.UpperEthnicity,
     r.LowerEthnicity,
     r.IMD_Decile,
     r.IMD_Quintile
 
 FROM Comb a
 INNER JOIN Ref r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID
 WHERE COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) BETWEEN 0 AND 17 ---use age at care contact, if not present use age at end of financial year 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHB_FirstCont_Final;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_FirstCont_Final USING DELTA AS
 ---final prep table bringing in referrals, contacts and demographics and fields to count person and first contact at each geographic level
 SELECT
 	a.UniqMonthID,
 	a.OrgIDProv,
 	o.NAME AS PROV_NAME,
     COALESCE(c.CCG21CDH,'UNKNOWN') AS SubICB_Code,
     COALESCE(c.CCG21NM,'UNKNOWN') AS SubICB_Name,
     COALESCE(c.STP21CDH,'UNKNOWN') AS ICB_Code,
     COALESCE(c.STP21NM,'UNKNOWN') AS ICB_Name,
     COALESCE(c.NHSER21CDH,'UNKNOWN') AS Region_Code,
     COALESCE(c.NHSER21NM,'UNKNOWN') AS Region_Name,
 	a.LADistrictAuth,
 	a.Person_ID,
 	a.RecordNumber,
 	a.UniqServReqID,
     a.Der_Gender,
     a.age_group_higher_level,
     a.age_group_lower_chap1,
     a.UpperEthnicity,
     a.LowerEthnicity,
     a.IMD_Decile,
     a.IMD_Quintile,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.LADistrictAuth ORDER BY a.Der_ContactDate ASC) AS AccessLARN, ---first contact for a person and local authority district
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH ORDER BY a.Der_ContactDate ASC) AS AccessSubICBRN, ---first contact for a person and ccg
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.CCG21CDH, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessSubICBProvRN, ---first contact for a person and ccg and provider
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_ContactDate ASC) AS AccessRNProv, ---first contact for a person and provider
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_ContactDate ASC) AS AccessEngRN, ---first contact for a person
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.STP21CDH ORDER BY a.Der_ContactDate ASC) AS AccessICBRN, ---first contact for a person and stp
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, c.NHSER21CDH ORDER BY a.Der_ContactDate ASC) AS AccessRegionRN, ---first contact for a person and region
     'MHS95' AS Metric --- add metric id to table (used for calculating rates later)
 
 FROM Act as A
 LEFT JOIN $db_output.ccg_mapping_2021 C on a.Der_OrgComm = C.CCG_UNMAPPED
 LEFT JOIN $db_output.mhb_org_daily o on a.OrgIDProv = o.ORG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.MHB_FirstCont_Final