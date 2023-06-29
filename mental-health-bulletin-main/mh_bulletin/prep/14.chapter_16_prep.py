# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.spells;
 CREATE TABLE $db_output.spells USING DELTA AS 
 ---final prep table for length of stay metrics using hopsital spell and ward stay tables. Demographics and georgaphical levels added via derived MPI table
 SELECT 
 A.PERSON_ID, 
 A.UniqHospProvSpellID,
 A.OrgIDProv,
 E.NAME AS PROV_NAME,
 A.StartDateHospProvSpell,
 A.DischDateHospProvSpell,
 DATEDIFF(A.DischDateHospProvSpell, A.StartDateHospProvSpell) as HOSP_LOS,
 B.UniqWardStayID,
 B.HospitalBedTypeMH,
 B.StartDateWardStay,
 B.EndDateWardStay,
 DATEDIFF(B.EndDateWardStay, B.StartDateWardStay) as WARD_LOS,
 C.AgeRepPeriodEnd,
 COALESCE(F.ORG_CODE,'UNKNOWN') AS IC_REC_CCG,
 COALESCE(F.NAME, 'UNKNOWN') as CCG_NAME,
 COALESCE(S.STP_CODE, 'UNKNOWN') as STP_CODE, 
 COALESCE(S.STP_NAME, 'UNKNOWN') as STP_NAME, 
 COALESCE(S.REGION_CODE, 'UNKNOWN') as REGION_CODE,
 COALESCE(S.REGION_NAME, 'UNKNOWN') as REGION_NAME,
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
          CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 13 THEN 'Under 14'
               WHEN AgeRepPeriodEnd BETWEEN 14 and 15 THEN '14 to 15'
               WHEN AgeRepPeriodEnd BETWEEN 16 and 17 THEN '16 to 17'
               WHEN AgeRepPeriodEnd BETWEEN 18 AND 19 THEN '18 to 19'
               WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
               WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
               WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
               WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
               WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
               WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
               WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
               WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
               WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
               WHEN AgeRepPeriodEnd BETWEEN 65 AND 69 THEN '65 to 69'
               WHEN AgeRepPeriodEnd BETWEEN 70 AND 74 THEN '70 to 74'
               WHEN AgeRepPeriodEnd BETWEEN 75 AND 79 THEN '75 to 79'
               WHEN AgeRepPeriodEnd BETWEEN 80 AND 84 THEN '80 to 84'
               WHEN AgeRepPeriodEnd BETWEEN 85 AND 89 THEN '85 to 89' 
               WHEN AgeRepPeriodEnd >= 90 THEN '90 or over' else 'Unknown' END As age_group_lower_chap45,
          CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 17 THEN 'Under 18'
               WHEN AgeRepPeriodEnd BETWEEN 18 and 24 THEN '18 to 24'
               WHEN AgeRepPeriodEnd BETWEEN 25 and 29 THEN '25 to 29'                
               WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
               WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
               WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
               WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
               WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
               WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
               WHEN AgeRepPeriodEnd >= 60 THEN '60 or over' else 'Unknown' END AS age_group_lower_chap7,
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
            CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
                WHEN AgeRepPeriodEnd BETWEEN 18 AND 19 THEN '18 to 19'
                WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                WHEN AgeRepPeriodEnd BETWEEN 65 AND 69 THEN '65 to 69'
                WHEN AgeRepPeriodEnd BETWEEN 70 AND 74 THEN '70 to 74'
                WHEN AgeRepPeriodEnd BETWEEN 75 AND 79 THEN '75 to 79'
                WHEN AgeRepPeriodEnd BETWEEN 80 AND 84 THEN '80 to 84'
                WHEN AgeRepPeriodEnd BETWEEN 85 AND 89 THEN '85 to 89'
                WHEN AgeRepPeriodEnd > 90 THEN '90 or over' ELSE 'Unknown' END as age_group_lower_chap12,                      
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
 FROM $db_source.MHS501HospProvSpell a 
 LEFT JOIN $db_source.MHS502WARDSTAY B on a.UniqHospProvSpellID = b.UniqHospProvSpellID and a.Recordnumber = b.RecordNumber
 LEFT JOIN $db_source.MHS001MPI C ON A.RECORDNUMBER = C.RECORDNUMBER ---using RecordNumber (so will use demographics of person in that provider at time of discharge)
 LEFT JOIN $db_output.mhb_rd_ccg_latest E ON A.ORGIDPROV = E.ORG_CODE
 LEFT JOIN $db_output.mhb_org_daily F ON C.ORGIDCCGRES = F.ORG_CODE
 LEFT JOIN $db_output.stp_region_mapping s ON COALESCE(F.ORG_CODE,'UNKNOWN') = S.CCG_CODE
 left join [DATABASE].[DEPRIVATION_REF] r 
                     on c.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 WHERE (a.RECORDENDDATE IS NULL OR a.RECORDENDDATE > '$rp_enddate') ---hospital spell in financial year
   AND a.RECORDSTARTDATE BETWEEN '$rp_startdate' AND '$rp_enddate'
   AND A.DischDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate' ---discharged within financial year