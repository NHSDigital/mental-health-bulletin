# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_refs1;
 CREATE TABLE         $db_output.CMH_Access_Refs1 USING DELTA AS
 ---identify all core community mental health referrals in financial year
 SELECT	            Der_FY,
                     UniqMonthID,
                     OrgIDProv,
                     Person_ID,
                     RecordNumber,
                     UniqServReqID,
                     CONCAT(Person_ID, UniqServReqID) as UniqPersRefID,
                     OrgIDCCGRes
 FROM                $db_output.NHSE_Pre_Proc_Referral
 WHERE               AgeServReferRecDate >= 18 --people aged 18 and over
                     AND ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10') ---core community MH teams
                     AND UniqMonthID BETWEEN $month_id_start AND $month_id_end ---in financial year
                     AND (LADistrictAuth LIKE 'E%' OR LADistrictAuth IS NULL OR LADistrictAuth = '') ---to include England or blank Local Authorities only               

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_outpatient_refs3;
 CREATE TABLE         $db_output.CMH_Access_Outpatient_Refs3 USING DELTA AS
 ---remove core community mental health referrals to inpatient services
 SELECT 
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 r.OrgIDCCGRes
 FROM $db_output.CMH_Access_Refs1 r
 LEFT JOIN $db_output.NHSE_Pre_Proc_Inpatients i ON r.UniqPersRefID = i.UniqPersRefID AND r.Der_FY = i.Der_FY
 WHERE i.UniqPersRefID is null ---no inpatient person_id/referral_id have hospital spell

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_activity;
 CREATE TABLE         $db_output.CMH_Activity USING DELTA AS
 ---build der_directcontactorder derivation for direct and attended activity only
 SELECT 
 a.Der_ActivityType,
 a.Der_ActivityUniqID,
 a.Person_ID,
 a.Der_PersonID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.RecordNumber,
 a.UniqServReqID,	
 a.Der_ContactDate,
 a.Der_ContactTime,
 ROW_NUMBER() OVER (PARTITION BY a.Der_PersonID, a.UniqServReqID ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_DirectContactOrder ---earliest contact date/time activity_id
 FROM $db_output.NHSE_Pre_Proc_Activity a
 WHERE a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND ---direct and attended activity only
 ((a.ConsMechanismMH NOT IN ('05', '06') AND a.UniqMonthID < 1459) OR a.ConsMechanismMH IN ('01', '02', '04', '11')) -- new for v5

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_activity_linked;
 CREATE TABLE         $db_output.CMH_Activity_Linked USING DELTA AS
 ---link referrals to direct and attended activity and add geographic data
 SELECT
 r.UniqMonthID,
 r.OrgIDProv,
 od.NAME as Provider_Name,
 ccg21.CCG21CDH as CCG_Code, 
 ccg21.CCG21NM as CCG_Name, 
 ccg21.NHSER21CDH as Region_Code, 
 ccg21.NHSER21NM as Region_Name,
 ccg21.STP21CDH as STP_Code,
 ccg21.STP21NM as STP_Name,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 ROW_NUMBER() OVER (PARTITION BY a.Person_ID, a.UniqServReqID ORDER BY a.Der_DirectContactOrder ASC) AS Der_DirectContactOrder
 FROM $db_output.CMH_Activity a
 INNER JOIN $db_output.CMH_Access_Outpatient_Refs3 r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID AND a.Der_DirectContactOrder IS NOT NULL ---direct and attended contacts only
 LEFT JOIN $db_output.ccg_mapping_2021 ccg21 ON r.OrgIDCCGRes = ccg21.CCG_UNMAPPED
 LEFT JOIN $db_output.mhb_org_daily od ON r.OrgIDProv = od.ORG_CODE

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_rolling_activity1;
 CREATE TABLE         $db_output.CMH_Rolling_Activity1 USING DELTA AS
 ---count each person once at each org level (England, provider, ccg, stp, region)
 SELECT
 	a.UniqMonthID,
 	a.OrgIDProv,
     COALESCE(a.Provider_Name, 'Unknown') as Provider_Name,
 	COALESCE(a.CCG_Code, 'Unknown') as CCG_Code,
     COALESCE(a.CCG_Name, 'Unknown') as CCG_Name,
 	COALESCE(a.Region_Code, 'Unknown') as Region_Code,
     COALESCE(a.Region_Name, 'Unknown') as Region_Name,
 	COALESCE(a.STP_Code, 'Unknown') as STP_Code,
     COALESCE(a.STP_Name, 'Unknown') as STP_Name,
 	a.Person_ID,
 	a.RecordNumber,
 	a.UniqServReqID,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.CCG_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessCCGRN, ---ccg
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_DirectContactOrder ASC) AS AccessProvRN, ---provider
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_DirectContactOrder ASC) AS AccessEngRN, ---england
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.STP_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessSTPRN, ---stp
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.Region_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessRegionRN ---commissioning region
 
 FROM $db_output.CMH_Activity_Linked a
 
 WHERE a.Der_DirectContactOrder = 2  ---people who have had 2 or more direct attended contacts
 AND a.UniqMonthID between $month_id_start and $month_id_end  ---getting only data for RP in question

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_rolling_activity;
 CREATE TABLE         $db_output.CMH_Rolling_Activity USING DELTA AS
 ---final prep table for people with cmh access with added demographic details using mpi table
 SELECT b.*,
         CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
             WHEN Gender IN ('1','2','9') THEN Gender
             ELSE 'Unknown' END AS Der_Gender,
         case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
              when AgeRepPeriodEnd >= 18 then '18 and over' 
              else 'Unknown' end as age_group_higher_level, 
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
            CASE WHEN a.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                 WHEN a.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                 WHEN a.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                 WHEN a.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                 WHEN a.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                 WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
                 WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
                 ELSE 'Unknown' END AS UpperEthnicity,
            CASE WHEN a.NHSDEthnicity = 'A' THEN 'A'
                  WHEN a.NHSDEthnicity = 'B' THEN 'B'
                  WHEN a.NHSDEthnicity = 'C' THEN 'C'
                  WHEN a.NHSDEthnicity = 'D' THEN 'D'
                  WHEN a.NHSDEthnicity = 'E' THEN 'E'
                  WHEN a.NHSDEthnicity = 'F' THEN 'F'
                  WHEN a.NHSDEthnicity = 'G' THEN 'G'
                  WHEN a.NHSDEthnicity = 'H' THEN 'H'
                  WHEN a.NHSDEthnicity = 'J' THEN 'J'
                  WHEN a.NHSDEthnicity = 'K' THEN 'K'
                  WHEN a.NHSDEthnicity = 'L' THEN 'L'
                  WHEN a.NHSDEthnicity = 'M' THEN 'M'
                  WHEN a.NHSDEthnicity = 'N' THEN 'N'
                  WHEN a.NHSDEthnicity = 'P' THEN 'P'
                  WHEN a.NHSDEthnicity = 'R' THEN 'R'
                  WHEN a.NHSDEthnicity = 'S' THEN 'S'
                  WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
                  WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
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
         
 FROM $db_output.CMH_Rolling_Activity1 as b
 LEFT JOIN $db_source.mhs001MPI as a on a.RecordNumber = b.RecordNumber
 left join       [DATABASE].[DEPRIVATION_REF] r 
                     on a.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_admissions;
 CREATE TABLE         $db_output.CMH_Admissions USING DELTA AS
 ---get all admissions in the financial year
 SELECT	
 	 i.UniqMonthID
 	,i.UniqHospProvSpellID --updated for v5
 	,i.Person_ID
     ,CASE WHEN r.NHSDEthnicity NOT IN ('A','99','-1') THEN 1 ELSE 0 END as NotWhiteBritish 
 	,CASE WHEN r.NHSDEthnicity = 'A' THEN 1 ELSE 0 END as WhiteBritish     
 	,i.OrgIDProv
     ,od.NAME as Provider_Name
     ,COALESCE(ccg21.CCG21CDH, CASE WHEN r.OrgIDCCGRes = 'X98' THEN 'UNKNOWN' ELSE r.OrgIDCCGRes END, 'UNKNOWN') as CCG_Code
     ,COALESCE(ccg21.CCG21NM, 'UNKNOWN') as CCG_Name
     ,COALESCE(ccg21.NHSER21CDH, 'UNKNOWN') as Region_Code
     ,COALESCE(ccg21.NHSER21NM, 'UNKNOWN') as Region_Name
     ,COALESCE(ccg21.STP21CDH, 'UNKNOWN') as STP_Code
     ,COALESCE(ccg21.STP21NM, 'UNKNOWN') as STP_Name
 	,i.StartDateHospProvSpell
 	,i.StartTimeHospProvSpell 
 	,date_add(last_day(add_months(i.StartDateHospProvSpell, -1)),1) AS Adm_month
 	,ia.HospitalBedTypeMH
 	,r.AgeServReferRecDate
 	,r.UniqServReqID 
 	,r.UniqMonthID AS RefMonth
 	,r.RecordNumber AS RefRecordNumber 
     ,i.LSOA2011
 	,ROW_NUMBER() OVER (PARTITION BY i.UniqHospProvSpellID ORDER BY r.UniqMonthID DESC, r.RecordNumber DESC) AS RN --new for v5
     -- added because joining to refs produces some duplicates --this also gets latest admission for a person in the same provider in the month
 	
 FROM $db_output.NHSE_Pre_Proc_Inpatients i 	
 	
 LEFT JOIN $db_output.CMH_Inpatients ia 
           ON i.UniqHospProvSpellID = ia.UniqHospProvSpellID --updated for v5
           AND i.Person_ID = ia.Person_ID 
           AND i.UniqServReqID = ia.UniqServReqID  ----- records are partitioned on spell, person and ref : therefore have joined on spell, person and ref	
 	      AND ia.Der_FirstWardStayRecord = 1 ---- ward stay at admission
 	
 LEFT JOIN $db_output.NHSE_Pre_Proc_Referral r ON i.RecordNumber = r.RecordNumber 
                                               AND i.Person_ID = r.Person_ID 
                                               AND i.UniqServReqID = r.UniqServReqID 
                                               AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth = '') 	
 LEFT JOIN $db_output.ccg_mapping_2021 ccg21 ON r.OrgIDCCGRes = ccg21.CCG_UNMAPPED  --- regions/stps taken from CCG rather than provider 
 LEFT JOIN $db_output.mhb_org_daily od ON i.OrgIDProv = od.ORG_CODE
 	
 WHERE i.StartDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'	
 AND i.UniqMonthID BETWEEN $month_id_start AND $month_id_end	---getting only data for RP in question
 AND ia.HospitalBedTypeMH IN ('10','11','12') --- adult/older acute and PICU admissions only  	
 AND i.SourceAdmMHHospProvSpell NOT IN ('49','53','87') --- excluding people transferred from other MH inpatient settings --updated for v5

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_previous_contacts;
 CREATE TABLE         $db_output.CMH_Previous_Contacts USING DELTA AS
 ---get previous attended and direct or indirect contacts for people admitted in the financial year
 SELECT 	
 	 a.UniqHospProvSpellID --updated for v5
 	,a.OrgIDProv as Adm_OrgIDProv 
 	,a.Person_ID
 	,a.StartDateHospProvSpell
 	,a.StartTimeHospProvSpell
 	,c.UniqServReqID 
 	,c.Der_ActivityUniqID
 	,c.OrgIDProv as Cont_OrgIDProv 
 	,c.Der_ActivityType 
 	,c.AttendOrDNACode 
 	,c.ConsMechanismMH 
 	,c.Der_ContactDate 
 	,i.UniqHospProvSPellID As Contact_spell
 	,DATEDIFF(a.StartDateHospProvSpell, c.Der_ContactDate) AS TimeToAdm 
 	,ROW_NUMBER() OVER(PARTITION BY a.UniqHospProvSpellID ORDER BY c.Der_ContactDate DESC) AS RN --- order to get most recent contact prior referral for each hospital spell 
 	
 FROM $db_output.NHSE_Pre_Proc_Activity c 	
 	
 INNER JOIN $db_output.CMH_Admissions a ON c.Person_ID = a.Person_ID --- same person 	
 	AND DATEDIFF(a.StartDateHospProvSpell, c.Der_ContactDate) <= 365 --- contact up to 1yr before admission
 	AND DATEDIFF(a.StartDateHospProvSpell, c.Der_ContactDate) > 2 --- exclude contacts in two days before admission 
 	AND a.RN = 1 ---has to be related to latest admission of a person in a given month accounting for duplicate referral records
 	
 LEFT JOIN $db_output.CMH_Inpatients i 
     ON c.Person_ID = i.Person_ID AND c.UniqServReqID = i.UniqServReqID AND i.Der_HospSpellRecordOrder = 1	
 	AND i.UniqHospProvSpellID IS NULL --- to get contacts related to inpatient activity and then exclude them --updated for v5
 	
 WHERE 	
 (c.Der_ActivityType = 'DIRECT' AND c.AttendOrDNACode IN ('5','6') AND 
 ((c.ConsMechanismMH NOT IN ('05', '06') AND c.UniqMonthID < 1459) OR c.ConsMechanismMH IN ('01', '02', '04', '11'))) --updated for v5 
 OR c.Der_ActivityType = 'INDIRECT'	

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_agg;
 CREATE TABLE         $db_output.CMH_Agg USING DELTA AS
 ---get ccg admissions and admissions for people known to services with added demographics using the mpi table
 SELECT 
 	 a.Adm_month AS ReportingPeriodStartDate
     ,a.OrgIDProv
     ,a.Provider_Name
 	,a.Region_Code as Region_Code
     ,a.Region_Name as Region_Name	
 	,a.CCG_Code as CCG_Code
     ,a.CCG_Name as CCG_Name
 	,a.STP_Code as STP_Code
     ,a.STP_Name as STP_Name
     ,CASE 
 		WHEN a.WhiteBritish = 1 THEN 'White British' 
 		WHEN a.NotWhiteBritish = 1 THEN 'Non-white British' 
 		ELSE 'Missing/invalid' 
 	END as Ethnicity,
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
     ,COUNT(DISTINCT a.UniqHospProvSpellID) AS Admissions --updated for v5
     ,SUM(CASE WHEN p.UniqHospProvSpellID IS NOT NULL THEN 1 ELSE 0 END) as Contact --updated for v5
 	,SUM(CASE WHEN p.UniqHospProvSpellID IS NULL THEN 1 ELSE 0 END) as NoContact --updated for v5
 
 FROM $db_output.CMH_Admissions a
 
 LEFT JOIN $db_output.CMH_Previous_Contacts p ON a.UniqHospProvSpellID = p.UniqHospProvSpellID AND p.RN = 1 --updated for v5
 LEFT JOIN $db_source.mhs001MPI as m on a.RefRecordNumber = m.RecordNumber
 left join [DATABASE].[DEPRIVATION_REF] r 
                     on a.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 
 WHERE a.RN = 1 ---latest admission for a person in the same provider in the month
 
 GROUP BY 
 a.Adm_month, 
 a.OrgIDProv, 
 a.Provider_Name,
 a.Region_Code,
 a.Region_Name,	
 a.CCG_Code,
 a.CCG_Name,
 a.STP_Code,
 a.STP_Name,
 CASE 
 		WHEN a.WhiteBritish = 1 THEN 'White British' 
 		WHEN a.NotWhiteBritish = 1 THEN 'Non-white British' 
 		ELSE 'Missing/invalid' 
 	END,
         CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
             WHEN Gender IN ('1','2','9') THEN Gender
             ELSE 'Unknown' END,
         case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
              when AgeRepPeriodEnd >= 18 then '18 and over' 
              else 'Unknown' end,
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
              when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end,
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
               WHEN AgeRepPeriodEnd >= 90 THEN '90 or over' else 'Unknown' END,
          CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 17 THEN 'Under 18'
               WHEN AgeRepPeriodEnd BETWEEN 18 and 24 THEN '18 to 24'
               WHEN AgeRepPeriodEnd BETWEEN 25 and 29 THEN '25 to 29'                
               WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
               WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
               WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
               WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
               WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
               WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
               WHEN AgeRepPeriodEnd >= 60 THEN '60 or over' else 'Unknown' END,
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
                WHEN AgeRepPeriodEnd > 64 THEN '65 and over' ELSE 'Unknown' END,  
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
                WHEN AgeRepPeriodEnd > 90 THEN '90 or over' ELSE 'Unknown' END ,
            CASE WHEN NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                 WHEN NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                 WHEN NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                 WHEN NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                 WHEN NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                 WHEN NHSDEthnicity = 'Z' THEN 'Not Stated'
                 WHEN NHSDEthnicity = '99' THEN 'Not Known'
                 ELSE 'Unknown' END,
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
                          ELSE 'Unknown' END,
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
               END,
               CASE 
               WHEN r.DECI_IMD IN (9, 10) THEN '05 Least deprived'
               WHEN r.DECI_IMD IN (7, 8) THEN '04'
               WHEN r.DECI_IMD IN (5, 6) THEN '03'
               WHEN r.DECI_IMD IN (3, 4) THEN '02'
               WHEN r.DECI_IMD IN (1, 2) THEN '01 Most deprived'                          
               ELSE 'Unknown' 
               END