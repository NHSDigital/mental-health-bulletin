# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
IMD_year = dbutils.widgets.get("IMD_year")

# COMMAND ----------

# DBTITLE 1,This section replicates the code from Create_STP_Region_Mapping


# COMMAND ----------

# DBTITLE 1,Identify all Core Community MH Referrals in RP
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_refs1;
 CREATE TABLE         $db_output.CMH_Access_Refs1 USING DELTA AS
 
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
                     AND (('$end_month_id' <= '1476' AND ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10'))
                     OR ('$end_month_id' > '1476' AND ServTeamTypeRefToMH IN ('A05','A06','A08','A09','A12','A13','A16','C03','C10','A14','D05')))-- Core community MH teams, two teams added for April 2023 onwards
                     AND UniqMonthID BETWEEN $start_month_id AND $end_month_id
                     AND (LADistrictAuth LIKE 'E%' OR LADistrictAuth IS NULL OR LADistrictAuth = '') --to include England or blank Local Authorities only               

# COMMAND ----------

# DBTITLE 1,Remove Referrals to Inpatient Services
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_access_outpatient_refs3;
 CREATE TABLE         $db_output.CMH_Access_Outpatient_Refs3 USING DELTA AS
 SELECT 
 r.UniqMonthID,
 r.OrgIDProv,
 r.Person_ID,
 r.RecordNumber,
 r.UniqServReqID,
 r.OrgIDCCGRes
 FROM $db_output.CMH_Access_Refs1 r
 LEFT JOIN $db_output.NHSE_Pre_Proc_Inpatients i ON r.UniqPersRefID = i.UniqPersRefID AND r.Der_FY = i.Der_FY
 WHERE i.UniqPersRefID is null
 ---Same as DELETE from Refs1 WHERE Person and Referral_ID and UniqMonthID are in Inpatient Table (NHS E Methodology)

# COMMAND ----------

# DBTITLE 1,Build Der_DirectContactOrder derivation for Direct and Attended Activity only
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_activity;
 CREATE TABLE         $db_output.CMH_Activity USING DELTA AS
 
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
 ROW_NUMBER() OVER (PARTITION BY a.Der_PersonID, a.UniqServReqID ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_DirectContactOrder
 FROM $db_output.NHSE_Pre_Proc_Activity a
 WHERE a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND 
 ((a.ConsMechanismMH NOT IN ('05', '06') AND a.UniqMonthID < 1459) OR a.ConsMechanismMH IN ('01', '02', '04', '11')) -- new for v5

# COMMAND ----------

# DBTITLE 1,Link Referrals to Direct and Attended Activity
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_activity_linked;
 CREATE TABLE         $db_output.CMH_Activity_Linked USING DELTA AS
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
 INNER JOIN $db_output.CMH_Access_Outpatient_Refs3 r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID AND a.Der_DirectContactOrder IS NOT NULL
 
 LEFT JOIN $db_output.CCG_MAPPING_2021_v2 ccg21 ON r.OrgIDCCGRes = ccg21.CCG_UNMAPPED
 LEFT JOIN $db_output.mhb_org_daily  od ON r.OrgIDProv = od.ORG_CODE

# COMMAND ----------

# DBTITLE 1,Count each Person once at each Org Level (England, Provider, CCG, STP, Region)
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_rolling_activity1;
 CREATE TABLE         $db_output.CMH_Rolling_Activity1 USING DELTA AS
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
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.CCG_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessCCGRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_DirectContactOrder ASC) AS AccessProvRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_DirectContactOrder ASC) AS AccessEngRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.STP_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessSTPRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.Region_Code ORDER BY a.Der_DirectContactOrder ASC) AS AccessRegionRN
 
 FROM $db_output.CMH_Activity_Linked a
 
 WHERE a.Der_DirectContactOrder = 2  ---people who have had 2 or more direct attended contacts
 AND a.UniqMonthID between $start_month_id and $end_month_id  ---getting only data for RP in question - NOT ACTUALLY NEEDED AS LIMITED TO 12 MONTHS RECORDS ALREADY

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_rolling_activity;
 CREATE TABLE         $db_output.CMH_Rolling_Activity USING DELTA AS
 SELECT b.*,
 get_provider_type_code(b.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(b.OrgIDProv) as ProvTypeName,
 Der_Gender,
 Der_Gender_Desc,
 age_group_higher_level,
 age_group_lower_common,                       
 UpperEthnicity,
 LowerEthnicityCode,
 LowerEthnicityName,
 IMD_Decile,
 IMD_Quintile
 
 FROM $db_output.CMH_Rolling_Activity1 as b
 LEFT JOIN $db_output.mpi as a on a.person_Id = b.person_id

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.cmh_rolling_activity

# COMMAND ----------

# DBTITLE 1,This Section replicates the code from CMH Access Reduced


# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cmh_inpatients;
 CREATE TABLE         $db_output.CMH_Inpatients USING DELTA AS
 SELECT
 MHS501UniqID,
 Person_ID,
 OrgIDProv,
 UniqMonthID,
 RecordNumber,
 UniqHospProvSpellID, --updated for v5
 UniqServReqID,
 CONCAT(Person_ID, UniqServReqID) as UniqPersRefID,
 StartDateHospProvSpell,
 StartTimeHospProvSpell,
 MHS502UniqID,
 UniqWardStayID,
 StartDateWardStay,
 StartTimeWardStay,
 SiteIDOfTreat,
 WardType,
 SpecialisedMHServiceCode,
 HospitalBedTypeMH,
 EndDateWardStay,
 EndTimeWardStay,
 ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, UniqHospProvSpellID ORDER BY UniqMonthID DESC) AS Der_HospSpellRecordOrder, --updated for v5
 ROW_NUMBER () OVER(PARTITION BY Person_ID, UniqServReqID, UniqHospProvSpellID ORDER BY UniqMonthID ASC, EndDateWardStay ASC, MHS502UniqID ASC) AS Der_FirstWardStayRecord --updated for v5
 FROM $db_output.NHSE_Pre_Proc_Inpatients

# COMMAND ----------

# DBTITLE 1,Get all admissions in the RP
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_admissions;
 CREATE TABLE         $db_output.CMH_Admissions USING DELTA AS
 SELECT	
 	 i.UniqMonthID
 	,i.UniqHospProvSpellID --updated for v5
 	,i.Person_ID
     ,CASE WHEN r.NHSDEthnicity NOT IN ('A','99','-1') THEN 1 ELSE 0 END as NotWhiteBritish 
 	,CASE WHEN r.NHSDEthnicity = 'A' THEN 1 ELSE 0 END as WhiteBritish     
 	,i.OrgIDProv
     ,od.NAME as Provider_Name
     ,COALESCE(ccg21.CCG21CDH, CASE WHEN r.OrgIDCCGRes = 'X98' THEN 'Unknown' ELSE r.OrgIDCCGRes END, 'Unknown') as CCG_Code
     ,COALESCE(ccg21.CCG21NM, 'Unknown') as CCG_Name
     ,COALESCE(ccg21.NHSER21CDH, 'Unknown') as Region_Code
     ,COALESCE(ccg21.NHSER21NM, 'Unknown') as Region_Name
     ,COALESCE(ccg21.STP21CDH, 'Unknown') as STP_Code
     ,COALESCE(ccg21.STP21NM, 'Unknown') as STP_Name
 	,i.StartDateHospProvSpell
 	,i.StartTimeHospProvSpell 
 	,date_add(last_day(add_months(i.StartDateHospProvSpell, -1)),1) AS Adm_month
 	,ia.HospitalBedTypeMH
 	,r.AgeServReferRecDate
 	,r.UniqServReqID 
 	,r.UniqMonthID AS RefMonth
 	,r.RecordNumber AS RefRecordNumber 
     ,r.LSOA2011
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
 LEFT JOIN $db_output.CCG_MAPPING_2021_v2 ccg21 ON r.OrgIDCCGRes = ccg21.CCG_UNMAPPED  --- regions/stps taken from CCG rather than provider 
 LEFT JOIN $db_output.mhb_org_daily od ON i.OrgIDProv = od.ORG_CODE
 	
 WHERE i.StartDateHospProvSpell BETWEEN '$rp_startdate' AND '$rp_enddate'	
 AND i.UniqMonthID BETWEEN $start_month_id AND $end_month_id	---getting only data for RP in question
 AND ia.HospitalBedTypeMH IN ('10','11','12') --- adult/older acute and PICU admissions only  	
 AND i.SourceAdmMHHospProvSpell NOT IN ('49','53','87') --- excluding people transferred from other MH inpatient settings --updated for v5

# COMMAND ----------

# DBTITLE 1,Get Previous Contacts for people admitted in the RP
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_previous_contacts;
 CREATE TABLE         $db_output.CMH_Previous_Contacts USING DELTA AS
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
 	,i.UniqHospProvSPellID As Contact_spell --- to be removed 
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

# DBTITLE 1,Get CCG Admissions and admissions for people known to services
 %sql
 DROP TABLE IF EXISTS $db_output.cmh_agg;
 CREATE TABLE         $db_output.CMH_Agg USING DELTA AS
 SELECT 
 	 a.Adm_month AS ReportingPeriodStartDate
     ,a.OrgIDProv
     ,a.Provider_Name
     ,get_provider_type_code(a.OrgIDProv) as ProvTypeCode
     ,get_provider_type_name(a.OrgIDProv) as ProvTypeName
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
         g.Der_Gender_Desc,
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
               END AS IMD_Quintile
     ,COUNT(DISTINCT a.UniqHospProvSpellID) AS Admissions --updated for v5
     ,SUM(CASE WHEN p.UniqHospProvSpellID IS NOT NULL THEN 1 ELSE 0 END) as Contact --updated for v5
 	,SUM(CASE WHEN p.UniqHospProvSpellID IS NULL THEN 1 ELSE 0 END) as NoContact --updated for v5
 
 FROM $db_output.CMH_Admissions a
 
 LEFT JOIN $db_output.CMH_Previous_Contacts p ON a.UniqHospProvSpellID = p.UniqHospProvSpellID AND p.RN = 1 --updated for v5
 LEFT JOIN $db_source.mhs001MPI as m on a.RefRecordNumber = m.RecordNumber
 left join       $reference_db.ENGLISH_INDICES_OF_DEP_V02 r 
                     on a.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 left join $db_output.Der_Gender g on CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
                                             WHEN Gender IN ('1','2','9') THEN Gender
                                             ELSE 'Unknown' END = g.Der_Gender
 
 WHERE a.RN = 1 AND a.Adm_month >= '$rp_startdate' AND last_day(a.Adm_month) <= '$rp_enddate' 
 
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
         g.Der_Gender_Desc,
         AgeRepPeriodEnd,
         case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
              when AgeRepPeriodEnd >= 18 then '18 and over' 
              else 'Unknown' end,
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end,
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

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.CMH_Agg