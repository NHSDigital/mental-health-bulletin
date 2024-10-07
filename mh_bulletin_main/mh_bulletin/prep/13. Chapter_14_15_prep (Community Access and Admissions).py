# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text("db_output", "sharif_salah_100137", "db_output")
# dbutils.widgets.text("db_source", "mh_v5_pre_clear", "db_source")
# dbutils.widgets.text("end_month_id", "1476", "end_month_id")
# dbutils.widgets.text("start_month_id", "1465", "start_month_id")
# dbutils.widgets.text("rp_enddate", "2023-03-31", "rp_enddate")
# dbutils.widgets.text("rp_startdate", "2022-04-01", "rp_startdate")
# dbutils.widgets.text("IMD_year", "2019", "IMD_year")

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
     COALESCE(a.Provider_Name, "UNKNOWN") as Provider_Name,
 	COALESCE(a.CCG_Code, "UNKNOWN") as CCG_Code,
     COALESCE(a.CCG_Name, "UNKNOWN") as CCG_Name,
 	COALESCE(a.Region_Code, "UNKNOWN") as Region_Code,
     COALESCE(a.Region_Name, "UNKNOWN") as Region_Name,
 	COALESCE(a.STP_Code, "UNKNOWN") as STP_Code,
     COALESCE(a.STP_Name, "UNKNOWN") as STP_Name,
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
     
     ---------- check if correct to change
     -- ,COALESCE(ccg21.CCG21CDH, CASE WHEN r.OrgIDCCGRes = 'X98' THEN "UNKNOWN" ELSE r.OrgIDCCGRes END, "UNKNOWN") as CCG_Code
     ,COALESCE(ccg21.CCG21CDH, 
         CASE WHEN r.OrgIDCCGRes in ('X98', '') THEN "UNKNOWN" 
         when r.OrgIDCCGRes is null then "UNKNOWN"
         ELSE r.OrgIDCCGRes END, 
      "UNKNOWN") as CCG_Code
     ----------
     
     ,COALESCE(ccg21.CCG21NM, "UNKNOWN") as CCG_Name
     ,COALESCE(ccg21.NHSER21CDH, "UNKNOWN") as Region_Code
     ,COALESCE(ccg21.NHSER21NM, "UNKNOWN") as Region_Name
     ,COALESCE(ccg21.STP21CDH, "UNKNOWN") as STP_Code
     ,COALESCE(ccg21.STP21NM, "UNKNOWN") as STP_Name
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
 		ELSE 'Missing/invalid' END as Ethnicity
     ,CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
         WHEN Gender IN ('1','2','9') THEN Gender
         ELSE "UNKNOWN" END AS Der_Gender
     ,coalesce(g.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc
     ,m.AgeRepPeriodEnd
     ,coalesce(ab.age_group_higher_level, "UNKNOWN") as age_group_higher_level
     ,coalesce(ab.age_group_lower_common, "UNKNOWN") as age_group_lower_common
     ,coalesce(eth.upper_description, "UNKNOWN") as UpperEthnicity
     ,coalesce(eth.key, "UNKNOWN") as LowerEthnicityCode
     ,coalesce(eth.lower_description, "UNKNOWN") as LowerEthnicityName
     ,coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile
     ,coalesce(imd.IMD_Quintile, "UNKNOWN") as IMD_Quintile
     ,COUNT(DISTINCT a.UniqHospProvSpellID) AS Admissions --updated for v5
     ,SUM(CASE WHEN p.UniqHospProvSpellID IS NOT NULL THEN 1 ELSE 0 END) as Contact --updated for v5
 	,SUM(CASE WHEN p.UniqHospProvSpellID IS NULL THEN 1 ELSE 0 END) as NoContact --updated for v5

 FROM $db_output.CMH_Admissions a

 LEFT JOIN $db_output.CMH_Previous_Contacts p ON a.UniqHospProvSpellID = p.UniqHospProvSpellID AND p.RN = 1 --updated for v5
 LEFT JOIN $db_source.mhs001MPI as m on a.RefRecordNumber = m.RecordNumber
 left join $db_output.Der_Gender g on CASE WHEN m.GenderIDCode IN ('1','2','3','4') THEN m.GenderIDCode
                                           WHEN m.Gender IN ('1','2','9') THEN m.Gender
                                           ELSE "UNKNOWN" END = g.Der_Gender 
 left join $db_output.age_band_desc ab on m.AgeRepPeriodEnd = ab.Age    -- Age
 left join $db_output.NHSDEthnicityDim eth on m.NHSDEthnicity = eth.key
 left join reference_data.ENGLISH_INDICES_OF_DEP_V02 r 
           on m.LSOA2011 = r.LSOA_CODE_2011 
           and r.imd_year = '$IMD_year'
 left join $db_output.imd_desc imd on r.DECI_IMD = imd.IMD_Number

 WHERE a.RN = 1 AND a.Adm_month >= '$rp_startdate' AND last_day(a.Adm_month) <= '$rp_enddate' 

 GROUP BY 
 a.Adm_month
 ,a.OrgIDProv
 ,a.Provider_Name
 ,get_provider_type_code(a.OrgIDProv)
 ,get_provider_type_name(a.OrgIDProv)
 ,a.Region_Code
 ,a.Region_Name
 ,a.CCG_Code
 ,a.CCG_Name
 ,a.STP_Code
 ,a.STP_Name
 ,CASE 
     WHEN a.WhiteBritish = 1 THEN 'White British' 
     WHEN a.NotWhiteBritish = 1 THEN 'Non-white British' 
     ELSE 'Missing/invalid' END
 ,CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
     WHEN Gender IN ('1','2','9') THEN Gender
     ELSE "UNKNOWN" END
 ,coalesce(g.Der_Gender_Desc, "UNKNOWN")
 ,m.AgeRepPeriodEnd
 ,coalesce(ab.age_group_higher_level, "UNKNOWN")
 ,coalesce(ab.age_group_lower_common, "UNKNOWN")
 ,coalesce(eth.upper_description, "UNKNOWN")
 ,coalesce(eth.key, "UNKNOWN")
 ,coalesce(eth.lower_description, "UNKNOWN")
 ,coalesce(imd.IMD_Decile, "UNKNOWN")
 ,coalesce(imd.IMD_Quintile, "UNKNOWN")

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.CMH_Agg