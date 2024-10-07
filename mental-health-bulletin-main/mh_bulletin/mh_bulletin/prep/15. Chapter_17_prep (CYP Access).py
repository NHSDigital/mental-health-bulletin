# Databricks notebook source
 %md
 ### MHSDS V5.0 Changes
 #### AM: Dec 16 2021 - Updated code (Cmd 5)for V5.0 change - ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data

# COMMAND ----------

dbutils.widgets.text("db_output", "sharif_salah_100137", "db_output")
db_output  = dbutils.widgets.get("db_output")
assert db_output

dbutils.widgets.text("db_source", "mh_v5_pre_clear", "db_source")
db_source = dbutils.widgets.get("db_source")
assert db_source

dbutils.widgets.text("rp_enddate", "2023-03-31", "rp_enddate")
rp_enddate = dbutils.widgets.get("rp_enddate")
assert rp_enddate

dbutils.widgets.text("status", "Final", "status")
status  = dbutils.widgets.get("status")
assert status

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW Ref AS 

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
          when (r.UniqMonthID > 1467 and r.OrgIDProv <> "DFC") then m.OrgIDSubICBLocResidence
          else "UNKNOWN" end as Der_OrgComm,
     case when LEFT(m.LADistrictAuth,1) in ('N', 'W', 'S') then LEFT(m.LADistrictAuth,1)
          when m.LADistrictAuth in ('L99999999', 'M99999999', 'X99999998') then m.LADistrictAuth
          when la.level is null then "UNKNOWN"
          when m.LADistrictAuth = '' then "UNKNOWN"
          else la.level end as LADistrictAuth,
     coalesce(la.level_description, "UNKNOWN") as LADistrictAuthName, 
 	r.AgeServReferRecDate,
 	m.AgeRepPeriodEnd,
     CASE WHEN m.GenderIDCode IN ('1','2','3','4') THEN m.GenderIDCode
             WHEN m.Gender IN ('1','2','9') THEN m.Gender
             ELSE "UNKNOWN" END AS Der_Gender,
     coalesce(g.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
     coalesce(ab.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
     coalesce(ab.age_group_lower_common, "UNKNOWN") as age_group_lower_common,
     coalesce(ab.age_group_lower_chap1, "UNKNOWN") as age_group_lower_chap1,
     coalesce(eth.upper_description, "UNKNOWN") as UpperEthnicity,
     coalesce(eth.key, "UNKNOWN") as LowerEthnicityCode,
     coalesce(eth.lower_description, "UNKNOWN") as LowerEthnicityName,
     coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,
     coalesce(imd.IMD_Quintile, "UNKNOWN") as IMD_Quintile

 FROM $db_source.mhs101referral r

 INNER JOIN $db_source.mhs001mpi m ON r.RecordNumber = m.RecordNumber
 left join $db_output.Der_Gender g ON CASE WHEN m.GenderIDCode IN ('1','2','3','4') THEN m.GenderIDCode
                                           WHEN m.Gender IN ('1','2','9') THEN m.Gender
                                           ELSE "UNKNOWN" END = g.Der_Gender 
 left join $db_output.age_band_desc ab on m.AgeRepPeriodEnd = ab.Age
 left join $db_output.NHSDEthnicityDim eth on m.NHSDEthnicity = eth.key
 left join reference_data.ENGLISH_INDICES_OF_DEP_V02 r 
           on m.LSOA2011 = r.LSOA_CODE_2011 
           and r.imd_year = '$IMD_year'
 left join $db_output.imd_desc imd on r.DECI_IMD = imd.IMD_Number  
 left join $db_output.la la on la.level = case when LEFT(m.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(m.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(m.LADistrictAuth,1) = 'W' then 'W'
                                         when m.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when m.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when m.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when m.LADistrictAuth = '' then "UNKNOWN"
                                         when m.LADistrictAuth is not null then m.LADistrictAuth
                                         else "UNKNOWN" end 
 WHERE 
 r.AgeServReferRecDate BETWEEN 0 AND 17 AND 
 r.UniqMonthID BETWEEN '$end_month_id' -11 AND '$end_month_id' 
 AND (m.LADistrictAuth LIKE 'E%' OR m.LADistrictAuth IS NULL OR LADistrictAuth = '');

 select * from ref

# COMMAND ----------

# DBTITLE 1,V5 will change from 'ConsMediumUsed' to 'ConsMechanismMH' , code '06' to '09', code '05' no change
 %sql

 CREATE OR REPLACE TEMPORARY VIEW Comb AS

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
 -------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data /*** updated to v5 AM ***/
     or 
     ( ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') or (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')) and c.OrgIdProv = 'DFC')            ---/*** change from Oct2021: v5 change AM **/
    )
 --c.AttendOrDNACode IN ('5','6') AND c.ConsMechanismMH NOT IN ('05','09') OR (c.OrgIDProv = 'DFC' AND c.ConsMechanismMH IN ('05','09')) 
 -------/*** ConsMediumUsed' will change to 'ConsMechanismMH', code '06' will change to '09' from Oct 2021 data V5.0 /*** updated to v5 AM ***/
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

 SELECT
 	r.UniqMonthID,
 	r.OrgIDProv,
 	r.Der_OrgComm,
 	r.LADistrictAuth,
     r.LADistrictAuthName,
 	r.Person_ID,
 	r.RecordNumber,
 	r.UniqServReqID,
 	COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) AS Der_ContAge,
 	a.Der_ContactDate,
     r.Der_Gender,
     r.Der_Gender_Desc,
     r.age_group_higher_level,
     r.age_group_lower_common,
     r.age_group_lower_chap1,
     r.UpperEthnicity,
     r.LowerEthnicityCode,
     r.LowerEthnicityName,
     r.IMD_Decile,
     r.IMD_Quintile

 FROM Comb a

 INNER JOIN Ref r ON a.RecordNumber = r.RecordNumber AND a.UniqServReqID = r.UniqServReqID

 WHERE COALESCE(a.AgeCareContDate,r.AgeRepPeriodEnd) BETWEEN 0 AND 17

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

# DBTITLE 1,added Metric field
 %sql
 DROP TABLE IF EXISTS $db_output.MHB_FirstCont_Final;

 CREATE TABLE IF NOT EXISTS $db_output.MHB_FirstCont_Final USING DELTA AS

 SELECT
 	a.UniqMonthID,
 	a.OrgIDProv,
 	o.NAME AS Provider_Name,
     get_provider_type_code(a.OrgIDProv) as ProvTypeCode,
     get_provider_type_name(a.OrgIDProv) as ProvTypeName,
     COALESCE(m.CCG_Code,"UNKNOWN") AS CCG_Code,
     COALESCE(m.CCG_Name,"UNKNOWN") AS CCG_Name,
     COALESCE(m.STP_Code,"UNKNOWN") AS STP_Code,
     COALESCE(m.STP_Name,"UNKNOWN") AS STP_Name,
     COALESCE(m.Region_Code,"UNKNOWN") AS Region_Code,
     COALESCE(m.Region_Name,"UNKNOWN") AS Region_Name,
 	a.LADistrictAuth,
     a.LADistrictAuthName,
 	a.Person_ID,
 	a.RecordNumber,
 	a.UniqServReqID,
     a.Der_Gender,
     a.Der_Gender_Desc,
     a.age_group_higher_level,
     a.age_group_lower_common,
     a.age_group_lower_chap1,
     a.UpperEthnicity,
     a.LowerEthnicityCode,
     a.LowerEthnicityName,
     a.IMD_Decile,
     a.IMD_Quintile,
     
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.LADistrictAuth ORDER BY a.Der_ContactDate ASC, a.recordnumber DESC) AS AccessLARN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, m.CCG_Code ORDER BY a.Der_ContactDate ASC, a.recordnumber DESC) AS AccessCCGRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID, m.CCG_Code, a.OrgIDProv ORDER BY a.Der_ContactDate ASC, a.recordnumber DESC) AS AccessCCGProvRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, a.OrgIDProv ORDER BY a.Der_ContactDate ASC, a.recordnumber DESC) AS AccessProvRN,
     ROW_NUMBER () OVER(PARTITION BY a.Person_ID ORDER BY a.Der_ContactDate ASC, a.recordnumber DESC) AS AccessEngRN,    
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, m.STP_Code ORDER BY a.Der_ContactDate ASC, a.recordnumber DESC) AS AccessSTPRN,
 	ROW_NUMBER () OVER(PARTITION BY a.Person_ID, m.STP_Code ORDER BY a.Der_ContactDate ASC, a.recordnumber DESC) AS AccessRegionRN,
     'MHS95' AS Metric-- add metric id to table

 FROM Act as A
 LEFT JOIN $db_output.stp_region_mapping m on A.Der_OrgComm = m.CCG_Code
 -- LEFT JOIN $db_output.CCG_MAPPING_2021 C on a.DER_ORGCOMM = C.CCG_UNMAPPED
 LEFT JOIN $db_output.mhb_org_daily o on a.OrgIDProv = o.ORG_CODE

 --WHERE a.Der_ContactOrder = 1

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.MHB_FirstCont_Final