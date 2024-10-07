# Databricks notebook source
 %md
 DO NOT USE THE BULLETIN ASSET TABLES FOR PERINATAL. THIS LIMITS THE NUMBER OF CONTACTS 

# COMMAND ----------

 %sql
 create widget text end_month_id default "1464";
 create widget text start_month_id default "1453";
 create widget text db_source default "mh_v5_pre_clear";
 create widget text rp_enddate default "2022-03-31";
 create widget text rp_startdate default "2021-04-01";
 create widget text status default "Final";
 create widget text db_output default "sharif_salah_100137";
 create widget text populationyear default '2020';
 create widget text IMD_year default '2019';

# COMMAND ----------

# %sql
# create widget text end_month_id default "1452";
# create widget text start_month_id default "1441";
# create widget text db_source default "mh_pre_pseudo_d1";
# create widget text rp_enddate default "2021-03-31";
# create widget text rp_startdate default "2020-04-01";
# create widget text status default "Final";
# create widget text db_output default "sharif_salah_100137";
# create widget text populationyear default '2020';
# create widget text IMD_year default '2019';

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
    Case 
      when m.UniqMonthID <= 1467  then m.OrgIDCCGRes -- Case When added to add CCG/SubICB derivation according to month
      when m.UniqMonthID > 1467  then m.OrgIDSubICBLocResidence
      else "UNKNOWN" end as OrgIDCCGRes,
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
     ELSE 'UNKNOWN' END AS Der_Gender,
    COALESCE(mp.CCG_CODE,'UNKNOWN') AS CCG_CODE,
    COALESCE(mp.CCG_NAME, 'UNKNOWN') as CCG_NAME,
    COALESCE(mp.STP_CODE, 'UNKNOWN') as STP_CODE,
    COALESCE(mp.STP_NAME, 'UNKNOWN') as STP_NAME, 
    COALESCE(mp.REGION_CODE, 'UNKNOWN') as REGION_CODE,
    COALESCE(mp.REGION_NAME, 'UNKNOWN') as REGION_NAME,
    case when LEFT(m.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(m.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(m.LADistrictAuth,1) = 'W' then 'W'
                           when m.LADistrictAuth = 'L99999999' then 'L99999999'
                           when m.LADistrictAuth = 'M99999999' then 'M99999999'
                           when m.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'UNKNOWN'
                           when m.LADistrictAuth = '' then 'UNKNOWN'
                           else la.level end as LADistrictAuth,
    la.level_description as LADistrictAuthName,
    coalesce(g.Der_Gender_Desc, "UNKNOWN") as Der_Gender_Desc,
    coalesce(ab.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
    coalesce(ab.age_group_lower_chap11, "UNKNOWN") as age_group_lower_chap11,                    
    coalesce(eth.upper_description, "UNKNOWN") as UpperEthnicity,
    coalesce(eth.key, "UNKNOWN") as LowerEthnicityCode,
    coalesce(eth.lower_description, "UNKNOWN") as LowerEthnicityName,
    coalesce(imd.IMD_Decile, "UNKNOWN") as IMD_Decile,
    coalesce(imd.IMD_Quintile, "UNKNOWN") as IMD_Quintile
    
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
                                         when m.LADistrictAuth = '' then 'UNKNOWN'
                                         when m.LADistrictAuth is not null then m.LADistrictAuth
                                         else 'UNKNOWN' end 
 LEFT JOIN $db_output.STP_Region_mapping as mp on Case 
      when m.UniqMonthID <= 1467  then m.OrgIDCCGRes -- Case When added to add CCG/SubICB derivation according to month
      when m.UniqMonthID > 1467  then m.OrgIDSubICBLocResidence
      else "UNKNOWN" end = mp.CCG_code
 left join $db_output.Der_Gender g on CASE WHEN m.GENDERIDCODE = '1' THEN '1'
                                           WHEN m.GENDERIDCODE = '2' THEN '2'
                                           WHEN m.GENDERIDCODE = '3' THEN '3'
                                           WHEN m.GENDERIDCODE = '4' THEN '4'
                                           WHEN m.GENDER = '1' THEN '1'
                                           WHEN m.GENDER = '2' THEN '2'
                                           WHEN m.GENDER = '9' THEN '9'
                                           ELSE 'UNKNOWN' END = g.Der_Gender 
 left join $db_output.age_band_desc ab on m.AgeRepPeriodEnd = ab.Age
 left join $db_output.NHSDEthnicityDim eth on m.NHSDEthnicity = eth.key
 left join reference_data.ENGLISH_INDICES_OF_DEP_V02 r 
                     on m.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 left join $db_output.imd_desc imd on r.DECI_IMD = imd.IMD_Number      
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
 r.Der_Gender_Desc,
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

# COMMAND ----------

 %sql
 select count(distinct Person_ID) from $db_output.Perinatal_Master 
 where AccessEngRN = 1

# COMMAND ----------

 %sql
 select Person_ID, count(distinct Region_Code) from $db_output.Perinatal_Master 
 where AccessRegionRN = 1
 group by Person_ID
 order by 2 desc

# COMMAND ----------

 %sql
 select * from $db_output.Perinatal_Master 
 where AccessRegionRN = 1 and Person_ID = "4569263941"

# COMMAND ----------

 %sql
 select sum(count)
 from (select CCG_Code, count(distinct Person_ID) as count from $db_output.Perinatal_Master 
 where AccessCCGRN = 1 group by CCG_Code)

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Perinatal_Master