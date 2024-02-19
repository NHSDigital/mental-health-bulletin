# Databricks notebook source
 %sql
 -- %sql
 
 -- /*
 -- Please note this chapter must be run on the live monthly tables, as it requires previous years' data. Please also run this chapter on the pseudo dataset, rather than the clear dataset, as this will produce different numbers
 -- */

# COMMAND ----------

# DBTITLE 1,CCG prep temp table
 %sql
 DROP TABLE IF EXISTS $db_output.CCG_PRAC;
 
 CREATE TABLE $db_output.CCG_PRAC USING DELTA AS 
  SELECT GP.Person_ID, GP.OrgIDCCGGPPractice, GP.RecordNumber 
  FROM       (  SELECT *
                FROM $db_output.MHB_MHS002GP
                WHERE uniqmonthid between '$end_month_id' -11 and '$end_month_id'
                ) AS GP
  INNER JOIN (  SELECT Person_ID, MAX(RecordNumber) AS RecordNumber
                FROM $db_output.MHB_MHS002GP
                WHERE GMPCodeReg NOT IN ('V81999','V81998','V819997') AND EndDateGMPRegistration is NULL 
                GROUP BY Person_ID
                ) AS max_GP
          ON GP.Person_ID = max_GP.Person_ID AND GP.RecordNumber = max_GP.RecordNumber
  WHERE GMPCodeReg NOT IN ('V81999','V81998','V819997')
    AND EndDateGMPRegistration is NULL 

# COMMAND ----------

# DBTITLE 1,CCG prep temp table 2
 %sql
 DROP TABLE IF EXISTS $db_output.CCG_PREP;
 
 CREATE TABLE $db_output.CCG_PREP USING DELTA AS
 SELECT  a.Person_ID
       , max(a.RecordNumber) as RecordNumber
     
 FROM       (  SELECT *
               FROM $db_output.MHB_MHS001MPI
               WHERE uniqmonthid between '$end_month_id' -11 and '$end_month_id'
                 AND PATMRecInRP = true
               ) AS a
 LEFT JOIN  (  SELECT *
               FROM $db_output.MHB_MHS002GP
               WHERE GMPCodeReg NOT IN ('V81999', 'V81998','V81997')
                 AND EndDateGMPRegistration is null
               ) AS b
           ON a.Person_ID = b.Person_ID
          AND a.uniqmonthid = b.uniqmonthid
          AND a.recordnumber = b.recordnumber
 LEFT JOIN $db_output.MHB_RD_CCG_LATEST AS c 
           ON a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN $db_output.MHB_RD_CCG_LATEST AS e 
           ON b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE (e.ORG_CODE is not null or c.ORG_CODE is not null)
 GROUP BY a.Person_ID

# COMMAND ----------

# DBTITLE 1,CCG prep temp table 3
 %sql
 DROP TABLE IF EXISTS $db_output.CCG_Prep2;
 
 CREATE TABLE $db_output.CCG_Prep2 USING DELTA AS 
 SELECT a.Person_ID, CASE WHEN b.OrgIDCCGGPPractice IS not null and e.ORG_CODE is not null then b.OrgIDCCGGPPractice
                          WHEN a.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null then  a.ORGIDCCGRes 
                          ELSE 'Unknown' end as IC_Rec_CCG
 FROM       (  SELECT *
               FROM $db_output.MHB_MHS001MPI
               WHERE uniqmonthid between '$end_month_id' -11 and '$end_month_id'
               ) AS a
 LEFT JOIN  (  SELECT *
               FROM $db_output.MHB_MHS002GP
               WHERE GMPCodeReg NOT IN ('V81999', 'V81998','V81997')
                 AND EndDateGMPRegistration is null
               ) AS b
           ON a.Person_ID = b.Person_ID
          AND a.uniqmonthid = b.uniqmonthid
          AND a.recordnumber = b.recordnumber
 INNER JOIN $db_output.CCG_PREP as ccg
           ON a.recordnumber = ccg.recordnumber
 LEFT JOIN $db_output.MHB_RD_CCG_LATEST AS c 
           ON a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN $db_output.MHB_RD_CCG_LATEST AS e 
           ON b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE (e.ORG_CODE is not null or c.ORG_CODE is not null) 

# COMMAND ----------

# DBTITLE 1,Final CCG table
 %sql
 DROP TABLE IF EXISTS $db_output.CCG;
 
 CREATE TABLE $db_output.CCG USING DELTA AS 
   SELECT Person_ID,
         CASE WHEN b.ORG_CODE is NULL THEN 'Unknown' ELSE b.ORG_CODE END AS IC_Rec_CCG,
         CASE WHEN NAME IS null THEN 'Unknown' ELSE NAME END AS NAME
   FROM $db_output.CCG_PREP2 AS a
         LEFT JOIN $db_output.MHB_RD_CCG_LATEST AS b
             ON a.IC_Rec_CCG = b.ORG_CODE

# COMMAND ----------

# DBTITLE 1,Pulls together contacts
 %sql
 DROP TABLE IF EXISTS $db_output.Cont;
 
 CREATE TABLE $db_output.Cont USING DELTA AS 
     SELECT c.UniqMonthID,
            c.Person_ID,
            c.UniqServReqID,
            c.AgeCareContDate,
            c.UniqCareContID AS ContID,
            c.CareContDate AS ContDate
       FROM $db_output.MHB_MHS201CareContact c
      WHERE (
 (c.AttendOrDNACode IN ('5','6') and
     ((c.ConsMechanismMH IN ('01','02','04','03') and c.UNiqMonthID <= 1458) ---v4.1 data
     or
      (c.ConsMechanismMH IN ('01','02','04','11') and c.UNiqMonthID > 1458))) ---v5 data ---Where patient attended
 --(c.AttendOrDNACode IN ('5','6') and c.ConsMediumUsed NOT IN ('05','06')) ---Where patient attended
 or 
 (   ((c.ConsMechanismMH IN ('05','06') and c.UNiqMonthID <= 1458) ---v4.1 data
     or
      (c.ConsMechanismMH IN ('05','09','10','12','13') and c.UNiqMonthID > 1458)) and OrgIdProv = 'DFC') ---Excluding Kooth
 )
 
 UNION ALL
 
     SELECT i.UniqMonthID,
            i.Person_ID,
            i.UniqServReqID,
            NULL AS AgeCareContDate,
            CAST(i.MHS204UniqID AS string) AS ContID,
            i.IndirectActDate AS ContDate
       FROM $db_source.MHS204IndirectActivity i

# COMMAND ----------

# DBTITLE 1,Joins contacts onto referrals
 %sql
 DROP TABLE IF EXISTS $db_output.RefCont;
 
 CREATE TABLE $db_output.RefCont USING DELTA AS
     SELECT c.UniqMonthID,
            r.Person_ID,
            r.UniqServReqID,
            CASE WHEN c.AgeCareContDate IS NULL THEN r.AgeServReferRecDate ELSE c.AgeCareContDate END AS AgeCareContDate,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ReferralRequestReceivedDate,
            c.ContID,
            c.ContDate,
            ROW_NUMBER () OVER(PARTITION BY r.Person_ID, r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS RN1, --orders person_id uniqservreqid by earliest to latest contact date
            ROW_NUMBER () OVER(PARTITION BY r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS DFC_RN1 --orders uniqservreqid by contact date for Kooth as person_ids are pseunomised between months
       FROM $db_output.Cont c
 INNER JOIN (  SELECT *
               FROM $db_output.MHB_MHS101Referral r
               WHERE AgeServReferRecDate BETWEEN 0 AND 18 ---inclusive as we want all people who have had a contact before 18th birthday
                 AND (RecordEndDate IS null OR RecordEndDate >= '$rp_enddate') ---using live table - gets latest record
                 AND RecordStartDate <= '$rp_enddate'
               ) AS r
            ON ((c.UniqServReqID = r.UniqServReqID AND c.Person_ID = r.Person_ID) 
                  OR (r.OrgIDProv = 'DFC' AND c.UniqServReqID = r.UniqServReqID))

# COMMAND ----------

# DBTITLE 1,Get all first contacts
 %sql
 DROP TABLE IF EXISTS $db_output.FirstCont;
 
 CREATE TABLE $db_output.FirstCont USING DELTA AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ReferralRequestReceivedDate,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM $db_output.RefCont r
      WHERE ((r.RN1 = 1 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 1 and r.OrgIDProv = 'DFC')) --first contact for oth normal providers and Kooth
            AND r.AgeCareContDate <18

# COMMAND ----------

# DBTITLE 1,Get all second contacts
 %sql
 DROP TABLE IF EXISTS $db_output.SubCont;
 
 CREATE TABLE $db_output.SubCont USING DELTA AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ReferralRequestReceivedDate,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM (  SELECT *
               FROM $db_output.RefCont r
               WHERE ((r.RN1 = 2 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 2 and r.OrgIDProv = 'DFC')) 
                 AND (r.ContDate BETWEEN '$rp_startdate' AND '$rp_enddate')
               ) AS r
 INNER JOIN $db_output.FirstCont f ---ensures people in this table are also present in First Contact table
            ON f.Person_ID = r.Person_ID 
           AND f.UniqServReqID = r.UniqServReqID

# COMMAND ----------

# DBTITLE 1,referrals and contacts in the financial year
 %sql
 DROP TABLE IF EXISTS $db_output.RefCont_inyear;
 
 CREATE TABLE $db_output.RefCont_inyear USING DELTA AS
     SELECT c.UniqMonthID,
            r.Person_ID,
            r.UniqServReqID,
            CASE WHEN c.AgeCareContDate IS NULL THEN r.AgeServReferRecDate ELSE c.AgeCareContDate END AS AgeCareContDate,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ReferralRequestReceivedDate,
            c.ContID,
            c.ContDate,
            ROW_NUMBER () OVER(PARTITION BY r.Person_ID, r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS RN1, ---orders person_id uniqservreqid by earliest to latest contact date
            ROW_NUMBER () OVER(PARTITION BY r.UniqServReqID ORDER BY c.ContDate ASC, c.ContID ASC) AS DFC_RN1 --orders uniqservreqid by contact date
       FROM (  SELECT * 
               FROM $db_output.Cont
               WHERE ContDate BETWEEN '$rp_startdate' AND '$rp_enddate' --contact within the financial year
               ) AS c
 INNER JOIN (  SELECT *
               FROM $db_output.MHB_MHS101Referral r
               WHERE AgeServReferRecDate BETWEEN 0 AND 18 ---inclusive as we want all people who have had a contact before 18th birthday
                 AND (RecordEndDate IS null OR RecordEndDate >= '$rp_enddate') ---using live table - gets latest record
                 AND RecordStartDate <= '$rp_enddate'
               ) AS r 
            ON ((c.UniqServReqID = r.UniqServReqID AND c.Person_ID = r.Person_ID) 
                  OR (r.OrgIDProv = 'DFC' AND c.UniqServReqID = r.UniqServReqID))

# COMMAND ----------

# DBTITLE 1,First contacts within the financial year
 %sql
 DROP TABLE IF EXISTS $db_output.FirstCont_inyear;
 
 CREATE TABLE $db_output.FirstCont_inyear USING DELTA AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ReferralRequestReceivedDate,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM $db_output.RefCont_inyear r
      WHERE ((r.RN1 = 1 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 1 and r.OrgIDProv = 'DFC')) 
            AND r.AgeCareContDate <18
            AND ContDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,Second contacts within the financial year
 %sql
 DROP TABLE IF EXISTS $db_output.SubCont_inyear;
 
 CREATE TABLE $db_output.SubCont_inyear USING DELTA AS
     SELECT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.OrgIDComm,
            r.ReferralRequestReceivedDate,
            r.ContID,
            r.ContDate,
            r.AgeCareContDate,
            r.RN1
       FROM (  SELECT *
               FROM $db_output.RefCont_inyear r
               WHERE ((r.RN1 = 2 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 2 and r.OrgIDProv = 'DFC')) 
                 AND (r.ContDate BETWEEN '$rp_startdate' AND '$rp_enddate')
               ) AS r
 INNER JOIN $db_output.FirstCont_inyear f 
            ON ((f.UniqServReqID = r.UniqServReqID AND f.Person_ID = r.Person_ID) 
            OR (r.OrgIDProv = 'DFC' AND f.UniqServReqID = r.UniqServReqID))

# COMMAND ----------

# DBTITLE 1,Join all first contacts together, ever and in year
 %sql
 DROP TABLE IF EXISTS $db_output.first_contacts;
 
 CREATE TABLE $db_output.first_contacts USING DELTA AS
     SELECT *
       FROM $db_output.FirstCont
      UNION
     SELECT * 
       FROM $db_output.FirstCont_inyear

# COMMAND ----------

# DBTITLE 1,Join all second contacts together, ever and in year
 %sql
 DROP TABLE IF EXISTS $db_output.contacts;
 
 CREATE TABLE $db_output.contacts USING DELTA AS
     SELECT *
       FROM $db_output.SubCont
      WHERE ContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
      UNION
     SELECT * 
       FROM $db_output.SubCont_inyear
      WHERE ContDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

# DBTITLE 1,Order all contacts by contact date and contact id
 %sql
 DROP TABLE IF EXISTS $db_output.FirstPersQtr;
 
 CREATE TABLE $db_output.FirstPersQtr USING DELTA AS
     SELECT *,
            ROW_NUMBER () OVER(PARTITION BY Person_ID ORDER BY ContDate ASC, ContID ASC) AS QtrRN
       FROM $db_output.contacts 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ContPer;
 
 CREATE TABLE $db_output.ContPer USING DELTA AS
     SELECT s.Person_ID,
            s.UniqServReqID,
            s.OrgIDProv,
            s.OrgIDComm,
            s.UniqMonthID,
            s.ReferralRequestReceivedDate,
            s.ContID,
            s.ContDate,
            s.AgeCareContDate,
            s.RN1,
            f.ContDate AS ContDate2,
            f.AgeCareContDate AS AgeCareContDate2,   
            s.UniqMonthID as Qtr
       FROM (  SELECT *
               FROM $db_output.FirstPersQtr
               WHERE QtrRN=1  ---get first contact date and ID for each person_id
               ) s
 INNER JOIN $db_output.first_contacts f 
            ON ((f.UniqServReqID = s.UniqServReqID AND f.Person_ID = s.Person_ID) 
            OR (s.OrgIDProv = 'DFC' AND f.UniqServReqID = s.UniqServReqID))

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.CYPFinal;
 
 CREATE TABLE $db_output.CYPFinal USING DELTA AS
      SELECT c.Person_ID,
             c.UniqServReqID,
             c.OrgIDProv,
             c.OrgIDComm,
             c.UniqMonthID,
             c.ReferralRequestReceivedDate,
             c.ContID,
             c.ContDate,
             c.AgeCareContDate,
             c.RN1,
             c.Qtr,
             c.ContDate2,
             c.AgeCareContDate2,
             DATEDIFF(ContDate, ReferralRequestReceivedDate) AS WAITING_TIME
        FROM $db_output.ContPer c
       WHERE Qtr BETWEEN ('$end_month_id'-11) and '$end_month_id'  --ensures we are getting all first contacts which are in the financial year

# COMMAND ----------

 %sql
 
 create or replace Table $db_output.temp_UM_MHB_MHS001MPI
 SELECT * FROM $db_output.MHB_MHS001MPI
 WHERE PatMRecInRP = TRUE
 AND uniqmonthid between '$end_month_id' -11 and '$end_month_id';
 
 create or replace Table $db_output.temp_REC_MHB_MHS001MPI
 select distinct max(recordnumber) as recordnumber, person_id from $db_output.MHB_MHS001MPI 
 where uniqmonthid between '$end_month_id' -11 and '$end_month_id'
 and PatMRecInRP = TRUE 
 and ((RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') AND RecordStartDate <= '$rp_enddate')
 group by person_id

# COMMAND ----------

# DBTITLE 1,Pulls together all demographics for output tables - first contacts
 %sql
 --People with 0,1 and 2 contacts -  Link using Person IDs.
 
 DROP TABLE IF EXISTS $db_output.Cont_prep_table_first;
 
 CREATE TABLE $db_output.Cont_prep_table_first USING DELTA AS
 SELECT DISTINCT
   fir.OrgIDProv,
   o.NAME as Provider_Name,
   mpi.age_group_higher_level,
   mpi.age_group_lower_common,
   mpi.age_group_lower_chap1,
   mpi.Der_Gender,
   mpi.Der_Gender_Desc,
   mpi.UpperEthnicity,
   mpi.LowerEthnicityCode,
   mpi.LowerEthnicityName,
   case when LEFT(mpi.LADistrictAuth,1) in ('N', 'W', 'S') then LEFT(mpi.LADistrictAuth,1)
   when mpi.LADistrictAuth in ('L99999999', 'M99999999', 'X99999998') then mpi.LADistrictAuth
   when la.level is null then 'Unknown'
   when mpi.LADistrictAuth = '' then 'Unknown'
   else la.level end as LADistrictAuth,
   la.level_description as LADistrictAuthName,                    
   mpi.IMD_Decile,
   mpi.IMD_Quintile,
   COALESCE(stp.CCG_CODE,'Unknown') AS CCG_CODE,
   COALESCE(stp.CCG_NAME, 'Unknown') as CCG_NAME,
   COALESCE(stp.STP_CODE, 'Unknown') as STP_CODE,
   COALESCE(stp.STP_NAME, 'Unknown') as STP_NAME, 
   COALESCE(stp.REGION_CODE, 'Unknown') as REGION_CODE,
   COALESCE(stp.REGION_NAME, 'Unknown') as REGION_NAME,
   fir.OrgIDComm,
   CASE WHEN fir.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown')
          ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END as IC_Rec_CCG,
   fir.person_id as 1_contact
 FROM $db_output.FirstCont_inyear fir
 left join $db_output.temp_UM_MHB_MHS001MPI a on fir.person_id = a.person_id
 
 inner join $db_output.temp_REC_MHB_MHS001MPI as x ---makes sure latest recordnumber is being used
 on a.person_id = x.person_id and a.recordnumber = x.recordnumber
 
 left join $db_output.mpi mpi on fir.Person_ID = mpi.Person_ID              
 left join $db_output.ccg ccg on ccg.person_id = a.person_id
 left join $db_output.la la on la.level = case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                                         when mpi.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when mpi.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when mpi.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when mpi.LADistrictAuth = '' then 'Unknown'
                                         when mpi.LADistrictAuth is not null then mpi.LADistrictAuth
                                         else 'Unknown' end 
 left join $db_output.MHB_RD_CCG_LATEST DFC_CCG ON OrgIDComm = DFC_CCG.ORG_CODE
 left join $db_output.STP_Region_mapping stp on CASE WHEN fir.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown') ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END = stp.CCG_code
 left join $db_output.MHB_ORG_DAILY o on fir.OrgIDProv = o.ORG_CODE

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

# DBTITLE 1,Pulls together all demogrpahics for output tables - second contacts
 %sql
 --People with 0,1 and 2 contacts -  Link using Person IDs.
 
 CREATE or replace TABLE $db_output.Cont_prep_table_second USING DELTA AS
 SELECT DISTINCT
 case when a.Der_Gender in ('1', '2', '3', '4', '9') then a.Der_Gender
      else 'Unknown' end as Der_Gender,
 
 case when a.Der_Gender = '1' then 'Male'
      when a.Der_Gender = '2' then 'Female'
      when a.Der_Gender = '3' then 'Non-binary'
      when a.Der_Gender = '4' then 'Other (not listed)'
      when a.Der_Gender = '9' then 'Indeterminate'
      else 'Unknown' end as Der_Gender_Desc,
            
 sec.OrgIDProv,
 o.NAME as Provider_Name,
 
 get_provider_type_code(sec.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(sec.OrgIDProv) as ProvTypeName,
  
  case when LEFT(a.LADistrictAuth, 1) in ('N', 'W', 'S') then LEFT(a.LADistrictAuth,1)
   when a.LADistrictAuth in ('L99999999', 'M99999999', 'X99999998') then a.LADistrictAuth
   when la.level is null then 'Unknown'
   when a.LADistrictAuth = '' then 'Unknown'
   else la.level end as LADistrictAuth,
                           
 la.level_description as LADistrictAuthName,
 
 case when a.AgeRepPeriodEnd between 0 and 125 then m.age_group_higher_level
 else 'Unknown' end as age_group_higher_level,
 
 case when a.AgeRepPeriodEnd between 0 and 125 then m.age_group_lower_common
 else 'Unknown' end as age_group_lower_common,
 
 case when a.AgeRepPeriodEnd between 0 and 125 then m.age_group_lower_chap1
 else 'Unknown' end as age_group_lower_chap1,
 
 case when a.NHSDEthnicity in (select distinct NHSDEthnicity from $db_output.mapethniccode) then e.UpperEthnicity
 ELSE 'Unknown' END AS UpperEthnicity,
 
 case when a.NHSDEthnicity in (select distinct NHSDEthnicity from $db_output.mapethniccode) then e.LowerEthnicityCode
 ELSE 'Unknown' END AS LowerEthnicityCode,
    
 case when a.NHSDEthnicity in (select distinct NHSDEthnicity from $db_output.mapethniccode) then e.LowerEthnicityName
 ELSE 'Unknown' END AS LowerEthnicityName,
 
 case when r.DECI_IMD in (1,2,3,4,5,6,7,8,9,10) then imd.IMD_DECILE
 ELSE 'Unknown'  END AS IMD_Decile,
 
 case when r.DECI_IMD in (1,2,3,4,5,6,7,8,9,10) then imd.IMD_Quintile
 ELSE 'Unknown'  END AS IMD_Quintile,
 
 COALESCE(stp.CCG_CODE,'Unknown') AS CCG_CODE,
 COALESCE(stp.CCG_NAME, 'Unknown') as CCG_NAME,
 COALESCE(stp.STP_CODE, 'Unknown') as STP_CODE,
 COALESCE(stp.STP_NAME, 'Unknown') as STP_NAME, 
 COALESCE(stp.REGION_CODE, 'Unknown') as REGION_CODE,
 COALESCE(stp.REGION_NAME, 'Unknown') as REGION_NAME,
 sec.OrgIDComm,
 
 CASE WHEN sec.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown')
          ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END as IC_Rec_CCG,
 
 sec.PERSON_ID as 2_contact
 
 FROM $db_output.CYPFinal sec
 left join $db_output.temp_UM_MHB_MHS001MPI a on sec.person_id = a.person_id
              
 inner join $db_output.temp_REC_MHB_MHS001MPI as x 
 on a.person_id = x.person_id and a.recordnumber = x.recordnumber    
               
 left join $db_output.ccg ccg on ccg.person_id = a.person_id
 left join $db_output.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         else 'Unknown' end 
 LEFT JOIN $db_output.MHB_RD_CCG_LATEST DFC_CCG ON sec.OrgIDComm = DFC_CCG.ORG_CODE
 left join $db_output.STP_Region_mapping stp on CASE  WHEN sec.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown') ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END = stp.CCG_code
 left join $db_output.MHB_ORG_DAILY o on sec.OrgIDProv = o.ORG_CODE
 left join  $reference_db.ENGLISH_INDICES_OF_DEP_V02 r --need to find this table
                     on a.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 left join $db_output.MapAge m on m.Age = a.AgeRepPeriodEnd
 left join $db_output.MAPIMD imd on r.DECI_IMD = imd.IMD
 left join $db_output.mapethniccode e on a.NHSDEthnicity = e.NHSDEthnicity

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Cont_prep_table_second