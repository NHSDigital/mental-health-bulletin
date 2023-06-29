# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.Cont;
 CREATE TABLE $db_output.Cont USING DELTA AS 
 ---get all contacts (direct and indirect activity) in the financial year
 SELECT c.UniqMonthID,
        c.Person_ID,
        c.UniqServReqID,
        c.AgeCareContDate,
        c.UniqCareContID AS ContID,
        c.CareContDate AS ContDate
 FROM $db_output.MHB_MHS201CareContact c
 WHERE (
 (c.AttendOrDNACode IN ('5','6') and
     ((c.ConsMechanismMH IN ('01','02','04','03') and c.UniqMonthID <= 1458) ---v4.1 data
     or
      (c.ConsMechanismMH IN ('01','02','04','11') and c.UniqMonthID > 1458))) ---v5 data ---Where patient attended
 or 
 (   ((c.ConsMechanismMH IN ('05','06') and c.UniqMonthID <= 1458) ---v4.1 data
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
 FROM $db_output.MHB_MHS204IndirectActivity i

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RefCont;
 CREATE TABLE $db_output.RefCont USING DELTA AS
 ---join contacts to children and young people referrals
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
                 AND (RecordEndDate IS null OR RecordEndDate >= '$rp_enddate') 
                 AND RecordStartDate <= '$rp_enddate'
               ) AS r
            ON ((c.UniqServReqID = r.UniqServReqID AND c.Person_ID = r.Person_ID) ---join on referral_id and person_id
            OR (r.OrgIDProv = 'DFC' AND c.UniqServReqID = r.UniqServReqID)) ---if provider is Kooth then join on referral_id only

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.FirstCont;
 CREATE TABLE $db_output.FirstCont USING DELTA AS
 ---get all first contacts for referrals
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
 WHERE ((r.RN1 = 1 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 1 and r.OrgIDProv = 'DFC')) ---first contact for both normal providers and Kooth
       AND r.AgeCareContDate <18 ---age at contact date under 18

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.SubCont;
 CREATE TABLE $db_output.SubCont USING DELTA AS
 ---get all second contacts for referrals
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
 FROM (SELECT *
       FROM $db_output.RefCont r
       WHERE ((r.RN1 = 2 and r.OrgIDProv <> 'DFC') OR (r.DFC_RN1 = 2 and r.OrgIDProv = 'DFC')) ---second contact for both normal providers and Kooth
       AND (r.ContDate BETWEEN '$rp_startdate' AND '$rp_enddate') ---second contact in financial year
       ) AS r
 INNER JOIN $db_output.FirstCont f ---ensures people in this table are also present in First Contact table
            ON f.Person_ID = r.Person_ID 
           AND f.UniqServReqID = r.UniqServReqID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.RefCont_inyear;
 CREATE TABLE $db_output.RefCont_inyear USING DELTA AS
 ---join contacts to children and young people referrals in the financial year
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

 %sql
 DROP TABLE IF EXISTS $db_output.FirstCont_inyear;
 CREATE TABLE $db_output.FirstCont_inyear USING DELTA AS
 ---get all first contacts for referrals in the financial year
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

 %sql
 DROP TABLE IF EXISTS $db_output.SubCont_inyear;
 CREATE TABLE $db_output.SubCont_inyear USING DELTA AS
 ---get all second contacts for referrals in the financial year
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

 %sql
 DROP TABLE IF EXISTS $db_output.first_contacts;
 CREATE TABLE $db_output.first_contacts USING DELTA AS
 ---join all first contacts together, ever and in year
 SELECT *
 FROM $db_output.FirstCont
 UNION
 SELECT * 
 FROM $db_output.FirstCont_inyear

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.contacts;
 CREATE TABLE $db_output.contacts USING DELTA AS
 ---join all second contacts together, ever and in year
 SELECT *
 FROM $db_output.SubCont
 WHERE ContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
 UNION
 SELECT * 
 FROM $db_output.SubCont_inyear
 WHERE ContDate BETWEEN '$rp_startdate' AND '$rp_enddate'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.FirstPersQtr;
 CREATE TABLE $db_output.FirstPersQtr USING DELTA AS
 ---add field to order all contacts by contact date and contact id
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
 FROM (SELECT *
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
 WHERE Qtr BETWEEN ('$month_id_end'-11) and '$month_id_end' ---ensures we are getting all first contacts which are in the financial year

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Cont_prep_table_first;
 CREATE TABLE $db_output.Cont_prep_table_first USING DELTA AS
 ---people with 0,1 and 2 contacts -  Link using Person IDs
 ---pulls together all demographics for output tables - first contacts
 SELECT 
 distinct case when a.Der_Gender = '1' then '1'
               when a.Der_Gender = '2' then '2'
               when a.Der_Gender = '3' then '3'
               when a.Der_Gender = '4' then '4'
               when a.Der_Gender = '9' then '9'
               else 'Unknown' end as Gender
 ,AgeRepPeriodEnd                                  
 ,case when AgeRepPeriodEnd between 0 and 5 then '0 to 5'
                           when AgeRepPeriodEnd between 6 and 10 then '6 to 10'
                           when AgeRepPeriodEnd between 11 and 15 then '11 to 15'
                           when AgeRepPeriodEnd = 16 then '16'
                           when AgeRepPeriodEnd = 17 then '17'
                           when AgeRepPeriodEnd = 18 then '18'
                           else 'Unknown' end  as age_group_lower_level
 ,fir.orgidprov   as orgidprov
 ,NHSDEthnicity
 ,CASE WHEN a.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN a.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN a.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN a.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN a.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
                    ELSE 'Unknown' END AS UpperEthnicity
              ,CASE WHEN a.NHSDEthnicity = 'A' THEN 'A'
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
                            ELSE 'Unknown' END AS LowerEthnicity
 ,CASE
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
                 END AS IMD_Decile
 ,CASE 
                 WHEN r.DECI_IMD IN (9, 10) THEN '05 Least deprived'
                 WHEN r.DECI_IMD IN (7, 8) THEN '04'
                 WHEN r.DECI_IMD IN (5, 6) THEN '03'
                 WHEN r.DECI_IMD IN (3, 4) THEN '02'
                 WHEN r.DECI_IMD IN (1, 2) THEN '01 Most deprived'                          
                 ELSE 'Unknown' 
                 END AS IMD_Quintile
 ,OrgIDComm
 ,CASE  WHEN fir.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown')
          ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END as IC_Rec_CCG
 ,case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                           when a.LADistrictAuth = 'L99999999' then 'L99999999'
                           when a.LADistrictAuth = 'M99999999' then 'M99999999'
                           when a.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when a.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth
 ,stp.STP_code
 ,stp.Region_code
 ,fir.person_id as 1_contact
 FROM $db_output.FirstCont_inyear fir
 left join (  SELECT *
              FROM $db_output.MHB_MHS001MPI
              WHERE PatMRecInRP = TRUE
                AND uniqmonthid between '$month_id_end' -11 and '$month_id_end'
              ) a on fir.person_id = a.person_id
 inner join (select distinct max(recordnumber) as recordnumber, person_id from $db_output.MHB_MHS001MPI 
                 where uniqmonthid between '$month_id_end' -11 and '$month_id_end'
                 and PatMRecInRP = TRUE 
                 and ((RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') AND RecordStartDate <= '$rp_enddate')
                 group by person_id) as x ---makes sure latest recordnumber is being used
               on a.person_id = x.person_id and a.recordnumber = x.recordnumber
 left join $db_output.ccg ccg on ccg.person_id = a.person_id
 left join $db_output.la la on la.level = a.LADistrictAuth
 left join $db_output.MHB_RD_CCG_LATEST DFC_CCG ON OrgIDComm = DFC_CCG.ORG_CODE
 left join $db_output.STP_Region_mapping stp on CASE  WHEN fir.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown') ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END = stp.CCG_code
 left join (  SELECT *
              FROM $ref_database.ENGLISH_INDICES_OF_DEP_V02
              WHERE imd_year = '$IMD_year'
              ) AS r  on a.LSOA2011 = r.LSOA_CODE_2011

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Cont_prep_table_second;
 CREATE TABLE $db_output.Cont_prep_table_second USING DELTA AS
 ---people with 0,1 and 2 contacts -  Link using Person IDs
 ---pulls together all demographics for output tables - second contacts
 SELECT 
 distinct case when a.Der_Gender = '1' then '1'
               when a.Der_Gender = '2' then '2'
               when a.Der_Gender = '3' then '3'
               when a.Der_Gender = '4' then '4'
               when a.Der_Gender = '9' then '9'
               else 'Unknown' end as Gender
 ,AgeRepPeriodEnd 
 ,case when AgeRepPeriodEnd between 0 and 5 then '0 to 5'
                           when AgeRepPeriodEnd between 6 and 10 then '6 to 10'
                           when AgeRepPeriodEnd between 11 and 15 then '11 to 15'
                           when AgeRepPeriodEnd = 16 then '16'
                           when AgeRepPeriodEnd = 17 then '17'
                           when AgeRepPeriodEnd = 18 then '18'
                           else 'Unknown' end  as age_group_lower_level
 ,sec.orgidprov   as orgidprov
 ,NHSDEthnicity
 ,CASE WHEN a.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN a.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN a.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN a.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN a.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
                    ELSE 'Unknown' END AS UpperEthnicity
              ,CASE WHEN a.NHSDEthnicity = 'A' THEN 'A'
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
                            ELSE 'Unknown' END AS LowerEthnicity
 ,CASE
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
                 END AS IMD_Decile
 ,CASE 
                 WHEN r.DECI_IMD IN (9, 10) THEN '05 Least deprived'
                 WHEN r.DECI_IMD IN (7, 8) THEN '04'
                 WHEN r.DECI_IMD IN (5, 6) THEN '03'
                 WHEN r.DECI_IMD IN (3, 4) THEN '02'
                 WHEN r.DECI_IMD IN (1, 2) THEN '01 Most deprived'                          
                 ELSE 'Unknown' 
                 END AS IMD_Quintile
 ,OrgIDComm
 ,CASE  WHEN sec.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown')
          ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END as IC_Rec_CCG
 ,case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                           when a.LADistrictAuth = 'L99999999' then 'L99999999'
                           when a.LADistrictAuth = 'M99999999' then 'M99999999'
                           when a.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when a.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth
 ,stp.STP_code
 ,stp.Region_code
 ,sec.PERSON_ID as 2_contact
 FROM $db_output.CYPFinal sec
 left join (  SELECT *
              FROM $db_output.MHB_MHS001MPI
              WHERE PatMRecInRP = TRUE
                AND uniqmonthid between '$month_id_end' -11 and '$month_id_end'
              ) a on sec.person_id = a.person_id
 inner join (select distinct max(recordnumber) as recordnumber, person_id from $db_output.MHB_MHS001MPI 
                 where uniqmonthid between '$month_id_end' -11 and '$month_id_end' 
                 and PatMRecInRP = true 
                 and ((RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') AND RecordStartDate <= '$rp_enddate')
                 group by person_id) as x 
               on a.person_id = x.person_id and a.recordnumber = x.recordnumber 
 left join $db_output.ccg ccg on ccg.person_id = a.person_id
 left join $db_output.la la on la.level = a.LADistrictAuth
 LEFT JOIN $db_output.MHB_RD_CCG_LATEST DFC_CCG ON OrgIDComm = DFC_CCG.ORG_CODE
 left join $db_output.STP_Region_mapping stp on CASE  WHEN sec.OrgIDProv = 'DFC' THEN COALESCE(DFC_CCG.ORG_CODE, 'Unknown') ELSE COALESCE(ccg.IC_Rec_CCG, 'Unknown') END = stp.CCG_code
 left join (  SELECT *
              FROM $ref_database.ENGLISH_INDICES_OF_DEP_V02
              WHERE imd_year = '$IMD_year'
              ) AS r  on a.LSOA2011 = r.LSOA_CODE_2011