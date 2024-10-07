# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("db_output", "_tootle1_100261")
dbutils.widgets.text("db_source", "mh_v5_pre_clear")
dbutils.widgets.text("rp_startdate", "2022-04-01")
dbutils.widgets.text("rp_enddate", "2023-03-31")
dbutils.widgets.text("start_month_id", "1476")
dbutils.widgets.text("end_month_id", "1465")

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
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW MHB_RD_ORG_DAILY_LATEST AS
 SELECT DISTINCT ORG_CODE, 
                 NAME
            FROM $db_output.MHB_org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');

# COMMAND ----------

# DBTITLE 1,providers_between_dates (EIP)
 %sql
 ---gets list of providers who have submitted data between the start and end dates
 DROP TABLE IF EXISTS $db_output.providers_between_rp_start_end_dates;

 CREATE TABLE $db_output.providers_between_rp_start_end_dates USING DELTA AS 
 SELECT DISTINCT OrgIDProvider, x.NAME as NAME
            FROM $db_source.MHS000Header as Z
                 LEFT OUTER JOIN global_temp.MHB_RD_ORG_DAILY_LATEST AS X
 					ON Z.OrgIDProvider = X.ORG_CODE
           WHERE	ReportingPeriodStartDate >= '$rp_startdate'
             AND ReportingPeriodEndDate <= '$rp_enddate';

# COMMAND ----------

 %sql

 -- This code replicates PatMRecInRP for Feb and Mar 2018 to account for these derivation not being available on legacy data. It has been coded so that this only makes a difference if the relevant reporting period is called, otherwise the subsequent
 -- bricks use the DDC coded derivation. Don't worry about it, it's fine. - DC

 DROP TABLE IF EXISTS $db_output.MHS001_PATMRECINRP_201819_F_M;

 CREATE TABLE $db_output.MHS001_PATMRECINRP_201819_F_M USING DELTA AS

 SELECT MPI.Person_ID,
        MPI.UniqSubmissionID,
        MPI.UniqMonthID,
        CASE WHEN x.Person_ID IS NULL THEN False ELSE True END AS  PatMRecInRP_temp
        

 FROM   $db_source.mhs001MPI MPI
 LEFT JOIN
 (
 SELECT Person_ID,
        UniqMonthID,
        MAX (UniqSubmissionID) AS UniqSubmissionID
        
 FROM   $db_source.mhs001MPI

 WHERE  UniqMonthID IN ('1427', '1428')

 GROUP BY Person_ID, UniqMonthID
 ) AS x
 ON MPI.Person_ID = x.Person_ID AND MPI.UniqSubmissionID = x.UniqSubmissionID AND MPI.UniqMonthID = x.UniqMonthID

 WHERE MPI.UniqMonthID IN ('1427', '1428');

# COMMAND ----------

 %sql

 DROP TABLE IF EXISTS $db_output.MHS001MPI_PATMRECINRP_FIX;

 CREATE TABLE $db_output.MHS001MPI_PATMRECINRP_FIX USING DELTA AS

 SELECT MPI.*,
        CASE WHEN FIX.Person_ID IS NULL THEN MPI.PatMRecInRP ELSE FIX.PatMRecInRP_temp END AS PatMRecInRP_FIX

 FROM $db_source.mhs001MPI MPI

 LEFT JOIN $db_output.MHS001_PATMRECINRP_201819_F_M FIX
 ON MPI.Person_ID = FIX.Person_ID AND MPI.UniqSubmissionID = FIX.UniqSubmissionID and MPI.UniqMonthID = FIX.UniqMonthID;

# COMMAND ----------

# DBTITLE 1,EIP CCG Methodology Prep Update
 %sql

 DROP TABLE IF EXISTS $db_output.CCG_prep_EIP;

 CREATE TABLE $db_output.CCG_prep_EIP USING DELTA AS
 SELECT DISTINCT    a.Person_ID,
 				   max(a.RecordNumber) as recordnumber --latest record number for a patient
                    
 FROM               $db_source.MHS001MPI a

 LEFT JOIN          $db_source.MHS002GP b ---CCG info derived from latest record number for a patient
 		           on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
 		           and a.recordnumber = b.recordnumber
 		           and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
 		           and b.EndDateGMPRegistration is null
                    
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on CASE WHEN a.UniqMonthID < 1467 THEN a.OrgIDCCGRes ELSE a.OrgIDSubICBLocResidence END = c.ORG_CODE

 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on CASE WHEN a.UniqMonthID < 1467 THEN b.OrgIDCCGGPPractice ELSE b.OrgIDSubICBLocGP END = e.ORG_CODE

 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    and a.uniqmonthid between '$end_month_id' - 11 AND '$end_month_id'
                    
 GROUP BY           a.Person_ID

# COMMAND ----------

# DBTITLE 1,EIP CCG Methodology Update
 %sql

 DROP TABLE IF EXISTS $db_output.MHS001_CCG_LATEST;

 CREATE TABLE $db_output.MHS001_CCG_LATEST USING DELTA as 

 select distinct    a.Person_ID,
 				   CASE
                        WHEN a.UNIQMONTHID <= 1467 and OrgIDCCGGPPractice is not null then OrgIDCCGGPPractice
                        WHEN a.UNIQMONTHID > 1467 and OrgIDSubICBLocGP is not null then OrgIDSubICBLocGP 
                        WHEN a.UNIQMONTHID <= 1467 then OrgIDCCGRes 
                        WHEN a.UNIQMONTHID > 1467 then OrgIDSubICBLocResidence
                        ELSE "UNKNOWN"
                        END AS IC_Rec_CCG		
 FROM               $db_source.mhs001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null
 INNER JOIN         $db_output.CCG_prep_EIP ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on CASE WHEN a.UniqMonthID < 1467 THEN a.OrgIDCCGRes ELSE a.OrgIDSubICBLocResidence END = c.ORG_CODE
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on CASE WHEN a.UniqMonthID < 1467 THEN a.OrgIDCCGRes ELSE a.OrgIDSubICBLocResidence END = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    and a.uniqmonthid between '$end_month_id' - 11 AND '$end_month_id'

# COMMAND ----------

# DBTITLE 1,MHS006CareCoord_LATEST (EIP)
 %sql
 ---Gets care co-ordinator information needed for 10e metric
 DROP TABLE IF EXISTS $db_output.MHS006MHCareCoord_LATEST;

 CREATE TABLE $db_output.MHS006MHCareCoord_LATEST USING DELTA AS 
      SELECT DISTINCT c.CareProfServOrTeamTypeAssoc, 
             c.UniqMonthID,
             c.OrgIDProv,
             c.StartDateAssCareCoord,
             c.Person_ID,
             c.EndDateAssCareCoord
        FROM $db_source.MHS006MHCareCoord as c
       WHERE ((c.RecordEndDate IS NULL OR c.RecordEndDate >= '$rp_enddate') AND c.RecordStartDate <= '$rp_enddate') ---ensures record is still active

# COMMAND ----------

# DBTITLE 1,MHS101Referral_LATEST (EIP)
 %sql
 ---gets required mhs101 information for making cohort of people EIP specific
 DROP TABLE IF EXISTS $db_output.MHS101Referral_LATEST;

 CREATE TABLE $db_output.MHS101Referral_LATEST USING DELTA AS
     SELECT DISTINCT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.ReferralRequestReceivedDate,
            r.PrimReasonReferralMH,
            r.ServDischDate,
            r.AgeServReferRecDate
       FROM $db_source.MHS101Referral AS r
      WHERE ((RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') AND RecordStartDate <= '$rp_enddate')

# COMMAND ----------

# DBTITLE 1,EIP_MHS101Referral_LATEST
 %sql
 ---Open referrals in RP with request after 2016 with Primary Referral Reason as suspected first episode of psychosis
 DROP TABLE IF EXISTS $db_output.EIP_MHS101Referral_LATEST;

 CREATE TABLE $db_output.EIP_MHS101Referral_LATEST USING DELTA AS
 SELECT DISTINCT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.ReferralRequestReceivedDate,
            r.PrimReasonReferralMH,
            r.ServDischDate,
            r.AgeServReferRecDate,
            CCG.IC_Rec_CCG
       FROM (  select *
               FROM $db_source.MHS101Referral AS r
               WHERE ((r.RecordEndDate IS NULL OR r.RecordEndDate >= '$rp_enddate') AND r.RecordStartDate <= '$rp_enddate')
                 AND r.ReferralRequestReceivedDate >= '2016-01-01'
                 AND r.PrimReasonReferralMH = '01' ---Reason for referral is (Suspected) First Episode Psychosis
                 ) AS R
       LEFT JOIN (  SELECT *
                    FROM $db_output.MHS001MPI_PATMRECINRP_FIX
                    WHERE PatMRecInRP_FIX = True) AS E ---get latest submission
           ON r.Person_ID = E.Person_ID 
           AND E.UniqMonthID = r.UniqMonthID 
       LEFT JOIN $db_output.MHS001_CCG_LATEST as CCG
           ON CCG.Person_ID = E.Person_ID;

 select AgeServReferRecDate, count(*) from $db_output.EIP_MHS101Referral_LATEST
 group by AgeServReferRecDate

# COMMAND ----------

# DBTITLE 1,MHS102ServicetypeReferredTo_LATEST (EIP)
 %sql
 ---gets required mhs102 information for making cohort of people EIP specific
 DROP TABLE IF EXISTS $db_output.MHS102ServiceTypeReferredTo_LATEST;

 CREATE TABLE $db_output.MHS102ServiceTypeReferredTo_LATEST USING DELTA AS
      SELECT S.UniqMonthID
             ,S.OrgIDProv
             ,S.ReferClosureDate
             ,S.ReferRejectionDate
             ,S.ServTeamTypeRefToMH
             ,S.UniqCareProfTeamID
             ,S.Person_ID
             ,S.UniqServReqID
        FROM $db_source.MHS102ServiceTypeReferredTo s
       WHERE (( s.RecordEndDate IS NULL OR s.RecordEndDate >= '$rp_enddate') AND s.RecordStartDate <= '$rp_enddate' );

# COMMAND ----------

# DBTITLE 1,EIP_MHS102ServicetypeReferredTo_LATEST (EIP)
 %sql
 ---Open referrals which weren't rejected or closed in RP where service type referred to is Early Intervention is Psychosis
 DROP TABLE IF EXISTS $db_output.EIP_MHS102ServiceTypeReferredTo_LATEST;

 CREATE TABLE $db_output.EIP_MHS102ServiceTypeReferredTo_LATEST USING DELTA AS
     SELECT s.UniqMonthID,
            s.OrgIDProv,
            s.ReferClosureDate,
            s.ReferRejectionDate,
            s.ServTeamTypeRefToMH,
            s.UniqCareProfTeamID,
            s.Person_ID,
            s.UniqServReqID
       FROM $db_output.MHS102ServiceTypeReferredTo_LATEST s
      WHERE s.ServTeamTypeRefToMH = 'A14' ---Early Intervention in Psychosis Service Type
            AND 
            (
              (
                (
                  (s.ReferClosureDate IS NULL OR s.ReferClosureDate > '$rp_enddate') AND 
                  (s.ReferRejectionDate IS NULL OR s.ReferRejectionDate > '$rp_enddate')
                ) 
              ) OR s.ReferClosureDate <= '$rp_enddate' OR s.ReferRejectionDate <= '$rp_enddate'
            );

# COMMAND ----------

# DBTITLE 1,earliest_care_contact_dates_by_service_request_id
 %sql

 DROP TABLE IF EXISTS $db_output.earliest_care_contact_dates_by_service_request_id;

 CREATE TABLE $db_output.earliest_care_contact_dates_by_service_request_id USING DELTA AS 
      SELECT a.UniqServReqID,
             MIN(CareContDate) AS CareContDate
        FROM $db_source.MHS201CareContact AS a
   LEFT JOIN $db_output.EIP_MHS101Referral_LATEST b
             ON a.UniqServReqID = b.UniqServReqID
   LEFT JOIN $db_source.MHS102ServiceTypeReferredTo AS x
             ON a.UniqCareProfTeamID = x.UniqCareProfTeamID
             AND a.UniqServReqID = x.UniqServReqID
       WHERE a.AttendOrDNACode IN ('5', '6')
             AND ((a.ConsMechanismMH IN ('01','02','04','03') and a.UNiqMonthID <= 1458) ---v4.1 data
                   or
                    (a.ConsMechanismMH IN ('01','02','04','11') and a.UNiqMonthID > 1458)) ---v5 data
             AND x.ServTeamTypeRefToMH = 'A14'
             AND a.CareContDate >= b.ReferralRequestReceivedDate
             AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$end_month_id') OR b.ServDischDate <= '$rp_enddate')
             AND a.UniqMonthId <= '$end_month_id'
             AND x.UniqMonthID <= '$end_month_id'
    GROUP BY a.UniqServReqID;

# COMMAND ----------

# DBTITLE 1,earliest_care_assessment_dates_by_service_request_id
 %sql

 DROP TABLE IF EXISTS $db_output.earliest_care_assessment_dates_by_service_request_id;

 CREATE TABLE $db_output.earliest_care_assessment_dates_by_service_request_id USING DELTA AS 
     SELECT B.UniqServReqID,
            MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord
       FROM $db_output.MHS006MHCareCoord_LATEST AS a
  LEFT JOIN $db_output.EIP_MHS101Referral_LATEST AS B
            ON A.Person_ID = B.Person_ID
      WHERE A.CareProfServOrTeamTypeAssoc = 'A14'
 	       AND A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
 	       AND ((B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') OR A.StartDateAssCareCoord <= B.ServDischDate)
            AND (((A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') AND A.UniqMonthID = '$end_month_id') OR A.EndDateAssCareCoord <= '$rp_enddate')
            AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$end_month_id') OR b.ServDischDate <= '$rp_enddate')
   GROUP BY B.UniqServReqID

# COMMAND ----------

# DBTITLE 1,earliest_care_assessment_dates_by_service_request_id_Prov
 %sql

 DROP TABLE IF EXISTS $db_output.earliest_care_assessment_dates_by_service_request_id_Prov;

 CREATE TABLE $db_output.earliest_care_assessment_dates_by_service_request_id_Prov USING DELTA AS 
     SELECT B.UniqServReqID,
            B.OrgIDProv,
            MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord
       FROM $db_output.MHS006MHCareCoord_LATEST AS a
  LEFT JOIN $db_output.EIP_MHS101Referral_LATEST AS B
            ON A.Person_ID = B.Person_ID
      WHERE A.CareProfServOrTeamTypeAssoc = 'A14'
            AND A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
            AND ((B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') OR A.StartDateAssCareCoord <= B.ServDischDate)
            AND (((A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') AND A.UniqMonthID = '$end_month_id') OR A.EndDateAssCareCoord <= '$rp_enddate')
            AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$end_month_id') OR b.ServDischDate <= '$rp_enddate')
   GROUP BY B.UniqServReqID,
            B.OrgIDProv

# COMMAND ----------

# DBTITLE 1,earliest_care_assessment_dates_by_service_request_id_any_team
 %sql

 DROP TABLE IF EXISTS $db_output.earliest_care_assessment_dates_by_service_request_id_any_team;

 CREATE TABLE $db_output.earliest_care_assessment_dates_by_service_request_id_any_team USING DELTA AS 
     SELECT B.UniqServReqID,
            MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord
       FROM $db_output.MHS006MHCareCoord_LATEST AS a
  LEFT JOIN $db_output.EIP_MHS101Referral_LATEST AS B
            ON A.Person_ID = B.Person_ID
      WHERE 
            A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
 	       AND (
                  (B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') 
                OR A.StartDateAssCareCoord <= B.ServDischDate
                )
            AND (
                  (
                     (A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') 
                     AND A.UniqMonthID = '$end_month_id'
                  ) 
                OR A.EndDateAssCareCoord <= '$rp_enddate'
                )
            AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$end_month_id') OR b.ServDischDate <= '$rp_enddate')
   GROUP BY B.UniqServReqID

# COMMAND ----------

# DBTITLE 1,Distinct_Referral_UniqServReqIDs
 %sql

 DROP TABLE IF EXISTS $db_output.Distinct_Referral_UniqServReqIDs;

 CREATE TABLE $db_output.Distinct_Referral_UniqServReqIDs USING DELTA AS
 select distinct a.UniqServReqID
            from $db_output.EIP_MHS101Referral_LATEST a 
       left join $db_output.MHS102ServiceTypeReferredTo_LATEST b 
                 ON A.UniqServReqID = B.UniqServReqID 
                 and a.UniqMonthID = b.UniqMonthID
           where B.ServTeamTypeRefToMH = 'A14'

# COMMAND ----------

# DBTITLE 1,Distinct_Referral_UniqServReqIDs_any_month
 %sql

 DROP TABLE IF EXISTS $db_output.Distinct_Referral_UniqServReqIDs_any_month;

 CREATE TABLE $db_output.Distinct_Referral_UniqServReqIDs_any_month USING DELTA AS
 select distinct a.UniqServReqID
            from $db_output.EIP_MHS101Referral_LATEST a 
       left join (  SELECT *
                    FROM $db_output.MHS102ServiceTypeReferredTo_LATEST
                    WHERE ServTeamTypeRefToMH = 'A14') AS b 
                 ON A.UniqServReqID = B.UniqServReqID 
           where B.ServTeamTypeRefToMH = 'A14'

# COMMAND ----------

# DBTITLE 1,EIP23a - Common
 %sql
 /* BREAKDOWNS NEEDED FOR EIP / CHAPTER 10 PREP TABLES: England, Provider, CCG - Registration or Residence, STP, Commissioning Region, LAD/UA, Ethnicity (Higher Level), Ethnicity (Lower Level), Age Group (Lower Level), Gender, IMD (and some combinations of those) */

 DROP TABLE IF EXISTS $db_output.EIP23a_common;

 CREATE TABLE $db_output.EIP23a_common USING DELTA AS

 SELECT A.UniqServReqID, A.OrgIDProv, o.NAME as Provider_Name,
   case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
     when AgeRepPeriodEnd >= 18 then '18 and over'
     else "UNKNOWN" end as age_group_higher_level,
    AgeRepPeriodEnd,
    coalesce(mpi.age_group_lower_chap10, "UNKNOWN") as age_group_lower_chap10,
    coalesce(mpi.age_group_lower_chap1, "UNKNOWN") as age_group_lower_chap1,
    coalesce(mpi.age_group_lower_chap10a, "UNKNOWN") as age_group_lower_chap10a,
    mpi.Der_Gender,
    mpi.Der_Gender_Desc,
    mpi.UpperEthnicity,
    mpi.LowerEthnicityCode,
    mpi.LowerEthnicityName,
    mpi.LADistrictAuth as MPI_LADistrictAuth,
   mpi.LADistrictAuthCode as LADistrictAuth,
   mpi.LADistrictAuthName,                    
   mpi.IMD_Decile,
   mpi.IMD_Quintile,
   COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
   COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
   COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
   COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
   COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
   COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME,
   DATEDIFF (
    CASE WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
           ELSE D.StartDateAssCareCoord END,
                A.ReferralRequestReceivedDate
               ) days_between_ReferralRequestReceivedDate
   FROM (  SELECT *
           FROM $db_output.EIP_MHS101Referral_LATEST AS a
           WHERE (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = '$end_month_id') OR A.ServDischDate <= '$rp_enddate') 
           ) AS A
   INNER JOIN (  SELECT *
                 FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b --Ask AH
                 WHERE B.ServTeamTypeRefToMH = 'A14'
                 ) AS B
              ON a.UniqServReqID = b.UniqServReqID 
   INNER JOIN $db_output.earliest_care_contact_dates_by_service_request_id AS c
              ON a.UniqServReqID = c.UniqServReqID
   INNER JOIN $db_output.earliest_care_assessment_dates_by_service_request_id AS d
              ON a.UniqServReqID = d.UniqServReqID
    LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG
              ON CCG.Person_ID = A.Person_ID
    left join $db_output.mpi mpi on a.person_id = mpi.person_id
    LEFT JOIN $db_output.STP_Region_mapping stp 
                  on ccg.IC_Rec_CCG = stp.CCG_code
    left join $db_output.MHB_ORG_DAILY o on A.OrgIDProv = o.ORG_CODE
   WHERE CASE WHEN C.CareContDate > D.StartDateAssCareCoord 
                       THEN C.CareContDate
                       ELSE D.StartDateAssCareCoord
                       END BETWEEN '$rp_startdate' AND '$rp_enddate'
               AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate')
                        AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate'))
                      AND B.UniqMonthID = A.UniqMonthID)
                    OR B.ReferClosureDate <= '$rp_enddate'
                    OR B.ReferRejectionDate <= '$rp_enddate')
              --AND A.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust--Removed for 2022-23

# COMMAND ----------

# DBTITLE 1,EIP23a - Common Prov
 %sql

 DROP TABLE IF EXISTS $db_output.EIP23a_common_Prov;

 CREATE TABLE $db_output.EIP23a_common_Prov USING DELTA AS
      SELECT coalesce(mpi.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
             CASE WHEN AgeServReferRecDate BETWEEN 14 AND 15 THEN '14 to 15'
                        WHEN AgeServReferRecDate BETWEEN 16 AND 17 THEN '16 to 17'
                        WHEN AgeServReferRecDate BETWEEN 18 AND 19 THEN '18 to 19'
                        WHEN AgeServReferRecDate BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeServReferRecDate BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeServReferRecDate BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeServReferRecDate BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeServReferRecDate BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeServReferRecDate BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeServReferRecDate BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeServReferRecDate BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeServReferRecDate BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeServReferRecDate > 64 THEN '65 or over' ELSE "UNKNOWN" END as age_group_lower_chap10,
                        
                    case when AgeServReferRecDate between 0 and 5 then '0 to 5'
                      when AgeServReferRecDate between 6 and 10 then '6 to 10'
                      when AgeServReferRecDate between 11 and 15 then '11 to 15'
                      when AgeServReferRecDate = 16 then '16'
                      when AgeServReferRecDate = 17 then '17'
                      when AgeServReferRecDate = 18 then '18'
                      when AgeServReferRecDate = 19 then '19'
                      when AgeServReferRecDate between 20 and 24 then '20 to 24'
                      when AgeServReferRecDate between 25 and 29 then '25 to 29'
                      when AgeServReferRecDate between 30 and 34 then '30 to 34'
                      when AgeServReferRecDate between 35 and 39 then '35 to 39'
                      when AgeServReferRecDate between 40 and 44 then '40 to 44'
                      when AgeServReferRecDate between 45 and 49 then '45 to 49'
                      when AgeServReferRecDate between 50 and 54 then '50 to 54'
                      when AgeServReferRecDate between 55 and 59 then '55 to 59'
                      when AgeServReferRecDate between 60 and 64 then '60 to 64'
                      when AgeServReferRecDate between 65 and 69 then '65 to 69'
                      when AgeServReferRecDate between 70 and 74 then '70 to 74'
                      when AgeServReferRecDate between 75 and 79 then '75 to 79'
                      when AgeServReferRecDate between 80 and 84 then '80 to 84'
                      when AgeServReferRecDate between 85 and 89 then '85 to 89'
                      when AgeServReferRecDate >= '90' then '90 or over' else "UNKNOWN" end as age_group_lower_chap1,
                      
                     case when AgeServReferRecDate between 0 and 13 then 'Under 14'
                       when AgeServReferRecDate between 14 and 17 then "14 to 17"
                       when AgeServReferRecDate between 18 and 19 then "18 to 19"
                       when AgeServReferRecDate between 20 and 24 then '20 to 24'
                       when AgeServReferRecDate between 25 and 29 then '25 to 29'
                       when AgeServReferRecDate between 30 and 34 then '30 to 34'
                       when AgeServReferRecDate between 35 and 39 then '35 to 39'
                       when AgeServReferRecDate between 40 and 44 then '40 to 44'
                       when AgeServReferRecDate between 45 and 49 then '45 to 49'
                       when AgeServReferRecDate between 50 and 54 then '50 to 54'
                       when AgeServReferRecDate between 55 and 59 then '55 to 59'
                       when AgeServReferRecDate between 60 and 64 then '60 to 64'     
                       when AgeServReferRecDate between 65 and 69 then "65 to 69"
                       when AgeServReferRecDate between 70 and 74 then "70 to 74"
                       when AgeServReferRecDate between 75 and 79 then "75 to 79"
                       when AgeServReferRecDate between 80 and 84 then "80 to 84"
                       when AgeServReferRecDate between 85 and 89 then "85 to 89"
                       when AgeServReferRecDate >= '90' then '90 or over' else "UNKNOWN" end as age_group_lower_chap10a,
                     
             a.UniqServReqID as UniqServReqID,
 			A.OrgIDProv as OrgIDPRov,-- Bring out the provider which will then be grouped on and joined in the aggregate step.
             coalesce(mpi.Der_Gender, "UNKNOWN") as Gender,
             coalesce(mpi.IMD_Quintile, "UNKNOWN") as IMD_Quintile,
             coalesce(mpi.UpperEthnicity, "UNKNOWN") AS UpperEthnicity,
             DATEDIFF (
                      CASE WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
                      ELSE D.StartDateAssCareCoord
                      END,
                      A.ReferralRequestReceivedDate
                      ) days_between_ReferralRequestReceivedDate --Clock Stop
        FROM (  SELECT *
                FROM $db_output.EIP_MHS101Referral_LATEST AS a
                WHERE (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = '$end_month_id') OR A.ServDischDate <= '$rp_enddate')
                ) AS A
   left join $db_output.mpi mpi on a.person_id = mpi.person_id
   INNER JOIN $db_output.EIP_MHS102ServiceTypeReferredTo_LATEST AS b 
             ON a.UniqServReqID = b.UniqServReqID 
             AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')) 
 				AND B.UniqMonthID = A.UniqMonthID) 
 			OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate')
   INNER JOIN (  SELECT *
                 FROM $db_output.earliest_care_contact_dates_by_service_request_id AS c
                 WHERE C.CareContDate IS NOT NULL
                 ) AS C
             ON a.UniqServReqID = c.UniqServReqID
   INNER JOIN (  SELECT *
                 FROM $db_output.earliest_care_assessment_dates_by_service_request_id_Prov AS d -- Uses the Prov version
                 WHERE D.StartDateAssCareCoord IS NOT NULL
                 ) AS D
             ON a.UniqServReqID = d.UniqServReqID 
             AND A.OrgIDProv = D.OrgIDProv -- also joins on Prov to ensure patient began and ended in same provider
       WHERE CASE WHEN C.CareContDate > D.StartDateAssCareCoord 
                 THEN C.CareContDate
                 ELSE D.StartDateAssCareCoord
                 END BETWEEN '$rp_startdate' AND '$rp_enddate'
       --AND A.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust--Removed for 2022-23

# COMMAND ----------

# DBTITLE 1,EIP23d - Common
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23d_common: This view is used as do most of the calculation of EIP23d metric. This is later used 
   in the National and CCG breakdowns for all age groupings. The age groups are all calculated together to
   save effort.
   
   Sam Hollings - 2018-02-25                                                                                 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 DROP TABLE IF EXISTS $db_output.EIP23d_common;

 CREATE TABLE $db_output.EIP23d_common USING DELTA AS
      SELECT
      A.UniqServReqID,
       A.OrgIDProv,
       o.NAME as Provider_Name,
       coalesce(mpi.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
        mpi.age_group_lower_chap10,
        mpi.age_group_lower_chap10a,
        mpi.age_group_lower_chap1,
        mpi.Der_Gender,
        mpi.Der_Gender_Desc,
        mpi.UpperEthnicity,
        mpi.LowerEthnicityCode,
        mpi.LowerEthnicityName,
       mpi.LADistrictAuth as MPI_LADistrictAuth,
     mpi.LADistrictAuthCode as LADistrictAuth,
     mpi.LADistrictAuthName,                        
       mpi.IMD_Decile,
       mpi.IMD_Quintile,
       COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
       COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
       COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
       COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
       COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
       COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME,
       DATEDIFF ('$rp_enddate',a.ReferralRequestReceivedDate) AS days_between_endate_ReferralRequestReceivedDate
        FROM  (  SELECT *
                 FROM $db_output.EIP_MHS101Referral_LATEST AS a
                 WHERE (((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') AND a.UniqMonthId = '$end_month_id') OR a.ServDischDate <= '$rp_enddate') 
                   AND (a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate')
                 ) AS a
   INNER JOIN (  SELECT *
                 FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b
                 WHERE B.ServTeamTypeRefToMH = 'A14'
                 ) AS b
             ON a.UniqServReqID = b.UniqServReqID 
   LEFT JOIN $db_output.earliest_care_contact_dates_by_service_request_id AS c
             ON a.UniqServReqID = c.UniqServReqID
   LEFT JOIN $db_output.earliest_care_assessment_dates_by_service_request_id AS d
             ON a.UniqServReqID = d.UniqServReqID
   LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG
             ON CCG.Person_ID = A.Person_ID
   LEFT JOIN $db_output.mpi mpi
             ON a.person_id = mpi.person_id

   LEFT JOIN $db_output.STP_Region_mapping stp 
             ON ccg.IC_Rec_CCG = stp.CCG_code
   left join $db_output.MHB_ORG_DAILY o on A.OrgIDProv = o.ORG_CODE
       WHERE (c.CareContDate IS NULL OR d.StartDateAssCareCoord IS NULL)
       		AND (
 					(
 						(
 							(B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
 						) AND B.UniqMonthID = a.UniqMonthID
 					) OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
 				)
             --AND a.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust--removed for 2022-23

# COMMAND ----------

# DBTITLE 1,EIP23d - Common Prov
 %sql

 DROP TABLE IF EXISTS $db_output.EIP23d_common_Prov;

 CREATE TABLE $db_output.EIP23d_common_Prov USING DELTA AS
      SELECT coalesce(mpi.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
      CASE WHEN AgeServReferRecDate BETWEEN 14 AND 15 THEN '14 to 15'
                        WHEN AgeServReferRecDate BETWEEN 16 AND 17 THEN '16 to 17'
                        WHEN AgeServReferRecDate BETWEEN 18 AND 19 THEN '18 to 19'
                        WHEN AgeServReferRecDate BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeServReferRecDate BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeServReferRecDate BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeServReferRecDate BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeServReferRecDate BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeServReferRecDate BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeServReferRecDate BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeServReferRecDate BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeServReferRecDate BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeServReferRecDate > 64 THEN '65 or over' ELSE "UNKNOWN" END as age_group_lower_chap10,
                        
                         case when AgeServReferRecDate between 0 and 5 then '0 to 5'
                      when AgeServReferRecDate between 6 and 10 then '6 to 10'
                      when AgeServReferRecDate between 11 and 15 then '11 to 15'
                      when AgeServReferRecDate = 16 then '16'
                      when AgeServReferRecDate = 17 then '17'
                      when AgeServReferRecDate = 18 then '18'
                      when AgeServReferRecDate = 19 then '19'
                      when AgeServReferRecDate between 20 and 24 then '20 to 24'
                      when AgeServReferRecDate between 25 and 29 then '25 to 29'
                      when AgeServReferRecDate between 30 and 34 then '30 to 34'
                      when AgeServReferRecDate between 35 and 39 then '35 to 39'
                      when AgeServReferRecDate between 40 and 44 then '40 to 44'
                      when AgeServReferRecDate between 45 and 49 then '45 to 49'
                      when AgeServReferRecDate between 50 and 54 then '50 to 54'
                      when AgeServReferRecDate between 55 and 59 then '55 to 59'
                      when AgeServReferRecDate between 60 and 64 then '60 to 64'
                      when AgeServReferRecDate between 65 and 69 then '65 to 69'
                      when AgeServReferRecDate between 70 and 74 then '70 to 74'
                      when AgeServReferRecDate between 75 and 79 then '75 to 79'
                      when AgeServReferRecDate between 80 and 84 then '80 to 84'
                      when AgeServReferRecDate between 85 and 89 then '85 to 89'
                      when AgeServReferRecDate >= '90' then '90 or over' else "UNKNOWN" end as age_group_lower_chap1,
                      
                      case when AgeServReferRecDate between 0 and 13 then 'Under 14'
                       when AgeServReferRecDate between 14 and 17 then "14 to 17"
                       when AgeServReferRecDate between 18 and 19 then "18 to 19"
                       when AgeServReferRecDate between 20 and 24 then '20 to 24'
                       when AgeServReferRecDate between 25 and 29 then '25 to 29'
                       when AgeServReferRecDate between 30 and 34 then '30 to 34'
                       when AgeServReferRecDate between 35 and 39 then '35 to 39'
                       when AgeServReferRecDate between 40 and 44 then '40 to 44'
                       when AgeServReferRecDate between 45 and 49 then '45 to 49'
                       when AgeServReferRecDate between 50 and 54 then '50 to 54'
                       when AgeServReferRecDate between 55 and 59 then '55 to 59'
                       when AgeServReferRecDate between 60 and 64 then '60 to 64'  
                       when AgeServReferRecDate between 65 and 69 then "65 to 69"
                       when AgeServReferRecDate between 70 and 74 then "70 to 74"
                       when AgeServReferRecDate between 75 and 79 then "75 to 79"
                       when AgeServReferRecDate between 80 and 84 then "80 to 84"
                       when AgeServReferRecDate between 85 and 89 then "85 to 89"
                       when AgeServReferRecDate >= '90' then '90 or over' else "UNKNOWN" end as age_group_lower_chap10a,
                        
             a.UniqServReqID,
 			A.OrgIDProv,
             coalesce(mpi.Der_Gender, "UNKNOWN") as Gender,
             IMD_Quintile,
             coalesce(mpi.UpperEthnicity, "UNKNOWN") AS UpperEthnicity,
             DATEDIFF('$rp_enddate',a.ReferralRequestReceivedDate) as days_between_endate_ReferralRequestReceivedDate
        FROM (  SELECT *
                FROM $db_output.EIP_MHS101Referral_LATEST AS a
                WHERE (((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') AND a.UniqMonthId = '$end_month_id') OR a.ServDischDate <= '$rp_enddate') 
                  AND (a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate')
                ) AS a
  INNER JOIN (  SELECT *
                FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b
                WHERE B.ServTeamTypeRefToMH = 'A14'
                ) AS b
             ON a.UniqServReqID = b.UniqServReqID 
   LEFT JOIN $db_output.earliest_care_contact_dates_by_service_request_id AS c
             ON a.UniqServReqID = c.UniqServReqID
   LEFT JOIN $db_output.earliest_care_assessment_dates_by_service_request_id_Prov AS d -- Uses the Prov version
             ON a.UniqServReqID = d.UniqServReqID             
             AND A.OrgIDProv = D.OrgIDProv -- also joins on Prov to ensure patient began and ended in same provider
   LEFT JOIN $db_output.mpi mpi
             ON a.person_id = mpi.person_id
       WHERE (c.CareContDate IS NULL OR d.StartDateAssCareCoord IS NULL)
       		AND (
 					(
 						(
 							(B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
 						) AND B.UniqMonthID = a.UniqMonthID
 					) OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
 				)
             --AND a.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust

# COMMAND ----------

# DBTITLE 1,EIP23g - Common
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23g_common: This view is used to do most of the calculation of EIP23a metric National, Provider and CCG
   breakdowns. This is different to the other metrics because it doesnt use 
   earliest_care_assessment_dates_by_service_request_id. The age groups are all calculated together to
   save effort 
   
   Sam Hollings - 2018-02-25                                                                                 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 DROP TABLE IF EXISTS $db_output.EIP23g_common;

 CREATE TABLE $db_output.EIP23g_common USING DELTA AS
      SELECT a.UniqServReqID,
             c.CareContDate,
             A.OrgIDProv,
       o.NAME as Provider_Name,
       coalesce(mpi.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
 --        CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
 --             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
 --             WHEN AgeServReferRecDate BETWEEN 18 AND 19 THEN '18 to 19'
 --             WHEN AgeServReferRecDate BETWEEN 20 AND 24 THEN '20 to 24'
 --             WHEN AgeServReferRecDate BETWEEN 25 AND 29 THEN '25 to 29'
 --             WHEN AgeServReferRecDate BETWEEN 30 AND 34 THEN '30 to 34'
 --             WHEN AgeServReferRecDate BETWEEN 35 AND 39 THEN '35 to 39'
 --             WHEN AgeServReferRecDate BETWEEN 40 AND 44 THEN '40 to 44'
 --             WHEN AgeServReferRecDate BETWEEN 45 AND 49 THEN '45 to 49'
 --             WHEN AgeServReferRecDate BETWEEN 50 AND 54 THEN '50 to 54'
 --             WHEN AgeServReferRecDate BETWEEN 55 AND 59 THEN '55 to 59'
 --             WHEN AgeServReferRecDate BETWEEN 60 AND 64 THEN '60 to 64'
 --             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
 --             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
 --             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
 --             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
 --             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
 --             WHEN AgeServReferRecDate >= 90 THEN '90 or over' 
 --             ELSE "UNKNOWN"
 --               END As 
        mpi.age_group_lower_chap10,
       mpi.age_group_lower_chap10a,
        mpi.age_group_lower_chap1,
        mpi.Der_Gender,
        mpi.Der_Gender_Desc,
        mpi.UpperEthnicity,
        mpi.LowerEthnicityCode,
        mpi.LowerEthnicityName,
       mpi.LADistrictAuth as MPI_LADistrictAuth,
   mpi.LADistrictAuthCode as LADistrictAuth,
   mpi.LADistrictAuthName,                 
       mpi.IMD_Decile,
       mpi.IMD_Quintile,
       COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
       COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
       COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
       COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
       COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
       COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME
            
        FROM (  SELECT *
                FROM $db_output.EIP_MHS101Referral_LATEST AS a
                WHERE (((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') AND a.UniqMonthId = '$end_month_id') OR a.ServDischDate <= '$rp_enddate')  
                ) AS a
  INNER JOIN (  SELECT *
                FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b -- DO NOT USE EIP_MHS102ServiceTypeReferredTo_Latest!
                WHERE B.ServTeamTypeRefToMH = 'A14'
                ) AS b
             ON a.UniqServReqID = b.UniqServReqID 
  INNER JOIN (  SELECT *
                FROM $db_output.earliest_care_contact_dates_by_service_request_id AS c
                WHERE c.CareContDate IS NOT NULL
                  AND c.CareContDate BETWEEN '$rp_startdate' AND '$rp_enddate'
                ) AS c
             ON a.UniqServReqID = c.UniqServReqID
   LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG
              ON CCG.Person_ID = A.Person_ID
   LEFT JOIN $db_output.mpi mpi
             ON a.person_id = mpi.person_id
   LEFT JOIN $db_output.STP_Region_mapping stp 
             ON ccg.IC_Rec_CCG = stp.CCG_code
   left join $db_output.MHB_ORG_DAILY o on A.OrgIDProv = o.ORG_CODE
       WHERE ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') 
                 AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
               )
               AND B.UniqMonthID = A.UniqMonthID -- <-- this is why EIP_MHS102ServiceTypeReferredTo_Latest could not be used
              ) 
              OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
             )
             --AND a.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust

# COMMAND ----------

# DBTITLE 1,EIP23h - Common
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23h_common: This view is used to do most of the calculation of EIP23h metric. This is later used 
   in the National and CCG breakdowns for all age groupings. The age groups are all calculated together to
   save effort 
   
   Sam Hollings - 2018-03-07                                                                                 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 DROP TABLE IF EXISTS $db_output.EIP23h_common;

 CREATE TABLE $db_output.EIP23h_common USING DELTA AS

 SELECT a.UniqServReqID,
        d.StartDateAssCareCoord,
        A.OrgIDProv,
        o.NAME as Provider_Name,
        coalesce(mpi.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
        mpi.age_group_lower_chap10,
        mpi.age_group_lower_chap10a,
        mpi.age_group_lower_chap1,
        mpi.Der_Gender,
        mpi.Der_Gender_Desc,
        mpi.UpperEthnicity,
        mpi.LowerEthnicityCode,
        mpi.LowerEthnicityName,
       mpi.LADistrictAuth as MPI_LADistrictAuth,
   mpi.LADistrictAuthCode as LADistrictAuth,
   mpi.LADistrictAuthName,                         
       mpi.IMD_Decile,
       mpi.IMD_Quintile,
       COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
       COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
       COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
       COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
       COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
       COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME
             
        FROM (  SELECT *
                FROM $db_output.EIP_MHS101Referral_LATEST AS a
                WHERE ((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate')
                         AND a.UniqMonthId = '$end_month_id' OR a.ServDischDate <= '$rp_enddate') 
                ) AS a
  INNER JOIN (  SELECT *
                FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b
                WHERE B.ServTeamTypeRefToMH = 'A14'
                ) AS b
             ON a.UniqServReqID = b.UniqServReqID 
  INNER JOIN (  SELECT *
                FROM $db_output.earliest_care_assessment_dates_by_service_request_id AS d
                WHERE d.StartDateAssCareCoord IS NOT NULL
                  AND D.StartDateAssCareCoord	BETWEEN '$rp_startdate' AND '$rp_enddate'
                ) AS d
             ON a.UniqServReqID = d.UniqServReqID
   LEFT JOIN $db_output.MHS001_CCG_LATEST AS ccg
              ON ccg.Person_ID = a.Person_ID
   left join $db_output.mpi mpi on a.person_id = mpi.person_id
   
   LEFT JOIN $db_output.STP_Region_mapping stp 
                  on ccg.IC_Rec_CCG = stp.CCG_code
   left join $db_output.MHB_ORG_DAILY o on A.OrgIDProv = o.ORG_CODE
       WHERE ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') 
                 AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
               )
               AND B.UniqMonthID = A.UniqMonthID 
              ) 
              OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
             )
             --AND a.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust

# COMMAND ----------

# DBTITLE 1,EIP23h - Common Prov
 %sql
 /* ---------------------------------------------------------------------------------------------------------*/
 /* EIP23h_common_Prov: This view is used to do most of the calculation of EIP23h metric Provider breakdown. It needs
   its own prep table due to the use of earliest_care_assessment_dates_by_service_request_id_Prov. 
   The age groups are all calculated together to save effort.
   
   Sam Hollings - 2018-03-07                                                                                 */ 
 /* ---------------------------------------------------------------------------------------------------------*/

 DROP TABLE IF EXISTS $db_output.EIP23h_common_Prov;

 CREATE TABLE $db_output.EIP23h_common_Prov USING DELTA AS
      SELECT a.UniqServReqID,
 			A.OrgIDProv,
             d.StartDateAssCareCoord
        FROM (  SELECT *
                FROM $db_output.EIP_MHS101Referral_LATEST AS a
                WHERE ((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') 
                         AND a.UniqMonthId = '$end_month_id' OR a.ServDischDate <= '$rp_enddate')
                ) AS a
  INNER JOIN (  SELECT *
                FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b
                WHERE B.ServTeamTypeRefToMH = 'A14'
                ) AS b
             ON a.UniqServReqID = b.UniqServReqID 
  INNER JOIN (  SELECT *
                FROM $db_output.earliest_care_assessment_dates_by_service_request_id_Prov AS d -- Uses the Prov version
                WHERE d.StartDateAssCareCoord IS NOT NULL
                  AND D.StartDateAssCareCoord	BETWEEN '$rp_startdate' AND '$rp_enddate'
                ) AS d
             ON a.UniqServReqID = d.UniqServReqID 
             AND A.OrgIDProv = D.OrgIDProv -- also joins on Prov to ensure patient began and ended in same provider
       WHERE ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
               )
               AND B.UniqMonthID = A.UniqMonthID -- <-- this is why EIP_MHS102ServiceTypeReferredTo_Latest could not be used
              ) 
              OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
             )
             --AND a.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust

# COMMAND ----------

# DBTITLE 1,earliest_care_contact_dates_by_service_request_id_any_prof
 %sql
 --/* This is used in EIP64abc, and is similar to earliest_care_contact_dates_by_service_request_id however the join is different */

 DROP TABLE IF EXISTS $db_output.earliest_care_contact_dates_by_service_request_id_any_prof;

 CREATE TABLE $db_output.earliest_care_contact_dates_by_service_request_id_any_prof USING DELTA AS
 SELECT	A.UniqServReqID,
 		MIN (CareContDate) AS CareContDate
 FROM	(  SELECT *
            FROM $db_source.MHS201CareContact AS A
            WHERE A.AttendOrDNACode IN ('5', '6')
              AND ((A.ConsMechanismMH IN ('01','02','04','03') and A.UNiqMonthID <= 1458) ---v4.1 data
                   or
                    (A.ConsMechanismMH IN ('01','02','04','11') and A.UNiqMonthID > 1458)) ---v5 data
            ) AS A
 		LEFT OUTER JOIN $db_output.EIP_MHS101Referral_LATEST AS B
 			ON A.UniqServReqID = B.UniqServReqID
 		LEFT JOIN $db_source.MHS102ServiceTypeReferredTo as x 
             ON a.UniqServReqID = x.UniqServReqID
 WHERE	A.CareContDate >= B.ReferralRequestReceivedDate
 GROUP BY	A.UniqServReqID

# COMMAND ----------

# DBTITLE 1,EIP64abc - Common
 %sql
 DROP TABLE IF EXISTS $db_output.EIP64abc_common;

 CREATE or replace table $db_output.EIP64abc_common AS

 SELECT A.UniqServReqID,
        A.ReferralRequestReceivedDate,
        A.OrgIDProv,
        o.NAME as Provider_Name,
        coalesce(mpi.age_group_higher_level, "UNKNOWN") age_group_higher_level,
        mpi.age_group_lower_chap10,
        mpi.age_group_lower_chap10a,
        mpi.age_group_lower_chap1,
        mpi.Der_Gender,
        mpi.Der_Gender_Desc,
        mpi.UpperEthnicity,
        mpi.LowerEthnicityCode,
        mpi.LowerEthnicityName,
       mpi.LADistrictAuth as MPI_LADistrictAuth,
   mpi.LADistrictAuthCode as LADistrictAuth,
   mpi.LADistrictAuthName,                            
       mpi.IMD_Decile,
       mpi.IMD_Quintile,
       COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
       COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
       COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
       COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
       COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
       COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME,
       GREATEST(C.CareContDate, D.StartDateAssCareCoord) AS CLOCK_STOP
     FROM	 (  SELECT *
                 FROM $db_output.EIP_MHS101Referral_LATEST as A
                 WHERE (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = '$end_month_id') OR A.ServDischDate <= '$rp_enddate')
                 ) AS A
   INNER JOIN $db_output.MHS102ServiceTypeReferredTo_LATEST AS B
 			ON A.UniqServReqID = B.UniqServReqID and a.UniqMonthID = b.UniqMonthID
   INNER JOIN	$db_output.earliest_care_contact_dates_by_service_request_id_any_prof AS C
 			ON A.UniqServReqID = C.UniqServReqID
   INNER JOIN	$db_output.earliest_care_assessment_dates_by_service_request_id_any_team AS D
 			ON A.UniqServReqID = D.UniqServReqID
    LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG ON CCG.Person_ID = A.Person_ID
 LEFT JOIN $db_output.Distinct_Referral_UniqServReqIDs_any_month as F             
 			ON a.UniqServReqID = F.UniqServReqID
    LEFT JOIN $db_output.mpi mpi
 			ON a.person_id = mpi.person_id
    LEFT JOIN $db_output.STP_Region_mapping stp 
             ON ccg.IC_Rec_CCG = stp.CCG_code
     left join $db_output.MHB_ORG_DAILY o on A.OrgIDProv = o.ORG_CODE
 WHERE	F.UniqServReqID IS NULL ---ADDED AT 9/8/21
 AND GREATEST(C.CareContDate, D.StartDateAssCareCoord) BETWEEN '$rp_startdate' AND '$rp_enddate'
 AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = '$end_month_id') OR A.ServDischDate <= '$rp_enddate') --ADDED AT 9/8/21
 --AND A.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust

# COMMAND ----------

# DBTITLE 1,All EIP Referrals common
 %sql
 DROP TABLE IF EXISTS $db_output.EIPRef_common;

 CREATE TABLE $db_output.EIPRef_common USING DELTA AS

 SELECT A.UniqServReqID,
       A.OrgIDProv,
        o.NAME as Provider_Name,
       coalesce(mpi.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
        mpi.age_group_lower_chap10,
       mpi.age_group_lower_chap10a,
        mpi.age_group_lower_chap1,
        mpi.Der_Gender,
        mpi.Der_Gender_Desc,
        mpi.UpperEthnicity,
        mpi.LowerEthnicityCode,
        mpi.LowerEthnicityName,
       mpi.LADistrictAuth as MPI_LADistrictAuth,
   mpi.LADistrictAuthCode as LADistrictAuth,
   mpi.LADistrictAuthName,                       
       mpi.IMD_Decile,
       mpi.IMD_Quintile,
       COALESCE(stp.CCG_CODE,"UNKNOWN") AS CCG_CODE,
       COALESCE(stp.CCG_NAME, "UNKNOWN") as CCG_NAME,
       COALESCE(stp.STP_CODE, "UNKNOWN") as STP_CODE,
       COALESCE(stp.STP_NAME, "UNKNOWN") as STP_NAME, 
       COALESCE(stp.REGION_CODE, "UNKNOWN") as REGION_CODE,
       COALESCE(stp.REGION_NAME, "UNKNOWN") as REGION_NAME
         FROM (  SELECT DISTINCT r.Person_ID,
                      r.UniqServReqID,
                      r.UniqMonthID,
                      r.OrgIDProv,
                      r.ReferralRequestReceivedDate,
                      r.PrimReasonReferralMH,
                      r.ServDischDate,
                      r.AgeServReferRecDate
                 FROM $db_source.MHS101Referral AS r
                WHERE ((RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') AND RecordStartDate <= '$rp_enddate')
                  AND ((R.ServDischDate IS NULL OR R.ServDischDate > '$rp_startdate')
                             AND R.ReferralRequestReceivedDate <= '$rp_enddate')
                   AND r.RecordStartDate between '$rp_startdate' and '$rp_enddate'
                 ) AS A
   INNER JOIN (  SELECT *
                 FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b
                 WHERE B.ServTeamTypeRefToMH = 'A14'
                 ) AS B
              ON a.UniqServReqID = b.UniqServReqID 
    LEFT JOIN $db_output.MHS001_CCG_LATEST AS CCG
              ON CCG.Person_ID = A.Person_ID
    left join $db_output.mpi mpi
              on a.person_id = mpi.person_id
  
    LEFT JOIN $db_output.STP_Region_mapping stp 
              on ccg.IC_Rec_CCG = stp.CCG_code
    left join $db_output.MHB_ORG_DAILY o on A.OrgIDProv = o.ORG_CODE
   WHERE ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate')
                        AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate'))
                      AND B.UniqMonthID = A.UniqMonthID)
                    OR B.ReferClosureDate <= '$rp_enddate'
                    OR B.ReferRejectionDate <= '$rp_enddate')
              --AND A.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust

# COMMAND ----------

 %sql

 DROP TABLE IF EXISTS $db_output.EIPRef_common_Prov;

 CREATE TABLE $db_output.EIPRef_common_Prov USING DELTA AS
      SELECT coalesce(mpi.age_group_higher_level, "UNKNOWN") as age_group_higher_level,
             CASE WHEN AgeServReferRecDate BETWEEN 14 AND 15 THEN '14 to 15'
                        WHEN AgeServReferRecDate BETWEEN 16 AND 17 THEN '16 to 17'
                        WHEN AgeServReferRecDate BETWEEN 18 AND 19 THEN '18 to 19'
                        WHEN AgeServReferRecDate BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeServReferRecDate BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeServReferRecDate BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeServReferRecDate BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeServReferRecDate BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeServReferRecDate BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeServReferRecDate BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeServReferRecDate BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeServReferRecDate BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeServReferRecDate > 64 THEN '65 or over' ELSE "UNKNOWN" END as age_group_lower_chap10,
                        
                         case when AgeServReferRecDate between 0 and 5 then '0 to 5'
                      when AgeServReferRecDate between 6 and 10 then '6 to 10'
                      when AgeServReferRecDate between 11 and 15 then '11 to 15'
                      when AgeServReferRecDate = 16 then '16'
                      when AgeServReferRecDate = 17 then '17'
                      when AgeServReferRecDate = 18 then '18'
                      when AgeServReferRecDate = 19 then '19'
                      when AgeServReferRecDate between 20 and 24 then '20 to 24'
                      when AgeServReferRecDate between 25 and 29 then '25 to 29'
                      when AgeServReferRecDate between 30 and 34 then '30 to 34'
                      when AgeServReferRecDate between 35 and 39 then '35 to 39'
                      when AgeServReferRecDate between 40 and 44 then '40 to 44'
                      when AgeServReferRecDate between 45 and 49 then '45 to 49'
                      when AgeServReferRecDate between 50 and 54 then '50 to 54'
                      when AgeServReferRecDate between 55 and 59 then '55 to 59'
                      when AgeServReferRecDate between 60 and 64 then '60 to 64'
                      when AgeServReferRecDate between 65 and 69 then '65 to 69'
                      when AgeServReferRecDate between 70 and 74 then '70 to 74'
                      when AgeServReferRecDate between 75 and 79 then '75 to 79'
                      when AgeServReferRecDate between 80 and 84 then '80 to 84'
                      when AgeServReferRecDate between 85 and 89 then '85 to 89'
                      when AgeServReferRecDate >= '90' then '90 or over' else "UNKNOWN" end as age_group_lower_chap1,
                      
                      case when AgeServReferRecDate between 0 and 13 then 'Under 14'
                       when AgeServReferRecDate between 14 and 17 then "14 to 17"
                       when AgeServReferRecDate between 18 and 19 then "18 to 19"
                       when AgeServReferRecDate between 20 and 24 then '20 to 24'
                       when AgeServReferRecDate between 25 and 29 then '25 to 29'
                       when AgeServReferRecDate between 30 and 34 then '30 to 34'
                       when AgeServReferRecDate between 35 and 39 then '35 to 39'
                       when AgeServReferRecDate between 40 and 44 then '40 to 44'
                       when AgeServReferRecDate between 45 and 49 then '45 to 49'
                       when AgeServReferRecDate between 50 and 54 then '50 to 54'
                       when AgeServReferRecDate between 55 and 59 then '55 to 59'
                       when AgeServReferRecDate between 60 and 64 then '60 to 64'  
                       when AgeServReferRecDate between 65 and 69 then "65 to 69"
                       when AgeServReferRecDate between 70 and 74 then "70 to 74"
                       when AgeServReferRecDate between 75 and 79 then "75 to 79"
                       when AgeServReferRecDate between 80 and 84 then "80 to 84"
                       when AgeServReferRecDate between 85 and 89 then "85 to 89"
                       when AgeServReferRecDate >= '90' then '90 or over' else "UNKNOWN" end as age_group_lower_chap10a,
                      
             a.UniqServReqID as UniqServReqID,
 			A.OrgIDProv as OrgIDPRov,-- Bring out the provider which will then be grouped on and joined in the aggregate step.
             coalesce(mpi.Der_Gender, "UNKNOWN") as Gender,
             IMD_Quintile,
             coalesce(mpi.UpperEthnicity, "UNKNOWN") AS UpperEthnicity
        FROM (  SELECT DISTINCT r.Person_ID,
                      r.UniqServReqID,
                      r.UniqMonthID,
                      r.OrgIDProv,
                      r.ReferralRequestReceivedDate,
                      r.PrimReasonReferralMH,
                      r.ServDischDate,
                      r.AgeServReferRecDate
                 FROM $db_source.MHS101Referral AS r
                WHERE ((RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') AND RecordStartDate <= '$rp_enddate')
                  AND ((R.ServDischDate IS NULL OR R.ServDischDate > '$rp_startdate')
                             AND R.ReferralRequestReceivedDate <= '$rp_enddate')
                   AND r.RecordStartDate between '$rp_startdate' and '$rp_enddate'
                ) AS A
   left join $db_output.mpi mpi on a.person_id = mpi.person_id
   INNER JOIN $db_output.EIP_MHS102ServiceTypeReferredTo_LATEST AS b 
             ON a.UniqServReqID = b.UniqServReqID 
             AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')) 
 				AND B.UniqMonthID = A.UniqMonthID) 
 			OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate')
       --WHERE A.OrgIDProv <> 'RW5' ---Added 3/11/2021 AT after EIP DQ discussion with Lancashire SC Trust

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.EIP23a_common;
 OPTIMIZE $db_output.EIP23d_common;
 OPTIMIZE $db_output.EIP64abc_common;
 OPTIMIZE $db_output.EIPRef_common;