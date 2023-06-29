# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.providers_between_rp_start_end_dates;
 CREATE TABLE $db_output.providers_between_rp_start_end_dates USING DELTA AS 
 ---gets list of providers who have submitted data between the start and end dates
 SELECT DISTINCT OrgIDProvider, x.NAME as NAME
            FROM $db_source.MHS000Header as Z
                 LEFT OUTER JOIN global_temp.MHB_RD_ORG_DAILY_LATEST AS X
 					ON Z.OrgIDProvider = X.ORG_CODE
           WHERE	ReportingPeriodStartDate >= '$rp_startdate'
             AND ReportingPeriodEndDate <= '$rp_enddate';

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHS001_PATMRECINRP_201819_F_M;
 CREATE TABLE $db_output.MHS001_PATMRECINRP_201819_F_M USING DELTA AS
 ---replicates PatMRecInRP for Feb and Mar 2018 to account for these derivation not being available on legacy data. It has been coded so that this only makes a difference if the relevant reporting period is called, otherwise the subsequent
 
 SELECT MPI.Person_ID,
        MPI.UniqSubmissionID,
        MPI.UniqMonthID,
        CASE WHEN x.Person_ID IS NULL THEN False ELSE True END AS  PatMRecInRP_temp       
 
 FROM $db_source.mhs001MPI MPI
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
 WHERE MPI.UniqMonthID IN ('1427', '1428'); ---Feb and Mar 2018 UniqMonthID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHS001MPI_PATMRECINRP_FIX;
 CREATE TABLE $db_output.MHS001MPI_PATMRECINRP_FIX USING DELTA AS
 ---applies PatMRecInRP fix for Feb and Mar 2018 to MPI data
 SELECT MPI.*,
        CASE WHEN FIX.Person_ID IS NULL THEN MPI.PatMRecInRP ELSE FIX.PatMRecInRP_temp END AS PatMRecInRP_FIX
 FROM $db_source.mhs001MPI MPI
 LEFT JOIN $db_output.MHS001_PATMRECINRP_201819_F_M FIX
 ON MPI.Person_ID = FIX.Person_ID AND MPI.UniqSubmissionID = FIX.UniqSubmissionID and MPI.UniqMonthID = FIX.UniqMonthID;

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.CCG_prep_EIP;
 CREATE TABLE $db_output.CCG_prep_EIP USING DELTA AS
 ---get max record number for a person in MPI
 SELECT DISTINCT    a.Person_ID,
 				   max(a.RecordNumber) as recordnumber ---latest record number for a patient                   
 FROM               $db_source.MHS001MPI a
 LEFT JOIN          $db_source.MHS002GP b ---CCG info derived from latest record number for a patient
 		           on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
 		           and a.recordnumber = b.recordnumber
 		           and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
 		           and b.EndDateGMPRegistration is null                   
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    and a.uniqmonthid between '$month_id_end' - 11 AND '$month_id_end'                   
 GROUP BY           a.Person_ID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHS001_CCG_LATEST;
 CREATE TABLE $db_output.MHS001_CCG_LATEST USING DELTA as 
 ---gets ccg of practice or residence for each person
 select distinct    a.Person_ID,
 				   CASE WHEN b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice ---if ccg of gp practice is not null and valid for financial year then use that 
 					    WHEN A.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes ---if ccg of residence is not null and valid for financial year then use that 
 						ELSE 'UNKNOWN' END AS IC_Rec_CCG 		
 FROM               $db_source.mhs001MPI a
 LEFT JOIN          $db_source.MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null
 INNER JOIN         $db_output.CCG_prep_EIP ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    and a.uniqmonthid between '$month_id_end' - 11 AND '$month_id_end'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHS006MHCareCoord_LATEST;
 CREATE TABLE $db_output.MHS006MHCareCoord_LATEST USING DELTA AS 
 ---gets care co-ordinator information needed for 10e metric
 SELECT DISTINCT c.CareProfServOrTeamTypeAssoc, 
     c.UniqMonthID,
     c.OrgIDProv,
     c.StartDateAssCareCoord,
     c.Person_ID,
     c.EndDateAssCareCoord
 FROM $db_source.MHS006MHCareCoord as c
 WHERE ((c.RecordEndDate IS NULL OR c.RecordEndDate >= '$rp_enddate') AND c.RecordStartDate <= '$rp_enddate') ---ensures record is still active

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHS101Referral_LATEST;
 CREATE TABLE $db_output.MHS101Referral_LATEST USING DELTA AS
 ---gets required referral information for making cohort of people EIP specific
 SELECT DISTINCT r.Person_ID,
      r.UniqServReqID,
      r.UniqMonthID,
      r.OrgIDProv,
      r.ReferralRequestReceivedDate,
      r.PrimReasonReferralMH,
      r.ServDischDate,
      r.AgeServReferRecDate
 FROM $db_source.MHS101Referral AS r
 WHERE ((RecordEndDate IS NULL OR RecordEndDate >= '$rp_enddate') AND RecordStartDate <= '$rp_enddate') ---ensures record is still active

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_MHS101Referral_LATEST;
 CREATE TABLE $db_output.EIP_MHS101Referral_LATEST USING DELTA AS
 ---open referrals in RP with request after 2016 with Primary Referral Reason as suspected first episode of psychosis
 SELECT DISTINCT r.Person_ID,
            r.UniqServReqID,
            r.UniqMonthID,
            r.OrgIDProv,
            r.ReferralRequestReceivedDate,
            r.PrimReasonReferralMH,
            r.ServDischDate,
            r.AgeServReferRecDate,
            CCG.IC_Rec_CCG ---ccg of practice or residence
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
           ON CCG.Person_ID = E.Person_ID     

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MHS102ServiceTypeReferredTo_LATEST;
 CREATE TABLE $db_output.MHS102ServiceTypeReferredTo_LATEST USING DELTA AS
 ---gets required service team type information for making cohort of people EIP specific
 SELECT S.UniqMonthID
     ,S.OrgIDProv
     ,S.ReferClosureDate
     ,S.ReferRejectionDate
     ,S.ServTeamTypeRefToMH
     ,S.UniqCareProfTeamID
     ,S.Person_ID
     ,S.UniqServReqID
 FROM $db_source.MHS102ServiceTypeReferredTo s
 WHERE ((s.RecordEndDate IS NULL OR s.RecordEndDate >= '$rp_enddate') AND s.RecordStartDate <= '$rp_enddate') ---ensures record is still active

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP_MHS102ServiceTypeReferredTo_LATEST;
 CREATE TABLE $db_output.EIP_MHS102ServiceTypeReferredTo_LATEST USING DELTA AS
 ---get open referrals which weren't rejected or closed in RP where service type referred to is Early Intervention is Psychosis
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

 %sql
 DROP TABLE IF EXISTS $db_output.earliest_care_contact_dates_by_service_request_id;
 CREATE TABLE $db_output.earliest_care_contact_dates_by_service_request_id USING DELTA AS 
 ---get the earliest care contact date for the open EIP referrals where service team type was psychosis team in the financial year where the contact was attended
 SELECT a.UniqServReqID,
     MIN(CareContDate) AS CareContDate ---first care contact date
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
     AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$month_id_end') OR b.ServDischDate <= '$rp_enddate')
     AND a.UniqMonthId <= '$month_id_end'
 GROUP BY a.UniqServReqID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.earliest_care_assessment_dates_by_service_request_id;
 CREATE TABLE $db_output.earliest_care_assessment_dates_by_service_request_id USING DELTA AS 
 ---get the earliest care coordinator assessment date for the open EIP referrals where service team type was psychosis team in the financial year where the contact was attended
 SELECT B.UniqServReqID,
      MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord ---first care coordinator assessment date
 FROM $db_output.MHS006MHCareCoord_LATEST AS a
 LEFT JOIN $db_output.EIP_MHS101Referral_LATEST AS B
      ON A.Person_ID = B.Person_ID
 WHERE A.CareProfServOrTeamTypeAssoc = 'A14'
      AND A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
      AND ((B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') OR A.StartDateAssCareCoord <= B.ServDischDate)
      AND (((A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') AND A.UniqMonthID = '$month_id_end') OR A.EndDateAssCareCoord <= '$rp_enddate')
      AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$month_id_end') OR b.ServDischDate <= '$rp_enddate')
 GROUP BY B.UniqServReqID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.earliest_care_assessment_dates_by_service_request_id_Prov;
 CREATE TABLE $db_output.earliest_care_assessment_dates_by_service_request_id_Prov USING DELTA AS 
 ---get the earliest care coordinator assessment date for the open EIP referrals where service team type was psychosis team and provider in the financial year where the contact was attended
 SELECT B.UniqServReqID,
      B.OrgIDProv,
      MIN (A.StartDateAssCareCoord) AS StartDateAssCareCoord ---first care coordinator assessment date
 FROM $db_output.MHS006MHCareCoord_LATEST AS a
 LEFT JOIN $db_output.EIP_MHS101Referral_LATEST AS B
      ON A.Person_ID = B.Person_ID
 WHERE A.CareProfServOrTeamTypeAssoc = 'A14'
      AND A.StartDateAssCareCoord >= B.ReferralRequestReceivedDate
      AND ((B.ServDischDate IS NULL OR B.ServDischDate > '$rp_enddate') OR A.StartDateAssCareCoord <= B.ServDischDate)
      AND (((A.EndDateAssCareCoord IS NULL OR A.EndDateAssCareCoord > '$rp_enddate') AND A.UniqMonthID = '$month_id_end') OR A.EndDateAssCareCoord <= '$rp_enddate')
      AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$month_id_end') OR b.ServDischDate <= '$rp_enddate')
 GROUP BY B.UniqServReqID, B.OrgIDProv

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.earliest_care_assessment_dates_by_service_request_id_any_team;
 CREATE TABLE $db_output.earliest_care_assessment_dates_by_service_request_id_any_team USING DELTA AS
 ---get the earliest care coordinator assessment date for the open EIP referrals where service team type was any team in the financial year where the contact was attended
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
               AND A.UniqMonthID = '$month_id_end'
            ) 
          OR A.EndDateAssCareCoord <= '$rp_enddate'
          )
      AND (((b.ServDischDate IS NULL OR b.ServDischDate > '$rp_enddate') AND b.UniqMonthId = '$month_id_end') OR b.ServDischDate <= '$rp_enddate')
 GROUP BY B.UniqServReqID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Distinct_Referral_UniqServReqIDs;
 CREATE TABLE $db_output.Distinct_Referral_UniqServReqIDs USING DELTA AS
 ---get distinct open eip referrals where the referral was referred service team type was a psychosis team in the same month
 select distinct a.UniqServReqID
 from $db_output.EIP_MHS101Referral_LATEST a 
 left join $db_output.MHS102ServiceTypeReferredTo_LATEST b 
     ON A.UniqServReqID = B.UniqServReqID 
     and a.UniqMonthID = b.UniqMonthID
 where B.ServTeamTypeRefToMH = 'A14'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Distinct_Referral_UniqServReqIDs_any_month;
 CREATE TABLE $db_output.Distinct_Referral_UniqServReqIDs_any_month USING DELTA AS
 ---get distinct open eip referrals where the referral was referred service team type was a psychosis team in any month of the financial year
 select distinct a.UniqServReqID
 from $db_output.EIP_MHS101Referral_LATEST a 
 left join (  SELECT *
      FROM $db_output.MHS102ServiceTypeReferredTo_LATEST
      WHERE ServTeamTypeRefToMH = 'A14') AS b 
   ON A.UniqServReqID = B.UniqServReqID 
 where B.ServTeamTypeRefToMH = 'A14'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP23a_common;
 CREATE TABLE $db_output.EIP23a_common USING DELTA AS
 ---prep table for eip referrals pathway that entered treatment at any time in the financial year with required demographic and geographic breakdowns
 SELECT A.UniqServReqID,
       case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
        CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
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
             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
             WHEN AgeServReferRecDate >= 90 THEN '90 or over' 
             ELSE 'Unknown'
               END As AGE_GROUP,
        CCG.IC_Rec_CCG,
        case when Der_Gender = '1' then '1'
             when Der_Gender = '2' then '2'
             when Der_Gender = '3' then '3'
             when Der_Gender = '4' then '4'
             when Der_Gender = '9' then '9'
             else 'Unknown' end as Gender,
        mpi.nhsdEthnicity
       ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
             WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
             WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
             WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
             WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
             WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
             WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
               ELSE 'Unknown' END AS UpperEthnicity
       ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'A'
                    WHEN mpi.NHSDEthnicity = 'B' THEN 'B'
                    WHEN mpi.NHSDEthnicity = 'C' THEN 'C'
                    WHEN mpi.NHSDEthnicity = 'D' THEN 'D'
                    WHEN mpi.NHSDEthnicity = 'E' THEN 'E'
                    WHEN mpi.NHSDEthnicity = 'F' THEN 'F'
                    WHEN mpi.NHSDEthnicity = 'G' THEN 'G'
                    WHEN mpi.NHSDEthnicity = 'H' THEN 'H'
                    WHEN mpi.NHSDEthnicity = 'J' THEN 'J'
                    WHEN mpi.NHSDEthnicity = 'K' THEN 'K'
                    WHEN mpi.NHSDEthnicity = 'L' THEN 'L'
                    WHEN mpi.NHSDEthnicity = 'M' THEN 'M'
                    WHEN mpi.NHSDEthnicity = 'N' THEN 'N'
                    WHEN mpi.NHSDEthnicity = 'P' THEN 'P'
                    WHEN mpi.NHSDEthnicity = 'R' THEN 'R'
                    WHEN mpi.NHSDEthnicity = 'S' THEN 'S'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                            ELSE 'Unknown' END AS LowerEthnicity
       ,case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                           when mpi.LADistrictAuth = 'L99999999' then 'L99999999'
                           when mpi.LADistrictAuth = 'M99999999' then 'M99999999'
                           when mpi.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when mpi.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth
       ,IMD_Decile
       ,IMD_Quintile
       ,stp.STP_code
       ,stp.Region_code 
       ,DATEDIFF (
            CASE WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
                   ELSE D.StartDateAssCareCoord END,
                        A.ReferralRequestReceivedDate
                       ) days_between_ReferralRequestReceivedDate
         FROM (  SELECT *
                 FROM $db_output.EIP_MHS101Referral_LATEST AS a
                 WHERE (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = '$month_id_end') OR A.ServDischDate <= '$rp_enddate') 
                 ) AS A
   INNER JOIN (  SELECT *
                 FROM $db_output.MHS102ServiceTypeReferredTo_LATEST AS b
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
    left join           $db_output.mhb_la la on la.level = mpi.LADistrictAuth
  LEFT JOIN   $db_output.STP_Region_mapping stp 
                  on ccg.IC_Rec_CCG = stp.CCG_code
   WHERE CASE WHEN C.CareContDate > D.StartDateAssCareCoord 
                       THEN C.CareContDate
                       ELSE D.StartDateAssCareCoord
                       END BETWEEN '$rp_startdate' AND '$rp_enddate'
               AND ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate')
                        AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate'))
                      AND B.UniqMonthID = A.UniqMonthID)
                    OR B.ReferClosureDate <= '$rp_enddate'
                    OR B.ReferRejectionDate <= '$rp_enddate')
              AND A.OrgIDProv <> 'RW5' 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP23a_common_Prov;
 CREATE TABLE $db_output.EIP23a_common_Prov USING DELTA AS
 ---prep table for eip referrals pathway that entered treatment at any time in the financial year at demographic and provider level
      SELECT   case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
             CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
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
             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
             WHEN AgeServReferRecDate >= 90 THEN '90 or over' 
             ELSE 'Unknown'
               END As AGE_GROUP,
             a.UniqServReqID as UniqServReqID,
 			A.OrgIDProv as OrgIDPRov,-- Bring out the provider which will then be grouped on and joined in the aggregate step.
             case when Der_Gender = '1' then '1'
                 when Der_Gender = '2' then '2'
                 when Der_Gender = '3' then '3'
                 when Der_Gender = '4' then '4'
                 when Der_Gender = '9' then '9'
                 else 'Unknown' end as Gender,
             IMD_Quintile,
             CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
             WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
             WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
             WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
             WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
             WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
             WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
               ELSE 'Unknown' END AS UpperEthnicity,
             DATEDIFF (
                      CASE WHEN C.CareContDate > D.StartDateAssCareCoord THEN C.CareContDate
                      ELSE D.StartDateAssCareCoord
                      END,
                      A.ReferralRequestReceivedDate
                      ) days_between_ReferralRequestReceivedDate --Clock Stop
        FROM (  SELECT *
                FROM $db_output.EIP_MHS101Referral_LATEST AS a
                WHERE (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthId = '$month_id_end') OR A.ServDischDate <= '$rp_enddate')
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
       AND A.OrgIDProv <> 'RW5'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP23d_common;
 CREATE TABLE $db_output.EIP23d_common USING DELTA AS
 ---prep table for eip referrals pathway that entered treatment within 2 weeks in the financial year with required demographic and geographic breakdowns
      SELECT case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
      CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
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
             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
             WHEN AgeServReferRecDate >= 90 THEN '90 or over'
             ELSE 'Unknown'
               END As AGE_GROUP,
             a.UniqServReqID,
             ccg.IC_Rec_CCG,
             case when Der_Gender = '1' then '1'
                 when Der_Gender = '2' then '2'
                 when Der_Gender = '3' then '3'
                 when Der_Gender = '4' then '4'
                 when Der_Gender = '9' then '9'
                 else 'Unknown' end as Gender,
              mpi.NHSDEthnicity
              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
             WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
             WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
             WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
             WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
             WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
             WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
               ELSE 'Unknown' END AS UpperEthnicity
       ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'A'
                    WHEN mpi.NHSDEthnicity = 'B' THEN 'B'
                    WHEN mpi.NHSDEthnicity = 'C' THEN 'C'
                    WHEN mpi.NHSDEthnicity = 'D' THEN 'D'
                    WHEN mpi.NHSDEthnicity = 'E' THEN 'E'
                    WHEN mpi.NHSDEthnicity = 'F' THEN 'F'
                    WHEN mpi.NHSDEthnicity = 'G' THEN 'G'
                    WHEN mpi.NHSDEthnicity = 'H' THEN 'H'
                    WHEN mpi.NHSDEthnicity = 'J' THEN 'J'
                    WHEN mpi.NHSDEthnicity = 'K' THEN 'K'
                    WHEN mpi.NHSDEthnicity = 'L' THEN 'L'
                    WHEN mpi.NHSDEthnicity = 'M' THEN 'M'
                    WHEN mpi.NHSDEthnicity = 'N' THEN 'N'
                    WHEN mpi.NHSDEthnicity = 'P' THEN 'P'
                    WHEN mpi.NHSDEthnicity = 'R' THEN 'R'
                    WHEN mpi.NHSDEthnicity = 'S' THEN 'S'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                            ELSE 'Unknown' END AS LowerEthnicity
                     ,case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                           when mpi.LADistrictAuth is null then 'Unknown'
                           when mpi.LADistrictAuth = '' then 'Unknown'
                           else mpi.LADistrictAuth end as LADistrictAuth
                     ,IMD_Decile
                     ,IMD_Quintile
                    ,stp.STP_code
                     ,stp.Region_code 
              ,DATEDIFF ('$rp_enddate',a.ReferralRequestReceivedDate) AS days_between_endate_ReferralRequestReceivedDate
        FROM  (  SELECT *
                 FROM $db_output.EIP_MHS101Referral_LATEST AS a
                 WHERE (((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') AND a.UniqMonthId = '$month_id_end') OR a.ServDischDate <= '$rp_enddate') 
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
       WHERE (c.CareContDate IS NULL OR d.StartDateAssCareCoord IS NULL)
       		AND (
 					(
 						(
 							(B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate') AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate')
 						) AND B.UniqMonthID = a.UniqMonthID
 					) OR B.ReferClosureDate <= '$rp_enddate' OR B.ReferRejectionDate <= '$rp_enddate'
 				)
             AND a.OrgIDProv <> 'RW5'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP23d_common_Prov;
 CREATE TABLE $db_output.EIP23d_common_Prov USING DELTA AS
 ---prep table for eip referrals pathway that entered treatment within 2 weeks in the financial year at demographic and provider level
      SELECT  case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
      CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
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
             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
             WHEN AgeServReferRecDate >= 90 THEN '90 or over' 
             ELSE 'Unknown'
               END As AGE_GROUP,
             a.UniqServReqID,
 			A.OrgIDProv,
             case when Der_Gender = '1' then '1'
                 when Der_Gender = '2' then '2'
                 when Der_Gender = '3' then '3'
                 when Der_Gender = '4' then '4'
                 when Der_Gender = '9' then '9'
                 else 'Unknown' end as Gender,
             IMD_Quintile,
             CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
             WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
             WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
             WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
             WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
             WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
             WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
               ELSE 'Unknown' END AS UpperEthnicity,
             DATEDIFF('$rp_enddate',a.ReferralRequestReceivedDate) as days_between_endate_ReferralRequestReceivedDate
        FROM (  SELECT *
                FROM $db_output.EIP_MHS101Referral_LATEST AS a
                WHERE (((a.ServDischDate IS NULL OR a.ServDischDate > '$rp_enddate') AND a.UniqMonthId = '$month_id_end') OR a.ServDischDate <= '$rp_enddate') 
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
             AND a.OrgIDProv <> 'RW5'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIP64abc_common;
 CREATE TABLE $db_output.EIP64abc_common USING DELTA AS
 ---prep table for referrals not on eip pathway, receiving a first contact and assigned a care co-ordinator with any team in the financial year with required demographic and geographic breakdowns
 SELECT	  case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
             CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
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
             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
             WHEN AgeServReferRecDate >= 90 THEN '90 or over'
             ELSE 'Unknown'
               END As AGE_GROUP,
 		A.OrgIDProv, -- will be used for Prov
 		A.UniqServReqID,
 		ccg.IC_Rec_CCG, -- will also be used for CCG
 		a.ReferralRequestReceivedDate, -- used for EIP66 and EIP65
         case when Der_Gender = '1' then '1'
             when Der_Gender = '2' then '2'
             when Der_Gender = '3' then '3'
             when Der_Gender = '4' then '4'
             when Der_Gender = '9' then '9'
             else 'Unknown' end as Gender,
              nhsdEthnicity
              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
             WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
             WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
             WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
             WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
             WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
             WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
               ELSE 'Unknown' END AS UpperEthnicity
       ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'A'
                    WHEN mpi.NHSDEthnicity = 'B' THEN 'B'
                    WHEN mpi.NHSDEthnicity = 'C' THEN 'C'
                    WHEN mpi.NHSDEthnicity = 'D' THEN 'D'
                    WHEN mpi.NHSDEthnicity = 'E' THEN 'E'
                    WHEN mpi.NHSDEthnicity = 'F' THEN 'F'
                    WHEN mpi.NHSDEthnicity = 'G' THEN 'G'
                    WHEN mpi.NHSDEthnicity = 'H' THEN 'H'
                    WHEN mpi.NHSDEthnicity = 'J' THEN 'J'
                    WHEN mpi.NHSDEthnicity = 'K' THEN 'K'
                    WHEN mpi.NHSDEthnicity = 'L' THEN 'L'
                    WHEN mpi.NHSDEthnicity = 'M' THEN 'M'
                    WHEN mpi.NHSDEthnicity = 'N' THEN 'N'
                    WHEN mpi.NHSDEthnicity = 'P' THEN 'P'
                    WHEN mpi.NHSDEthnicity = 'R' THEN 'R'
                    WHEN mpi.NHSDEthnicity = 'S' THEN 'S'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                            ELSE 'Unknown' END AS LowerEthnicity
                     ,case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                           when mpi.LADistrictAuth is null then 'Unknown'
                           when mpi.LADistrictAuth = '' then 'Unknown'
                           else mpi.LADistrictAuth end as LADistrictAuth
                     ,IMD_Decile
                     ,IMD_Quintile
                    ,stp.STP_code
                     ,stp.Region_code
 		,GREATEST(C.CareContDate, D.StartDateAssCareCoord) AS CLOCK_STOP
     FROM	 (  SELECT *
                 FROM $db_output.EIP_MHS101Referral_LATEST as A
                 WHERE (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = '$month_id_end') OR A.ServDischDate <= '$rp_enddate')
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
 WHERE	F.UniqServReqID IS NULL
 AND GREATEST(C.CareContDate, D.StartDateAssCareCoord) BETWEEN '$rp_startdate' AND '$rp_enddate'
 AND (((A.ServDischDate IS NULL OR A.ServDischDate > '$rp_enddate') AND A.UniqMonthID = '$month_id_end') OR A.ServDischDate <= '$rp_enddate')
 AND A.OrgIDProv <> 'RW5'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIPRef_common;
 CREATE TABLE $db_output.EIPRef_common USING DELTA AS
 ---prep table for eip referrals pathway in the financial year with required demographic and geographic breakdowns
 SELECT A.UniqServReqID,
       case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
        CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
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
             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
             WHEN AgeServReferRecDate >= 90 THEN '90 or over' 
             ELSE 'Unknown'
               END As AGE_GROUP,
        CCG.IC_Rec_CCG,
        case when Der_Gender = '1' then '1'
             when Der_Gender = '2' then '2'
             when Der_Gender = '3' then '3'
             when Der_Gender = '4' then '4'
             when Der_Gender = '9' then '9'
             else 'Unknown' end as Gender,
        mpi.nhsdEthnicity
       ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
             WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
             WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
             WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
             WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
             WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
             WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
               ELSE 'Unknown' END AS UpperEthnicity
       ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'A'
                    WHEN mpi.NHSDEthnicity = 'B' THEN 'B'
                    WHEN mpi.NHSDEthnicity = 'C' THEN 'C'
                    WHEN mpi.NHSDEthnicity = 'D' THEN 'D'
                    WHEN mpi.NHSDEthnicity = 'E' THEN 'E'
                    WHEN mpi.NHSDEthnicity = 'F' THEN 'F'
                    WHEN mpi.NHSDEthnicity = 'G' THEN 'G'
                    WHEN mpi.NHSDEthnicity = 'H' THEN 'H'
                    WHEN mpi.NHSDEthnicity = 'J' THEN 'J'
                    WHEN mpi.NHSDEthnicity = 'K' THEN 'K'
                    WHEN mpi.NHSDEthnicity = 'L' THEN 'L'
                    WHEN mpi.NHSDEthnicity = 'M' THEN 'M'
                    WHEN mpi.NHSDEthnicity = 'N' THEN 'N'
                    WHEN mpi.NHSDEthnicity = 'P' THEN 'P'
                    WHEN mpi.NHSDEthnicity = 'R' THEN 'R'
                    WHEN mpi.NHSDEthnicity = 'S' THEN 'S'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                            ELSE 'Unknown' END AS LowerEthnicity
       ,case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                           when mpi.LADistrictAuth = 'L99999999' then 'L99999999'
                           when mpi.LADistrictAuth = 'M99999999' then 'M99999999'
                           when mpi.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when mpi.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth
       ,IMD_Decile
       ,IMD_Quintile
       ,stp.STP_code
       ,stp.Region_code
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
    left join $db_output.mhb_la la
              on la.level = mpi.LADistrictAuth
    LEFT JOIN $db_output.STP_Region_mapping stp 
              on ccg.IC_Rec_CCG = stp.CCG_code
   WHERE ((((B.ReferClosureDate IS NULL OR B.ReferClosureDate > '$rp_enddate')
                        AND (B.ReferRejectionDate IS NULL OR B.ReferRejectionDate > '$rp_enddate'))
                      AND B.UniqMonthID = A.UniqMonthID)
                    OR B.ReferClosureDate <= '$rp_enddate'
                    OR B.ReferRejectionDate <= '$rp_enddate')
              AND A.OrgIDProv <> 'RW5' 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.EIPRef_common_Prov;
 CREATE TABLE $db_output.EIPRef_common_Prov USING DELTA AS
 ---prep table for eip referrals pathway in the financial year at demographic and provider level
      SELECT   case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
             CASE WHEN AgeServReferRecDate BETWEEN 0 and 13 THEN '0 to 13'
             WHEN AgeServReferRecDate BETWEEN 14 and 17 THEN '14 to 17'
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
             WHEN AgeServReferRecDate BETWEEN 65 AND 69 THEN '65 to 69'
             WHEN AgeServReferRecDate BETWEEN 70 AND 74 THEN '70 to 74'
             WHEN AgeServReferRecDate BETWEEN 75 AND 79 THEN '75 to 79'
             WHEN AgeServReferRecDate BETWEEN 80 AND 84 THEN '80 to 84'
             WHEN AgeServReferRecDate BETWEEN 85 AND 89 THEN '85 to 89' 
             WHEN AgeServReferRecDate >= 90 THEN '90 or over' 
             ELSE 'Unknown'
               END As AGE_GROUP,
             a.UniqServReqID as UniqServReqID,
 			A.OrgIDProv as OrgIDPRov,-- Bring out the provider which will then be grouped on and joined in the aggregate step.
             case when Der_Gender = '1' then '1'
                 when Der_Gender = '2' then '2'
                 when Der_Gender = '3' then '3'
                 when Der_Gender = '4' then '4'
                 when Der_Gender = '9' then '9'
                 else 'Unknown' end as Gender,
             IMD_Quintile,
             CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
             WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
             WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
             WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
             WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
             WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
             WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
               ELSE 'Unknown' END AS UpperEthnicity
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
       WHERE A.OrgIDProv <> 'RW5'