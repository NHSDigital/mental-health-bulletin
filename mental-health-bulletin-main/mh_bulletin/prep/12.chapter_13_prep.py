# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.Dementia_prep;
 CREATE TABLE        $db_output.Dementia_prep AS 
 ---dementia prep table used for referrals, contacts and attended contacts for memory services
 Select      CC.UniqCareContID
                       ,CCG.IC_REC_CCG
                       ,Ref.OrgIDProv
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,REF.UniqServReqID
                       ,CC.CareContDate
                       ,REF.ServDischDate
                       ,case when Der_Gender = '1' then '1'
                             when Der_Gender = '2' then '2'
                             when Der_Gender = '3' then '3'
                             when Der_Gender = '4' then '4'
                             when Der_Gender = '9' then '9'
                             else 'Unknown' end as Gender 
                     ,case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level
                     ,case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
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
                           when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_level
                     ,AgeRepPeriodEnd
                     ,NHSDEthnicity
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
 FROM       $db_output.mpi as mpi -- Uses the generic prep MPi derivation
 LEFT JOIN  $db_output.MHB_MHS101referral as ref 
            ON mpi.person_id = ref.person_id
 LEFT JOIN  $db_output.MHB_MHS102ServiceTypeReferredTo as serv
            ON serv.UniqServReqID = ref.UniqServReqID --count of activity so join on UniqServReqID
 LEFT JOIN  $db_output.ccg_final as ccg
            ON mpi.person_id = ccg.person_id
 LEFT JOIN  $db_output.mha_la la on la.level = mpi.LADistrictAuth
 LEFT JOIN  $db_output.MHB_MHS201CareContact as cc
            on cc.UniqServReqID = serv.UniqServReqID
            AND CC.UniqCareProfTeamID = serv.UniqCareProfTeamID  --count of activity so join on UniqServReqID and UniqCareProfTeamID
            and (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate') -- contact date in the financial year
 LEFT JOIN   $db_output.stp_region_mapping stp
             on ccg.IC_Rec_CCG = stp.CCG_code
 WHERE       ServTeamTypeRefToMH = 'A17' ---memory services/clinic service team    