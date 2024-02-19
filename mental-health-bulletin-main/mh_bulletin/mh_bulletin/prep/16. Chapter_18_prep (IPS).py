# Databricks notebook source
# DBTITLE 1,Get MPI Table for Rolling 12 month period
 %sql
 DROP TABLE IF EXISTS $db_output.ips_mpi;
 CREATE TABLE $db_output.ips_mpi AS 
 SELECT 
 MPI.uniqmonthid
 ,MPI.person_id
 ,MPI.OrgIDProv
 ,OD.NAME AS OrgIDProvName
 ,MPI.recordnumber
 ,MPI.PatMRecInRP
 ,MPI.NHSDEthnicity
            ,CASE WHEN MPI.NHSDEthnicity = 'A' THEN 'A'
                  WHEN MPI.NHSDEthnicity = 'B' THEN 'B'
                  WHEN MPI.NHSDEthnicity = 'C' THEN 'C'
                  WHEN MPI.NHSDEthnicity = 'D' THEN 'D'
                  WHEN MPI.NHSDEthnicity = 'E' THEN 'E'
                  WHEN MPI.NHSDEthnicity = 'F' THEN 'F'
                  WHEN MPI.NHSDEthnicity = 'G' THEN 'G'
                  WHEN MPI.NHSDEthnicity = 'H' THEN 'H'
                  WHEN MPI.NHSDEthnicity = 'J' THEN 'J'
                  WHEN MPI.NHSDEthnicity = 'K' THEN 'K'
                  WHEN MPI.NHSDEthnicity = 'L' THEN 'L'
                  WHEN MPI.NHSDEthnicity = 'M' THEN 'M'
                  WHEN MPI.NHSDEthnicity = 'N' THEN 'N'
                  WHEN MPI.NHSDEthnicity = 'P' THEN 'P'
                  WHEN MPI.NHSDEthnicity = 'R' THEN 'R'
                  WHEN MPI.NHSDEthnicity = 'S' THEN 'S'
                  WHEN MPI.NHSDEthnicity = 'Z' THEN 'Z'
                  WHEN MPI.NHSDEthnicity = '99' THEN '99'
                  ELSE 'Unknown' END AS LowerEthnicityCode
             ,CASE WHEN MPI.NHSDEthnicity = 'A' THEN 'British'
                   WHEN MPI.NHSDEthnicity = 'B' THEN 'Irish'
                   WHEN MPI.NHSDEthnicity = 'C' THEN 'Any Other White Background'
                   WHEN MPI.NHSDEthnicity = 'D' THEN 'White and Black Caribbean' 
                   WHEN MPI.NHSDEthnicity = 'E' THEN 'White and Black African'
                   WHEN MPI.NHSDEthnicity = 'F' THEN 'White and Asian'
                   WHEN MPI.NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
                   WHEN MPI.NHSDEthnicity = 'H' THEN 'Indian'
                   WHEN MPI.NHSDEthnicity = 'J' THEN 'Pakistani'
                   WHEN MPI.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                   WHEN MPI.NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
                   WHEN MPI.NHSDEthnicity = 'M' THEN 'Caribbean'
                   WHEN MPI.NHSDEthnicity = 'N' THEN 'African'
                   WHEN MPI.NHSDEthnicity = 'P' THEN 'Any Other Black Background'
                   WHEN MPI.NHSDEthnicity = 'R' THEN 'Chinese'
                   WHEN MPI.NHSDEthnicity = 'S' THEN 'Any Other Ethnic Group'
                   WHEN MPI.NHSDEthnicity = 'Z' THEN 'Not Stated'
                   WHEN MPI.NHSDEthnicity = '99' THEN 'Not Known' 
                   ELSE 'Unknown' END AS LowerEthnicityName   
                  
                  ,CASE WHEN MPI.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN MPI.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN MPI.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN MPI.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN MPI.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN MPI.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN MPI.NHSDEthnicity = '99' THEN 'Not Known'
                    ELSE 'Unknown' END AS UpperEthnicity
                                     
                   ,MPI.Gender
            ,MPI.GenderIDCode
            ,CASE WHEN MPI.GenderIDCode IN ('1','2','3','4') THEN MPI.GenderIDCode 
                  WHEN MPI.Gender IN ('1','2','9') THEN MPI.Gender 
                  ELSE 'Unknown' END AS Der_Gender          
                  ,MPI.AgeRepPeriodEnd
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_common
  
                      ,CASE
                 WHEN IMD.DECI_IMD = 10 THEN '10 Least deprived'
                 WHEN IMD.DECI_IMD = 9 THEN '09 Less deprived'
                 WHEN IMD.DECI_IMD = 8 THEN '08 Less deprived'
                 WHEN IMD.DECI_IMD = 7 THEN '07 Less deprived'
                 WHEN IMD.DECI_IMD = 6 THEN '06 Less deprived'
                 WHEN IMD.DECI_IMD = 5 THEN '05 More deprived'
                 WHEN IMD.DECI_IMD = 4 THEN '04 More deprived'
                 WHEN IMD.DECI_IMD = 3 THEN '03 More deprived'
                 WHEN IMD.DECI_IMD = 2 THEN '02 More deprived'
                 WHEN IMD.DECI_IMD = 1 THEN '01 Most deprived'
                 ELSE 'Unknown' -- updated to Unknown tbc
                 END AS IMD_Decile
                 FROM $db_source.MHS001MPI MPI
 LEFT JOIN $reference_db.ENGLISH_INDICES_OF_DEP_V02 IMD 
                     on MPI.LSOA2011 = IMD.LSOA_CODE_2011 
                     and IMD.imd_year = '$IMD_year'
 LEFT JOIN $db_output.mhb_org_daily OD on MPI.OrgIDProv = OD.ORG_CODE
 WHERE MPI.UniqMonthID between '$start_month_id' AND '$end_month_id'

# COMMAND ----------

# DBTITLE 1,Get Referrals to IPS Team Types (ServTeamTypeRefToMH = 'D05')
 %sql
 DROP TABLE IF EXISTS $db_output.ips_referrals;
 CREATE TABLE         $db_output.ips_referrals AS
 SELECT 
     r.UniqServReqID 
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,'ServTeamTypeRefToMH' AS Identifier   
  
 FROM $db_output.NHSE_Pre_Proc_Referral r
 WHERE r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND  r.UniqMonthID BETWEEN '$start_month_id' AND '$end_month_id'
 AND r.ServTeamTypeRefToMH = 'D05' 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth ='')

# COMMAND ----------

# DBTITLE 1,Get SNOMED Interventions for IPS (All Providers)
 %sql
 DROP TABLE IF EXISTS $db_output.ips_interventions;
 CREATE TABLE         $db_output.ips_interventions AS
  
 SELECT
     a.UniqMonthID,
     a.OrgIDProv,
     a.RecordNumber, 
     a.UniqServReqID,
     a.Der_SNoMEDProcQual --ADDED IN TO TEST
  
 FROM $db_output.NHSE_Pre_Proc_Interventions a
 LEFT JOIN $db_output.NHSE_Pre_Proc_Activity p ON a.UniqMonthID = p.UniqMonthID and a.OrgIDProv = p.OrgIDProv AND a.RecordNumber = p.RecordNumber AND a.UniqCareContID = p.UniqCareContID
  
 WHERE (a.Der_SNoMEDProcCode IN ('1082621000000104', '772822000') OR regexp_replace(a.Der_SNoMEDProcCode, "\n|\r", "")=1082621000000104 or regexp_replace(a.Der_SNoMEDProcCode, "\n|\r", "")=772822000)
 AND ((a.Der_SNoMEDProcQual != '443390004' OR a.Der_SNoMEDProcQual IS NULL) OR regexp_replace(a.Der_SNoMEDProcQual, "\n|\r", "")!= 443390004)
 AND (a.Der_SNoMEDProcQual != 443390004 OR a.Der_SNoMEDProcQual IS NULL)--to make sure 443390004 is filtered out and not removing nulls, POSSIBLE use case statement to handle this?
 AND p.Der_DirectContact = 1 -- and only bring in direct contacts that are F2F, video, telephone or other, codes need to be linked to snomed code 
  
 GROUP BY a.UniqMonthID, a.OrgIDProv, a.RecordNumber, a.UniqServReqID, a.Der_SNoMEDProcQual

# COMMAND ----------

# DBTITLE 1,Insert Referral Records for IPS SNOMED activity (instead of ServTeamTypeRefToMH) into ips_referrals
 %sql 
 INSERT INTO $db_output.ips_referrals
 SELECT 
     r.UniqServReqID
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,'SNoMED' AS Identifier
     
 FROM $db_output.NHSE_Pre_Proc_Referral r
 INNER JOIN $db_output.ips_interventions a 
 ON a.UniqMonthID = r.UniqMonthID 
 AND a.OrgIDProv = r.OrgIDProv 
 AND a.RecordNumber = r.RecordNumber 
 AND a.UniqServReqID = r.UniqServReqID -- Select records for referrals that have an IPS SNOMED intervention recorded
  
 WHERE r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND  r.UniqMonthID BETWEEN '$start_month_id' AND '$end_month_id'
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth ='')

# COMMAND ----------

# DBTITLE 1,Get IPS Care Contacts for Referrals to IPS Team Types (ServTeamTypeRefToMH = 'D05')
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity;
 CREATE TABLE         $db_output.ips_activity AS
  
 SELECT
     r.UniqMonthID
     ,r.Person_ID
     ,r.RecordNumber
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,r.UniqServReqID
     ,r.Identifier
     ,c.UniqCareContID
     ,c.Der_ContactDate
     ,c.Der_FY
  
 FROM $db_output.ips_referrals r
  
 INNER JOIN $db_output.NHSE_Pre_Proc_Activity c ON r.RecordNumber = c.RecordNumber 
        AND r.UniqServReqID = c.UniqServReqID 
        AND c.Der_DirectContact=1
        AND  c.UniqMonthID BETWEEN '$start_month_id' AND '$end_month_id'
  
 WHERE r.Identifier = 'ServTeamTypeRefToMH' --bring through care contacts for IPS referrals identified via Team Type code 

# COMMAND ----------

# DBTITLE 1,Insert IPS Care Contacts Referral Records for IPS SNOMED activity (instead of ServTeamTypeRefToMH) into ips_activity
 %sql
 INSERT INTO $db_output.ips_activity
  
 SELECT
      r.UniqMonthID
     ,r.Person_ID
     ,r.RecordNumber
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
     ,r.UniqServReqID
     ,r.Identifier
     ,c.UniqCareContID
     ,c.Der_ContactDate
     ,c.Der_FY
  
  FROM $db_output.ips_referrals r
  
  INNER JOIN $db_output.NHSE_Pre_Proc_Activity c ON r.RecordNumber = c.RecordNumber 
     AND r.UniqServReqID = c.UniqServReqID 
     AND r.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id' 
   
  INNER JOIN $db_output.NHSE_Pre_Proc_Interventions i ON r.RecordNumber = i.RecordNumber 
     AND r.UniqServReqID = i.UniqServReqID 
     AND c.UniqCareContID = i.UniqCareContID
  
 WHERE (i.Der_SNoMEDProcCode IN ('1082621000000104', '772822000') OR regexp_replace(i.Der_SNoMEDProcCode, "\n|\r", "")=1082621000000104 or regexp_replace(i.Der_SNoMEDProcCode, "\n|\r", "")=772822000)
 AND ((i.Der_SNoMEDProcQual != '443390004' OR i.Der_SNoMEDProcQual IS NULL) OR regexp_replace(i.Der_SNoMEDProcQual, "\n|\r", "")!= 443390004)
 AND (i.Der_SNoMEDProcQual != 443390004 OR i.Der_SNoMEDProcQual IS NULL) --to make sure 443390004 is filtered out POSSIBLE use case statement to handle this?
 AND c.Der_DirectContact=1-- and only bring in direct contacts that are F2F, video, telephone or other, codes need to be linked to snomed code 
  AND r.Identifier = 'SNoMED' ---bring through contacts for IPS referrals identified via SNoMED codes

# COMMAND ----------

# DBTITLE 1,Select distinct IPS Referrals to get a single referral for referrals which flowed under ServTeamTypeRefToMH and SNOMED
 %sql
 DROP TABLE IF EXISTS $db_output.ips_referrals_distinct;
 CREATE TABLE         $db_output.ips_referrals_distinct AS
  
 SELECT DISTINCT
     r.UniqServReqID
     ,r.Person_ID
     ,r.UniqMonthID
     ,r.RecordNumber
     ,r.ReportingPeriodStartDate
     ,r.ReportingPeriodEndDate
     ,r.Der_FY
     ,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,r.OrgIDProv
     ,r.OrgIDCCGRes
 
 FROM $db_output.ips_referrals r

# COMMAND ----------

# DBTITLE 1,Select distinct IPS Care Contacts to get a single referral for referrals which flowed under ServTeamTypeRefToMH and SNOMED
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity_distinct;
 CREATE TABLE         $db_output.ips_activity_distinct AS
  
 SELECT DISTINCT
      a.UniqMonthID
     ,a.Person_ID
     ,a.RecordNumber
     ,a.OrgIDProv
     ,a.OrgIDCCGRes
     ,a.UniqServReqID
     ,a.UniqCareContID
     ,a.Der_ContactDate
     ,a.Der_FY
  
 FROM $db_output.ips_activity a

# COMMAND ----------

# DBTITLE 1,Partition Care Contacts to flag first contact per referral (ever and within financial year)
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity_order;
 CREATE TABLE $db_output.ips_activity_order AS 
  
 SELECT
     a.RecordNumber
     ,a.UniqMonthID
     ,a.Person_ID
     ,a.UniqServReqID
     ,ROW_NUMBER() OVER (PARTITION BY a.UniqServReqID ORDER BY a.Der_ContactDate ASC) AS AccessFlag 
     ,ROW_NUMBER() OVER (PARTITION BY a.UniqServReqID, a.Der_FY ORDER BY a.Der_ContactDate ASC) AS FYAccessFlag 
     ,a.Der_ContactDate
  
 FROM $db_output.ips_activity_distinct a

# COMMAND ----------

# DBTITLE 1,Aggregate by referral per month
 %sql
 DROP TABLE IF EXISTS $db_output.ips_activity_agg;
 CREATE TABLE $db_output.ips_activity_agg AS 
 SELECT
 	a.RecordNumber
 	,a.UniqMonthID 
 	,a.Person_ID
 	,a.UniqServReqID 
 	,MIN(a.AccessFlag) AS AccessFlag
 	,MIN(a.FYAccessFlag) AS FYAccessFlag
 	,MIN(a.Der_ContactDate) AS AccessDate
 	,COUNT(*) AS TotalContacts
 
 FROM $db_output.ips_activity_order a
 
 GROUP BY a.RecordNumber, a.UniqMonthID, a.Person_ID, a.UniqServReqID  

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

# DBTITLE 1,Final Prep Table for Counts
 %sql
 DROP TABLE IF EXISTS $db_output.ips_master;
 CREATE TABLE IF NOT EXISTS $db_output.ips_master USING DELTA AS
 
 SELECT 
 	r.UniqServReqID
     ,r.Person_ID
 	,r.UniqMonthID
 	,r.RecordNumber
 	,r.ReportingPeriodStartDate
 	,r.ReportingPeriodEndDate
 	,r.Der_FY   
 	,r.ReferralRequestReceivedDate
     ,r.ServDischDate
     ,COALESCE(m.age_group_lower_common, "Unknown") as age_group_lower_common
     ,CASE WHEN Der_Gender = "1" THEN '1'
           WHEN Der_Gender = "2" THEN '2'
           WHEN Der_Gender = "3" THEN '3'
           WHEN Der_Gender = "4" THEN '4'
           WHEN Der_Gender = "9" THEN '9'
           ELSE 'Unknown' END AS Der_Gender
     ,CASE WHEN Der_Gender = "1" THEN 'Male'
           WHEN Der_Gender = "2" THEN 'Female'
           WHEN Der_Gender = "3" THEN 'Non-binary'
           WHEN Der_Gender = "4" THEN 'Other (not listed)'
           WHEN Der_Gender = "9" THEN 'Indeterminate'
           WHEN Der_Gender ="" THEN 'Unknown'
           ELSE 'Unknown' END AS Der_Gender_Desc
     ,COALESCE(m.UpperEthnicity, "Unknown") as UpperEthnicity
     ,LowerEthnicityCode
     ,LowerEthnicityName
     ,COALESCE(m.IMD_Decile, "Unknown") as IMD_Decile
 	,r.OrgIDProv
     ,m.OrgIDProvName as Provider_Name
     ,get_provider_type_code(r.OrgIDProv) as ProvTypeCode
     ,get_provider_type_name(r.OrgIDProv) as ProvTypeName
 	,r.OrgIDCCGRes
     ,COALESCE(i.ccg_code,'Unknown') AS CCG_Code
     ,COALESCE(i.ccg_name,'Unknown') AS CCG_Name
     ,COALESCE(i.STP_CODE, 'Unknown') AS STP_Code
     ,COALESCE(i.STP_NAME, 'Unknown') AS STP_Name
     ,COALESCE(i.REGION_CODE, 'Unknown') AS Region_Code
     ,COALESCE(i.REGION_NAME, 'Unknown') AS Region_Name
 	,CASE WHEN a.AccessFlag = 1 THEN 1 ELSE 0 END AS AccessFlag 
 	,CASE WHEN a.FYAccessFlag = 1 THEN 1 ELSE 0 END AS FYAccessFlag
 	,a.AccessDate
     ,TotalContacts AS Contacts
 
 FROM $db_output.ips_referrals_distinct r 
 LEFT JOIN $db_output.ips_activity_agg a ON r.UniqServReqID = a.UniqServReqID AND r.RecordNumber = a.RecordNumber
 LEFT JOIN $db_output.stp_region_mapping i ON r.OrgIDCCGRes = i.ccg_code --- double check with AT  should it be OrgIDProv? 
 INNER JOIN $db_output.ips_mpi m ON a.Person_ID = m.Person_ID and a.RecordNumber = m.RecordNumber and a.AccessFlag = 1 ---joining on record number to get demographics submitted by provider at first contact

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.ips_master