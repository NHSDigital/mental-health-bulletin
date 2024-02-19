# Databricks notebook source
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

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

# DBTITLE 1,Dementia prep table - Used for 13a, 13b and 13c
 %sql
 
 --This prep table prodcues all the breakdowns needed to produce each output measure in Chapter 13
 
 
 DROP TABLE IF EXISTS $db_output.Dementia_prep;
 CREATE TABLE        $db_output.Dementia_prep USING DELTA AS 
 Select                 CC.UniqCareContID
                       ,CC.MHS201UniqID
                       ,Ref.OrgIDProv
                       ,o.Name as Provider_Name
                       ,get_provider_type_code(Ref.OrgIDProv) as ProvTypeCode
                       ,get_provider_type_name(Ref.OrgIDProv) as ProvTypeName
                       ,CC.AttendOrDNACode
                       ,CC.Person_ID
                       ,REF.UniqServReqID
                       ,CC.CareContDate
                       ,REF.ServDischDate
                       ,MPI.Der_Gender
                       ,case when Der_Gender = '1' then 'Male'
                             when Der_Gender = '2' then 'Female'
                             when Der_Gender = '3' then 'Non-binary'
                             when Der_Gender = '4' then 'Other (not listed)'
                             when Der_Gender = '9' then 'Indeterminate'
                             else 'Unknown' end as Der_Gender_Desc
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
                      when AgeRepPeriodEnd >= 90 then '90 or over' else 'Unknown' end as age_group_lower_common
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
                      when AgeRepPeriodEnd >= 85 then '85 or over' else 'Unknown' end as age_group_lower_imd
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
                            ELSE 'Unknown' END AS LowerEthnicityCode
                    ,CASE WHEN NHSDEthnicity = 'A' THEN 'British'
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
                     ELSE 'Unknown' END AS LowerEthnicityName
                     ,case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                           when mpi.LADistrictAuth = 'L99999999' then 'L99999999'
                           when mpi.LADistrictAuth = 'M99999999' then 'M99999999'
                           when mpi.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when mpi.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth
                     ,coalesce(la.level_description, 'Unknown') as LADistrictAuthName
                     ,IMD_Decile
                     ,IMD_Quintile
                     ,COALESCE(stp.CCG_CODE,'Unknown') AS CCG_CODE
                     ,COALESCE(stp.CCG_NAME, 'Unknown') as CCG_NAME
                     ,COALESCE(stp.STP_CODE, 'Unknown') as STP_CODE 
                     ,COALESCE(stp.STP_NAME, 'Unknown') as STP_NAME 
                     ,COALESCE(stp.REGION_CODE, 'Unknown') as REGION_CODE
                     ,COALESCE(stp.REGION_NAME, 'Unknown') as REGION_NAME
 FROM       $db_output.mpi as mpi -- Uses the generic prep MPi derivation
 LEFT JOIN  $db_output.MHB_MHS101referral as ref 
             on mpi.person_id = ref.person_id
 LEFT JOIN  $db_output.MHB_MHS102ServiceTypeReferredTo as serv
             on serv.UniqServReqID = ref.UniqServReqID --count of activity so join on UniqServReqID
 LEFT JOIN   $db_output.ccg_final as ccg
             on mpi.person_id = ccg.person_id
 LEFT JOIN   $db_output.la la on la.level = case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                                         when mpi.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when mpi.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when mpi.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when mpi.LADistrictAuth is not null then mpi.LADistrictAuth
                                         when mpi.LADistrictAuth = '' then 'Unknown'
                                         else 'Unknown' end 
 LEFT JOIN   $db_output.MHB_MHS201CareContact as cc
             on cc.UniqServReqID = serv.UniqServReqID
             AND CC.UniqCareProfTeamID = serv.UniqCareProfTeamID  --count of activity so join on UniqServReqID and UniqCareProfTeamID
             and (CareContDate >= '$rp_startdate' AND CareContDate <= '$rp_enddate') -- contact date within the reporting period
 LEFT JOIN   $db_output.STP_Region_mapping stp --STP breakdown may need changing for future years, currently a hard coded script
             on ccg.IC_Rec_CCG = stp.CCG_code
 LEFT JOIN $db_output.MHB_ORG_DAILY o ON ref.OrgIDProv = o.ORG_CODE
 WHERE       ServTeamTypeRefToMH = 'A17'     -- for for Memory Services/Clinic     

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Dementia_prep