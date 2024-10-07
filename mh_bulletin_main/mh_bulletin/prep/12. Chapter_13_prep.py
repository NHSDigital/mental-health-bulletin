# Databricks notebook source
# %sql
# create widget text end_month_id default "1452";
# create widget text start_month_id default "1441";
# create widget text db_source default "mh_pre_pseudo_d1";
# create widget text rp_enddate default "2021-03-31";
# create widget text rp_startdate default "2020-04-01";
# create widget text status default "Final";
# create widget text db_output default "_tootle1_100261";
# create widget text populationyear default 2020;
# create widget text IMD_year default 2019;

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
                       ,MPI.Der_Gender_Desc
                       ,MPI.age_group_higher_level
                       ,MPI.age_group_lower_common
                       ,MPI.age_group_lower_chap1
                       ,MPI.UpperEthnicity
                       ,MPI.LowerEthnicityCode
                       ,MPI.LowerEthnicityName
                       ,MPI.IMD_Decile
                       ,MPI.IMD_Quintile 
                       ,case when LEFT(mpi.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(mpi.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(mpi.LADistrictAuth,1) = 'W' then 'W'
                           when mpi.LADistrictAuth = 'L99999999' then 'L99999999'
                           when mpi.LADistrictAuth = 'M99999999' then 'M99999999'
                           when mpi.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'UNKNOWN'
                           when mpi.LADistrictAuth = '' then 'UNKNOWN'
                           else la.level end as LADistrictAuth
                       ,coalesce(la.level_description, 'UNKNOWN') as LADistrictAuthName
                       ,COALESCE(stp.CCG_CODE,'UNKNOWN') AS CCG_CODE
                       ,COALESCE(stp.CCG_NAME, 'UNKNOWN') as CCG_NAME
                       ,COALESCE(stp.STP_CODE, 'UNKNOWN') as STP_CODE 
                       ,COALESCE(stp.STP_NAME, 'UNKNOWN') as STP_NAME 
                       ,COALESCE(stp.REGION_CODE, 'UNKNOWN') as REGION_CODE
                       ,COALESCE(stp.REGION_NAME, 'UNKNOWN') as REGION_NAME
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
                                         when mpi.LADistrictAuth = '' then 'UNKNOWN'
                                         else 'UNKNOWN' end 
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