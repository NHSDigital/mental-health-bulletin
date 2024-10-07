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

# DBTITLE 1,Creates python versions of the widgets
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status  = dbutils.widgets.get("status")
# populationyear = dbutils.widgets.get("populationyear")
# IMD_year = dbutils.widgets.get("IMD_year")

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS000Header
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS000Header;
 CREATE TABLE         $db_output.MHB_MHS000Header USING DELTA AS
 SELECT *
 FROM $db_source.MHS000Header
 WHERE UniqMonthID between $start_month_id and $end_month_id;

 -- SELECT * FROM $db_output.MHB_MHS000Header

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS001MPI
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS001MPI;
 CREATE TABLE         $db_output.MHB_MHS001MPI USING DELTA AS
 SELECT * FROM

 (

 SELECT LocalPatientId
       ,OrgIDLocalPatientId
       ,OrgIDResidenceResp
       ,OrgIDEduEstab
       ,NHSNumber
       ,NHSNumberStatus
       ,Gender
       ,GenderIDCode
       ,CASE WHEN GenderIDCode IN ('1','2','3','4') THEN GenderIDCode
             WHEN Gender IN ('1','2','9') THEN Gender
             ELSE 'Unknown' END AS Der_Gender
       ,MaritalStatus
       ,EthnicCategory
       ,LanguageCodePreferred
       ,RecordNumber
       ,MHS001UniqID
       ,OrgIDProv
       ,CASE WHEN UniqMonthID > 1467 THEN OrgIDSubICBLocResidence
             when OrgIDCCGRes in ('00C','00K','00M') THEN '16C'
             when OrgIDCCGRes in ('05F','05J','05T','06D') THEN '18C'
             when OrgIDCCGRes in ('06M','06V','06W','06Y','07J') THEN '26A'
             when OrgIDCCGRes in ('01C','01R','02D','02F') THEN '27D'
             when OrgIDCCGRes in ('02N','02W','02R') THEN '36J'
             when OrgIDCCGRes in ('07V','08J','08R','08P','08T','08X') THEN '36L'
             when OrgIDCCGRes in ('03D','03E','03M') THEN '42D'
             when OrgIDCCGRes in ('04E','04H','04K','04L','04M','04N') THEN '52R'
             when OrgIDCCGRes in ('09G','09H','09X') THEN '70F'
             when OrgIDCCGRes in ('03T','04D','99D','04Q') THEN '71E'
             when OrgIDCCGRes in ('07N','07Q','08A','08K','08L','08Q') THEN '72Q'
             when OrgIDCCGRes in ('03V','04G') THEN '78H'
             when OrgIDCCGRes in ('00D','00J') THEN '84H'
             when OrgIDCCGRes in ('09C','09E','09J','09W','10A','10D','10E','99J') THEN '91Q'
             when OrgIDCCGRes in ('09L','09N','09Y','99H') THEN '92A'
             when OrgIDCCGRes in ('11E','12D','99N') THEN '92G'
             when OrgIDCCGRes in ('07M','07R','07X','08D','08H') THEN '93C'
             when OrgIDCCGRes in ('09F','09P','99K') THEN '97R'
             ELSE OrgIDCCGRes
             END as OrgIDCCGRes
       ,Person_ID
       ,UniqSubmissionID
       ,AgeRepPeriodStart
       ,AgeRepPeriodEnd
       ,AgeDeath
       ,PostcodeDistrict
       ,DefaultPostcode
       ,LSOA2011
       ,LADistrictAuth
       ,County
       ,ElectoralWard
       ,UniqMonthID
       ,NHSDEthnicity
       ,PatMRecInRP
       ,RecordStartDate
       ,RecordEndDate
       ,CCGGPRes
       ,IMDQuart
       ,PersDeathDate

 ,dense_rank() OVER (PARTITION BY Person_ID, OrgIDProv ORDER BY UniqMonthID DESC, RecordNumber DESC) AS RANK

 FROM $db_source.MHS001MPI

 WHERE UniqMonthID between $start_month_id and $end_month_id
 -- WHERE UniqMonthID between 1465 and 1476

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate'

 )

 WHERE RANK = '1';

 SELECT * FROM $db_output.MHB_MHS001MPI

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS002GP
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS002GP;
 CREATE TABLE         $db_output.MHB_MHS002GP USING DELTA AS
 SELECT LocalPatientId
       ,GMPCodeReg
       ,StartDateGMPRegistration
       ,EndDateGMPRegistration
       ,OrgIDGPPrac
       ,RecordNumber
       ,MHS002UniqID
       ,OrgIDProv
       ,Person_ID
       ,UniqSubmissionID
       ,CASE WHEN UniqMOnthID >= '1468' THEN OrgIDSubICBLocGP
             when OrgIDCCGGPPractice in ('00C','00K','00M') THEN '16C'
             when OrgIDCCGGPPractice in ('05F','05J','05T','06D') THEN '18C'
             when OrgIDCCGGPPractice in ('06M','06V','06W','06Y','07J') THEN '26A'
             when OrgIDCCGGPPractice in ('01C','01R','02D','02F') THEN '27D'
             when OrgIDCCGGPPractice in ('02N','02W','02R') THEN '36J'
             when OrgIDCCGGPPractice in ('07V','08J','08R','08P','08T','08X') THEN '36L'
             when OrgIDCCGGPPractice in ('03D','03E','03M') THEN '42D'
             when OrgIDCCGGPPractice in ('04E','04H','04K','04L','04M','04N') THEN '52R'
             when OrgIDCCGGPPractice in ('09G','09H','09X') THEN '70F'
             when OrgIDCCGGPPractice in ('03T','04D','99D','04Q') THEN '71E'
             when OrgIDCCGGPPractice in ('07N','07Q','08A','08K','08L','08Q') THEN '72Q'
             when OrgIDCCGGPPractice in ('03V','04G') THEN '78H'
             when OrgIDCCGGPPractice in ('00D','00J') THEN '84H'
             when OrgIDCCGGPPractice in ('09C','09E','09J','09W','10A','10D','10E','99J') THEN '91Q'
             when OrgIDCCGGPPractice in ('09L','09N','09Y','99H') THEN '92A'
             when OrgIDCCGGPPractice in ('11E','12D','99N') THEN '92G'
             when OrgIDCCGGPPractice in ('07M','07R','07X','08D','08H') THEN '93C'
             when OrgIDCCGGPPractice in ('09F','09P','99K') THEN '97R'
             ELSE OrgIDCCGGPPractice
             END AS OrgIDCCGGPPractice
       ,GPDistanceHome
       ,UniqMonthID
       ,RecordStartDate
       ,RecordEndDate
 FROM $db_source.MHS002GP
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

 SELECT * FROM $db_output.MHB_MHS002GP

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS101Referral
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS101Referral;
 CREATE TABLE         $db_output.MHB_MHS101Referral USING DELTA AS
 SELECT *
 FROM $db_source.MHS101Referral
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

 SELECT * FROM $db_output.MHB_MHS101Referral

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS102ServiceTypeReferredTo
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS102ServiceTypeReferredTo;
 CREATE TABLE         $db_output.MHB_MHS102ServiceTypeReferredTo USING DELTA AS
 SELECT *
 FROM $db_source.MHS102ServiceTypeReferredTo
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

 -- SELECT * FROM $db_output.MHB_MHS102ServiceTypeReferredTo

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS105OnwardReferral
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS105OnwardReferral;
 CREATE TABLE         $db_output.MHB_MHS105OnwardReferral USING DELTA AS
 SELECT *
 FROM $db_source.MHS105OnwardReferral
 WHERE UniqMonthID between $start_month_id and $end_month_id

 -- SELECT * FROM $db_output.MHB_MHS105OnwardReferral

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS201CareContact
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS201CareContact;
 CREATE TABLE         $db_output.MHB_MHS201CareContact USING DELTA AS
 SELECT *
 FROM $db_source.MHS201CareContact
 WHERE UniqMonthID between $start_month_id and $end_month_id;

 -- SELECT * FROM $db_output.MHB_MHS201CareContact

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS204IndirectActivity
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS204IndirectActivity;
 CREATE TABLE         $db_output.MHB_MHS204IndirectActivity USING DELTA AS
 SELECT *
 FROM $db_source.MHS204IndirectActivity
 WHERE UniqMonthID between $start_month_id and $end_month_id;

 -- SELECT * FROM $db_output.MHB_MHS204IndirectActivity

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS501HospProvSpell
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS501HospProvSpell;
 CREATE TABLE         $db_output.MHB_MHS501HospProvSpell USING DELTA AS
 SELECT *
 FROM $db_source.MHS501HospProvSpell
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

 -- SELECT * FROM $db_output.MHB_MHS501HospProvSpell

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS502WardStay
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS502WardStay;
 CREATE TABLE         $db_output.MHB_MHS502WardStay USING DELTA AS
 SELECT *
 FROM $db_source.MHS502WardStay
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

 -- SELECT * FROM $db_output.MHB_MHS502WardStay

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS505Restrictiveinterventinc
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS505RestrictiveinterventInc;
 CREATE TABLE         $db_output.MHB_MHS505RestrictiveinterventInc USING DELTA AS
 SELECT *
 FROM $db_source.mhs505restrictiveinterventinc
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS515RestrictiveInterventType
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS515RestrictiveInterventType;
 CREATE TABLE         $db_output.MHB_MHS515RestrictiveInterventType USING DELTA AS
 SELECT *
 FROM $db_source.mhs515restrictiveinterventtype
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS701CPACareEpisode
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS701CPACareEpisode;
 CREATE TABLE         $db_output.MHB_MHS701CPACareEpisode USING DELTA AS
 SELECT *
 FROM $db_source.MHS701CPACareEpisode
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

 -- SELECT * FROM $db_output.MHB_MHS701CPACareEpisode

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS801ClusterTool
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS801ClusterTool;
 CREATE TABLE         $db_output.MHB_MHS801ClusterTool USING DELTA AS
 SELECT *
 FROM $db_source.MHS801ClusterTool
 WHERE UniqMonthID between $start_month_id and $end_month_id;

 -- SELECT * FROM $db_output.MHB_MHS801ClusterTool

# COMMAND ----------

# DBTITLE 1,Create cut down version of MHS803CareCluster
 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS803CareCluster;
 CREATE TABLE         $db_output.MHB_MHS803CareCluster USING DELTA AS
 SELECT *
 FROM $db_source.MHS803CareCluster
 WHERE UniqMonthID between $start_month_id and $end_month_id

 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate';

 -- SELECT * FROM $db_output.MHB_MHS803CareCluster