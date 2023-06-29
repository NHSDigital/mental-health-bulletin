# Databricks notebook source
 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS000Header;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS000Header USING DELTA AS
 ---get all mhs000header data for the financial year
 SELECT *
 FROM $db_source.MHS000Header
 WHERE UniqMonthID between $month_id_start and $month_id_end ---mhs000header records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS001MPI;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS001MPI USING DELTA AS
 ---get all mhs001mpi data for the financial year
 SELECT * FROM
 (
 SELECT 
 LocalPatientId
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
 ,CASE 
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
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---mhs001mpi records in the financial year only
 )
 WHERE RANK = '1'

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS002GP;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS002GP USING DELTA AS
 ---get all mhs002gp data for the financial year
 SELECT 
 LocalPatientId
 ,GMPCodeReg
 ,StartDateGMPRegistration
 ,EndDateGMPRegistration
 ,OrgIDGPPrac
 ,RecordNumber
 ,MHS002UniqID
 ,OrgIDProv
 ,Person_ID
 ,UniqSubmissionID
 ,case 
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
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---mhs002gp records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS101Referral;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS101Referral USING DELTA AS
 ---get all mhs101referral data for the financial year
 SELECT *
 FROM $db_source.MHS101Referral
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---mhs101referral records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS102ServiceTypeReferredTo;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS102ServiceTypeReferredTo USING DELTA AS
 ---get all mhs102servicetypereferredto data for the financial year
 SELECT *
 FROM $db_source.MHS102ServiceTypeReferredTo
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---mhs102servicetypereferredto records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS105OnwardReferral;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS105OnwardReferral USING DELTA AS
 ---get all mhs105onwardreferral data for the financial year
 SELECT *
 FROM $db_source.MHS105OnwardReferral
 WHERE UniqMonthID between $month_id_start and $month_id_end ---mhs105onwardreferral records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS201CareContact;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS201CareContact USING DELTA AS
 ---get all mhs201carecontact data for the financial year
 SELECT *
 FROM $db_source.MHS201CareContact
 WHERE UniqMonthID between $month_id_start and $month_id_end ---mhs201carecontact records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS204IndirectActivity;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS204IndirectActivity USING DELTA AS
 ---get all mhs204indirectactivity data for the financial year
 SELECT *
 FROM $db_source.MHS204IndirectActivity
 WHERE UniqMonthID between $month_id_start and $month_id_end ---mhs204indirectactivity records in the financial year only

# COMMAND ----------

 %sql
  
 DROP TABLE IF EXISTS $db_output.MHB_MHS501HospProvSpell;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS501HospProvSpell USING DELTA AS
 ---get all mhs501hospprovspell data for the financial year
 SELECT *
 FROM $db_source.MHS501HospProvSpell
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---get all mhs501hospprovspell records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS502WardStay;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS502WardStay USING DELTA AS
 ---get all mhs502wardstay data for the financial year
 SELECT *
 FROM $db_source.MHS502WardStay
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---get all mhs502wardstay records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS505RestrictiveIntervention;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS505RestrictiveIntervention USING DELTA AS
 ---get all mhs505restrictiveinterventio data for the financial year (this code is only valid for the 21/22 financial year due to v4.1/v5 mid-year version change)
 SELECT  
 a.person_id,
 a.orgidprov,
 a.uniqmonthid,
 a.uniqsubmissionid,
 a.RestrictiveIntType as restrictiveinttype,
 a.mhs505uniqid,
 a.mhs505uniqid as restrictiveintincid,
 a.startdaterestrictiveint,
 a.enddaterestrictiveint,
 a.starttimerestrictiveint,
 a.endtimerestrictiveint, 
 a.uniqhospprovspellnum as uniqhospprovspellid, ---uniqhospprovspellnum in v4.1 changed to uniqhospprovspellid in v5
 CASE WHEN StartDateRestrictiveInt < '$rp_startdate' THEN '$rp_startdate' ELSE StartDateRestrictiveInt END AS Der_RestIntStartDate, ---if start of ri before financial year then rp_startdate
 CASE WHEN EndDateRestrictiveInt is null then date_add('$rp_enddate', 1) ELSE EndDateRestrictiveInt END AS Der_RestIntEndDate, ---if end of ri null or after financial year then rp_enddate plus 1 day
 CASE WHEN starttimerestrictiveint is null then to_timestamp('1970-01-01') else starttimerestrictiveint end as Der_RestIntStartTime, ---if time null then set time as 00:00:00
 CASE WHEN endtimerestrictiveint is null then to_timestamp('1970-01-01') else endtimerestrictiveint end as Der_RestIntEndTime, ---if time null then set time as 00:00:00
 case 
 when startdaterestrictiveint < '$rp_startdate' 
 or enddaterestrictiveint > '$rp_enddate' 
 or startdaterestrictiveint is null 
 or starttimerestrictiveint is null 
 or enddaterestrictiveint is null 
 or endtimerestrictiveint is null
 then 'N'
 else 'Y' end as pre_flag,
 a.recordnumber
 FROM $db_source.MHS505RestrictiveIntervention_pre_v5 as a
 WHERE UniqMonthID between $month_id_start and 1458 ---this is getting pre-v5 restrictive intervention data
 UNION ALL
 SELECT  
 a.person_id,
 a.orgidprov,
 a.uniqmonthid,
 a.uniqsubmissionid,
 a.restrictiveinttype,
 a.mhs515uniqid,
 a.UniqRestrictiveIntIncID,
 a.startdaterestrictiveinttype,
 a.enddaterestrictiveinttype,
 a.starttimerestrictiveinttype,
 a.endtimerestrictiveinttype,
 a.uniqhospprovspellid,
 CASE WHEN StartDateRestrictiveIntType < '$rp_startdate' THEN '$rp_startdate' ELSE StartDateRestrictiveIntType END AS Der_RestIntStartDate, ---if start of ri before financial year then rp_startdate
 CASE WHEN EndDateRestrictiveIntType is null then date_add('$rp_enddate', 1) ELSE EndDateRestrictiveIntType END AS Der_RestIntEndDate, ---if end of ri null or after financial year then rp_enddate plus 1 day
 CASE WHEN starttimerestrictiveintType is null then to_timestamp('1970-01-01') else starttimerestrictiveintType end as Der_RestIntStartTime, ---if time null then set time as 00:00:00
 CASE WHEN endtimerestrictiveintType is null then to_timestamp('1970-01-01') else endtimerestrictiveintType end as Der_RestIntEndTime, ---if time null then set time as 00:00:00
 case 
 when startdaterestrictiveintType < '$rp_startdate' 
 or enddaterestrictiveintType > '$rp_enddate' 
 or startdaterestrictiveintType is null 
 or starttimerestrictiveintType is null 
 or enddaterestrictiveintType is null 
 or endtimerestrictiveintType is null
 then 'N'
 else 'Y' end as pre_flag,
 a.recordnumber
 FROM $db_source.MHS515RestrictiveInterventType as a
 WHERE UniqMonthID between $month_id_start and $month_id_end ---this is getting post-v5 restrictive intervention data

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS701CPACareEpisode;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS701CPACareEpisode USING DELTA AS
 ---get all mhs701cpacareepisode data for the financial year
 SELECT *
 FROM $db_source.MHS701CPACareEpisode
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---mhs701cpacareepisode records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS801ClusterTool;
 CREATE TABLE IF NOT EXISTS $db_output.MHB_MHS801ClusterTool USING DELTA AS
 ---get all mhs801clustertool data for the financial year
 SELECT *
 FROM $db_source.MHS801ClusterTool
 WHERE UniqMonthID between $month_id_start and $month_id_end ---mhs801clustertool records in the financial year only

# COMMAND ----------

 %sql 
 DROP TABLE IF EXISTS $db_output.MHB_MHS803CareCluster;
 CREATE TABLE         $db_output.MHB_MHS803CareCluster USING DELTA AS
 ---get all mhs803carecluster data for the financial year
 SELECT *
 FROM $db_source.MHS803CareCluster
 WHERE UniqMonthID between $month_id_start and $month_id_end
 AND (RecordEndDate is null or RecordEndDate >= '$rp_enddate') AND RecordStartDate BETWEEN '$rp_startdate' AND '$rp_enddate' ---mhs803carecluster records in the financial year only

# COMMAND ----------

 %sql
 ---get all valid/open healthcare provider records for the financial year
 DROP TABLE IF EXISTS $db_output.mhb_org_daily;
 CREATE TABLE         $db_output.mhb_org_daily USING DELTA AS
 SELECT *
 FROM [DATABASE].[ORG_REF]
 WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
   AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
   AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN'); ---not including MP constituency, social service inspectorate region, family practitioner committee, government office region or cancer network
   
 OPTIMIZE $db_output.mhb_org_daily;

# COMMAND ----------

 %sql
 ---gets all valid CCGs between the startdate and enddate widgets
 TRUNCATE TABLE $db_output.mhb_rd_ccg_latest;
 INSERT INTO $db_output.mhb_rd_ccg_latest USING DELTA AS
 SELECT               ORG_CODE,
                      NAME
 FROM                 $db_output.mhb_org_daily
 WHERE                (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
                      AND ORG_TYPE_CODE = 'CC' ---commissioning groups only
                      AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
                      AND ORG_OPEN_DATE <= '$rp_enddate'
                      AND NAME NOT LIKE '%HUB' ---exclude commissioning hubs
                      AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW ORG_DAILY AS
 ---get latest organisation reference data as of the end of the financial year. Used further down to get CCG/SubICB/STP/ICB/Commissioning region reference data
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM [DATABASE].[ORG_REF]
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW ORG_RELATIONSHIP_DAILY AS 
 ---get latest organisation reference relationship data as of the end of the financial year. Used further down to get CCG/SubICB/STP/ICB/Commissioning Region reference data
 ---i.e CCG/SubICB maps to STP/ICB to Region
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 [DATABASE].[ORG_RELATIONSHIP_REF]
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.stp_region_mapping;
 INSERT INTO $db_output.stp_region_mapping 
 ---get reference data for each CCG/SubICB/STP/ICB/Commissioning Region relationship as of the end of the financial year from CCG/SubICB upwards
 SELECT 
 C.ORG_CODE as CCG_Code,
 A.ORG_CODE as STP_Code, 
 A.NAME as STP_Name, 
 C.NAME as CCG_Name, 
 E.ORG_CODE as Region_Code,
 E.NAME as Region_Name
 FROM ORG_DAILY A
 LEFT JOIN ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST' ---CCG/SubICB relationships only
 LEFT JOIN ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE' ---STP/ICB relationships only
 LEFT JOIN ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 A.ORG_TYPE_CODE = 'ST'---get commissioning related organisations only i.e CCG/SubICB/STP/ICB/Commissioning Region
 AND B.REL_TYPE_CODE is not null
 ORDER BY C.ORG_COD

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhb_la;
 CREATE TABLE IF NOT EXISTS $db_output.mhb_la USING DELTA AS
 ---get all local authority district codes and names for the financial year
 SELECT		DISTINCT 'LADUA' as type
             ,LADCD as level
             ,LADNM as level_description
 FROM		[DATABASE].[LA_ORG_REF]
 WHERE		DSS_ONS_PUBLISHED_DATE = '$LAD_published_date'
             AND DSS_RECORD_END_DATE IS NULL
             AND LADCD NOT  LIKE 'M%' AND LADCD NOT LIKE  'X%' AND LADCD NOT  in ('N%', 'W%', 'S%') --Excluding Northern Ireland, Wales, Scotland LAs

# COMMAND ----------

 %sql
 ---hard-coded ccg mapping 2021 values
 TRUNCATE TABLE $db_output.ccg_mapping_2021
 INSERT INTO $db_output.ccg_mapping_2021
 VALUES 
 ('00C','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00D','84H','E38000234','NHS COUNTY DURHAM CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00J','84H','E38000234','NHS COUNTY DURHAM CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00K','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00L','00L','E38000130','NHS NORTHUMBERLAND CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00M','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00N','00N','E38000163','NHS SOUTH TYNESIDE CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00P','00P','E38000176','NHS SUNDERLAND CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('00Q','00Q','E38000014','NHS BLACKBURN WITH DARWEN CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('00R','00R','E38000015','NHS BLACKPOOL CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('00T','00T','E38000016','NHS BOLTON CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('00V','00V','E38000024','NHS BURY CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('00X','00X','E38000034','NHS CHORLEY AND SOUTH RIBBLE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('00Y','00Y','E38000135','NHS OLDHAM CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('01A','01A','E38000050','NHS EAST LANCASHIRE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('01C','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('01D','01D','E38000080','NHS HEYWOOD, MIDDLETON AND ROCHDALE CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('01E','01E','E38000227','NHS GREATER PRESTON CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('01F','01F','E38000068','NHS HALTON CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('01G','01G','E38000143','NHS SALFORD CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('01H','01H','E38000215','NHS NORTH CUMBRIA CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('01J','01J','E38000091','NHS KNOWSLEY CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('01K','01K','E38000228','NHS MORECAMBE BAY CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('01R','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('01T','01T','E38000161','NHS SOUTH SEFTON CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('01V','01V','E38000170','NHS SOUTHPORT AND FORMBY CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('01W','01W','E38000174','NHS STOCKPORT CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('01X','01X','E38000172','NHS ST HELENS CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('01Y','01Y','E38000182','NHS TAMESIDE AND GLOSSOP CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('02A','02A','E38000187','NHS TRAFFORD CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('02D','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('02E','02E','E38000194','NHS WARRINGTON CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('02F','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('02G','02G','E38000200','NHS WEST LANCASHIRE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('02H','02H','E38000205','NHS WIGAN BOROUGH CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('02M','02M','E38000226','NHS FYLDE AND WYRE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
 ('02N','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('02P','02P','E38000006','NHS BARNSLEY CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
 ('02Q','02Q','E38000008','NHS BASSETLAW CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
 ('02R','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('02T','02T','E38000025','NHS CALDERDALE CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('02W','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('02X','02X','E38000044','NHS DONCASTER CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
 ('02Y','02Y','E38000052','NHS EAST RIDING OF YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03A','X2C4Y','E38000254','NHS KIRKLEES CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('03D','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03E','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03F','03F','E38000085','NHS HULL CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03H','03H','E38000119','NHS NORTH EAST LINCOLNSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03J','X2C4Y','E38000254','NHS KIRKLEES CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('03K','03K','E38000122','NHS NORTH LINCOLNSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03L','03L','E38000141','NHS ROTHERHAM CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
 ('03M','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03N','03N','E38000146','NHS SHEFFIELD CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
 ('03Q','03Q','E38000188','NHS VALE OF YORK CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('03R','03R','E38000190','NHS WAKEFIELD CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('03T','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
 ('03V','78H','E38000242','NHS NORTHAMPTONSHIRE CCG','E54000020','QPM','Northamptonshire','E40000008','Y60','Midlands'),
 ('03W','03W','E38000051','NHS EAST LEICESTERSHIRE AND RUTLAND CCG','E54000015','QK1','Leicester, Leicestershire and Rutland','E40000008','Y60','Midlands'),
 ('03X','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
 ('03Y','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
 ('04C','04C','E38000097','NHS LEICESTER CITY CCG','E54000015','QK1','Leicester, Leicestershire and Rutland','E40000008','Y60','Midlands'),
 ('04D','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
 ('04E','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
 ('04F','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
 ('04G','78H','E38000242','NHS NORTHAMPTONSHIRE CCG','E54000020','QPM','Northamptonshire','E40000008','Y60','Midlands'),
 ('04H','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
 ('04J','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
 ('04K','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
 ('04L','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
 ('04M','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
 ('04N','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
 ('04Q','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
 ('04R','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
 ('04V','04V','E38000201','NHS WEST LEICESTERSHIRE CCG','E54000015','QK1','Leicester, Leicestershire and Rutland','E40000008','Y60','Midlands'),
 ('04Y','04Y','E38000028','NHS CANNOCK CHASE CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
 ('05A','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
 ('05C','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
 ('05D','05D','E38000053','NHS EAST STAFFORDSHIRE CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
 ('05F','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
 ('05G','05G','E38000126','NHS NORTH STAFFORDSHIRE CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
 ('05H','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
 ('05J','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
 ('05L','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
 ('05N','M2L0M','E38000257','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','E54000011','QOC','Shropshire and Telford and Wrekin','E40000008','Y60','Midlands'),
 ('05Q','05Q','E38000153','NHS SOUTH EAST STAFFORDSHIRE AND SEISDON PENINSULA CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
 ('05R','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
 ('05T','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
 ('05V','05V','E38000173','NHS STAFFORD AND SURROUNDS CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
 ('05W','05W','E38000175','NHS STOKE ON TRENT CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
 ('05X','M2L0M','E38000257','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','E54000011','QOC','Shropshire and Telford and Wrekin','E40000008','Y60','Midlands'),
 ('05Y','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
 ('06A','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
 ('06D','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
 ('06F','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
 ('06H','06H','E38000026','NHS CAMBRIDGESHIRE AND PETERBOROUGH CCG','E54000021','QUE','Cambridgeshire and Peterborough','E40000007','Y61','East of England'),
 ('06K','06K','E38000049','NHS EAST AND NORTH HERTFORDSHIRE CCG','E54000025','QM7','Hertfordshire and West Essex','E40000007','Y61','East of England'),
 ('06L','06L','E38000086','NHS IPSWICH AND EAST SUFFOLK CCG','E54000023','QJG','Suffolk and North East Essex','E40000007','Y61','East of England'),
 ('06M','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
 ('06N','06N','E38000079','NHS HERTS VALLEY CCG','E54000025','QM7','Hertfordshire and West Essex','E40000007','Y61','East of England'),
 ('06P','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
 ('06Q','06Q','E38000106','NHS MID ESSEX CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
 ('06T','06T','E38000117','NHS NORTH EAST ESSEX CCG','E54000023','QJG','Suffolk and North East Essex','E40000007','Y61','East of England'),
 ('06V','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
 ('06W','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
 ('06Y','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
 ('07G','07G','E38000185','NHS THURROCK CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
 ('07H','07H','E38000197','NHS WEST ESSEX CCG','E54000025','QM7','Hertfordshire and West Essex','E40000007','Y61','East of England'),
 ('07J','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
 ('07K','07K','E38000204','NHS WEST SUFFOLK CCG','E54000023','QJG','Suffolk and North East Essex','E40000007','Y61','East of England'),
 ('07L','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('07M','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
 ('07N','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
 ('07P','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('07Q','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
 ('07R','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
 ('07T','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('07V','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
 ('07W','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('07X','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
 ('07Y','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('08A','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
 ('08C','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('08D','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
 ('08E','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('08F','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('08G','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('08H','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
 ('08J','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
 ('08K','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
 ('08L','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
 ('08M','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('08N','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('08P','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
 ('08Q','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
 ('08R','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
 ('08T','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
 ('08V','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('08W','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('08X','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
 ('08Y','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('09A','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('09C','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('09D','09D','E38000021','NHS BRIGHTON AND HOVE CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('09E','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('09F','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('09G','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('09H','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('09J','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('09L','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
 ('09N','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
 ('09P','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('09W','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('09X','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('09Y','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
 ('10A','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('10C','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
 ('10D','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('10E','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('10J','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('10K','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('10L','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('10Q','10Q','E38000136','NHS OXFORDSHIRE CCG','E54000044','QU9','Buckinghamshire, Oxfordshire and Berkshire West','E40000005','Y59','South East'),
 ('10R','10R','E38000137','NHS PORTSMOUTH CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('10V','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('10X','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('11A','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('11E','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
 ('11J','11J','E38000045','NHS DORSET CCG','E54000041','QVV','Dorset','E40000006','Y58','South West'),
 ('11M','11M','E38000062','NHS GLOUCESTERSHIRE CCG','E54000043','QR1','Gloucestershire','E40000006','Y58','South West'),
 ('11N','11N','E38000089','NHS KERNOW CCG','E54000036','QT6','Cornwall and the Isles of Scilly Health and Social Care Partnership','E40000006','Y58','South West'),
 ('11X','11X','E38000150','NHS SOMERSET CCG','E54000038','QSL','Somerset','E40000006','Y58','South West'),
 ('12D','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
 ('12F','12F','E38000208','NHS WIRRAL CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('13T','13T','E38000212','NHS NEWCASTLE GATESHEAD CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('14L','14L','E38000217','NHS MANCHESTER CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
 ('14Y','14Y','E38000223','NHS BUCKINGHAMSHIRE CCG','E54000044','QU9','Buckinghamshire, Oxfordshire and Berkshire West','E40000005','Y59','South East'),
 ('15A','15A','E38000221','NHS BERKSHIRE WEST CCG','E54000044','QU9','Buckinghamshire, Oxfordshire and Berkshire West','E40000005','Y59','South East'),
 ('15C','15C','E38000222','NHS BRISTOL, NORTH SOMERSET AND SOUTH GLOUCESTERSHIRE CCG','E54000039','QUY','Bristol, North Somerset and South Gloucestershire','E40000006','Y58','South West'),
 ('15D','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
 ('15E','15E','E38000220','NHS BIRMINGHAM AND SOLIHULL CCG','E54000017','QHL','Birmingham and Solihull','E40000008','Y60','Midlands'),
 ('15F','15F','E38000225','NHS LEEDS CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('15M','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
 ('15N','15N','E38000230','NHS DEVON CCG','E54000037','QJK','Devon','E40000006','Y58','South West'),
 ('16C','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('18C','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
 ('26A','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
 ('27D','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('36J','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
 ('36L','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
 ('42D','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
 ('52R','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
 ('70F','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('71E','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
 ('72Q','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
 ('78H','78H','E38000242','NHS NORTHAMPTONSHIRE CCG','E54000020','QPM','Northamptonshire','E40000008','Y60','Midlands'),
 ('84H','84H','E38000234','NHS COUNTY DURHAM CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('91Q','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('92A','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
 ('92G','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
 ('93C','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
 ('97R','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('99A','99A','E38000101','NHS LIVERPOOL CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
 ('99C','99C','E38000127','NHS NORTH TYNESIDE CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
 ('99D','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
 ('99E','99E','E38000007','NHS BASILDON AND BRENTWOOD CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
 ('99F','99F','E38000030','NHS CASTLE POINT AND ROCHFORD CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
 ('99G','99G','E38000168','NHS SOUTHEND CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
 ('99H','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
 ('99J','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
 ('99K','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
 ('99M','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
 ('99N','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
 ('99P','15N','E38000230','NHS DEVON CCG','E54000037','QJK','Devon','E40000006','Y58','South West'),
 ('99Q','15N','E38000230','NHS DEVON CCG','E54000037','QJK','Devon','E40000006','Y58','South West'),
 ('A3A8R','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
 ('B2M3M','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
 ('D2P2L','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
 ('D4U1Y','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
 ('D9Y0V','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
 ('M1J4Y','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
 ('M2L0M','M2L0M','E38000257','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','E54000011','QOC','Shropshire and Telford and Wrekin','E40000008','Y60','Midlands'),
 ('Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown'),
 ('W2U3Z','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
 ('X2C4Y','X2C4Y','E38000254','NHS KIRKLEES CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.mhb_los_ccg_pop;
 INSERT INTO $db_output.mhb_los_ccg_pop
 VALUES
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS Brent CCG','07P','329771','210996','41088','77687'),
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS Central London (Westminster) CCG','09A','190034','130417','25247','34370'),
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS Ealing CCG','07W','341806','214788','44846','82172'),
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS Hammersmith and Fulham CCG','08C','185143','127742','20369','37032'),
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS Harrow CCG','08E','251160','151797','39988','59375'),
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS Hillingdon CCG','08G','306870','191398','41395','74077'),
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS Hounslow CCG','07Y','271523','172860','33431','65232'),
 ('W2U3Z','NHS NORTH WEST LONDON CCG','NHS West London CCG','08Y','227412','151638','32430','43344'),
 ('93C','NHS NORTH CENTRAL LONDON CCG','NHS North Central London CCG','93C','1510806','994339','183808','332659'),
 ('A3A8R','NHS NORTH EAST LONDON CCG','NHS Barking and Dagenham CCG','07L','212906','129579','19780','63547'),
 ('A3A8R','NHS NORTH EAST LONDON CCG','NHS City and Hackney CCG','07T','290841','202020','23334','65487'),
 ('A3A8R','NHS NORTH EAST LONDON CCG','NHS Havering CCG','08F','259552','154482','46709','58361'),
 ('A3A8R','NHS NORTH EAST LONDON CCG','NHS Newham CCG','08M','353134','239838','27228','86068'),
 ('A3A8R','NHS NORTH EAST LONDON CCG','NHS Redbridge CCG','08N','305222','190123','38852','76247'),
 ('A3A8R','NHS NORTH EAST LONDON CCG','NHS Tower Hamlets CCG','08V','324745','231596','20859','72290'),
 ('A3A8R','NHS NORTH EAST LONDON CCG','NHS Waltham Forest CCG','08W','276983','180246','29980','66757'),
 ('72Q','NHS SOUTH EAST LONDON CCG','NHS South East London CCG','72Q','1819271','1209085','213122','397064'),
 ('36L','NHS SOUTH WEST LONDON CCG','NHS South West London CCG','36L','1504810','965103','199049','340658'),
 ('91Q','NHS KENT AND MEDWAY CCG','NHS Kent and Medway CCG','91Q','1860111','1087038','364255','408818'),
 ('D4U1Y','NHS FRIMLEY CCG','NHS North East Hampshire and Farnham CCG','99M','211709','126495','37972','47242'),
 ('D4U1Y','NHS FRIMLEY CCG','NHS Surrey Heath CCG','10C','96564','57157','18923','20484'),
 ('D4U1Y','NHS FRIMLEY CCG','NHS East Berkshire CCG','15D','436701','265724','63070','107907'),
 ('D9Y0V','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','NHS Fareham and Gosport CCG','10K','201071','116799','44857','39415'),
 ('D9Y0V','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','NHS Isle of Wight CCG','10L','141771','76847','40186','24738'),
 ('D9Y0V','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','NHS North Hampshire CCG','10J','225387','134214','41855','49318'),
 ('10R','NHS PORTSMOUTH CCG','NHS Portsmouth CCG','10R','214905','140716','30433','43756'),
 ('D9Y0V','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','NHS South Eastern Hampshire CCG','10V','217701','123041','50867','43793'),
 ('D9Y0V','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','NHS Southampton CCG','10X','252520','167372','33873','51275'),
 ('D9Y0V','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','NHS West Hampshire CCG','11A','570799','321887','134379','114533'),
 ('10Q','NHS OXFORDSHIRE CCG','NHS Oxfordshire CCG','10Q','676171','408515','124997','142659'),
 ('15A','NHS BERKSHIRE WEST CCG','NHS Berkshire West CCG','15A','491349','297343','80985','113021'),
 ('14Y','NHS BUCKINGHAMSHIRE CCG','NHS Buckinghamshire CCG','14Y','546626','316827','103487','126312'),
 ('92A','NHS SURREY HEARTLANDS CCG','NHS Surrey Heartlands CCG','92A','1049170','616219','199665','233286'),
 ('09D','NHS BRIGHTON AND HOVE CCG','NHS Brighton and Hove CCG','09D','290885','201779','38839','50267'),
 ('97R','NHS EAST SUSSEX CCG','NHS East Sussex CCG','97R','557229','306299','144592','106338'),
 ('70F','NHS WEST SUSSEX CCG','NHS West Sussex CCG','70F','857166','485425','197297','174444'),
 ('11N','NHS KERNOW CCG','NHS Kernow CCG','11N','571802','319904','143046','108852'),
 ('15N','NHS DEVON CCG','NHS Devon CCG','15N','1200739','686065','289343','225331'),
 ('11X','NHS SOMERSET CCG','NHS Somerset CCG','11X','562225','311122','139913','111190'),
 ('15C','NHS BRISTOL, NORTH SOMERSET AND SOUTH GLOUCESTERSHIRE CCG','NHS Bristol, North Somerset and South Gloucestershire CCG','15C','963522','600749','165683','197090'),
 ('92G','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','NHS Bath and North East Somerset, Swindon and Wiltshire CCG','92G','921917','545340','182340','194237'),
 ('11J','NHS DORSET CCG','NHS Dorset CCG','11J','773839','434057','196346','143436'),
 ('11M','NHS GLOUCESTERSHIRE CCG','NHS Gloucestershire CCG','11M','637070','370650','137510','128910'),
 ('06H','NHS CAMBRIDGESHIRE AND PETERBOROUGH CCG','NHS Cambridgeshire and Peterborough CCG','06H','892627','532752','163568','196307'),
 ('26A','NHS NORFOLK AND WAVENEY CCG','NHS Norfolk and Waveney CCG','26A','1026193','576905','255016','194272'),
 ('06L','NHS IPSWICH AND EAST SUFFOLK CCG','NHS Ipswich and East Suffolk CCG','06L','411211','233284','95010','82917'),
 ('06T','NHS NORTH EAST ESSEX CCG','NHS North East Essex CCG','06T','341267','196661','77320','67286'),
 ('07K','NHS WEST SUFFOLK CCG','NHS West Suffolk CCG','07K','231706','131615','52033','48058'),
 ('M1J4Y','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','NHS Bedfordshire CCG','06F','461940','274888','82698','104354'),
 ('M1J4Y','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','NHS Luton CCG','06P','213052','128624','26941','57487'),
 ('M1J4Y','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','NHS Milton Keynes CCG','04F','275882','166128','39632','70122'),
 ('06K','NHS EAST AND NORTH HERTFORDSHIRE CCG','NHS East and North Hertfordshire CCG','06K','571713','346819','98770','126124'),
 ('06N','NHS HERTS VALLEY CCG','NHS Herts Valleys CCG','06N','598034','354430','101400','142204'),
 ('07H','NHS WEST ESSEX CCG','NHS West Essex CCG','07H','310040','182878','57270','69892'),
 ('99E','NHS BASILDON AND BRENTWOOD CCG','NHS Basildon and Brentwood CCG','99E','264220','156315','47925','59980'),
 ('99F','NHS CASTLE POINT AND ROCHFORD CCG','NHS Castle Point and Rochford CCG','99F','177744','100225','43197','34322'),
 ('06Q','NHS MID ESSEX CCG','NHS Mid Essex CCG','06Q','395918','231380','81855','82683'),
 ('99G','NHS SOUTHEND CCG','NHS Southend CCG','99G','183125','107762','35625','39738'),
 ('07G','NHS THURROCK CCG','NHS Thurrock CCG','07G','174341','105628','24063','44650'),
 ('04Y','NHS CANNOCK CHASE CCG','NHS Cannock Chase CCG','04Y','137595','82863','27456','27276'),
 ('05D','NHS EAST STAFFORDSHIRE CCG','NHS East Staffordshire CCG','05D','129944','76347','25515','28082'),
 ('05G','NHS NORTH STAFFORDSHIRE CCG','NHS North Staffordshire CCG','05G','219600','130420','49192','39988'),
 ('05Q','NHS SOUTH EAST STAFFORDSHIRE AND SEISDON PENINSULA CCG','NHS South East Staffordshire and Seisdon Peninsula CCG','05Q','226837','131507','51502','43828'),
 ('05V','NHS STAFFORD AND SURROUNDS CCG','NHS Stafford and Surrounds CCG','05V','157308','91332','36329','29647'),
 ('05W','NHS STOKE ON TRENT CCG','NHS Stoke on Trent CCG','05W','264651','158935','46441','59275'),
 ('M2L0M','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','NHS Shropshire CCG','05N','323136','183361','79762','60013'),
 ('M2L0M','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','NHS Telford and Wrekin CCG','05X','179854','107647','31087','41120'),
 ('15M','NHS DERBY AND DERBYSHIRE CCG','NHS Derby and Derbyshire CCG','15M','1026426','608228','210747','207451'),
 ('71E','NHS LINCOLNSHIRE CCG','NHS Lincolnshire CCG','71E','761224','435007','179805','146412'),
 ('52R','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','NHS Nottingham and Nottinghamshire CCG','52R','1043665','644492','186055','213118'),
 ('03W','NHS EAST LEICESTERSHIRE AND RUTLAND CCG','NHS East Leicestershire and Rutland CCG','03W','338592','194934','74480','69178'),
 ('04C','NHS LEICESTER CITY CCG','NHS Leicester City CCG','04C','354224','227021','43121','84082'),
 ('04V','NHS WEST LEICESTERSHIRE CCG','NHS West Leicestershire CCG','04V','407490','246379','80575','80536'),
 ('D2P2L','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','NHS Dudley CCG','05C','321596','186450','65602','69544'),
 ('D2P2L','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','NHS Sandwell and West Birmingham CCG','05L','506073','311168','65889','129016'),
 ('D2P2L','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','NHS Walsall CCG','05Y','285478','166387','50121','68970'),
 ('D2P2L','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','NHS Wolverhampton CCG','06A','263357','157219','43862','62276'),
 ('15E','NHS BIRMINGHAM AND SOLIHULL CCG','NHS Birmingham and Solihull CCG','15E','1180567','713664','178528','288375'),
 ('B2M3M','NHS COVENTRY AND WARWICKSHIRE CCG','NHS Coventry and Rugby CCG','05A','480456','305157','70966','104333'),
 ('B2M3M','NHS COVENTRY AND WARWICKSHIRE CCG','NHS South Warwickshire CCG','05R','273851','161636','60208','52007'),
 ('B2M3M','NHS COVENTRY AND WARWICKSHIRE CCG','NHS Warwickshire North CCG','05H','195147','114672','39330','41145'),
 ('18C','NHS HEREFORDSHIRE CCG','NHS Herefordshire and Worcestershire CCG','18C','788587','450107','183572','154908'),
 ('78H','NHS NORTHAMPTONSHIRE CCG','NHS Northamptonshire CCG','78H','736219','435858','132497','167864'),
 ('02P','NHS BARNSLEY CCG','NHS Barnsley CCG','02P','246866','147621','48162','51083'),
 ('02Q','NHS BASSETLAW CCG','NHS Bassetlaw CCG','02Q','117459','67924','26035','23500'),
 ('02X','NHS DONCASTER CCG','NHS Doncaster CCG','02X','311890','185227','59745','66918'),
 ('03L','NHS ROTHERHAM CCG','NHS Rotherham CCG','03L','265411','155590','52299','57522'),
 ('03N','NHS SHEFFIELD CCG','NHS Sheffield CCG','03N','584853','372677','94440','117736'),
 ('99C','NHS NORTH TYNESIDE CCG','NHS North Tyneside CCG','99C','207913','123959','42103','41851'),
 ('00L','NHS NORTHUMBERLAND CCG','NHS Northumberland CCG','00L','322434','183601','79783','59050'),
 ('00N','NHS SOUTH TYNESIDE CCG','NHS South Tyneside CCG','00N','150976','90215','30593','30168'),
 ('00P','NHS SUNDERLAND CCG','NHS Sunderland CCG','00P','277705','168013','54843','54849'),
 ('13T','NHS NEWCASTLE GATESHEAD CCG','NHS Newcastle Gateshead CCG','13T','504875','323493','83544','97838'),
 ('01H','NHS NORTH CUMBRIA CCG','NHS North Cumbria CCG','01H','319371','183696','75679','59996'),
 ('84H','NHS COUNTY DURHAM CCG','NHS County Durham CCG','84H','530094','318174','110452','101468'),
 ('16C','NHS TEES VALLEY CCG','NHS Tees Valley CCG','16C','675944','398421','130690','146833'),
 ('02Y','NHS EAST RIDING OF YORKSHIRE CCG','NHS East Riding of Yorkshire CCG','02Y','318400','175934','83650','58816'),
 ('03F','NHS HULL CCG','NHS Hull CCG','03F','259778','163085','39323','57370'),
 ('03H','NHS NORTH EAST LINCOLNSHIRE CCG','NHS North East Lincolnshire CCG','03H','159563','92174','32871','34518'),
 ('03K','NHS NORTH LINCOLNSHIRE CCG','NHS North Lincolnshire CCG','03K','172292','99998','36656','35638'),
 ('03Q','NHS VALE OF YORK CCG','NHS Vale of York CCG','03Q','366072','224277','74885','66910'),
 ('42D','NHS NORTH YORKSHIRE CCG','NHS North Yorkshire CCG','42D','428231','240385','106588','81258'),
 ('02T','NHS CALDERDALE CCG','NHS Calderdale CCG','02T','211455','125582','39755','46118'),
 ('X2C4Y','NHS KIRKLEES CCG','NHS Greater Huddersfield CCG','03A','246604','148220','45572','52812'),
 ('X2C4Y','NHS KIRKLEES CCG','NHS North Kirklees CCG','03J','193183','113376','32525','47282'),
 ('03R','NHS WAKEFIELD CCG','NHS Wakefield CCG','03R','348312','208191','66276','73845'),
 ('15F','NHS LEEDS CCG','NHS Leeds CCG','15F','793139','500201','123516','169422'),
 ('36J','NHS BRADFORD DISTRICT AND CRAVEN CCG','NHS Bradford District and Craven CCG','36J','590901','344365','94805','151731'),
 ('00T','NHS BOLTON CCG','NHS Bolton CCG','00T','287550','169343','49822','68385'),
 ('00V','NHS BURY CCG','NHS Bury CCG','00V','190990','112676','35025','43289'),
 ('01D','NHS HEYWOOD, MIDDLETON AND ROCHDALE CCG','NHS Heywood, Middleton and Rochdale CCG','01D','222412','132498','36615','53299'),
 ('00Y','NHS OLDHAM CCG','NHS Oldham CCG','00Y','237110','139206','38312','59592'),
 ('01G','NHS SALFORD CCG','NHS Salford CCG','01G','258834','164653','36729','57452'),
 ('01W','NHS STOCKPORT CCG','NHS Stockport CCG','01W','293423','171179','58726','63518'),
 ('01Y','NHS TAMESIDE AND GLOSSOP CCG','NHS Tameside and Glossop CCG','01Y','260063','156090','46618','57355'),
 ('02A','NHS TRAFFORD CCG','NHS Trafford CCG','02A','237354','139660','41183','56511'),
 ('02H','NHS WIGAN BOROUGH CCG','NHS Wigan Borough CCG','02H','328662','196838','62908','68916'),
 ('14L','NHS MANCHESTER CCG','NHS Manchester CCG','14L','552858','378503','51441','122914'),
 ('01F','NHS HALTON CCG','NHS Halton CCG','01F','129410','76828','23812','28770'),
 ('01J','NHS KNOWSLEY CCG','NHS Knowsley CCG','01J','150862','91027','26033','33802'),
 ('99A','NHS LIVERPOOL CCG','NHS Liverpool CCG','99A','498042','328476','73514','96052'),
 ('01T','NHS SOUTH SEFTON CCG','NHS South Sefton CCG','01T','160226','94380','33268','32578'),
 ('01V','NHS SOUTHPORT AND FORMBY CCG','NHS Southport and Formby CCG','01V','116184','62847','31858','21479'),
 ('01X','NHS ST HELENS CCG','NHS St Helens CCG','01X','180585','106514','37203','36868'),
 ('02E','NHS WARRINGTON CCG','NHS Warrington CCG','02E','210014','125978','39645','44391'),
 ('12F','NHS WIRRAL CCG','NHS Wirral CCG','12F','324011','185640','70863','67508'),
 ('27D','NHS CHESHIRE CCG','NHS Cheshire CCG','27D','727223','418805','162472','145946'),
 ('00Q','NHS BLACKBURN WITH DARWEN CCG','NHS Blackburn with Darwen CCG','00Q','149696','89180','21774','38742'),
 ('00R','NHS BLACKPOOL CCG','NHS Blackpool CCG','00R','139446','81736','28495','29215'),
 ('00X','NHS CHORLEY AND SOUTH RIBBLE CCG','NHS Chorley and South Ribble CCG','00X','178547','106155','35317','37075'),
 ('01A','NHS EAST LANCASHIRE CCG','NHS East Lancashire CCG','01A','382746','222709','73962','86075'),
 ('02G','NHS WEST LANCASHIRE CCG','NHS West Lancashire CCG','02G','114306','66847','25313','22146'),
 ('02M','NHS FYLDE AND WYRE CCG','NHS Fylde and Wyre CCG','02M','194337','106197','53756','34384'),
 ('01E','NHS GREATER PRESTON CCG','NHS Greater Preston CCG','01E','203825','124259','35573','43993'),
 ('01K','NHS MORECAMBE BAY CCG','NHS Morecambe Bay CCG','01K','332696','194338','77496','60862')

# COMMAND ----------

#below section recreates nhs england pre-processing tables found here:
#https://github.com/nhsengland/MHSDS/tree/master/PreProcessing

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_header;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Header USING DELTA AS
 ---PreProc_Header: https://github.com/nhsengland/MHSDS/blob/master/PreProcessing/PreProcessing%20-%201%20Checks.sql 
 select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate, label as Der_FY
 from $db_source.mhs000header h
 left join [DATABASE].[CALENDAR_FY] fy on h.reportingperiodstartdate between fy.START_DATE and fy.END_DATE
 order by 1 desc

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_referral;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Referral USING DELTA AS
 ---PreProc_Referral: https://github.com/nhsengland/MHSDS/blob/master/PreProcessing/PreProcessing%20-%202%20Referral.sql
 SELECT
 h.Der_FY,
 r.MHS101UniqID,
 r.Person_ID,
 r.OrgIDProv,
 m.UniqMonthID,
 r.RecordNumber,
 r.UniqServReqID,
 r.OrgIDComm,
 r.ReferralRequestReceivedDate,
 r.ReferralRequestReceivedTime,
 r.SpecialisedMHServiceCode,
 r.PrimReasonReferralMH,
 r.ReasonOAT,
 r.DischPlanCreationDate,
 r.DischPlanCreationTime,
 r.DischPlanLastUpdatedDate,
 r.DischPlanLastUpdatedTime,
 r.ServDischDate,
 r.ServDischTime,
 r.DischLetterIssDate,
 r.AgeServReferRecDate,
 r.AgeServReferDischDate,
 r.RecordStartDate,
 r.RecordEndDate,
 r.InactTimeRef,
 m.MHS001UniqID,
 m.OrgIDCCGRes,
 m.OrgIDEduEstab,
 m.EthnicCategory,
 --m.EthnicCategory2021, --new for v5 but not being used in final prep table
 m.NHSDEthnicity,
 m.Gender,
 CASE WHEN m.GenderIDCode IN ('1','2','3','4','X','Z') THEN m.GenderIDCode ELSE m.Gender END AS Gender2021, --new for v5 but not being used in final prep table
 m.MaritalStatus,
 m.PersDeathDate,
 m.AgeDeath,
 m.OrgIDLocalPatientId,
 m.OrgIDResidenceResp,
 m.LADistrictAuth,
 m.LSOA2011,
 m.PostcodeDistrict,
 m.DefaultPostcode,
 m.AgeRepPeriodStart,
 m.AgeRepPeriodEnd,
 s.MHS102UniqID,
 s.UniqCareProfTeamID,
 s.ServTeamTypeRefToMH,
 s.CAMHSTier,
 s.ReferRejectionDate,
 s.ReferRejectionTime,
 s.ReferRejectReason,
 s.ReferClosureDate,
 s.ReferClosureTime,
 s.ReferClosReason,
 s.AgeServReferClosure,
 s.AgeServReferRejection
 FROM                $db_source.mhs101referral r
 INNER JOIN          $db_source.mhs001mpi m 
                     ON r.RecordNumber = m.RecordNumber ---joining on recordnumber opposed to person_id as we want OrgIDCCGRes as it was inputted when referral was submitted in that month
 LEFT JOIN           $db_source.mhs102servicetypereferredto s 
                     ON r.UniqServReqID = s.UniqServReqID 
                     AND r.RecordNumber = s.RecordNumber --joining on recordnumber aswell to match historic records as they will all have the same uniqservreqid    
 LEFT JOIN           $db_output.NHSE_Pre_Proc_Header h
                     ON r.UniqMonthID = h.UniqMonthID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_activity;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Activity USING DELTA AS
 ---PreProc_Activity: https://github.com/nhsengland/MHSDS/blob/master/PreProcessing/PreProcessing%20-%203%20Activity.sql
 SELECT
 	h.Der_FY,
     'DIRECT' AS Der_ActivityType,
 	c.MHS201UniqID AS Der_ActivityUniqID,
 	c.Person_ID,
 	c.UniqMonthID,
 	c.OrgIDProv,
 	c.RecordNumber,
 	c.UniqServReqID,
     c.CareContDate AS Der_ContactDate,
 	c.CareContTime AS Der_ContactTime,
     c.ConsMechanismMH, --new for v5
     c.AttendOrDNACode,
 	CASE WHEN c.OrgIDProv = 'DFC' THEN '1' ELSE c.Person_ID END AS Der_PersonID -- derivation added to better reflect anonymous services where personID may change every month    
     
 FROM $db_source.mhs201carecontact c
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON c.UniqMonthID = h.UniqMonthID
 
 UNION ALL
 
 SELECT
     h.Der_FY,
 	'INDIRECT' AS Der_ActivityType,
 	i.MHS204UniqID AS Der_ActivityUniqID,
 	i.Person_ID,
 	i.UniqMonthID,
 	i.OrgIDProv,
 	i.RecordNumber,
 	i.UniqServReqID,
     i.IndirectActDate AS Der_ContactDate,
 	i.IndirectActTime AS Der_ContactTime,
     'NULL' AS ConsMechanismMH, --new for v5
 	'NULL' AS AttendOrDNACode,
 	CASE WHEN i.OrgIDProv = 'DFC' THEN '1' ELSE i.Person_ID END AS Der_PersonID -- derivation added to better reflect anonymous services where personID may change every month
 	
 FROM $db_source.mhs204indirectactivity i
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header h ON i.UniqMonthID = h.UniqMonthID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_activity;
 CREATE TABLE         $db_output.Der_NHSE_Pre_Proc_Activity USING DELTA AS
 ---applying derivations to PreProc_Activity
 SELECT *,
 ROW_NUMBER() OVER (PARTITION BY 
                    CASE WHEN a.OrgIDProv = 'DFC' THEN a.UniqServReqID
                    ELSE a.Person_ID END, 
                    a.UniqServReqID 
                    ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_ContactOrder,
 ROW_NUMBER() OVER (PARTITION BY 
                    CASE WHEN a.OrgIDProv = 'DFC' THEN a.UniqServReqID
                    ELSE a.Person_ID END, 
                    a.UniqServReqID, a.Der_FY 
                    ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_FYContactOrder
                    
 FROM $db_output.NHSE_Pre_Proc_Activity a

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_inpatients;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Inpatients USING DELTA AS
 ---PreProc_Inpatients: https://github.com/nhsengland/MHSDS/blob/master/PreProcessing/PreProcessing%20-%205%20Inpatients.sql
 SELECT
 he.Der_FY,
 h.MHS501UniqID,
 h.Person_ID,
 h.OrgIDProv,
 h.UniqMonthID,
 h.RecordNumber,
 h.UniqHospProvSpellID, --new for v5
 h.UniqServReqID,
 CONCAT(h.Person_ID, h.UniqServReqID) as UniqPersRefID,
 CONCAT(h.Person_ID, h.UniqServReqID, h.UniqMonthID) as UniqPersRefID_FY,
 h.StartDateHospProvSpell,
 h.StartTimeHospProvSpell,
 h.SourceAdmMHHospProvSpell, --new for v5
 h.MethAdmMHHospProvSpell, --new for v5
 h.EstimatedDischDateHospProvSpell,
 h.PlannedDischDateHospProvSpell,
 h.DischDateHospProvSpell,
 h.DischTimeHospProvSpell,
 h.MethOfDischMHHospProvSpell, --new for v5
 h.DestOfDischHospProvSpell, --new for v5
 h.InactTimeHPS,
 h.PlannedDestDisch,
 h.PostcodeDistrictMainVisitor,
 h.PostcodeDistrictDischDest,
 r.LSOA2011,
 w.MHS502UniqID,
 w.UniqWardStayID,
 w.StartDateWardStay,
 w.StartTimeWardStay,
 w.SiteIDOfTreat,
 w.WardType,
 w.WardSexTypeCode,
 w.IntendClinCareIntenCodeMH,
 w.WardSecLevel,
 w.SpecialisedMHServiceCode,
 w.WardCode,
 w.WardLocDistanceHome,
 w.LockedWardInd,
 w.InactTimeWS,
 w.WardAge,
 w.HospitalBedTypeMH,
 w.EndDateMHTrialLeave,
 w.EndDateWardStay,
 w.EndTimeWardStay,
 CASE WHEN h.DischDateHospProvSpell IS NOT NULL THEN 'CLOSED' ELSE 'OPEN' END AS Der_HospSpellStatus    
 FROM $db_source.mhs501hospprovspell h
 LEFT JOIN $db_source.mhs502wardstay w ON h.UniqServReqID = w.UniqServReqID 
                                       AND h.UniqHospProvSpellID = w.UniqHospProvSpellID  --updated for v5
                                       AND h.RecordNumber = w.RecordNumber
 LEFT JOIN $db_output.NHSE_Pre_Proc_Header he ON h.UniqMonthID = he.UniqMonthID  
 LEFT JOIN $db_output.nhse_pre_proc_referral r on h.UniqServReqID = r.UniqServReqID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_inpatients;
 CREATE TABLE         $db_output.Der_NHSE_Pre_Proc_Inpatients USING DELTA AS
 ---applying derivations to PreProc_Inpatients
 SELECT *,
     ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC) AS Der_HospSpellRecordOrder, 
 	ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC, i.EndDateWardStay DESC, i.MHS502UniqID DESC) AS Der_LastWardStayRecord,
 	ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID ASC, i.EndDateWardStay ASC, i.MHS502UniqID ASC) AS Der_FirstWardStayRecord
     
 FROM $db_output.NHSE_Pre_Proc_Inpatients i

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.metric_name_lookup;
 INSERT INTO $db_output.metric_name_lookup 
 ---metric names reference data
 VALUES
 ('10a','Number of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment'),
 ('10b','Number of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks'),
 ('10c','Proportion of referrals on Early Intervention for Psychosis (EIP) pathway that waited 2 weeks or less to enter treatment '),
 ('10d','Number of open referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks'),
 ('10e','Number of referrals not on Early Intervention for Psychosis (EIP) pathway, receiving a first contact and assigned a care co-ordinator with any team'),
 ('10f','Number of referrals on Early Intervention for Psychosis (EIP) pathway'),
 ('10g','Crude rate of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment per 100,000 population'),
 ('10h','Crude rate of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks per 100,000 population'),
 ('10i','Crude rate of open referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks per 100,000 population'),
 ('10j','Crude rate of referrals not on Early Intervention for Psychosis (EIP) pathway, receiving a first contact and assigned a care co-ordinator with any team per 100,000 population'),
 ('10k','Crude rate of referrals on Early Intervention for Psychosis (EIP) pathway per 100,000 population'),
 ('11a', 'Number of people in contact with Specialist Perinatal Mental Health Community Services'),
 ('11b', 'Crude rate of people in contact with Specialist Perinatal Mental Health Community Services per 100,000 females'),
 ('12a', 'Number of discharges from adult acute beds eligible for 72 hour follow up'),
 ('12b', 'Number of discharges from adult acute beds followed up within 72 hours'),
 ('12c', 'Proportion of discharges from adult acute beds that were followed up within 72 hours'),
 ('13a','Number of open referrals to memory services team at the end of the year'),
 ('13b','Number of contacts with memory services team'),
 ('13c','Number of attended contacts with memory services team'),
 
 ('14a','Number of people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts within the RP'),
 ('14b','Crude rate of people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts within the RP per 100,000 population'),
 ('15a','Adult and older adult acute admissions in the reporting period'),
 ('15b','Adult and older adult acute admissions for patients with contact in the prior year with mental health services, in the RP'),
 ('15c','Adult and older adult acute admissions for patients with no contact in the prior year with mental health services, in the RP'),
 ('15d','Percentage of adult and older adult acute admissions for patients with contact in the prior year with mental health services, in the RP'),
 ('15e','Percentage of adult and older adult acute admissions for patients with no contact in the prior year with mental health services, in the RP'),
 ('16a','The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 60+ days'),
 ('16b','The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 90+ days'),
 ('16c','The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 60+ days'),
 ('16d','The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 90+ days'),
 ('16e','The number of people discharged in the RP aged 0 to 17 with a length of stay of 60+ days'),
 ('16f','The number of people discharged in the RP aged 0 to 17 with a length of stay of 90+ days'),
 ('16g','Crude rate of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 60+ days per 100,000 population'),
 ('16h','Crude rate of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 90+ days per 100,000 population'),
 ('16i','Crude rate of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 60+ days per 100,000 population'),
 ('16j','Crude rate of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 90+ days per 100,000 population'),
 ('16k','Crude rate of people discharged in the RP aged 0 to 17 with a length of stay of 60+ days per 100,000 population'),
 ('16l','Crude rate of people discharged in the RP aged 0 to 17 with a length of stay of 90+ days per 100,000 population'),
 ('17a','Number of children and young people aged under 18 supported through NHS funded mental health with at least one contact'),
 ('17b','Crude rate of children and young people aged under 18 supported through NHS funded mental health with at least one contact per 100,000 population aged 0-17'),
 
 ('1a','Number of people in contact with NHS funded secondary mental health, learning disabilities and autism services'),
 ('1b','Number of people admitted as an inpatient with NHS funded secondary mental health, learning disabilities and autism services'),
 ('1c','Number of people not admitted as an inpatient while in contact with NHS funded secondary mental health, learning disabilities and autism services'),
 ('1d','Proportion of people in contact with NHS funded secondary mental health, learning disabilities and autism services admitted as an inpatient'),
 ('1e','Proportion of people in contact with NHS funded secondary mental health, learning disabilities and autism services not admitted as an inpatient'),
 ('1f','Census population - 2019 Mid-Year Estimate'),
 ('1g','Proportion of the population in contact with NHS funded secondary mental health, learning disabilities and autism services'),
 ('1h','Number of people in contact with NHS funded secondary mental health, learning disabilities and autism services with a known age, gender and ethnicity'),
 ('1i','Crude rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population'),
 ('1j','Standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population'),
 ('1k','Confidence interval for the standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population (+/-)'),
 ('1m','Census population - 2011'),
 ('3a','Number of Care Clusters assigned at the end of the year'),
 ('4a','Number of in year bed days'),
 ('5a','Number of admissions to NHS funded secondary mental health, learning disabilities and autism inpatient services'),
 ('5b','Number of discharges from NHS funded secondary mental health, learning disabilities and autism inpatient services'),
 ('5c','Average (mean) number of daily occupied beds in NHS funded secondary mental health, learning disabilities and autism inpatient services'),
 ('6a','Number of contacts with secondary mental health, learning disabilities and autism services'),
 ('6b','Proportion of contacts with secondary mental health, learning disabilities and autism services which the patient attended'),
 ('6c','Proportion of contacts with secondary mental health, learning disabilities and autism services which the patient did not attend'),
 ('6d','Proportion of contacts with secondary mental health, learning disabilities and autism services which the provider cancelled'),
 ('6e','Proportion of contacts with secondary mental health, learning disabilities and autism services with an invalid or missing attendance code'),
 ('7a','Number of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services'),
 ('7b','Number of restrictive interventions in NHS funded secondary mental health, learning disabilities and autism services'),
 ('7c','Number of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services whose age, gender and ethnicity are known'),
 ('7d','Crude rate of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population'),
 ('7e','Standardised rate of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population'),
 ('7f','Confidence intervals for the standardised rate of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population (+/-)'),
 ('9a','Number of children and young people, receiving at least two contacts (including indirect contacts) in the reporting period and where their first contact occurs before their 18th birthday'),
 ('9b','Crude rate of children and young people, receiving at least two contacts (including indirect contacts) in the reporting period and where their first contact occurs before their 18th birthday per 100,000 population aged 0-17');