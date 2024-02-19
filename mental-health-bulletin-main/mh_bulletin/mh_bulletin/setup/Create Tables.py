# Databricks notebook source
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
start_month_id = dbutils.widgets.get("start_month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
status = dbutils.widgets.get("status")
populationyear = dbutils.widgets.get("populationyear")
IMD_year = dbutils.widgets.get("IMD_year")

# COMMAND ----------

 %md
 #### CSV Lookup Tables

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.bulletin_breakdown_values;
 CREATE TABLE IF NOT EXISTS $db_output.bulletin_breakdown_values 
 (
 BREAKDOWN STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.bulletin_level_values;
 CREATE TABLE IF NOT EXISTS $db_output.bulletin_level_values 
 (
 BREAKDOWN STRING,
 LEVEL_ONE STRING,
 LEVEL_ONE_DESCRIPTION STRING,
 LEVEL_TWO STRING,
 LEVEL_TWO_DESCRIPTION STRING,
 LEVEL_THREE STRING,
 LEVEL_THREE_DESCRIPTION STRING,
 LEVEL_FOUR STRING,
 LEVEL_FOUR_DESCRIPTION STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.bulletin_csv_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.bulletin_csv_lookup
 (
 REPORTING_PERIOD_START date,
 REPORTING_PERIOD_END date,
 STATUS string,
 BREAKDOWN string,
 LEVEL_ONE STRING,
 LEVEL_ONE_DESCRIPTION STRING,
 LEVEL_TWO STRING,
 LEVEL_TWO_DESCRIPTION STRING,
 LEVEL_THREE STRING,
 LEVEL_THREE_DESCRIPTION STRING,
 LEVEL_FOUR STRING,
 LEVEL_FOUR_DESCRIPTION STRING,
 METRIC string,
 METRIC_NAME string,
 SOURCE_DB string
 ) USING DELTA

# COMMAND ----------

 %md
 #### Demographic Reference Tables

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.NHSDEthnicityDim;
 CREATE TABLE IF NOT EXISTS $db_output.NHSDEthnicityDim
 (
 key string,
 id string,
 description string,
 FK_dd_ethnic_category_code_DSS_KEY int,
 FK_custom_ethnic_group_v01_DSS_KEY int
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.TeamType;
 CREATE TABLE IF NOT EXISTS $db_output.TeamType
 (
 TeamTypeCode string,
 TeamName string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.attendance_code;
 CREATE TABLE IF NOT EXISTS $db_output.attendance_code
 (
 AttCode string,
 AttName string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Der_Gender;
 CREATE TABLE IF NOT EXISTS $db_output.Der_Gender 
 (
 Der_Gender string,
 Der_Gender_Desc string
 )

# COMMAND ----------

 %md
 #### Geographic Reference Data Tables

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhb_org_daily;
 CREATE TABLE IF NOT EXISTS $db_output.mhb_org_daily 
 (
 ORG_CODE string,
 ORG_TYPE_CODE string,
 NAME string,
 ORG_OPEN_DATE date,
 ORG_CLOSE_DATE date,
 BUSINESS_START_DATE date,
 BUSINESS_END_DATE date,
 ORG_IS_CURRENT int
 ) USING delta

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhb_rd_ccg_latest;
 CREATE TABLE IF NOT EXISTS $db_output.mhb_rd_ccg_latest 
 (
 ORG_CODE        STRING,
 NAME            STRING
 ) USING delta

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_region_mapping;
 CREATE TABLE IF NOT EXISTS $db_output.stp_region_mapping 
 (
 CCG_Code string, 
 CCG_Name string, 
 STP_Code string, 
 STP_Name string, 
 Region_Code string, 
 Region_Name string
 ) USING DELTA;

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhsds_providers_in_year;
 CREATE TABLE IF NOT EXISTS $db_output.mhsds_providers_in_year
 (
 OrgIDProv string, 
 Provider_Name string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.la;
 CREATE TABLE IF NOT EXISTS $db_output.la
 (
 type string, 
 level string,
 level_description string
 ) USING DELTA

# COMMAND ----------

 %sql
  DROP TABLE IF EXISTS $db_output.ccg_mapping_2021;
  CREATE TABLE IF NOT EXISTS $db_output.ccg_mapping_2021 
  ---ccg mapping 2021 table (needed for community access and admissions metrics)
  (
  CCG_UNMAPPED STRING, 
  CCG21CDH STRING, 
  CCG21CD STRING, 
  CCG21NM STRING, 
  STP21CD STRING, 
  STP21CDH STRING, 
  STP21NM STRING, 
  NHSER21CD STRING, 
  NHSER21CDH STRING, 
  NHSER21NM STRING
  ) USING DELTA

# COMMAND ----------

 %md
 #### MHSDS Reference Data Tables

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mh_ass;
 CREATE TABLE IF NOT EXISTS $db_output.mh_ass 
 (
 Category STRING,
 Assessment_Tool_Name STRING,
 Preferred_Term_SNOMED STRING,
 Active_Concept_ID_SNOMED BIGINT,
 SNOMED_Version STRING,
 Lower_Range INT,
 Upper_Range INT,
 CYPMH STRING,
 EIP STRING,
 Rater STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.validcodes1; 
 CREATE TABLE IF NOT EXISTS $db_output.validcodes1
 (
 Tablename string,
 Field string,
 Measure string,
 Type string,
 ValidValue string,
 FirstMonth int,
 LastMonth int
 ) USING DELTA;

# COMMAND ----------

 %md
 #### NHS England Pre-Processing Tables 
 (https://github.com/nhsengland/MHSDS/tree/master/PreProcessing)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_referral;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_referral
 (
 Der_FY STRING,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 MHS101UniqID BIGINT,
 Person_ID string,
 OrgIDProv string,
 UniqMonthID BIGINT,
 RecordNumber BIGINT,
 UniqServReqID string,
 OrgIDComm string,
 ReferralRequestReceivedDate date,
 ReferralRequestReceivedTime timestamp,
 SpecialisedMHServiceCode string,
 PrimReasonReferralMH string,
 ReasonOAT string,
 DischPlanCreationDate date,
 DischPlanCreationTime timestamp,
 DischPlanLastUpdatedDate date,
 DischPlanLastUpdatedTime timestamp,
 ServDischDate date,
 ServDischTime timestamp,
 DischLetterIssDate string,
 AgeServReferRecDate BIGINT,
 AgeServReferDischDate BIGINT,
 RecordStartDate date,
 RecordEndDate date,
 InactTimeRef date,
 MHS001UniqID BIGINT,
 OrgIDCCGRes string,
 OrgIDEduEstab string,
 EthnicCategory string,
 EthnicCategory2021 string,
 NHSDEthnicity string,
 Gender string,
 Gender2021 string,
 MaritalStatus string,
 PersDeathDate date,
 AgeDeath BIGINT,
 OrgIDLocalPatientId string,
 OrgIDResidenceResp string,
 LADistrictAuth string,
 LSOA2011 string,
 PostcodeDistrict string,
 DefaultPostcode string,
 AgeRepPeriodStart BIGINT,
 AgeRepPeriodEnd BIGINT,
 MHS102UniqID BIGINT,
 UniqCareProfTeamID string,
 ServTeamTypeRefToMH string,
 CAMHSTier string,
 ReferRejectionDate date,
 ReferRejectionTime timestamp,
 ReferRejectReason string,
 ReferClosureDate date,
 ReferClosureTime timestamp,
 ReferClosReason string,
 AgeServReferClosure BIGINT,
 AgeServReferRejection BIGINT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_distinct_indirect_activity;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_distinct_indirect_activity
 (
 UniqSubmissionID string,
 UniqMonthID bigint,
 OrgIDProv string,
 Person_ID string,
 Der_PersonID string,
 RecordNumber string,
 UniqServReqID string,
 OrgIDComm string,
 CareProfTeamLocalId string,
 IndirectActDate date,
 IndirectActTime timestamp,
 DurationIndirectAct bigint,
 MHS204UniqID string,
 Der_ActRN int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_activity;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_activity
 (
 Der_FY string,
 Der_ActivityType string,
 Der_ActivityUniqID BIGINT,
 Person_ID string,
 UniqMonthID BIGINT,
 OrgIDProv string,
 RecordNumber BIGINT,
 UniqServReqID string,
 UniqCareContID string,
 Der_ContactDate date,
 Der_ContactTime timestamp,
 ConsMechanismMH string,
 AttendOrDNACode string,
 Der_PersonID string,
 Der_DirectContact string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_activity;
 CREATE TABLE IF NOT EXISTS $db_output.der_nhse_pre_proc_activity
 (
 Der_FY string,
 Der_ActivityType string,
 Der_ActivityUniqID BIGINT,
 Person_ID string,
 UniqMonthID BIGINT,
 OrgIDProv string,
 RecordNumber BIGINT,
 UniqServReqID string,
 UniqCareContID string,
 Der_ContactDate date,
 Der_ContactTime timestamp,
 ConsMechanismMH string,
 AttendOrDNACode string,
 Der_PersonID string,
 Der_DirectContact string,
 Der_ContactOrder INT,
 Der_FYContactOrder INT
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_inpatients;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_inpatients
 (
 Der_FY STRING,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 MHS501UniqID BIGINT,
 Person_ID string,
 OrgIDProv string,
 UniqMonthID BIGINT,
 RecordNumber BIGINT,
 UniqHospProvSpellID string,
 UniqServReqID string,
 UniqPersRefID string,
 UniqPersRefID_FY string,
 StartDateHospProvSpell date,
 StartTimeHospProvSpell timestamp,
 SourceAdmMHHospProvSpell string,
 MethAdmMHHospProvSpell string,
 EstimatedDischDateHospProvSpell date,
 PlannedDischDateHospProvSpell date,
 DischDateHospProvSpell date,
 DischTimeHospProvSpell timestamp,
 MethOfDischMHHospProvSpell string,
 DestOfDischHospProvSpell string,
 InactTimeHPS date,
 PlannedDestDisch string,
 PostcodeDistrictMainVisitor string,
 PostcodeDistrictDischDest string,
 MHS502UniqID BIGINT,
 UniqWardStayID string,
 StartDateWardStay date,
 StartTimeWardStay timestamp,
 SiteIDOfTreat string,
 WardType string,
 WardSexTypeCode string,
 IntendClinCareIntenCodeMH string,
 WardSecLevel string,
 SpecialisedMHServiceCode string,
 WardCode string,
 WardLocDistanceHome bigint,
 LockedWardInd string,
 InactTimeWS date,
 WardAge string,
 HospitalBedTypeMH string,
 EndDateMHTrialLeave date,
 EndDateWardStay date,
 EndTimeWardStay timestamp,
 Der_HospSpellStatus string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.der_nhse_pre_proc_inpatients;
 CREATE TABLE IF NOT EXISTS $db_output.der_nhse_pre_proc_inpatients
 (
 Der_FY STRING,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 MHS501UniqID BIGINT,
 Person_ID string,
 OrgIDProv string,
 UniqMonthID BIGINT,
 RecordNumber BIGINT,
 UniqHospProvSpellID string,
 UniqServReqID string,
 UniqPersRefID string,
 UniqPersRefID_FY string,
 StartDateHospProvSpell date,
 StartTimeHospProvSpell timestamp,
 SourceAdmMHHospProvSpell string,
 MethAdmMHHospProvSpell string,
 EstimatedDischDateHospProvSpell date,
 PlannedDischDateHospProvSpell date,
 DischDateHospProvSpell date,
 DischTimeHospProvSpell timestamp,
 MethOfDischMHHospProvSpell string,
 DestOfDischHospProvSpell string,
 InactTimeHPS date,
 PlannedDestDisch string,
 PostcodeDistrictMainVisitor string,
 PostcodeDistrictDischDest string,
 MHS502UniqID BIGINT,
 UniqWardStayID string,
 StartDateWardStay date,
 StartTimeWardStay timestamp,
 SiteIDOfTreat string,
 WardType string,
 WardSexTypeCode string,
 IntendClinCareIntenCodeMH string,
 WardSecLevel string,
 SpecialisedMHServiceCode string,
 WardCode string,
 WardLocDistanceHome bigint,
 LockedWardInd string,
 InactTimeWS date,
 WardAge string,
 HospitalBedTypeMH string,
 EndDateMHTrialLeave date,
 EndDateWardStay date,
 EndTimeWardStay timestamp,
 Der_HospSpellStatus string,
 Der_HospSpellRecordOrder int, 
 Der_LastWardStayRecord int,
 Der_FirstWardStayRecord int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.prep_nhse_pre_proc_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.prep_nhse_pre_proc_assessments
 (
 Der_AssTable string,
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID bigint,
 UniqMonthID bigint,
 CodedAssToolType string,
 PersScore string,
 Der_AssToolCompDate date,
 RecordNumber string,
 Der_AssUniqID bigint,
 OrgIDProv string,
 Person_ID string,
 UniqServReqID string,
 Der_AgeAssessTool string,
 UniqCareContID string,
 UniqCareActID string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_assessments
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID string,
 Der_AssUniqID bigint,
 Der_AssTable string, 
 Person_ID string,    
 UniqMonthID bigint,    
 OrgIDProv string,
 RecordNumber string,   
 UniqServReqID string,    
 UniqCareContID string,
 UniqCareActID string,       
 Der_AssToolCompDate date,
 CodedAssToolType string,
 PersScore string,
 Der_AgeAssessTool string,
 Der_AssessmentCategory string,
 Der_AssessmentToolName string,
 Der_PreferredTermSNOMED string,
 Der_SNOMEDCodeVersion string,
 Der_LowerRange string,
 Der_UpperRange string,
 Rater string,
 Der_ValidScore string,
 Der_UniqAssessment string,
 Der_AssKey string,
 Der_AssInMonth int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_assessments_unique;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_assessments_unique
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID string,
 UniqMonthID string,
 OrgIDProv string,
 Person_ID string,
 RecordNumber string,
 UniqServReqID string,
 UniqCareContID string,
 UniqCareActID string,
 CodedAssToolType string,
 PersScore string,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate string,
 Der_AgeAssessTool string,
 Der_AssessmentToolName string,
 Der_PreferredTermSNOMED string,
 Der_SNOMEDCodeVersion string,
 Der_LowerRange int,
 Der_UpperRange int,
 Der_ValidScore string,
 Der_AssessmentCategory string,
 Der_AssKey string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_assessments_unique_valid;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_assessments_unique_valid
 (
 ReportingPeriodStartDate date,
 ReportingPeriodEndDate date,
 Der_FY string,
 UniqSubmissionID string,
 UniqMonthID string,
 OrgIDProv string,
 Person_ID string,
 RecordNumber string,
 UniqServReqID string,
 UniqCareContID string,
 UniqCareActID string,
 CodedAssToolType string,
 PersScore string,
 Der_AssUniqID string,
 Der_AssTable string,
 Der_AssToolCompDate string,
 Der_AgeAssessTool string,
 Der_AssessmentToolName string,
 Der_PreferredTermSNOMED string,
 Der_SNOMEDCodeVersion string,
 Der_LowerRange int,
 Der_UpperRange int,
 Der_ValidScore string,
 Der_AssessmentCategory string,
 Der_AssOrderAsc_OLD int,
 Der_AssOrderDesc_OLD int,
 Der_AssOrderAsc_NEW int,
 Der_AssOrderDesc_NEW int,
 Der_AssKey string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_interventions;
 CREATE TABLE IF NOT EXISTS $db_output.nhse_pre_proc_interventions
 (
 RecordNumber string,
 OrgIDProv string,
 Person_ID string,
 UniqMonthID string,
 UniqServReqID string,
 UniqCareContID string,
 Der_ContactDate date,
 UniqCareActID string,
 Der_InterventionUniqID string,
 CodeProcAndProcStatus string,
 Der_SNoMEDProcCode string,
 Der_SNoMEDProcQual string,   
 CodeObs string
 ) USING DELTA

# COMMAND ----------

 %md
 #### Final Output Tables

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.automated_output_unsuppressed1;
 CREATE or replace TABLE $db_output.automated_output_unsuppressed1
 (
 REPORTING_PERIOD_START      STRING,
 REPORTING_PERIOD_END        STRING,
 STATUS                      STRING,
 BREAKDOWN                   STRING,
 LEVEL_ONE                   STRING,
 LEVEL_ONE_DESCRIPTION       STRING,
 LEVEL_TWO                   STRING,
 LEVEL_TWO_DESCRIPTION       STRING,
 LEVEL_THREE                 STRING,
 LEVEL_THREE_DESCRIPTION     STRING,
 LEVEL_FOUR                  STRING,
 LEVEL_FOUR_DESCRIPTION      STRING,
 METRIC                      STRING,
 METRIC_NAME                 STRING,
 METRIC_VALUE                DECIMAL(10,2),
 SOURCE_DB                   STRING  
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.automated_output_suppressed;
 CREATE TABLE IF NOT EXISTS $db_output.automated_output_suppressed
 (
 REPORTING_PERIOD_START      STRING,
 REPORTING_PERIOD_END        STRING,
 STATUS                      STRING,
 BREAKDOWN                   STRING,
 LEVEL_ONE                   STRING,
 LEVEL_ONE_DESCRIPTION       STRING,
 LEVEL_TWO                   STRING,
 LEVEL_TWO_DESCRIPTION       STRING,
 LEVEL_THREE                 STRING,
 LEVEL_THREE_DESCRIPTION     STRING,
 LEVEL_FOUR                  STRING,
 LEVEL_FOUR_DESCRIPTION      STRING,
 METRIC                      STRING,
 METRIC_NAME                 STRING,
 METRIC_VALUE                STRING,
 SOURCE_DB                   STRING 
 )
 USING delta

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.error_log;
 CREATE TABLE IF NOT EXISTS $db_output.error_log
 (
 RUNDATE date,
 Tablename string,
 Test string,
 Metric string,
 Breakdown string,
 Pass boolean
 ) USING DELTA;
 OPTIMIZE $db_output.error_log

# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.NHSDEthnicityDim;
 
 CREATE TABLE IF NOT EXISTS $db_output.NHSDEthnicityDim 
 (
 key STRING,
 id STRING,
 description STRING,
 FK_dd_ethnic_category_code_DSS_KEY INT,
 FK_custom_ethnic_group_v01_DSS_KEY INT
 ) USING DELTA

# COMMAND ----------

