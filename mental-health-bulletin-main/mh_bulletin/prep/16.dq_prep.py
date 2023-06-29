# Databricks notebook source
#initialise widgets
dbutils.widgets.text("db_output", "output_database") #output database
dbutils.widgets.text("db_source", "mh_database") #mhsds database
dbutils.widgets.text("month_id_end", "1452") #uniqmonthid in mhs000header for end month of financial year
dbutils.widgets.text("month_id_start", "1441", "month_id_start") #uniqmonthid in mhs000header for start month of financial year
dbutils.widgets.text("rp_enddate", "2021-03-31") #end date of the financial year
dbutils.widgets.text("rp_startdate", "2020-04-01", "rp_startdate") #start date of the financial year
dbutils.widgets.text("populationyear", "2020") #year of count for population data
dbutils.widgets.text("status", "Final") #mhsds submission window
dbutils.widgets.text("IMD_year", "2019", "IMD_year") #year for deprivation data (usually in 2 year cycles)
dbutils.widgets.text("LAD_published_date", "2020-03-27") #date at which latest local authority reference data was published

#get widgets as python variables
db_output  = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id_end = dbutils.widgets.get("month_id_end")
month_id_start = dbutils.widgets.get("month_id_start")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")
populationyear  = dbutils.widgets.get("populationyear")
status  = dbutils.widgets.get("status")
IMD_year  = dbutils.widgets.get("IMD_year")
LAD_published_date  = dbutils.widgets.get("LAD_published_date")

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

def suppression(x, base=5): 
  #mhsds suppression logic
    if x < 5:
        return '*'
    elif x== 999999999:
        return '*'
    else:
        return str(int(base * round(float(x)/base)))

# COMMAND ----------

def tb2_suppression(x, base=5): 
  #dq table 2 suppression - 0 needs to be replaced with "-"
    if ((x < 5) & (x != 0)):
      return '*'
    elif x== 999999999:
      return '*'
    elif x== 0:
      return '-'  
    else:
        return str(int(base * round(float(x)/base)))

# COMMAND ----------

 %sql
 --- calculate the actual counts for each table
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW TableCounts AS
 (
   SELECT
     'MHS001MPI' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs001mpi
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS002GP' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs002gp
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS003AccommStatus' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs003accommstatus
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS004EmpStatus' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs004empstatus
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS005PatInd' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs005patind
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS006MHCareCoord' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs006mhcarecoord
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS007DisabilityType' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs007disabilitytype
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS008CarePlanType' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs008careplantype
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS009CarePlanAgreement' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs009careplanagreement
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS010AssTechToSupportDisTyp' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs010asstechtosupportdistyp
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS011SocPerCircumstances' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs011socpercircumstances
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS012OverseasVisitorChargCat' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs012overseasvisitorchargcat
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS101Referral' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs101referral
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS102ServiceTypeReferredTo' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs102servicetypereferredto
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS103OtherReasonReferral' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs103otherreasonreferral
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS104RTT' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs104rtt
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS105OnwardReferral' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs105onwardreferral
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS106DischargePlanAgreement' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs106dischargeplanagreement
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS107MedicationPrescription' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs107medicationprescription
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS201CareContact' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs201carecontact
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS202CareActivity' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs202careactivity
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS203OtherAttend' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs203otherattend
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS204IndirectActivity' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs204indirectactivity
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS301GroupSession' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs301groupsession
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS401MHActPeriod' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs401mhactperiod
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS402RespClinicianAssignPeriod' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.MHS402RespClinicianAssignPeriod
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS403ConditionalDischarge' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs403conditionaldischarge
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS404CommTreatOrder' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs404commtreatorder
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS405CommTreatOrderRecall' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs405commtreatorderrecall
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS501HospProvSpell' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs501hospprovspell
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS502WardStay' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs502wardstay
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS503AssignedCareProf' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs503assignedcareprof
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS504DelayedDischarge' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs504delayeddischarge
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS505RestrictiveInterventInc' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.MHS505RestrictiveInterventInc
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS515RestrictiveInterventType' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.MHS515RestrictiveInterventType
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS516PoliceAssistanceRequest' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.MHS516PoliceAssistanceRequest
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS506Assault' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs506assault
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS507SelfHarm' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs507selfharm
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS509HomeLeave' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs509homeleave
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS510LeaveOfAbsence' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs510leaveofabsence
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS511AbsenceWithoutLeave' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs511absencewithoutleave
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS512HospSpellCommAssPer' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs512hospspellcommassper
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS513SubstanceMisuse' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs513substancemisuse
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS514TrialLeave' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs514trialleave
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS517SMHExceptionalPackOfCare' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.MHS517SMHExceptionalPackOfCare
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS601MedHistPrevDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs601medhistprevdiag
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS603ProvDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs603provdiag
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS604PrimDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs604primdiag
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS605SecDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs605secdiag
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS606CodedScoreAssessmentRefer' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs606codedscoreassessmentrefer
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS607CodedScoreAssessmentAct' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs607codedscoreassessmentact
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL
   SELECT
     'MHS608AnonSelfAssess' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs608anonselfassess
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS701CPACareEpisode' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs701cpacareepisode
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS702CPAReview' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs702cpareview
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS801ClusterTool' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs801clustertool
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS802ClusterAssess' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs802clusterassess
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS803CareCluster' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs803carecluster
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS804FiveForensicPathways' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs804fiveforensicpathways
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS901StaffDetails' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $db_source.mhs901staffdetails
   WHERE UniqMonthID between $month_id_start and $month_id_end
   GROUP BY UniqMonthID, OrgIDProv
 )

# COMMAND ----------

 %sql
 ---define user-friendly table names
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW TableNames AS
 (
   SELECT 'MHS001MPI' AS TableName
   UNION ALL SELECT 'MHS002GP' AS TableName
   UNION ALL SELECT 'MHS003AccommStatus' AS TableName
   UNION ALL SELECT 'MHS004EmpStatus' AS TableName
   UNION ALL SELECT 'MHS005PatInd' AS TableName
   UNION ALL SELECT 'MHS006MHCareCoord' AS TableName
   UNION ALL SELECT 'MHS007DisabilityType' AS TableName
   UNION ALL SELECT 'MHS008CarePlanType' AS TableName
   UNION ALL SELECT 'MHS009CarePlanAgreement' AS TableName
   UNION ALL SELECT 'MHS010AssTechToSupportDisTyp' AS TableName
   UNION ALL SELECT 'MHS011SocPerCircumstances' AS TableName
   UNION ALL SELECT 'MHS012OverseasVisitorChargCat' AS TableName
   UNION ALL SELECT 'MHS101Referral' AS TableName
   UNION ALL SELECT 'MHS102ServiceTypeReferredTo' AS TableName
   UNION ALL SELECT 'MHS103OtherReasonReferral' AS TableName
   UNION ALL SELECT 'MHS104RTT' AS TableName
   UNION ALL SELECT 'MHS105OnwardReferral' AS TableName
   UNION ALL SELECT 'MHS106DischargePlanAgreement' AS TableName
   UNION ALL SELECT 'MHS107MedicationPrescription' AS TableName
   UNION ALL SELECT 'MHS201CareContact' AS TableName
   UNION ALL SELECT 'MHS202CareActivity' AS TableName
   UNION ALL SELECT 'MHS203OtherAttend' AS TableName
   UNION ALL SELECT 'MHS204IndirectActivity' AS TableName
   UNION ALL SELECT 'MHS301GroupSession' AS TableName
   UNION ALL SELECT 'MHS401MHActPeriod' AS TableName
   UNION ALL SELECT 'MHS402RespClinicianAssignment' AS TableName
   UNION ALL SELECT 'MHS403ConditionalDischarge' AS TableName
   UNION ALL SELECT 'MHS404CommTreatOrder' AS TableName
   UNION ALL SELECT 'MHS405CommTreatOrderRecall' AS TableName
   UNION ALL SELECT 'MHS501HospProvSpell' AS TableName
   UNION ALL SELECT 'MHS502WardStay' AS TableName
   UNION ALL SELECT 'MHS503AssignedCareProf' AS TableName
   UNION ALL SELECT 'MHS504DelayedDischarge' AS TableName
   UNION ALL SELECT 'MHS505RestrictiveIntervention' AS TableName
   UNION ALL SELECT 'MHS506Assault' AS TableName
   UNION ALL SELECT 'MHS507SelfHarm' AS TableName
   UNION ALL SELECT 'MHS509HomeLeave' AS TableName
   UNION ALL SELECT 'MHS510LeaveOfAbsence' AS TableName
   UNION ALL SELECT 'MHS511AbsenceWithoutLeave' AS TableName
   UNION ALL SELECT 'MHS512HospSpellComm' AS TableName
   UNION ALL SELECT 'MHS513SubstanceMisuse' AS TableName
   UNION ALL SELECT 'MHS514TrialLeave' AS TableName
   UNION ALL SELECT 'MHS601MedHistPrevDiag' AS TableName
   UNION ALL SELECT 'MHS603ProvDiag' AS TableName
   UNION ALL SELECT 'MHS604PrimDiag' AS TableName
   UNION ALL SELECT 'MHS605SecDiag' AS TableName
   UNION ALL SELECT 'MHS606CodedScoreAssessmentRefer' AS TableName
   UNION ALL SELECT 'MHS607CodedScoreAssessmentCont' AS TableName
   UNION ALL SELECT 'MHS608AnonSelfAssess' AS TableName
   UNION ALL SELECT 'MHS701CPACareEpisode' AS TableName
   UNION ALL SELECT 'MHS702CPAReview' AS TableName
   UNION ALL SELECT 'MHS801ClusterTool' AS TableName
   UNION ALL SELECT 'MHS802ClusterAssess' AS TableName
   UNION ALL SELECT 'MHS803CareCluster' AS TableName
   UNION ALL SELECT 'MHS804FiveForensicPathways' AS TableName
   UNION ALL SELECT 'MHS901StaffDetails' AS TableName
 )

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Table1Ref AS
 ---get reference table of each provider and monthly information in the financial year
 Select distinct a.orgidprovider, a.uniqmonthid, a.reportingperiodstartdate, a.reportingperiodenddate, b.tableName
 from $db_source.mhs000header a
 cross join global_temp.TableNames b
 where uniqmonthid between $month_id_start and $month_id_end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T1_Full;
 CREATE TABLE         $db_output.DQ_T1_Full USING DELTA AS
 ---final dq table 1 submission prep table before suppression and pivoting
 SELECT
   ot.ReportingPeriodStartDate AS REPORTING_PERIOD_START,
   date_format(ot.ReportingPeriodStartDate, 'MMMMM') AS MONTH,
   (CASE WHEN ot.OrgIdProvider IS NULL THEN 'England' ELSE ot.OrgIdProvider END) AS ORGANISATION_CODE,
   (CASE WHEN COALESCE(od.NAME, pre_od.NAME) IS NULL THEN 'England' ELSE COALESCE(od.NAME, pre_od.NAME) END) AS ORGANISATION_NAME,
   (CASE WHEN ot.TableName IS NULL THEN 'Any' ELSE ot.TableName END) AS TABLE_NAME,
   COALESCE(SUM(t.RowCount), '*') AS COVERAGE_SUM
 FROM global_temp.Table1Ref ot
 LEFT JOIN $db_output.mhb_org_daily as od
            ON COALESCE(ot.orgidprovider, 'UNKNOWN') = COALESCE(od.ORG_CODE, 'UNKNOWN') 
            AND od.BUSINESS_END_DATE IS NULL
            AND od.ORG_OPEN_DATE <= '$rp_enddate'
            AND ((od.ORG_CLOSE_DATE >= '$rp_enddate') OR od.ORG_CLOSE_DATE is NULL)
 LEFT JOIN $db_output.mhb_org_daily pre_od on COALESCE(ot.orgidprovider, 'UNKNOWN') = COALESCE(pre_od.ORG_CODE, 'UNKNOWN')           
   LEFT OUTER JOIN global_temp.TableCounts t ON ot.OrgIDProvider = t.OrgIDProv 
                                    AND ot.TableName = t.TableName AND ot.uniqmonthid = t.uniqmonthid
 -- Add total groups to result set
 GROUP BY
   GROUPING SETS
   (
     (ot.ReportingPeriodStartDate),
     (ot.TableName, ot.ReportingPeriodStartDate),
     (ot.OrgIDProvider, COALESCE(od.NAME, pre_od.NAME), ot.ReportingPeriodStartDate),
     (ot.OrgIDProvider, COALESCE(od.NAME, pre_od.NAME), ot.TableName, ot.ReportingPeriodStartDate)
   )
 ORDER BY ORGANISATION_NAME, REPORTING_PERIOD_START, TABLE_NAME

# COMMAND ----------

# DBTITLE 1,Checking no duplicate codes with different names
 %sql
 select distinct ORGANISATION_CODE, COUNT(DISTINCT ORGANISATION_NAME)
 from $db_output.DQ_T1_Full
 group by ORGANISATION_CODE
 having COUNT(DISTINCT ORGANISATION_NAME) > 1

# COMMAND ----------

#pivot data
data = sqlContext.sql(""" select CASE WHEN ORGANISATION_CODE='England' THEN 1 ELSE 2 END AS ORDER1,
                                  CASE WHEN TABLE_NAME='Any' THEN 1 ELSE 2 END AS ORDER2,
                                  REPORTING_PERIOD_START,
                                  MONTH,
                                  ORGANISATION_CODE,
                                  ORGANISATION_NAME,
                                  CASE WHEN TABLE_NAME='Any' THEN ORGANISATION_NAME ELSE TABLE_NAME END AS TABLE_NAME,
                                  CASE WHEN ORGANISATION_CODE='England' THEN CAST(COVERAGE_SUM AS FLOAT)
                                       WHEN CAST(COVERAGE_SUM AS FLOAT)<5 THEN 999999999
                                       ELSE ROUND(CAST(COVERAGE_SUM AS FLOAT)/5,0)*5 END AS COVERAGE_SUM
                          from """+db_output+""".DQ_T1_Full
                          order by CASE WHEN ORGANISATION_CODE='England' THEN '0' ELSE ORGANISATION_NAME END,
                                    REPORTING_PERIOD_START,
                                    ORDER1,
                                    TABLE_NAME""")

pivot = data.groupby("ORDER1","ORDER2","ORGANISATION_CODE","ORGANISATION_NAME", "TABLE_NAME").pivot("REPORTING_PERIOD_START").sum("COVERAGE_SUM").sort(["ORDER1","ORGANISATION_NAME","ORDER2","TABLE_NAME"])
dq1_prov = pivot.drop("ORDER1","ORDER2","ORGANISATION_NAME").collect()
dq1_prov = spark.createDataFrame(dq1_prov)
display(dq1_prov)

# COMMAND ----------

#apply suppression at provider level to mhsds table submissions in each month of the financial year
dq1_prov_noneng = dq1_prov.filter(dq1_prov.ORGANISATION_CODE!="England").collect()
dq1_prov_eng = dq1_prov.filter(dq1_prov.ORGANISATION_CODE=="England").collect()

dq1_prov_noneng = spark.createDataFrame(dq1_prov_noneng).toPandas()
dq1_prov_eng = spark.createDataFrame(dq1_prov_eng).toPandas()

for i in dq1_prov_noneng.columns[2:14]:
  dq1_prov_noneng[i] = dq1_prov_noneng[i].fillna(999999999)
  dq1_prov_noneng[i] = dq1_prov_noneng[i].apply(suppression)
  
for i in dq1_prov_eng.columns[2:14]:
  dq1_prov_eng[i] = dq1_prov_eng[i].astype('str')
  dq1_prov_eng[i] = dq1_prov_eng[i].replace("nan", "0")

dq1_prov_2 = dq1_prov_eng.append(dq1_prov_noneng)
dq1_prov = spark.createDataFrame(dq1_prov_2)
dq1_prov.write.mode('overwrite').saveAsTable(f"{db_output}.mhb_dq_table1_mhsds_table_submissions")

# COMMAND ----------

#dq table 1 - number of providers submitting in each month of the financial year
data = sqlContext.sql(""" select distinct 'Number of providers submitting' AS TITLE,
                                   h.ReportingPeriodStartDate,
                                   --date_format(h.ReportingPeriodStartDate, 'MMMMM YYYY') AS MONTH,
                                   count(distinct m.orgidprov) as providers_submitting
                            from """+dbutils.widgets.get("db_source")+""".mhs001mpi m
                            left join """+dbutils.widgets.get("db_source")+""".mhs000header h on h.uniqmonthid = m.uniqmonthid
                            where m.uniqmonthid between """+dbutils.widgets.get("month_id_start")+""" and """+dbutils.widgets.get("month_id_end")+"""
                            group by h.ReportingPeriodStartDate
                            order by h.ReportingPeriodStartDate""")

dq1_countprov = data.groupby("TITLE").pivot("ReportingPeriodStartDate").sum("providers_submitting")
dq1_countprov.write.mode('overwrite').saveAsTable(f"{db_output}.mhb_dq_table1_providers_submitting")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T2_Ref;
 CREATE TABLE         $db_output.DQ_T2_Ref
 ---reference data for dq table 2 (submissions and people comparison between current and previous financial year)
 select   OrgIDProvider,
          h_all.UniqMonthID, 
          h_all.ReportingPeriodStartDate,
          h_all.ReportingPeriodEndDate
 from (select distinct OrgIDProvider, 'a' as tag from $db_source.mhs000header where uniqmonthid between $month_id_start and $month_id_end) h
 CROSS JOIN (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate, 'a' as tag from $db_source.mhs000header where uniqmonthid between $month_id_start-12 and $month_id_end) h_all on h.tag = h_all.tag

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T2_Full;
 CREATE TABLE         $db_output.DQ_T2_Full
 select  h.orgidprovider AS ORGANISATION_CODE,
         COALESCE(od.Name, pre_od.Name) AS ORGANISATION_NAME,
          CASE WHEN MONTH(h.ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END AS YEAR,
          CASE WHEN h.ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               END AS ReportingPeriodStartDate,
          count (distinct r.person_id) AS COUNT
 from $db_output.DQ_T2_Ref h
 left join $db_source.mhs101referral r on h.orgidprovider = r.orgidprov 
                                 and h.uniqmonthid = r.uniqmonthid
                                 and (r.ServDischDate IS NULL OR r.ServDischDate > h.ReportingPeriodEndDate)
                                 and r.ReferralRequestReceivedDate <= h.ReportingPeriodEndDate
 left join $db_output.mhb_org_daily od 
                   ON h.orgidprovider = od.ORG_CODE 
                   AND ORG_OPEN_DATE <= '$rp_enddate'
                   AND ((ORG_CLOSE_DATE >= '$rp_enddate') OR ORG_CLOSE_DATE is NULL)
                   AND BUSINESS_END_DATE IS NULL
 left join $db_output.mhb_org_daily pre_od on h.orgidprovider = pre_od.ORG_CODE
 GROUP BY 
 h.orgidprovider,
 COALESCE(od.Name, pre_od.Name),
 CASE WHEN MONTH(h.ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END,
 CASE WHEN h.ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               END
 ORDER BY ORGANISATION_NAME, ReportingPeriodStartDate   

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T2_ProvCount;
 CREATE TABLE         $db_output.DQ_T2_ProvCount USING DELTA AS
 ---number of submitting providers for current and previous financial year
 select OrgIDProvider as ORGANISATION_CODE,       
        CASE WHEN MONTH(ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END AS YEAR,
          CASE WHEN ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(ReportingPeriodStartDate)),2),'-01')
               END AS ReportingPeriodStartDate,
        COUNT(DISTINCT OrgIDProvider) AS Providers
 from $db_source.mhs000header
 where uniqmonthid between $month_id_start-12 and $month_id_end
 group by OrgIDProvider,
       CASE WHEN MONTH(ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END,
        CASE WHEN ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(ReportingPeriodStartDate)),2),'-01')
               END

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T2_National;
 CREATE TABLE         $db_output.DQ_T2_National AS
 ---national figures for number of people in contact with mental health services in the current and previous financial year
 select CASE WHEN MONTH(h.ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END AS YEAR,
          CASE WHEN h.ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               END AS ReportingPeriodStartDate,
  count (distinct r.person_id) AS COUNT
 from (select distinct uniqmonthid, ReportingPeriodStartDate, ReportingPeriodEndDate from $db_source.mhs000header) h
 left join $db_source.mhs101referral r on h.uniqmonthid = r.uniqmonthid
                                 and (r.ServDischDate IS NULL OR r.ServDischDate > h.ReportingPeriodEndDate)
                                 and r.ReferralRequestReceivedDate <= h.ReportingPeriodEndDate 
 where h.uniqmonthid between $month_id_start-12 and $month_id_end                                
 GROUP BY CASE WHEN MONTH(h.ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END,
          CASE WHEN h.ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               END                                

# COMMAND ----------

data = sqlContext.sql(f"select * from {db_output}.DQ_T2_Full")
data2 = sqlContext.sql(f"select * from {db_output}.DQ_T2_ProvCount")
data3 = sqlContext.sql(f"select * from {db_output}.DQ_T2_National")

dq2_patients = data.groupby("ORGANISATION_CODE","ORGANISATION_NAME","YEAR").pivot("ReportingPeriodStartDate").sum("COUNT").sort("ORGANISATION_NAME","YEAR")
dq2_provcount = data2.groupby("YEAR").pivot("ReportingPeriodStartDate").sum("Providers").sort("YEAR")
dq2_national = data3.groupby("YEAR").pivot("ReportingPeriodStartDate").sum("Count").sort("YEAR")

# COMMAND ----------

#apply Suppression to dq2_patients
dq2_patients_2 = dq2_patients.toPandas()

for i in dq2_patients_2.columns[3:15]:
  dq2_patients_2[i] = dq2_patients_2[i].fillna(999999999)
  dq2_patients_2[i] = dq2_patients_2[i].apply(tb2_suppression)

dq2_patients = spark.createDataFrame(dq2_patients_2)

# COMMAND ----------

 %sql
 --icurrent reporting year providers
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Providers_cur AS
 Select a.orgidprov, count(distinct a.person_id) as count
 from $db_source.mhs001mpi a
 inner join $db_source.mhs101referral b on a.person_id = b.person_id and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprov
 inner join 
 (
 select distinct orgidprovider from $db_source.mhs000header 
 where uniqmonthid between $month_id_start and $month_id_end
 group by orgidprovider
 having count(distinct uniqmonthid) = 12
 ) c on a.orgidprov = c.orgidprovider
 and a.uniqmonthid between $month_id_start and $month_id_end
 group by a.orgidprov

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Providers_pre AS
 ---previous reporting year providers
 Select a.orgidprov, count(distinct a.person_id) as count
 from $db_source.mhs001mpi a
 inner join $db_source.mhs101referral b on a.person_id = b.person_id and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprov
 inner join 
 (
 select distinct orgidprovider from $db_source.mhs000header 
 where uniqmonthid between $month_id_start-12 and $pre_month_id_end
 group by orgidprovider
 having count(distinct uniqmonthid) = 12
 ) c on a.orgidprov = c.orgidprovider
 and a.uniqmonthid between $month_id_start-12 and $pre_month_id_end
 group by a.orgidprov

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T3_Full;
 CREATE TABLE         $db_output.DQ_T3_Full USING DELTA AS
 
 Select  case when a.orgidprov is null then b.orgidprov else a.orgidprov end as Provider,
         coalesce(od.name, od2.name) as Provider_Name,
         b.count as prev_year_count,
         a.count as cur_year_count
 from global_temp.Providers_cur a
 full outer join global_temp.Providers_pre b on a.orgidprov = b.orgidprov
 LEFT JOIN $db_output.mhb_org_daily as od
    ON case when a.orgidprov is null then b.orgidprov else a.orgidprov end = od.ORG_CODE AND od.BUSINESS_END_DATE IS NULL
            AND od.ORG_OPEN_DATE <= '$rp_enddate'
            AND ((od.ORG_CLOSE_DATE >= '$rp_enddate') OR od.ORG_CLOSE_DATE is NULL)
 LEFT JOIN $db_output.mhb_org_daily as od2 ON case when a.orgidprov is null then b.orgidprov else a.orgidprov end = od2.ORG_CODE           
 order by CASE WHEN b.count is null THEN 1 ELSE 0 END, case when a.orgidprov is null then b.orgidprov else a.orgidprov end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T3_National;
 CREATE TABLE         $db_output.DQ_T3_National USING DELTA AS
 select SUM(prev_year_count) AS prev_year_count,
        SUM(cur_year_count) as cur_year_count
 from $db_output.DQ_T3_Full

# COMMAND ----------

dq3 = sqlContext.sql(""" select  * from """+dbutils.widgets.get("db_output")+""".DQ_T3_Full order by CASE WHEN prev_year_count is null THEN 1 ELSE 0 END, provider""")
dq3_total = sqlContext.sql(""" select  * from """+dbutils.widgets.get("db_output")+""".DQ_T3_National""")

# COMMAND ----------

#create excel report variables
SRP_Year = pd.to_datetime(dbutils.widgets.get("rp_startdate")).date().strftime("%Y")
ERP_Year = pd.to_datetime(dbutils.widgets.get("rp_enddate")).date().strftime("%Y")
RptPeriod = f'Reporting Period {SRP_Year} - {ERP_Year}'
# Unique filename created using timestamp 
filename = f'MHB_{SRP_Year}_{ERP_Year}_DQTables.xlsx'

# COMMAND ----------

from dsp.common.exports import create_excel_for_download
from dsp.common.digitrials import cover_page, interpretation_page, footnotes_page, add_digitrials_styling
#create single file
# '''
# In cover_page section, you can change sheet name, applicant name, organisation, ref number, title and table title as appropriate
# In interpretation_page section, you can change sheet name and add as many tables as you want
# '''

pre_pages = [{
 'function': cover_page,
 'sheet_name': 'NHS Digital',
 'options': {
 'applicant_name': 'MHB DQ Tables',
 'organisation': 'NHS Digital',
 'ref_number': RptPeriod,
 'title': 'MHB DQ Tables ' + SRP_Year +'_' + ERP_Year ,
 'table_title': 'Data to produce Mental Heatlh Bulletin DQ Tables for '  + SRP_Year +'_' + ERP_Year
 }},
 {
 'function': interpretation_page,
 'sheet_name': 'MHB DQ Tables',
 'options': {
 'organisation': 'NHS Digital',
 'tables': [
   {'name': 'Sheet1 - Mental Health Bulletin DQ Tables - DQ1 Count of Providers', 'interpretation': 'Sheet1 - Extraction Data for Table 1 Row 27 ' + SRP_Year +'_' + ERP_Year },
   {'name': 'Sheet2 - Mental Health Bulletin DQ Tables - DQ1 Providers Data', 'interpretation': 'Sheet2 - Extraction Data for Table 1 Row 29 ' + SRP_Year +'_' + ERP_Year },
   {'name': 'Sheet3 - Mental Health Bulletin DQ Tables - DQ2 Patients', 'interpretation': 'Sheet3 - Extraction Data for Table 2 Row 28 ' + SRP_Year +'_' + ERP_Year },
   {'name': 'Sheet4 - Mental Health Bulletin DQ Tables - DQ2 Provider Counts', 'interpretation': 'Sheet3 - Extraction Data for Table 2 Row 21 ' + SRP_Year +'_' + ERP_Year },
   {'name': 'Sheet5 - Mental Health Bulletin DQ Tables - DQ2 National', 'interpretation': 'Sheet3 - Extraction Data for Table 2 Row 25 ' + SRP_Year +'_' + ERP_Year },
   {'name': 'Sheet6 - Mental Health Bulletin DQ Tables - DQ3', 'interpretation': 'Sheet4 - Extraction Data for Table 4 Row 31 ' + SRP_Year +'_' + ERP_Year},
   {'name': 'Sheet7 - Mental Health Bulletin DQ Tables - DQ3 National', 'interpretation': 'Sheet4 - Extraction Data for Table 4 Row 28 ' + SRP_Year +'_' + ERP_Year}
           ]
 }}
]
# '''
# In footnotes_page section, you can customise sheet_name, and add as many footnotes as you want
# '''
post_pages = [{
 'function': footnotes_page,
 'sheet_name': 'Footnotes',
 'options': {
 'footnotes': [
   {'title': 'MHB DQ Tables', 'content': 'This Explains what each criteria is: '}
 ]}
}]
# '''
# You can add multiple dataframes in the dataframes section. Each dataframe gets outputted to a separate sheet
# '''
data = create_excel_for_download(dataframes=[dq1_countprov, dq1_prov, dq2_patients, dq2_provcount, dq2_national, dq3, dq3_total],
 pre_pages=pre_pages,
 post_pages=post_pages,
 style_function=add_digitrials_styling,
 max_mb=20
 )

displayHTML(f"<h4>{filename} is ready to <a href='data:text/csv;base64,{data.decode()}' download='{filename}'>download</a></h4>")