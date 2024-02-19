# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

startchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodStartDate from mh_v5_pre_pseudo_d1.mhs000header order by ReportingPeriodStartDate").collect()]
endchoices = [str(r[0]) for r in spark.sql("select distinct ReportingPeriodEndDate from mh_v5_pre_pseudo_d1.mhs000header order by ReportingPeriodEndDate").collect()]
monthid = [str(r[0]) for r in spark.sql("select distinct Uniqmonthid from mh_v5_pre_pseudo_d1.mhs000header order by Uniqmonthid").collect()]

dbutils.widgets.dropdown("rp_startdate", "2019-04-01", startchoices)
dbutils.widgets.dropdown("rp_enddate", "2020-03-31", endchoices)
dbutils.widgets.dropdown("start_month_id", "1429", monthid)
dbutils.widgets.dropdown("end_month_id", "1440", monthid)
dbutils.widgets.dropdown("pre_start_month_id", "1417", monthid)
dbutils.widgets.dropdown("pre_end_month_id", "1428", monthid)
dbutils.widgets.text("db_output","personal_db")
dbutils.widgets.text("dbm","mh_pre_pseudo_d1")
#mh_pre_pseudo_d1
# dbutils.widgets.text("status","Final")

# COMMAND ----------

# DBTITLE 1,Number of submitters per month in FY - Table in CMS
 %sql
 select ReportingPeriodStartDate, count(distinct OrgIDProvider) from $dbm.mhs000header 
 where UniqMonthID between $start_month_id and $end_month_id
 group by ReportingPeriodStartDate
 order by ReportingPeriodStartDate

# COMMAND ----------

# DBTITLE 1,Define Suppression Function
def suppression(x, base=5):   
    if x < 5:
        return '*'
    elif x== 999999999:
        return '*'
    else:
        return str(int(base * round(float(x)/base)))

# COMMAND ----------

# DBTITLE 1,Define Table 2 Suppression - 0 needs to be replaced with "-"
def tb2_suppression(x, base=5): 
    if ((x < 5) & (x != 0)):
      return '*'
    elif x== 999999999:
      return '*'
    elif x== 0:
      return '-'  
    else:
        return str(int(base * round(float(x)/base)))

# COMMAND ----------

# DBTITLE 1,DQ Table 1
 %sql
 -- Calculate the actual counts for each table
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW TableCounts AS
 (
   SELECT
     'MHS001MPI' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs001mpi
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS002GP' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs002gp
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS003AccommStatus' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs003accommstatus
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS004EmpStatus' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs004empstatus
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS005PatInd' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs005patind
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS006MHCareCoord' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs006mhcarecoord
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS007DisabilityType' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs007disabilitytype
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS008CarePlanType' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs008careplantype
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS009CarePlanAgreement' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs009careplanagreement
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS010AssTechToSupportDisTyp' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs010asstechtosupportdistyp
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS011SocPerCircumstances' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs011socpercircumstances
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS012OverseasVisitorChargCat' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs012overseasvisitorchargcat
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS101Referral' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs101referral
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS102ServiceTypeReferredTo' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs102servicetypereferredto
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS103OtherReasonReferral' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs103otherreasonreferral
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS104RTT' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs104rtt
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS105OnwardReferral' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs105onwardreferral
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS106DischargePlanAgreement' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs106dischargeplanagreement
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS107MedicationPrescription' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs107medicationprescription
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS201CareContact' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs201carecontact
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS202CareActivity' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs202careactivity
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS203OtherAttend' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs203otherattend
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS204IndirectActivity' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs204indirectactivity
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS301GroupSession' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs301groupsession
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS401MHActPeriod' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs401mhactperiod
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS402RespClinicianAssignPeriod' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS402RespClinicianAssignPeriod
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS403ConditionalDischarge' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs403conditionaldischarge
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS404CommTreatOrder' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs404commtreatorder
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS405CommTreatOrderRecall' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs405commtreatorderrecall
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS501HospProvSpell' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs501hospprovspell
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS502WardStay' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs502wardstay
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS503AssignedCareProf' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs503assignedcareprof
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS504DelayedDischarge' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs504delayeddischarge
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS505RestrictiveInterventInc' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS505RestrictiveInterventInc
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS515RestrictiveInterventType' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS515RestrictiveInterventType
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS516PoliceAssistanceRequest' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS516PoliceAssistanceRequest
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS506Assault' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs506assault
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS507SelfHarm' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs507selfharm
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS509HomeLeave' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs509homeleave
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS510LeaveOfAbsence' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs510leaveofabsence
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS511AbsenceWithoutLeave' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs511absencewithoutleave
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS512HospSpellCommAssPer' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs512hospspellcommassper
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS513SubstanceMisuse' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs513substancemisuse
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS514TrialLeave' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs514trialleave
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS517SMHExceptionalPackOfCare' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.MHS517SMHExceptionalPackOfCare
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS601MedHistPrevDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs601medhistprevdiag
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS603ProvDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs603provdiag
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS604PrimDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs604primdiag
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS605SecDiag' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs605secdiag
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS606CodedScoreAssessmentRefer' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs606codedscoreassessmentrefer
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS607CodedScoreAssessmentAct' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs607codedscoreassessmentact
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL
   SELECT
     'MHS608AnonSelfAssess' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs608anonselfassess
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS701CPACareEpisode' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs701cpacareepisode
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS702CPAReview' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs702cpareview
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS801ClusterTool' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs801clustertool
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS802ClusterAssess' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs802clusterassess
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS803CareCluster' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs803carecluster
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS804FiveForensicPathways' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs804fiveforensicpathways
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
   UNION ALL SELECT
     'MHS901StaffDetails' AS TableName,
     UniqMonthID,
     OrgIDProv,
     COUNT(*) AS RowCount
   FROM $dbm.mhs901staffdetails
   WHERE UniqMonthID between $start_month_id and $end_month_id
   GROUP BY UniqMonthID, OrgIDProv
 )

# COMMAND ----------

# DBTITLE 1,Create reference data for DQ T1 part1
 %sql
 
 -- Define user-friendly table names
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
 Select distinct a.orgidprovider, a.uniqmonthid, a.reportingperiodstartdate, a.reportingperiodenddate, b.tableName
 from $dbm.mhs000header a
 cross join global_temp.TableNames b
 where uniqmonthid between $start_month_id and $end_month_id

# COMMAND ----------

# DBTITLE 1,DQ Table 1 Final Main Section - Download this output
 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T1_Full;
 CREATE TABLE         $db_output.DQ_T1_Full USING DELTA AS
 
 SELECT
   ot.ReportingPeriodStartDate AS REPORTING_PERIOD_START,
 --   ot.ReportingPeriodEndDate AS REPORTING_PERIOD_END,
   date_format(ot.ReportingPeriodStartDate, 'MMMMM') AS MONTH,
   (CASE WHEN ot.OrgIdProvider IS NULL THEN 'England' ELSE ot.OrgIdProvider END) AS ORGANISATION_CODE,
   (CASE WHEN COALESCE(od.NAME, pre_od.NAME) IS NULL THEN 'England' ELSE COALESCE(od.NAME, pre_od.NAME) END) AS ORGANISATION_NAME,
   (CASE WHEN ot.TableName IS NULL THEN 'Any' ELSE ot.TableName END) AS TABLE_NAME,
   COALESCE(SUM(t.RowCount), '*') AS COVERAGE_SUM
 FROM global_temp.Table1Ref ot
 LEFT JOIN $db_output.MHB_ORG_DAILY as od
            ON COALESCE(ot.orgidprovider, 'UNKNOWN') = COALESCE(od.ORG_CODE, 'UNKNOWN') 
            AND od.BUSINESS_END_DATE IS NULL
            AND od.ORG_OPEN_DATE <= '$rp_enddate'
            AND ((od.ORG_CLOSE_DATE >= '$rp_enddate') OR od.ORG_CLOSE_DATE is NULL)
 LEFT JOIN $db_output.MHB_ORG_DAILY pre_od on COALESCE(ot.orgidprovider, 'UNKNOWN') = COALESCE(pre_od.ORG_CODE, 'UNKNOWN')           
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

import pandas as pd
import numpy as np

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
                          from """+dbutils.widgets.get("db_output")+""".DQ_T1_Full
                          order by CASE WHEN ORGANISATION_CODE='England' THEN '0' ELSE ORGANISATION_NAME END,
                                    REPORTING_PERIOD_START,
                                    ORDER1,
                                    TABLE_NAME""")

# pivot = data.groupby("ORGANISATION_NAME", "TABLE_NAME").pivot("REPORTING_PERIOD_START","MONTH").sum("COVERAGE_SUM").sort(["ORGANISATION_NAME","TABLE_NAME"])
pivot = data.groupby("ORDER1","ORDER2","ORGANISATION_CODE","ORGANISATION_NAME", "TABLE_NAME").pivot("REPORTING_PERIOD_START").sum("COVERAGE_SUM").sort(["ORDER1","ORGANISATION_NAME","ORDER2","TABLE_NAME"])
dq1_prov = pivot.drop("ORDER1","ORDER2","ORGANISATION_NAME").collect()
dq1_prov = spark.createDataFrame(dq1_prov)
display(dq1_prov)

# COMMAND ----------

# DBTITLE 1,Apply Suppression to dq1_prov
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
display(dq1_prov_2)


# COMMAND ----------

# DBTITLE 1,DQ Table 1 Final Number of Providers submitting - Download this output
data = sqlContext.sql(""" select distinct 'Number of providers submitting' AS TITLE,
                                   h.ReportingPeriodStartDate,
                                   --date_format(h.ReportingPeriodStartDate, 'MMMMM YYYY') AS MONTH,
                                   count(distinct m.orgidprov) as providers_submitting
                            from """+dbutils.widgets.get("dbm")+""".mhs001mpi m
                            left join """+dbutils.widgets.get("dbm")+""".mhs000header h on h.uniqmonthid = m.uniqmonthid
                            where m.uniqmonthid between """+dbutils.widgets.get("start_month_id")+""" and """+dbutils.widgets.get("end_month_id")+"""
                            group by h.ReportingPeriodStartDate
                            order by h.ReportingPeriodStartDate""")

dq1_countprov = data.groupby("TITLE").pivot("ReportingPeriodStartDate").sum("providers_submitting")
display(dq1_countprov)

# COMMAND ----------

# DBTITLE 1,Ref data for DQ Table 2
 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T2_Ref;
 CREATE TABLE         $db_output.DQ_T2_Ref
 select   OrgIDProvider,
          h_all.UniqMonthID, 
          h_all.ReportingPeriodStartDate,
          h_all.ReportingPeriodEndDate
 from (select distinct OrgIDProvider, 'a' as tag from $dbm.mhs000header where uniqmonthid between $start_month_id and $end_month_id) h
 CROSS JOIN (select distinct UniqMonthID, ReportingPeriodStartDate, ReportingPeriodEndDate, 'a' as tag from $dbm.mhs000header where uniqmonthid between $pre_start_month_id and $end_month_id) h_all on h.tag = h_all.tag

# COMMAND ----------

# DBTITLE 1,DQ Table 2 Full (New)
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
 left join $dbm.mhs101referral r on h.orgidprovider = r.orgidprov 
                                 and h.uniqmonthid = r.uniqmonthid
                                 and (r.ServDischDate IS NULL OR r.ServDischDate > h.ReportingPeriodEndDate)
                                 and r.ReferralRequestReceivedDate <= h.ReportingPeriodEndDate
 left join $db_output.MHB_ORG_DAILY od 
                   ON h.orgidprovider = od.ORG_CODE 
                   AND ORG_OPEN_DATE <= '$rp_enddate'
                   AND ((ORG_CLOSE_DATE >= '$rp_enddate') OR ORG_CLOSE_DATE is NULL)
                   AND BUSINESS_END_DATE IS NULL
 left join $db_output.MHB_ORG_DAILY pre_od on h.orgidprovider = pre_od.ORG_CODE
 -- WHERE COALESCE(od.Name, pre_od.Name) is not null
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

# DBTITLE 1,DQ Table 2 - Number of Submitting Providers for current and previous financial year
 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T2_ProvCount;
 CREATE TABLE         $db_output.DQ_T2_ProvCount USING DELTA AS
 
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
 from $dbm.mhs000header
 where uniqmonthid between $pre_start_month_id and $end_month_id
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

# DBTITLE 1,DQ Table 2 National level figures
 %sql
 DROP TABLE IF EXISTS $db_output.DQ_T2_National;
 CREATE TABLE         $db_output.DQ_T2_National AS
 
 select CASE WHEN MONTH(h.ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END AS YEAR,
          CASE WHEN h.ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               END AS ReportingPeriodStartDate,
  count (distinct r.person_id) AS COUNT
 from (select distinct uniqmonthid, ReportingPeriodStartDate, ReportingPeriodEndDate from $dbm.mhs000header) h
 left join $dbm.mhs101referral r on h.uniqmonthid = r.uniqmonthid
                                 and (r.ServDischDate IS NULL OR r.ServDischDate > h.ReportingPeriodEndDate)
                                 and r.ReferralRequestReceivedDate <= h.ReportingPeriodEndDate 
 where h.uniqmonthid between $pre_start_month_id and $end_month_id                                
 GROUP BY CASE WHEN MONTH(h.ReportingPeriodStartDate)>=4
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate)+1 AS VARCHAR(4)),2))
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),"-",RIGHT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),2))
               END,
          CASE WHEN h.ReportingPeriodStartDate not between '$rp_startdate' and '$rp_enddate'
               THEN CONCAT(CAST(YEAR(h.ReportingPeriodStartDate) AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               ELSE CONCAT(CAST(YEAR(h.ReportingPeriodStartDate)-1 AS VARCHAR(4)),'-',RIGHT(CONCAT('00',MONTH(h.ReportingPeriodStartDate)),2),'-01')
               END                                

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
data = sqlContext.sql(f"select * from {db_output}.DQ_T2_Full")
data2 = sqlContext.sql(f"select * from {db_output}.DQ_T2_ProvCount")
data3 = sqlContext.sql(f"select * from {db_output}.DQ_T2_National")

dq2_patients = data.groupby("ORGANISATION_CODE","ORGANISATION_NAME","YEAR").pivot("ReportingPeriodStartDate").sum("COUNT").sort("ORGANISATION_NAME","YEAR")
dq2_provcount = data2.groupby("YEAR").pivot("ReportingPeriodStartDate").sum("Providers").sort("YEAR")
dq2_national = data3.groupby("YEAR").pivot("ReportingPeriodStartDate").sum("Count").sort("YEAR")
# display(dq2_national)

# COMMAND ----------

# DBTITLE 1,Apply Suppression to dq2_patients
dq2_patients_2 = dq2_patients.toPandas()

for i in dq2_patients_2.columns[3:15]:
  dq2_patients_2[i] = dq2_patients_2[i].fillna(999999999)
  dq2_patients_2[i] = dq2_patients_2[i].apply(tb2_suppression)

dq2_patients = spark.createDataFrame(dq2_patients_2)
# display(dq2_patients_2)

# COMMAND ----------

# DBTITLE 1,DQ Table 3 - Current Reporting Year
 %sql
 
 --current reporting year providers
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Providers_cur AS
 Select a.orgidprov, count(distinct a.person_id) as count
 from $dbm.mhs001mpi a
 inner join $dbm.mhs101referral b on a.person_id = b.person_id and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprov
 inner join 
 (
 select distinct orgidprovider from $dbm.mhs000header 
 where uniqmonthid between $start_month_id and $end_month_id
 group by orgidprovider
 having count(distinct uniqmonthid) = 12
 ) c on a.orgidprov = c.orgidprovider
 and a.uniqmonthid between $start_month_id and $end_month_id
 group by a.orgidprov

# COMMAND ----------

# DBTITLE 1,DQ Table 3 - Previous Reporting Year
 %sql
 
 --previous reporting year providers
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW Providers_pre AS
 Select a.orgidprov, count(distinct a.person_id) as count
 from $dbm.mhs001mpi a
 inner join $dbm.mhs101referral b on a.person_id = b.person_id and a.uniqmonthid = b.uniqmonthid and a.orgidprov = b.orgidprov
 inner join 
 (
 select distinct orgidprovider from $dbm.mhs000header 
 where uniqmonthid between $pre_start_month_id and $pre_end_month_id
 group by orgidprovider
 having count(distinct uniqmonthid) = 12
 ) c on a.orgidprov = c.orgidprovider
 and a.uniqmonthid between $pre_start_month_id and $pre_end_month_id
 group by a.orgidprov

# COMMAND ----------

# DBTITLE 1,DQ Table 3 Final - Download this output
 %sql
 
 DROP TABLE IF EXISTS $db_output.DQ_T3_Full;
 CREATE TABLE         $db_output.DQ_T3_Full USING DELTA AS
 
 Select  case when a.orgidprov is null then b.orgidprov else a.orgidprov end as Provider,
         coalesce(od.name, od2.name) as Provider_Name,
         b.count as prev_year_count,
         a.count as cur_year_count
 from global_temp.Providers_cur a
 full outer join global_temp.Providers_pre b on a.orgidprov = b.orgidprov
 LEFT JOIN $db_output.MHB_ORG_DAILY as od
    ON case when a.orgidprov is null then b.orgidprov else a.orgidprov end = od.ORG_CODE AND od.BUSINESS_END_DATE IS NULL
            AND od.ORG_OPEN_DATE <= '$rp_enddate'
            AND ((od.ORG_CLOSE_DATE >= '$rp_enddate') OR od.ORG_CLOSE_DATE is NULL)
 LEFT JOIN $db_output.MHB_ORG_DAILY as od2 ON case when a.orgidprov is null then b.orgidprov else a.orgidprov end = od2.ORG_CODE           
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

display(dq3)

# COMMAND ----------

# DBTITLE 1,CREATE OUTPUT EXCEL FILE


# COMMAND ----------

# DBTITLE 1,Create Excel Report Variables
# from datetime import datetime

# Convert
# MonthSRP = Rpstartmonthname
SRP_Year = pd.to_datetime(dbutils.widgets.get("rp_startdate")).date().strftime("%Y")
ERP_Year = pd.to_datetime(dbutils.widgets.get("rp_enddate")).date().strftime("%Y")
# SRP = pd.to_datetime(ReportingStartPeriod).date().strftime("%d/%m/%Y")
# ERP = pd.to_datetime(ReportingEndPeriod).date().strftime("%d/%m/%Y")
RptPeriod = 'Reporting Period ' + SRP_Year + ' - ' + ERP_Year
# RptPeriod
# Month_Year = MonthSRP + ' ' + SRP_Year

# Today_Timestamp
# Today_Timestamp = datetime.datetime.now() 
# File_Created_Timestamp = Today_Timestamp.strftime("%d%m%Y_%H%M%S")

# Unique filename created using timestamp 
filename = 'MHB_'+ SRP_Year +'_' + ERP_Year + '_DQTables.xlsx'
print('filename = ', filename)

# # Split_Database_name
# Split_Database_name = My_Database.split('_')
# First_Name = Split_Database_name[0]
# Surname = Split_Database_name[1]
# User_Name = First_Name + ' ' + Surname
# User_Name = 'CNST Scorecard ' + MonthSRP + '_' + SRP_Year
# print ('User_Name = ', User_Name)

# COMMAND ----------

# DBTITLE 1,Create Single File
from dsp.common.exports import create_excel_for_download
from dsp.common.digitrials import cover_page, interpretation_page, footnotes_page, add_digitrials_styling

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

# # filename = 'CQIM_Report.xlsx'

displayHTML(f"<h4>{filename} is ready to <a href='data:text/csv;base64,{data.decode()}' download='{filename}'>download</a></h4>")

# COMMAND ----------

