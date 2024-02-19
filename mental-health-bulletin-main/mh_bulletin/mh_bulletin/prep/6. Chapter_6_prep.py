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

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.Care_Contacts;
 CREATE TABLE $db_output.Care_Contacts USING DELTA AS
 SELECT
 cc.UniqMonthID,
 cc.Person_ID,
 mpi.AgeRepPeriodEnd,
 mpi.age_group_higher_level,
 mpi.age_group_lower_common,
 mpi.Der_Gender,
 mpi.Der_Gender_Desc,
 mpi.NHSDEthnicity,
 mpi.UpperEthnicity,
 mpi.LowerEthnicityCode,
 mpi.LowerEthnicityName,                  
 mpi.IMD_Decile,
 mpi.IMD_Quintile,
 cc.OrgIDProv,
 o.Name as Provider_Name,
 get_provider_type_code(cc.OrgIDProv) as ProvTypeCode,
 get_provider_type_name(cc.OrgIDProv) as ProvTypeName,
 cc.MHS201UniqID,
 cc.UniqCareContID,
 cc.CareContDate,
 coalesce(tt.TeamTypeCode, "Unknown") as ServTeamTypeRefToMH,
 coalesce(tt.TeamName, "Unknown") as TeamTypeName,
 coalesce(ac.AttCode, "Unknown") as AttCode,
 coalesce(ac.AttName, "Unknown") as AttName
 
 FROM $db_output.MHB_MHS201CareContact cc
 LEFT JOIN $db_output.mpi mpi on cc.Person_ID = mpi.Person_ID
 LEFT JOIN $db_output.MHB_MHS102ServiceTypeReferredTo st on cc.UniqServReqID = st.UniqServReqID and cc.UniqCareProfTeamID = st.UniqCareProfTeamID ---count of activity so joining on UniqServReqID and UniqCareProfTeamID
 LEFT JOIN $db_output.TeamType tt on st.ServTeamTypeRefToMH = tt.TeamTypeCode
 LEFT JOIN $db_output.attendance_code ac on cc.AttendOrDNACode = ac.AttCode
 LEFT JOIN $db_output.MHB_ORG_DAILY o on cc.OrgIDProv = o.ORG_CODE

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.Care_Contacts