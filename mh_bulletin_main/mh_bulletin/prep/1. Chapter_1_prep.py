# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text("db_output", "_tootle1_100261", "db_output")
# dbutils.widgets.text("db_source", "mh_v5_pre_clear", "db_source")
# dbutils.widgets.text("end_month_id", "1476", "end_month_id")
# dbutils.widgets.text("start_month_id", "1465", "start_month_id")
# dbutils.widgets.text("rp_enddate", "2023-03-31", "rp_enddate")
# dbutils.widgets.text("rp_startdate", "2022-04-01", "rp_startdate")
# dbutils.widgets.text("IMD_year", "2019", "IMD_year")

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

# DBTITLE 1,Distinct people in contact with MHLDA services
 %sql
 --get distinct cohort of people who are in both mhs101 and mhs001
 CREATE OR REPLACE GLOBAL TEMP VIEW People AS
 select            distinct a.Person_id as People
 from              $db_output.MHB_MHS001MPI a
 inner join        $db_output.MHB_MHS101Referral b 
                       on a.Person_id = b.Person_id

# COMMAND ----------

# DBTITLE 1,Admitted within MHLDA services
 %sql
 --getting list of people from initial cohort who have had a referral and also had a Hospital Spell (admitted) joined with people who have also had a ward stay
 CREATE OR REPLACE GLOBAL TEMP VIEW Admitted AS
 SELECT              DISTINCT MPI.Person_id, HospitalBedTypeMH
 FROM                $db_output.MHB_MHS001MPI MPI
 INNER JOIN          $db_output.MHB_MHS101Referral REF ON MPI.Person_id = REF.Person_id
 INNER JOIN           $db_output.MHB_MHS501HospProvSpell HSP ON REF.Person_id = HSP.Person_id --join on person_id as we are counting people
 LEFT JOIN           $db_output.MHB_MHS502WardStay ws ON ws.Person_id = MPI.Person_id

# COMMAND ----------

# DBTITLE 1,Service type temp table
 %sql
 ---getting distinct person_id and service_type from MHS102. A person may have been directed to more than one service_type throughout the year
 CREATE OR REPLACE GLOBAL TEMP VIEW ServiceType AS
 select              distinct a.Person_id
                     ,ServTeamTypeRefToMH
 from                $db_output.MHB_MHS102ServiceTypeReferredTo a
 inner join          $db_output.MPI b on a.Person_id = b.Person_id
 inner join          $db_output.MHB_MHS101Referral c on b.Person_id = c.Person_id --join on person_id as we are counting people
 group by            a.Person_id, ServTeamTypeRefToMH

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

# DBTITLE 1,People in contact Annual asset
 %sql
 --creates final prep table with needed breakdowns for 1a, 1b, 1c, 1d, 1e. Provider breakdowns use separate table
 DROP TABLE IF EXISTS $db_output.people_in_contact;
 CREATE TABLE        $db_output.people_in_contact USING DELTA AS
 select              
                     a.Person_ID,
                     b.OrgIDProv,                    
                     o.NAME as Provider_Name,
                     get_provider_type_code(b.OrgIDProv) as ProvTypeCode,
                     get_provider_type_name(b.OrgIDProv) as ProvTypeName,
                     a.AgeRepPeriodEnd,
                     a.age_group_higher_level,
                     a.age_group_lower_chap1,
                     a.Der_Gender,
                     a.Der_Gender_Desc,
                     a.NHSDEthnicity,
                     a.UpperEthnicity,
                     a.LowerEthnicityCode,
                     a.LowerEthnicityName,
                     case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when la.level is null then 'UNKNOWN'
                                         when a.LADistrictAuth = '' then 'UNKNOWN'
                                         else la.level end as LADistrictAuth,
                     coalesce(la.level_description, 'UNKNOWN') as LADistrictAuthName,                    
                     a.IMD_Decile,
                     a.IMD_Quintile,
                     COALESCE(stp.CCG_CODE,'UNKNOWN') AS CCG_CODE,
                     COALESCE(stp.CCG_NAME, 'UNKNOWN') as CCG_NAME,
                     COALESCE(stp.STP_CODE, 'UNKNOWN') as STP_CODE,
                     COALESCE(stp.STP_NAME, 'UNKNOWN') as STP_NAME, 
                     COALESCE(stp.REGION_CODE, 'UNKNOWN') as REGION_CODE,
                     COALESCE(stp.REGION_NAME, 'UNKNOWN') as REGION_NAME, 
                     COALESCE(bd.HigherMHAdmittedPatientClassName, "Invalid") AS Bed_Type,
                     coalesce(tt.TeamTypeCode, "UNKNOWN") as TeamTypeCode,   -- ServTeamTypeRefToMH,
                     coalesce(tt.TeamName, "UNKNOWN") as TeamName,   -- TeamTypeName,         
                     coalesce(tt.HigherTeamName, "UNKNOWN") as ServiceType,
                     case when ad.person_id IS not Null then 'Admitted' else 'Non_Admitted' end as Admitted
                                                                      
 from                $db_output.MPI a
 left join           global_temp.ServiceType s on a.person_id = s.person_id ---count of people so join on person_id
 left join           global_temp.Admitted ad on a.person_id = ad.person_id 
 left join           $db_output.TeamType tt on s.ServTeamTypeRefToMH = tt.TeamTypeCode
 left join           $db_output.hosp_bed_desc bd on ad.HospitalBedTypeMH = bd.MHAdmittedPatientClass
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         when a.LADistrictAuth = '' then 'UNKNOWN'
                                         when a.LADistrictAuth is null then 'UNKNOWN'
                                         else 'UNKNOWN' end
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 inner join          $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id  --make sure people in mpi table are also present in referral table
 left join           $db_output.MHB_ORG_DAILY o on b.OrgIDProv = o.ORG_CODE;
 OPTIMIZE $db_output.people_in_contact ZORDER BY (Admitted);

# COMMAND ----------

 %sql
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 CREATE OR REPLACE GLOBAL TEMP VIEW people_prov AS
 select                distinct x.OrgIDProv
                       ,x.person_id 
 from                  $db_output.MHB_MHS001MPI x
 inner join            $db_output.MHB_MHS101Referral ref 
                       on x.person_id = ref.person_id	
                       and x.OrgIDProv = ref.OrgIDProv		

# COMMAND ----------

 %sql
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 CREATE OR REPLACE GLOBAL TEMP VIEW admitted_prov AS
 SELECT                DISTINCT A.OrgIDProv
                       ,A.Person_ID, HospitalBedTypeMH
 FROM                  $db_output.MHB_MHS001MPI A
 INNER JOIN            $db_output.MHB_MHS101Referral B 
                       ON a.Person_ID = B.Person_ID 
                       AND A.OrgIDProv = B.OrgIDProv
 INNER JOIN            $db_output.MHB_MHS501HospProvSpell C  ---methodology used could be changed here to be conistent with national. As a count of people join on person_id
                       ON b.UniqServReqID = C.UniqServReqID
 LEFT JOIN             $db_output.MHB_MHS502WardStay D 
                       ON a.Person_ID = D.Person_ID ---count of people per provider so joining on person_id and orgidprov
                       AND A.OrgIDProv = D.OrgIDProv

# COMMAND ----------

 %sql
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 CREATE OR REPLACE GLOBAL TEMP VIEW ServiceType_Prov AS
 select              distinct a.OrgIDProv
                     ,a.Person_id
                     ,ServTeamTypeRefToMH
 from                $db_output.MHB_MHS102ServiceTypeReferredTo a
 inner join          $db_output.MPI b on a.Person_id = b.Person_id AND a.OrgIDProv = b.OrgIDProv
 inner join          $db_output.MHB_MHS101Referral c on b.Person_id = c.Person_id AND b.OrgIDProv = c.OrgIDProv ---count of people per provider so joining on person_id and orgidprov
 group by            a.OrgIDProv, a.Person_id, ServTeamTypeRefToMH

# COMMAND ----------

# DBTITLE 1,People in contact Annual asset - Provider
 %sql
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 DROP TABLE IF EXISTS $db_output.people_in_contact_prov;
 CREATE TABLE         $db_output.people_in_contact_prov AS
 select              
                     a.Person_ID,
                     a.OrgIDProv,                    
                     o.NAME as Provider_Name,
                     get_provider_type_code(a.OrgIDProv) as ProvTypeCode,
                     get_provider_type_name(a.OrgIDProv) as ProvTypeName,
                     a.AgeRepPeriodEnd,
                     a.age_group_higher_level,
                     a.age_group_lower_chap1,
                     a.Der_Gender,
                     a.Der_Gender_Desc,
                     a.NHSDEthnicity,
                     a.UpperEthnicity,
                     a.LowerEthnicityCode,
                     a.LowerEthnicityName,
                     case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when la.level is null then 'UNKNOWN'
                                         when a.LADistrictAuth = '' then 'UNKNOWN'
                                         else la.level end as LADistrictAuth,
                     COALESCE(la.level_description, 'UNKNOWN') as LADistrictAuthName,                    
                     a.IMD_Decile,
                     a.IMD_Quintile,
                     COALESCE(stp.CCG_CODE,'UNKNOWN') AS CCG_CODE,
                     COALESCE(stp.CCG_NAME, 'UNKNOWN') as CCG_NAME,
                     COALESCE(stp.STP_CODE, 'UNKNOWN') as STP_CODE,
                     COALESCE(stp.STP_NAME, 'UNKNOWN') as STP_NAME, 
                     COALESCE(stp.REGION_CODE, 'UNKNOWN') as REGION_CODE,
                     COALESCE(stp.REGION_NAME, 'UNKNOWN') as REGION_NAME,
                     COALESCE(bd.HigherMHAdmittedPatientClassName, "Invalid") AS Bed_Type,
                     coalesce(tt.TeamTypeCode, "UNKNOWN") as TeamTypeCode,   -- ServTeamTypeRefToMH,
                     coalesce(tt.TeamName, "UNKNOWN") as TeamName,   -- TeamTypeName,           
                     coalesce(tt.HigherTeamName, "UNKNOWN") as ServiceType,
                     case when ad.person_id IS not Null then 'Admitted' else 'Non_Admitted' end as Admitted
                                                                      
 from                $db_output.MPI_PROV a
 left join           global_temp.admitted_prov ad on a.Person_ID = ad.Person_ID and a.OrgIDProv = ad.OrgIDProv  --count of people per provider so join on person_id and orgidprov
 left join           global_temp.ServiceType_prov s on a.person_id = s.person_id AND a.OrgIDProv = s.OrgIDProv
 left join           $db_output.TeamType tt on s.ServTeamTypeRefToMH = tt.TeamTypeCode
 left join           $db_output.hosp_bed_desc bd on ad.HospitalBedTypeMH = bd.MHAdmittedPatientClass
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         when a.LADistrictAuth = '' then 'UNKNOWN'
                                         when a.LADistrictAuth is null then 'UNKNOWN'
                                         else 'UNKNOWN' end
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 inner join          $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id AND a.OrgIDProv = b.OrgIDProv  --make sure people in mpi table are also present in referral table
 left join           $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE;
 OPTIMIZE $db_output.people_in_contact_prov ZORDER BY (Admitted);

# COMMAND ----------

# DBTITLE 1,People in contact - Provider Type Admitted
# %sql
# --gets required breakdowns for Provider Type. Classifies Providers as 'NHS Providers' and 'Non NHS Providers'
# --Admitted patients will be present in MHS501
# CREATE OR REPLACE GLOBAL TEMP VIEW provider_type_admitted AS
# select            distinct case when a.Der_Gender = '1' then '1'
#                                   when a.Der_Gender = '2' then '2'
#                                   when a.Der_Gender = '3' then '3'
#                                   when a.Der_Gender = '4' then '4'
#                                   when a.Der_Gender = '9' then '9'
#                                   else 'UNKNOWN' end as gender 
#                   ,mpi.AgeRepPeriodEnd
#                   ,mpi.age_group_higher_level
#                   ,mpi.age_group_lower_chap1 
#                   ,mpi.NHSDEthnicity
#              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
#                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
#                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
#                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
#                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known' 
#                      ELSE 'UNKNOWN' END AS UpperEthnicity
#              ,CASE WHEN a.NHSDEthnicity = 'A' THEN 'British'
#                    WHEN a.NHSDEthnicity = 'B' THEN 'Irish'
#                    WHEN a.NHSDEthnicity = 'C' THEN 'Any other White background'
#                    WHEN a.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
#                    WHEN a.NHSDEthnicity = 'E' THEN 'White and Black African'
#                    WHEN a.NHSDEthnicity = 'F' THEN 'White and Asian'
#                    WHEN a.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
#                    WHEN a.NHSDEthnicity = 'H' THEN 'Indian'
#                    WHEN a.NHSDEthnicity = 'J' THEN 'Pakistani'
#                    WHEN a.NHSDEthnicity = 'K' THEN 'Bangladeshi'
#                    WHEN a.NHSDEthnicity = 'L' THEN 'Any other Asian background'
#                    WHEN a.NHSDEthnicity = 'M' THEN 'Caribbean'
#                    WHEN a.NHSDEthnicity = 'N' THEN 'African'
#                    WHEN a.NHSDEthnicity = 'P' THEN 'Any other Black background'
#                    WHEN a.NHSDEthnicity = 'R' THEN 'Chinese'
#                    WHEN a.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
#                      ELSE 'UNKNOWN' END AS LowerEthnicity
#                   ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
#                         else 'Non NHS Providers' end as Provider_type 
#                   ,d.OrgIDProv 
#                   ,a.Person_id
# from              $db_output.MHB_MHS001MPI a
# inner join        $db_output.MHB_MHS101Referral d on a.Person_id = d.Person_id and a.OrgIDProv = d.OrgIDProv  --count of people per provider so join on person_id and orgidprov
# inner join        $db_output.MHB_MHS501HospProvSpell b on b.UniqServReqID = d.UniqServReqID  ---link referral to Hosipital Spell by UniqServReqID
# left join         $db_output.MPI mpi on a.Person_id = mpi.Person_id ---make sure all people in edited mpi table are in live mpi table ---ASK AH
# LEFT JOIN         global_temp.MHB_RD_ORG_DAILY_LATEST AS ORG ON a.OrgIDProv = ORG.ORG_CODE                
# GROUP BY      case when a.Der_Gender = '1' then '1'
#                     when a.Der_Gender = '2' then '2'
#                     when a.Der_Gender = '3' then '3'
#                     when a.Der_Gender = '4' then '4'
#                     when a.Der_Gender = '9' then '9'
#                     else 'UNKNOWN' end
#                   ,mpi.AgeRepPeriodEnd
#                   ,mpi.age_group_higher_level
#                   ,mpi.age_group_lower_chap1 
#                   ,mpi.NHSDEthnicity
#              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
#                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
#                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
#                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
#                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
#                      ELSE 'UNKNOWN' END 
#              ,CASE WHEN a.NHSDEthnicity = 'A' THEN 'British'
#                    WHEN a.NHSDEthnicity = 'B' THEN 'Irish'
#                    WHEN a.NHSDEthnicity = 'C' THEN 'Any other White background'
#                    WHEN a.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
#                    WHEN a.NHSDEthnicity = 'E' THEN 'White and Black African'
#                    WHEN a.NHSDEthnicity = 'F' THEN 'White and Asian'
#                    WHEN a.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
#                    WHEN a.NHSDEthnicity = 'H' THEN 'Indian'
#                    WHEN a.NHSDEthnicity = 'J' THEN 'Pakistani'
#                    WHEN a.NHSDEthnicity = 'K' THEN 'Bangladeshi'
#                    WHEN a.NHSDEthnicity = 'L' THEN 'Any other Asian background'
#                    WHEN a.NHSDEthnicity = 'M' THEN 'Caribbean'
#                    WHEN a.NHSDEthnicity = 'N' THEN 'African'
#                    WHEN a.NHSDEthnicity = 'P' THEN 'Any other Black background'
#                    WHEN a.NHSDEthnicity = 'R' THEN 'Chinese'
#                    WHEN a.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
#                      ELSE 'UNKNOWN' END 
#                   ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
#                         else 'Non NHS Providers' end 
#                   ,d.OrgIDProv 
#                   ,a.Person_id

# COMMAND ----------

# DBTITLE 1,People in contact - Provider Type Non-Admitted
# %sql
# --Non-admitted patients will not be present in MHS501
# CREATE OR REPLACE GLOBAL TEMP VIEW provider_type_non_admitted AS
# select             distinct case when a.Der_Gender = '1' then '1'
#                                   when a.Der_Gender = '2' then '2'
#                                   when a.Der_Gender = '3' then '3'
#                                   when a.Der_Gender = '4' then '4'
#                                   when a.Der_Gender = '9' then '9'
#                                   else 'UNKNOWN' end as gender 
#                    ,mpi.AgeRepPeriodEnd
#                    ,mpi.age_group_higher_level
#                    ,mpi.age_group_lower_chap1 
#                    ,mpi.NHSDEthnicity
#              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
#                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
#                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
#                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
#                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
#                      ELSE 'UNKNOWN' END AS UpperEthnicity
#              ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'British'
#                    WHEN mpi.NHSDEthnicity = 'B' THEN 'Irish'
#                    WHEN mpi.NHSDEthnicity = 'C' THEN 'Any other White background'
#                    WHEN mpi.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
#                    WHEN mpi.NHSDEthnicity = 'E' THEN 'White and Black African'
#                    WHEN mpi.NHSDEthnicity = 'F' THEN 'White and Asian'
#                    WHEN mpi.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
#                    WHEN mpi.NHSDEthnicity = 'H' THEN 'Indian'
#                    WHEN mpi.NHSDEthnicity = 'J' THEN 'Pakistani'
#                    WHEN mpi.NHSDEthnicity = 'K' THEN 'Bangladeshi'
#                    WHEN mpi.NHSDEthnicity = 'L' THEN 'Any other Asian background'
#                    WHEN mpi.NHSDEthnicity = 'M' THEN 'Caribbean'
#                    WHEN mpi.NHSDEthnicity = 'N' THEN 'African'
#                    WHEN mpi.NHSDEthnicity = 'P' THEN 'Any other Black background'
#                    WHEN mpi.NHSDEthnicity = 'R' THEN 'Chinese'
#                    WHEN mpi.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
#                      ELSE 'UNKNOWN' END AS LowerEthnicity
#                    ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
#                          else 'Non NHS Providers' end as Provider_type
#                    ,a.Person_id
# from               $db_output.MHB_MHS001MPI a
# LEFT JOIN          global_temp.MHB_RD_ORG_DAILY_LATEST AS ORG ON a.OrgIDProv = ORG.ORG_CODE
# inner join         $db_output.MHB_MHS101Referral d on a.Person_id = d.Person_id and a.OrgIDProv = d.OrgIDProv
# left outer join    ( select distinct Person_id 
#                                    ,Provider_type 
#                      from global_temp.provider_type_admitted
#                     ) b ----methodology: gather admitted cohort of patients, if not present in this cohort then class as 'Non Admitted'
#                      on b.Person_id = d.Person_id 
#                      and case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
#                          else 'Non NHS Providers' end = b.Provider_type ---ASK AH
# left join           $db_output.MPI mpi 
#                     on a.Person_id = mpi.Person_id
# where               b.Person_id is null
# GROUP BY          case when a.Der_Gender = '1' then '1'
#                         when a.Der_Gender = '2' then '2'
#                         when a.Der_Gender = '3' then '3'
#                         when a.Der_Gender = '4' then '4'
#                         when a.Der_Gender = '9' then '9'
#                         else 'UNKNOWN' end
#                    ,mpi.AgeRepPeriodEnd
#                    ,mpi.age_group_higher_level
#                    ,mpi.age_group_lower_chap1 
#                    ,mpi.NHSDEthnicity
#              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
#                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
#                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
#                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
#                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
#                      ELSE 'UNKNOWN' END 
#              ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'British'
#                    WHEN mpi.NHSDEthnicity = 'B' THEN 'Irish'
#                    WHEN mpi.NHSDEthnicity = 'C' THEN 'Any other White background'
#                    WHEN mpi.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
#                    WHEN mpi.NHSDEthnicity = 'E' THEN 'White and Black African'
#                    WHEN mpi.NHSDEthnicity = 'F' THEN 'White and Asian'
#                    WHEN mpi.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
#                    WHEN mpi.NHSDEthnicity = 'H' THEN 'Indian'
#                    WHEN mpi.NHSDEthnicity = 'J' THEN 'Pakistani'
#                    WHEN mpi.NHSDEthnicity = 'K' THEN 'Bangladeshi'
#                    WHEN mpi.NHSDEthnicity = 'L' THEN 'Any other Asian background'
#                    WHEN mpi.NHSDEthnicity = 'M' THEN 'Caribbean'
#                    WHEN mpi.NHSDEthnicity = 'N' THEN 'African'
#                    WHEN mpi.NHSDEthnicity = 'P' THEN 'Any other Black background'
#                    WHEN mpi.NHSDEthnicity = 'R' THEN 'Chinese'
#                    WHEN mpi.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
#                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
#                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
#                      ELSE 'UNKNOWN' END 
#                    ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
#                          else 'Non NHS Providers' end
#                    ,a.Person_id

# COMMAND ----------

# DBTITLE 1,People in contact Annual data aseet- Provider Type
# %sql
# ---union on both admitted and non-admitted to create one final table
# DROP TABLE IF EXISTS $db_output.people_in_contact_prov_type;
# CREATE TABLE         $db_output.people_in_contact_prov_type AS
# SELECT              gender
#                     ,AgeRepPeriodEnd
#                     ,age_group_higher_level
#                     ,age_group_lower_chap1 
#                     ,NHSDEthnicity
#                     ,UpperEthnicity
#                     ,LowerEthnicity
#                     ,Provider_type
#                     ,'Admitted' as Admitted
#                     ,COUNT(distinct Person_id) as people 
# FROM                global_temp.provider_type_admitted
# GROUP BY            gender
#                     ,AgeRepPeriodEnd
#                     ,age_group_higher_level
#                     ,age_group_lower_chap1 
#                     ,NHSDEthnicity
#                     ,UpperEthnicity
#                     ,LowerEthnicity
#                     ,Provider_type
                    
# UNION ALL

# SELECT              gender
#                     ,AgeRepPeriodEnd
#                     ,age_group_higher_level
#                     ,age_group_lower_chap1 
#                     ,NHSDEthnicity
#                     ,UpperEthnicity
#                     ,LowerEthnicity
#                     ,Provider_type
#                     ,'Non_Admitted' as Admitted
#                     ,COUNT(distinct Person_id) as people 
# FROM                global_temp.provider_type_non_admitted
# GROUP BY            gender
#                     ,AgeRepPeriodEnd
#                     ,age_group_higher_level
#                     ,age_group_lower_chap1 
#                     ,NHSDEthnicity
#                     ,UpperEthnicity
#                     ,LowerEthnicity
#                     ,Provider_type
