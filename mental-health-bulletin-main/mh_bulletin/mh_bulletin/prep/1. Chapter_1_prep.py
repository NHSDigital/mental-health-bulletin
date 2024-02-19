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
                                         when la.level is null then 'Unknown'
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         else la.level end as LADistrictAuth,
                     coalesce(la.level_description, 'Unknown') as LADistrictAuthName,                    
                     a.IMD_Decile,
                     a.IMD_Quintile,
                     COALESCE(stp.CCG_CODE,'Unknown') AS CCG_CODE,
                     COALESCE(stp.CCG_NAME, 'Unknown') as CCG_NAME,
                     COALESCE(stp.STP_CODE, 'Unknown') as STP_CODE,
                     COALESCE(stp.STP_NAME, 'Unknown') as STP_NAME, 
                     COALESCE(stp.REGION_CODE, 'Unknown') as REGION_CODE,
                     COALESCE(stp.REGION_NAME, 'Unknown') as REGION_NAME, 
                     CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END AS Bed_Type,
                     coalesce(tt.TeamTypeCode, "Unknown") as ServTeamTypeRefToMH,
                     coalesce(tt.TeamName, "Unknown") as TeamTypeName,         
                     CASE WHEN tt.TeamTypeCode IN ('A08', 'A09', 'A16', 'A05', 'A13', 'A12', 'C10', 'A06') THEN 'Core community mental health'
                           WHEN tt.TeamTypeCode = 'A14' THEN 'Early Intervention Team for Psychosis'
                           WHEN tt.TeamTypeCode = 'D05' THEN 'Individual Placement and Support'
                           WHEN tt.TeamTypeCode = 'C02' THEN 'Specialist Perinatal Mental Health'
                           WHEN tt.TeamTypeCode = 'A19' THEN '24/7 Crisis Response Line'
                           WHEN tt.TeamTypeCode IN ('A24', 'A21', 'A25', 'A03', 'A02', 'A20', 'A04', 'A23', 'A22') THEN 'Crisis and acute mental health activity in community settings'
                           WHEN tt.TeamTypeCode IN ('C05', 'A11') THEN 'Mental health activity in general hospitals'
                           WHEN tt.TeamTypeCode IN ('E01', 'E02', 'E03', 'E04') THEN 'LDA'
                           WHEN tt.TeamTypeCode IN ('B01', 'B02') THEN 'Forensic Services'
                           WHEN tt.TeamTypeCode = 'C01' THEN 'Autism Service'
                           WHEN tt.TeamTypeCode IN ('C08', 'C06', 'C07', 'C04') THEN 'Specialist Services'
                           WHEN tt.TeamTypeCode IN ('A01', 'A07', 'A10', 'A17', 'A18', 'A15', 'D07', 'D01', 'D04', 'D08', 'D02', 'D03', 'D06', 'F01', 'F02','F03', 'F04','F05','F06') THEN 'Other mental health services'
                           WHEN tt.TeamTypeCode IN ('Z01', 'Z02') THEN 'Other'
                           ELSE 'Unknown' END as ServiceType,
                     case when ad.person_id IS not Null then 'Admitted' else 'Non_Admitted' end as Admitted
                                                                      
 from                $db_output.MPI a
 left join           global_temp.ServiceType s on a.person_id = s.person_id ---count of people so join on person_id
 left join           global_temp.Admitted ad on a.person_id = ad.person_id 
 left join           $db_output.TeamType tt on s.ServTeamTypeRefToMH = tt.TeamTypeCode
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         when a.LADistrictAuth is null then 'Unknown'
                                         else 'Unknown' end
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 inner join          $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id  --make sure people in mpi table are also present in referral table
 left join           $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE;
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
                                         when la.level is null then 'Unknown'
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         else la.level end as LADistrictAuth,
                     COALESCE(la.level_description, 'Unknown') as LADistrictAuthName,                    
                     a.IMD_Decile,
                     a.IMD_Quintile,
                     COALESCE(stp.CCG_CODE,'Unknown') AS CCG_CODE,
                     COALESCE(stp.CCG_NAME, 'Unknown') as CCG_NAME,
                     COALESCE(stp.STP_CODE, 'Unknown') as STP_CODE,
                     COALESCE(stp.STP_NAME, 'Unknown') as STP_NAME, 
                     COALESCE(stp.REGION_CODE, 'Unknown') as REGION_CODE,
                     COALESCE(stp.REGION_NAME, 'Unknown') as REGION_NAME, 
                     CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END AS Bed_Type,
                     coalesce(tt.TeamTypeCode, "Unknown") as ServTeamTypeRefToMH,
                     coalesce(tt.TeamName, "Unknown") as TeamTypeName,  
                     CASE WHEN tt.TeamTypeCode IN ('A08', 'A09', 'A16', 'A05', 'A13', 'A12', 'C10', 'A06') THEN 'Core community mental health'
                           WHEN tt.TeamTypeCode = 'A14' THEN 'Early Intervention Team for Psychosis'
                           WHEN tt.TeamTypeCode = 'D05' THEN 'Individual Placement and Support'
                           WHEN tt.TeamTypeCode = 'C02' THEN 'Specialist Perinatal Mental Health'
                           WHEN tt.TeamTypeCode = 'A19' THEN '24/7 Crisis Response Line'
                           WHEN tt.TeamTypeCode IN ('A24', 'A21', 'A25', 'A03', 'A02', 'A20', 'A04', 'A23', 'A22') THEN 'Crisis and acute mental health activity in community settings'
                           WHEN tt.TeamTypeCode IN ('C05', 'A11') THEN 'Mental health activity in general hospitals'
                           WHEN tt.TeamTypeCode IN ('E01', 'E02', 'E03', 'E04') THEN 'LDA'
                           WHEN tt.TeamTypeCode IN ('B01', 'B02') THEN 'Forensic Services'
                           WHEN tt.TeamTypeCode IN ('C01', 'C08', 'C06', 'C07', 'C04') THEN 'Specialist Services'
                           WHEN tt.TeamTypeCode IN ('A01', 'A07', 'A10', 'A17', 'A18', 'A15', 'D07', 'D01', 'D04', 'D08', 'D02', 'D03') THEN 'Other mental health services'
                           WHEN tt.TeamTypeCode IN ('Z01', 'Z02') THEN 'Other'
                           ELSE 'Unknown' END as ServiceType,
                     case when ad.person_id IS not Null then 'Admitted' else 'Non_Admitted' end as Admitted
                                                                      
 from                $db_output.MPI_PROV a
 left join           global_temp.admitted_prov ad on a.Person_ID = ad.Person_ID and a.OrgIDProv = ad.OrgIDProv  --count of people per provider so join on person_id and orgidprov
 left join           global_temp.ServiceType_prov s on a.person_id = s.person_id AND a.OrgIDProv = s.OrgIDProv
 left join           $db_output.TeamType tt on s.ServTeamTypeRefToMH = tt.TeamTypeCode
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         when a.LADistrictAuth is null then 'Unknown'
                                         else 'Unknown' end
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 inner join          $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id AND a.OrgIDProv = b.OrgIDProv  --make sure people in mpi table are also present in referral table
 left join           $db_output.MHB_ORG_DAILY o on a.OrgIDProv = o.ORG_CODE;
 OPTIMIZE $db_output.people_in_contact_prov ZORDER BY (Admitted);