# Databricks notebook source
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW People AS
 ---get distinct cohort of people who are in both mhs101referral and mhs001mpi in the financial year
 select            distinct a.Person_id as People
 from              $db_output.MHB_MHS001MPI a
 inner join        $db_output.MHB_MHS101Referral b 
                       on a.Person_id = b.Person_id

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Admitted AS
 ---getting list of people from initial cohort who have had a referral and also had a Hospital Spell (admitted) and also had a ward stay in the financial year
 SELECT              DISTINCT MPI.Person_id, HospitalBedTypeMH
 FROM                $db_output.MHB_MHS001MPI MPI
 INNER JOIN          $db_output.MHB_MHS101Referral REF ON MPI.Person_id = REF.Person_id
 INNER JOIN           $db_output.MHB_MHS501HospProvSpell HSP ON REF.Person_id = HSP.Person_id --join on person_id as we are counting people
 LEFT JOIN           $db_output.MHB_MHS502WardStay ws ON ws.Person_id = MPI.Person_id

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ServiceType AS
 ---getting distinct person_id and ServTeamTypeRefToMH. A person may have been directed to more than one service_type throughout the year
 select              distinct a.Person_id
                     ,ServTeamTypeRefToMH
 from                $db_output.MHB_MHS102ServiceTypeReferredTo a
 inner join          $db_output.MPI b on a.Person_id = b.Person_id
 inner join          $db_output.MHB_MHS101Referral c on b.Person_id = c.Person_id --join on person_id as we are counting people
 group by            a.Person_id, ServTeamTypeRefToMH

# COMMAND ----------

 %sql
 --creates final prep table with needed breakdowns for 1a, 1b, 1c, 1d, 1e. Provider breakdowns use separate table
 DROP TABLE IF EXISTS $db_output.people_in_contact;
 CREATE TABLE        $db_output.people_in_contact USING DELTA AS
 select              distinct case when a.Der_Gender = '1' then '1'
                                   when a.Der_Gender = '2' then '2'
                                   when a.Der_Gender = '3' then '3'
                                   when a.Der_Gender = '4' then '4'
                                   when a.Der_Gender = '9' then '9'
                                   else 'Unknown' end as Gender   
                     ,a.AgeRepPeriodEnd
                     ,a.age_group_higher_level
                     ,case when AgeRepPeriodEnd between 0 and 5 then '0 to 5'
                      when AgeRepPeriodEnd between 6 and 10 then '6 to 10'
                      when AgeRepPeriodEnd between 11 and 15 then '11 to 15'
                      when AgeRepPeriodEnd = 16 then '16'
                      when AgeRepPeriodEnd = 17 then '17'
                      when AgeRepPeriodEnd = 18 then '18'
                      when AgeRepPeriodEnd = 19 then '19'
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_chap1                    
                     ,a.NHSDEthnicity
                     ,a.UpperEthnicity
                     ,a.LowerEthnicity
                     ,case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                           when a.LADistrictAuth = 'L99999999' then 'L99999999'
                           when a.LADistrictAuth = 'M99999999' then 'M99999999'
                           when a.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when a.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth
                     ,IMD_Decile
                     ,IMD_Quintile
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END AS bed_type
                     ,ServTeamTypeRefToMH
                     ,CASE WHEN ServTeamTypeRefToMH IN ('A08', 'A09', 'A16', 'A05', 'A13', 'A12', 'C10', 'A06') THEN 'Core Community Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A14' THEN 'Early Intervention Team for Psychosis'
                           WHEN ServTeamTypeRefToMH = 'D05' THEN 'Individual Placement and Support'
                           WHEN ServTeamTypeRefToMH = 'C02' THEN 'Specialist Perinatal Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A19' THEN '24/7 Crisis Response Line'
                           WHEN ServTeamTypeRefToMH IN ('A24', 'A21', 'A25', 'A03', 'A02', 'A20', 'A04', 'A23', 'A22') THEN 'Crisis and Acute Mental Health Activity in Community Settings'
                           WHEN ServTeamTypeRefToMH IN ('C05', 'A11') THEN 'Mental Health Activity in General Hospitals'
                           WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04') THEN 'LDA'
                           WHEN ServTeamTypeRefToMH IN ('B01', 'B02') THEN 'Forensic Services'
                           WHEN ServTeamTypeRefToMH = 'C01' THEN 'Autism Service' 
                           WHEN ServTeamTypeRefToMH IN ('C08', 'C06', 'C07', 'C04') THEN 'Specialist Services'
                           WHEN ServTeamTypeRefToMH IN ('A01', 'A07', 'A10', 'A17', 'A18', 'A15', 'D07', 'D01', 'D04', 'D08', 'D02', 'D03') THEN 'Other Mental Health Services'
                           WHEN ServTeamTypeReftoMH IN ('Z01', 'Z02') THEN 'Other'
                           ELSE 'Unknown' END AS service_type
                     ,a.person_id
                     ,IC_Rec_CCG
                     ,stp.STP_code
                     ,stp.Region_code                   
                     ,case when ad.person_id IS not Null then 'Admitted' else 'Non_Admitted' end as Admitted
                                                                      
 from                $db_output.MPI a
 
 left join           global_temp.ServiceType s on a.person_id = s.person_id ---count of people so join on person_id
 left join           global_temp.Admitted ad on a.person_id = ad.person_id 
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.mhb_la la on la.level = a.LADistrictAuth
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 inner join           $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id  --make sure people in mpi table are also present in referral table
 GROUP BY            case when a.Der_Gender = '1' then '1'
                          when a.Der_Gender = '2' then '2'
                          when a.Der_Gender = '3' then '3'
                          when a.Der_Gender = '4' then '4'
                          when a.Der_Gender = '9' then '9'
                          else 'Unknown' end
                    ,a.AgeRepPeriodEnd
                    ,a.age_group_higher_level
                    ,case when AgeRepPeriodEnd between 0 and 5 then '0 to 5'
                      when AgeRepPeriodEnd between 6 and 10 then '6 to 10'
                      when AgeRepPeriodEnd between 11 and 15 then '11 to 15'
                      when AgeRepPeriodEnd = 16 then '16'
                      when AgeRepPeriodEnd = 17 then '17'
                      when AgeRepPeriodEnd = 18 then '18'
                      when AgeRepPeriodEnd = 19 then '19'
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end               
                    ,a.NHSDEthnicity
                    ,a.UpperEthnicity
                    ,a.LowerEthnicity   
                    ,case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                           when a.LADistrictAuth = 'L99999999' then 'L99999999'
                           when a.LADistrictAuth = 'M99999999' then 'M99999999'
                           when a.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when a.LADistrictAuth = '' then 'Unknown'
                           else la.level end
                     ,IMD_Decile
                     ,IMD_Quintile                 
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END
                     ,ServTeamTypeRefToMH
                     ,CASE WHEN ServTeamTypeRefToMH IN ('A08', 'A09', 'A16', 'A05', 'A13', 'A12', 'C10', 'A06') THEN 'Core Community Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A14' THEN 'Early Intervention Team for Psychosis'
                           WHEN ServTeamTypeRefToMH = 'D05' THEN 'Individual Placement and Support'
                           WHEN ServTeamTypeRefToMH = 'C02' THEN 'Specialist Perinatal Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A19' THEN '24/7 Crisis Response Line'
                           WHEN ServTeamTypeRefToMH IN ('A24', 'A21', 'A25', 'A03', 'A02', 'A20', 'A04', 'A23', 'A22') THEN 'Crisis and Acute Mental Health Activity in Community Settings'
                           WHEN ServTeamTypeRefToMH IN ('C05', 'A11') THEN 'Mental Health Activity in General Hospitals'
                           WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04') THEN 'LDA'
                           WHEN ServTeamTypeRefToMH IN ('B01', 'B02') THEN 'Forensic Services'
                           WHEN ServTeamTypeRefToMH = 'C01' THEN 'Autism Service' 
                           WHEN ServTeamTypeRefToMH IN ('C08', 'C06', 'C07', 'C04') THEN 'Specialist Services'
                           WHEN ServTeamTypeRefToMH IN ('A01', 'A07', 'A10', 'A17', 'A18', 'A15', 'D07', 'D01', 'D04', 'D08', 'D02', 'D03') THEN 'Other Mental Health Services'
                           WHEN ServTeamTypeReftoMH IN ('Z01', 'Z02') THEN 'Other'
                           ELSE 'Unknown' END
                     ,a.person_id
                     ,IC_Rec_CCG
                     ,stp.STP_code
                     ,stp.Region_code                 
                     ,CASE WHEN ad.person_id IS NOT NULL THEN 'Admitted' ELSE 'Non_Admitted' END
                     ;
 
 OPTIMIZE $db_output.people_in_contact ZORDER BY (Admitted);

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW people_prov AS
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 select                distinct x.OrgIDProv
                       ,x.person_id 
 from                  $db_output.MHB_MHS001MPI x
 inner join            $db_output.MHB_MHS101Referral ref 
                       on x.person_id = ref.person_id	
                       and x.OrgIDProv = ref.OrgIDProv		

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW admitted_prov AS
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 SELECT                DISTINCT A.OrgIDProv
                       ,A.Person_ID, HospitalBedTypeMH
 FROM                  $db_output.MHB_MHS001MPI A
 INNER JOIN            $db_output.MHB_MHS101Referral B 
                       ON a.Person_ID = B.Person_ID 
                       AND A.OrgIDProv = B.OrgIDProv
 INNER JOIN            $db_output.MHB_MHS501HospProvSpell C 
                       ON b.UniqServReqID = C.UniqServReqID
 LEFT JOIN             $db_output.MHB_MHS502WardStay D 
                       ON a.Person_ID = D.Person_ID ---count of people per provider so joining on person_id and orgidprov
                       AND A.OrgIDProv = D.OrgIDProv

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ServiceType_Prov AS
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 select              distinct a.OrgIDProv
                     ,a.Person_id
                     ,ServTeamTypeRefToMH
 from                $db_output.MHB_MHS102ServiceTypeReferredTo a
 inner join          $db_output.MPI b on a.Person_id = b.Person_id AND a.OrgIDProv = b.OrgIDProv
 inner join          $db_output.MHB_MHS101Referral c on b.Person_id = c.Person_id AND b.OrgIDProv = c.OrgIDProv ---count of people per provider so joining on person_id and orgidprov
 group by            a.OrgIDProv, a.Person_id, ServTeamTypeRefToMH

# COMMAND ----------

 %sql
 ---same tables as above just adding in Provider too. As someone could have been treated by more than one provider throughout the year
 DROP TABLE IF EXISTS $db_output.people_in_contact_prov;
 CREATE TABLE         $db_output.people_in_contact_prov AS
 SELECT               distinct case when a.Der_Gender = '1' then '1'
                                   when a.Der_Gender = '2' then '2'
                                   when a.Der_Gender = '3' then '3'
                                   when a.Der_Gender = '4' then '4'
                                   when a.Der_Gender = '9' then '9'
                                   else 'Unknown' end as Gender   
                     ,a.AgeRepPeriodEnd
                     ,a.age_group_higher_level
                     ,a.age_group_lower_chap1                    
                     ,a.NHSDEthnicity
                     ,a.UpperEthnicity
                     ,a.LowerEthnicity
                     ,a.OrgIDProv
                     ,IMD_Decile
                     ,IMD_Quintile
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END AS bed_type
                     ,ServTeamTypeRefToMH
                     ,CASE WHEN ServTeamTypeRefToMH IN ('A08', 'A09', 'A16', 'A05', 'A13', 'A12', 'C10', 'A06') THEN 'Core Community Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A14' THEN 'Early Intervention Team for Psychosis'
                           WHEN ServTeamTypeRefToMH = 'D05' THEN 'Individual Placement and Support'
                           WHEN ServTeamTypeRefToMH = 'C02' THEN 'Specialist Perinatal Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A19' THEN '24/7 Crisis Response Line'
                           WHEN ServTeamTypeRefToMH IN ('A24', 'A21', 'A25', 'A03', 'A02', 'A20', 'A04', 'A23', 'A22') THEN 'Crisis and Acute Mental Health Activity in Community Settings'
                           WHEN ServTeamTypeRefToMH IN ('C05', 'A11') THEN 'Mental Health Activity in General Hospitals'
                           WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04') THEN 'LDA'
                           WHEN ServTeamTypeRefToMH IN ('B01', 'B02') THEN 'Forensic Services'
                           WHEN ServTeamTypeRefToMH = 'C01' THEN 'Autism Service' 
                           WHEN ServTeamTypeRefToMH IN ('C08', 'C06', 'C07', 'C04') THEN 'Specialist Services'
                           WHEN ServTeamTypeRefToMH IN ('A01', 'A07', 'A10', 'A17', 'A18', 'A15', 'D07', 'D01', 'D04', 'D08', 'D02', 'D03') THEN 'Other Mental Health Services'
                           WHEN ServTeamTypeReftoMH IN ('Z01', 'Z02') THEN 'Other'
                           ELSE 'Unknown' END AS service_type
                     ,a.person_id                                      
                     ,case when ad.person_id IS not Null then 'Admitted' else 'Non_Admitted' end as Admitted                       
 from                $db_output.MPI_PROV a
 left join           global_temp.admitted_prov ad on a.Person_ID = ad.Person_ID and a.OrgIDProv = ad.OrgIDProv  --count of people per provider so join on person_id and orgidprov
 left join           global_temp.ServiceType_prov s on a.person_id = s.person_id AND a.OrgIDProv = s.OrgIDProv
 inner join           $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id AND a.OrgIDProv = b.OrgIDProv
 group by            case when a.Der_Gender = '1' then '1'
                           when a.Der_Gender = '2' then '2'
                           when a.Der_Gender = '3' then '3'
                           when a.Der_Gender = '4' then '4'
                           when a.Der_Gender = '9' then '9'
                           else 'Unknown' end 
                     ,a.AgeRepPeriodEnd
                     ,a.age_group_higher_level
                     ,a.age_group_lower_chap1                    
                     ,a.NHSDEthnicity
                     ,a.UpperEthnicity
                     ,a.LowerEthnicity
                     ,a.OrgIDProv
                     ,IMD_Decile
                     ,IMD_Quintile
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END
                     ,ServTeamTypeRefToMH
                     ,CASE WHEN ServTeamTypeRefToMH IN ('A08', 'A09', 'A16', 'A05', 'A13', 'A12', 'C10', 'A06') THEN 'Core Community Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A14' THEN 'Early Intervention Team for Psychosis'
                           WHEN ServTeamTypeRefToMH = 'D05' THEN 'Individual Placement and Support'
                           WHEN ServTeamTypeRefToMH = 'C02' THEN 'Specialist Perinatal Mental Health'
                           WHEN ServTeamTypeRefToMH = 'A19' THEN '24/7 Crisis Response Line'
                           WHEN ServTeamTypeRefToMH IN ('A24', 'A21', 'A25', 'A03', 'A02', 'A20', 'A04', 'A23', 'A22') THEN 'Crisis and Acute Mental Health Activity in Community Settings'
                           WHEN ServTeamTypeRefToMH IN ('C05', 'A11') THEN 'Mental Health Activity in General Hospitals'
                           WHEN ServTeamTypeRefToMH IN ('E01', 'E02', 'E03', 'E04') THEN 'LDA'
                           WHEN ServTeamTypeRefToMH IN ('B01', 'B02') THEN 'Forensic Services'
                           WHEN ServTeamTypeRefToMH = 'C01' THEN 'Autism Service' 
                           WHEN ServTeamTypeRefToMH IN ('C08', 'C06', 'C07', 'C04') THEN 'Specialist Services'
                           WHEN ServTeamTypeRefToMH IN ('A01', 'A07', 'A10', 'A17', 'A18', 'A15', 'D07', 'D01', 'D04', 'D08', 'D02', 'D03') THEN 'Other Mental Health Services'
                           WHEN ServTeamTypeReftoMH IN ('Z01', 'Z02') THEN 'Other'
                           ELSE 'Unknown' END
                     ,a.person_id                             
                     ,case when ad.person_id IS not Null then 'Admitted' else 'Non_Admitted' end

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW provider_type_admitted AS
 ---gets required breakdowns for Provider Type. Classifies Providers as 'NHS Providers' and 'Non NHS Providers'
 ---Admitted patients will be present in MHS501
 select            distinct case when a.Der_Gender = '1' then '1'
                                   when a.Der_Gender = '2' then '2'
                                   when a.Der_Gender = '3' then '3'
                                   when a.Der_Gender = '4' then '4'
                                   when a.Der_Gender = '9' then '9'
                                   else 'Unknown' end as gender 
                   ,mpi.AgeRepPeriodEnd
                   ,mpi.age_group_higher_level
                   ,mpi.age_group_lower_chap1 
                   ,mpi.NHSDEthnicity
              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known' 
                      ELSE 'Unknown' END AS UpperEthnicity
              ,CASE WHEN a.NHSDEthnicity = 'A' THEN 'British'
                    WHEN a.NHSDEthnicity = 'B' THEN 'Irish'
                    WHEN a.NHSDEthnicity = 'C' THEN 'Any other White background'
                    WHEN a.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
                    WHEN a.NHSDEthnicity = 'E' THEN 'White and Black African'
                    WHEN a.NHSDEthnicity = 'F' THEN 'White and Asian'
                    WHEN a.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
                    WHEN a.NHSDEthnicity = 'H' THEN 'Indian'
                    WHEN a.NHSDEthnicity = 'J' THEN 'Pakistani'
                    WHEN a.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                    WHEN a.NHSDEthnicity = 'L' THEN 'Any other Asian background'
                    WHEN a.NHSDEthnicity = 'M' THEN 'Caribbean'
                    WHEN a.NHSDEthnicity = 'N' THEN 'African'
                    WHEN a.NHSDEthnicity = 'P' THEN 'Any other Black background'
                    WHEN a.NHSDEthnicity = 'R' THEN 'Chinese'
                    WHEN a.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                      ELSE 'Unknown' END AS LowerEthnicity
                   ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
                         else 'Non NHS Providers' end as Provider_type 
                   ,d.OrgIDProv 
                   ,a.Person_id
 from              $db_output.MHB_MHS001MPI a
 inner join        $db_output.MHB_MHS101Referral d on a.Person_id = d.Person_id and a.OrgIDProv = d.OrgIDProv  ---count of people per provider so join on person_id and orgidprov
 inner join        $db_output.MHB_MHS501HospProvSpell b on b.UniqServReqID = d.UniqServReqID  ---inner join so people with hsopital spell only included
 left join         $db_output.MPI mpi on a.Person_id = mpi.Person_id ---make sure all people in edited mpi table are in live mpi table
 LEFT JOIN         global_temp.MHB_RD_ORG_DAILY_LATEST AS ORG ON a.OrgIDProv = ORG.ORG_CODE                
 GROUP BY      case when a.Der_Gender = '1' then '1'
                     when a.Der_Gender = '2' then '2'
                     when a.Der_Gender = '3' then '3'
                     when a.Der_Gender = '4' then '4'
                     when a.Der_Gender = '9' then '9'
                     else 'Unknown' end
                   ,mpi.AgeRepPeriodEnd
                   ,mpi.age_group_higher_level
                   ,mpi.age_group_lower_chap1 
                   ,mpi.NHSDEthnicity
              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                      ELSE 'Unknown' END 
              ,CASE WHEN a.NHSDEthnicity = 'A' THEN 'British'
                    WHEN a.NHSDEthnicity = 'B' THEN 'Irish'
                    WHEN a.NHSDEthnicity = 'C' THEN 'Any other White background'
                    WHEN a.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
                    WHEN a.NHSDEthnicity = 'E' THEN 'White and Black African'
                    WHEN a.NHSDEthnicity = 'F' THEN 'White and Asian'
                    WHEN a.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
                    WHEN a.NHSDEthnicity = 'H' THEN 'Indian'
                    WHEN a.NHSDEthnicity = 'J' THEN 'Pakistani'
                    WHEN a.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                    WHEN a.NHSDEthnicity = 'L' THEN 'Any other Asian background'
                    WHEN a.NHSDEthnicity = 'M' THEN 'Caribbean'
                    WHEN a.NHSDEthnicity = 'N' THEN 'African'
                    WHEN a.NHSDEthnicity = 'P' THEN 'Any other Black background'
                    WHEN a.NHSDEthnicity = 'R' THEN 'Chinese'
                    WHEN a.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                      ELSE 'Unknown' END 
                   ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
                         else 'Non NHS Providers' end 
                   ,d.OrgIDProv 
                   ,a.Person_id

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW provider_type_non_admitted AS
 ---Non-admitted patients will not be present in MHS501
 select             distinct case when a.Der_Gender = '1' then '1'
                                   when a.Der_Gender = '2' then '2'
                                   when a.Der_Gender = '3' then '3'
                                   when a.Der_Gender = '4' then '4'
                                   when a.Der_Gender = '9' then '9'
                                   else 'Unknown' end as gender 
                    ,mpi.AgeRepPeriodEnd
                    ,mpi.age_group_higher_level
                    ,mpi.age_group_lower_chap1 
                    ,mpi.NHSDEthnicity
              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                      ELSE 'Unknown' END AS UpperEthnicity
              ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'British'
                    WHEN mpi.NHSDEthnicity = 'B' THEN 'Irish'
                    WHEN mpi.NHSDEthnicity = 'C' THEN 'Any other White background'
                    WHEN mpi.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
                    WHEN mpi.NHSDEthnicity = 'E' THEN 'White and Black African'
                    WHEN mpi.NHSDEthnicity = 'F' THEN 'White and Asian'
                    WHEN mpi.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
                    WHEN mpi.NHSDEthnicity = 'H' THEN 'Indian'
                    WHEN mpi.NHSDEthnicity = 'J' THEN 'Pakistani'
                    WHEN mpi.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                    WHEN mpi.NHSDEthnicity = 'L' THEN 'Any other Asian background'
                    WHEN mpi.NHSDEthnicity = 'M' THEN 'Caribbean'
                    WHEN mpi.NHSDEthnicity = 'N' THEN 'African'
                    WHEN mpi.NHSDEthnicity = 'P' THEN 'Any other Black background'
                    WHEN mpi.NHSDEthnicity = 'R' THEN 'Chinese'
                    WHEN mpi.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                      ELSE 'Unknown' END AS LowerEthnicity
                    ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
                          else 'Non NHS Providers' end as Provider_type
                    ,a.Person_id
 from               $db_output.MHB_MHS001MPI a
 LEFT JOIN          global_temp.MHB_RD_ORG_DAILY_LATEST AS ORG ON a.OrgIDProv = ORG.ORG_CODE
 inner join         $db_output.MHB_MHS101Referral d on a.Person_id = d.Person_id and a.OrgIDProv = d.OrgIDProv
 left outer join    ( select distinct Person_id 
                                    ,Provider_type 
                      from global_temp.provider_type_admitted
                     ) b ----methodology: gather admitted cohort of patients, if not present in this cohort then class as 'Non Admitted'
                      on b.Person_id = d.Person_id 
                      and case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
                          else 'Non NHS Providers' end = b.Provider_type ---ASK AH
 left join           $db_output.MPI mpi 
                     on a.Person_id = mpi.Person_id
 where               b.Person_id is null ---no hopsital spell
 GROUP BY          case when a.Der_Gender = '1' then '1'
                         when a.Der_Gender = '2' then '2'
                         when a.Der_Gender = '3' then '3'
                         when a.Der_Gender = '4' then '4'
                         when a.Der_Gender = '9' then '9'
                         else 'Unknown' end
                    ,mpi.AgeRepPeriodEnd
                    ,mpi.age_group_higher_level
                    ,mpi.age_group_lower_chap1 
                    ,mpi.NHSDEthnicity
              ,CASE WHEN mpi.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                    WHEN mpi.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                    WHEN mpi.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                    WHEN mpi.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                    WHEN mpi.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                      ELSE 'Unknown' END 
              ,CASE WHEN mpi.NHSDEthnicity = 'A' THEN 'British'
                    WHEN mpi.NHSDEthnicity = 'B' THEN 'Irish'
                    WHEN mpi.NHSDEthnicity = 'C' THEN 'Any other White background'
                    WHEN mpi.NHSDEthnicity = 'D' THEN 'White and Black Caribbean'
                    WHEN mpi.NHSDEthnicity = 'E' THEN 'White and Black African'
                    WHEN mpi.NHSDEthnicity = 'F' THEN 'White and Asian'
                    WHEN mpi.NHSDEthnicity = 'G' THEN 'Any other Mixed background'
                    WHEN mpi.NHSDEthnicity = 'H' THEN 'Indian'
                    WHEN mpi.NHSDEthnicity = 'J' THEN 'Pakistani'
                    WHEN mpi.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                    WHEN mpi.NHSDEthnicity = 'L' THEN 'Any other Asian background'
                    WHEN mpi.NHSDEthnicity = 'M' THEN 'Caribbean'
                    WHEN mpi.NHSDEthnicity = 'N' THEN 'African'
                    WHEN mpi.NHSDEthnicity = 'P' THEN 'Any other Black background'
                    WHEN mpi.NHSDEthnicity = 'R' THEN 'Chinese'
                    WHEN mpi.NHSDEthnicity = 'S' THEN 'Any other ethnic group'
                    WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
                      ELSE 'Unknown' END 
                    ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' 
                          else 'Non NHS Providers' end
                    ,a.Person_id

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.people_in_contact_prov_type;
 CREATE TABLE         $db_output.people_in_contact_prov_type AS
 ---union on both admitted and non-admitted to create one final table
 SELECT              gender
                     ,AgeRepPeriodEnd
                     ,age_group_higher_level
                     ,age_group_lower_chap1 
                     ,NHSDEthnicity
                     ,UpperEthnicity
                     ,LowerEthnicity
                     ,Provider_type
                     ,'Admitted' as Admitted
                     ,COUNT(distinct Person_id) as people 
 FROM                global_temp.provider_type_admitted
 GROUP BY            gender
                     ,AgeRepPeriodEnd
                     ,age_group_higher_level
                     ,age_group_lower_chap1 
                     ,NHSDEthnicity
                     ,UpperEthnicity
                     ,LowerEthnicity
                     ,Provider_type
                     
 UNION ALL
 
 SELECT              gender
                     ,AgeRepPeriodEnd
                     ,age_group_higher_level
                     ,age_group_lower_chap1 
                     ,NHSDEthnicity
                     ,UpperEthnicity
                     ,LowerEthnicity
                     ,Provider_type
                     ,'Non_Admitted' as Admitted
                     ,COUNT(distinct Person_id) as people 
 FROM                global_temp.provider_type_non_admitted
 GROUP BY            gender
                     ,AgeRepPeriodEnd
                     ,age_group_higher_level
                     ,age_group_lower_chap1 
                     ,NHSDEthnicity
                     ,UpperEthnicity
                     ,LowerEthnicity
                     ,Provider_type