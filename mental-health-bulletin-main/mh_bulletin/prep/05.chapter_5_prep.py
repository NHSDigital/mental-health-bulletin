# Databricks notebook source
 %sql
 DROP TABLE IF EXISTS $db_output.admissions_discharges;
 CREATE TABLE        $db_output.admissions_discharges AS
 ---final table for admissions with required breakdowns and bed type.
 select distinct a.person_id
 ,case when a.Der_Gender = '1' then '1'
       when a.Der_Gender = '2' then '2'
       when a.Der_Gender = '3' then '3'
       when a.Der_Gender = '4' then '4'
       when a.Der_Gender = '9' then '9'
       else 'Unknown' end as Gender
 ,a.AgeRepPeriodEnd
 ,a.age_group_higher_level                    
 ,a.age_group_lower_chap45
 ,a.NHSDEthnicity
 ,a.UpperEthnicity
 ,a.LowerEthnicity
 ,IMD_Decile
 ,IMD_Quintile
 ,HospitalBedTypeMH                     
 ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
       WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
       ELSE 'Invalid' END AS bed_type
 ,a.OrgIDProv
 ,case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                           when a.LADistrictAuth = 'L99999999' then 'L99999999'
                           when a.LADistrictAuth = 'M99999999' then 'M99999999'
                           when a.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when a.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth              
 
 ,IC_Rec_CCG
 ,stp.STP_code
 ,stp.Region_code
 ,hs.StartDateHospProvSpell 
 ,hs.DischDateHospProvSpell 
 ,hs.UniqHospProvSpellID  ---count will then be taken in aggregation phase with where clause on StartDateHospProvSpell and DischDateHospProvSpell to disgtinguish between admissions and discharges
 from $db_output.MHB_MHS501HospProvSpell hs
 left join $db_output.mpi a on a.person_id = hs.person_id --join on person_id for demographic information
 left join $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join global_temp.la la on la.level = a.LADistrictAuth
 left join $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code
 left join $db_output.MHB_MHS502wardstay ws on hs.UniqHospProvSpellID = ws.UniqHospProvSpellID --as this is an activity, join on UniqHospProvSpellNum

# COMMAND ----------

 %sql
 ---final table for admissions with required breakdowns apart from bed type
 DROP TABLE IF EXISTS $db_output.admissions_discharges_prov;
 CREATE TABLE        $db_output.admissions_discharges_prov AS
 select distinct a.person_id
 ,case when a.Der_Gender = '1' then '1'
       when a.Der_Gender = '2' then '2'
       when a.Der_Gender = '3' then '3'
       when a.Der_Gender = '4' then '4'
       when a.Der_Gender = '9' then '9'
       else 'Unknown' end as Gender
 ,a.AgeRepPeriodEnd
 ,a.age_group_higher_level                    
 ,a.age_group_lower_chap45
 ,a.NHSDEthnicity
 ,a.UpperEthnicity
 ,a.LowerEthnicity
 ,IMD_Decile
 ,IMD_Quintile
 ,HospitalBedTypeMH                     
 ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
       WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
       WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
       WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
       WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
       ELSE 'Invalid' END AS bed_type
 ,a.OrgIDProv
 ,case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                           when a.LADistrictAuth = 'L99999999' then 'L99999999'
                           when a.LADistrictAuth = 'M99999999' then 'M99999999'
                           when a.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when a.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth              
 
 ,IC_Rec_CCG
 ,stp.STP_code
 ,stp.Region_code
 ,hs.StartDateHospProvSpell 
 ,hs.DischDateHospProvSpell 
 ,hs.UniqHospProvSpellID  ---count will then be taken in aggregation phase with where clause on StartDateHospProvSpell and DischDateHospProvSpell to disgtinguish between admissions and discharges
 from $db_output.MHB_MHS501HospProvSpell hs
 left join $db_output.mpi_prov a on a.person_id = hs.person_id and a.orgidprov = hs.orgidprov ---join on person_id and orgidprov for demographic information
 left join $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join global_temp.la la on la.level = a.LADistrictAuth
 left join $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code
 left join $db_output.MHB_MHS502wardstay ws on hs.UniqHospProvSpellID = ws.UniqHospProvSpellID ---as this is an activity, join on UniqHospProvSpellID