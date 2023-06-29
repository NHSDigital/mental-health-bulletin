# Databricks notebook source
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bedtype AS
 ---get distinct person_id, bed type and associated bed days in the financial year
 select            distinct Person_id
                   ,HospitalBedTypeMH
                   ,sum(datediff(CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                                       WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                                       ELSE EndDateWardStay END
                                       ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                         ELSE StartDateWardStay END)) as Beddays                                        
 from              $db_output.MHB_MHS502WardStay
 group by          Person_id, HospitalBedTypeMH

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.beddays;
 CREATE TABLE        $db_output.beddays AS
 ---national and commissioning level final prep table for bed days
 select              distinct case when a.Der_Gender = '1' then '1'
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
                     ,IC_Rec_CCG
                     ,stp.STP_code
                     ,stp.Region_code 
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END AS bed_type                  
                     ,sum(Beddays) as Beddays                                            
 from                $db_output.MPI a
 left join           global_temp.bedtype bd on bd.person_id = a.person_id 
 left join           $db_output.CCG_final ccg on a.person_id = ccg.person_id
 left join           $db_output.mhb_la la on la.level = a.LADistrictAuth
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 GROUP BY            case when a.Der_Gender = '1' then '1'
                           when a.Der_Gender = '2' then '2'
                           when a.Der_Gender = '3' then '3'
                           when a.Der_Gender = '4' then '4'
                           when a.Der_Gender = '9' then '9'
                           else 'Unknown' end
                    ,a.AgeRepPeriodEnd
                    ,a.NHSDEthnicity
                    ,a.AgeRepPeriodEnd
                    ,a.age_group_higher_level                   
                    ,a.age_group_lower_chap45
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
                     ,IC_Rec_CCG
                     ,stp.STP_code
                     ,stp.Region_code
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END                ;

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW bedtype_prov AS
 ---get distinct provider, person_id, bed type and associated bed days in the financial year
 select                distinct  OrgIDProv
                       ,Person_ID 
                       ,HospitalBedTypeMH
                       ,sum(datediff( CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD('$rp_enddate',1)
                             WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                             ELSE EndDateWardStay END
                             ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                             ELSE StartDateWardStay END)) as Beddays
 from                  $db_output.MHB_MHS502WardStay 
 group by              OrgIDProv
                       ,Person_ID
                       ,HospitalBedTypeMH

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.beddays_prov;
 CREATE TABLE         $db_output.beddays_prov AS
 ---same table as $db_output.beddays with provider breakdown added. As someone could have been admitted by more than one provider throughout the year
 SELECT               distinct case when a.Der_Gender = '1' then '1'
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
                     ,a.OrgIDProv                 
                     ,IMD_Quintile
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END AS bed_type                   
                     ,sum(Beddays) as beddays                                        
 from                $db_output.MPI_PROV a
 left join           global_temp.bedtype_prov bed 
                     on bed.Person_ID = a.Person_ID 
                     and bed.OrgIDProv = a.OrgIDProv
 group by            case when a.Der_Gender = '1' then '1'
                           when a.Der_Gender = '2' then '2'
                           when a.Der_Gender = '3' then '3'
                           when a.Der_Gender = '4' then '4'
                           when a.Der_Gender = '9' then '9'
                           else 'Unknown' end
                     ,a.AgeRepPeriodEnd
                     ,a.age_group_higher_level             
                     ,a.age_group_lower_chap45
                     ,a.NHSDEthnicity
                     ,a.UpperEthnicity
                     ,a.LowerEthnicity 
                     ,a.OrgIDProv                 
                     ,IMD_Quintile
                     ,HospitalBedTypeMH
                     ,CASE WHEN HospitalBedTypeMH IN ('23', '24', '34') THEN 'CYP Acute'
                           WHEN HospitalBedTypeMH IN ('29', '25', '26', '30', '27', '31', '28', '32', '33') THEN 'CYP Specialist'
                           WHEN HospitalBedTypeMH IN ('10', '12') THEN 'Adult Acute'
                           WHEN HospitalBedTypeMH IN ('13', '17', '21', '15', '18', '19', '16', '20', '14', '22') THEN 'Adult Specialist'
                           WHEN HospitalBedTypeMH = '11' THEN 'Older Adult Acute'
                           ELSE 'Invalid' END                 

# COMMAND ----------

 %sql
 
 DROP TABLE IF EXISTS $db_output.beddays_provider_type;
 CREATE TABLE         $db_output.beddays_provider_type AS
 ---same table as $db_output.beddays with provider type breakdown added. As someone could have been admitted by more than one provider type throughout the year
 SELECT              case when mpi.Der_Gender = '1' then '1'
                                   when mpi.Der_Gender = '2' then '2'
                                   when mpi.Der_Gender = '3' then '3'
                                   when mpi.Der_Gender = '4' then '4'
                                   when mpi.Der_Gender = '9' then '9'
                                   else 'Unknown' end as Gender   
                     ,mpi.AgeRepPeriodEnd
                     ,wrd.Person_id
                     ,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' else 'Non NHS Providers' end as Provider_type
                     ,sum(datediff(
                                     CASE WHEN (EndDateWardStay IS NULL AND InactTimeWS IS NULL) THEN DATE_ADD ('$rp_enddate',1)
                                           WHEN InactTimeWS IS NOT NULL THEN InactTimeWS
                                           ELSE EndDateWardStay END
                                         ,CASE WHEN StartDateWardStay < '$rp_startdate' THEN '$rp_startdate'
                                           ELSE StartDateWardStay END)) as Beddays
 FROM                $db_output.MHB_MHS502WardStay wrd
 LEFT JOIN           global_temp.MHB_RD_ORG_DAILY_LATEST AS ORG ON wrd.OrgIDProv = ORG.ORG_CODE
 LEFT JOIN           $db_output.mpi mpi 
                     on mpi.Person_id = wrd.Person_id
 GROUP BY            case when mpi.Der_Gender = '1' then '1'
                           when mpi.Der_Gender = '2' then '2'
                           when mpi.Der_Gender = '3' then '3'
                           when mpi.Der_Gender = '4' then '4'
                           when mpi.Der_Gender = '9' then '9'
                           else 'Unknown' end 
                     ,mpi.AgeRepPeriodEnd
                     ,wrd.Person_id,case when org.ORG_CODE like 'R%' or org.ORG_CODE like 'T%' then 'NHS Providers' else 'Non NHS Providers' end