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

# DBTITLE 1,Care Cluster temp table part 1
 %sql
 ---gets required Care Cluster Info
 CREATE OR REPLACE GLOBAL TEMP VIEW StartDateCareClustMax AS
 SELECT			distinct PRSN.Person_id
 				,MAX (CC.StartDateCareClust) AS StartDateCareClustMAX
 FROM			$db_output.MHB_MHS001MPI AS PRSN
 INNER JOIN      $db_output.MHB_MHS801ClusterTool AS CCT
 					ON PRSN.Person_ID = CCT.Person_ID 
                     AND CCT.uniqmonthid = '$end_month_id' 
 INNER JOIN      $db_output.MHB_MHS803CareCluster AS CC
 					ON CCT.UniqClustID = CC.UniqClustID 
                     AND CC.uniqmonthid = '$end_month_id' 
 INNER JOIN      $db_output.MHB_MHS101Referral AS REF
 					ON PRSN.Person_id = REF.Person_id 
                     AND REF.uniqmonthid = '$end_month_id' 
 WHERE			PRSN.uniqmonthid = '$end_month_id' 
                 AND (CC.EndDateCareClust IS NULL OR CC.EndDateCareClust > '$rp_enddate') --care clusters assigned before end of RP
                 AND CC.AMHCareClustCodeFin IN ('00', '0', '01', '1', '02', '2', '03', '3', '04', '4', '05', '5', '06', '6', '07', '7', '08', '8', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21')
                 AND (REF.ServDischDate IS NULL OR REF.ServDischDate > '$rp_enddate')
 GROUP BY		PRSN.Person_id

# COMMAND ----------

# DBTITLE 1,Care Cluster temp table part 2
 %sql
 --gets latest MHS803ID for each person
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS803UniqIDMAX AS
 SELECT			distinct PRSN.Person_id
 				,MAX (CC.MHS803UniqID) AS MHS803UniqIDMAX
 FROM			$db_output.MHB_MHS001MPI AS PRSN
 				INNER JOIN $db_output.MHB_MHS801ClusterTool AS CCT
 					ON PRSN.Person_id = CCT.Person_id 
                     AND CCT.uniqmonthid = '$end_month_id' ---open at end of rp
 				INNER JOIN $db_output.MHB_MHS803CareCluster AS CC
 					ON CCT.UniqClustID = CC.UniqClustID    ---counting of number of care clusters so join on UniqClustID
                     AND CC.uniqmonthid = '$end_month_id' 
 				INNER JOIN $db_output.MHB_MHS101Referral AS REF
 					ON PRSN.Person_id = REF.Person_id 
                     AND REF.uniqmonthid = '$end_month_id' 
 				INNER JOIN global_temp.StartDateCareClustMax AS SDCCMAX
 					ON CC.StartDateCareClust = SDCCMAX.StartDateCareClustMAX 
                     AND CC.Person_id = SDCCMAX.Person_id
 WHERE			PRSN.uniqmonthid = '$end_month_id' 
                 AND (CC.EndDateCareClust IS NULL OR CC.EndDateCareClust > '$rp_enddate')
                 AND CC.AMHCareClustCodeFin IN ('00', '0', '01', '1', '02', '2', '03', '3', '04', '4', '05', '5', '06', '6', '07', '7', '08', '8', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21')
                 AND (REF.ServDischDate IS NULL OR REF.ServDischDate > '$rp_enddate') ---open at end of rp
 GROUP BY		PRSN.Person_id

# COMMAND ----------

# DBTITLE 1,Care Cluster temp table part 3
 %sql
 ---grouping Care Cluster Codes into Cluster Classes
 CREATE OR REPLACE GLOBAL TEMP VIEW careclusterlist AS
 select          distinct   
                 case when AMHCareClustCodeFin IN ('1','2','3','4','5','6','7','8','01','02','03','04','05','06','07','08') THEN 'Non Psychotic'
                      when AMHCareClustCodeFin IN ('10','11','12','13','14','15','16','17') THEN 'Psychotic'
                      when AMHCareClustCodeFin IN ('18','19','20','21') THEN 'Organic'
                      else 'Unknown' end as Cluster_super_class
                 ,a.Person_ID
 from            global_temp.MHS803UniqIDMAX a
 left join       $db_output.MHB_MHS803CareCluster b 
                     on a.MHS803UniqIDMAX = b.MHS803UniqID 
                     and a.Person_id = b.Person_id 
 where           AMHCareClustCodeFin IN ('00', '0', '01', '1', '02', '2', '03', '3', '04', '4', '05', '5', '06', '6', '07', '7', '08', '8', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21')

# COMMAND ----------

# DBTITLE 1,Final table - AMH Cluster Class
 %sql
 --creates final table with neccessary breakdowns for Chapter 3 measure values.
 DROP TABLE IF EXISTS $db_output.people_in_contact_cluster;
 CREATE TABLE        $db_output.people_in_contact_cluster AS
 select              distinct case when a.Der_Gender = '1' then '1'
                                   when a.Der_Gender = '2' then '2'
                                   when a.Der_Gender = '3' then '3'
                                   when a.Der_Gender = '4' then '4'
                                   else 'Unknown' end as Gender   
                     ,a.AgeRepPeriodEnd
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
                     ,IC_Rec_CCG
                     ,stp.STP_code
                     ,stp.Region_code
                     ,Cluster_super_class                    
                     ,a.person_id                          
 from                $db_output.MPI a
 inner join           $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id
 left join           global_temp.careclusterlist clst on a.person_id = clst.person_id
 left join           $db_output.CCG_final ccg on a.Person_id = ccg.person_id
 left join           global_temp.la la on la.level = case when LEFT(a.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(a.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(a.LADistrictAuth,1) = 'W' then 'W'
                                         when a.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when a.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when a.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when a.LADistrictAuth = '' then 'Unknown'
                                         when a.LADistrictAuth is not null then a.LADistrictAuth
                                         else 'Unknown' end 
 left join           $db_output.STP_Region_mapping stp on ccg.IC_Rec_CCG = stp.CCG_code ------ Need to make sure using correct table here
 GROUP BY            case when a.Der_Gender = '1' then '1'
                           when a.Der_Gender = '2' then '2'
                           when a.Der_Gender = '3' then '3'
                           when a.Der_Gender = '4' then '4'
                           else 'Unknown' end
                          ,a.AgeRepPeriodEnd
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
                     ,IC_Rec_CCG
                     ,stp.STP_code
                     ,stp.Region_code
                     ,Cluster_super_class
                     ,a.person_id

# COMMAND ----------

# DBTITLE 1,Care Cluster Provider temp table part 1
 %sql
 ---same as table below but for Provider breakdown. A person may have been treated in more than one provider, could contain different person information
 CREATE OR REPLACE GLOBAL TEMP VIEW StartDateCareClustMaxProv AS
 SELECT			      distinct PRSN.person_id
                       ,PRSN.OrgIDProv
                       ,MAX (CC.StartDateCareClust) AS StartDateCareClustMAX
 FROM			      $db_output.MHB_MHS001MPI AS PRSN
                       INNER JOIN $db_output.MHB_MHS801ClusterTool AS CCT
                       ON PRSN.person_id = CCT.person_id 
                       AND CCT.uniqMonthID = '$end_month_id'
                       AND PRSN.OrgIDProv = CCT.OrgIDProv
                       INNER JOIN $db_output.MHB_MHS803CareCluster AS CC
                       ON CCT.UniqClustID = CC.UniqClustID 
                       AND CC.uniqMonthID = '$end_month_id'
                       INNER JOIN $db_output.MHB_MHS101Referral AS REF
                       ON PRSN.person_id = REF.person_id 
                       AND REF.uniqMonthID = '$end_month_id' 
                       AND REF.OrgIDProv = PRSN.OrgIDProv
 WHERE			      PRSN.uniqMonthID = '$end_month_id'
                       AND (CC.EndDateCareClust IS NULL OR CC.EndDateCareClust > '$rp_enddate')
                       AND CC.AMHCareClustCodeFin IN ('00', '0', '01', '1', '02', '2', '03', '3', '04', '4', '05', '5', '06', '6', '07', '7', '08', '8', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21')
                       AND (REF.ServDischDate IS NULL OR REF.ServDischDate > '$rp_enddate')
 GROUP BY		      PRSN.person_id
                       ,PRSN.OrgIDProv

# COMMAND ----------

# DBTITLE 1,Care Cluster Provider temp table part 2
 %sql
 ---same as table below but for Provider breakdown. A person may have been treated in more than one provider, could contain different person information
 CREATE OR REPLACE GLOBAL TEMP VIEW MHS803UniqIDMAXProv AS
 SELECT			      distinct PRSN.Person_ID
                       ,PRSN.OrgIDProv
                       ,MAX (CC.MHS803UniqID) AS MHS803UniqIDMAX
 FROM			      $db_output.MHB_MHS001MPI AS PRSN
 				      INNER JOIN $db_output.MHB_MHS801ClusterTool AS CCT
                       ON PRSN.Person_ID = CCT.Person_ID 
                       AND CCT.uniqmonthid = '$end_month_id' 
                       AND CCT.OrgIDProv = PRSN.OrgIDProv
                       INNER JOIN $db_output.MHB_MHS803CareCluster AS CC
                       ON CCT.UniqClustID = CC.UniqClustID 
                       AND CC.uniqmonthid = '$end_month_id'
                       INNER JOIN $db_output.MHB_MHS101Referral AS REF
                       ON PRSN.Person_ID = REF.Person_ID 
                       AND REF.uniqmonthid = '$end_month_id' 
                       AND PRSN.OrgIDProv = REF.OrgIDProv
                       INNER JOIN global_temp.StartDateCareClustMaxProv AS SDCCMAX
                       ON CC.StartDateCareClust = SDCCMAX.StartDateCareClustMAX 
                       AND CC.Person_ID = SDCCMAX.Person_ID 
                       AND CC.OrgIDProv = SDCCMAX.OrgIDProv
 WHERE			      PRSN.uniqmonthid = '$end_month_id'
                       AND (CC.EndDateCareClust IS NULL OR CC.EndDateCareClust > '$rp_enddate')
                       AND CC.AMHCareClustCodeFin IN ('00', '0', '01', '1', '02', '2', '03', '3', '04', '4', '05', '5', '06', '6', '07', '7', '08', '8', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21')
                       AND (REF.ServDischDate IS NULL OR REF.ServDischDate > '$rp_enddate')
 GROUP BY		      PRSN.Person_ID
                       ,PRSN.OrgIDProv

# COMMAND ----------

# DBTITLE 1,Care Cluster Provider temp table part 3
 %sql
 ---same as table below but for Provider breakdown. A person may have been treated in more than one provider, could contain different person information
 CREATE OR REPLACE GLOBAL TEMP VIEW careclusterlistProv AS
 Select                distinct case when AMHCareClustCodeFin IN ('1','2','3','4','5','6','7','8','01','02','03','04','05','06','07','08') THEN 'Non Psychotic'
                                     when AMHCareClustCodeFin IN ('10','11','12','13','14','15','16','17') THEN 'Psychotic'
                                     when AMHCareClustCodeFin IN ('18','19','20','21') THEN 'Organic'
                                     else 'Unknown' end as Cluster_super_class
                       ,a.OrgIDProv
                       ,a.Person_ID
 from                  global_temp.MHS803UniqIDMAXProv a
 left join             $db_output.MHB_MHS803CareCluster b 
                       on a.MHS803UniqIDMAX = b.MHS803UniqID 
                       and a.Person_ID = b.Person_ID
 where                 AMHCareClustCodeFin IN ('00', '0', '01', '1', '02', '2', '03', '3', '04', '4', '05', '5', '06', '6', '07', '7', '08', '8', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21')

# COMMAND ----------

# DBTITLE 1,Final table - AMH Cluster Class - Provider
 %sql
 ---same as table below but for Provider breakdown. A person may have been treated in more than one provider, could contain different person information
 DROP TABLE IF EXISTS $db_output.people_in_contact_cluster_prov;
 CREATE TABLE         $db_output.people_in_contact_cluster_prov AS
 SELECT               distinct case when a.Der_Gender = '1' then '1'
                                   when a.Der_Gender = '2' then '2'
                                   when a.Der_Gender = '3' then '3'
                                   when a.Der_Gender = '4' then '4'
                                   else 'Unknown' end as Gender   
                     ,a.AgeRepPeriodEnd
                     ,a.NHSDEthnicity
                     ,a.UpperEthnicity
                     ,a.LowerEthnicity 
                     ,a.OrgIDProv
                     ,Cluster_super_class                   
                     ,a.person_id                      
 from                $db_output.MPI_PROV a
 inner join           $db_output.MHB_MHS101Referral b on a.Person_id = b.Person_id AND a.OrgIDProv = b.OrgIDProv
 left join           global_temp.careclusterlistprov clst 
                     on a.Person_ID = clst.Person_ID 
                     and a.OrgIDProv = clst.OrgIDProv
 group by            case when a.Der_Gender = '1' then '1'
                           when a.Der_Gender = '2' then '2'
                           when a.Der_Gender = '3' then '3'
                           when a.Der_Gender = '4' then '4'
                           else 'Unknown' end
                     ,a.AgeRepPeriodEnd
                     ,a.NHSDEthnicity
                     ,a.UpperEthnicity
                     ,a.LowerEthnicity 
                     ,a.OrgIDProv
                     ,Cluster_super_class                   
                     ,a.person_id         

# COMMAND ----------

