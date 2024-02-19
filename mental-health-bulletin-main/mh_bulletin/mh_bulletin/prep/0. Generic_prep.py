# Databricks notebook source
# dbutils.widgets.text("start_month_id", "1453")
# dbutils.widgets.text("end_month_id", "1464")
# dbutils.widgets.text("db_source", "mhsds_database")
# dbutils.widgets.text("status", "Final")

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

# DBTITLE 1,MPI temp table part 1
 %sql
 --Gets the max uniqmonthid for each person
 CREATE OR REPLACE GLOBAL TEMP VIEW MPI_max_month AS
 SELECT			x.Person_id
 				,MAX(x.uniqmonthid) AS uniqmonthid --change back to monthid for 19-20 (record_number for 18/9)
 FROM			$db_output.MHB_MHS001MPI x	
 where           PatMRecInRP = true
 GROUP BY		x.Person_id

# COMMAND ----------

# DBTITLE 1,MPI temp table part 2
 %sql
 --joins max month id back onto mpi table to get latest information for each person. Uses PatMrecInRP for unique person details within same month
 CREATE OR REPLACE GLOBAL TEMP VIEW MPI_part1 AS
 Select          distinct x.*
 from            $db_output.MHB_MHS001MPI x  
 INNER JOIN      global_temp.MPI_max_month AS z
                 ON x.Person_id = z.Person_id 
                 AND x.uniqmonthid = z.uniqmonthid 
 where           x.PatMRecInRP = true

# COMMAND ----------

# DBTITLE 1,Creates final MPI demogrpahics table
 %sql
 
 --incorporates majority of required demographic breakdowns into the final mpi table
 
 DROP TABLE IF EXISTS $db_output.MPI;
 CREATE TABLE         $db_output.MPI AS
 select          a.*,
                 g.Der_Gender_Desc,
                 case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                      when AgeRepPeriodEnd >= 18 then '18 and over' 
                      else 'Unknown' end as age_group_higher_level,
                case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                      when AgeRepPeriodEnd between 18 and 19 then '18 to 19'
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' 
                      else 'Unknown' end as age_group_lower_common,
                 case when AgeRepPeriodEnd between 0 and 5 then '0 to 5'
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' 
                      else 'Unknown' end as age_group_lower_chap1,
                  CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 13 THEN 'Under 14'
                       WHEN AgeRepPeriodEnd BETWEEN 14 and 15 THEN '14 to 15'
                       WHEN AgeRepPeriodEnd BETWEEN 16 and 17 THEN '16 to 17'
                       WHEN AgeRepPeriodEnd BETWEEN 18 AND 19 THEN '18 to 19'
                       WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                       WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                       WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                       WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                       WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                       WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                       WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                       WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                       WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                       WHEN AgeRepPeriodEnd BETWEEN 65 AND 69 THEN '65 to 69'
                       WHEN AgeRepPeriodEnd BETWEEN 70 AND 74 THEN '70 to 74'
                       WHEN AgeRepPeriodEnd BETWEEN 75 AND 79 THEN '75 to 79'
                       WHEN AgeRepPeriodEnd BETWEEN 80 AND 84 THEN '80 to 84'
                       WHEN AgeRepPeriodEnd BETWEEN 85 AND 89 THEN '85 to 89' 
                       WHEN AgeRepPeriodEnd >= 90 THEN '90 or over' 
                       else 'Unknown' END As age_group_lower_chap45,
                   CASE WHEN AgeRepPeriodEnd BETWEEN 14 AND 15 THEN '14 to 15'
                        WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
                        WHEN AgeRepPeriodEnd BETWEEN 18 AND 19 THEN '18 to 19'
                        WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeRepPeriodEnd > 64 THEN '65 or over' 
                        ELSE 'Unknown' END as age_group_lower_chap10, 
                        
                   case when AgeRepPeriodEnd between 0 and 13 then '0 to 13'
                       when AgeRepPeriodEnd between 14 and 17 then "14 to 17"
                       when AgeRepPeriodEnd between 18 and 19 then "18 to 19"
                       WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                       when AgeRepPeriodEnd between 65 and 69 then "65 to 69"
                       when AgeRepPeriodEnd between 70 and 74 then "70 to 74"
                       when AgeRepPeriodEnd between 75 and 79 then "75 to 79"
                       when AgeRepPeriodEnd between 80 and 84 then "80 to 84"
                       when AgeRepPeriodEnd between 85 and 89 then "80 to 84"
                       when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_chap10a,
                        
                   CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 14 THEN 'Under 15'
                        WHEN AgeRepPeriodEnd BETWEEN 15 AND 19 THEN '15 to 19'
                        WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeRepPeriodEnd > 64 THEN '65 or over' 
                        ELSE 'Unknown' END as age_group_lower_chap11,                    
                    CASE WHEN a.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                         WHEN a.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                         WHEN a.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                         WHEN a.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                         WHEN a.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                         WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
                         WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
                         ELSE 'Unknown' END AS UpperEthnicity,
              CASE WHEN a.NHSDEthnicity = 'A' THEN 'A'
                    WHEN a.NHSDEthnicity = 'B' THEN 'B'
                    WHEN a.NHSDEthnicity = 'C' THEN 'C'
                    WHEN a.NHSDEthnicity = 'D' THEN 'D'
                    WHEN a.NHSDEthnicity = 'E' THEN 'E'
                    WHEN a.NHSDEthnicity = 'F' THEN 'F'
                    WHEN a.NHSDEthnicity = 'G' THEN 'G'
                    WHEN a.NHSDEthnicity = 'H' THEN 'H'
                    WHEN a.NHSDEthnicity = 'J' THEN 'J'
                    WHEN a.NHSDEthnicity = 'K' THEN 'K'
                    WHEN a.NHSDEthnicity = 'L' THEN 'L'
                    WHEN a.NHSDEthnicity = 'M' THEN 'M'
                    WHEN a.NHSDEthnicity = 'N' THEN 'N'
                    WHEN a.NHSDEthnicity = 'P' THEN 'P'
                    WHEN a.NHSDEthnicity = 'R' THEN 'R'
                    WHEN a.NHSDEthnicity = 'S' THEN 'S'
                    WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
                    ELSE 'Unknown' END AS LowerEthnicityCode,
                CASE WHEN a.NHSDEthnicity = 'A' THEN 'British'
                     WHEN a.NHSDEthnicity = 'B' THEN 'Irish'
                     WHEN a.NHSDEthnicity = 'C' THEN 'Any Other White Background'
                     WHEN a.NHSDEthnicity = 'D' THEN 'White and Black Caribbean' 
                     WHEN a.NHSDEthnicity = 'E' THEN 'White and Black African'
                     WHEN a.NHSDEthnicity = 'F' THEN 'White and Asian'
                     WHEN a.NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
                     WHEN a.NHSDEthnicity = 'H' THEN 'Indian'
                     WHEN a.NHSDEthnicity = 'J' THEN 'Pakistani'
                     WHEN a.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                     WHEN a.NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
                     WHEN a.NHSDEthnicity = 'M' THEN 'Caribbean'
                     WHEN a.NHSDEthnicity = 'N' THEN 'African'
                     WHEN a.NHSDEthnicity = 'P' THEN 'Any Other Black Background'
                     WHEN a.NHSDEthnicity = 'R' THEN 'Chinese'
                     WHEN a.NHSDEthnicity = 'S' THEN 'Any Other Ethnic Group'
                     WHEN a.NHSDEthnicity = 'Z' THEN 'Not Stated'
                     WHEN a.NHSDEthnicity = '99' THEN 'Not Known'
                     ELSE 'Unknown' END AS LowerEthnicityName,
                 CASE
                 WHEN r.DECI_IMD = 10 THEN '10 Least deprived'
                 WHEN r.DECI_IMD = 9 THEN '09 Less deprived'
                 WHEN r.DECI_IMD = 8 THEN '08 Less deprived'
                 WHEN r.DECI_IMD = 7 THEN '07 Less deprived'
                 WHEN r.DECI_IMD = 6 THEN '06 Less deprived'
                 WHEN r.DECI_IMD = 5 THEN '05 More deprived'
                 WHEN r.DECI_IMD = 4 THEN '04 More deprived'
                 WHEN r.DECI_IMD = 3 THEN '03 More deprived'
                 WHEN r.DECI_IMD = 2 THEN '02 More deprived'
                 WHEN r.DECI_IMD = 1 THEN '01 Most deprived'
                 ELSE 'Unknown'
                 END AS IMD_Decile,
                 CASE 
                 WHEN r.DECI_IMD IN (9, 10) THEN '05 Least deprived'
                 WHEN r.DECI_IMD IN (7, 8) THEN '04'
                 WHEN r.DECI_IMD IN (5, 6) THEN '03'
                 WHEN r.DECI_IMD IN (3, 4) THEN '02'
                 WHEN r.DECI_IMD IN (1, 2) THEN '01 Most deprived'                          
                 ELSE 'Unknown' 
                 END AS IMD_Quintile
 from            global_temp.MPI_part1 a
 left join       $reference_db.ENGLISH_INDICES_OF_DEP_V02 r 
                     on a.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 left join       $db_output.Der_Gender g on a.Der_Gender = g.Der_Gender                  

# COMMAND ----------

# DBTITLE 1,Provider level mpi temp table
 %sql
 
 --A person may have been treated in more than one provider, could contain different person information. For the purposes of provider breakdowns, require 1 set of person details per provider
 
 CREATE OR REPLACE GLOBAL TEMP VIEW MPI_max_month_prov AS
 SELECT			x.orgidprov
                 ,x.Person_id
 				,MAX(x.uniqmonthid) AS uniqmonthid
 FROM			$db_output.MHB_MHS001MPI x	
 GROUP BY		x.orgidprov
                 ,x.Person_id

# COMMAND ----------

# DBTITLE 1,Provider level MPI table
 %sql
 --incorporates majority of required demographic breakdowns into the final mpi provider table
 DROP TABLE IF EXISTS $db_output.MPI_PROV;
 CREATE TABLE         $db_output.MPI_PROV AS
 Select          distinct x.*,
                 g.Der_Gender_Desc,
                 case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
                  case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                      when AgeRepPeriodEnd between 18 and 19 then '18 to 19'
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' 
                      else 'Unknown' end as age_group_lower_common,
                 case when AgeRepPeriodEnd between 0 and 5 then '0 to 5'
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' 
                      else 'Unknown' end as age_group_lower_chap1,
                  CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 13 THEN 'Under 14'
                       WHEN AgeRepPeriodEnd BETWEEN 14 and 15 THEN '14 to 15'
                       WHEN AgeRepPeriodEnd BETWEEN 16 and 17 THEN '16 to 17'
                       WHEN AgeRepPeriodEnd BETWEEN 18 AND 19 THEN '18 to 19'
                       WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                       WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                       WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                       WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                       WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                       WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                       WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                       WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                       WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                       WHEN AgeRepPeriodEnd BETWEEN 65 AND 69 THEN '65 to 69'
                       WHEN AgeRepPeriodEnd BETWEEN 70 AND 74 THEN '70 to 74'
                       WHEN AgeRepPeriodEnd BETWEEN 75 AND 79 THEN '75 to 79'
                       WHEN AgeRepPeriodEnd BETWEEN 80 AND 84 THEN '80 to 84'
                       WHEN AgeRepPeriodEnd BETWEEN 85 AND 89 THEN '85 to 89' 
                       WHEN AgeRepPeriodEnd >= 90 THEN '90 or over' 
                       else 'Unknown' END As age_group_lower_chap45,
                   CASE WHEN AgeRepPeriodEnd BETWEEN 14 AND 15 THEN '14 to 15'
                        WHEN AgeRepPeriodEnd BETWEEN 16 AND 17 THEN '16 to 17'
                        WHEN AgeRepPeriodEnd BETWEEN 18 AND 19 THEN '18 to 19'
                        WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeRepPeriodEnd > 64 THEN '65 or over' 
                        ELSE 'Unknown' END as age_group_lower_chap10,
                        
                        case when AgeRepPeriodEnd between 0 and 13 then 'Under 14'
                       when AgeRepPeriodEnd between 14 and 17 then "14 to 17"
                       when AgeRepPeriodEnd between 18 and 19 then "18 to 19"
                       WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                       when AgeRepPeriodEnd between 65 and 69 then "65 to 69"
                       when AgeRepPeriodEnd between 70 and 74 then "70 to 74"
                       when AgeRepPeriodEnd between 75 and 79 then "75 to 79"
                       when AgeRepPeriodEnd between 80 and 84 then "80 to 84"
                       when AgeRepPeriodEnd between 85 and 89 then "80 to 84"
                       when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_chap10a,
                        
                   CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 14 THEN 'Under 15'
                        WHEN AgeRepPeriodEnd BETWEEN 15 AND 19 THEN '15 to 19'
                        WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                        WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                        WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                        WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                        WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                        WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                        WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                        WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                        WHEN AgeRepPeriodEnd BETWEEN 60 AND 64 THEN '60 to 64'
                        WHEN AgeRepPeriodEnd > 64 THEN '65 or over' 
                        ELSE 'Unknown' END as age_group_lower_chap11,  
                    CASE WHEN x.NHSDEthnicity IN ('A', 'B', 'C') THEN 'White'
                         WHEN x.NHSDEthnicity IN ('D', 'E', 'F', 'G') THEN 'Mixed'
                         WHEN x.NHSDEthnicity IN ('H', 'J', 'K', 'L') THEN 'Asian or Asian British'
                         WHEN x.NHSDEthnicity IN ('M', 'N', 'P') THEN 'Black or Black British'
                         WHEN x.NHSDEthnicity IN ('R', 'S') THEN 'Other Ethnic Groups'
                         WHEN x.NHSDEthnicity = 'Z' THEN 'Not Stated'
                         WHEN x.NHSDEthnicity = '99' THEN 'Not Known'
                         ELSE 'Unknown' END AS UpperEthnicity,
              CASE WHEN x.NHSDEthnicity = 'A' THEN 'A'
                    WHEN x.NHSDEthnicity = 'B' THEN 'B'
                    WHEN x.NHSDEthnicity = 'C' THEN 'C'
                    WHEN x.NHSDEthnicity = 'D' THEN 'D'
                    WHEN x.NHSDEthnicity = 'E' THEN 'E'
                    WHEN x.NHSDEthnicity = 'F' THEN 'F'
                    WHEN x.NHSDEthnicity = 'G' THEN 'G'
                    WHEN x.NHSDEthnicity = 'H' THEN 'H'
                    WHEN x.NHSDEthnicity = 'J' THEN 'J'
                    WHEN x.NHSDEthnicity = 'K' THEN 'K'
                    WHEN x.NHSDEthnicity = 'L' THEN 'L'
                    WHEN x.NHSDEthnicity = 'M' THEN 'M'
                    WHEN x.NHSDEthnicity = 'N' THEN 'N'
                    WHEN x.NHSDEthnicity = 'P' THEN 'P'
                    WHEN x.NHSDEthnicity = 'R' THEN 'R'
                    WHEN x.NHSDEthnicity = 'S' THEN 'S'
                    WHEN x.NHSDEthnicity = 'Z' THEN 'Not Stated'
                    WHEN x.NHSDEthnicity = '99' THEN 'Not Known'
                    ELSE 'Unknown' END AS LowerEthnicityCode,
              CASE WHEN x.NHSDEthnicity = 'A' THEN 'British'
                     WHEN x.NHSDEthnicity = 'B' THEN 'Irish'
                     WHEN x.NHSDEthnicity = 'C' THEN 'Any Other White Background'
                     WHEN x.NHSDEthnicity = 'D' THEN 'White and Black Caribbean' 
                     WHEN x.NHSDEthnicity = 'E' THEN 'White and Black African'
                     WHEN x.NHSDEthnicity = 'F' THEN 'White and Asian'
                     WHEN x.NHSDEthnicity = 'G' THEN 'Any Other Mixed Background'
                     WHEN x.NHSDEthnicity = 'H' THEN 'Indian'
                     WHEN x.NHSDEthnicity = 'J' THEN 'Pakistani'
                     WHEN x.NHSDEthnicity = 'K' THEN 'Bangladeshi'
                     WHEN x.NHSDEthnicity = 'L' THEN 'Any Other Asian Background'
                     WHEN x.NHSDEthnicity = 'M' THEN 'Caribbean'
                     WHEN x.NHSDEthnicity = 'N' THEN 'African'
                     WHEN x.NHSDEthnicity = 'P' THEN 'Any Other Black Background'
                     WHEN x.NHSDEthnicity = 'R' THEN 'Chinese'
                     WHEN x.NHSDEthnicity = 'S' THEN 'Any Other Ethnic Group'
                     WHEN x.NHSDEthnicity = 'Z' THEN 'Not Stated'
                     WHEN x.NHSDEthnicity = '99' THEN 'Not Known'
                     ELSE 'Unknown' END AS LowerEthnicityName,
                 CASE
                 WHEN r.DECI_IMD = 10 THEN '10 Least deprived'
                 WHEN r.DECI_IMD = 9 THEN '09 Less deprived'
                 WHEN r.DECI_IMD = 8 THEN '08 Less deprived'
                 WHEN r.DECI_IMD = 7 THEN '07 Less deprived'
                 WHEN r.DECI_IMD = 6 THEN '06 Less deprived'
                 WHEN r.DECI_IMD = 5 THEN '05 More deprived'
                 WHEN r.DECI_IMD = 4 THEN '04 More deprived'
                 WHEN r.DECI_IMD = 3 THEN '03 More deprived'
                 WHEN r.DECI_IMD = 2 THEN '02 More deprived'
                 WHEN r.DECI_IMD = 1 THEN '01 Most deprived'
                 ELSE 'Unknown'
                 END AS IMD_Decile,
                 CASE 
                 WHEN r.DECI_IMD IN (9, 10) THEN '05 Least deprived'
                 WHEN r.DECI_IMD IN (7, 8) THEN '04'
                 WHEN r.DECI_IMD IN (5, 6) THEN '03'
                 WHEN r.DECI_IMD IN (3, 4) THEN '02'
                 WHEN r.DECI_IMD IN (1, 2) THEN '01 Most deprived'                          
                 ELSE 'Unknown' 
                 END AS IMD_Quintile
 from            $db_output.MHB_MHS001MPI x  
 INNER JOIN      global_temp.MPI_max_month_prov AS z
                     ON x.Person_id = z.Person_id 
                     AND x.uniqmonthid = z.uniqmonthid
                     and x.orgidprov = z.orgidprov
 left join       $reference_db.ENGLISH_INDICES_OF_DEP_V02 r --need to find this table
                     on x.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'
 left join       $db_output.Der_Gender g on x.Der_Gender = g.Der_Gender

# COMMAND ----------

# DBTITLE 1,CCG temp table
 %sql
 ---getting latest patient demographic info using MHS002GP
 CREATE OR REPLACE GLOBAL TEMP VIEW CCG_prep AS
 SELECT DISTINCT    a.Person_ID,
 				   max(a.RecordNumber) as recordnumber
                    
 FROM               ( SELECT a.Person_ID,
                             a.UniqMonthID,
                             CASE 
                               when a.OrgIDCCGRes in ('00C','00K','00M') THEN '16C'
                               when a.OrgIDCCGRes in ('05F','05J','05T','06D') THEN '18C'
                               when a.OrgIDCCGRes in ('06M','06V','06W','06Y','07J') THEN '26A'
                               when a.OrgIDCCGRes in ('01C','01R','02D','02F') THEN '27D'
                               when a.OrgIDCCGRes in ('02N','02W','02R') THEN '36J'
                               when a.OrgIDCCGRes in ('07V','08J','08R','08P','08T','08X') THEN '36L'
                               when a.OrgIDCCGRes in ('03D','03E','03M') THEN '42D'
                               when a.OrgIDCCGRes in ('04E','04H','04K','04L','04M','04N') THEN '52R'
                               when a.OrgIDCCGRes in ('09G','09H','09X') THEN '70F'
                               when a.OrgIDCCGRes in ('03T','04D','99D','04Q') THEN '71E'
                               when a.OrgIDCCGRes in ('07N','07Q','08A','08K','08L','08Q') THEN '72Q'
                               when a.OrgIDCCGRes in ('03V','04G') THEN '78H'
                               when a.OrgIDCCGRes in ('00D','00J') THEN '84H'
                               when a.OrgIDCCGRes in ('09C','09E','09J','09W','10A','10D','10E','99J') THEN '91Q'
                               when a.OrgIDCCGRes in ('09L','09N','09Y','99H') THEN '92A'
                               when a.OrgIDCCGRes in ('11E','12D','99N') THEN '92G'
                               when a.OrgIDCCGRes in ('07M','07R','07X','08D','08H') THEN '93C'
                               when a.OrgIDCCGRes in ('09F','09P','99K') THEN '97R'
                               ELSE a.OrgIDCCGRes
                               END as OrgIDCCGRes,
                             a.RecordNumber
                      FROM $db_output.MHB_MHS001MPI a
                      WHERE a.uniqmonthid between '$start_month_id' AND '$end_month_id') a
 LEFT JOIN          ( SELECT GP.Person_ID,
                             GP.UniqMonthID,
                             case 
                               when GP.OrgIDCCGGPPractice in ('00C','00K','00M') THEN '16C'
                               when GP.OrgIDCCGGPPractice in ('05F','05J','05T','06D') THEN '18C'
                               when GP.OrgIDCCGGPPractice in ('06M','06V','06W','06Y','07J') THEN '26A'
                               when GP.OrgIDCCGGPPractice in ('01C','01R','02D','02F') THEN '27D'
                               when GP.OrgIDCCGGPPractice in ('02N','02W','02R') THEN '36J'
                               when GP.OrgIDCCGGPPractice in ('07V','08J','08R','08P','08T','08X') THEN '36L'
                               when GP.OrgIDCCGGPPractice in ('03D','03E','03M') THEN '42D'
                               when GP.OrgIDCCGGPPractice in ('04E','04H','04K','04L','04M','04N') THEN '52R'
                               when GP.OrgIDCCGGPPractice in ('09G','09H','09X') THEN '70F'
                               when GP.OrgIDCCGGPPractice in ('03T','04D','99D','04Q') THEN '71E'
                               when GP.OrgIDCCGGPPractice in ('07N','07Q','08A','08K','08L','08Q') THEN '72Q'
                               when GP.OrgIDCCGGPPractice in ('03V','04G') THEN '78H'
                               when GP.OrgIDCCGGPPractice in ('00D','00J') THEN '84H'
                               when GP.OrgIDCCGGPPractice in ('09C','09E','09J','09W','10A','10D','10E','99J') THEN '91Q'
                               when GP.OrgIDCCGGPPractice in ('09L','09N','09Y','99H') THEN '92A'
                               when GP.OrgIDCCGGPPractice in ('11E','12D','99N') THEN '92G'
                               when GP.OrgIDCCGGPPractice in ('07M','07R','07X','08D','08H') THEN '93C'
                               when GP.OrgIDCCGGPPractice in ('09F','09P','99K') THEN '97R'
                               ELSE GP.OrgIDCCGGPPractice
                               END AS OrgIDCCGGPPractice,
                             GP.RecordNumber
                      FROM $db_output.MHB_MHS002GP GP
                      WHERE GP.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                        AND GP.EndDateGMPRegistration is null ) b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    
 GROUP BY           a.Person_ID

# COMMAND ----------

# DBTITLE 1,Creates CCG table
 %sql
 
 --This produces one ccg per person based upon their latest information submitted
 
 DROP TABLE IF EXISTS $db_output.CCG_final;
 CREATE TABLE         $db_output.CCG_final AS
 select distinct    a.Person_ID,
 				   CASE WHEN b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
 					    WHEN A.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
 						ELSE 'Unknown' END AS IC_Rec_CCG		
 FROM               ( SELECT a.Person_ID,
                             a.UniqMonthID,
                             CASE 
                               when a.OrgIDCCGRes in ('00C','00K','00M') THEN '16C'
                               when a.OrgIDCCGRes in ('05F','05J','05T','06D') THEN '18C'
                               when a.OrgIDCCGRes in ('06M','06V','06W','06Y','07J') THEN '26A'
                               when a.OrgIDCCGRes in ('01C','01R','02D','02F') THEN '27D'
                               when a.OrgIDCCGRes in ('02N','02W','02R') THEN '36J'
                               when a.OrgIDCCGRes in ('07V','08J','08R','08P','08T','08X') THEN '36L'
                               when a.OrgIDCCGRes in ('03D','03E','03M') THEN '42D'
                               when a.OrgIDCCGRes in ('04E','04H','04K','04L','04M','04N') THEN '52R'
                               when a.OrgIDCCGRes in ('09G','09H','09X') THEN '70F'
                               when a.OrgIDCCGRes in ('03T','04D','99D','04Q') THEN '71E'
                               when a.OrgIDCCGRes in ('07N','07Q','08A','08K','08L','08Q') THEN '72Q'
                               when a.OrgIDCCGRes in ('03V','04G') THEN '78H'
                               when a.OrgIDCCGRes in ('00D','00J') THEN '84H'
                               when a.OrgIDCCGRes in ('09C','09E','09J','09W','10A','10D','10E','99J') THEN '91Q'
                               when a.OrgIDCCGRes in ('09L','09N','09Y','99H') THEN '92A'
                               when a.OrgIDCCGRes in ('11E','12D','99N') THEN '92G'
                               when a.OrgIDCCGRes in ('07M','07R','07X','08D','08H') THEN '93C'
                               when a.OrgIDCCGRes in ('09F','09P','99K') THEN '97R'
                               ELSE a.OrgIDCCGRes
                               END as OrgIDCCGRes,
                             a.RecordNumber
                      FROM $db_output.MHB_MHS001MPI a
                      WHERE a.uniqmonthid between '$start_month_id' AND '$end_month_id') a
 LEFT JOIN          ( SELECT GP.Person_ID,
                             GP.UniqMonthID,
                             case 
                               when GP.OrgIDCCGGPPractice in ('00C','00K','00M') THEN '16C'
                               when GP.OrgIDCCGGPPractice in ('05F','05J','05T','06D') THEN '18C'
                               when GP.OrgIDCCGGPPractice in ('06M','06V','06W','06Y','07J') THEN '26A'
                               when GP.OrgIDCCGGPPractice in ('01C','01R','02D','02F') THEN '27D'
                               when GP.OrgIDCCGGPPractice in ('02N','02W','02R') THEN '36J'
                               when GP.OrgIDCCGGPPractice in ('07V','08J','08R','08P','08T','08X') THEN '36L'
                               when GP.OrgIDCCGGPPractice in ('03D','03E','03M') THEN '42D'
                               when GP.OrgIDCCGGPPractice in ('04E','04H','04K','04L','04M','04N') THEN '52R'
                               when GP.OrgIDCCGGPPractice in ('09G','09H','09X') THEN '70F'
                               when GP.OrgIDCCGGPPractice in ('03T','04D','99D','04Q') THEN '71E'
                               when GP.OrgIDCCGGPPractice in ('07N','07Q','08A','08K','08L','08Q') THEN '72Q'
                               when GP.OrgIDCCGGPPractice in ('03V','04G') THEN '78H'
                               when GP.OrgIDCCGGPPractice in ('00D','00J') THEN '84H'
                               when GP.OrgIDCCGGPPractice in ('09C','09E','09J','09W','10A','10D','10E','99J') THEN '91Q'
                               when GP.OrgIDCCGGPPractice in ('09L','09N','09Y','99H') THEN '92A'
                               when GP.OrgIDCCGGPPractice in ('11E','12D','99N') THEN '92G'
                               when GP.OrgIDCCGGPPractice in ('07M','07R','07X','08D','08H') THEN '93C'
                               when GP.OrgIDCCGGPPractice in ('09F','09P','99K') THEN '97R'
                               ELSE GP.OrgIDCCGGPPractice
                               END AS OrgIDCCGGPPractice,
                             GP.RecordNumber
                      FROM $db_output.MHB_MHS002GP GP
                      WHERE GP.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                        AND GP.EndDateGMPRegistration is null ) b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
 INNER JOIN         global_temp.CCG_prep ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    

# COMMAND ----------

# DBTITLE 1,This section below runs the prep code from NHSE_Pre_Processing_Tables - MOVED FROM 13


# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.nhse_pre_proc_header;
 CREATE TABLE         $db_output.NHSE_Pre_Proc_Header USING DELTA AS
 select distinct uniqmonthid, reportingperiodstartdate, reportingperiodenddate, label as Der_FY
 from $db_source.mhs000header h
 left join $reference_db.calendar_financial_year fy on h.reportingperiodstartdate between fy.START_DATE and fy.END_DATE
 order by 1 desc

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_referral
 SELECT
 h.Der_FY,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 r.MHS101UniqID,
 r.Person_ID,
 r.OrgIDProv,
 m.UniqMonthID,
 r.RecordNumber,
 r.UniqServReqID,
 r.OrgIDComm,
 r.ReferralRequestReceivedDate,
 r.ReferralRequestReceivedTime,
 r.SpecialisedMHServiceCode,
 r.PrimReasonReferralMH,
 r.ReasonOAT,
 r.DischPlanCreationDate,
 r.DischPlanCreationTime,
 r.DischPlanLastUpdatedDate,
 r.DischPlanLastUpdatedTime,
 r.ServDischDate,
 r.ServDischTime,
 r.DischLetterIssDate,
 r.AgeServReferRecDate,
 r.AgeServReferDischDate,
 r.RecordStartDate,
 r.RecordEndDate,
 r.InactTimeRef,
 m.MHS001UniqID,
 CASE WHEN m.UniqMonthID <= 1467 then m.OrgIDCCGRes ---Added new case when statement for CCG/SubICB change
      WHEN m.UniqMonthID > 1467 then m.OrgIDSubICBLocResidence ---Maybe look at combining with CCG REF data to see if codes are valid (further in pipeline potentially)
      ELSE 'ERROR' end as OrgIDCCGRes, 
 m.OrgIDEduEstab,
 m.EthnicCategory,
 m.EthnicCategory2021, --new for v5 but not being used in final prep table
 m.NHSDEthnicity,
 m.Gender,
 CASE WHEN m.GenderIDCode IN ('1','2','3','4','X','Z') THEN m.GenderIDCode ELSE m.Gender END AS Gender2021, --new for v5 but not being used in final prep table ---remove hard-coded gender list
 m.MaritalStatus,
 m.PersDeathDate,
 m.AgeDeath,
 m.OrgIDLocalPatientId,
 m.OrgIDResidenceResp,
 m.LADistrictAuth,
 m.LSOA2011,
 m.PostcodeDistrict,
 m.DefaultPostcode,
 m.AgeRepPeriodStart,
 m.AgeRepPeriodEnd,
 s.MHS102UniqID,
 s.UniqCareProfTeamID,
 s.ServTeamTypeRefToMH,
 s.CAMHSTier,
 s.ReferRejectionDate,
 s.ReferRejectionTime,
 s.ReferRejectReason,
 s.ReferClosureDate,
 s.ReferClosureTime,
 s.ReferClosReason,
 s.AgeServReferClosure,
 s.AgeServReferRejection
 FROM                $db_source.mhs101referral r
 INNER JOIN          $db_source.mhs001mpi m 
                     ON r.RecordNumber = m.RecordNumber ---joining on recordnumber opposed to person_id as we want OrgIDCCGRes as it was inputted when referral was submitted in that month
 LEFT JOIN           $db_source.mhs102servicetypereferredto s 
                     ON r.UniqServReqID = s.UniqServReqID 
                     AND r.RecordNumber = s.RecordNumber --joining on recordnumber aswell to match historic records as they will all have the same uniqservreqid    
 LEFT JOIN           $db_output.NHSE_Pre_Proc_Header h
                     ON r.UniqMonthID = h.UniqMonthID

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_distinct_indirect_activity
 SELECT
 i.UniqSubmissionID,
 i.UniqMonthID,
 i.OrgIDProv,
 i.Person_ID,
 CASE WHEN i.OrgIDProv = 'DFC' THEN '1' ELSE i.Person_ID END AS Der_PersonID,
 i.RecordNumber,
 i.UniqServReqID,
 i.OrgIDComm,
 i.CareProfTeamLocalId,
 i.IndirectActDate,
 i.IndirectActTime,
 i.DurationIndirectAct,
 i.MHS204UniqID,
 ROW_NUMBER () OVER(PARTITION BY i.UniqServReqID, i.IndirectActDate, i.IndirectActTime ORDER BY i.IndirectActTime DESC) AS Der_ActRN 
 FROM $db_source.MHS204IndirectActivity i

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_activity
 SELECT
 h.Der_FY,
 'DIRECT' AS Der_ActivityType,
 c.MHS201UniqID AS Der_ActivityUniqID,
 c.Person_ID,
 c.UniqMonthID,
 c.OrgIDProv,
 c.RecordNumber,
 c.UniqServReqID,
 c.UniqCareContID,
 c.CareContDate AS Der_ContactDate,
 c.CareContTime AS Der_ContactTime,
 c.ConsMechanismMH, --new for v5
 c.AttendOrDNACode,
 CASE WHEN c.OrgIDProv = 'DFC' THEN '1' ELSE c.Person_ID END AS Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
 CASE 
     WHEN c.AttendOrDNACode IN ('5','6') 
     AND (((c.ConsMechanismMH NOT IN ('05', '06') and c.UniqMonthID < '1459') --v4.1 ConsMediumUsed
     OR (c.ConsMechanismMH IN ('01', '02', '04', '11') and c.UniqMonthID >= '1459')) 
     OR c.OrgIDProv = 'DFC' AND ((c.ConsMechanismMH IN ('05', '06') and c.UniqMonthID < '1459') 
     OR (c.ConsMechanismMH IN ('05', '09', '10', '13') and c.UniqMonthID >= '1459')))
     THEN 1 ELSE 'NULL' 
 END AS Der_DirectContact 
         
 FROM $db_source.mhs201carecontact c
 LEFT JOIN $db_output.nhse_pre_proc_header h ON c.UniqMonthID = h.UniqMonthID
 
 UNION ALL
  
 SELECT
 h.Der_FY,
 'INDIRECT' AS Der_ActivityType,
 i.MHS204UniqID AS Der_ActivityUniqID,
 i.Person_ID,
 i.UniqMonthID,
 i.OrgIDProv,
 i.RecordNumber,
 i.UniqServReqID,
 'NULL' AS UniqCareContID,
 i.IndirectActDate AS Der_ContactDate,
 i.IndirectActTime AS Der_ContactTime,
 'NULL' AS ConsMechanismMH, --new for v5
 'NULL' AS AttendOrDNACode,
 Der_PersonID, -- derivation added to better reflect anonymous services where personID may change every month
 'NULL' as Der_DirectContact
     
 FROM $db_output.nhse_pre_proc_distinct_indirect_activity i
 LEFT JOIN $db_output.nhse_pre_proc_header h ON i.UniqMonthID = h.UniqMonthID
  
 WHERE i.Der_ActRN = 1

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.der_nhse_pre_proc_activity
  
 SELECT *,
 ROW_NUMBER() OVER (PARTITION BY 
                    CASE WHEN a.OrgIDProv = 'DFC' THEN a.UniqServReqID
                    ELSE a.Person_ID END, 
                    a.UniqServReqID 
                    ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_ContactOrder,
 ROW_NUMBER() OVER (PARTITION BY 
                    CASE WHEN a.OrgIDProv = 'DFC' THEN a.UniqServReqID
                    ELSE a.Person_ID END, 
                    a.UniqServReqID, a.Der_FY 
                    ORDER BY a.Der_ContactDate ASC, a.Der_ContactTime ASC, a.Der_ActivityUniqID ASC) AS Der_FYContactOrder
                    
 FROM $db_output.nhse_pre_proc_activity a
  
 WHERE a.UniqMonthID < 1459 AND 
      ((a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (a.ConsMechanismMH NOT IN ('05', '06') OR OrgIDProv = 'DFC' AND a.ConsMechanismMH IN ('05','06'))) OR a.Der_ActivityType = 'INDIRECT') 
 OR
     a.UniqMonthID >= 1459 AND 
 ((a.Der_ActivityType = 'DIRECT' AND a.AttendOrDNACode IN ('5','6') AND (a.ConsMechanismMH IN ('01', '02', '04', '11') OR OrgIDProv = 'DFC' AND a.ConsMechanismMH IN ('05','09', '10', '13'))) OR a.Der_ActivityType = 'INDIRECT')

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_inpatients
 SELECT
 he.Der_FY,
 he.ReportingPeriodStartDate,
 he.ReportingPeriodEndDate,
 h.MHS501UniqID,
 h.Person_ID,
 h.OrgIDProv,
 h.UniqMonthID,
 h.RecordNumber,
 h.UniqHospProvSpellID, --new for v5
 h.UniqServReqID,
 CONCAT(h.Person_ID, h.UniqServReqID) as UniqPersRefID,
 CONCAT(h.Person_ID, h.UniqServReqID, h.UniqMonthID) as UniqPersRefID_FY,
 h.StartDateHospProvSpell,
 h.StartTimeHospProvSpell,
 h.SourceAdmMHHospProvSpell, --new for v5
 h.MethAdmMHHospProvSpell, --new for v5
 h.EstimatedDischDateHospProvSpell,
 h.PlannedDischDateHospProvSpell,
 h.DischDateHospProvSpell,
 h.DischTimeHospProvSpell,
 h.MethOfDischMHHospProvSpell, --new for v5
 h.DestOfDischHospProvSpell, --new for v5
 h.InactTimeHPS,
 h.PlannedDestDisch,
 h.PostcodeDistrictMainVisitor,
 h.PostcodeDistrictDischDest,
 w.MHS502UniqID,
 w.UniqWardStayID,
 w.StartDateWardStay,
 w.StartTimeWardStay,
 w.SiteIDOfTreat,
 w.WardType,
 w.WardSexTypeCode,
 w.IntendClinCareIntenCodeMH,
 w.WardSecLevel,
 w.SpecialisedMHServiceCode,
 w.WardCode,
 w.WardLocDistanceHome,
 w.LockedWardInd,
 w.InactTimeWS,
 w.WardAge,
 w.HospitalBedTypeMH,
 w.EndDateMHTrialLeave,
 w.EndDateWardStay,
 w.EndTimeWardStay,
 CASE WHEN h.DischDateHospProvSpell IS NOT NULL THEN 'CLOSED' ELSE 'OPEN' END AS Der_HospSpellStatus
     
 FROM $db_source.mhs501hospprovspell h
 LEFT JOIN $db_source.mhs502wardstay w ON h.UniqServReqID = w.UniqServReqID 
                                       AND h.UniqHospProvSpellID = w.UniqHospProvSpellID  --updated for v5
                                       AND h.RecordNumber = w.RecordNumber
 LEFT JOIN $db_output.nhse_pre_proc_header he ON h.UniqMonthID = he.UniqMonthID  

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.der_nhse_pre_proc_inpatients
 SELECT *,
     ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC) AS Der_HospSpellRecordOrder, 
 	ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID DESC, i.EndDateWardStay DESC, i.MHS502UniqID DESC) AS Der_LastWardStayRecord,
 	ROW_NUMBER () OVER(PARTITION BY i.Person_ID, i.UniqServReqID, i.UniqHospProvSpellID ORDER BY i.UniqMonthID ASC, i.EndDateWardStay ASC, i.MHS502UniqID ASC) AS Der_FirstWardStayRecord
     
 FROM $db_output.NHSE_Pre_Proc_Inpatients i

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.prep_nhse_pre_proc_assessments
 SELECT
 'CON' AS Der_AssTable,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 h.Der_FY,
 a.UniqSubmissionID,
 a.UniqMonthID,
 a.CodedAssToolType,
 a.PersScore,
 c.CareContDate AS Der_AssToolCompDate,
 a.RecordNumber,
 a.MHS607UniqID AS Der_AssUniqID,
 a.OrgIDProv,
 CASE WHEN a.OrgIDProv = 'DFC' THEN '1' ELSE a.Person_ID END AS Person_ID, ---Der_Person_ID Derivation
 a.UniqServReqID,
 a.AgeAssessToolCont AS Der_AgeAssessTool,
 a.UniqCareContID,
 a.UniqCareActID
 
 FROM $db_source.mhs607codedscoreassessmentact a 
 
 LEFT JOIN $db_source.mhs201carecontact c ON a.RecordNumber = c.RecordNumber AND a.UniqServReqID = c.UniqServReqID AND a.UniqCareContID = c.UniqCareContID
 
 LEFT JOIN $db_output.nhse_pre_proc_header h ON h.UniqMonthID = a.UniqMonthID
 
 UNION ALL
 
 SELECT
 'REF' AS Der_AssTable,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 h.Der_FY,
 r.UniqSubmissionID,
 r.UniqMonthID,
 r.CodedAssToolType,
 r.PersScore,    
 date_format(COALESCE(r.AssToolCompTimestamp, r.AssToolCompDate), "yyyy-MM-dd") AS Der_AssToolCompDate, ---new field for v5 ---changed to COALESCE as this field is not mapped from v4.1 to v5
 r.RecordNumber,
 r.MHS606UniqID AS Der_AssUniqID,
 r.OrgIDProv,
 CASE WHEN r.OrgIDProv = 'DFC' THEN '1' ELSE r.Person_ID END AS Person_ID, ---Der_Person_ID Derivation,
 r.UniqServReqID,
 r.AgeAssessToolReferCompDate AS Der_AgeAssessTool,
 'NULL' AS UniqCareContID,
 'NULL' AS UniqCareActID
 
 FROM $db_source.mhs606codedscoreassessmentrefer r 
 
 LEFT JOIN $db_output.nhse_pre_proc_header h ON h.UniqMonthID = r.UniqMonthID
 
 UNION ALL
 
 SELECT
 'CLU' AS Der_AssTable,
 h.ReportingPeriodStartDate,
 h.ReportingPeriodEndDate,
 h.Der_FY,
 a.UniqSubmissionID,
 c.UniqMonthID,
 a.CodedAssToolType,
 a.PersScore,
 c.AssToolCompDate AS Der_AssToolCompDate,
 c.RecordNumber,
 a.MHS802UniqID AS Der_AssUniqID,
 a.OrgIDProv,
 CASE WHEN a.OrgIDProv = 'DFC' THEN '1' ELSE a.Person_ID END AS Person_ID,
 r.UniqServReqID,
 'NULL' AS Der_AgeAssessTool,
 'NULL' AS UniqCareContID,
 'NULL' AS UniqCareActID
  
 FROM $db_source.mhs802clusterassess a
  
 LEFT JOIN $db_source.mhs801clustertool c ON c.UniqClustID = a.UniqClustID AND c.RecordNumber = a.RecordNumber
  
 LEFT JOIN $db_output.nhse_pre_proc_header h ON h.UniqMonthID = a.UniqMonthID
  
 INNER JOIN $db_source.mhs101referral r ON r.RecordNumber = c.RecordNumber 
                                        AND c.AssToolCompDate BETWEEN r.ReferralRequestReceivedDate AND COALESCE(r.ServDischDate,h.ReportingPeriodEndDate) ---ISNULL() used in NHSE code

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_assessments
 SELECT 
 a.ReportingPeriodStartDate,
 a.ReportingPeriodEndDate,
 a.Der_FY,
 a.UniqSubmissionID,
 a.Der_AssUniqID,
 a.Der_AssTable, 
 a.Person_ID,    
 a.UniqMonthID,    
 a.OrgIDProv,
 a.RecordNumber,    
 a.UniqServReqID,    
 a.UniqCareContID,    
 a.UniqCareActID,        
 a.Der_AssToolCompDate,
 a.CodedAssToolType,
 a.PersScore,
 a.Der_AgeAssessTool,
 r.Category AS Der_AssessmentCategory,
 r.Assessment_Tool_Name AS Der_AssessmentToolName,
 r.Preferred_Term_SNOMED AS Der_PreferredTermSNOMED,
 r.SNOMED_Version AS Der_SNOMEDCodeVersion,
 r.Lower_Range AS Der_LowerRange,
 r.Upper_Range AS Der_UpperRange,
 r.Rater,
 CASE 
     WHEN CAST(a.PersScore as float) BETWEEN r.Lower_Range AND r.Upper_Range THEN 'Y' ---TRY_CONVERT() IN NHSE Code
     ELSE NULL 
 END AS Der_ValidScore,
 CASE 
     WHEN ROW_NUMBER () OVER (PARTITION BY a.Person_ID, a.Der_AssToolCompDate, COALESCE(a.UniqServReqID,0), r.Preferred_Term_SNOMED, a.PersScore ORDER BY a.Der_AssUniqID ASC) = 1 ---IS_NULL(UniqServReqID, 0) in NHSE
     THEN 'Y' 
     ELSE NULL 
 END AS Der_UniqAssessment,
 CONCAT(a.Der_AssToolCompDate,a.UniqServReqID,a.CodedAssToolType,a.PersScore) AS Der_AssKey,
 CASE WHEN a.Der_AssToolCompDate BETWEEN a.ReportingPeriodStartDate AND a.ReportingPeriodEndDate THEN 1 ELSE 0 END AS Der_AssInMonth
  
 FROM $db_output.prep_nhse_pre_proc_assessments a 
 LEFT JOIN $db_output.mh_ass r ON a.CodedAssToolType = r.Active_Concept_ID_SNOMED

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_assessments_unique
 SELECT
 a.ReportingPeriodStartDate,
 a.ReportingPeriodEndDate,
 a.Der_FY,
 a.UniqSubmissionID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.Person_ID,
 a.RecordNumber,
 a.UniqServReqID,
 a.UniqCareContID,
 a.UniqCareActID,
 a.CodedAssToolType,
 a.PersScore,
 a.Der_AssUniqID,
 a.Der_AssTable,
 a.Der_AssToolCompDate,
 a.Der_AgeAssessTool,
 a.Der_AssessmentToolName,
 a.Der_PreferredTermSNOMED,
 a.Der_SNOMEDCodeVersion,
 a.Der_LowerRange,
 a.Der_UpperRange,
 a.Der_ValidScore,
 a.Der_AssessmentCategory,
 a.Der_AssKey
     
 FROM $db_output.nhse_pre_proc_assessments a
  
 WHERE Der_UniqAssessment = 'Y' AND Der_AssInMonth = 1 ---add assessments in-month

# COMMAND ----------

 %sql
 INSERT INTO $db_output.nhse_pre_proc_assessments_unique
 SELECT 
 a.ReportingPeriodStartDate,
 a.ReportingPeriodEndDate,
 a.Der_FY,
 a.UniqSubmissionID,
 a.UniqMonthID,
 a.OrgIDProv,
 a.Person_ID,
 a.RecordNumber,
 a.UniqServReqID,
 a.UniqCareContID,
 a.UniqCareActID,
 a.CodedAssToolType,
 a.PersScore,
 a.Der_AssUniqID,
 a.Der_AssTable,
 a.Der_AssToolCompDate,
 a.Der_AgeAssessTool,
 a.Der_AssessmentToolName,
 a.Der_PreferredTermSNOMED,
 a.Der_SNOMEDCodeVersion,
 a.Der_LowerRange,
 a.Der_UpperRange,
 a.Der_ValidScore,
 a.Der_AssessmentCategory,    
 a.Der_AssKey
     
 FROM $db_output.nhse_pre_proc_assessments a
 LEFT JOIN $db_output.nhse_pre_proc_assessments b ON a.Der_AssKey = b.Der_AssKey AND b.Der_AssInMonth = 1 AND b.Der_UniqAssessment = 'Y'
 WHERE a.Der_UniqAssessment = 'Y' AND a.Der_AssInMonth = 0 ---add assessments out of month
 AND b.Der_AssKey IS NULL ---NHSE METHOD a.Der_AssKey NOT IN (SELECT Der_AssKey FROM $db_output.NHSE_Pre_Proc_Assessments_Stage2)

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_assessments_unique_valid
 SELECT
 ReportingPeriodStartDate,
 ReportingPeriodEndDate,
 Der_FY,
 UniqSubmissionID,
 UniqMonthID,
 OrgIDProv,
 Person_ID,
 RecordNumber,
 UniqServReqID,
 UniqCareContID,
 UniqCareActID,
 CodedAssToolType,
 PersScore,
 Der_AssUniqID,
 Der_AssTable,
 Der_AssToolCompDate,
 Der_AgeAssessTool,
 Der_AssessmentToolName,
 Der_PreferredTermSNOMED,
 Der_SNOMEDCodeVersion,
 Der_LowerRange,
 Der_UpperRange,
 Der_ValidScore,
 Der_AssessmentCategory,        
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, CodedAssToolType ORDER BY Der_AssToolCompDate ASC) AS Der_AssOrderAsc_OLD, --First assessment
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, CodedAssToolType ORDER BY Der_AssToolCompDate DESC) AS Der_AssOrderDesc_OLD, -- Last assessment
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, Der_PreferredTermSNOMED ORDER BY Der_AssToolCompDate ASC) AS Der_AssOrderAsc_NEW, --First assessment
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, Der_PreferredTermSNOMED ORDER BY Der_AssToolCompDate DESC) AS Der_AssOrderDesc_NEW, -- Last assessment
 Der_AssKey
 FROM $db_output.nhse_pre_proc_assessments_unique
 WHERE Der_ValidScore = 'Y'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.nhse_pre_proc_interventions
 SELECT                
 ca.RecordNumber,
 ca.OrgIDProv,
 ca.Person_ID,
 ca.UniqMonthID,
 ca.UniqServReqID,
 ca.UniqCareContID,
 cc.CareContDate AS Der_ContactDate,
 ca.UniqCareActID,
 ca.MHS202UniqID as Der_InterventionUniqID,
 ca.CodeProcAndProcStatus as CodeProcAndProcStatus,                 
 CASE WHEN position(':' in ca.CodeProcAndProcStatus) > 0 ---gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":"     
      THEN LEFT(ca.CodeProcAndProcStatus, position (':' in ca.CodeProcAndProcStatus)-1) 
      ELSE ca.CodeProcAndProcStatus
      END AS Der_SNoMEDProcCode,
 CASE WHEN position('=', ca.CodeProcAndProcStatus) > 0
      THEN RIGHT(ca.CodeProcAndProcStatus,position('=', REVERSE(ca.CodeProcAndProcStatus))-1)
      ELSE NULL
      END AS Der_SNoMEDProcQual,   
 ca.CodeObs 
                        
 FROM $db_source.mhs202careactivity ca
 LEFT JOIN $db_source.mhs201carecontact cc ON ca.RecordNumber = cc.RecordNumber AND ca.UniqCareContID = cc.UniqCareContID
 WHERE (ca.CodeFind IS NOT NULL OR ca.CodeObs IS NOT NULL OR ca.CodeProcAndProcStatus IS NOT NULL)
  
 UNION ALL
 
 SELECT                
 i.RecordNumber,
 i.OrgIDProv,
 i.Person_ID,
 i.UniqMonthID,
 i.UniqServReqID,
 'NULL' as UniqCareContID,
 i.IndirectActDate AS Der_ContactDate,
 'NULL' as UniqCareActID,
 i.MHS204UniqID as Der_InterventionUniqID,
 i.CodeIndActProcAndProcStatus as CodeProcAndProcStatus,                      
 CASE WHEN position(':' in i.CodeIndActProcAndProcStatus) > 0 ---gets first snomed code in list where CodeIndActProcAndProcStatus contains a ":" 
      THEN LEFT(i.CodeIndActProcAndProcStatus, position (':' in i.CodeIndActProcAndProcStatus)-1) 
      ELSE i.CodeIndActProcAndProcStatus
      END AS Der_SNoMEDProcCode,
 CASE WHEN position('=',i.CodeIndActProcAndProcStatus) > 0
      THEN RIGHT(i.CodeIndActProcAndProcStatus,position('=', REVERSE(i.CodeIndActProcAndProcStatus))-1)
      ELSE NULL
      END AS Der_SNoMEDProcQual,                                
 'NULL' AS CodeObs                   
  
 FROM $db_source.mhs204indirectactivity i
 WHERE (i.CodeFind IS NOT NULL OR i.CodeIndActProcAndProcStatus IS NOT NULL)