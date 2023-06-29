# Databricks notebook source
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MPI_max_month AS
 ---gets max UniqMonthID for a person_id where PatMRecInRP is true (latest submitted mhs001mpi record for a person_id in month)
 SELECT			x.Person_id,
 				MAX(x.uniqmonthid) AS uniqmonthid --change back to monthid for 19-20 (record_number for 18/9)
 FROM			$db_output.MHB_MHS001MPI x	
 where           PatMRecInRP = true 
 GROUP BY		x.Person_id

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MPI_part1 AS
 ---joins max UniqMonthID back onto mpi table to get latest information for each person. Uses PatMrecInRP for unique person details within same month
 Select          distinct x.*
 from            $db_output.MHB_MHS001MPI x  
 INNER JOIN      global_temp.MPI_max_month AS z
                 ON x.Person_id = z.Person_id 
                 AND x.uniqmonthid = z.uniqmonthid 
 where           x.PatMRecInRP = true 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MPI;
 CREATE TABLE         $db_output.MPI AS
 ---incorporates majority of required demographic breakdowns into the final mpi table
 select          a.*,
                 case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                      when AgeRepPeriodEnd >= 18 then '18 and over' 
                      else 'Unknown' end as age_group_higher_level,
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_chap1,
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
                       WHEN AgeRepPeriodEnd >= 90 THEN '90 or over' else 'Unknown' END As age_group_lower_chap45,
                  CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 17 THEN 'Under 18'
                       WHEN AgeRepPeriodEnd BETWEEN 18 and 24 THEN '18 to 24'
                       WHEN AgeRepPeriodEnd BETWEEN 25 and 29 THEN '25 to 29'                
                       WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                       WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                       WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                       WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                       WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                       WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                       WHEN AgeRepPeriodEnd >= 60 THEN '60 or over' else 'Unknown' END AS age_group_lower_chap7,
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
                        WHEN AgeRepPeriodEnd > 64 THEN '65 and over' ELSE 'Unknown' END as age_group_lower_chap11,  
                    CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
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
                        WHEN AgeRepPeriodEnd > 90 THEN '90 and over' ELSE 'Unknown' END as age_group_lower_chap12,                      
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
                            ELSE 'Unknown' END AS LowerEthnicity,
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
 left join       [DATABASE].[DEPRIVATION_REF] r 
                 on a.LSOA2011 = r.LSOA_CODE_2011 
                 and r.imd_year = '$IMD_year'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW MPI_max_month_prov AS
 ---A person may have been treated in more than one provider, could contain different person information. For the purposes of provider breakdowns, require 1 set of person details per provider
 SELECT			x.orgidprov
                 ,x.Person_id
 				,MAX(x.uniqmonthid) AS uniqmonthid
 FROM			$db_output.MHB_MHS001MPI x	
 GROUP BY		x.orgidprov
                 ,x.Person_id

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MPI_PROV;
 CREATE TABLE         $db_output.MPI_PROV AS
 ---incorporates majority of required demographic breakdowns into the final mpi provider table
 Select          distinct x.*,
                 case when AgeRepPeriodEnd between 0 and 17 then 'Under 18'
                           when AgeRepPeriodEnd >= 18 then '18 and over'
                           else 'Unknown' end as age_group_higher_level,
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
                      when AgeRepPeriodEnd >= '90' then '90 or over' else 'Unknown' end as age_group_lower_chap1,
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
                       WHEN AgeRepPeriodEnd >= 90 THEN '90 or over' else 'Unknown' END As age_group_lower_chap45,
                  CASE WHEN AgeRepPeriodEnd BETWEEN 0 and 17 THEN 'Under 18'
                       WHEN AgeRepPeriodEnd BETWEEN 18 and 24 THEN '18 to 24'
                       WHEN AgeRepPeriodEnd BETWEEN 25 and 29 THEN '16 to 17'
                       WHEN AgeRepPeriodEnd BETWEEN 18 AND 19 THEN '18 to 19'
                       WHEN AgeRepPeriodEnd BETWEEN 20 AND 24 THEN '20 to 24'
                       WHEN AgeRepPeriodEnd BETWEEN 25 AND 29 THEN '25 to 29'
                       WHEN AgeRepPeriodEnd BETWEEN 30 AND 34 THEN '30 to 34'
                       WHEN AgeRepPeriodEnd BETWEEN 35 AND 39 THEN '35 to 39'
                       WHEN AgeRepPeriodEnd BETWEEN 40 AND 44 THEN '40 to 44'
                       WHEN AgeRepPeriodEnd BETWEEN 45 AND 49 THEN '45 to 49'
                       WHEN AgeRepPeriodEnd BETWEEN 50 AND 54 THEN '50 to 54'
                       WHEN AgeRepPeriodEnd BETWEEN 55 AND 59 THEN '55 to 59'
                       WHEN AgeRepPeriodEnd >= 60 THEN '60 or over' else 'Unknown' END AS age_group_lower_chap7, 
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
                        WHEN AgeRepPeriodEnd > 64 THEN '65 and over' ELSE 'Unknown' END as age_group_lower_chap11, 
                        CASE WHEN AgeRepPeriodEnd BETWEEN 0 AND 17 THEN 'Under 18'
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
                        WHEN AgeRepPeriodEnd > 90 THEN '90 and over' ELSE 'Unknown' END as age_group_lower_chap12,  
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
                            ELSE 'Unknown' END AS LowerEthnicity,
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
 left join       [DATABASE].[DEPRIVATION_REF] r 
                     on x.LSOA2011 = r.LSOA_CODE_2011 
                     and r.imd_year = '$IMD_year'

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CCG_prep AS
 ---getting latest patient demographic info using MHS002GP
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
                      WHERE a.uniqmonthid between '$month_id_start' AND '$month_id_end') a
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
                    
 LEFT JOIN          $db_output.mhb_rd_ccg_latest c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          $db_output.mhb_rd_ccg_latest e on b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)                   
 GROUP BY           a.Person_ID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.CCG_final;
 CREATE TABLE         $db_output.CCG_final AS
 ---this produces one ccg per person based upon their latest information submitted
 select distinct    a.Person_ID,
 				   CASE WHEN b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
 					    WHEN A.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
 						ELSE 'UNKNOWN' END AS IC_Rec_CCG		
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
                      WHERE a.uniqmonthid between '$month_id_start' AND '$month_id_end') a
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
                    