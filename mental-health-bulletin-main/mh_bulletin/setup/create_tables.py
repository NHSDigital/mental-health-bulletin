# Databricks notebook source
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

def create_table(table_name: str, database_name: str = db_output, create_sql_script: str=None, if_exists=True) -> None:
  """
  Will save to a table from a global_temp view of the same name as the supplied table name (if not SQL script is supplied)
  Otherwise, can simply supply a sql script and this will be used to make the table the specified name, in the specified database
  """
  
  #this command removes the s3 bucket error when trying to delete
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if create_sql_script is None:
    create_sql_script = f"SELECT * FROM global_temp.{table_name}"
    
  if if_exists is True:
    if_exists_script = ' IF EXISTS'
  else:
    if_exists_script = ''
    
  spark.sql(f"""DROP TABLE {if_exists_script} {database_name}.{table_name}
                {create_sql_script}""")
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ref_master;
 CREATE TABLE IF NOT EXISTS $db_output.ref_master
 ---table for every level in mh bulletin csv output
 (
 type string,
 level string,
 level_description string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.measure_level_type_mapping;
 CREATE TABLE IF NOT EXISTS $db_output.measure_level_type_mapping
 ---table to map metric, level and reference data type
 (
 metric string,
 breakdown string,
 type string, -- foreign key rel to ref_master.type
 cross_join_tables_l2 string, -- level1 cross join table
 cross_join_tables_l3 string -- level2 cross join table
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.csv_master;
 CREATE TABLE IF NOT EXISTS $db_output.csv_master
 ---table of mh bulletin csv skeleton with every possible breakdown, level and metric combination
 (
 reporting_period_start string,
 reporting_period_end string,
 status string,
 breakdown string,
 level_one string,
 level_one_description string,
 level_two string,
 level_two_description string,
 level_three string,
 level_three_description string,
 metric string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.TeamType;
 CREATE TABLE IF NOT EXISTS $db_output.TeamType
 ---table for ServTeamTypeRefToMH code values and their associated name
 (
 TeamTypeCode string,
 TeamName string
 )

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.stp_region_mapping;
 CREATE TABLE IF NOT EXISTS $db_output.stp_region_mapping 
 ---table for CCG > STP > Region mapping used in breakdowns for metrics
 (
 CCG_Code string, 
 STP_Code string, 
 STP_Name string, 
 CCG_Name string, 
 Region_Code string, 
 Region_Name string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output1;
 CREATE TABLE IF NOT EXISTS $db_output.output1
 ---raw inserted aggregated metric data
 (
 REPORTING_PERIOD_START     date,
 REPORTING_PERIOD_END       date,
 STATUS                     string,
 BREAKDOWN                  string,
 LEVEL_1                    string,
 LEVEL_1_DESCRIPTION        string,
 LEVEL_2                    string,
 LEVEL_2_DESCRIPTION        string,
 LEVEL_3                    string,
 LEVEL_3_DESCRIPTION        string,
 LEVEL_4                    string,
 LEVEL_4_DESCRIPTION        string,
 METRIC                     string,
 METRIC_VALUE               string  
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output_unsuppressed;
 CREATE TABLE IF NOT EXISTS $db_output.output_unsuppressed
 ---raw metric data joined to $db_output.csv_master with no numeric formatting applied (i.e. unsuppressed)
 (  
 REPORTING_PERIOD_START     date,
 REPORTING_PERIOD_END       date,
 STATUS                     string,
 BREAKDOWN                  string,
 LEVEL_1                    string,
 LEVEL_1_DESCRIPTION        string,
 LEVEL_2                    string,
 LEVEL_2_DESCRIPTION        string,
 LEVEL_3                    string,
 LEVEL_3_DESCRIPTION        string,
 LEVEL_4                    string,
 LEVEL_4_DESCRIPTION        string,
 METRIC                     string,
 METRIC_VALUE               string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.output_suppressed_final_1;
 CREATE TABLE IF NOT EXISTS $db_output.output_suppressed_final_1
 ---$db_output.output_unsuppressed table with mhsds suppression rules applied to METRIC_VALUE
 (
 REPORTING_PERIOD_START     date,
 REPORTING_PERIOD_END       date,
 STATUS                     string,
 BREAKDOWN                  string,
 LEVEL_ONE                  string,
 LEVEL_ONE_DESCRIPTION      string,
 LEVEL_TWO                  string,
 LEVEL_TWO_DESCRIPTION      string,
 LEVEL_THREE                string,
 LEVEL_THREE_DESCRIPTION    string,
 LEVEL_FOUR                 string,
 LEVEL_FOUR_DESCRIPTION     string,
 METRIC                     string,
 METRIC_VALUE               string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.people_in_contact;
 CREATE TABLE IF NOT EXISTS $db_output.people_in_contact
 ---national and commissioning level chapter 1 final prep table to be aggregated
 (
 GENDER                    string,
 AGEREPPERIODEND           string,
 AGE_GROUP_HIGHER_LEVEL    string,
 AGE_GROUP_LOWER_CHAP1     string,
 NHSDETHNICITY             string, 
 UPPERETHNICITY            string,
 LOWERETHNICITY            string,
 LADISTRICTAUTH            string,
 IMD_DECILE                string,
 IMD_QUINTIILE              string,
 HOSPITALBEDTYPEMH         string,
 BED_TYPE                  string,
 SERVTEAMTYPEREFTOMH       string,
 SERVICE_TYPE              string,
 PERSON_ID                 string,
 IC_REC_CCG                string,
 STP_CODE                  string,
 REGION_CODE               string,
 ADMITTED                  int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.people_in_contact_prov;
 CREATE TABLE IF NOT EXISTS $db_output.people_in_contact_prov
 ---provider level chapter 1 final prep table to be aggregated
 (
 GENDER                    string,
 AGEREPPERIODEND           string,
 AGE_GROUP_HIGHER_LEVEL    string,
 AGE_GROUP_LOWER_CHAP1     string,
 NHSDETHNICITY             string, 
 UPPERETHNICITY            string,
 LOWERETHNICITY            string,
 ORGIDPROV                 string,
 IMD_DECILE                string,
 IMD_QUINTILE              string,
 HOSPITALBEDTYPEMH         string,
 BED_TYPE                  string,
 SERVTEAMTYPEREFTOMH       string,
 SERVICE_TYPE              string,
 PERSON_ID                 string,  
 ADMITTED                  int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.people_in_contact_prov_type;
 CREATE TABLE IF NOT EXISTS $db_output.people_in_contact_prov_type
 ---provider type (nhs/independent) level chapter 1 final prep table to be aggregated
 (
 GENDER                    string,
 AGEREPPERIODEND           string,
 AGE_GROUP_HIGHER_LEVEL    string,
 AGE_GROUP_LOWER_CHAP1     string,
 NHSDETHNICITY             string, 
 UPPERETHNICITY            string,
 LOWERETHNICITY            string,
 PROVIDER_TYPE             string,  
 ADMITTED                  string,
 PEOPLE                    int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.beddays;
 CREATE TABLE IF NOT EXISTS $db_output.beddays
 ---national and commissioning level chapter 4 final prep table to be aggregated
 (
 GENDER                    string,
 AGEREPPERIODEND           string,
 AGE_GROUP_HIGHER_LEVEL    string,
 AGE_GROUP_LOWER_CHAP45    string,
 NHSDETHNICITY             string, 
 UPPERETHNICITY            string,
 LOWERETHNICITY            string,
 LADISTRICTAUTH            string,
 IMD_DECILE                string,
 IMD_QUINTILE              string,
 IC_REC_CCG                string,
 STP_CODE                  string,
 REGION_CODE               string,
 HOSPITALBEDTYPEMH         string,
 BED_TYPE                  string,
 BEDDAYS                   int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.beddays_prov;
 CREATE TABLE IF NOT EXISTS $db_output.beddays_prov
 ---provider level chapter 4 final prep table to be aggregated
 (
 GENDER                    string,
 AGEREPPERIODEND           string,
 AGE_GROUP_HIGHER_LEVEL    string,
 AGE_GROUP_LOWER_CHAP45    string,
 NHSDETHNICITY             string, 
 UPPERETHNICITY            string,
 LOWERETHNICITY            string,
 LADISTRICTAUTH            string,
 IMD_DECILE                string,
 IMD_QUINTILE              string,
 ORGIDPROV                 string,
 HOSPITALBEDTYPEMH         string,
 BED_TYPE                  string,
 BEDDAYS                   int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.beddays_provider_type;
 CREATE TABLE IF NOT EXISTS $db_output.beddays_provider_type
 ---provider type (nhs/independent) level chapter 4 final prep table to be aggregated
 (
 GENDER                    string,
 AGEREPPERIODEND           string,
 PERSON_ID                 string,
 PROVIDER_TYPE             string,
 BEDDAYS                   int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.admissions_discharges;
 CREATE TABLE IF NOT EXISTS $db_output.admissions_discharges
 ---national and commissioning level chapter 5 final prep table to be aggregated
 (
 PERSON_ID                 string,
 GENDER                    string,
 AGEREPPERIODEND           string,
 AGE_GROUP_HIGHER_LEVEL    string,
 AGE_GROUP_LOWER_CHAP45    string,
 NHSDETHNICITY             string, 
 UPPERETHNICITY            string,
 LOWERETHNICITY            string,
 IMD_DECILE                string,
 IMD_QUINTILE              string, 
 HOSPITALBEDTYPEMH         string,
 BED_TYPE                  string,
 ORGIDPROV                 string,
 LADISTRICTAUTH            string,
 IC_REC_CCG                string,
 STP_CODE                  string,
 REGION_CODE               string,
 STARTDATEHOSPPROVSPELL    date, 
 DISCHDATEHSOPPROVSPELL    date,
 UNIQHOSPPROVSPELLID       string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.CCG_FINAL;
 CREATE TABLE IF NOT EXISTS $db_output.CCG_FINAL
 ---table for latest submitted ccg for each person_id present in the financial year
 (
 PERSON_ID    string,
 IC_REC_CCG   string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MPI_PROV;
 CREATE TABLE IF NOT EXISTS $db_output.MPI_PROV
 ---latest mhs001mpi data for each person_id, orgidprov combination in the financial year
 (
 GENDER           string,
 NHSDEthnicity    string,
 RecordNumber     string,
 MHS001UniqID     string,
 ORGIDPROV        string,
 ORGIDCCGRES      string,
 PERSON_ID        string,
 UNIQSUBMISSIONID string,
 AGEREPPERIODEND  string,
 LADISTRICTAUTH   string,
 UNIQMONTHID      string,
 PATMRECINRP      string,
 AGE_GROUP_HIGHER_LEVEL string,
 AGE_GROUP_LOWER_CHAP1 string,
 AGE_GROUP_LOWER_CHAP45 string,
 AGE_GROUP_LOWER_CHAP7 string,
 UPPERETHNICITY   string,
 LOWERETHNICITY   string,
 IMD_DECILE       string,
 IMD_QUINTILE     string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.MPI;
 CREATE TABLE IF NOT EXISTS $db_output.MPI
 ---latest mhs001mpi data for each person_id in the financial year
 (
 GENDER           string,
 NHSDEthnicity    string,
 RecordNumber     string,
 MHS001UniqID     string,
 ORGIDPROV        string,
 ORGIDCCGRES      string,
 PERSON_ID        string,
 UNIQSUBMISSIONID string,
 AGEREPPERIODEND  string,
 LADISTRICTAUTH   string,
 UNIQMONTHID      string,
 PATMRECINRP      string,
 AGE_GROUP_HIGHER_LEVEL string,
 AGE_GROUP_LOWER_CHAP1 string,
 AGE_GROUP_LOWER_CHAP45 string,
 AGE_GROUP_LOWER_CHAP7 string,
 UPPERETHNICITY   string,
 LOWERETHNICITY   string,
 IMD_DECILE       string,
 IMD_QUINTILE     string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhb_rd_ccg_latest;
 CREATE TABLE IF NOT EXISTS $db_output.mhb_rd_ccg_latest 
 ---latest ccg code and ccg code for all valid ccgs in the financial year
 (
 ORG_CODE        string,
 NAME            string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhb_provider_list;
 CREATE TABLE IF NOT EXISTS $db_output.mhb_provider_list
 ---latest provider code and provider code for all provders which submitted to mhsds in the financial year
 (
 ORG_CODE          string,
 NAME              string
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.ccg_mapping_2021;
 CREATE TABLE IF NOT EXISTS $db_output.ccg_mapping_2021 
 ---ccg mapping 2021 table (needed for community access and admissions metrics)
 (
 CCG_UNMAPPED STRING, 
 CCG21CDH STRING, 
 CCG21CD STRING, 
 CCG21NM STRING, 
 STP21CD STRING, 
 STP21CDH STRING, 
 STP21NM STRING, 
 NHSER21CD STRING, 
 NHSER21CDH STRING, 
 NHSER21NM STRING
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.mhb_los_ccg_pop;
 CREATE TABLE IF NOT EXISTS $db_output.mhb_los_ccg_pop
 ---ccg 2021 popultation table (needed for length of stay metrics)
 (
 CCG_2021_CODE string,
 CCG_2021_NAME string, 
 CCG_2020_NAME string,
 CCG_2020_CODE string,
 All_Ages int,
 Aged_18_to_64 int,
 Aged_65_OVER int,
 Aged_0_to_17 int
 ) USING DELTA

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.metric_name_lookup;
 CREATE TABLE IF NOT EXISTS $db_output.metric_name_lookup
 (
 METRIC STRING,
 METRIC_DESCRIPTION STRING
 )
 using delta;

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.error_log;
 CREATE TABLE IF NOT EXISTS $db_output.error_log
 (
 RUNDATE date,
 Tablename string,
 Test string,
 Metric string,
 Breakdown string,
 Pass boolean
 )
 USING DELTA;
 DELETE FROM $db_output.error_log WHERE RUNDATE = current_date()