# Databricks notebook source
db_output = dbutils.widgets.get("db_output")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

 %run ./test_functions

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Load Prep Tables as Spark DataFrames
chap1_df = spark.table(f"{db_output}.people_in_contact")
chap1_std_rate_df = spark.table(f"{db_output}.standardisation")
chap4_df = spark.table(f"{db_output}.beddays")
chap5_df = spark.table(f"{db_output}.admissions_discharges")
chap6_df = spark.table(f"{db_output}.care_contacts")
chap7_df = spark.table(f"{db_output}.ri_prep")
chap7_std_rate_df = spark.table(f"{db_output}.standardisation_restraint")
chap9_df = spark.table(f"{db_output}.cont_prep_table_second")
chap10abc_df = spark.table(f"{db_output}.eip23a_common")
chap10d_df = spark.table(f"{db_output}.eip23d_common")
chap10e_df = spark.table(f"{db_output}.eip64abc_common")
chap10f_df = spark.table(f"{db_output}.eipref_common")
chap11_df = spark.table(f"{db_output}.perinatal_master")
chap12_df = spark.table(f"{db_output}.72h_follow_master")
chap13_df = spark.table(f"{db_output}.dementia_prep")
chap14_df = spark.table(f"{db_output}.cmh_rolling_activity")
chap15_df = spark.table(f"{db_output}.cmh_agg")
chap16_df = spark.table(f"{db_output}.spells")
chap17_df = spark.table(f"{db_output}.mhb_firstcont_final")
chap18_df = spark.table(f"{db_output}.ips_master")
chap19_df = spark.table(f"{db_output}.cyp_master")

# COMMAND ----------

# DBTITLE 1,Chapter 1 Tests
test_null_values(chap1_df, ["age_group_higher_level", "age_group_lower_chap1", "Bed_Type", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ServiceType", "ProvTypeCode"])
test_duplicate_breakdown_values(chap1_df, ["age_group_higher_level", "age_group_lower_chap1", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "Person_ID")
test_percent_unknown_geog_values(chap1_df, "CCG_Code", "Person_ID")

# COMMAND ----------

# DBTITLE 1,Chapter 4 Tests
test_null_values(chap1_df, ["age_group_higher_level", "age_group_lower_chap1", "Bed_Type", "UpperEthnicity", "LowerEthnicityName", "LADistrictAuth", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap1_df, ["age_group_higher_level", "age_group_lower_chap1", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "Person_ID")
test_percent_unknown_geog_values(chap1_df, "CCG_Code", "Person_ID")

# COMMAND ----------

# DBTITLE 1,Chapter 5 Tests
test_null_values(chap5_df, ["age_group_higher_level", "age_group_lower_chap45", "Bed_Type", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap5_df, ["age_group_higher_level", "age_group_lower_chap45", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqHospProvSpellID")
test_percent_unknown_geog_values(chap5_df, "CCG_Code", "UniqHospProvSpellID")

# COMMAND ----------

# DBTITLE 1,Chapter 6 Tests
test_null_values(chap6_df, ["ProvTypeCode", "TeamTypeCode"])

# COMMAND ----------

# DBTITLE 1,Chapter 7 Tests
test_null_values(chap7_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode", "IntTypeCode"])
test_duplicate_breakdown_values(chap7_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "Person_ID")
test_duplicate_breakdown_values(chap7_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqRestraintID")
test_duplicate_breakdown_values(chap7_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqRestrictiveIntIncID")
test_percent_unknown_geog_values(chap7_df, "CCG_Code", "Person_ID")
test_percent_unknown_geog_values(chap7_df, "CCG_Code", "UniqRestraintID")
test_percent_unknown_geog_values(chap7_df, "CCG_Code", "UniqRestrictiveIntIncID")

# COMMAND ----------

# DBTITLE 1,Chapter 9 Tests
test_null_values(chap9_df, ["age_group_higher_level", "age_group_lower_chap1", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap9_df, ["age_group_higher_level", "age_group_lower_chap1", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "2_contact")
test_percent_unknown_geog_values(chap9_df, "CCG_Code", "2_contact")

# COMMAND ----------

# DBTITLE 1,Chapter 10 Tests
test_null_values(chap10abc_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"])
test_null_values(chap10d_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"])
test_null_values(chap10e_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"])
test_null_values(chap10f_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"])

test_duplicate_breakdown_values(chap10abc_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqServReqID")
test_duplicate_breakdown_values(chap10d_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqServReqID")
test_duplicate_breakdown_values(chap10e_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqServReqID")
test_duplicate_breakdown_values(chap10f_df, ["age_group_higher_level", "age_group_lower_chap10", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqServReqID")

test_percent_unknown_geog_values(chap10abc_df, "CCG_Code", "UniqServReqID")
test_percent_unknown_geog_values(chap10d_df, "CCG_Code", "UniqServReqID")
test_percent_unknown_geog_values(chap10e_df, "CCG_Code", "UniqServReqID")
test_percent_unknown_geog_values(chap10f_df, "CCG_Code", "UniqServReqID")

# COMMAND ----------

# DBTITLE 1,Chapter 11 Tests
chap11_att_cont_df = chap11_df.filter((F.col("AttContacts") > 0) & (F.col("AccessEngRN") == 1))
chap11_att_cont_df.createOrReplaceTempView("chap11")
test_null_values(chap11_att_cont_df, ["age_group_higher_level", "age_group_lower_chap11", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap11_att_cont_df, ["age_group_higher_level", "age_group_lower_chap11", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "Person_ID")
test_percent_unknown_geog_values(chap11_df, "CCG_Code", "Person_ID")

# COMMAND ----------

# DBTITLE 1,Chapter 12 Tests
test_null_values(chap12_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode", "LADistrictAuth"])

# COMMAND ----------

# DBTITLE 1,Chapter 13 Tests
chap13_df_contacts = chap13_df.filter(F.col("UniqCareContID").isNotNull())
test_null_values(chap13_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode", "LADistrictAuth"])
test_duplicate_breakdown_values(chap13_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqServReqID")
# test_duplicate_breakdown_values(chap13_df_contacts, ["age_group_higher_level", "age_group_lower_chap1", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "UniqCareContID")
test_percent_unknown_geog_values(chap13_df, "CCG_Code", "UniqServReqID")
test_percent_unknown_geog_values(chap13_df_contacts, "CCG_Code", "UniqCareContID")

# COMMAND ----------

# DBTITLE 1,Chapter 14 Tests
test_null_values(chap14_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap14_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "Person_ID")
test_percent_unknown_geog_values(chap14_df, "CCG_Code", "Person_ID")

# COMMAND ----------

# DBTITLE 1,Chapter 15 Tests
test_null_values(chap15_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode"])

# COMMAND ----------

# DBTITLE 1,Chapter 16 Tests
test_null_values(chap16_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap16_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile"], "UniqHospProvSpellID")
test_percent_unknown_geog_values(chap16_df, "CCG_Code", "UniqHospProvSpellID")

# COMMAND ----------

# DBTITLE 1,Chapter 17 Tests
chap17_mhs95_df = chap17_df.filter((F.col("METRIC") == "MHS95") & (F.col("AccessEngRN") == 1))
test_null_values(chap17_mhs95_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap17_mhs95_df, ["age_group_higher_level", "age_group_lower_common", "UpperEthnicity", "LowerEthnicityName", "Der_Gender", "IMD_Decile", "IMD_Quintile"], "Person_ID")
test_percent_unknown_geog_values(chap17_mhs95_df, "CCG_Code", "Person_ID")

# COMMAND ----------

# DBTITLE 1,Chapter 18 Tests
test_null_values(chap18_df, ["age_group_lower_common", "UpperEthnicity", "Der_Gender", "IMD_Decile", "ProvTypeCode"])
test_duplicate_breakdown_values(chap18_df, ["age_group_lower_common", "UpperEthnicity", "Der_Gender", "IMD_Decile"], "UniqServReqID")
test_percent_unknown_geog_values(chap18_df, "CCG_Code", "UniqServReqID")

# COMMAND ----------

# DBTITLE 1,Chapter 19 Tests
test_null_values(chap19_df, ["OrgIDProv"])
test_duplicate_breakdown_values(chap19_df, ["OrgIDProv"], "UniqServReqID")
test_percent_unknown_geog_values(chap19_df, "STP_Code", "UniqServReqID")