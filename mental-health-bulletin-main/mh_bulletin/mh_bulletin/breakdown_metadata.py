# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

 %run ./mhsds_functions

# COMMAND ----------

# DBTITLE 1,National Breakdowns
eng_bd = {
    "breakdown_name": "England",
    "level_tier": 1,
    "level_tables": ["eng_desc"],
    "primary_level": F.lit("England"),
    "primary_level_desc": F.lit("England"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list": [("England", )],
    "lookup_col": ["England"],
    "level_fields" : [  
      F.col("England").alias("breakdown"),
      F.col("England").alias("primary_level"),  
      F.col("England").alias("primary_level_desc"),  
      F.lit("NONE").alias("secondary_level"),   
      F.lit("NONE").alias("secondary_level_desc"),
      F.lit("NONE").alias("third_level"),   
      F.lit("NONE").alias("third_level_desc"),
      F.lit("NONE").alias("fourth_level"),   
      F.lit("NONE").alias("fourth_level_desc")
    ],   
}
age_band_higher_bd = {
    "breakdown_name": "England; Age Group (Higher Level)",
    "level_tier": 1,
    "level_tables": ["age_band_desc"],
    "primary_level": F.col("age_group_higher_level"),
    "primary_level_desc": F.col("age_group_higher_level"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Higher Level)").alias("breakdown"),
    F.col("age_group_higher_level").alias("primary_level"),
    F.col("age_group_higher_level").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}
gender_bd = {
    "breakdown_name": "England; Gender",
    "level_tier": 1,
    "level_tables": ["Der_Gender"],
    "primary_level": F.col("Der_Gender"),
    "primary_level_desc": F.col("Der_Gender_Desc"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Gender").alias("breakdown"),
    F.col("Der_Gender").alias("primary_level"),
    F.col("Der_Gender_Desc").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
  ],  
}

lower_eth_bd = {
    "breakdown_name": "England; Ethnicity (Lower Level)",
    "level_tier": 1,
    "level_tables": ["NHSDEthnicityDim"],
    "primary_level": F.col("LowerEthnicityCode"),
    "primary_level_desc": F.col("LowerEthnicityName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("key").alias("primary_level"),
    F.col("lower_description").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")    
  ]
}
upper_eth_bd = {
    "breakdown_name": "England; Ethnicity (Higher Level)",
    "level_tier": 1,
    "level_tables": ["NHSDEthnicityDim"],
    "primary_level": F.col("UpperEthnicity"),
    "primary_level_desc": F.col("UpperEthnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("upper_description").alias("primary_level"),
    F.col("upper_description").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}

imd_decile_bd = {
    "breakdown_name": "England; IMD Decile",
    "level_tier": 1,
    "level_tables": ["imd_desc"],
    "primary_level": F.col("IMD_Decile"),
    "primary_level_desc": F.col("IMD_Decile"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; IMD Decile").alias("breakdown"),
    F.col("IMD_Decile").alias("primary_level"),
    F.col("IMD_Decile").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
  ],
}
imd_quintile_bd = {
    "breakdown_name": "England; IMD Quintile",
    "level_tier": 1,
    "level_tables": ["imd_desc"],
    "primary_level": F.col("IMD_Quintile"),
    "primary_level_desc": F.col("IMD_Quintile"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; IMD Quintile").alias("breakdown"),
    F.col("IMD_Quintile").alias("primary_level"),
    F.col("IMD_Quintile").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
  ],
}

upper_eth_age_band_higher_bd = cross_dict([upper_eth_bd, age_band_higher_bd])
imd_decile_upper_eth_bd = cross_dict([imd_decile_bd, upper_eth_bd])

# COMMAND ----------

# DBTITLE 1,Single Age Band Lower Breakdowns
age_band_common_bd = {
  "breakdown_name": "England; Age Group (Lower Level)",
  "level_tier": 1,
  "level_tables": ["age_band_desc"],
  "primary_level": F.col("age_group_lower_common"),
  "primary_level_desc": F.col("age_group_lower_common"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Lower Level)").alias("breakdown"),
    F.col("age_group_lower_common").alias("primary_level"),
    F.col("age_group_lower_common").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}
age_band_chap1_bd = {
  "breakdown_name": "England; Age Group (Lower Level)",
  "level_tier": 1,
  "level_tables": ["age_band_desc"],
  "primary_level": F.col("age_group_lower_chap1"),
  "primary_level_desc": F.col("age_group_lower_chap1"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Lower Level)").alias("breakdown"),
    F.col("age_group_lower_chap1").alias("primary_level"),
    F.col("age_group_lower_chap1").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}
age_band_chap45_bd = {
  "breakdown_name": "England; Age Group (Lower Level)",
  "level_tier": 1,
  "level_tables": ["age_band_desc"],
  "primary_level": F.col("age_group_lower_chap45"),
  "primary_level_desc": F.col("age_group_lower_chap45"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Lower Level)").alias("breakdown"),
    F.col("age_group_lower_chap45").alias("primary_level"),
    F.col("age_group_lower_chap45").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}
age_band_chap10_bd = {
  "breakdown_name": "England; Age Group (Lower Level)",
  "level_tier": 1,
  "level_tables": ["age_band_desc"],
  "primary_level": F.col("age_group_lower_chap10"),
  "primary_level_desc": F.col("age_group_lower_chap10"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),   
  "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Lower Level)").alias("breakdown"),
    F.col("age_group_lower_chap10").alias("primary_level"),
    F.col("age_group_lower_chap10").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}
age_band_chap10a_bd = {
  "breakdown_name": "England; Age Group (Lower Level)",
  "level_tier": 1,
  "level_tables": ["age_band_desc"],
  "primary_level": F.col("age_group_lower_chap10a"),
  "primary_level_desc": F.col("age_group_lower_chap10a"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),   
  "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Lower Level)").alias("breakdown"),
    F.col("age_group_lower_chap10a").alias("primary_level"),
    F.col("age_group_lower_chap10a").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}

age_band_chap11_bd = {
  "breakdown_name": "England; Age Group (Lower Level)",
  "level_tier": 1,
  "level_tables": ["age_band_desc"],
  "primary_level": F.col("age_group_lower_chap11"),
  "primary_level_desc": F.col("age_group_lower_chap11"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Lower Level)").alias("breakdown"),
    F.col("age_group_lower_chap11").alias("primary_level"),
    F.col("age_group_lower_chap11").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}
age_band_imd_bd = {
  "breakdown_name": "England; Age Group (Lower Level)",
  "level_tier": 1,
  "level_tables": ["age_band_desc"],
  "primary_level": F.col("age_group_lower_imd"),
  "primary_level_desc": F.col("age_group_lower_imd"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Age Group (Lower Level)").alias("breakdown"),
    F.col("age_group_lower_imd").alias("primary_level"),
    F.col("age_group_lower_imd").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}

# COMMAND ----------

# DBTITLE 1,Multi Age Band Lower Breakdowns
# For cross_dict: please only use single level dictionaries (eg gender_bd) as inputs
# check that "level_tier": 0,  "level_tables": [] for each input dictionary

gender_age_band_lower_common_bd = cross_dict([gender_bd, age_band_common_bd])
gender_age_band_lower_chap1_bd = cross_dict([gender_bd, age_band_chap1_bd])
gender_age_band_lower_chap45_bd = cross_dict([gender_bd, age_band_chap45_bd])
gender_age_band_lower_chap10_bd = cross_dict([gender_bd, age_band_chap10_bd])
gender_age_band_lower_chap10a_bd = cross_dict([gender_bd, age_band_chap10a_bd])
gender_age_band_lower_chap11_bd = cross_dict([gender_bd, age_band_chap11_bd])

upper_eth_age_band_lower_common_bd = cross_dict([upper_eth_bd, age_band_common_bd])
upper_eth_age_band_lower_chap1_bd = cross_dict([upper_eth_bd, age_band_chap1_bd])
upper_eth_age_band_lower_chap45_bd = cross_dict([upper_eth_bd, age_band_chap45_bd])
upper_eth_age_band_lower_chap10_bd = cross_dict([upper_eth_bd, age_band_chap10_bd])
upper_eth_age_band_lower_chap10a_bd = cross_dict([upper_eth_bd, age_band_chap10a_bd])
upper_eth_age_band_lower_chap11_bd = cross_dict([upper_eth_bd, age_band_chap11_bd])

lower_eth_age_band_lower_common_bd = cross_dict([lower_eth_bd, age_band_common_bd])
lower_eth_age_band_lower_chap1_bd = cross_dict([lower_eth_bd, age_band_chap1_bd])
lower_eth_age_band_lower_chap45_bd = cross_dict([lower_eth_bd, age_band_chap45_bd])
lower_eth_age_band_lower_chap10_bd = cross_dict([lower_eth_bd, age_band_chap10_bd])
lower_eth_age_band_lower_chap10a_bd = cross_dict([lower_eth_bd, age_band_chap10a_bd])
lower_eth_age_band_lower_chap11_bd = cross_dict([lower_eth_bd, age_band_chap11_bd])

imd_decile_age_band_lower_common_bd = cross_dict([imd_decile_bd, age_band_common_bd])
imd_decile_age_band_lower_chap1_bd = cross_dict([imd_decile_bd, age_band_chap1_bd])
imd_decile_age_band_lower_chap45_bd = cross_dict([imd_decile_bd, age_band_chap45_bd])
imd_decile_age_band_lower_chap10_bd = cross_dict([imd_decile_bd, age_band_chap10_bd])
imd_decile_age_band_lower_chap10a_bd = cross_dict([imd_decile_bd, age_band_chap10a_bd])
imd_decile_age_band_lower_chap11_bd = cross_dict([imd_decile_bd, age_band_chap11_bd])
imd_decile_age_band_lower_imd_bd = cross_dict([imd_decile_bd, age_band_imd_bd])

imd_decile_gender_age_band_lower_common_bd = cross_dict([imd_decile_bd, gender_bd, age_band_common_bd])
imd_decile_gender_age_band_lower_chap1_bd = cross_dict([imd_decile_bd, gender_bd, age_band_chap1_bd])
imd_decile_gender_age_band_lower_chap45_bd = cross_dict([imd_decile_bd, gender_bd, age_band_chap45_bd])
imd_decile_gender_age_band_lower_chap10_bd = cross_dict([imd_decile_bd, gender_bd, age_band_chap10_bd])
imd_decile_gender_age_band_lower_chap10a_bd = cross_dict([imd_decile_bd, gender_bd, age_band_chap10a_bd])
imd_decile_gender_age_band_lower_chap11_bd = cross_dict([imd_decile_bd, gender_bd, age_band_chap11_bd])

# COMMAND ----------

# DBTITLE 1,Intervention Type Breakdowns
int_type_bd = {
    "breakdown_name": "England; Intervention Type",
    "level_tier": 1,
    "level_tables": ["RestrictiveIntTypeDim"],
    "primary_level": F.col("IntTypeCode"),
    "primary_level_desc": F.col("IntTypeName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
   "lookup_col" : [],
   "level_fields" : [
    F.lit("England; Intervention Type").alias("breakdown"),
    F.col("key").alias("primary_level"),
    F.col("description").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")    
  ]
}

int_type_gender_bd = cross_dict([int_type_bd, gender_bd])
int_type_age_band_lower_common_bd = cross_dict([int_type_bd, age_band_common_bd])
int_type_gender_age_band_lower_common_bd = cross_dict([int_type_bd, gender_bd, age_band_common_bd])


# COMMAND ----------

# DBTITLE 1,Service Type Breakdowns
team_type_bd = {
    "breakdown_name": "England; Service or Team Type",
    "level_tier": 1,
    "level_tables": ["TeamType"],
    "primary_level": F.col("TeamTypeCode"),
    "primary_level_desc": F.col("TeamName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_list" : [],
     "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Service or Team Type").alias("breakdown"),
    F.col("TeamTypeCode").alias("primary_level"),
    F.col("TeamName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")    
  ]
}

team_type_age_band_higher_bd = cross_dict([team_type_bd, age_band_higher_bd])
team_type_upper_eth_bd = cross_dict([team_type_bd, upper_eth_bd])
team_type_gender_bd = cross_dict([team_type_bd, gender_bd])
team_type_imd_quintile_bd = cross_dict([team_type_bd, imd_quintile_bd])

# COMMAND ----------

# DBTITLE 1,Service Type as Reported to MHSDS
service_type_bd = {
    "breakdown_name": "England; Service Type as Reported to MHSDS",
    "level_tier": 1,
    "level_tables": ["TeamType"],
    "primary_level": F.col("ServiceType"),
    "primary_level_desc": F.col("ServiceType"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Service Type as Reported to MHSDS").alias("breakdown"),
    F.col("HigherTeamName").alias("primary_level"),
    F.col("HigherTeamName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")    
  ]
}
# number of columns in lookup_col should be equal to number of columns in level_list
print(">>>>> service_type_bd >>>>>")
print(service_type_bd["lookup_col"])
for x in service_type_bd["level_list"]: print(x)

service_type_age_band_higher_bd = cross_dict([service_type_bd, age_band_higher_bd])
service_type_upper_eth_bd = cross_dict([service_type_bd, upper_eth_bd])
service_type_gender_bd = cross_dict([service_type_bd, gender_bd])
service_type_imd_quintile_bd = cross_dict([service_type_bd, imd_quintile_bd])

print("\n>>>>> service_type_age_band_higher_bd >>>>>")
print(service_type_age_band_higher_bd["lookup_col"])
for x in service_type_age_band_higher_bd["level_list"]: print(x)

# COMMAND ----------

# DBTITLE 1,Attendance Code Breakdowns
att_bd = {
    "breakdown_name": "England; Attendance Code",
    "level_tier": 1,
    "level_tables": ["attendance_code"],
    "primary_level": F.col("AttCode"),
    "primary_level_desc": F.col("AttName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Attendance Code").alias("breakdown"),
    F.col("AttCode").alias("primary_level"),
    F.col("AttName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")    
  ]
}

att_team_type_bd = cross_dict([att_bd, team_type_bd])

# COMMAND ----------

# DBTITLE 1,Bed Type Breakdowns
bed_type_bd = {
    "breakdown_name": "England; Bed Type as Reported to MHSDS",
    "level_tier": 1,
    "level_tables": ["hosp_bed_desc"],
    "primary_level": F.col("Bed_Type"),
    "primary_level_desc": F.col("Bed_Type"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Bed Type as Reported to MHSDS").alias("breakdown"),
    F.col("HigherMHAdmittedPatientClassName").alias("primary_level"),
    F.col("HigherMHAdmittedPatientClassName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")    
  ]
}
bed_type_age_band_higher_bd = cross_dict([bed_type_bd, age_band_higher_bd])
bed_type_upper_eth_bd = cross_dict([bed_type_bd, upper_eth_bd])
bed_type_gender_bd = cross_dict([bed_type_bd, gender_bd])
bed_type_imd_quintile_bd = cross_dict([bed_type_bd, imd_quintile_bd])

# COMMAND ----------

# DBTITLE 1,Provider Type Breakdowns
prov_type_bd = {
    "breakdown_name": "Provider Type",
    "level_tier": 1,
    "level_tables": ["prov_type_desc"],
    "primary_level": F.col("ProvTypeCode"),
    "primary_level_desc": F.col("ProvTypeName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("Provider Type").alias("breakdown"),
    F.col("ProvTypeCode").alias("primary_level"),
    F.col("ProvTypeName").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}
age_band_higher_prov_type_bd = cross_dict([age_band_higher_bd, prov_type_bd])
age_band_lower_common_prov_type_bd = cross_dict([age_band_common_bd, prov_type_bd])
age_band_lower_chap1_prov_type_bd = cross_dict([age_band_chap1_bd, prov_type_bd])
age_band_lower_chap45_prov_type_bd = cross_dict([age_band_chap45_bd, prov_type_bd])
gender_prov_type_bd = cross_dict([gender_bd, prov_type_bd])
gender_age_band_lower_common_prov_type_bd = cross_dict([gender_bd, age_band_common_bd, prov_type_bd])
gender_age_band_lower_chap1_prov_type_bd = cross_dict([gender_bd, age_band_chap1_bd, prov_type_bd])
gender_age_band_lower_chap45_prov_type_bd = cross_dict([gender_bd, age_band_chap45_bd, prov_type_bd])

# COMMAND ----------

# DBTITLE 1,Provider Breakdowns
prov_bd = {
    "breakdown_name": "Provider",
    "level_tier": 1,
    "level_tables": ["mhsds_providers_in_year"],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [
    F.lit("Provider").alias("breakdown"),  
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_age_band_higher_bd = cross_dict([prov_bd, age_band_higher_bd])
prov_age_band_common_bd = cross_dict([prov_bd, age_band_common_bd])
prov_age_band_chap1_bd = cross_dict([prov_bd, age_band_chap1_bd])
prov_age_band_chap45_bd = cross_dict([prov_bd, age_band_chap45_bd])
prov_age_band_chap10_bd = cross_dict([prov_bd, age_band_chap10_bd])
prov_age_band_chap10a_bd = cross_dict([prov_bd, age_band_chap10a_bd])
prov_age_band_chap11_bd = cross_dict([prov_bd, age_band_chap11_bd])
prov_upper_eth_bd = cross_dict([prov_bd, upper_eth_bd])
prov_lower_eth_bd = cross_dict([prov_bd, lower_eth_bd])
prov_gender_bd = cross_dict([prov_bd, gender_bd])
prov_imd_decile_bd = cross_dict([prov_bd, imd_decile_bd])
prov_imd_quintile_bd = cross_dict([prov_bd, imd_quintile_bd])
prov_int_type_bd = cross_dict([prov_bd, int_type_bd])
prov_team_type_bd = cross_dict([prov_bd, team_type_bd])
prov_bed_type_bd = cross_dict([prov_bd, bed_type_bd])

# COMMAND ----------

# DBTITLE 1,CCG/Sub ICB of Residence Breakdowns
ccg_res_bd = {
    "breakdown_name": "CCG of Residence",
    "level_tier": 1,
    "level_tables": ["stp_region_mapping"],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence").alias("breakdown"),
    F.col("CCG_CODE").alias("primary_level"),
    F.col("CCG_NAME").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prov_bd = cross_dict([ccg_res_bd, prov_bd])
ccg_res_age_band_higher_bd = cross_dict([ccg_res_bd, age_band_higher_bd])
ccg_res_age_band_common_bd = cross_dict([ccg_res_bd, age_band_common_bd])
ccg_res_age_band_chap1_bd = cross_dict([ccg_res_bd, age_band_chap1_bd])
ccg_res_age_band_chap45_bd = cross_dict([ccg_res_bd, age_band_chap45_bd])
ccg_res_age_band_chap10_bd = cross_dict([ccg_res_bd, age_band_chap10_bd])
ccg_res_age_band_chap10a_bd = cross_dict([ccg_res_bd, age_band_chap10a_bd])
ccg_res_age_band_chap11_bd = cross_dict([ccg_res_bd, age_band_chap11_bd])
ccg_res_upper_eth_bd = cross_dict([ccg_res_bd, upper_eth_bd])
ccg_res_lower_eth_bd = cross_dict([ccg_res_bd, lower_eth_bd])
ccg_res_gender_bd = cross_dict([ccg_res_bd, gender_bd])
ccg_res_imd_decile_bd = cross_dict([ccg_res_bd, imd_decile_bd])
ccg_res_imd_quintile_bd = cross_dict([ccg_res_bd, imd_quintile_bd])

# COMMAND ----------

# DBTITLE 1,CCG/Sub ICB Registration or Residence Breakdowns
ccg_prac_res_bd = {
    "breakdown_name": "CCG - Registration or Residence",
    "level_tier": 1,
    "level_tables": ["stp_region_mapping"],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_age_band_higher_bd = cross_dict([ccg_prac_res_bd, age_band_higher_bd])
ccg_prac_res_age_band_common_bd = cross_dict([ccg_prac_res_bd, age_band_common_bd])
ccg_prac_res_age_band_chap1_bd = cross_dict([ccg_prac_res_bd, age_band_chap1_bd])
ccg_prac_res_age_band_chap45_bd = cross_dict([ccg_prac_res_bd, age_band_chap45_bd])
ccg_prac_res_age_band_chap10_bd = cross_dict([ccg_prac_res_bd, age_band_chap10_bd])
ccg_prac_res_age_band_chap10a_bd = cross_dict([ccg_prac_res_bd, age_band_chap10a_bd])
ccg_prac_res_age_band_chap11_bd = cross_dict([ccg_prac_res_bd, age_band_chap11_bd])
ccg_prac_res_upper_eth_bd = cross_dict([ccg_prac_res_bd, upper_eth_bd])
ccg_prac_res_gender_bd = cross_dict([ccg_prac_res_bd, gender_bd])
ccg_prac_res_imd_decile_bd = cross_dict([ccg_prac_res_bd, imd_decile_bd])
ccg_prac_res_imd_quintile_bd = cross_dict([ccg_prac_res_bd, imd_quintile_bd])
ccg_prac_res_int_type_bd = cross_dict([ccg_prac_res_bd, int_type_bd])
ccg_prac_res_bed_type_bd = cross_dict([ccg_prac_res_bd, bed_type_bd])
ccg_prac_res_team_type_bd = cross_dict([ccg_prac_res_bd, team_type_bd])
ccg_prac_res_gender_age_band_lower_chap1_bd = cross_dict([ccg_prac_res_bd, gender_bd, age_band_chap1_bd])

# COMMAND ----------

# DBTITLE 1,STP/ICB Breakdowns
stp_res_bd = {
    "breakdown_name": "STP of Residence",
    "level_tier": 1,
    "level_tables": ["stp_region_mapping"],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_prac_res_bd = {
    "breakdown_name": "STP of GP Practice or Residence",
    "level_tier": 1,
    "level_tables": ["stp_region_mapping"],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of GP Practice or Residence").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_age_band_higher_bd = cross_dict([stp_res_bd, age_band_higher_bd])
stp_res_age_band_common_bd = cross_dict([stp_res_bd, age_band_common_bd])
stp_res_age_band_chap1_bd = cross_dict([stp_res_bd, age_band_chap1_bd])
stp_res_age_band_chap45_bd = cross_dict([stp_res_bd, age_band_chap45_bd])
stp_res_age_band_chap10_bd = cross_dict([stp_res_bd, age_band_chap10_bd])
stp_res_age_band_chap10a_bd = cross_dict([stp_res_bd, age_band_chap10a_bd])
stp_res_age_band_chap11_bd = cross_dict([stp_res_bd, age_band_chap11_bd])
stp_res_upper_eth_bd = cross_dict([stp_res_bd, upper_eth_bd])
stp_res_lower_eth_bd = cross_dict([stp_res_bd, lower_eth_bd])
stp_res_gender_bd = cross_dict([stp_res_bd, gender_bd])
stp_res_imd_decile_bd = cross_dict([stp_res_bd, imd_decile_bd])
stp_res_int_type_bd = cross_dict([stp_res_bd, int_type_bd])

# COMMAND ----------

# DBTITLE 1,Commissioning Region Breakdowns
comm_region_bd = {
    "breakdown_name": "Commissioning Region",
    "level_tier": 1,
    "level_tables": ["stp_region_mapping"],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_age_band_higher_bd = cross_dict([comm_region_bd, age_band_higher_bd])
comm_region_age_band_common_bd = cross_dict([comm_region_bd, age_band_common_bd])
comm_region_age_band_chap1_bd = cross_dict([comm_region_bd, age_band_chap1_bd])
comm_region_age_band_chap45_bd = cross_dict([comm_region_bd, age_band_chap45_bd])
comm_region_age_band_chap10_bd = cross_dict([comm_region_bd, age_band_chap10_bd])
comm_region_age_band_chap10a_bd = cross_dict([comm_region_bd, age_band_chap10a_bd])
comm_region_age_band_chap11_bd = cross_dict([comm_region_bd, age_band_chap11_bd])
comm_region_upper_eth_bd = cross_dict([comm_region_bd, upper_eth_bd])
comm_region_lower_eth_bd = cross_dict([comm_region_bd, lower_eth_bd])
comm_region_gender_bd = cross_dict([comm_region_bd, gender_bd])
comm_region_imd_decile_bd = cross_dict([comm_region_bd, imd_decile_bd])
comm_region_int_type_bd = cross_dict([comm_region_bd, int_type_bd])

# COMMAND ----------

# DBTITLE 1,Local Authority/Unitary Authority Breakdowns
la_bd = {
    "breakdown_name": "LAD/UA",
    "level_tier": 1,
    "level_tables": ["la"],
    "primary_level": F.col("LADistrictAuth"),
    "primary_level_desc": F.col("LADistrictAuthName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("LAD/UA").alias("breakdown"),
    F.col("level").alias("primary_level"),
    F.col("level_description").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
la_age_band_higher_bd = cross_dict([la_bd, age_band_higher_bd])
la_int_type_bd = cross_dict([la_bd, int_type_bd])

# COMMAND ----------

# DBTITLE 1,Standardised Rate Breakdowns
std_lower_eth_bd = {
    "breakdown_name": "England; Ethnicity (Lower Level)",
    "level_tier": 1,
    "level_tables": ["NHSDEthnicityDim"],
    "primary_level": F.col("EthnicGroupCode"),
    "primary_level_desc": F.col("EthnicGroupName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("key").alias("primary_level"),
    F.col("lower_description").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")    
  ]
}
std_upper_eth_bd = {
    "breakdown_name": "England; Ethnicity (Higher Level)",
    "level_tier": 1,
    "level_tables": ["NHSDEthnicityDim"],
    "primary_level": F.col("EthnicGroupCode"),
    "primary_level_desc": F.col("EthnicGroupName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
  "lookup_col" : [],
  "level_fields" : [
    F.lit("England; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("upper_description").alias("primary_level"),
    F.col("upper_description").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}

# COMMAND ----------

def print_var_name(variable):
    for name in globals():
        if eval(name) is variable:
            return name
print_var_name(std_upper_eth_bd), print_var_name(ccg_prac_res_gender_age_band_lower_chap1_bd)