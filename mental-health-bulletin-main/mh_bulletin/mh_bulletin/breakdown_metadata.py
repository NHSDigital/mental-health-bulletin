# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

 %run ./mhsds_functions

# COMMAND ----------

spark.conf.set("spark.databricks.queryWatchdog.maxQueryTasks", 1800000)

# COMMAND ----------

# DBTITLE 1,Functions
import pyspark.sql.functions as F

def list_dataframes():
    from pyspark.sql import DataFrame
    return [[k] for (k, v) in globals().items() if isinstance(v, DataFrame)]

def flatlist(list1):
  # change [[1,2,3],[4,5],...] to [1,2,3,4,5,...]
  return [item for sublist in list1 for item in sublist]  

def cross_join_level_listD(list1):
  ''' input list of 1 to 5 dictionaries from metadata to get cross-join'''
  
  len1 = len(list1)
  listU = flatlist([list1[i]["lookup_col"] for i in range(len1)])
  listC = [[[x[0],x[1]] if len(x) == 2 else [x[0]] for x in  list1[i]["level_list"]]
           for i in range(len1)]
  if len1 == 1: listLL = [x0 for x0 in listC[0]]
  if len1 == 2: listLL = [x0 + x1 for x0 in listC[0] for x1 in listC[1]]
  if len1 == 3: listLL = [x0 + x1 + x2 for x0 in listC[0] for x1 in listC[1] for x2 in listC[2]]
  if len1 == 4: listLL = [x0 + x1 + x2 + x3 for x0 in listC[0] for x1 in listC[1] for x2 in listC[2] for x3 in listC[3]]
  if len1 == 5: listLL = [x0 + x1 + x2 + x3 + x4 for x0 in listC[0] for x1 in listC[1]
                         for x2 in listC[2] for x3 in listC[3] for x4 in listC[4]]

  if 1 <= len1 and len1 <= 5:
    ''' dataframe option for output, commented out as returning dictionary instead
    schema1 = ", ".join([f"{x} string" for x in listU])
    print(schema1)
    
    df1 = spark.createDataFrame(listLL, schema = schema1)
    cols = [x for x in listU]
    return df1.select(cols)
    '''
    d1 = {"lookup_col": listU, "level_list": listLL}
    return d1

def cross_dict(list1):
  # input list of up to 4 single level dictionaries -> output cross join dictionary
  # please do not use double / triple level dictionaries as inputs
  len1 = len(list1)
  lln = [d["primary_level"] for d in list1] + [F.lit("NONE")]*(4-len1)
  lld = [d["primary_level_desc"] for d in list1] + [F.lit("NONE")]*(4-len1)
  cross_cols = cross_join_level_listD(list1)
  breakdown_name = "; ".join([d["breakdown_name"] for d in list1])
  
  '''
  replaceKey = {"England; ": ""}
  for key in replaceKey:
    breakdown_name = breakdown_name.replace(key, replaceKey[key])
  '''
  breakdown_name = breakdown_name.replace("England; ", "")
  
  dict1 = {
    "breakdown_name": breakdown_name,
    "level_tier": 0,
    "level_tables": [],
    "primary_level": lln[0],
    "primary_level_desc": lld[0],
    "secondary_level": lln[1],
    "secondary_level_desc": lld[1], 
    "third_level":lln[2],
    "third_level_desc": lld[2], 
    "fourth_level": lln[3],
    "fourth_level_desc": lld[3], 
    "level_list" : cross_cols["level_list"],
    "lookup_col" : cross_cols["lookup_col"],
    "level_fields" : [
    F.lit(breakdown_name).alias("breakdown"),
    lln[0].alias("primary_level"),
    lld[0].alias("primary_level_desc"),
    lln[1].alias("secondary_level"), 
    lld[1].alias("secondary_level_desc"),
    lln[2].alias("third_level"),   
    lld[2].alias("third_level_desc"),
    lln[3].alias("fourth_level"),   
    lld[3].alias("fourth_level_desc")]
  }  
  return dict1

# COMMAND ----------

# DBTITLE 1,Level lists
# ideally we would put these into map / reference tables and extract into list of tuples via python function
# these are used in the metadata for each breakdown

LL = {"age_group_higher_level": [("Under 18", ), ("18 and over", ), ("Unknown", )],
      "age_group_lower_common": [("Under 18", ), ("18 to 19", ),("20 to 24", ),("25 to 29", ),("30 to 34", ),
                                 ("35 to 39", ),("40 to 44", ),("45 to 49", ),("50 to 54", ),("55 to 59", ),
                                 ("60 to 64", ),("65 to 69", ),("70 to 74", ),("75 to 79", ),("80 to 84", ),
                                 ("85 to 89", ),("90 or over", ),("Unknown", )],
      "age_group_lower_chap1": [("0 to 5", ), ("6 to 10", ), ("11 to 15", ), ("16", ), ("17", ), ("18", ), ("19", ), 
                                ("20 to 24", ), ("25 to 29", ), ("30 to 34", ), ("35 to 39", ),("40 to 44", ),
                                ("45 to 49", ),("50 to 54", ),("55 to 59", ),("60 to 64", ),("65 to 69", ),
                                ("70 to 74", ),("75 to 79", ),("80 to 84", ),("85 to 89", ),("90 or over", ),
                                ("Unknown", )],
      "age_group_lower_chap45": [("Under 14", ),("14 to 15", ),("16 to 17", ),("18 to 19", ),("20 to 24", ),
                                 ("25 to 29", ),("30 to 34", ),("35 to 39", ),("40 to 44", ),("45 to 49", ),
                                 ("50 to 54", ),("55 to 59", ),("60 to 64", ),("65 to 69", ),("70 to 74", ),
                                 ("75 to 79", ),("80 to 84", ),("85 to 89", ),("90 or over", ),("Unknown", ),],
      "age_group_lower_chap10": [("14 to 15", ),("16 to 17", ),("18 to 19", ),("20 to 24", ),("25 to 29", ),
                                   ("30 to 34", ),("35 to 39", ),("40 to 44", ),("45 to 49", ),("50 to 54", ),
                                   ("55 to 59", ),("60 to 64", ),("65 or over", ),("Unknown", )],
      "age_group_lower_chap10a": [("0 to 13", ), ("14 to 17", ), ("18 to 19", ), ("20 to 24", ),("25 to 29", ),
                                   ("30 to 34", ),("35 to 39", ),("40 to 44", ),("45 to 49", ),("50 to 54", ),
                                   ("55 to 59", ),("60 to 64", ),("65 to 69", ), ("70 to 74", ), ("75 to 79", ), ("80 to 84", ), 
                                  ("85 to 89", ), ("90 or over", ), ("Unknown", )],
      "age_group_lower_chap11": [("Under 15", ),("15 to 19", ),("20 to 24", ),("25 to 29", ),("30 to 34", ),
                                 ("35 to 39", ),("40 to 44", ),("45 to 49", ),("50 to 54", ),("55 to 59", ),
                                 ("60 to 64", ),("65 or over", ),("Unknown", )],
      "age_group_lower_imd": [("Under 18", ), ("18 to 19", ),("20 to 24", ),("25 to 29", ),("30 to 34", ),
                                 ("35 to 39", ),("40 to 44", ),("45 to 49", ),("50 to 54", ),("55 to 59", ),
                                 ("60 to 64", ),("65 to 69", ),("70 to 74", ),("75 to 79", ),("80 to 84", ),
                                 ("85 or over", ),("Unknown", )],
      "Gender": [("1", "Male"),("2", "Female"),("3", "Non-binary"),("4", "Other (not listed)"),("9", "Indeterminate"),
                 ("Unknown", "Unknown"),],
      "LowerEthnicity": [("A", "British"),("B", "Irish"),("C", "Any Other White Background"),
                         ("D", "White and Black Caribbean"),("E", "White and Black African"),
                         ("F", "White and Asian"),("G", "Any Other Mixed Background"),("H", "Indian"),
                         ("J", "Pakistani"),("K", "Bangladeshi"),("L", "Any Other Asian Background"),
                         ("M", "Caribbean"),("N", "African"),("P", "Any Other Black Background"),("R", "Chinese"),
                         ("S", "Any Other Ethnic Group"),("Z", "Not Stated"),("99", "Not Known"),
                         ("Unknown", "Unknown"),],
      "UpperEthnicity": [("White", ),("Mixed", ),("Asian or Asian British", ),("Black or Black British", ),
                         ("Other Ethnic Groups", ),("Not Stated", ),("Not Known", ),("Unknown", ),],
      "IMD_Decile": [("01 Most deprived", ),("02 More deprived", ),("03 More deprived", ),("04 More deprived", ),
                     ("05 More deprived", ),("06 Less deprived", ),("07 Less deprived", ),("08 Less deprived", ),
                     ("09 Less deprived", ),("10 Least deprived", ),("Unknown", ),],
      "IMD_Quintile": [("01 Most deprived", ),("02", ),("03", ),("04", ),("05 Least deprived", ),("Unknown", ),],
      "IntType": [("01", "Physical restraint - Prone"),
                  ("04", "Mechanical restraint"),
                  ("05", "Seclusion"),
                  ("06", "Segregation"),
                  ("07", "Physical restraint - Standing"),
                  ("08", "Physical restraint - Restrictive escort"),
                  ("09", "Physical restraint - Supine"),
                  ("10", "Physical restraint - Side"),
                  ("11", "Physical restraint - Seated"), 
                  ("12", "Physical restraint - Kneeling"),
                  ("13", "Physical restraint - Other (not listed)"),
                  ("14", "Chemical restraint - Injection (Rapid Tranquillisation)"),
                  ("15", "Chemical restraint - Injection (Non Rapid Tranquillisation)"),
                  ("16", "Chemical restraint - Oral"),
                  ("17", "Chemical restraint - Other (not listed)"),
                  ("Unknown", "Unknown"),],
      "ServTeamTypeRefToMH": [('A01', 'Day Care Services'),
                                ('A02', 'Crisis Resolution Team/Home Treatment'),
                                ('A03', 'Adult Community Mental Health Team'),
                                ('A04', 'Older People Community Mental Health Team'),
                                ('A05', 'Assertive Outreach Team'),
                                ('A06', 'Rehabilitation and Recovery Team'),
                                ('A07', 'General Psychiatry'),
                                ('A08', 'Psychiatric Liaison'),
                                ('A09', 'Psychotherapy Service'),
                                ('A10', 'Psychological Therapy Service (IAPT)'),
                                ('A11', 'Psychological Therapy Service (non-IAPT)'),
                                ('A12', 'Young Onset Dementia'),
                                ('A13', 'Personality Disorder Service'),
                                ('A14', 'Early Intervention in Psychosis Team'),
                                ('A15', 'Primary Care Mental Health Services'),
                                ('A16', 'Memory Services/Clinic'),
                                ('B01', 'Forensic Service'),
                                ('B02', 'Community Forensic Service'),
                                ('C01', 'Learning Disability Service'),
                                ('C02', 'Autistic Spectrum Disorder Service'),
                                ('C03', 'Peri-Natal Mental Illness'),
                                ('C04', 'Eating Disorders/Dietetics'),
                                ('D01', 'Substance Misuse Team'),
                                ('D02', 'Criminal Justice Liaison and Diversion Service'),
                                ('D03', 'Prison Psychiatric Inreach Service'),
                                ('D04', 'Asylum Service'),
                                ('E01', 'Community Team for Learning Disabilities'),
                                ('E02', 'Epilepsy/Neurological Service'),
                                ('E03', 'Specialist Parenting Service'),
                                ('E04', 'Enhanced/Intensive Support Service'),
                                ('Z01', 'Other Mental Health Service - in scope of National Tariff Payment System'),
                                ('Z02', 'Other Mental Health Service - out of scope of National Tariff Payment System'),
                                ('ZZZ', 'Other Mental Health Service'),
                                ("Unknown", "Unknown"),],
      
      "ServiceType": [('Core community mental health', ),
                          ('Early Intervention Team for Psychosis', ),
                          ('Individual Placement and Support', ),
                          ('Specialist Perinatal Mental Health', ),
                          ('Autism Service', ),
                          ('24/7 Crisis Response Line', ),
                          ('Crisis and acute mental health activity in community settings', ),
                          ('Mental health activity in general hospitals', ),
                          ('LDA', ),
                          ('Forensic Services', ),
                          ('Specialist Services', ),
                          ('Other mental health services', ),
                          ('Other', ),
                          ('Unknown', )],

      
      "DNAReason": [("2", "Appointment cancelled by, or on behalf of, the patient"),
                    ("3", "Did not attend - no advance warning given"),
                    ("4", "Appointment cancelled or postponed by the health care provider"),
                    ("5", "Seen, having attended on time or, if late, before the relevant care professional was ready to see the patient"),
                    ("6", "Arrived late, after the relevant care professional was ready to see the patient, but was seen"),
                    ("7", "Did not attend - patient arrived late and could not be seen"),
                    ("Unknown", "Unknown"),],
      "BedType": [("CYP Acute", ),("CYP Specialist", ),("Adult Acute", ),("Adult Specialist", ),("Older Adult Acute", ),("Invalid", ),],
      "ProviderType":[("NHS", "NHS Providers"),("Non NHS", "Non NHS Providers"),]}

# COMMAND ----------

# DBTITLE 1,National Breakdowns
eng_bd = {
    "breakdown_name": "England",
    "level_tier": 0,
    "level_tables": [],
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("age_group_higher_level"),
    "primary_level_desc": F.col("age_group_higher_level"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["age_group_higher_level"],
  "lookup_col" : ["age_group_higher_level"],
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("Der_Gender"),
    "primary_level_desc": F.col("Der_Gender_Desc"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["Gender"],
  "lookup_col" : ["Der_Gender", "Der_Gender_Desc"],
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("LowerEthnicityCode"),
    "primary_level_desc": F.col("LowerEthnicityName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["LowerEthnicity"],
  "lookup_col" : ["LowerEthnicityCode", "LowerEthnicityName"],
  "level_fields" : [
    F.lit("England; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("LowerEthnicityCode").alias("primary_level"),
    F.col("LowerEthnicityName").alias("primary_level_desc"),
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("UpperEthnicity"),
    "primary_level_desc": F.col("UpperEthnicity"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["UpperEthnicity"],
  "lookup_col" : ["UpperEthnicity"],
  "level_fields" : [
    F.lit("England; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("UpperEthnicity").alias("primary_level"),
    F.col("UpperEthnicity").alias("primary_level_desc"),
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("IMD_Decile"),
    "primary_level_desc": F.col("IMD_Decile"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["IMD_Decile"],
  "lookup_col" : ["IMD_Decile"],
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("IMD_Quintile"),
    "primary_level_desc": F.col("IMD_Quintile"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["IMD_Quintile"],
  "lookup_col" : ["IMD_Quintile"],
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
  "level_tier": 0,
  "level_tables": [],
  "primary_level": F.col("age_group_lower_common"),
  "primary_level_desc": F.col("age_group_lower_common"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : LL["age_group_lower_common"],
  "lookup_col" : ["age_group_lower_common"],
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
  "level_tier": 0,
  "level_tables": [],
  "primary_level": F.col("age_group_lower_chap1"),
  "primary_level_desc": F.col("age_group_lower_chap1"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : LL["age_group_lower_chap1"],
  "lookup_col" : ["age_group_lower_chap1"],
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
  "level_tier": 0,
  "level_tables": [],
  "primary_level": F.col("age_group_lower_chap45"),
  "primary_level_desc": F.col("age_group_lower_chap45"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : LL["age_group_lower_chap45"],
  "lookup_col" : ["age_group_lower_chap45"],
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
  "level_tier": 0,
  "level_tables": [],
  "primary_level": F.col("age_group_lower_chap10"),
  "primary_level_desc": F.col("age_group_lower_chap10"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),   
  "level_list" : LL["age_group_lower_chap10"],
  "lookup_col" : ["age_group_lower_chap10"],
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
  "level_tier": 0,
  "level_tables": [],
  "primary_level": F.col("age_group_lower_chap10a"),
  "primary_level_desc": F.col("age_group_lower_chap10a"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),   
  "level_list" : LL["age_group_lower_chap10a"],
  "lookup_col" : ["age_group_lower_chap10a"],
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
  "level_tier": 0,
  "level_tables": [],
  "primary_level": F.col("age_group_lower_chap11"),
  "primary_level_desc": F.col("age_group_lower_chap11"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : LL["age_group_lower_chap11"],
  "lookup_col" : ["age_group_lower_chap11"],
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
  "level_tier": 0,
  "level_tables": [],
  "primary_level": F.col("age_group_lower_imd"),
  "primary_level_desc": F.col("age_group_lower_imd"),
  "secondary_level": F.lit("NONE"),
  "secondary_level_desc": F.lit("NONE"), 
  "third_level": F.lit("NONE"),
  "third_level_desc": F.lit("NONE"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"), 
  "level_list" : LL["age_group_lower_imd"],
  "lookup_col" : ["age_group_lower_imd"],
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("IntTypeCode"),
    "primary_level_desc": F.col("IntTypeName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["IntType"],
   "lookup_col" : ["IntTypeCode", "IntTypeName"],
   "level_fields" : [
    F.lit("England; Intervention Type").alias("breakdown"),
    F.col("IntTypeCode").alias("primary_level"),
    F.col("IntTypeName").alias("primary_level_desc"),
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("ServTeamTypeRefToMH"),
    "primary_level_desc": F.col("TeamTypeName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["ServTeamTypeRefToMH"],
  "lookup_col" : ["ServTeamTypeRefToMH", "TeamTypeName"],
  "level_fields" : [
    F.lit("England; Service or Team Type").alias("breakdown"),
    F.col("ServTeamTypeRefToMH").alias("primary_level"),
    F.col("TeamTypeName").alias("primary_level_desc"),
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("ServiceType"),
    "primary_level_desc": F.col("ServiceType"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["ServiceType"],
  "lookup_col" : ["ServiceType"],
  "level_fields" : [
    F.lit("England; Service Type as Reported to MHSDS").alias("breakdown"),
    F.col("ServiceType").alias("primary_level"),
    F.col("ServiceType").alias("primary_level_desc"),
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("AttCode"),
    "primary_level_desc": F.col("AttName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["DNAReason"],
  "lookup_col" : ["AttCode", "AttName"],
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("Bed_Type"),
    "primary_level_desc": F.col("Bed_Type"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["BedType"],
  "lookup_col" : ["Bed_Type"],
  "level_fields" : [
    F.lit("England; Bed Type as Reported to MHSDS").alias("breakdown"),
    F.col("Bed_Type").alias("primary_level"),
    F.col("Bed_Type").alias("primary_level_desc"),
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("ProvTypeCode"),
    "primary_level_desc": F.col("ProvTypeName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [("NHS", "NHS Providers"), ("Non NHS", "Non NHS Providers"),],
  "lookup_col" : ["ProvTypeCode", "ProvTypeName"],
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
prov_age_band_higher_bd = {
    "breakdown_name": "Provider; Age Group (Higher Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", age_band_higher_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("age_group_higher_level"),
    "secondary_level_desc": F.col("age_group_higher_level"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Age Group (Higher Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("age_group_higher_level").alias("secondary_level"), 
    F.col("age_group_higher_level").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_age_band_common_bd = {
    "breakdown_name": "Provider; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", age_band_common_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("age_group_lower_common"),
    "secondary_level_desc": F.col("age_group_lower_common"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Age Group (Lower Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("age_group_lower_common").alias("secondary_level"), 
    F.col("age_group_lower_common").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_age_band_chap1_bd = {
    "breakdown_name": "Provider; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", age_band_chap1_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("age_group_lower_chap1"),
    "secondary_level_desc": F.col("age_group_lower_chap1"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Age Group (Lower Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap1").alias("secondary_level"), 
    F.col("age_group_lower_chap1").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_age_band_chap45_bd = {
    "breakdown_name": "Provider; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", age_band_chap45_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("age_group_lower_chap45"),
    "secondary_level_desc": F.col("age_group_lower_chap45"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Age Group (Lower Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap45").alias("secondary_level"), 
    F.col("age_group_lower_chap45").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_age_band_chap10_bd = {
    "breakdown_name": "Provider; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", age_band_chap10_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("age_group_lower_chap10"),
    "secondary_level_desc": F.col("age_group_lower_chap10"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Age Group (Lower Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10").alias("secondary_level"), 
    F.col("age_group_lower_chap10").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_age_band_chap10a_bd = {
    "breakdown_name": "Provider; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", age_band_chap10a_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("age_group_lower_chap10a"),
    "secondary_level_desc": F.col("age_group_lower_chap10a"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Age Group (Lower Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10a").alias("secondary_level"), 
    F.col("age_group_lower_chap10a").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_age_band_chap11_bd = {
    "breakdown_name": "Provider; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", age_band_chap11_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("age_group_lower_chap11"),
    "secondary_level_desc": F.col("age_group_lower_chap11"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Age Group (Lower Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap11").alias("secondary_level"), 
    F.col("age_group_lower_chap11").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_upper_eth_bd = {
    "breakdown_name": "Provider; Ethnicity (Higher Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", upper_eth_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("UpperEthnicity"),
    "secondary_level_desc": F.col("UpperEthnicity"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("UpperEthnicity").alias("secondary_level"), 
    F.col("UpperEthnicity").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_lower_eth_bd = {
    "breakdown_name": "Provider; Ethnicity (Lower Level)",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", lower_eth_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("LowerEthnicityCode"),
    "secondary_level_desc": F.col("LowerEthnicityName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("LowerEthnicityCode").alias("secondary_level"), 
    F.col("LowerEthnicityName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_gender_bd = {
    "breakdown_name": "Provider; Gender",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", gender_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("Der_Gender"),
    "secondary_level_desc": F.col("Der_Gender_Desc"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Gender").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("Der_Gender").alias("secondary_level"), 
    F.col("Der_Gender_Desc").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_imd_decile_bd = {
    "breakdown_name": "Provider; IMD Decile",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", imd_decile_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("IMD_Decile"),
    "secondary_level_desc": F.col("IMD_Decile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; IMD Decile").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("IMD_Decile").alias("secondary_level"), 
    F.col("IMD_Decile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_imd_quintile_bd = {
    "breakdown_name": "Provider; IMD Quintile",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", imd_quintile_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("IMD_Quintile"),
    "secondary_level_desc": F.col("IMD_Quintile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; IMD Quintile").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("IMD_Quintile").alias("secondary_level"), 
    F.col("IMD_Quintile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_int_type_bd = {
    "breakdown_name": "Provider; Intervention Type",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", int_type_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("IntTypeCode"),
    "secondary_level_desc": F.col("IntTypeName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Intervention Type").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("IntTypeCode").alias("secondary_level"), 
    F.col("IntTypeName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_team_type_bd = {
    "breakdown_name": "Provider; Service or Team Type",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", team_type_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("ServTeamTypeRefToMH"),
    "secondary_level_desc": F.col("TeamTypeName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Service or Team Type").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("ServTeamTypeRefToMH").alias("secondary_level"), 
    F.col("TeamTypeName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
prov_bed_type_bd = {
    "breakdown_name": "Provider; Bed Type as Reported to MHSDS",
    "level_tier": 2,
    "level_tables": ["mhsds_providers_in_year", bed_type_bd],
    "primary_level": F.col("OrgIDProv"),
    "primary_level_desc": F.col("Provider_Name"),
    "secondary_level": F.col("Bed_Type"),
    "secondary_level_desc": F.col("Bed_Type"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Provider; Bed Type as Reported to MHSDS").alias("breakdown"),
    F.col("OrgIDProv").alias("primary_level"),
    F.col("Provider_Name").alias("primary_level_desc"),
    F.col("Bed_Type").alias("secondary_level"), 
    F.col("Bed_Type").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

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
ccg_prov_bd = {
    "breakdown_name": "CCG of Residence; Provider",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", "mhsds_providers_in_year"],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("OrgIDProv"),
    "secondary_level_desc": F.col("Provider_Name"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Provider").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("OrgIDProv").alias("secondary_level"), 
    F.col("Provider_Name").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

ccg_res_age_band_higher_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_higher_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_higher_level"),
    "secondary_level_desc": F.col("age_group_higher_level"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Higher Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_higher_level").alias("secondary_level"), 
    F.col("age_group_higher_level").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

ccg_res_age_band_common_bd = {
    "breakdown_name": "CCG of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_common_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_common"),
    "secondary_level_desc": F.col("age_group_lower_common"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_common").alias("secondary_level"), 
    F.col("age_group_lower_common").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

ccg_res_age_band_chap1_bd = {
    "breakdown_name": "CCG of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap1_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap1"),
    "secondary_level_desc": F.col("age_group_lower_chap1"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap1").alias("secondary_level"), 
    F.col("age_group_lower_chap1").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_age_band_chap45_bd = {
    "breakdown_name": "CCG of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap45_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap45"),
    "secondary_level_desc": F.col("age_group_lower_chap45"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap45").alias("secondary_level"), 
    F.col("age_group_lower_chap45").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_age_band_chap10_bd = {
    "breakdown_name": "CCG of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap10"),
    "secondary_level_desc": F.col("age_group_lower_chap10"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10").alias("secondary_level"), 
    F.col("age_group_lower_chap10").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_age_band_chap10a_bd = {
    "breakdown_name": "CCG of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10a_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap10a"),
    "secondary_level_desc": F.col("age_group_lower_chap10a"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10a").alias("secondary_level"), 
    F.col("age_group_lower_chap10a").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_age_band_chap11_bd = {
    "breakdown_name": "CCG of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap11_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap11"),
    "secondary_level_desc": F.col("age_group_lower_chap11"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap11").alias("secondary_level"), 
    F.col("age_group_lower_chap11").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_upper_eth_bd = {
    "breakdown_name": "CCG of Residence; Ethnicity (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", upper_eth_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("UpperEthnicity"),
    "secondary_level_desc": F.col("UpperEthnicity"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("UpperEthnicity").alias("secondary_level"), 
    F.col("UpperEthnicity").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_lower_eth_bd = {
    "breakdown_name": "CCG of Residence; Ethnicity (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", lower_eth_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("LowerEthnicityCode"),
    "secondary_level_desc": F.col("LowerEthnicityName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("LowerEthnicityCode").alias("secondary_level"), 
    F.col("LowerEthnicityName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_gender_bd = {
    "breakdown_name": "CCG of Residence; Gender",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", gender_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("Der_Gender"),
    "secondary_level_desc": F.col("Der_Gender_Desc"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; Gender").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("Der_Gender").alias("secondary_level"), 
    F.col("Der_Gender_Desc").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_res_imd_decile_bd = {
    "breakdown_name": "CCG of Residence; IMD Decile",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", imd_decile_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("IMD_Decile"),
    "secondary_level_desc": F.col("IMD_Decile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG of Residence; IMD Decile").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("IMD_Decile").alias("secondary_level"), 
    F.col("IMD_Decile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

ccg_res_imd_quintile_bd = {
    "breakdown_name": "CCG - Registration or Residence; IMD Quintile",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", imd_quintile_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("IMD_Quintile"),
    "secondary_level_desc": F.col("IMD_Quintile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; IMD Quintile").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("IMD_Quintile").alias("secondary_level"), 
    F.col("IMD_Quintile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

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
ccg_prac_res_age_band_higher_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_higher_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_higher_level"),
    "secondary_level_desc": F.col("age_group_higher_level"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Higher Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_higher_level").alias("secondary_level"), 
    F.col("age_group_higher_level").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_age_band_common_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_common_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_common"),
    "secondary_level_desc": F.col("age_group_lower_common"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_common").alias("secondary_level"), 
    F.col("age_group_lower_common").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_age_band_chap1_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap1_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap1"),
    "secondary_level_desc": F.col("age_group_lower_chap1"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap1").alias("secondary_level"), 
    F.col("age_group_lower_chap1").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_age_band_chap45_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap45_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap45"),
    "secondary_level_desc": F.col("age_group_lower_chap45"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap45").alias("secondary_level"), 
    F.col("age_group_lower_chap45").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_age_band_chap10_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap10"),
    "secondary_level_desc": F.col("age_group_lower_chap10"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10").alias("secondary_level"), 
    F.col("age_group_lower_chap10").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_age_band_chap10a_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10a_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap10a"),
    "secondary_level_desc": F.col("age_group_lower_chap10a"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10a").alias("secondary_level"), 
    F.col("age_group_lower_chap10a").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_age_band_chap11_bd = {
    "breakdown_name": "CCG - Registration or Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap11_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("age_group_lower_chap11"),
    "secondary_level_desc": F.col("age_group_lower_chap11"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap11").alias("secondary_level"), 
    F.col("age_group_lower_chap11").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_upper_eth_bd = {
    "breakdown_name": "CCG - Registration or Residence; Ethnicity (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", upper_eth_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("UpperEthnicity"),
    "secondary_level_desc": F.col("UpperEthnicity"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("UpperEthnicity").alias("secondary_level"), 
    F.col("UpperEthnicity").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_gender_bd = {
    "breakdown_name": "CCG - Registration or Residence; Gender",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", gender_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("Der_Gender"),
    "secondary_level_desc": F.col("Der_Gender_Desc"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Gender").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("Der_Gender").alias("secondary_level"), 
    F.col("Der_Gender_Desc").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_imd_decile_bd = {
    "breakdown_name": "CCG - Registration or Residence; IMD Decile",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", imd_decile_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("IMD_Decile"),
    "secondary_level_desc": F.col("IMD_Decile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; IMD Decile").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("IMD_Decile").alias("secondary_level"), 
    F.col("IMD_Decile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_imd_quintile_bd = {
    "breakdown_name": "CCG - Registration or Residence; IMD Quintile",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", imd_quintile_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("IMD_Quintile"),
    "secondary_level_desc": F.col("IMD_Quintile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; IMD Quintile").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("IMD_Quintile").alias("secondary_level"), 
    F.col("IMD_Quintile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_int_type_bd = {
    "breakdown_name": "CCG - Registration or Residence; Intervention Type",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", int_type_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("IntTypeCode"),
    "secondary_level_desc": F.col("IntTypeName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Intervention Type").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("IntTypeCode").alias("secondary_level"), 
    F.col("IntTypeName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_bed_type_bd = {
    "breakdown_name": "CCG - Registration or Residence; Bed Type as Reported to MHSDS",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", bed_type_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("Bed_Type"),
    "secondary_level_desc": F.col("Bed_Type"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Bed Type as Reported to MHSDS").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("Bed_Type").alias("secondary_level"), 
    F.col("Bed_Type").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_team_type_bd = {
    "breakdown_name": "CCG - Registration or Residence; Service or Team Type",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", team_type_bd],
    "primary_level": F.col("CCG_Code"),
    "primary_level_desc": F.col("CCG_Name"),
    "secondary_level": F.col("ServTeamTypeRefToMH"),
    "secondary_level_desc": F.col("TeamTypeName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("CCG - Registration or Residence; Service or Team Type").alias("breakdown"),
    F.col("CCG_Code").alias("primary_level"),
    F.col("CCG_Name").alias("primary_level_desc"),
    F.col("ServTeamTypeRefToMH").alias("secondary_level"), 
    F.col("TeamTypeName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
ccg_prac_res_gender_age_band_lower_chap1_bd = {  
  "breakdown_name": "CCG - Registration or Residence; Gender; Age Group (Lower Level)",  
  "level_tier": 3,
  "level_tables": ["stp_region_mapping", gender_bd, age_band_chap1_bd],
  "primary_level": F.col("CCG_Code"),
  "primary_level_desc": F.col("CCG_Name"),
  "secondary_level": F.col("Der_Gender"),
  "secondary_level_desc": F.col("Der_Gender_Desc"), 
  "third_level": F.col("age_group_lower_chap1"),
  "third_level_desc": F.col("age_group_lower_chap1"), 
  "fourth_level": F.lit("NONE"),
  "fourth_level_desc": F.lit("NONE"),
  "level_list" : [],
  "lookup_col": [],
  "level_fields" : [ 
      F.lit("CCG - Registration or Residence; Gender; Age Group (Lower Level)").alias("breakdown"),
      F.col("CCG_Code").alias("primary_level"),
      F.col("CCG_Name").alias("primary_level_desc"),
      F.col("Der_Gender").alias("secondary_level"), 
      F.col("Der_Gender_Desc").alias("secondary_level_desc"),
      F.col("age_group_lower_chap1").alias("third_level"),   
      F.col("age_group_lower_chap1").alias("third_level_desc"),
      F.lit("NONE").alias("fourth_level"),   
      F.lit("NONE").alias("fourth_level_desc")  
    ],   
}

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
stp_res_age_band_higher_bd = {
    "breakdown_name": "STP of Residence; Age Group (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_higher_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("age_group_higher_level"),
    "secondary_level_desc": F.col("age_group_higher_level"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Age Group (Higher Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("age_group_higher_level").alias("secondary_level"), 
    F.col("age_group_higher_level").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_age_band_common_bd = {
    "breakdown_name": "STP of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_common_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("age_group_lower_common"),
    "secondary_level_desc": F.col("age_group_lower_common"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("age_group_lower_common").alias("secondary_level"), 
    F.col("age_group_lower_common").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_age_band_chap1_bd = {
    "breakdown_name": "STP of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap1_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("age_group_lower_chap1"),
    "secondary_level_desc": F.col("age_group_lower_chap1"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap1").alias("secondary_level"), 
    F.col("age_group_lower_chap1").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_age_band_chap45_bd = {
    "breakdown_name": "STP of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap45_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("age_group_lower_chap45"),
    "secondary_level_desc": F.col("age_group_lower_chap45"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap45").alias("secondary_level"), 
    F.col("age_group_lower_chap45").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_age_band_chap10_bd = {
    "breakdown_name": "STP of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("age_group_lower_chap10"),
    "secondary_level_desc": F.col("age_group_lower_chap10"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10").alias("secondary_level"), 
    F.col("age_group_lower_chap10").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_age_band_chap10a_bd = {
    "breakdown_name": "STP of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10a_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("age_group_lower_chap10a"),
    "secondary_level_desc": F.col("age_group_lower_chap10a"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10a").alias("secondary_level"), 
    F.col("age_group_lower_chap10a").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_age_band_chap11_bd = {
    "breakdown_name": "STP of Residence; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap11_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("age_group_lower_chap11"),
    "secondary_level_desc": F.col("age_group_lower_chap11"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Age Group (Lower Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap11").alias("secondary_level"), 
    F.col("age_group_lower_chap11").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_upper_eth_bd = {
    "breakdown_name": "STP of Residence; Ethnicity (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", upper_eth_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("UpperEthnicity"),
    "secondary_level_desc": F.col("UpperEthnicity"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("UpperEthnicity").alias("secondary_level"), 
    F.col("UpperEthnicity").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_lower_eth_bd = {
    "breakdown_name": "STP of Residence; Ethnicity (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", lower_eth_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("LowerEthnicityCode"),
    "secondary_level_desc": F.col("LowerEthnicityName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("LowerEthnicityCode").alias("secondary_level"), 
    F.col("LowerEthnicityName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_gender_bd = {
    "breakdown_name": "STP of Residence; Gender",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", gender_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("Der_Gender"),
    "secondary_level_desc": F.col("Der_Gender_Desc"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Gender").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("Der_Gender").alias("secondary_level"), 
    F.col("Der_Gender_Desc").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_imd_decile_bd = {
    "breakdown_name": "STP of Residence; IMD Decile",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", imd_decile_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("IMD_Decile"),
    "secondary_level_desc": F.col("IMD_Decile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; IMD Decile").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("IMD_Decile").alias("secondary_level"), 
    F.col("IMD_Decile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
stp_res_int_type_bd = {
    "breakdown_name": "STP of Residence; Intervention Type",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", int_type_bd],
    "primary_level": F.col("STP_Code"),
    "primary_level_desc": F.col("STP_Name"),
    "secondary_level": F.col("IntTypeCode"),
    "secondary_level_desc": F.col("IntTypeName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("STP of Residence; Intervention Type").alias("breakdown"),
    F.col("STP_Code").alias("primary_level"),
    F.col("STP_Name").alias("primary_level_desc"),
    F.col("IntTypeCode").alias("secondary_level"), 
    F.col("IntTypeName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

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
comm_region_age_band_higher_bd = {
    "breakdown_name": "Commissioning Region; Age Group (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_higher_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("age_group_higher_level"),
    "secondary_level_desc": F.col("age_group_higher_level"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Age Group (Higher Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("age_group_higher_level").alias("secondary_level"), 
    F.col("age_group_higher_level").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_age_band_common_bd = {
    "breakdown_name": "Commissioning Region; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_common_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("age_group_lower_common"),
    "secondary_level_desc": F.col("age_group_lower_common"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Age Group (Lower Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("age_group_lower_common").alias("secondary_level"), 
    F.col("age_group_lower_common").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_age_band_chap1_bd = {
    "breakdown_name": "Commissioning Region; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap1_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("age_group_lower_chap1"),
    "secondary_level_desc": F.col("age_group_lower_chap1"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Age Group (Lower Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap1").alias("secondary_level"), 
    F.col("age_group_lower_chap1").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_age_band_chap45_bd = {
    "breakdown_name": "Commissioning Region; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap45_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("age_group_lower_chap45"),
    "secondary_level_desc": F.col("age_group_lower_chap45"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Age Group (Lower Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap45").alias("secondary_level"), 
    F.col("age_group_lower_chap45").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_age_band_chap10_bd = {
    "breakdown_name": "Commissioning Region; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("age_group_lower_chap10"),
    "secondary_level_desc": F.col("age_group_lower_chap10"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Age Group (Lower Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10").alias("secondary_level"), 
    F.col("age_group_lower_chap10").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_age_band_chap10a_bd = {
    "breakdown_name": "Commissioning Region; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap10a_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("age_group_lower_chap10a"),
    "secondary_level_desc": F.col("age_group_lower_chap10a"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Age Group (Lower Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap10a").alias("secondary_level"), 
    F.col("age_group_lower_chap10a").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_age_band_chap11_bd = {
    "breakdown_name": "Commissioning Region; Age Group (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", age_band_chap11_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("age_group_lower_chap11"),
    "secondary_level_desc": F.col("age_group_lower_chap11"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Age Group (Lower Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("age_group_lower_chap11").alias("secondary_level"), 
    F.col("age_group_lower_chap11").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_upper_eth_bd = {
    "breakdown_name": "Commissioning Region; Ethnicity (Higher Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", upper_eth_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("UpperEthnicity"),
    "secondary_level_desc": F.col("UpperEthnicity"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("UpperEthnicity").alias("secondary_level"), 
    F.col("UpperEthnicity").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_lower_eth_bd = {
    "breakdown_name": "Commissioning Region; Ethnicity (Lower Level)",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", lower_eth_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("LowerEthnicityCode"),
    "secondary_level_desc": F.col("LowerEthnicityName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("LowerEthnicityCode").alias("secondary_level"), 
    F.col("LowerEthnicityName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_gender_bd = {
    "breakdown_name": "Commissioning Region; Gender",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", gender_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("Der_Gender"),
    "secondary_level_desc": F.col("Der_Gender_Desc"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Gender").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("Der_Gender").alias("secondary_level"), 
    F.col("Der_Gender_Desc").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_imd_decile_bd = {
    "breakdown_name": "Commissioning Region; IMD Decile",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", imd_decile_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("IMD_Decile"),
    "secondary_level_desc": F.col("IMD_Decile"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; IMD Decile").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("IMD_Decile").alias("secondary_level"), 
    F.col("IMD_Decile").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
comm_region_int_type_bd = {
    "breakdown_name": "Commissioning Region; Intervention Type",
    "level_tier": 2,
    "level_tables": ["stp_region_mapping", int_type_bd],
    "primary_level": F.col("Region_Code"),
    "primary_level_desc": F.col("Region_Name"),
    "secondary_level": F.col("IntTypeCode"),
    "secondary_level_desc": F.col("IntTypeName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("Commissioning Region; Intervention Type").alias("breakdown"),
    F.col("Region_Code").alias("primary_level"),
    F.col("Region_Name").alias("primary_level_desc"),
    F.col("IntTypeCode").alias("secondary_level"), 
    F.col("IntTypeName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

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
la_age_band_higher_bd = {
    "breakdown_name": "LAD/UA; Age Group (Higher Level)",
    "level_tier": 2,
    "level_tables": ["la", age_band_higher_bd],
    "primary_level": F.col("LADistrictAuth"),
    "primary_level_desc": F.col("LADistrictAuthName"),
    "secondary_level": F.col("age_group_higher_level"),
    "secondary_level_desc": F.col("age_group_higher_level"), 
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
    F.col("age_group_higher_level").alias("secondary_level"), 
    F.col("age_group_higher_level").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}
la_int_type_bd = {
    "breakdown_name": "LAD/UA; Intervention Type",
    "level_tier": 2,
    "level_tables": ["la", int_type_bd],
    "primary_level": F.col("LADistrictAuth"),
    "primary_level_desc": F.col("LADistrictAuthName"),
    "secondary_level": F.col("IntTypeCode"),
    "secondary_level_desc": F.col("IntTypeName"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : [],
    "lookup_col" : [],
    "level_fields" : [ 
    F.lit("LAD/UA; Intervention Type").alias("breakdown"),
    F.col("level").alias("primary_level"),
    F.col("level_description").alias("primary_level_desc"),
    F.col("IntTypeCode").alias("secondary_level"), 
    F.col("IntTypeName").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")
    ],
}

# COMMAND ----------

# DBTITLE 1,Standardised Rate Breakdowns
std_lower_eth_bd = {
    "breakdown_name": "England; Ethnicity (Lower Level)",
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("EthnicGroupCode"),
    "primary_level_desc": F.col("EthnicGroupName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["LowerEthnicity"],
  "lookup_col" : ["Ethnic_Code", "Ethnic_Name"],
  "level_fields" : [
    F.lit("England; Ethnicity (Lower Level)").alias("breakdown"),
    F.col("Ethnic_Code").alias("primary_level"),
    F.col("Ethnic_Name").alias("primary_level_desc"),
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
    "level_tier": 0,
    "level_tables": [],
    "primary_level": F.col("EthnicGroupCode"),
    "primary_level_desc": F.col("EthnicGroupName"),
    "secondary_level": F.lit("NONE"),
    "secondary_level_desc": F.lit("NONE"), 
    "third_level": F.lit("NONE"),
    "third_level_desc": F.lit("NONE"), 
    "fourth_level": F.lit("NONE"),
    "fourth_level_desc": F.lit("NONE"), 
    "level_list" : LL["UpperEthnicity"],
  "lookup_col" : ["UpperEthnicity"],
  "level_fields" : [
    F.lit("England; Ethnicity (Higher Level)").alias("breakdown"),
    F.col("UpperEthnicity").alias("primary_level"),
    F.col("UpperEthnicity").alias("primary_level_desc"),
    F.lit("NONE").alias("secondary_level"), 
    F.lit("NONE").alias("secondary_level_desc"),
    F.lit("NONE").alias("third_level"),   
    F.lit("NONE").alias("third_level_desc"),
    F.lit("NONE").alias("fourth_level"),   
    F.lit("NONE").alias("fourth_level_desc")     
  ],
}