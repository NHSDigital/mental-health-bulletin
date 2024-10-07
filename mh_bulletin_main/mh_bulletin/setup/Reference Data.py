# Databricks notebook source
 %run ../mhsds_functions

# COMMAND ----------

 %md
 #### Demographic Reference Data

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.eng_desc;
 INSERT INTO $db_output.eng_desc VALUES
 ("England")

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.Der_Gender;
 INSERT INTO $db_output.Der_Gender VALUES
 ("1", "Male"),
 ("2", "Female"),
 ("3", "Non-binary"),
 ("4", "Other (not listed)"),
 ("9", "Indeterminate"),
 ("UNKNOWN", "UNKNOWN")

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.NHSDEthnicityDim;
 INSERT INTO $db_output.NHSDEthnicityDim VALUES
 ('A', '1', 'British', 'White'),
 ('B', '1', 'Irish', 'White'),
 ('C', '1', 'Any Other White Background', 'White'),
 ('D', '2', 'White and Black Caribbean', 'Mixed'),
 ('E', '2', 'White and Black African', 'Mixed'),
 ('F', '2', 'White and Asian', 'Mixed'),
 ('G', '2', 'Any Other Mixed Background', 'Mixed'),
 ('H', '3', 'Indian', 'Asian or Asian British'),
 ('J', '3', 'Pakistani', 'Asian or Asian British'),
 ('K', '3', 'Bangladeshi', 'Asian or Asian British'),
 ('L', '3', 'Any Other Asian Background', 'Asian or Asian British'),
 ('M', '4', 'Caribbean', 'Black or Black British'),
 ('N', '4', 'African', 'Black or Black British'),
 ('P', '4', 'Any Other Black Background', 'Black or Black British'),
 ('R', '5', 'Chinese', 'Other Ethnic Groups'), -- Chinese
 ('S', '5', 'Any Other Ethnic Group', 'Other Ethnic Groups'),
 ('Z', '6', 'Not Stated', 'Not Stated'),
 ('99', '7', 'Not Known', 'Not Known'),
 ('UNKNOWN', '8', 'UNKNOWN', 'UNKNOWN')

# COMMAND ----------

AgeCat = {
  "age_group_higher_level": ["Under 18", "18 and over"],
  "age_group_lower_common": ["Under 18", "18 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84",
                             "85 to 89","90 or over"],
   "age_group_lower_chap1": ["0 to 5", "6 to 10", "11 to 15", "16", "17", "18", "19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49", "50 to 54", "55 to 59", "60 to 64", "65 to 69",                                      "70 to 74", "75 to 79", "80 to 84", "85 to 89", "90 or over"],
   "age_group_lower_chap45": ["Under 14","14 to 15","16 to 17","18 to 19","20 to 24", "25 to 29","30 to 34","35 to 39","40 to 44","45 to 49", "50 to 54","55 to 59","60 to 64","65 to 69","70 to 74",
                                 "75 to 79","80 to 84","85 to 89","90 or over"],
   "age_group_lower_chap10": ["Under 14","14 to 15","16 to 17","18 to 19","20 to 24","25 to 29", "30 to 34","35 to 39","40 to 44","45 to 49","50 to 54", "55 to 59","60 to 64","65 or over"],
   "age_group_lower_chap10a": ["Under 14", "14 to 17", "18 to 19", "20 to 24","25 to 29","30 to 34","35 to 39","40 to 44","45 to 49","50 to 54", "55 to 59","60 to 64","65 to 69", "70 to 74", "75 to 79", "80 to 84", 
                               "85 to 89", "90 or over"],
   "age_group_lower_chap11": ["Under 15","15 to 19","20 to 24","25 to 29","30 to 34", "35 to 39","40 to 44","45 to 49","50 to 54","55 to 59", "60 to 64","65 or over"],
   "age_group_lower_imd": ["Under 18", "18 to 19","20 to 24","25 to 29","30 to 34", "35 to 39","40 to 44","45 to 49","50 to 54","55 to 59", "60 to 64","65 to 69","70 to 74","75 to 79","80 to 84", "85 or over"]
}
 
AgeCat1 = {key: {x1: x for x in AgeCat[key] for x1 in mapage(x)} for key in AgeCat}
AgeData = [[i] + [AgeCat1[key][i] if i in AgeCat1[key] else "NA" for key in AgeCat1] for i in range(125)]
 
lh = ["Age int"] + [f"{x} string" for x in AgeCat]
schema1 = ', '.join(lh)
df1 = spark.createDataFrame(AgeData, schema = schema1)
unknown_row = spark.createDataFrame([("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN")], df1.columns)
df1 = df1.union(unknown_row)
db_output = dbutils.widgets.get("db_output")
df1.write.insertInto(f"{db_output}.age_band_desc", overwrite=True)

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.imd_desc;
 INSERT INTO $db_output.imd_desc VALUES
 ("1", "01 Most deprived", "01 Most deprived"),
 ("2", "02 More deprived", "01 Most deprived"),
 ("3", "03 More deprived", "02"),
 ("4", "04 More deprived", "02"),
 ("5", "05 More deprived", "03"),
 ("6", "06 Less deprived", "03"),
 ("7", "07 Less deprived", "04"),
 ("8", "08 Less deprived", "04"),
 ("9", "09 Less deprived", "05 Least deprived"),
 ("10", "10 Least deprived", "05 Least deprived"),
 ("UNKNOWN", "UNKNOWN", "UNKNOWN")

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.TeamType;
 INSERT INTO $db_output.TeamType VALUES 
 ('A01','Day Care Service', 'Other mental health services'),
 ('A02','Crisis Resolution Team/Home Treatment Service', 'Crisis and acute mental health activity in community settings'),
 ('A05','Primary Care Mental Health Service', 'Core community mental health'),
 ('A06','Community Mental Health Team - Functional', 'Core community mental health'),
 ('A07','Community Mental Health Team - Organic', 'Other mental health services'),
 ('A08','Assertive Outreach Team', 'Core community mental health'),
 ('A09','Community Rehabilitation Service', 'Core community mental health'),
 ('A10','General Psychiatry Service', 'Other mental health services'),
 ('A11','Psychiatric Liaison Service', 'Mental health activity in general hospitals'),
 ('A12','Psychotherapy Service', 'Core community mental health'),
 ('A13','Psychological Therapy Service (non IAPT)', 'Core community mental health'),
 ('A14','Early Intervention Team for Psychosis', 'Early Intervention Team for Psychosis'),
 ('A15','Young Onset Dementia Team', 'Other mental health services'),
 ('A16','Personality Disorder Service', 'Core community mental health'),
 ('A17','Memory Services/Clinic/Drop in service', 'Other mental health services'),
 ('A18','Single Point of Access Service', 'Other mental health services'),
 ('A19','24/7 Crisis Response Line', '24/7 Crisis Response Line'),
 ('A20','Health Based Place Of Safety Service', 'Crisis and acute mental health activity in community settings'),
 ('A21','Crisis Café/Safe Haven/Sanctuary Service', 'Crisis and acute mental health activity in community settings'),
 ('A22','Walk-in Crisis Assessment Unit Service', 'Crisis and acute mental health activity in community settings'),
 ('A23','Psychiatric Decision Unit Service', 'Crisis and acute mental health activity in community settings'),
 ('A24','Acute Day Service', 'Crisis and acute mental health activity in community settings'),
 ('A25','Crisis House Service', 'Crisis and acute mental health activity in community settings'),
 ('B01','Forensic Mental Health Service', 'Forensic Services'),
 ('B02','Forensic Learning Disability Service', 'Forensic Services'),
 ('C01','Autism Service', 'Autism Service'),
 ('C02','Specialist Perinatal Mental Health Community Service', 'Specialist Perinatal Mental Health'),
 ('C04','Neurodevelopment Team', 'Specialist Services'),
 ('C05','Paediatric Liaison Service', 'Mental health activity in general hospitals'),
 ('C06','Looked After Children Service', 'Specialist Services'),
 ('C07','Youth Offending Service', 'Specialist Services'),
 ('C08','Acquired Brain Injury Service', 'Specialist Services'),
 ('C10','Community Eating Disorder Service', 'Core community mental health'),
 ('D01','Substance Misuse Team', 'Other mental health services'),
 ('D02','Criminal Justice Liaison and Diversion Service', 'Other mental health services'),
 ('D03','Prison Psychiatric Inreach Service', 'Other mental health services'),
 ('D04','Asylum Service', 'Other mental health services'),
 ('D05','Individual Placement and Support Service', 'Individual Placement and Support'),
 ('D06','Mental Health In Education Service', 'Other mental health services'),
 ('D07','Problem Gambling Service', 'Other mental health services'),
 ('D08','Rough Sleeping Service', 'Other mental health services'),
 ('E01','Community Team for Learning Disabilities', 'LDA'),
 ('E02','Epilepsy/Neurological Service', 'LDA'),
 ('E03','Specialist Parenting Service', 'LDA'),
 ('E04','Enhanced/Intensive Support Service', 'LDA'),
 ('F01','Mental Health Support Team', 'Other mental health services'),
 ('F02','Maternal Mental Health Service', 'Other mental health services'),
 ('F03','Mental Health Services for Deaf people', 'Other mental health services'),
 ('F04','Veterans Complex Treatment Service', 'Other mental health services'),
 ('F05','Enhanced care in care homes teams', 'Other mental health services'),
 ('F06','Mental Health and Wellbeing Hubs', 'Other mental health services'),
 ('Z01','Other Mental Health Service - in scope of National Tariff Payment System', 'Other'),
 ('Z02','Other Mental Health Service - out of scope of National Tariff Payment System', 'Other'),
 ('ZZZ', 'Other Mental Health Service', 'UNKNOWN'),
 ('UNKNOWN', 'UNKNOWN', 'UNKNOWN')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.attendance_code;
 INSERT INTO $db_output.attendance_code VALUES
 ('5', 'Attended on time or, if late, before the relevant professional was ready to see the patient'),
 ('6', 'Arrived late, after the relevant professional was ready to see the patient, but was seen'),
 ('7', 'Patient arrived late and could not be seen'),
 ('2', 'Appointment cancelled by, or on behalf of the patient'),
 ('3', 'Did not attend, no advance warning given'),
 ('4', 'Appointment cancelled or postponed by the health care provider'),
 ('UNKNOWN', 'UNKNOWN')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.RestrictiveIntTypeDim;
 INSERT INTO $db_output.RestrictiveIntTypeDim VALUES
 ('06','16','Segregation'),
 ('05', '15', 'Seclusion'),
 ('13', '14', 'Physical restraint - Other (not listed)'),
 ('09', '13', 'Physical restraint - Supine'),
 ('07', '12', 'Physical restraint - Standing'),
 ('10', '11', 'Physical restraint - Side'),
 ('11', '10', 'Physical restraint - Seated'),
 ('08', '9', 'Physical restraint - Restrictive escort'),
 ('12', '8', 'Physical restraint - Kneeling'),
 ('01', '7', 'Physical restraint - Prone'),
 ('04', '6', 'Mechanical restraint'),
 ('17', '5', 'Chemical restraint - Other (not listed)'),
 ('16', '4', 'Chemical restraint - Oral'),
 ('14', '3', 'Chemical restraint - Injection (Rapid Tranquillisation)'),
 ('15', '2', 'Chemical restraint - Injection (Non Rapid Tranquillisation)'),
 ('UNKNOWN', 'UNKNOWN', 'UNKNOWN')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.hosp_bed_desc;
 INSERT INTO $db_output.hosp_bed_desc VALUES
 ("10","Acute adult mental health care", 'Adult Acute'),
 ("11","Acute older adult mental health care (organic and functional)", 'Older Adult Acute'),
 ("35","Adult admitted patient continuing care", 'Invalid'),
 ("36","Adult community rehabilitation unit", 'Invalid'),
 ("13","Adult Eating Disorders", 'Adult Specialist'),
 ("17","Adult High dependency rehabilitation", 'Adult Specialist'),
 ("21","Adult High secure", 'Adult Specialist'),
 ("37","Adult highly specialist high dependency rehabilitation unit", 'Invalid'),
 ("15","Adult Learning Disabilities", 'Adult Specialist'),
 ("38","Adult longer term high dependency rehabilitation unit", 'Invalid'),
 ("19","Adult Low secure", 'Adult Specialist'),
 ("20","Adult Medium secure", 'Adult Specialist'),
 ("39","Adult mental health admitted patient services for the Deaf", 'Invalid'),
 ("22","Adult Neuro-psychiatry / Acquired Brain Injury", 'Adult Specialist'),
 ("40","Adult personality disorder", 'Invalid'),
 ("12","Adult Psychiatric Intensive Care Unit (acute mental health care)", 'Adult Acute'),
 ("30","Child and Young Person Learning Disabilities / Autism admitted patient", 'CYP Specialist'),
 ("31","Child and Young Person Low Secure Learning Disabilities", 'CYP Specialist'),
 ("27","Child and Young Person Low Secure Mental Illness", 'CYP Specialist'),
 ("32","Child and Young Person Medium Secure Learning Disabilities", 'CYP Specialist'),
 ("28","Child and Young Person Medium Secure Mental Illness", 'CYP Specialist'),
 ("34","Child and Young Person Psychiatric Intensive Care Unit", 'CYP Acute'),
 ("29","Child Mental Health admitted patient services for the Deaf", 'CYP Specialist'),
 ("26","Eating Disorders admitted patient - Child (12 years and under)", 'CYP Specialist'),
 ("25","Eating Disorders admitted patient - Young person (13 years and over)", 'CYP Specialist'),
 ("23","General child and young PERSON admitted PATIENT - Child (including High Dependency)", 'CYP Acute'),
 ("24","General child and young PERSON admitted PATIENT - Young PERSON (including High Dependency)", 'CYP Acute'),
 ("14","Mother and baby", 'Adult Specialist'),
 ("33","Severe Obsessive Compulsive Disorder and Body Dysmorphic Disorder - Young person", 'CYP Specialist'),
 ("UNKNOWN", "UNKNOWN", "Invalid")

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.prov_type_desc;
 INSERT INTO $db_output.prov_type_desc VALUES
 ("NHS", "NHS Providers"), 
 ("Non NHS", "Non NHS Providers"),
 ("UNKNOWN", "UNKNOWN")

# COMMAND ----------

 %md
 #### Geographic Reference Data

# COMMAND ----------

 %sql 
 INSERT OVERWRITE TABLE $db_output.mhb_org_daily
 SELECT ORG_CODE, ORG_TYPE_CODE, NAME, ORG_OPEN_DATE, ORG_CLOSE_DATE, BUSINESS_START_DATE, BUSINESS_END_DATE, ORG_IS_CURRENT
 FROM reference_data.org_daily
 WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
   AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)
   AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN');
   
 OPTIMIZE $db_output.mhb_org_daily;

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.MHB_RD_CCG_LATEST
 SELECT ORG_CODE, NAME
 FROM $db_output.MHB_ORG_DAILY
 WHERE (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
        AND ORG_TYPE_CODE = 'CC'
        AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
        AND ORG_OPEN_DATE <= '$rp_enddate'
        AND NAME NOT LIKE '%HUB'
        AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW ORG_DAILY AS
 SELECT DISTINCT ORG_CODE,
                 NAME,
                 ORG_TYPE_CODE,
                 ORG_OPEN_DATE, 
                 ORG_CLOSE_DATE, 
                 BUSINESS_START_DATE, 
                 BUSINESS_END_DATE
            FROM reference_data.org_daily
           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                 --AND ORG_TYPE_CODE = 'CC'
                 AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ISNULL(ORG_CLOSE_DATE))              
                 AND ORG_OPEN_DATE <= '$rp_enddate'
                 --AND NAME NOT LIKE '%HUB'
                 --AND NAME NOT LIKE '%NATIONAL%';

# COMMAND ----------

 %sql
 CREATE OR REPLACE TEMPORARY VIEW ORG_RELATIONSHIP_DAILY AS 
 SELECT 
 REL_TYPE_CODE,
 REL_FROM_ORG_CODE,
 REL_TO_ORG_CODE, 
 REL_OPEN_DATE,
 REL_CLOSE_DATE
 FROM 
 reference_data.ORG_RELATIONSHIP_DAILY
 WHERE
 (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
 AND REL_OPEN_DATE <= '$rp_enddate'

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.STP_Region_Mapping

 SELECT 
 C.ORG_CODE as CCG_CODE, 
 C.NAME as CCG_NAME,
 A.ORG_CODE as STP_CODE,
 A.NAME as STP_NAME, 
 E.ORG_CODE as REGION_CODE,
 E.NAME as REGION_NAME
 FROM 
 ORG_DAILY A
 LEFT JOIN ORG_RELATIONSHIP_DAILY B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
 LEFT JOIN ORG_DAILY C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
 LEFT JOIN ORG_RELATIONSHIP_DAILY D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
 LEFT JOIN ORG_DAILY E ON D.REL_TO_ORG_CODE = E.ORG_CODE
 WHERE
 --ORG_CODE = 'QF7'
 A.ORG_TYPE_CODE = 'ST'
 AND B.REL_TYPE_CODE is not null
 ORDER BY 1;

 INSERT INTO $db_output.STP_Region_Mapping VALUES
 ("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN")

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.mhsds_providers_in_year
 SELECT
 HDR.OrgIDProvider as OrgIDProv,
 ORG.NAME as Provider_Name					
 FROM $db_output.MHB_MHS000Header AS HDR
 LEFT OUTER JOIN (
                   SELECT DISTINCT ORG_CODE, 
                         NAME
                    FROM $db_output.MHB_ORG_DAILY
                   WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
                         AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)	
                         AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN')                 
                 ) AS ORG
     ON HDR.OrgIDProvider = ORG.ORG_CODE

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.LA_Hardcodes; 
 CREATE TABLE IF NOT EXISTS $db_output.LA_Hardcodes 
 (LADUA string,
 LADCD string,
 LADNM string);

 INSERT INTO $db_output.LA_Hardcodes VALUES
 ('LADUA', 'N', 'Northern Ireland'),
 ('LADUA', 'S', 'Scotland'),
 ('LADUA', 'W', 'Wales'),
 ('LADUA', 'M99999999', 'Isle of Man'),
 ('LADUA', 'L99999999', 'Channel Islands'),
 ('LADUA', 'UNKNOWN', 'UNKNOWN')

# COMMAND ----------

 %sql
 SELECT * FROM $db_output.LA_Hardcodes 

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.la
 SELECT DISTINCT 
 'LADUA' as type,
  GEOGRAPHY_CODE as level, 
  GEOGRAPHY_NAME as level_description
 FROM reference_data.ons_chd_geo_equivalents e
 WHERE e.GEOGRAPHY_CODE LIKE 'E%' 
 AND ENTITY_CODE IN ("E06", "E07", "E08", "E09") ---county councils removed as in MHSDS the more granular LA is assigned
 AND (DATE_OF_TERMINATION IS NULL OR DATE_OF_TERMINATION >= "$rp_enddate")
 AND DATE_OF_OPERATION <= "$rp_enddate"
             
 UNION ALL 

 SELECT 
 *
 FROM $db_output.LA_Hardcodes

# COMMAND ----------

 %sql
  ---hard-coded ccg mapping 2021 values
  TRUNCATE TABLE $db_output.ccg_mapping_2021;
  INSERT INTO $db_output.ccg_mapping_2021
  VALUES 
  ('00C','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00D','84H','E38000234','NHS COUNTY DURHAM CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00J','84H','E38000234','NHS COUNTY DURHAM CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00K','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00L','00L','E38000130','NHS NORTHUMBERLAND CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00M','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00N','00N','E38000163','NHS SOUTH TYNESIDE CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00P','00P','E38000176','NHS SUNDERLAND CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('00Q','00Q','E38000014','NHS BLACKBURN WITH DARWEN CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('00R','00R','E38000015','NHS BLACKPOOL CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('00T','00T','E38000016','NHS BOLTON CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('00V','00V','E38000024','NHS BURY CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('00X','00X','E38000034','NHS CHORLEY AND SOUTH RIBBLE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('00Y','00Y','E38000135','NHS OLDHAM CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('01A','01A','E38000050','NHS EAST LANCASHIRE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('01C','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('01D','01D','E38000080','NHS HEYWOOD, MIDDLETON AND ROCHDALE CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('01E','01E','E38000227','NHS GREATER PRESTON CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('01F','01F','E38000068','NHS HALTON CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('01G','01G','E38000143','NHS SALFORD CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('01H','01H','E38000215','NHS NORTH CUMBRIA CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('01J','01J','E38000091','NHS KNOWSLEY CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('01K','01K','E38000228','NHS MORECAMBE BAY CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('01R','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('01T','01T','E38000161','NHS SOUTH SEFTON CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('01V','01V','E38000170','NHS SOUTHPORT AND FORMBY CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('01W','01W','E38000174','NHS STOCKPORT CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('01X','01X','E38000172','NHS ST HELENS CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('01Y','01Y','E38000182','NHS TAMESIDE AND GLOSSOP CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('02A','02A','E38000187','NHS TRAFFORD CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('02D','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('02E','02E','E38000194','NHS WARRINGTON CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('02F','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('02G','02G','E38000200','NHS WEST LANCASHIRE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('02H','02H','E38000205','NHS WIGAN BOROUGH CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('02M','02M','E38000226','NHS FYLDE AND WYRE CCG','E54000048','QE1','Healthier Lancashire and South Cumbria','E40000010','Y62','North West'),
  ('02N','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('02P','02P','E38000006','NHS BARNSLEY CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
  ('02Q','02Q','E38000008','NHS BASSETLAW CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
  ('02R','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('02T','02T','E38000025','NHS CALDERDALE CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('02W','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('02X','02X','E38000044','NHS DONCASTER CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
  ('02Y','02Y','E38000052','NHS EAST RIDING OF YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03A','X2C4Y','E38000254','NHS KIRKLEES CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('03D','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03E','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03F','03F','E38000085','NHS HULL CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03H','03H','E38000119','NHS NORTH EAST LINCOLNSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03J','X2C4Y','E38000254','NHS KIRKLEES CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('03K','03K','E38000122','NHS NORTH LINCOLNSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03L','03L','E38000141','NHS ROTHERHAM CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
  ('03M','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03N','03N','E38000146','NHS SHEFFIELD CCG','E54000009','QF7','South Yorkshire and Bassetlaw','E40000009','Y63','North East and Yorkshire'),
  ('03Q','03Q','E38000188','NHS VALE OF YORK CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('03R','03R','E38000190','NHS WAKEFIELD CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('03T','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
  ('03V','78H','E38000242','NHS NORTHAMPTONSHIRE CCG','E54000020','QPM','Northamptonshire','E40000008','Y60','Midlands'),
  ('03W','03W','E38000051','NHS EAST LEICESTERSHIRE AND RUTLAND CCG','E54000015','QK1','Leicester, Leicestershire and Rutland','E40000008','Y60','Midlands'),
  ('03X','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
  ('03Y','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
  ('04C','04C','E38000097','NHS LEICESTER CITY CCG','E54000015','QK1','Leicester, Leicestershire and Rutland','E40000008','Y60','Midlands'),
  ('04D','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
  ('04E','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
  ('04F','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
  ('04G','78H','E38000242','NHS NORTHAMPTONSHIRE CCG','E54000020','QPM','Northamptonshire','E40000008','Y60','Midlands'),
  ('04H','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
  ('04J','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
  ('04K','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
  ('04L','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
  ('04M','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
  ('04N','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
  ('04Q','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
  ('04R','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
  ('04V','04V','E38000201','NHS WEST LEICESTERSHIRE CCG','E54000015','QK1','Leicester, Leicestershire and Rutland','E40000008','Y60','Midlands'),
  ('04Y','04Y','E38000028','NHS CANNOCK CHASE CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
  ('05A','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
  ('05C','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
  ('05D','05D','E38000053','NHS EAST STAFFORDSHIRE CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
  ('05F','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
  ('05G','05G','E38000126','NHS NORTH STAFFORDSHIRE CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
  ('05H','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
  ('05J','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
  ('05L','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
  ('05N','M2L0M','E38000257','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','E54000011','QOC','Shropshire and Telford and Wrekin','E40000008','Y60','Midlands'),
  ('05Q','05Q','E38000153','NHS SOUTH EAST STAFFORDSHIRE AND SEISDON PENINSULA CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
  ('05R','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
  ('05T','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
  ('05V','05V','E38000173','NHS STAFFORD AND SURROUNDS CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
  ('05W','05W','E38000175','NHS STOKE ON TRENT CCG','E54000010','QNC','Staffordshire and Stoke on Trent','E40000008','Y60','Midlands'),
  ('05X','M2L0M','E38000257','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','E54000011','QOC','Shropshire and Telford and Wrekin','E40000008','Y60','Midlands'),
  ('05Y','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
  ('06A','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
  ('06D','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
  ('06F','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
  ('06H','06H','E38000026','NHS CAMBRIDGESHIRE AND PETERBOROUGH CCG','E54000021','QUE','Cambridgeshire and Peterborough','E40000007','Y61','East of England'),
  ('06K','06K','E38000049','NHS EAST AND NORTH HERTFORDSHIRE CCG','E54000025','QM7','Hertfordshire and West Essex','E40000007','Y61','East of England'),
  ('06L','06L','E38000086','NHS IPSWICH AND EAST SUFFOLK CCG','E54000023','QJG','Suffolk and North East Essex','E40000007','Y61','East of England'),
  ('06M','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
  ('06N','06N','E38000079','NHS HERTS VALLEY CCG','E54000025','QM7','Hertfordshire and West Essex','E40000007','Y61','East of England'),
  ('06P','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
  ('06Q','06Q','E38000106','NHS MID ESSEX CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
  ('06T','06T','E38000117','NHS NORTH EAST ESSEX CCG','E54000023','QJG','Suffolk and North East Essex','E40000007','Y61','East of England'),
  ('06V','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
  ('06W','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
  ('06Y','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
  ('07G','07G','E38000185','NHS THURROCK CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
  ('07H','07H','E38000197','NHS WEST ESSEX CCG','E54000025','QM7','Hertfordshire and West Essex','E40000007','Y61','East of England'),
  ('07J','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
  ('07K','07K','E38000204','NHS WEST SUFFOLK CCG','E54000023','QJG','Suffolk and North East Essex','E40000007','Y61','East of England'),
  ('07L','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('07M','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
  ('07N','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
  ('07P','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('07Q','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
  ('07R','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
  ('07T','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('07V','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
  ('07W','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('07X','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
  ('07Y','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('08A','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
  ('08C','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('08D','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
  ('08E','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('08F','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('08G','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('08H','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
  ('08J','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
  ('08K','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
  ('08L','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
  ('08M','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('08N','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('08P','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
  ('08Q','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
  ('08R','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
  ('08T','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
  ('08V','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('08W','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('08X','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
  ('08Y','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('09A','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('09C','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('09D','09D','E38000021','NHS BRIGHTON AND HOVE CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('09E','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('09F','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('09G','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('09H','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('09J','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('09L','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
  ('09N','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
  ('09P','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('09W','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('09X','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('09Y','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
  ('10A','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('10C','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
  ('10D','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('10E','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('10J','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('10K','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('10L','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('10Q','10Q','E38000136','NHS OXFORDSHIRE CCG','E54000044','QU9','Buckinghamshire, Oxfordshire and Berkshire West','E40000005','Y59','South East'),
  ('10R','10R','E38000137','NHS PORTSMOUTH CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('10V','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('10X','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('11A','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('11E','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
  ('11J','11J','E38000045','NHS DORSET CCG','E54000041','QVV','Dorset','E40000006','Y58','South West'),
  ('11M','11M','E38000062','NHS GLOUCESTERSHIRE CCG','E54000043','QR1','Gloucestershire','E40000006','Y58','South West'),
  ('11N','11N','E38000089','NHS KERNOW CCG','E54000036','QT6','Cornwall and the Isles of Scilly Health and Social Care Partnership','E40000006','Y58','South West'),
  ('11X','11X','E38000150','NHS SOMERSET CCG','E54000038','QSL','Somerset','E40000006','Y58','South West'),
  ('12D','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
  ('12F','12F','E38000208','NHS WIRRAL CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('13T','13T','E38000212','NHS NEWCASTLE GATESHEAD CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('14L','14L','E38000217','NHS MANCHESTER CCG','E54000007','QOP','Greater Manchester Health and Social Care Partnership','E40000010','Y62','North West'),
  ('14Y','14Y','E38000223','NHS BUCKINGHAMSHIRE CCG','E54000044','QU9','Buckinghamshire, Oxfordshire and Berkshire West','E40000005','Y59','South East'),
  ('15A','15A','E38000221','NHS BERKSHIRE WEST CCG','E54000044','QU9','Buckinghamshire, Oxfordshire and Berkshire West','E40000005','Y59','South East'),
  ('15C','15C','E38000222','NHS BRISTOL, NORTH SOMERSET AND SOUTH GLOUCESTERSHIRE CCG','E54000039','QUY','Bristol, North Somerset and South Gloucestershire','E40000006','Y58','South West'),
  ('15D','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
  ('15E','15E','E38000220','NHS BIRMINGHAM AND SOLIHULL CCG','E54000017','QHL','Birmingham and Solihull','E40000008','Y60','Midlands'),
  ('15F','15F','E38000225','NHS LEEDS CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('15M','15M','E38000229','NHS DERBY AND DERBYSHIRE CCG','E54000012','QJ2','Joined Up Care Derbyshire','E40000008','Y60','Midlands'),
  ('15N','15N','E38000230','NHS DEVON CCG','E54000037','QJK','Devon','E40000006','Y58','South West'),
  ('16C','16C','E38000247','NHS TEES VALLEY CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('18C','18C','E38000236','NHS HEREFORDSHIRE CCG','E54000019','QGH','Herefordshire and Worcestershire','E40000008','Y60','Midlands'),
  ('26A','26A','E38000239','NHS NORFOLK AND WAVENEY CCG','E54000022','QMM','Norfolk and Waveney Health and Care Partnership','E40000007','Y61','East of England'),
  ('27D','27D','E38000233','NHS CHESHIRE CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('36J','36J','E38000232','NHS BRADFORD DISTRICT AND CRAVEN CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire'),
  ('36L','36L','E38000245','NHS SOUTH WEST LONDON CCG','E54000031','QWE','South West London Health and Care Partnership','E40000003','Y56','London'),
  ('42D','42D','E38000241','NHS NORTH YORKSHIRE CCG','E54000051','QOQ','Humber, Coast and Vale','E40000009','Y63','North East and Yorkshire'),
  ('52R','52R','E38000243','NHS NOTTINGHAM AND NOTTINGHAMSHIRE CCG','E54000014','QT1','Nottingham and Nottinghamshire Health and Care','E40000008','Y60','Midlands'),
  ('70F','70F','E38000248','NHS WEST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('71E','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
  ('72Q','72Q','E38000244','NHS SOUTH EAST LONDON CCG','E54000030','QKK','Our Healthier South East London','E40000003','Y56','London'),
  ('78H','78H','E38000242','NHS NORTHAMPTONSHIRE CCG','E54000020','QPM','Northamptonshire','E40000008','Y60','Midlands'),
  ('84H','84H','E38000234','NHS COUNTY DURHAM CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('91Q','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('92A','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
  ('92G','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
  ('93C','93C','E38000240','NHS NORTH CENTRAL LONDON CCG','E54000028','QMJ','North London Partners in Health and Care','E40000003','Y56','London'),
  ('97R','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('99A','99A','E38000101','NHS LIVERPOOL CCG','E54000008','QYG','Cheshire and Merseyside','E40000010','Y62','North West'),
  ('99C','99C','E38000127','NHS NORTH TYNESIDE CCG','E54000050','QHM','Cumbria and North East','E40000009','Y63','North East and Yorkshire'),
  ('99D','71E','E38000238','NHS LINCOLNSHIRE CCG','E54000013','QJM','Lincolnshire','E40000008','Y60','Midlands'),
  ('99E','99E','E38000007','NHS BASILDON AND BRENTWOOD CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
  ('99F','99F','E38000030','NHS CASTLE POINT AND ROCHFORD CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
  ('99G','99G','E38000168','NHS SOUTHEND CCG','E54000026','QH8','Mid and South Essex','E40000007','Y61','East of England'),
  ('99H','92A','E38000246','NHS SURREY HEARTLANDS CCG','E54000052','QXU','Surrey Heartlands Health and Care Partnership','E40000005','Y59','South East'),
  ('99J','91Q','E38000237','NHS KENT AND MEDWAY CCG','E54000032','QKS','Kent and Medway','E40000005','Y59','South East'),
  ('99K','97R','E38000235','NHS EAST SUSSEX CCG','E54000053','QNX','Sussex Health and Care Partnership','E40000005','Y59','South East'),
  ('99M','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
  ('99N','92G','E38000231','NHS BATH AND NORTH EAST SOMERSET, SWINDON AND WILTSHIRE CCG','E54000040','QOX','Bath and North East Somerset, Swindon and Wiltshire','E40000006','Y58','South West'),
  ('99P','15N','E38000230','NHS DEVON CCG','E54000037','QJK','Devon','E40000006','Y58','South West'),
  ('99Q','15N','E38000230','NHS DEVON CCG','E54000037','QJK','Devon','E40000006','Y58','South West'),
  ('A3A8R','A3A8R','E38000255','NHS NORTH EAST LONDON CCG','E54000029','QMF','East London Health and Care Partnership','E40000003','Y56','London'),
  ('B2M3M','B2M3M','E38000251','NHS COVENTRY AND WARWICKSHIRE CCG','E54000018','QWU','Coventry and Warwickshire','E40000008','Y60','Midlands'),
  ('D2P2L','D2P2L','E38000250','NHS BLACK COUNTRY AND WEST BIRMINGHAM CCG','E54000016','QUA','The Black Country and West Birmingham','E40000008','Y60','Midlands'),
  ('D4U1Y','D4U1Y','E38000252','NHS FRIMLEY CCG','E54000034','QNQ','Frimley Health and Care ICS','E40000005','Y59','South East'),
  ('D9Y0V','D9Y0V','E38000253','NHS HAMPSHIRE, SOUTHAMPTON AND ISLE OF WIGHT CCG','E54000042','QRL','Hampshire and the Isle of Wight','E40000005','Y59','South East'),
  ('M1J4Y','M1J4Y','E38000249','NHS BEDFORDSHIRE, LUTON AND MILTON KEYNES CCG','E54000024','QHG','Bedfordshire, Luton and Milton Keynes','E40000007','Y61','East of England'),
  ('M2L0M','M2L0M','E38000257','NHS SHROPSHIRE, TELFORD AND WREKIN CCG','E54000011','QOC','Shropshire and Telford and Wrekin','E40000008','Y60','Midlands'),
  ('Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown','Unknown'),
  ('W2U3Z','W2U3Z','E38000256','NHS NORTH WEST LONDON CCG','E54000027','QRV','North West London Health and Care Partnership','E40000003','Y56','London'),
  ('X2C4Y','X2C4Y','E38000254','NHS KIRKLEES CCG','E54000054','QWO','West Yorkshire and Harrogate (Health and Care Partnership)','E40000009','Y63','North East and Yorkshire')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.CCG_MAPPING_2021_v2;
 CREATE TABLE IF NOT EXISTS $db_output.CCG_MAPPING_2021_v2 AS
  ---ccg mapping 2021 table (needed for community access and admissions metrics)
 SELECT
 DISTINCT
  CCG_UNMAPPED, 
  CCG21CDH, 
  CCG21CD, 
  coalesce(CCG_NAME, "UNKNOWN") as CCG21NM, 
  STP21CD, 
  coalesce(STP_CODE, "UNKNOWN") AS STP21CDH, 
  coalesce(STP_NAME, "UNKNOWN") as STP21NM, 
  NHSER21CD, 
  coalesce(REGION_CODE, "UNKNOWN") as NHSER21CDH, 
  coalesce(REGION_NAME, "UNKNOWN") as NHSER21NM
 FROM $db_output.CCG_MAPPING_2021 c
 LEFT JOIN $db_output.STP_Region_Mapping s on c.CCG21CDH = s.CCG_Code

# COMMAND ----------

 %md
 #### MHSDS Reference Data

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.mh_ass
 VALUES
 ('PROM','Brief Parental Self Efficacy Scale (BPSES)','Brief Parental Self-Efficacy Scale score','961031000000108','v2','5','25','','',''),
 ('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure)','Clinical Outcomes in Routine Evaluation Outcome Measure Functioning score (observable entity)','718490000','v1','0','48','','',''),
 ('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure)','Clinical Outcomes in Routine Evaluation Outcome Measure Global Distress score (observable entity)','718612000','v1','0','40','','',''),
 ('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure)','Clinical Outcomes in Routine Evaluation Outcome Measure Problems symptom score (observable entity)','718613005','v1','0','48','','',''),
 ('PROM','CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure)','Clinical Outcomes in Routine Evaluation Outcome Measure Well being score (observable entity)','718492008','v1','0','16','','',''),
 ('PREM','Child Group Session Rating Score (CGSRS)','Child Group Session Rating Scale score','718431003','v2','0','40','','',''),
 ('PROM','Child Outcome Rating Scale (CORS)','Child Outcome Rating Scale total score','718458005','v2','0','40','Y','','Self'),
 ('PREM','Child Session Rating Scale (CSRS)','Child Session Rating Scale score','718762000','v2','0','40','','',''),
 ('PROM','Childrens Revised Impact of Event Scale (8) (CRIES 8)','Revised Child Impact of Events Scale score','718152002','v2','0','40','','',''),
 ('CROM','Childrens Global Assessment Scale (CGAS)','Childrens global assessment scale score','860591000000104','v2','1','100','Y','','Clinician'),
 ('PROM','Clinical Outcomes in Routine Evaluation 10 (CORE 10)','Clinical Outcomes in Routine Evaluation - 10 clinical score','718583008','v2','0','40','Y','','Self'),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 1 score - I feel that the people who have seen my child listened to me','1035681000000101','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 2 score - it was easy to talk to the people who have seen my child','1035711000000102','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 3 score - I was treated well by the people who have seen my child','1035721000000108','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 4 score - my views and worries were taken seriously','1035731000000105','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 5 score - I feel the people here know how to help with the problem I came for','1035741000000101','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 6 score - I have been given enough explanation about the help available here','1035761000000100','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 7 score - I feel that the people who have seen my child are working together to help with the problem(s)','1035771000000107','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 8 score - the facilities here are comfortable (e.g. waiting area)','1035781000000109','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 9 score - the appointments are usually at a convenient time (e.g. do not interfere with work, school)','1035791000000106','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 10 score - it is quite easy to get to the place where the appointments are','1035801000000105','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 11 score - if a friend needed similar help, I would recommend that he or she come here','1035811000000107','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated','Commission for Health Improvement Experience of Service Questionnaire Parent or Carer item 12 score - overall, the help I have received here is good','1035821000000101','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 1 score - I feel that the people who saw me listened to me','1035861000000109','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 2 score - it was easy to talk to the people who saw me','1035871000000102','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 3 score - I was treated well by the people who saw me','1035881000000100','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 4 score - my views and worries were taken seriously','1035891000000103','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 5 score - I feel the people here know how to help me','1035901000000102','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 6 score - I have been given enough explanation about the help available here','1035911000000100','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 7 score - I feel that the people who have seen me are working together to help me','1035921000000106','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 8 score - the facilities here are comfortable (e.g. waiting area)','1035931000000108','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 9 score - my appointments are usually at a convenient time (e.g. do not interfere with school, clubs, college, work)','1035941000000104','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 10 score - it is quite easy to get to the place where I have my appointments','1035951000000101','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 11 score - if a friend needed this sort of help, I would suggest to them to come here','1035961000000103','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 12-18 year olds item 12 score - overall, the help I have received here is good','1035971000000105','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 1 score - did the people who saw you listen to you?','1035981000000107','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 2 score - was it easy to talk to the people who saw you?','1035991000000109','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 3 score - how were you treated by the people who saw you?','1036001000000108','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 4 score - were your views and worries taken seriously?','1036011000000105','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 5 score - do you feel that the people here know how to help you?','1036021000000104','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 6 score - were you given enough explanation about the help available here?','1036031000000102','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 7 score - do you feel that the people here are working together to help you?','1036041000000106','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 8 score - the facilities here (like the waiting area) are','1036061000000107','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 9 score - the time of my appointments was','1036051000000109','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 10 score - the place where I had my appointments was','1036071000000100','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 11 score - if a friend needed this sort of help, do you think they should come here?','1036081000000103','v2','1','3','','',''),
 ('PREM','Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs','Commission for Health Improvement Experience of Service Questionnaire self-report for 9-11 year olds item 12 score - has the help you got here been good?','1036091000000101','v2','1','3','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech frequency and duration score','718889002','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech global rating scale score','718918004','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech level of distress score','718894002','v2','0','100','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas frequency and duration score','718885008','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas global rating scale score','718892003','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas level of distress score','718915001','v2','0','100','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities frequency and duration score','718891005','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities global rating scale score','718917009','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities level of distress score','718886009','v2','0','100','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content frequency and duration score','718884007','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content global rating scale score','718888005','v2','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content level of distress score','718887000','v2','0','100','','',''),
 ('CROM','Current View','Current View Contextual Problems score - community','987191000000101','v2','0','3','','',''),
 ('CROM','Current View','Current View Contextual Problems score - home','987201000000104','v2','0','3','','',''),
 ('CROM','Current View','Current View Contextual Problems score - school, work or training','987211000000102','v2','0','3','','',''),
 ('CROM','Current View','Current View Contextual Problems score - service engagement','987221000000108','v2','0','3','','',''),
 ('CROM','Current View','Current View Education,Employment,Training score - attainment difficulties','987231000000105','v2','0','3','','',''),
 ('CROM','Current View','Current View Education,Employment,Training score - attendance difficulties','987241000000101','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 1 score - anxious away from caregivers','987251000000103','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 2 score - anxious in social situations','987261000000100','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 3 score - anxious generally','987271000000107','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 4 score - compelled to do or think things','987281000000109','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 5 score - panics','987291000000106','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 6 score - avoids going out','987301000000105','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 7 score - avoids specific things','987311000000107','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 8 score - repetitive problematic behaviours','987321000000101','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 9 score - depression/low mood','987331000000104','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 10 score - self-harm','987341000000108','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 11 score - extremes of mood','987351000000106','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 12 score - delusional beliefs and hallucinations','987361000000109','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 13 score - drug and alcohol difficulties','987371000000102','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 14 score - difficulties sitting still or concentrating','987381000000100','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 15 score - behavioural difficulties','987391000000103','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 16 score - poses risk to others','987401000000100','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 17 score - carer management of CYP (child or young person) behaviour','987411000000103','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 18 score - doesnt get to toilet in time','987421000000109','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 19 score - disturbed by traumatic event','987431000000106','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 20 score - eating issues','987441000000102','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 21 score - family relationship difficulties','987451000000104','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 22 score - problems in attachment to parent or carer','987461000000101','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 23 score - peer relationship difficulties','987471000000108','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 24 score - persistent difficulties managing relationships with others','987481000000105','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 25 score - does not speak','987491000000107','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 26 score - gender discomfort issues','987501000000101','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 27 score - unexplained physical symptoms','987511000000104','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 28 score - unexplained developmental difficulties','987521000000105','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 29 score - self-care issues','987531000000107','v2','0','3','','',''),
 ('CROM','Current View','Current View Provisional Problem Description item 30 score - adjustment to health issues','987541000000103','v2','0','3','','',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 1 score - how satisfied are you with your mental health','1037651000000100','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 2 score - how satisfied are you with your physical health','1037661000000102','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 3 score - how satisfied are you with your job situation','1037671000000109','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 4 score - how satisfied are you with your accommodation','1037681000000106','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 5 score - how satisfied are you with your leisure activities','1037691000000108','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 6 score - how satisfied are you with your friendships','1037701000000108','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 7 score - how satisfied are you with your partner/family','1037711000000105','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 8 score - how satisfied are you with your personal safety','1037721000000104','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 9 score - how satisfied are you with your medication','1037731000000102','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 10 score - how satisfied are you with the practical help you receive','1037741000000106','v2','0','8','','Y',''),
 ('PROM','DIALOG','DIALOG patient rated outcome measure item 11 score - how satisfied are you with consultations with mental health professionals','1037751000000109','v2','0','8','','Y',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - eating concern subscale score','959601000000103','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - global score','959611000000101','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - restraint subscale score','959621000000107','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - shape concern subscale score','959631000000109','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents','Eating Disorder Examination Questionnaire - Adolescents, 14-16 years - weight concern subscale score','959641000000100','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire eating concern subscale score','473345001','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire global score','446826001','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire restraint subscale score','473348004','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire shape concern subscale score','473346000','v2','0','6','','',''),
 ('PROM','Eating Disorder Examination Questionnaire (EDE-Q)','Eating disorder examination questionnaire weight concern subscale score','473347009','v2','0','6','','',''),
 ('PROM','Genralised Anxiety Disorder 7 (GAD-7)','Generalized anxiety disorder 7 item score','445455005','v2','0','21','Y','','Self'),
 ('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal score','959951000000108','v2','0','10','','',''),
 ('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal 1 score','1034351000000101','v2','0','10','Y','','Goals'),
 ('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal 2 score','1034361000000103','v2','0','10','Y','','Goals'),
 ('PROM','Goal Based Outcomes (GBO)','Goal Progress Chart - Child/Young Person - goal 3 score','1034371000000105','v2','0','10','Y','','Goals'),
 ('PROM','Goal Based Outcomes (GBO)','Goal-Based Outcomes tool goal progress chart - goal 1 score','1638531000000102','v2','0','10','Y','','Goals'), ---added AT BITC-5078
 ('PROM','Goal Based Outcomes (GBO)','Goal-Based Outcomes tool goal progress chart - goal 2 score','1638541000000106','v2','0','10','Y','','Goals'), ---added AT BITC-5078
 ('PROM','Goal Based Outcomes (GBO)','Goal-Based Outcomes tool goal progress chart - goal 3 score','1638551000000109','v2','0','10','Y','','Goals'), ---added AT BITC-5078
 ('PREM','Group Session Rating Scale (GSRS)','Group Session Rating Scale score','718441000','v2','0','40','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 1 score - active disturbance of social behaviour (observable entity)','1052961000000108','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 2 score - self directed injury (observable entity)','1052971000000101','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 3 score - problem drinking or drug use (observable entity)','1052981000000104','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 4 score - cognitive problems (observable entity)','1052991000000102','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 5 score - physical illness or disability problems (observable entity)','1053001000000103','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 6 score - problems associated with hallucinations or delusions or confabulations (observable entity)','1053011000000101','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 7 score - problems with depressive symptoms (observable entity)','1053021000000107','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 8 score - other mental and behavioural problems (observable entity)','1053031000000109','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 9 score - problems with relationships (observable entity)','1053041000000100','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 10 score - problems with activities of daily living (observable entity)','1053051000000102','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 11 score - problems with living conditions (observable entity)','1053061000000104','v2','0','4','','',''),
 ('CROM','HoNOS-ABI','Health of the Nation Outcome Scales for Acquired Brain Injury rating scale 12 score - problems with activities (observable entity)','1053071000000106','v2','0','4','','',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 1 score - overactive, aggressive, disruptive or agitated behaviour','979641000000103','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 2 score - non-accidental self-injury','979651000000100','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 3 score - problem drinking or drug-taking','979661000000102','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 4 score - cognitive problems','979671000000109','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 5 score - physical illness or disability problems','979681000000106','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 6 score - problems associated with hallucinations and delusions','979691000000108','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 7 score - problems with depressed mood','979701000000108','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 8 score - other mental and behavioural problems','979711000000105','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 9 score - problems with relationships','979721000000104','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 10 score - problems with activities of daily living','979731000000102','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 11 score - problems with living conditions','979741000000106','v2','0','4','','Y',''),
 ('CROM','HoNOS Working Age Adults','HoNOS (Health of the Nation Outcome Scales) for working age adults rating scale 12 score - problems with occupation and activities','979751000000109','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 1 score - behavioural disturbance','980761000000107','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 2 score - non-accidental self-injury','980771000000100','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 3 score - problem-drinking or drug-use','980781000000103','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 4 score - cognitive problems','980791000000101','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 5 score - problems related to physical illness/disability','980801000000102','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 6 score - problems associated with hallucinations and/or delusions (or false beliefs)','980811000000100','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 7 score - problems with depressive symptoms','980821000000106','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 8 score - other mental and behavioural problems','980831000000108','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 9 score - problems with social or supportive relationships','980841000000104','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 10 score - problems with activities of daily living','980851000000101','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 11 score - overall problems with living conditions','980861000000103','v2','0','4','','Y',''),
 ('CROM','HoNOS 65+ (Older Persons)','HoNOS 65+ (Health of the Nation Outcome Scales 65+) rating scale 12 score - problems with work and leisure activities - quality of daytime environment','980871000000105','v2','0','4','','Y',''),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 1 score - disruptive, antisocial or aggressive behaviour','989881000000104','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 2 score - overactivity, attention and concentration','989931000000107','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 3 score - non-accidental self injury','989941000000103','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 4 score - alcohol, substance/solvent misuse','989951000000100','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 5 score - scholastic or language skills','989961000000102','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 6 score - physical illness or disability problems','989971000000109','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 7 score - hallucinations and delusions','989981000000106','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 8 score - non-organic somatic symptoms','989991000000108','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 9 score - emotional and related symptoms','990001000000107','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 10 score - peer relationships','989891000000102','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 11 score - self care and independence','989901000000101','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 12 score - family life and relationships','989911000000104','v2','0','4','Y','Y','Parent'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Parent rated','HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 13 score - poor school attendance','989921000000105','v2','0','4','Y','Y','Parent'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 1 score - disruptive, antisocial or aggressive behaviour','989751000000102','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 2 score - overactivity, attention and concentration','989801000000109','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 3 score - non-accidental self injury','989811000000106','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 4 score - alcohol, substance/solvent misuse','989821000000100','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 5 score - scholastic or language skills','989831000000103','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 6 score - physical illness or disability problems','989841000000107','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 7 score - hallucinations and delusions','989851000000105','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 8 score - non-organic somatic symptoms','989861000000108','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 9 score - emotional and related symptoms','989871000000101','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 10 score - peer relationships','989761000000104','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 11 score - self care and independence','989771000000106','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 12 score - family life and relationships','989781000000108','v2','0','4','Y','Y','Clinician'),
 ('CROM','HoNOS-CA (Child and Adolescent) - Clinician rated','HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 13 score - poor school attendance','989791000000105','v2','0','4','Y','Y','Clinician'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 1 score - disruptive, antisocial or aggressive behaviour','989621000000101','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 2 score - overactivity, attention and concentration','989671000000102','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 3 score - non-accidental self injury','989681000000100','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 4 score - alcohol, substance/solvent misuse','989691000000103','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 5 score - scholastic or language skills','989701000000103','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 6 score - physical illness or disability problems','989711000000101','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 7 score - hallucinations and delusions','989721000000107','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 8 score - non-organic somatic symptoms','989731000000109','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 9 score - emotional and related symptoms','989741000000100','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 10 score - peer relationships','989631000000104','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 11 score - self care and independence','989641000000108','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 12 score - family life and relationships','989651000000106','v2','0','4','Y','Y','Self'),
 ('PROM','HoNOS-CA (Child and Adolescent) - Self rated','HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 13 score - poor school attendance','989661000000109','v2','0','4','Y','Y','Self'),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 1 score - behavioural problems (directed at others)','987711000000106','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 2 score - behavioural problems directed towards self (self-injury)','987811000000101','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3A score - behaviour destructive to property','988261000000101','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3B score - problems with personal behaviours','988271000000108','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3D score - anxiety, phobias, obsessive or compulsive behaviour','988291000000107','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 3E score - others','988301000000106','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 4 score - attention and concentration','987831000000109','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 5 score - memory and orientation','987841000000100','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 6 score - communication (problems with understanding)','987851000000102','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 7 score - communication (problems with expression)','987861000000104','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 8 score - problems associated with hallucinations and delusions','987871000000106','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 9 score - problems associated with mood changes','987881000000108','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 10 score - problems with sleeping','987721000000100','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 11 score - problems with eating and drinking','987731000000103','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 12 score - physical problems','987741000000107','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 13 score - seizures','987751000000105','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 14 score - activities of daily living at home','987761000000108','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 15 score - activities of daily living outside the home','987771000000101','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 16 score - level of self-care','987781000000104','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 17 score - problems with relationships','987791000000102','v2','0','4','','',''),
 ('CROM','HoNOS-LD (Learning Disabilities)','HoNOS-LD (Health of the Nation Outcome Scales for People with Learning Disabilities) rating scale 18 score - occupation and activities','987801000000103','v2','0','4','','',''),
 ('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale A score - risk of harm to adults or children','981391000000108','v2','0','4','','',''),
 ('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale B score - risk of self-harm (deliberate or accidental)','981401000000106','v2','0','4','','',''),
 ('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale C score - need for buildings security to prevent escape','981411000000108','v2','0','4','','',''),
 ('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale D score - need for safely staffed living environment','981421000000102','v2','0','4','','',''),
 ('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale E score - need for escort on leave (beyond secure perimeter)','981431000000100','v2','0','4','','',''),
 ('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale F score - risk to individual from others','981441000000109','v2','0','4','','',''),
 ('CROM','HoNOS-Secure','HoNOS-secure (Health of the Nation Outcome Scales-secure) rating scale G score - need for risk management procedures','981451000000107','v2','0','4','','',''),
 ('PROM ','ODD (Parent)','How Are Things? Behavioural Difficulties (Oppositional Defiant Disorder) - Parent/Carer score','961231000000104','v2','0','8','','',''),
 ('PROM','Kessler Psychological Distress Scale 10','Kessler Psychological Distress Scale 10 score','720211004','v2','0','40','','',''),
 ('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire behavioural difficulties score','718459002','v2','0','12','','',''),
 ('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire score','718562002','v2','0','32','','',''),
 ('PROM','Me and My Feelings Questionnaire','Me and My Feelings Questionnaire behavioural subscale score','1047101000000106','v2','0','12','','',''),
 ('PROM','Me and My Feelings Questionnaire','Me and My Feelings Questionnaire emotional subscale score','1047091000000103','v2','0','20','','',''),
 ('PROM','Outcome Rating Scale (ORS)','Outcome Rating Scale total score','716613004','v2','0','40','Y','','Self'),
 ('PROM','Patient Health Questionnaire (PHQ-9)','Patient health questionnaire 9 score','720433000','v2','0','27','Y','','Self'),
 ('PROM','Questionnaire about the Process of Recovery (QPR)','Questionnaire about the Process of Recovery total score','1035441000000106','v2','0','60','','Y',''),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score','718655001','v2','0','30','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score','718656000','v2','0','18','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score','718657009','v2','0','18','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score','718658004','v2','0','27','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score','718665007','v2','0','30','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score','718666008','v2','0','18','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score','718667004','v2','0','18','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score','718668009','v2','0','27','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score','718669001','v2','0','21','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score','718670000','v2','0','27','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety and Depression score','718672008','v2','0','141','','',''),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety score','718671001','v2','0','111','','',''),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score','718659007','v2','0','21','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score','718660002','v2','0','27','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety and Depression score','718662005','v2','0','141','','',''),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety score','718661003','v2','0','111','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - average total score','720197006','v2','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability average score','718434006','v2','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability total score','720200007','v2','5','25','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties average score','718456009','v2','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties total score','720196002','v2','5','25','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication average score','721955008','v2','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication total score','720594007','v2','5','25','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - total score','718455008','v2','15','75','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - conduct problems - educator score','986241000000103','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - conduct problems - parent score','986311000000109','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - emotional symptoms - educator score','986251000000100','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - emotional symptoms - parent score','986321000000103','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - hyperactivity - educator score','986261000000102','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - hyperactivity - parent score','986331000000101','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - impact - educator score','986271000000109','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - impact - parent score','986341000000105','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - peer problems - educator score','986281000000106','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - peer problems - parent score','986351000000108','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - prosocial - educator score','986291000000108','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - prosocial - parent score','986361000000106','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - total difficulties - educator score','986301000000107','v2','0','40','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 2-4 year olds - total difficulties - parent score','986371000000104','v2','0','40','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - parent score','986061000000108','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - teacher score','986151000000106','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - parent score','986071000000101','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - teacher score','986161000000109','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - parent score','986081000000104','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - teacher score','986171000000102','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - parent score','986091000000102','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - teacher score','986181000000100','v2','0','10','Y','','Parent'),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - parent score','986101000000105','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - teacher score','986191000000103','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - parent score','986111000000107','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - teacher score','986201000000101','v2','0','10','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - total difficulties - parent score','986121000000101','v2','0','40','','',''),
 ('CROM','Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - total difficulties - teacher score','986211000000104','v2','0','40','','',''),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score','718143006','v2','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score','718145004','v2','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score','718482000','v2','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score','718477007','v2','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score','718146003','v2','0','10','','',''),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score','718147007','v2','0','10','','',''),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - total difficulties score','718134002','v2','0','40','','',''),
 ('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 1 score - did you feel listened to','1047381000000103','v2','1','5','','',''),
 ('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 2 score - did you talk about what you wanted to talk about','1047391000000101','v2','1','5','','',''),
 ('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 3 score - did you understand the things said in the meeting','1047401000000103','v2','1','5','','',''),
 ('PREM','Session Feedback Questionnaire (SFQ)','Session Feedback Questionnaire item 4 score - did you feel the meeting gave you ideas for what to do','1047411000000101','v2','1','5','','',''),
 ('PREM','Session Rating Scale (SRS)','Session Rating Scale score','720482000','v2','0','40','','',''),
 ('PROM','Sheffield Learning Disabilities Outcome Measure (SLDOM)','Sheffield learning disabilities outcome measure score','860641000000108','v2','0','40','','',''),
 ('PROM','Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)','Short Warwick-Edinburgh Mental Well-being Scale score','718466001','v2','7','35','','',''),
 ('PROM','Warwick-Edinburgh Mental Well-being Scale (WEMWBS)','Warwick-Edinburgh Mental Well-being Scale score','718426000','v2','14','70','','',''),
 ('PROM','Young Child Outcome Rating Scale (YCORS)','Young Child Outcome Rating Scale score','718461006','v2','1','4','','',''),
 ('PROM','YP-CORE','Young Persons Clinical Outcomes in Routine Evaluation clinical score','718437004','v2','0','40','','',''),
 ('PREM','Child Group Session Rating Score (CGSRS)','Child Group Session Rating Scale score','960771000000103','v1','0','40','','',''),
 ('PROM','Child Outcome Rating Scale (CORS)','Child Outcome Rating Scale total score','960321000000103','v1','0','40','Y','','Self'),
 ('PREM','Child Session Rating Scale (CSRS)','Child Session Rating Scale score','1036271000000108','v1','0','40','','',''),
 ('PROM','Childrens Revised Impact of Event Scale (8) (CRIES 8)','Revised Child Impact of Events Scale score','960181000000100','v1','0','40','','',''),
 ('PROM','Clinical Outcomes in Routine Evaluation 10 (CORE 10)','Clinical Outcomes in Routine Evaluation - 10 clinical score','958051000000104','v1','0','40','Y','','Self'),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech frequency and duration score','1037571000000108','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech global rating scale score','1037561000000101','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - disorganised speech level of distress score','1037581000000105','v1','0','100','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas frequency and duration score','1037591000000107','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas global rating scale score','1037541000000102','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - non-bizarre ideas level of distress score','1037601000000101','v1','0','100','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities frequency and duration score','1037611000000104','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities global rating scale score','1037551000000104','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - perceptual abnormalities level of distress score','1037621000000105','v1','0','100','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content frequency and duration score','1037631000000107','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content global rating scale score','1037531000000106','v1','0','6','','',''),
 ('PROM','Comprehensive Assessment of At-Risk Mental States (CAARMS)','Comprehensive Assessment of At-Risk Mental States - unusual thought content level of distress score','1037641000000103','v1','0','100','','',''),
 ('PREM','Group Session Rating Scale (GSRS)','Group Session Rating Scale score','960711000000108','v1','0','40','','',''),
 ('PROM','Kessler Psychological Distress Scale 10','Kessler Psychological Distress Scale 10 score','963561000000106','v1','0','40','','',''),
 ('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire behavioural difficulties score','960221000000105','v1','0','12','','',''),
 ('PROM','MAMS (Me and My School) Questionnaire','MAMS (Me and My School) Questionnaire score','960211000000104','v1','0','32','','',''),
 ('PROM','Outcome Rating Scale (ORS)','Outcome Rating Scale total score','960251000000100','v1','0','40','Y','','Self'),
 ('PROM','Patient Health Questionnaire (PHQ-9)','Patient health questionnaire 9 score','506701000000100','v1','0','27','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score','958231000000102','v1','0','30','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score','958251000000109','v1','0','18','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score','958261000000107','v1','0','18','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score','958221000000104','v1','0','27','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score','958301000000102','v1','0','30','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score','958311000000100','v1','0','18','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score','958321000000106','v1','0','18','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score','958331000000108','v1','0','27','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score','958341000000104','v1','0','21','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score','958351000000101','v1','0','27','Y','','Parent'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety and Depression score','958361000000103','v1','0','141','','',''),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Total Anxiety score','958371000000105','v1','0','111','','',''),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score','958241000000106','v1','0','21','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score','958211000000105','v1','0','27','Y','','Self'),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety and Depression score','958281000000103','v1','0','141','','',''),
 ('PROM','RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated','RCADS (Revised Childrens Anxiety and Depression Scale) - Total Anxiety score','958271000000100','v1','0','111','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - average total score','960021000000100','v1','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability average score','960131000000104','v1','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 1 - Strengths and Adaptability total score','959981000000102','v1','5','25','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties average score','960141000000108','v1','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 2 - Overwhelmed by Difficulties total score','959991000000100','v1','5','25','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication average score','960151000000106','v1','1','5','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - Dimension 3 - Disrupted Communication total score','960001000000109','v1','5','25','','',''),
 ('PROM','SCORE-15 Index of Family Functioning and Change','SCORE Index of Family Function and Change - 15 - total score','959961000000106','v1','15','75','','',''),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score','985991000000105','v1','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score','986001000000109','v1','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score','986011000000106','v1','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score','986051000000105','v1','0','10','Y','','Self'),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score','986021000000100','v1','0','10','','',''),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score','986031000000103','v1','0','10','','',''),
 ('PROM','Strengths and Difficulties Questionnaire (SDQ)','SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - total difficulties score','986041000000107','v1','0','40','','',''),
 ('PREM','Session Rating Scale (SRS)','Session Rating Scale score','960451000000101','v1','0','40','','',''),
 ('PROM','Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)','Short Warwick-Edinburgh Mental Well-being Scale score','960481000000107','v1','7','35','','',''),
 ('PROM','Warwick-Edinburgh Mental Well-being Scale (WEMWBS)','Warwick-Edinburgh Mental Well-being Scale score','885541000000103','v1','14','70','','',''),
 ('PROM','Young Child Outcome Rating Scale (YCORS)','Young Child Outcome Rating Scale score','960391000000100','v1','1','4','','',''),
 ('PROM','YP-CORE','Young Persons Clinical Outcomes in Routine Evaluation clinical score','960601000000104','v1','0','40','','',''),
 ('PROM','PGSI (Problem Gambling Severity Index)','Problem Gambling Severity Index score','492451000000101','v1','0','27','','',''),
 ('PROM','ReQoL (Recovering Quality of Life 20-item)','Recovering Quality of Life 20-item questionnaire score','1091081000000103','v1','0','80','','',''),
 ('PROM','ReQoL (Recovering Quality of Life 10-item)','Recovering Quality of Life 10-item questionnaire score','1091071000000100','v1','0','40','','','')

# COMMAND ----------

 %sql
 TRUNCATE TABLE $db_output.validcodes1

# COMMAND ----------

 %sql
 INSERT INTO $db_output.validcodes1
  VALUES ('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '1', 1390, null)
  ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '2', 1390, null)
  ,('mhs101referral', 'ClinRespPriorityType', 'ED86_89', 'include', '4', 1459, null)
  
  ,('mhs101referral', 'ClinRespPriorityType', 'ED87_90', 'include', '3', 1390, null)
  
  ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '3', 1390, null)
  ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WaitingTimes', 'include', '4', 1459, null)
  
  ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '1', 1390, null)
  ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '2', 1390, null)
  ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '3', 1390, null)
  ,('mhs101referral', 'ClinRespPriorityType', 'CYP_ED_WT', 'include', '4', 1459, null)
  
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'A1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'A2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'A3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'B1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'B2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'C1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'C2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'D1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E4', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E5', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'E6', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'F1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'F2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'F3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'G4', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'H1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'H2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'I1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'I2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'J4', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K4', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'K5', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'L1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'L2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M2', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M4', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M5', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M6', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M7', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'M9', 1459, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'N3', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'P1', 1429, null)
  ,('mhs101referral', 'SourceOfReferralMH', 'MH32', 'include', 'Q1', 1459, null)


# COMMAND ----------

 %sql
 INSERT INTO $db_output.validcodes1
  VALUES ('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A01', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A02', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A03', 1429, 1458)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A04', 1429, 1458)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A05', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A06', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A07', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A08', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A09', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A10', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A11', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A12', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A13', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A14', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A15', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A16', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A17', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A18', 1429, null) 
  
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A21', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A22', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A23', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A24', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'A25', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'B01', 1429, null)
  
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C02', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C04', 1429, null)
  
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C08', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'C10', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D01', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D02', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D03', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D04', 1429, null)
  
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D06', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D07', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'D08', 1429, null)
  
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F01', 1459, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F02', 1459, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F03', 1459, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F04', 1459, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F05', 1459, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'F06', 1459, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z01', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'referral_list', 'include', 'Z02', 1429, null)
  
  
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A02', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'CCR7071_prep', 'include', 'A03', 1429, 1458)
  
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A02', 1429, null)
  ,('MHS102ServiceTypeReferredTo', 'ServTeamTypeRefToMH', 'crisis_resolution', 'include', 'A03', 1429, 1458)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.validcodes1
  VALUES ('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '01', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '02', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '03', 1390, 1458)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_ED_WaitingTimes', 'include', '11', 1459, null)
  
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '01', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '02', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '03', 1390, 1458)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '04', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS_FOLLOWUP', 'include', '11', 1459, null)
  
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '05', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '06', 1390, 1458)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '09', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '10', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '12', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '13', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', '72HOURS', 'exclude', '98', 1459, null)
  
  ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '01', 1390, null) -- 01_Prepare/2.AWT_prep
  ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '02', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '03', 1390, 1458)
  ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '04', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'AWT', 'include', '11', 1459, null)
  
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '05', 1390, null) -- 01_Prepare/3.CYP_2nd_contact_prep
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '06', 1390, 1458)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '09', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '10', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '12', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'exclude', '13', 1459, null)
  
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '01', 1390, null) -- /menh_publications/notebooks/06_CYP_Outcome_Measures/01_Prepare/PrepViews
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '02', 1390, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '03', 1390, 1458)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '04', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP', 'include', '11', 1459, null)
  
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '05', 1390, null) -- /menh_publications/notebooks/06_CYP_Outcome_Measures/01_Prepare/PrepViews
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '06', 1390, 1458)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '09', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '10', 1459, null)
  ,('mhs201carecontact', 'ConsMechanismMH', 'CYP_KOOTH', 'include', '13', 1459, null)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.validcodes1
  VALUES ('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '51484002', 1390, null)
  ,('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '1111811000000109', 1390, 1488)
  ,('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '443730003', 1390, null)
  ,('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '444175001', 1390, null)
  ,('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '718023002', 1390, 1488)
  ,('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '984421000000104', 1390, 1488)
  ,('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '1323681000000103', 1477, null)
  ,('MHS202CareActivity', 'CodeProcAndProcStatus', 'CYP_ED_WaitingTimes', 'include', '1362001000000104', 1477, null)

# COMMAND ----------

 %sql
 INSERT INTO $db_output.validcodes1
  VALUES ('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '30', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '37', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '38', 1390, 1458)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '40', 1459, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '42', 1459, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '48', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '49', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '50', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '53', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '79', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '84', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '87', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'exclude', '89', 1459, null)
  
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '37', 1390, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '38', 1390, 1458)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '40', 1459, null)
  ,('mhs501hospprovspell', 'DestOfDischHospProvSpell', '72HOURS', 'include', '42', 1459, null)