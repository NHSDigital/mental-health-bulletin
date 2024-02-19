# Databricks notebook source
dbutils.widgets.text("start_month_id", "1453")
dbutils.widgets.text("end_month_id", "1464")
dbutils.widgets.text("db_source", "mhsds_database")
dbutils.widgets.text("status", "Final")
dbutils.widgets.text("rp_enddate", "2022-03-31")

# COMMAND ----------

db_source = dbutils.widgets.get("db_source")
end_month_id = dbutils.widgets.get("end_month_id")
start_month_id = dbutils.widgets.get("start_month_id")

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_outcomes_ass1;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_outcomes_ass1 
 (
 Assessment_Tool_Name STRING,
 Active_Concept_ID_SNOMED STRING,
 Preferred_Term_SNOMED STRING,
 SNOMED_Version STRING,
 Der_Rater STRING
 )

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_outcomes_ass1
 SELECT
 a.Assessment_Tool_Name,
 a.Active_Concept_ID_SNOMED,
 a.Preferred_Term_SNOMED,
 a.SNOMED_Version,
 CASE WHEN a.Preferred_Term_SNOMED IN 
         ('Childrens global assessment scale score'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 1 score - disruptive, antisocial or aggressive behaviour'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 10 score - peer relationships'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 11 score - self care and independence'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 12 score - family life and relationships'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 13 score - poor school attendance'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 2 score - overactivity, attention and concentration'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 3 score - non-accidental self injury'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 4 score - alcohol, substance/solvent misuse'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 5 score - scholastic or language skills'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 6 score - physical illness or disability problems'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 7 score - hallucinations and delusions'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 8 score - non-organic somatic symptoms'
         ,'HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 9 score - emotional and related symptoms'
         ) THEN 'Clinician' 
      WHEN a.Preferred_Term_SNOMED IN 
         ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 10 score - peer relationships'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 11 score - self care and independence'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 12 score - family life and relationships'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 13 score - poor school attendance'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 2 score - overactivity, attention and concentration'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 3 score - non-accidental self injury'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 4 score - alcohol, substance/solvent misuse'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 5 score - scholastic or language skills'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 6 score - physical illness or disability problems'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 7 score - hallucinations and delusions'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 8 score - non-organic somatic symptoms'
         ,'HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 9 score - emotional and related symptoms'
         ,'Child Outcome Rating Scale total score'
         ,'Clinical Outcomes in Routine Evaluation - 10 clinical score'
         ,'Generalized anxiety disorder 7 item score'
         ,'Goal Progress Chart - Child/Young Person - goal 1 score'
         ,'Goal Progress Chart - Child/Young Person - goal 2 score'
         ,'Goal Progress Chart - Child/Young Person - goal 3 score'
         ,'Goal Progress Chart - Child/Young Person - goal score'
         ,'Goal-Based Outcomes tool goal progress chart - goal 1 score' ---added AT BITC-5078
         ,'Goal-Based Outcomes tool goal progress chart - goal 2 score' ---added AT BITC-5078
         ,'Goal-Based Outcomes tool goal progress chart - goal 3 score' ---added AT BITC-5078
         ,'Outcome Rating Scale total score'
         ,'Patient health questionnaire 9 score'
         ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'
         ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'
         ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'
         ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'
         ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'
         ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'
         ,'SCORE Index of Family Function and Change - 15 - total score'
         ,'Short Warwick-Edinburgh Mental Well-being Scale score'
         ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score'
         ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score'
         ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score'
         ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score'
         ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score'
         ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score'
         ,'Warwick-Edinburgh Mental Well-being Scale score'
         ,"Young Persons Clinical Outcomes in Routine Evaluation clinical score") THEN 'Self' 
      ELSE 'Parent' 
      END as Der_Rater
 FROM $db_output.mh_ass a
 WHERE a.Assessment_Tool_Name IN 
     ('Childrens Global Assessment Scale (CGAS)'
     ,"HoNOS-CA (Child and Adolescent) - Self rated"
     ,"HoNOS-CA (Child and Adolescent) - Parent rated"
     ,"HoNOS-CA (Child and Adolescent) - Clinician rated" 
     ,'Child Outcome Rating Scale (CORS)'
     ,'Clinical Outcomes in Routine Evaluation 10 (CORE 10)'  
     ,'Genralised Anxiety Disorder 7 (GAD-7)'
     ,'Goal Based Outcomes (GBO)'
     ,'Outcome Rating Scale (ORS)'
     ,'Patient Health Questionnaire (PHQ-9)'
     ,'Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)'
     ,'Warwick-Edinburgh Mental Well-being Scale (WEMWBS)'
     ,'YP-CORE')
 OR a.Preferred_Term_SNOMED IN 
     ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - parent score'
     ,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - parent score'
     ,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - parent score'
     ,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - parent score'
     ,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - parent score'
     ,'SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - parent score'
     ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score'
     ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score'
     ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score'
     ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score'
     ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score'
     ,'SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score'
     ,'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score'
     ,'SCORE Index of Family Function and Change - 15 - total score')

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.rcads_transform_score;
 CREATE TABLE IF NOT EXISTS $db_output.rcads_transform_score 
 (
 Gender STRING,
 Age STRING,
 AssName STRING,
 Sample_Mean FLOAT,
 Sample_SD FLOAT
 )

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.rcads_transform_score
 VALUES
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.25, 4.09),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.25, 4.09),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.98, 3.36),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.98, 3.36),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.15, 3.2),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.15, 3.2),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.15),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.15),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.87, 3.93),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.87, 3.93),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 9.77, 4.51),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 9.77, 4.51),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.74, 4.75),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 8.74, 4.75),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.77, 3.77),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.77, 3.77),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 7.62, 3.68),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 7.62, 3.68),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 6.51, 4.73),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 6.51, 4.73),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 7.05, 4.31),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 7.05, 4.31),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.61, 4.98),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.61, 4.98),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.07, 3.64),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.07, 3.64),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.44, 3.13),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.44, 3.13),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.01, 3.26),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.01, 3.26),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.06, 3.6),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.06, 3.6),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.2, 3.05),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.2, 3.05),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.3, 4.75),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.3, 4.75),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.64, 4.1),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.64, 4.1),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.01, 3.68),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.01, 3.68),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.39, 3.46),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 6.39, 3.46),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.3),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.25, 4.3),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.74, 3.78),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 4.74, 3.78),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.92, 5.21),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.92, 5.21),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 6.71, 3.64),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 6.71, 3.64),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.2, 3.14),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.2, 3.14),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.22, 3.4),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.22, 3.4),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.62, 3.36),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.62, 3.36),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.26, 2.47),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.26, 2.47),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.05, 4.74),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.05, 4.74),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.89, 3.91),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.89, 3.91),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.42, 3.16),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.42, 3.16),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.12, 3.34),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.12, 3.34),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.03, 3.92),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.03, 3.92),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3, 2.72),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3, 2.72),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 13.01, 4.94),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 13.01, 4.94),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.44, 4.1),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.44, 4.1),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.07, 2.93),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.07, 2.93),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.65, 2.89),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.65, 2.89),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.76, 3.21),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.76, 3.21),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.5, 2.46),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.5, 2.46),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.68, 4.74),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 11.68, 4.74),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.65, 3.68),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.65, 3.68),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.28, 3.44),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 7.28, 3.44),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.12, 2.79),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 4.12, 2.79),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.18, 3.07),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 4.18, 3.07),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.34, 2.23),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 2.34, 2.23),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.27, 5),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.27, 5),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.32, 3.81),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 7.32, 3.81),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.76, 3.44),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 6.76, 3.44),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.18, 3.12),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.18, 3.12),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.79, 2.71),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 3.79, 2.71),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 1.9, 2.03),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 1.9, 2.03),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.67, 4.49),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 10.67, 4.49),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 9.36, 4.45),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 9.36, 4.45),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.49, 3.71),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 8.49, 3.71),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.48, 3.82),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 5.48, 3.82),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.26, 4.28),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 5.26, 4.28),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.05, 2.57),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 3.05, 2.57),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.85, 4.98),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 12.85, 4.98),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.71, 2.93),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.71, 2.93),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.11, 3),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.11, 3),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.04, 2.43),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.04, 2.43),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.91, 1.9),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.91, 1.9),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.29, 3),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.29, 3),
 ('M', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.44, 3.88),
 ('M', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.44, 3.88),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.25, 3.58),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.25, 3.58),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4, 2.87),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4, 2.87),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.63),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.63),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.87, 2.61),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.87, 2.61),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.2, 3),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 4.2, 3),
 ('F', '8', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.01, 3.87),
 ('F', '9', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.01, 3.87),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.62, 2.87),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.62, 2.87),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.74, 2.49),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.74, 2.49),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.31),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.01, 2.31),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.64, 1.84),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.64, 1.84),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.85, 2.79),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.85, 2.79),
 ('M', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.71, 3.94),
 ('M', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.71, 3.94),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.75, 3.63),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.75, 3.63),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.18, 3.18),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 4.18, 3.18),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.03, 2.65),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.03, 2.65),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.79, 2.3),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.79, 2.3),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 3.46, 2.95),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 3.46, 2.95),
 ('F', '10', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.94, 5.16),
 ('F', '11', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.94, 5.16),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.54, 3.18),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.54, 3.18),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.26, 2.6),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.26, 2.6),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.62, 1.98),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.62, 1.98),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.61, 1.56),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.61, 1.56),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.97, 2.21),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.97, 2.21),
 ('M', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.59, 4.31),
 ('M', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.59, 4.31),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.6, 3.37),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.6, 3.37),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.23, 2.54),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.23, 2.54),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.41, 1.94),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.41, 1.94),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.82, 1.98),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.82, 1.98),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.08, 2.33),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 2.08, 2.33),
 ('F', '12', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.62, 4.65),
 ('F', '13', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.62, 4.65),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 5.21, 3.51),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 5.21, 3.51),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.73, 2.75),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.73, 2.75),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.58, 3.03),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 2.58, 3.03),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.19, 2.34),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.19, 2.34),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.69, 1.89),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.69, 1.89),
 ('M', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.39, 4.19),
 ('M', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.39, 4.19),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.97, 3.25),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.97, 3.25),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.46, 3.02),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.46, 3.02),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.89, 2.57),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.89, 2.57),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.83, 2.13),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.83, 2.13),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.91, 2.49),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.91, 2.49),
 ('F', '14', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.83, 4.73),
 ('F', '15', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.83, 4.73),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.94, 3.88),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 3.94, 3.88),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.22, 2.5),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.22, 2.5),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.11, 1.96),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.11, 1.96),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.5, 1.69),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 1.5, 1.69),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.15, 1.55),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.15, 1.55),
 ('M', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.32, 3.69),
 ('M', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 7.32, 3.69),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 4.91, 3.17),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 4.91, 3.17),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.76, 2.28),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 3.76, 2.28),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.8, 2.34),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 1.8, 2.34),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.04, 2.27),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 2.04, 2.27),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.92, 1.98),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 1.92, 1.98),
 ('F', '16', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.35, 4.38),
 ('F', '17', 'RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 8.35, 4.38)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_reliable_change_thresholds;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_reliable_change_thresholds
 (
 AssName STRING,
 Threshold FLOAT
 )

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_reliable_change_thresholds
 VALUES
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Depression score', 22.87), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Generalized Anxiety score', 18.3), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Obsessions/Compulsions score', 24.06), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Panic score', 40.93), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Separation Anxiety score', 28),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent version - Social Phobia score', 16.63),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - conduct problems - parent score', 4), ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - emotional symptoms - parent score', 4),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - hyperactivity - parent score', 4), ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - impact - parent score', 3), ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - peer problems - parent score', 3), --- derived from SDQ norms / Goodman (2001) paper
 ('SDQ (Strengths and Difficulties Questionnaire) for parents or educators of 4-17 year olds - prosocial - parent score', 3), --- derived from SDQ norms / Goodman (2001) paper
 ('Child Outcome Rating Scale total score', 10),  ---- CYP IAPT 2015
 ('Clinical Outcomes in Routine Evaluation - 10 clinical score', 5), ---- CYP IAPT 2015
 ('Generalized anxiety disorder 7 item score', 4),  ---- CYP IAPT 2015
 ('Outcome Rating Scale total score', 6.6),  ---- CYP IAPT 2015
 ('Patient health questionnaire 9 score', 6),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Depression score', 17.73), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Generalized Anxiety score', 14.91), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Obsessions/Compulsions score', 16.35), ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Panic score', 18.29),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Separation Anxiety score', 22.95),  ---- CYP IAPT 2015
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Social Phobia score', 13.99),  ---- CYP IAPT 2015
 ('SCORE Index of Family Function and Change - 15 - total score', 10), -- from Stratton et al. (2014)
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - conduct problems score', 4),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - emotional symptoms score', 4),  ---- CYP IAPT 2015Br
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - hyperactivity score', 4),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - impact score', 3),  ---- CYP IAPT 2015
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - peer problems score', 5), --- derived from SDQ norms / Goodman (2001) paper 
 ('SDQ (Strengths and Difficulties Questionnaire) self-rated for 11-17 year olds - prosocial score', 4), --- derived from SDQ norms / Goodman (2001) paper 
 ("Young Persons Clinical Outcomes in Routine Evaluation clinical score", 8),  ---- CYP IAPT 2015
 ('Goal-Based Outcomes tool goal progress chart - goal 1 score', 3), ---added AT BITC-5078
 ('Goal-Based Outcomes tool goal progress chart - goal 2 score', 3), ---added AT BITC-5078
 ('Goal-Based Outcomes tool goal progress chart - goal 3 score', 3), ---added AT BITC-5078
 ('Goal Progress Chart - Child/Young Person - goal score', 3),
 ('Goal Progress Chart - Child/Young Person - goal 1 score', 3),
 ('Goal Progress Chart - Child/Young Person - goal 2 score', 3),
 ('Goal Progress Chart - Child/Young Person - goal 3 score', 3),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 1 score - disruptive, antisocial or aggressive behaviour', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 2 score - overactivity, attention and concentration', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 3 score - non-accidental self injury', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 4 score - alcohol, substance/solvent misuse', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 5 score - scholastic or language skills', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 6 score - physical illness or disability problems', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 7 score - hallucinations and delusions', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 8 score - non-organic somatic symptoms', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 9 score - emotional and related symptoms', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 10 score - peer relationships', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 11 score - self care and independence', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 12 score - family life and relationships', 2),
 ('HoNOSCA-SR (Health of the Nation Outcome Scales for Children and Adolescents - self-rated) scale 13 score - poor school attendance', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 1 score - disruptive, antisocial or aggressive behaviour', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 2 score - overactivity, attention and concentration', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 3 score - non-accidental self injury', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 4 score - alcohol, substance/solvent misuse', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 5 score - scholastic or language skills', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 6 score - physical illness or disability problems', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 7 score - hallucinations and delusions', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 8 score - non-organic somatic symptoms', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 9 score - emotional and related symptoms', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 10 score - peer relationships', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 11 score - self care and independence', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 12 score - family life and relationships', 2),
 ('HoNOSCA (Health of the Nation Outcome Scales for Children and Adolescents) - parents assessment scale 13 score - poor school attendance', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 1 score - disruptive, antisocial or aggressive behaviour', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 2 score - overactivity, attention and concentration', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 3 score - non-accidental self injury', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 4 score - alcohol, substance/solvent misuse', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 5 score - scholastic or language skills', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 6 score - physical illness or disability problems', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 7 score - hallucinations and delusions', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 8 score - non-organic somatic symptoms', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 9 score - emotional and related symptoms', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 10 score - peer relationships', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 11 score - self care and independence', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 12 score - family life and relationships', 2),
 ('HoNOSCA-CR (Health of the Nation Outcome Scales for Children and Adolescents - clinician-rated) scale 13 score - poor school attendance', 2),
 ('Short Warwick-Edinburgh Mental Well-being Scale score', 3), ---from Shah et al. (2018)
 ('Warwick-Edinburgh Mental Well-being Scale score', 9), ---from Maheswaran et al. (2012) 
 ('Childrens global assessment scale score', 11) ---from Bird et al. (1987)

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_ass_type_scaling_v2;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_ass_type_scaling_v2
 (
 AssType STRING,
 Scale STRING
 )

# COMMAND ----------

 %sql
 INSERT OVERWRITE TABLE $db_output.cyp_ass_type_scaling_v2
 ---Positive means high score good symptoms
 ---Negative means high score bad symptoms
 VALUES
 ('Brief Parental Self Efficacy Scale (BPSES)', 'Negative'),
 ('CORE-OM (Clinical Outcomes in Routine Evaluation - Outcome Measure)', 'Negative'),
 ('Child Group Session Rating Score (CGSRS)', 'Negative'),
 ('Child Outcome Rating Scale (CORS)', 'Positive'),
 ('Child Session Rating Scale (CSRS)', 'Negative'),
 ('Childrens Global Assessment Scale (CGAS)', 'Positive'),
 ('Childrens Revised Impact of Event Scale (8) (CRIES 8)', 'Negative'),
 ('Clinical Outcomes in Routine Evaluation 10 (CORE 10)', 'Negative'),
 ('Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Parent rated', 'Negative'),
 ('Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 12 to 18 yrs', 'Negative'),
 ('Commission for Health Improvement Experience of Service Questionnaire (CHI-ESQ) - Self rated 9 to 11 yrs', 'Negative'),
 ('Comprehensive Assessment of At-Risk Mental States (CAARMS)', 'Negative'),
 ('Current View', 'Negative'),
 ('DIALOG', 'Negative'),
 ('Eating Disorder Examination Questionnaire (EDE-Q)', 'Negative'),
 ('Eating Disorder Examination Questionnaire (EDE-Q) - Adolescents', 'Negative'),
 ('Genralised Anxiety Disorder 7 (GAD-7)', 'Negative'),
 ('Goal Based Outcomes (GBO)', 'Positive'),
 ('Group Session Rating Scale (GSRS)', 'Negative'),
 ('HoNOS Working Age Adults', 'Negative'),
 ('HoNOS-ABI', 'Negative'),
 ('HoNOS-CA (Child and Adolescent) - Clinician rated', 'Negative'),
 ('HoNOS-CA (Child and Adolescent) - Parent rated', 'Negative'),
 ('HoNOS-CA (Child and Adolescent) - Self rated', 'Negative'),
 ('HoNOS-LD (Learning Disabilities)', 'Negative'),
 ('HoNOS 65+ (Older Persons)', 'Negative'),
 ('HoNOS-Secure', 'Negative'),
 ('Kessler Psychological Distress Scale 10', 'Negative'),
 ('MAMS (Me and My School) Questionnaire', 'Negative'),
 ('Me and My Feelings Questionnaire', 'Negative'),
 ('ODD (Parent)', 'Negative'),
 ('Outcome Rating Scale (ORS)', 'Positive'),
 ('PGSI (Problem Gambling Severity Index)', 'Negative'),
 ('Patient Health Questionnaire (PHQ-9)', 'Negative'),
 ('Questionnaire about the Process of Recovery (QPR)', 'Negative'),
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Self rated', 'Negative'),
 ('RCADS (Revised Childrens Anxiety and Depression Scale) - Parent rated', 'Negative'),
 ('ReQoL (Recovering Quality of Life 20-item)', 'Negative'),
 ('ReQoL (Recovering Quality of Life 10-item)', 'Negative'),
 ('SCORE-15 Index of Family Functioning and Change', 'Positive'),
 ('Session Feedback Questionnaire (SFQ)', 'Negative'),
 ('Session Rating Scale (SRS)', 'Negative'),
 ('Sheffield Learning Disabilities Outcome Measure (SLDOM)', 'Negative'),
 ('Short Warwick-Edinburgh Mental Well-being Scale (SWEMWBS)', 'Positive'),
 ('Strengths and Difficulties Questionnaire (SDQ)', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 2 to 4 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Parent rated 2 to 4 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Parent rated 4 to 17 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Teacher rated 4 to 17 yrs', 'Negative'),
 ('Strengths and Difficulties Questionnaire (SDQ) - Self rated 11 to 17 yrs', 'Negative'),
 ('Warwick-Edinburgh Mental Well-being Scale (WEMWBS)', 'Positive'),
 ('YP-CORE', 'Negative'),
 ('Young Child Outcome Rating Scale (YCORS)', 'Negative')

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_closed_referrals;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_closed_referrals AS
 SELECT 
 DISTINCT ---selecting distinct as same referral could have been referred to different servteamtype/careprofteamid in month
  r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.ReportingPeriodEndDate
 ,r.OrgIDProv AS Provider_Code 
 ,COALESCE(o.NAME,'Unknown') AS Provider_Name
 ,COALESCE(c.STP21CDH,'Unknown') AS ICB_Code
 ,COALESCE(c.STP21NM,'Unknown') AS ICB_Name
 ,COALESCE(c.NHSER21CDH,'Unknown') AS Region_Code
 ,COALESCE(c.NHSER21NM,'Unknown') AS Region_Name
 ,r.LADistrictAuth
 ,r.AgeServReferRecDate
 ,CASE WHEN r.Gender = '1' THEN 'M' WHEN r.Gender = '2' THEN 'F' ELSE NULL END as Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate
 ,r.ReferRejectionDate
 ,i.Der_HospSpellCount    
  
 FROM $db_output.nhse_pre_proc_referral r 
 
 LEFT JOIN (
   SELECT i.Person_ID, i.UniqServReqID, COUNT(i.Der_HospSpellRecordOrder) AS Der_HospSpellCount
   FROM $db_output.Der_NHSE_Pre_Proc_Inpatients i ---using this table as it has Der_HospSpellRecordOrder deriavtion
   GROUP BY i.Person_ID, i.UniqServReqID
   ) i ON i.Person_ID = r.Person_ID AND i.UniqServReqID = r.UniqServReqID ---to indentify referrals to inpatient services (and subsequently remove) 
 
 LEFT JOIN $db_output.mhb_org_daily o ON r.OrgIDProv = o.ORG_CODE ---provider reference data
 LEFT JOIN $db_output.ccg_mapping_2021_v2 c on r.OrgIDCCGRes = c.CCG21CDH ---ICB and Region reference data
  
 WHERE r.AgeServReferRecDate BETWEEN 0 AND 17 ---changed from 0-18 to match CQUIN 
 AND r.UniqMonthID BETWEEN $start_month_id AND $end_month_id
 AND r.ReferralRequestReceivedDate >= '2016-01-01' 
 AND r.ReferRejectionDate IS NULL
 AND (r.ServDischDate BETWEEN r.ReportingPeriodStartDate AND r.ReportingPeriodEndDate) ---only bring referral record for the month it was discharged in
 AND i.Der_HospSpellCount IS NULL ---to exclude referrals with an associated hospital spell 
 AND (r.LADistrictAuth LIKE 'E%' OR r.LADistrictAuth IS NULL OR r.LADistrictAuth = "") ---to limit to those people whose commissioner is an English organisation 
 AND (r.ServTeamTypeRefToMH NOT IN ('B02','E01','E02','E03','E04','A14') OR r.ServTeamTypeRefToMH IS NULL)--- exclude specialist teams *but include those where team type is null

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_closed_contacts;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_closed_contacts AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_closed_contacts
 SELECT 
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber 
 ,r.Provider_Code
 ,MAX(CASE WHEN a.Der_ContactOrder = 1 THEN a.Der_ContactDate ELSE NULL END) AS Contact1
 ,MAX(CASE WHEN a.Der_ContactOrder = 2 THEN a.Der_ContactDate ELSE NULL END) AS Contact2
  
 FROM $db_output.cyp_closed_referrals r  
 INNER JOIN $db_output.Der_NHSE_Pre_Proc_Activity a 
 ON r.Person_ID = a.Person_ID AND a.UniqServReqID = r.UniqServReqID AND a.Der_ContactDate <= '$rp_enddate' AND a.Der_ContactDate <= r.ServDischDate ---contact before or on day of discharge or end of reporting period
 GROUP BY r.Person_ID, r.UniqServReqID, r.RecordNumber, r.Provider_Code

# COMMAND ----------

 %sql
 -- DROP TABLE IF EXISTS $db_output.cyp_all_assessments;
 CREATE or replace TABLE $db_output.cyp_all_assessments AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_all_assessments
 SELECT
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Provider_Code
 ,a.Der_AssUniqID
 ,a.Der_AssTable
 ,a.Der_AssToolCompDate
 ,a.CodedAssToolType
 ,a.Der_PreferredTermSNOMED
 ,a.Der_AssessmentToolName
 ,a.Der_AssessmentCategory
 ,a.PersScore
 ,a.Der_ValidScore
 ,mh.Der_Rater as Rater  
 
 FROM $db_output.cyp_closed_referrals r
 INNER JOIN $db_output.nhse_pre_proc_assessments_unique a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.Der_AssToolCompDate <= '$rp_enddate' AND a.Der_AssToolCompDate <= r.ServDischDate
 LEFT JOIN $db_output.cyp_outcomes_ass1 mh ON a.CodedAssToolType = mh.Active_Concept_ID_SNOMED
 WHERE mh.Assessment_Tool_Name IS NOT NULL and mh.Preferred_Term_SNOMED IS NOT NULL AND is_numeric(a.PersScore) = 1 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_ref_cont_out;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_ref_cont_out AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_ref_cont_out
 SELECT 
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.Provider_Code 
 ,r.Provider_Name
 ,r.ICB_Code
 ,r.ICB_Name
 ,r.Region_Code
 ,r.Region_Name
 ,r.AgeServReferRecDate AS Age
 ,r.Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate
 ,c.Contact1
 ,c.Contact2
 ,a.Der_AssUniqID
 ,a.Der_AssTable
 ,a.Der_AssToolCompDate
 ,a.CodedAssToolType
 ,a.Der_PreferredTermSNOMED AS AssName
 ,a.Der_AssessmentToolName AS AssType 
 ,a.Rater
 ,a.Der_AssessmentCategory
 ,CAST(a.PersScore AS float) AS RawScore
 ,a.Der_ValidScore
  
 FROM $db_output.cyp_closed_referrals r  
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.Person_ID = c.Person_ID AND r.UniqServReqID = c.UniqServReqID AND r.RecordNumber = c.RecordNumber 
 LEFT JOIN $db_output.cyp_all_assessments a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND r.RecordNumber = a.RecordNumber 

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_valid_unique_assessments_rcads;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_valid_unique_assessments_rcads AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_valid_unique_assessments_rcads
 SELECT 
 a.Person_ID
 ,a.UniqServReqID
 ,a.RecordNumber
 ,a.Der_FY
 ,a.UniqMonthID
 ,a.ReportingPeriodStartDate
 ,a.Provider_Code 
 ,a.Provider_Name
 ,a.ICB_Code
 ,a.ICB_Name
 ,a.Region_Code
 ,a.Region_Name
 ,a.Age
 ,a.Gender
 ,a.ReferralRequestReceivedDate
 ,a.ServDischDate
 ,a.Contact1
 ,a.Contact2
 ,a.Der_AssUniqID
 ,a.Der_AssTable
 ,a.Der_AssToolCompDate
 ,a.CodedAssToolType
 ,a.AssName
 ,a.AssType 
 ,a.Rater
 ,a.Der_AssessmentCategory
 ,a.RawScore
 ,a.Der_ValidScore
 ,CASE WHEN t.Gender IS NOT null AND t.Age IS NOT NULL AND t.AssName IS NOT NULL ---if Gender, Age and RCADS assessment are not null then transform
      THEN (
      (a.RawScore - t.Sample_Mean) 
      / t.Sample_SD ---Z score formula (RawScore - Sample Mean) / Sample Standard Distribution
      ) * 10 
      + 50 ---T score formula (Z score * 10) + 50
       ELSE a.RawScore ---only transform RCADS scores else keep same
       END as TScore 
  
 FROM $db_output.cyp_ref_cont_out a
 LEFT JOIN $db_output.rcads_transform_score t ON a.Gender = t.Gender AND a.Age = t.Age AND a.AssName = t.AssName
 WHERE a.Der_ValidScore = 'Y'

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_partition_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_partition_assessments AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_partition_assessments
 SELECT *,
 ROW_NUMBER () OVER (PARTITION BY Person_ID, UniqServReqID, AssName, RecordNumber ORDER BY Der_AssToolCompDate ASC, Der_AssUniqID ASC) AS RN
  
 FROM $db_output.cyp_valid_unique_assessments_rcads
  
 WHERE Contact1 <= Der_AssToolCompDate

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_last_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_last_assessments AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_last_assessments
 SELECT 
 a.Person_ID,
 a.UniqServReqID,
 a.AssName,
 MAX(a.RN) AS max_ass
  
 FROM $db_output.cyp_partition_assessments a 
 WHERE a.RN > 1 and a.Der_AssToolCompDate >= a.Contact2  
 GROUP BY a.Person_ID, a.UniqServReqID, a.AssName

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_first_and_last_assessments;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_first_and_last_assessments AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_first_and_last_assessments
 SELECT      
 r.Person_ID
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.Provider_Code 
 ,r.Provider_Name
 ,r.ICB_Code
 ,r.ICB_Name
 ,r.Region_Code
 ,r.Region_Name
 ,r.AgeServReferRecDate AS Age
 ,r.Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate       
 ,c.Contact1
 ,c.Contact2
 ,a.Der_AssessmentCategory
 ,a.Rater
 ,a.AssType
 ,a.AssName
 ,a.Der_AssUniqID AS Der_AssUniqID_first
 ,a.Der_AssToolCompDate AS AssDate_first
 ,a.TScore AS Score_first
 ,b.Der_AssUniqID AS Der_AssUniqID_last
 ,b.Der_AssToolCompDate AS AssDate_last
 ,b.TScore AS Score_last
 ,b.TScore - a.TScore AS Score_Change
 ,rct.Threshold ---if assessment doesn't have threshold (i.e. not in reference data) keep as null
  
 FROM $db_output.cyp_closed_referrals r 
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.UniqServReqID = c.UniqServReqID AND r.Person_ID = c.Person_ID 
 LEFT JOIN $db_output.cyp_partition_assessments a ON r.Person_ID = a.Person_ID AND r.UniqServReqID = a.UniqServReqID AND a.RN = 1 ---join to get first assessment
  
 LEFT JOIN 
     (SELECT x.* 
     FROM $db_output.cyp_partition_assessments x 
     INNER JOIN $db_output.cyp_last_assessments y 
     ON x.Person_ID = y.Person_ID 
     AND x.UniqServReqID = y.UniqServReqID 
     AND x.Person_ID = y.Person_ID 
     AND x.AssName = y.AssName 
     AND x.RN = y.max_ass) AS b ON a.Person_ID = b.Person_ID AND a.UniqServReqID = b.UniqServReqID AND a.AssName = b.AssName ---join to get last assessment
     
 LEFT JOIN $db_output.cyp_reliable_change_thresholds rct ON a.AssName = rct.AssName

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_rci;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_rci AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_rci
 SELECT
 a.Person_ID,
 a.UniqServReqID,
 a.RecordNumber,
 a.Der_FY,
 a.UniqMonthID,
 a.ReportingPeriodStartDate,
 a.Provider_Code,
 a.Provider_Name,
 a.ICB_Code,
 a.ICB_Name,
 a.Region_Code,
 a.Region_Name,
 a.Age,
 a.Gender,
 a.ReferralRequestReceivedDate,
 a.ServDischDate,
 a.Contact1,
 a.Contact2,
 a.Der_AssessmentCategory,
 a.Rater,
 a.AssType,
 a.AssName,
 a.Der_AssUniqID_first,
 a.AssDate_first,
 a.Score_first,
 a.Der_AssUniqID_last,
 a.AssDate_last,
 a.Score_last,
 a.Score_Change,
 a.Threshold,
 CASE WHEN s.Scale = 'Positive' AND a.Score_Change > 0 AND ABS(a.Score_Change) >= a.Threshold ---Positive means high score good symptoms (i.e. improvement means score will go up)
      THEN 1
      WHEN s.Scale = 'Negative' AND a.Score_Change < 0 AND ABS(a.Score_Change) >= a.Threshold ---Negative means high score bad symptoms (i.e. improvement means score will go down)
      THEN 1
      ELSE NULL END AS Reliable_Improvement,
 CASE WHEN s.Scale = 'Positive' AND a.Score_Change < 0 AND ABS(a.Score_Change) >= a.Threshold ---Positive means high score good symptoms (i.e. deterioration means score will go down)
      THEN 1
      WHEN s.Scale = 'Negative' AND a.Score_Change > 0 AND ABS(a.Score_Change) >= a.Threshold ---Negative means high score bad symptoms (i.e. deterioration means score will go up)
      THEN 1
      ELSE NULL END AS Reliable_Deterioration,
 CASE WHEN ABS(a.Score_Change) < a.Threshold ---If Threshold has not been met between first and last assessment scores then class as No Change
      THEN 1
      ELSE NULL END AS No_Change
 FROM $db_output.cyp_first_and_last_assessments a
 LEFT JOIN $db_output.cyp_ass_type_scaling_v2 s ON a.AssType = s.AssType

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_rci_referral;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_rci_referral AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_rci_referral
 SELECT
 r.Person_ID,
 r.UniqServReqID,
 r.Rater,
 MAX(r.Contact1) AS Contact1, ---first contact for rater
 MAX(r.Contact2) AS Contact2, ---second contact for rater
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_first IS NOT NULL THEN 1 ELSE NULL END) as Assessment, ---had at least 2 contacts and at least 1 assessment for rater
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN 1 ELSE NULL END) as Paired, ---had at least 2 contacts and at least 2 of the same assessment for the rater (paired score)
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN Reliable_Improvement ELSE NULL END) as Improvement, ---paired score with reliable improvement for rater across all assessments
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN No_Change ELSE NULL END) as NoChange, ---paired score with no change for rater across all assessments
 MAX(CASE WHEN r.Contact2 IS NOT NULL AND Der_AssUniqID_last IS NOT NULL THEN Reliable_Deterioration ELSE NULL END) as Deter ---paired score with reliable deterioration for rater across all assessments
  
 FROM $db_output.cyp_rci r
 --WHERE r.AssName IS NOT NULL  ---valid assessments only as per mh_ass 
 GROUP BY r.Person_ID, r.UniqServReqID, r.Rater

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_meaningful_change;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_meaningful_change AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_meaningful_change
 SELECT 
 Person_ID
 ,UniqServReqID
 ,Rater
 ,Contact1
 ,Contact2
 ,Assessment
 ,Paired 
 ,CASE WHEN Improvement = 1 AND Deter IS NULL THEN 1 ELSE 0 END as Improvement ---patient has improvement and not detriorated across all assessments then meaningful improvement
 ,CASE WHEN NoChange = 1 AND Improvement IS NULL AND Deter IS NULL THEN 1 ELSE 0 END as NoChange ---patient has no change and not improved/deteriorated then no change 
 ,CASE WHEN Deter = 1 THEN 1 ELSE 0 END as Deter ---patient has deteriorated at all across all assessments then meaningful deterioration
  
 FROM $db_output.cyp_rci_referral

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.cyp_master;
 CREATE TABLE IF NOT EXISTS $db_output.cyp_master USING DELTA AS
 -- INSERT OVERWRITE TABLE $db_output.cyp_master
 SELECT 
 r.Person_ID
 ,mpi.age_group_higher_level
 ,mpi.age_group_lower_common
 ,mpi.age_group_lower_chap1
 ,mpi.Der_Gender
 ,mpi.Der_Gender_Desc
 ,mpi.UpperEthnicity
 ,mpi.LowerEthnicityCode
 ,mpi.LowerEthnicityName
 ,mpi.IMD_Decile
 ,mpi.IMD_Quintile
 ,r.UniqServReqID
 ,r.RecordNumber
 ,r.Der_FY
 ,r.UniqMonthID
 ,r.ReportingPeriodStartDate
 ,r.Provider_Code as OrgIDProv
 ,r.Provider_Name
 ,get_provider_type_code(r.Provider_Code) as ProvTypeCode
 ,get_provider_type_name(r.Provider_Code) as ProvTypeName
 ,r.ICB_Code as STP_Code
 ,r.ICB_Name as STP_Name
 ,r.Region_Code
 ,r.Region_Name
 ,r.AgeServReferRecDate AS Age
 ,r.Gender
 ,r.ReferralRequestReceivedDate
 ,r.ServDischDate  
 ,c.Contact1
 ,c.Contact2
 -- self-rated measures 
 ,m1.Assessment AS Assessment_SR
 ,m1.Paired AS Paired_SR
 ,m1.Improvement AS Improvement_SR
 ,m1.NoChange AS NoChange_SR
 ,m1.Deter AS Deter_SR
 -- parent-rated measures 
 ,m2.Assessment AS Assessment_PR
 ,m2.Paired AS Paired_PR
 ,m2.Improvement AS Improvement_PR
 ,m2.NoChange AS NoChange_PR
 ,m2.Deter AS Deter_PR
 -- clinician-rated measures
 ,m3.Assessment AS Assessment_CR
 ,m3.Paired AS Paired_CR
 ,m3.Improvement AS Improvement_CR
 ,m3.NoChange AS NoChange_CR
 ,m3.Deter AS Deter_CR
 ,CASE WHEN m1.Assessment = 1 OR m2.Assessment = 1 OR m3.Assessment = 1 THEN 1 ELSE 0 END as Assessment_ANY
 ,CASE WHEN m1.Paired = 1 OR m2.Paired = 1 OR m3.Paired = 1 THEN 1 ELSE 0 END as Paired_ANY
  
 FROM $db_output.cyp_closed_referrals r  
 LEFT JOIN $db_output.cyp_closed_contacts c ON r.UniqServReqID = c.UniqServReqID AND r.Person_ID = c.Person_ID AND r.RecordNumber = c.RecordNumber 
 LEFT JOIN $db_output.MPI mpi ON r.Person_ID = mpi.Person_ID
  
 LEFT JOIN $db_output.cyp_meaningful_change m1 ON r.UniqServReqID = m1.UniqServReqID AND r.Person_ID = m1.Person_ID AND m1.Rater = 'Self' 
 LEFT JOIN $db_output.cyp_meaningful_change m2 ON r.UniqServReqID = m2.UniqServReqID AND r.Person_ID = m2.Person_ID AND m2.Rater = 'Parent' 
 LEFT JOIN $db_output.cyp_meaningful_change m3 ON r.UniqServReqID = m3.UniqServReqID AND r.Person_ID = m3.Person_ID AND m3.Rater = 'Clinician' 

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.cyp_master