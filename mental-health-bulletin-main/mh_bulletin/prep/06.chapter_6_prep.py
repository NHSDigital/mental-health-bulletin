# Databricks notebook source
 %sql
 CREATE OR REPLACE TEMP VIEW TeamType AS
 ---hard coding Team Type name reference data into a table
 select 'A01' as Team_Type_Code, 'Day Care Services' as Team_Name
 union
 (select 'A02', 'Crisis Resolution Team/Home Treatment')
 union
 (select 'A03', 'Adult Community Mental Health Team')
 union
 (select 'A04', 'Older People Community Mental Health Team')
 union
 (select 'A05', 'Assertive Outreach Team')
 union
 (select 'A06', 'Rehabilitation and Recovery Team')
 union
 (select 'A07', 'General Psychiatry')
 union
 (select 'A08', 'Psychiatric Liaison')
 union
 (select 'A09', 'Psychotherapy Service')
 union
 (select 'A10', 'Psychological Therapy Service (IAPT)')
 union
 (select 'A11', 'Psychological Therapy Service (non-IAPT)')
 union
 (select 'A12', 'Young Onset Dementia')
 union
 (select 'A13', 'Personality Disorder Service')
 union
 (select 'A14', 'Early Intervention in Psychosis Team')
 union
 (select 'A15', 'Primary Care Mental Health Services')
 union
 (select 'A16', 'Memory Services/Clinic')
 union
 (select 'B01', 'Forensic Service')
 union
 (select 'B02', 'Community Forensic Service')
 union
 (select 'C01', 'Learning Disability Service')
 union
 (select 'C02', 'Autistic Spectrum Disorder Service')
 union
 (select 'C03', 'Peri-Natal Mental Illness')
 union
 (select 'C04', 'Eating Disorders/Dietetics')
 union
 (select 'D01', 'Substance Misuse Team')
 union
 (select 'D02', 'Criminal Justice Liaison and Diversion Service')
 union
 (select 'D03', 'Prison Psychiatric Inreach Service')
 union
 (select 'D04', 'Asylum Service')
 union
 (select 'ZZZ', 'Other Mental Health Service')

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6a_teamtype AS
 ---service type care contacts count
 select 
 'Service or Team Type' as Breakdown
 --filter to required service type code and name otherwise unknown
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end as Level_1
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6a' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of unique care contacts ---denominator for 6b, 6c, 6d
 from $db_output.MHB_MHS201CareContact a
 left join $db_output.MHB_MHS102ServiceTypeReferredTo b on a.UniqServReqID = b.UniqServReqID and a.UniqCareProfTeamID = b.UniqCareProfTeamID ---count of activity so joining on UniqServReqID and UniqCareProfTeamID
 left join TeamType c on b.ServTeamTypeRefToMH = c.Team_Type_Code
 group by 
 case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end 
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6b_teamtype AS
 ---service type attended care contacts count
 select 
 'Service or Team Type' as Breakdown
 --filter to required service type code and name otherwise unknown
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end as Level_1
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6b' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of unique care contacts where patient attended
 from $db_output.MHB_MHS201CareContact a
 left join $db_output.MHB_MHS102ServiceTypeReferredTo b on a.UniqServReqID = b.UniqServReqID and a.UniqCareProfTeamID = b.UniqCareProfTeamID ---count of activity so joining on UniqServReqID and UniqCareProfTeamID
 left join TeamType c on b.ServTeamTypeRefToMH = c.Team_Type_Code
 where AttendOrDNACode in ('5','6') ---patient attended on time or arrived late and could be seen by professional
 group by 
 case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end 
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6c_teamtype AS
 ---service type non-attended care contacts count
 select 
 'Service or Team Type' as Breakdown
 --filter to required service type code and name otherwise unknown
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end as Level_1
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6c' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of unique care contacts where patient did not attend
 from $db_output.MHB_MHS201CareContact a
 left join $db_output.MHB_MHS102ServiceTypeReferredTo b on a.UniqServReqID = b.UniqServReqID and a.UniqCareProfTeamID = b.UniqCareProfTeamID ---count of activity so joining on UniqServReqID and UniqCareProfTeamID
 left join TeamType c on b.ServTeamTypeRefToMH = c.Team_Type_Code
 where AttendOrDNACode in ('7','2','3') ---patient arrived late and could not be seen by professional or cancelled or did not attend appointment
 group by 
 case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end 
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6d_teamtype AS
 ---service type provider cancelled care contacts count
 select 
 'Service or Team Type' as Breakdown
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end as Level_1
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6d' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of unique care contacts where provider cancelled
 from $db_output.MHB_MHS201CareContact a
 left join $db_output.MHB_MHS102ServiceTypeReferredTo b on a.UniqServReqID = b.UniqServReqID and a.UniqCareProfTeamID = b.UniqCareProfTeamID ---count of activity so joining on UniqServReqID and UniqCareProfTeamID
 left join TeamType c on b.ServTeamTypeRefToMH = c.Team_Type_Code
 where AttendOrDNACode = '4' ---appointment cancelled or postponed by healthcare provider
 group by 
 case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end 
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6e_teamtype AS
 ---service type invalid or missing attendance code contacts count
 select 'Service or Team Type' as Breakdown
 ,a.Level_1
 ,a.Level_1_description
 ,a.Level_2
 ,a.Level_2_description
 ,a.Level_3
 ,a.Level_3_description
 ,a.Level_4
 ,a.Level_4_description
 ,'6e' as metric
 ,sum(a.Metric_value)-SUM(b.Metric_value)-SUM(c.Metric_value)-SUM(d.Metric_value) as metric_value ---getting remaining count of MHS201IDs from 6a denominator
 from global_temp.6a_teamtype a
 left join global_temp.6b_teamtype b on a.Level_1 = b.Level_1
 left join global_temp.6c_teamtype c on a.Level_1 = c.Level_1
 left join global_temp.6d_teamtype d on a.Level_1 = d.Level_1
 group by a.Level_1,a.Level_1_description, a.Level_2, a.Level_2_description, a.Level_3, a.Level_3_description, a.Level_4, a.Level_4_description

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6a_provider AS
 ---provider care contacts count
 select 
 'Provider' as Breakdown
 ,OrgIDProv as Level_1
 ,OrgIDProv as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6a' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of care contacts by provider
 from $db_output.MHB_MHS201CareContact
 group by OrgIDProv

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6b_provider AS
 ---provider attended care contacts count
 select 
 'Provider' as Breakdown
 ,OrgIDProv as Level_1
 ,OrgIDProv as Level_1_description
 ,'NULL' as Level_2
 ,'NULL' as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6b' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of care contacts by provider where patient attended
 from $db_output.MHB_MHS201CareContact
 where AttendOrDNACode in ('5','6')
 group by OrgIDProv

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW 6a_provider_teamtype AS
 ---provider and service type care contacts count
 select 
 'Provider; Service or Team Type' as Breakdown
 ,a.OrgIDProv as Level_1
 ,a.OrgIDProv as Level_1_description
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end as Level_2
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6a' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of care contacts by provider
 from $db_output.MHB_MHS201CareContact a
 left join $db_output.MHB_MHS102ServiceTypeReferredTo b on a.UniqServReqID = b.UniqServReqID and a.UniqCareProfTeamID = b.UniqCareProfTeamID and a.OrgIDProv = b.OrgIDProv 
 ---count of activity by provider so joining on UniqServReqID ,UniqCareProfTeamID and OrgIDProv
 left join TeamType c on b.ServTeamTypeRefToMH = c.Team_Type_Code
 group by a.OrgIDProv
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end 
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.6b_provider_teamtype;
 CREATE TABLE         $db_output.6b_provider_teamtype USING DELTA AS
 ---provider and service type attended care contacts count
 select 
 'Provider; Service or Team Type' as Breakdown
 ,a.OrgIDProv as Level_1
 ,a.OrgIDProv as Level_1_description
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end as Level_2
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end as Level_2_description
 ,'NULL' as Level_3
 ,'NULL' as Level_3_description
 ,'NULL' as Level_4
 ,'NULL' as Level_4_description
 ,'6b' as Metric
 ,Count(MHS201UniqID) as Metric_value ---number of care contacts by provider where patient attended
 from $db_output.MHB_MHS201CareContact a
 left join $db_output.MHB_MHS102ServiceTypeReferredTo b on a.UniqServReqID = b.UniqServReqID and a.UniqCareProfTeamID = b.UniqCareProfTeamID and a.OrgIDProv = b.OrgIDProv
 ---count of activity by provider so joining on UniqServReqID ,UniqCareProfTeamID and OrgIDProv
 left join TeamType c on b.ServTeamTypeRefToMH = c.Team_Type_Code
 where AttendOrDNACode in ('5','6')
 group by a.OrgIDProv
 ,case when b.ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then ServTeamTypeRefToMH else 'Unknown' end 
 ,case when ServTeamTypeRefToMH in ('A01','A02','A03','A04','A05','A06','A07','A08','A09','A10','A11','A12','A13','A14','A15'
 			,'A16','A17','A18','A19','A20','B01','B02','C01','C02','C03','C04','C05','C06','C07','C08','C09'
 			,'D01','D02','D03','D04','D05','E01','E02','E03','E04','Z01','Z02') then c.Team_Name else 'Unknown' end