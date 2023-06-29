# Databricks notebook source
#chapter 1 metrics
ch01_measure_ids = {  
"1a": {
  "freq": "12M", 
  "name": "Number of people in contact with NHS funded secondary mental health, learning disabilities and autism services",
  "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Higher Level); Provider Type','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; Gender; Age Group (Lower Level)','CCG - Registration or Residence; IMD','CCG - Registration or Residence; Service Type Group','Commissioning Region','Commissioning Region; Age Group (Higher Level)','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','Gender; Provider Type','IMD','IMD Quintiles','IMD; Ethnicity (Higher Level)','IMD; Gender; Age Group (Lower Level)','LAD/UA','LAD/UA; Age Group (Higher Level)','Provider','Provider Type','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','Provider; Service Type Group','Service Type as Reported to MHSDS','Service Type as Reported to MHSDS; Age Group (Higher Level)','Service Type as Reported to MHSDS; Ethnicity (Higher Level)','Service Type as Reported to MHSDS; Gender','Service Type as Reported to MHSDS; IMD Quintiles','STP','STP; Age Group (Higher Level)']
},
"1b": {
  "freq": "12M", 
  "name": "Number of people admitted as an inpatient with NHS funded secondary mental health, learning disabilities and autism services",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Higher Level); Provider Type','Age Group (Lower Level)','Age Group (Lower Level); Provider Type','Bed Type as Reported to MHSDS','Bed Type as Reported to MHSDS; Age Group (Higher Level)','Bed Type as Reported to MHSDS; Ethnicity (Higher Level)','Bed Type as Reported to MHSDS; Gender','Bed Type as Reported to MHSDS; IMD Quintiles','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Bed Type Group','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','Commissioning Region; Age Group (Higher Level)','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','Gender; Age Group (Lower Level); Provider Type','Gender; Provider Type','IMD','IMD Quintiles','LAD/UA','LAD/UA; Age Group (Higher Level)','Provider','Provider Type','Provider; Age Group (Higher Level)','Provider; Bed Type Group','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP','STP; Age Group (Higher Level)']
},
"1c": {
  "freq": "12M", 
  "name": "Number of people not admitted as an inpatient while in contact with NHS funded secondary mental health, learning disabilities and autism services",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Higher Level); Provider Type','Age Group (Lower Level)','Age Group (Lower Level); Provider Type','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD','CCG - Registration or Residence; Service Type Group','Commissioning Region','Commissioning Region; Age Group (Higher Level)','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','Gender; Age Group (Lower Level); Provider Type','Gender; Provider Type','IMD','IMD Quintiles','LAD/UA','LAD/UA; Age Group (Higher Level)','Provider','Provider Type','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','Service Type as Reported to MHSDS','Service Type as Reported to MHSDS; Age Group (Higher Level)','Service Type as Reported to MHSDS; Ethnicity (Higher Level)','Service Type as Reported to MHSDS; Gender','Service Type as Reported to MHSDS; IMD Quintiles','STP','STP; Age Group (Higher Level)']
},
"1d": {
  "freq": "12M", 
  "name":  "Proportion of people in contact with NHS funded secondary mental health, learning disabilities and autism services admitted as an inpatient",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Higher Level); Provider Type','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','Commissioning Region','Commissioning Region; Age Group (Higher Level)','England','Gender','Gender; Age Group (Lower Level)','Gender; Provider Type','LAD/UA','LAD/UA; Age Group (Higher Level)','Provider','Provider Type','Provider; Age Group (Higher Level)','STP','STP; Age Group (Higher Level)']
},
"1e": {
  "freq": "12M", 
  "name":  "Proportion of people in contact with NHS funded secondary mental health, learning disabilities and autism services not admitted as an inpatient",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD','CCG - Registration or Residence; Service Type Group','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','Gender; Provider Type','IMD','IMD Quintiles','Provider','Provider Type','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','Service Type as Reported to MHSDS','Service Type as Reported to MHSDS; Age Group (Higher Level)','Service Type as Reported to MHSDS; Ethnicity (Higher Level)','Service Type as Reported to MHSDS; Gender','Service Type as Reported to MHSDS; IMD Quintiles']
},
"1f": {
  "freq": "12M", 
  "name":  "Census population - 2020 Mid-Year Estimate",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','England','Gender','Gender; Age Group (Lower Level)']
},
"1g": {
  "freq": "12M", 
  "name": "Proportion of the population in contact with NHS funded secondary mental health, learning disabilities and autism services",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','England','Gender','Gender; Age Group (Lower Level)']
},
"1h": {
  "freq": "12M", 
  "name": "Number of people in contact with NHS funded secondary mental health, learning disabilities and autism services with a known age, gender and ethnicity",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['England','Ethnicity (Higher Level)','Ethnicity (Lower Level)']
},
"1i": {
  "freq": "12M", 
  "name": "Crude rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['England','Ethnicity (Higher Level)','Ethnicity (Lower Level)']
},
"1j": {
  "freq": "12M", 
  "name": "Standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
  "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Ethnicity (Higher Level)','Ethnicity (Lower Level)']
},
"1k": {
  "freq": "12M", 
  "name": "Confidence interval for the standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Ethnicity (Higher Level)','Ethnicity (Lower Level)']
},
"1m": {
  "freq": "12M", 
  "name": "Census population - 2011",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['England','Ethnicity (Higher Level)','Ethnicity (Lower Level)']
}
}

# COMMAND ----------

#chapter 4 metrics
ch04_measure_ids = {  
"4a": {
  "freq": "12M", 
  "name": "Number of in year bed days",
  "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','Age Group (Lower Level); Provider Type','Bed Type as Reported to MHSDS','Bed Type as Reported to MHSDS; Age Group (Higher Level)','Bed Type as Reported to MHSDS; Ethnicity (Higher Level)','Bed Type as Reported to MHSDS; Gender','Bed Type as Reported to MHSDS; IMD Quintiles','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Bed Type Group','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','Gender; Age Group (Lower Level); Provider Type','Gender; Provider Type','IMD','IMD Quintiles','LAD/UA','Provider','Provider Type','Provider; Age Group (Higher Level)','Provider; Bed Type Group','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
}
}

# COMMAND ----------

#chapter 5 metrics
ch05_measure_ids = {  
"5a": {
  "freq": "12M", 
  "name": "Number of admissions to NHS funded secondary mental health, learning disabilities and autism inpatient services",
  "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','Bed Type as Reported to MHSDS','Bed Type as Reported to MHSDS; Age Group (Higher Level)','Bed Type as Reported to MHSDS; Ethnicity (Higher Level)','Bed Type as Reported to MHSDS; IMD Quintiles','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"5b": {
  "freq": "12M", 
  "name": "Number of discharges from NHS funded secondary mental health, learning disabilities and autism inpatient services",
   "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','Commissioning Region','England','Gender','Gender; Age Group (Lower Level)','LAD/UA','Provider','STP']
},
"5c": {
  "freq": "12M", 
  "name": "Average (mean) number of daily occupied beds in NHS funded secondary mental health, learning disabilities and autism inpatient services",
   "numerator": 1,
  "denominator": 1,
  "related_to": "hospital spells",
  "primary_breakdowns": ['CCG - Registration or Residence','Commissioning Region','England','LAD/UA','Provider','STP']
}
}

# COMMAND ----------

#chapter 6 metrics
ch06_measure_ids = {  
"6a": {
  "freq": "12M", 
  "name": "Number of contacts with secondary mental health, learning disabilities and autism services",
  "numerator": 1,
  "denominator": 0,
  "related_to": "contacts",
  "primary_breakdowns": ['Attendance Code','Attendance Code; Service or Team Type','England','Provider','Provider; Service or Team Type','Service or Team Type']
},
"6b": {
  "freq": "12M", 
  "name": "Proportion of contacts with secondary mental health, learning disabilities and autism services which the patient attended",
   "numerator": 1,
  "denominator": 1,
  "related_to": "contacts",
  "primary_breakdowns": ['England','Provider','Provider; Service or Team Type','Service or Team Type']
},
"6c": {
  "freq": "12M", 
  "name": "Proportion of contacts with secondary mental health, learning disabilities and autism services which the patient did not attend",
   "numerator": 1,
  "denominator": 1,
  "related_to": "contacts",
  "primary_breakdowns": ['England','Service or Team Type']
},
"6d": {
  "freq": "12M", 
  "name": "Proportion of contacts with secondary mental health, learning disabilities and autism services which the provider cancelled",
   "numerator": 1,
  "denominator": 1,
  "related_to": "contacts",
  "primary_breakdowns": ['England','Service or Team Type']
},
"6e": {
  "freq": "12M", 
  "name": "Proportion of contacts with secondary mental health, learning disabilities and autism services with an invalid or missing attendance code",
   "numerator": 1,
  "denominator": 1,
  "related_to": "contacts",
  "primary_breakdowns": ['England','Service or Team Type']
}
}

# COMMAND ----------

#chapter 7 metrics
ch07_measure_ids = {  
"7a": {
  "freq": "12M", 
  "name": "Number of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services",
  "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','CCG - Registration or Residence; Intervention Type','Commissioning Region','Commissioning Region; Intervention Type','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','Intervention Type','Intervention Type; Age Group (Lower Level)','Intervention Type; Gender','Intervention Type; Gender; Age Group (Lower Level)','LAD/UA','LAD/UA; Intervention Type','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','Provider; Intervention Type','STP','STP; Intervention Type']
},
"7b": {
  "freq": "12M", 
  "name": "Number of restrictive interventions in NHS funded secondary mental health, learning disabilities and autism services",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','CCG - Registration or Residence; Intervention Type','Commissioning Region','Commissioning Region; Intervention Type','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','Intervention Type','Intervention Type; Age Group (Lower Level)','Intervention Type; Gender','Intervention Type; Gender; Age Group (Lower Level)','LAD/UA','LAD/UA; Intervention Type','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','Provider; Intervention Type','STP','STP; Intervention Type']
},
"7c": {
  "freq": "12M", 
  "name": "Number of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services whose age, gender and ethnicity are known",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['England','Ethnicity (Higher Level)','Ethnicity (Lower Level)']
},
"7d": {
  "freq": "12M", 
  "name": "Crude rate of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['England','Ethnicity (Higher Level)','Ethnicity (Lower Level)']
},
"7e": {
  "freq": "12M", 
  "name": "Standardised rate of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Ethnicity (Higher Level)','Ethnicity (Lower Level)']
},
"7f": {
  "freq": "12M", 
  "name": "Confidence intervals for the standardised rate of people subject to a restrictive intervention in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population (+/-)",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Ethnicity (Higher Level)','Ethnicity (Lower Level)']
}
}

# COMMAND ----------

#chapter 9 metrics
ch09_measure_ids = {  
"9a": {
  "freq": "12M", 
  "name": "Number of children and young people, receiving at least two contacts (including indirect contacts) in the reporting period and where their first contact occurs before their 18th birthday",
  "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Lower Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"9b": {
  "freq": "12M", 
  "name": "Crude rate of children and young people, receiving at least two contacts (including indirect contacts) in the reporting period and where their first contact occurs before their 18th birthday per 100,000 population aged 0-17",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG - Registration or Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','STP']
}
}

# COMMAND ----------

#chapter 10 metrics
ch10_measure_ids = {  
"10a": {
  "freq": "12M", 
  "name": "Number of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment",
  "numerator": 1,
  "denominator": 0,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"10b": {
  "freq": "12M", 
  "name": "Number of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks",
   "numerator": 1,
  "denominator": 0,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"10c": {
  "freq": "12M", 
  "name": "Proportion of referrals on Early Intervention for Psychosis (EIP) pathway that waited 2 weeks or less to enter treatment",
   "numerator": 1,
  "denominator": 1,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"10d": {
  "freq": "12M", 
  "name": "Number of open referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks",
   "numerator": 1,
  "denominator": 0,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"10e": {
  "freq": "12M", 
  "name": "Number of referrals not on Early Intervention for Psychosis (EIP) pathway, receiving a first contact and assigned a care co-ordinator with any team",
   "numerator": 1,
  "denominator": 0,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"10f": {
  "freq": "12M", 
  "name": "Number of referrals on Early Intervention for Psychosis (EIP) pathway",
   "numerator": 1,
  "denominator": 0,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"10g": {
  "freq": "12M", 
  "name": "Crude rate of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','STP']
},
"10h": {
  "freq": "12M", 
  "name": "Crude rate of referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','STP']
},
"10i": {
  "freq": "12M", 
  "name": "Crude rate of open referrals on Early Intervention for Psychosis (EIP) pathway that entered treatment within 2 weeks per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','STP']
},
"10j": {
  "freq": "12M", 
  "name": "Crude rate of referrals not on Early Intervention for Psychosis (EIP) pathway, receiving a first contact and assigned a care co-ordinator with any team per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','STP']
},
"10k": {
  "freq": "12M", 
  "name": "Crude rate of referrals on Early Intervention for Psychosis (EIP) pathway per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','STP']
}
}

# COMMAND ----------

#chapter 11 metrics
ch11_measure_ids = {  
"11a": {
  "freq": "12M", 
  "name": "Number of people in contact with Specialist Perinatal Mental Health Community Services",
  "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; IMD Quintiles','STP']
},
"11b": {
  "freq": "12M", 
  "name": "Crude rate of people in contact with Specialist Perinatal Mental Health Community Services per 100,000 females",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','England','Ethnicity (Lower Level)']
}
}

# COMMAND ----------

#chapter 12 metrics
ch12_measure_ids = {  
"12a": {
  "freq": "12M", 
  "name": "Number of discharges from adult acute beds eligible for 72 hour follow up",
  "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"12b": {
  "freq": "12M", 
  "name": "Number of discharges from adult acute beds followed up within 72 hours",
   "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"12c": {
  "freq": "12M", 
  "name": "Proportion of discharges from adult acute beds that were followed up within 72 hours",
   "numerator": 1,
  "denominator": 1,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
}
}

# COMMAND ----------

#chapter 13 metrics
ch13_measure_ids = {  
"13a": {
  "freq": "12M", 
  "name": "Number of open referrals to memory services team at the end of the year",
  "numerator": 1,
  "denominator": 0,
  "related_to": "referrals",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"13b": {
  "freq": "12M", 
  "name": "Number of contacts with memory services team",
   "numerator": 1,
  "denominator": 0,
  "related_to": "contacts",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
},
"13c": {
  "freq": "12M", 
  "name": "Number of attended contacts with memory services team",
   "numerator": 1,
  "denominator": 1,
  "related_to": "contacts",
  "primary_breakdowns": ['Age Group (Higher Level)','Age Group (Lower Level)','CCG - Registration or Residence','CCG - Registration or Residence; Age Group (Higher Level)','CCG - Registration or Residence; Ethnicity (Higher Level)','CCG - Registration or Residence; Gender','CCG - Registration or Residence; IMD Quintiles','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD Quintiles','IMD; Age Group (Lower Level)','LAD/UA','Provider','Provider; Age Group (Higher Level)','Provider; Ethnicity (Higher Level)','Provider; Gender','Provider; IMD Quintiles','STP']
}
}

# COMMAND ----------

#chapter 14 metrics
ch14_measure_ids = {  
"14a": {
  "freq": "12M", 
  "name": "Number of people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts within the RP",
  "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"14b": {
  "freq": "12M", 
  "name": "Crude rate of people accessing community mental health services for adults and older adults with serious mental illness who received 2 or more care contacts within the RP per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
}
}

# COMMAND ----------

#chapter 15 metrics
ch15_measure_ids = {  
"15a": {
  "freq": "12M", 
  "name": "Adult and older adult acute admissions in the reporting period",
  "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"15b": {
  "freq": "12M", 
  "name": "Adult and older adult acute admissions for patients with contact in the prior year with mental health services, in the RP",
   "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"15c": {
  "freq": "12M", 
  "name": "Adult and older adult acute admissions for patients with no contact in the prior year with mental health services, in the RP",
   "numerator": 1,
  "denominator": 0,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"15d": {
  "freq": "12M", 
  "name": "Percentage of adult and older adult acute admissions for patients with contact in the prior year with mental health services, in the RP",
   "numerator": 1,
  "denominator": 1,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"15e": {
  "freq": "12M", 
  "name": "Percentage of adult and older adult acute admissions for patients with no contact in the prior year with mental health services, in the RP",
   "numerator": 1,
  "denominator": 1,
  "related_to": "hospital spells",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
}
}

# COMMAND ----------

#chapter 16 metrics
ch16_measure_ids = {  
"16a": {
  "freq": "12M", 
  "name": "The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 60+ days",
  "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"16b": {
  "freq": "12M", 
  "name": "The number of people discharged in the RP from adult acute beds aged 18 to 64 with a length of stay of 90+ days",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"16c": {
  "freq": "12M", 
  "name": "The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 60+ days",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"16d": {
  "freq": "12M",
  "name":  "The number of people discharged in the RP from older adult acute beds aged 65 and over with a length of stay of 90+ days",
  "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"16e": {
  "freq": "12M", 
  "name":  "The number of people discharged in the RP aged 0 to 17 with a length of stay of 60+ days",
  "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"16f": {
  "freq": "12M", 
  "name":  "Census population - 2020 Mid-Year Estimate",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"16g": {
  "freq": "12M", 
  "name": "Proportion of the population in contact with NHS funded secondary mental health, learning disabilities and autism services",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
},
"16h": {
  "freq": "12M", 
  "name": "Number of people in contact with NHS funded secondary mental health, learning disabilities and autism services with a known age, gender and ethnicity",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
},
"16i": {
  "freq": "12M", 
  "name": "Crude rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
},
"16j": {
  "freq": "12M", 
  "name": "Standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
  "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
},
"16k": {
  "freq": "12M", 
  "name": "Confidence interval for the standardised rate of people in contact with NHS funded secondary mental health, learning disabilities and autism services per 100,000 population",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
},
"16l": {
  "freq": "12M", 
  "name": "Census population - 2011",
   "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
}
}

# COMMAND ----------

#chapter 17 metrics
ch17_measure_ids = {  
"17a": {
  "freq": "12M", 
  "name": "Number of children and young people aged under 18 supported through NHS funded mental health with at least one contact",
  "numerator": 1,
  "denominator": 0,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','CCG of Residence; Age Group (Lower Level)','CCG of Residence; Ethnicity (Higher Level)','CCG of Residence; Ethnicity (Lower Level)','CCG of Residence; Gender','CCG of Residence; IMD','Commissioning Region','Commissioning Region; Age Group (Lower Level)','Commissioning Region; Ethnicity (Higher Level)','Commissioning Region; Ethnicity (Lower Level)','Commissioning Region; Gender','Commissioning Region; IMD','England','Ethnicity (Higher Level)','Ethnicity (Higher Level); Age Group (Lower Level)','Ethnicity (Lower Level)','Ethnicity (Lower Level); Age Group (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','IMD; Age Group (Lower Level)','Provider','Provider; Age Group (Lower Level)','Provider; Ethnicity (Higher Level)','Provider; Ethnicity (Lower Level)','Provider; Gender','Provider; IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Ethnicity (Lower Level)','STP; Gender','STP; IMD']
},
"17b": {
  "freq": "12M", 
  "name": "Crude rate of children and young people aged under 18 supported through NHS funded mental health with at least one contact per 100,000 population aged 0-17",
   "numerator": 1,
  "denominator": 1,
  "related_to": "people",
  "primary_breakdowns": ['Age Group (Lower Level)','CCG of Residence','Commissioning Region','England','Ethnicity (Higher Level)','Ethnicity (Lower Level)','Gender','Gender; Age Group (Lower Level)','IMD','STP','STP; Age Group (Lower Level)','STP; Ethnicity (Higher Level)','STP; Gender','STP; IMD']
}
}

# COMMAND ----------

#combine chapter dictionaries into one
measure_metadata = {**ch01_measure_ids, **ch04_measure_ids, **ch05_measure_ids, **ch06_measure_ids, **ch07_measure_ids, **ch09_measure_ids, **ch10_measure_ids, **ch11_measure_ids, **ch12_measure_ids, **ch13_measure_ids, **ch14_measure_ids, **ch15_measure_ids, **ch16_measure_ids, **ch17_measure_ids, }