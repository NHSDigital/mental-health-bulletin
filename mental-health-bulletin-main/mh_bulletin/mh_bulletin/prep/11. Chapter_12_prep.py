# Databricks notebook source
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

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Cont AS
 SELECT        c.UniqMonthID
               ,c.OrgIDProv
               ,c.Person_ID
               ,c.UniqServReqID
               ,c.RecordNumber
               ,c.UniqCareContID AS ContID
               ,c.CareContDate AS ContDate
               ,c.ConsMechanismMH
 FROM          $db_output.MHB_MHS201CareContact c
 WHERE         
 -- c.UniqMonthID between '$start_month_id' and '$end_month_id' 
 --               AND 
               c.AttendOrDNACode IN ('5','6') 
               AND ((c.ConsMechanismMH IN ('01','02','03','04') and c.UNiqMonthID <= 1458) ---v4.1 data
                   or
                    (c.ConsMechanismMH IN ('01','02','04','11') and c.UNiqMonthID > 1458)) ---v5 data --attended contacts as per DMS methodology
  
 union all
  
 SELECT        i.UniqMonthID 
               ,i.OrgIDProv
               ,i.Person_ID
               ,i.UniqServReqID
               ,i.RecordNumber
               ,CAST(i.MHS204UniqID AS string) AS ContID
               ,i.IndirectActDate AS ContDate
               ,NULL AS ConsMechanismMH
 FROM          $db_output.MHB_MHS204IndirectActivity i
 -- WHERE         i.UniqMonthID between '$start_month_id' and '$end_month_id' --commented out as the MHB asset tables already have this filter

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW hosp AS
 SELECT      h.uniqmonthid
             ,h.orgidprov
             ,h.person_id
             ,h.uniqservreqid
             ,h.uniqhospprovspellID
             ,h.recordnumber
             ,h.startdatehospprovspell
             ,h.DischDateHospProvSpell
             ,h.inacttimehps
             ,h.MethOfDischMHHospProvSpell
             ,h.DestOfDischHospProvSpell
             ,m.PersDeathDate              
             ,row_number() over(partition by h.person_id, h.uniqservreqid, h.uniqhospprovspellID order by h.uniqmonthid desc) as HospRN
 FROM        $db_output.MHB_mhs501hospprovspell h
 INNER JOIN  $db_source.mhs001mpi m on h.recordnumber = m.recordnumber and (m.ladistrictauth like 'E%' OR ladistrictauth is null OR ladistrictauth = '') ---can't use MPI_PROV here as we don't want to exclude historic records
 ---WHERE       h.uniqmonthid between '$start_month_id' and '$end_month_id' --commented out as the MHB asset tables already have this filter

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Bed AS
 SELECT      w.UniqMonthID
             ,w.Person_ID
             ,w.RecordNumber
             ,w.UniqServReqID
             ,w.UniqHospProvSpellID
             ,w.UniqWardStayID
             ,w.HospitalBedTypeMH
             ,ROW_NUMBER() OVER(PARTITION BY w.Person_ID, w.UniqServReqID, w.UniqHospProvSpellID ORDER BY w.UniqMonthID DESC, w.InactTimeWS DESC, w.EndDateWardStay DESC, w.MHS502UniqID DESC) AS BedRN     
 FROM        $db_output.MHB_MHS502WardStay w
 -- WHERE       w.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id' --commented out as the MHB asset tables already have this filter

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Follow AS
 SELECT      c.Person_ID
             ,c.RecordNumber
             ,c.OrgIdProv
             ,c.ContDate
 FROM        global_temp.Cont c
 WHERE       ((c.ConsMechanismMH IN ('01','02','03','04') and c.UNiqMonthID <= 1458) ---v4.1 data
                   or
                    (c.ConsMechanismMH IN ('01','02','04','11') and c.UNiqMonthID > 1458)) ---v5 data --as per Carl and DMS methodology
  
 Union all
  
 SELECT      h.Person_ID
             ,h.RecordNumber
             ,h.OrgIdProv
             ,h.StartDateHospProvSpell AS ContDate
 FROM        global_temp.Hosp h
 WHERE       h.HospRN = 1

# COMMAND ----------

 %sql 
 CREATE OR REPLACE GLOBAL TEMP VIEW Onward AS
 SELECT DISTINCT   o.Person_ID
                   ,o.UniqServReqID
                   ,o.RecordNumber
                   ,o.OnwardReferDate
                   ,o.OrgIdProv
                   ,CASE 
                       WHEN mp.OrgIDProvider IS NULL THEN NULL
                       WHEN LEFT(o.OrgIDReceiving,1) = '8' THEN o.OrgIDReceiving 
                       ELSE LEFT(o.OrgIDReceiving,3) 
                   END AS OrgIDReceiving
 FROM              $db_output.MHB_MHS105OnwardReferral o
 LEFT JOIN 
           (
           SELECT DISTINCT h.OrgIDProvider
           FROM             $db_output.MHB_MHS000Header h
           ---WHERE            h.UniqMonthID BETWEEN '$start_month_id' and '$end_month_id'
           ) mp 
           ON CASE WHEN LEFT(o.OrgIDReceiving,1) = '8' THEN o.OrgIDReceiving ELSE LEFT(o.OrgIDReceiving,3) END = mp.OrgIDProvider
 WHERE     CASE WHEN LEFT(o.OrgIDReceiving,1) = '8' THEN OrgIDProv ELSE LEFT(o.OrgIDReceiving,3) END <> o.OrgIdProv

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMP VIEW CCG_prep_EIP_2months AS
 SElECT DISTINCT    a.Person_ID
                    ,max(a.RecordNumber) as recordnumber    
 FROM               $db_output.MPI_PROV a
 LEFT JOIN          $db_output.MHB_MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID
                    and a.recordnumber = b.recordnumber
                    and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
 --                    and a.uniqmonthid between '$start_month_id' AND '$end_month_id'
 GROUP BY           a.Person_ID

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CCG_LATEST_2months AS
 select distinct    a.Person_ID,
                    CASE WHEN b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
                         WHEN A.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
                         ELSE 'Unknown' END AS IC_Rec_CCG        
 FROM               $db_output.MPI_PROV a
 LEFT JOIN          $db_output.MHB_MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                    --and b.OrgIDGPPrac <> '-1' 
                    and b.EndDateGMPRegistration is null
 INNER JOIN         global_temp.CCG_prep_EIP_2months ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    and a.uniqmonthid between '$start_month_id' AND '$end_month_id'

# COMMAND ----------

 %sql 
 CREATE OR REPLACE GLOBAL TEMP VIEW Disch AS
 SELECT DISTINCT       
        h.UniqMonthID
       ,h.OrgIDProv
       ,COALESCE(o.OrgIDReceiving,h.OrgIDProv) AS ResponsibleProv
       ,h.Person_ID
       ,h.UniqServReqID
       ,h.UniqHospProvSpellID
       ,h.RecordNumber
       ,h.DischDateHospProvSpell
       ,CASE WHEN h.DischDateHospProvSpell between date_add('$rp_startdate',-4) and date_add('$rp_enddate',-4)  
             THEN 1 ELSE 0 END AS DischFlag
       ,CASE WHEN h.InactTimeHPS < '$rp_enddate' 
             THEN 1 ELSE 0 END AS InactiveFlag
       ,CASE WHEN ((h.UniqMonthID > 1464 and (MethOfDischMHHospProvSpell NOT IN ('4','5') OR MethOfDischMHHospProvSpell IS NULL)) OR (h.UniqMonthID <= 1464 AND MethOfDischMHHospProvSpell NOT IN ('4','5')))
             AND ((DestOfDischHospProvSpell is not null AND vc.Measure is null) OR DestOfDischHospProvSpell IS NULL) --Updated to include null discharge destinations.
             AND DischDateHospProvSpell between date_add('$rp_startdate',-4) and date_add('$rp_enddate',-4)        
             THEN 1 ELSE 0 
             END AS ElgibleDischFlag
       ,CASE WHEN b.HospitalBedTypeMH IN ('10', '11', '12', '16', '17', '18') 
             THEN 1 ELSE 0 END AS AcuteBed
       ,CASE WHEN b.HospitalBedTypeMH IN ('13', '14', '15', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34') 
             THEN 1 ELSE 0 END AS OtherBed
       ,CASE WHEN b.HospitalBedTypeMH NOT IN ('10', '11', '12', '16', '17', '18', '13', '14', '15', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34') 
             THEN 1 ELSE 0 END AS InvalidBed
       ,CASE WHEN b.HospitalBedTypeMH IS NULL 
             THEN 1 ELSE 0 END AS MissingBed
       ,CASE WHEN DATEDIFF(h.PersDeathDate, h.DischDateHospProvSpell) <= 3  
             THEN 1 ELSE 0 END AS DiedBeforeFollowUp
       ,CASE WHEN DestOfDischHospProvSpell IN ('37', '38') 
             THEN 1 ELSE 0 END AS PrisonCourtDischarge
        
 FROM  global_temp.Hosp h
 LEFT  JOIN global_temp.Onward o 
            ON h.Person_ID = o.Person_ID 
            AND h.UniqServReqID = o.UniqServReqID
            AND OnwardReferDate between h.StartDateHospProvSpell and h.DischDateHospProvSpell
 LEFT  JOIN global_temp.Bed b 
            ON b.Person_ID = h.Person_ID 
            AND b.UniqServReqID = h.UniqServReqID
            AND b.UniqHospProvSpellID = h.UniqHospProvSpellID 
            AND b.BedRN = 1
 LEFT JOIN $db_output.validcodes1 as vc
    ON vc.tablename = 'mhs501hospprovspell' 
    and vc.field = 'DestOfDischHospProvSpell' 
    and vc.Measure = '72HOURS' 
    and vc.type = 'exclude' 
    and h.DestOfDischHospProvSpell = vc.ValidValue 
    and h.UniqMonthID >= vc.FirstMonth 
    and (vc.LastMonth is null or h.UniqMonthID <= vc.LastMonth)       
             
  LEFT JOIN $db_output.validcodes1 as vc1
    ON vc1.tablename = 'mhs501hospprovspell' 
    and vc1.field = 'DestOfDischHospProvSpell' 
    and vc1.Measure = '72HOURS' 
    and vc1.type = 'include' 
    and h.DestOfDischHospProvSpell = vc1.ValidValue 
    and h.UniqMonthID >= vc1.FirstMonth 
    and (vc1.LastMonth is null or h.UniqMonthID <= vc1.LastMonth)  
 WHERE h.HospRN = 1 AND h.DischDateHospProvSpell >= '$rp_startdate' AND h.DischDateHospProvSpell < date_add('$rp_enddate',-3) -- to exclude discharges in the last three days of the month (this was in Carl's code but not in DMS methodology)    

# COMMAND ----------

 %sql
  
 CREATE OR REPLACE GLOBAL TEMP VIEW Fup AS
 SELECT d.UniqMonthID
       ,d.OrgIDProv
       ,CASE WHEN d.ElgibleDischFlag = 1 AND AcuteBed = 1 
             THEN d.ResponsibleProv 
             ELSE d.OrgIDProv 
             END AS ResponsibleProv
       ,d.Person_ID
       ,d.UniqServReqID
       ,d.UniqHospProvSpellID
       ,d.RecordNumber
       ,d.DischDateHospProvSpell
       ,d.DischFlag
       ,d.InactiveFlag
       ,d.ElgibleDischFlag
       ,d.AcuteBed
       ,d.OtherBed
       ,d.InvalidBed
       ,d.MissingBed
       ,d.DiedBeforeFollowUp
       ,d.PrisonCourtDischarge
       ,c.FirstCont
       ,DATEDIFF(c.FirstCont, d.DischDateHospProvSpell) as diff
       ,CASE WHEN DATEDIFF(c.FirstCont, d.DischDateHospProvSpell)  <= 3
             THEN 1 ELSE 0 
             END AS FollowedUp3Days
       ,CASE WHEN c.FirstCont IS NULL 
             THEN 1 ELSE 0 
             END AS NoFollowUp
 FROM  global_temp.Disch d
 LEFT  JOIN
 (
    SELECT       h.Person_ID
                 ,h.UniqServReqID
                 ,h.UniqHospProvSpellID
                 ,MIN(c.ContDate) AS FirstCont
  
    FROM         global_temp.Disch h
    INNER JOIN 
    (
      SELECT       
      c.Person_ID
      ,c.OrgIdProv
      ,c.ContDate
      FROM         global_temp.Follow c) c 
      ON c.Person_ID = h.Person_ID
      AND c.ContDate > h.DischDateHospProvSpell 
      AND c.OrgIdProv = h.ResponsibleProv
      GROUP BY     h.Person_ID,  h.UniqServReqID, h.UniqHospProvSpellID
 ) AS c 
 ON d.Person_ID = c.Person_ID 
 AND d.UniqServReqID = c.UniqServReqID 
 AND d.UniqHospProvSpellID = c.UniqHospProvSpellID

# COMMAND ----------

 %run ../mhsds_functions

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.72h_follow_Master;
 CREATE TABLE         $db_output.72h_follow_Master USING DELTA AS
 
 SELECT f.UniqMonthID,
        f.person_id,
        f.ResponsibleProv AS OrgIDProv,
        o.Name as Provider_Name,
        get_provider_type_code(f.ResponsibleProv) as ProvTypeCode,
        get_provider_type_name(f.ResponsibleProv) as ProvTypeName,
        COALESCE(mp.CCG_CODE,'Unknown') AS CCG_CODE,
        COALESCE(mp.CCG_NAME, 'Unknown') as CCG_NAME,
        COALESCE(mp.STP_CODE, 'Unknown') as STP_CODE,
        COALESCE(mp.STP_NAME, 'Unknown') as STP_NAME, 
        COALESCE(mp.REGION_CODE, 'Unknown') as REGION_CODE,
        COALESCE(mp.REGION_NAME, 'Unknown') as REGION_NAME,
 --        age_group_lower_chap12,
        age_group_lower_common,
        age_group_higher_level,
        UpperEthnicity,
        LowerEthnicityCode,
        LowerEthnicityName,
        m.Der_Gender,
        g.Der_Gender_Desc,
        IMD_Quintile,
        IMD_Decile,
        case when LEFT(m.LADistrictAuth,1) = 'S' then 'S'
                           when LEFT(m.LADistrictAuth,1) = 'N' then 'N'
                           when LEFT(m.LADistrictAuth,1) = 'W' then 'W'
                           when m.LADistrictAuth = 'L99999999' then 'L99999999'
                           when m.LADistrictAuth = 'M99999999' then 'M99999999'
                           when m.LADistrictAuth = 'X99999998' then 'X99999998'
                           when la.level is null then 'Unknown'
                           when m.LADistrictAuth = '' then 'Unknown'
                           else la.level end as LADistrictAuth,
        la.level_description as LADistrictAuthName,
        f.ElgibleDischFlag,
        (CASE WHEN f.DiedBeforeFollowUp = 0 AND f.AcuteBed = 1  THEN f.ElgibleDischFlag ELSE 0 END) AS ElgibleDischFlag_Modified,
        (CASE WHEN f.ElgibleDischFlag = 1 AND f.AcuteBed = 1 AND f.DiedBeforeFollowUp = 0 THEN f.FollowedUp3Days ELSE 0 END) AS FollowedUp3Days
        
 FROM              GLOBAL_TEMP.fup f
 LEFT JOIN         GLOBAL_TEMP.CCG_LATEST_2months ccg ON f.person_id = ccg.person_id 
 INNER JOIN        $db_output.MPI_PROV m ON f.person_id = m.person_id AND f.orgidprov = m.orgidprov --adding in bulletin demographic breakdowns --inner joined to get latest demographic data only for a person per provider
 LEFT JOIN         $db_output.STP_Region_mapping as mp on ccg.IC_Rec_CCG = mp.CCG_code
 LEFT JOIN         $db_output.la la on la.level = case when LEFT(m.LADistrictAuth,1) = 'S' then 'S'
                                         when LEFT(m.LADistrictAuth,1) = 'N' then 'N'
                                         when LEFT(m.LADistrictAuth,1) = 'W' then 'W'
                                         when m.LADistrictAuth = 'L99999999' then 'L99999999'
                                         when m.LADistrictAuth = 'M99999999' then 'M99999999'
                                         when m.LADistrictAuth = 'X99999998' then 'X99999998'
                                         when m.LADistrictAuth = '' then 'Unknown'
                                         when m.LADistrictAuth is not null then m.LADistrictAuth
                                         else 'Unknown' end 
 LEFT JOIN         $db_output.Der_Gender g on m.Der_Gender = g.Der_Gender
 LEFT JOIN         $db_output.MHB_ORG_DAILY o on f.ResponsibleProv = o.ORG_CODE
 WHERE             DischDateHospProvSpell BETWEEN date_add('$rp_startdate',-4) AND date_add('$rp_enddate',-4)

# COMMAND ----------

 %sql
 OPTIMIZE $db_output.72h_follow_Master