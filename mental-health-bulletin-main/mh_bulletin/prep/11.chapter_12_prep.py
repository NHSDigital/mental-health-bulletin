# Databricks notebook source
 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Cont AS
 ---get all attended direct contacts and indirect contacts in the financial year
 SELECT        c.UniqMonthID
               ,c.OrgIDProv
               ,c.Person_ID
               ,c.UniqServReqID
               ,c.RecordNumber
               ,c.UniqCareContID AS ContID
               ,c.CareContDate AS ContDate
               ,c.ConsMechanismMH
 FROM          $db_output.MHB_MHS201CareContact c
 WHERE         c.AttendOrDNACode IN ('5','6') 
               AND ((c.ConsMechanismMH IN ('01','02','03','04') and c.UNiqMonthID <= 1458) ---v4.1 data
               OR (c.ConsMechanismMH IN ('01','02','04','11') and c.UNiqMonthID > 1458)) ---v5 data
 UNION ALL 
 SELECT        i.UniqMonthID 
               ,i.OrgIDProv
               ,i.Person_ID
               ,i.UniqServReqID
               ,i.RecordNumber
               ,CAST(i.MHS204UniqID AS string) AS ContID
               ,i.IndirectActDate AS ContDate
               ,NULL AS ConsMechanismMH
 FROM          $db_output.MHB_MHS204IndirectActivity i

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW hosp AS
 ---get all hospital spells and add ranking for person, referral and hospital spell combination ordered by latest month in financial year
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
 INNER JOIN  $db_source.mhs001mpi m on h.recordnumber = m.recordnumber and (m.ladistrictauth like 'E%' OR ladistrictauth is null OR ladistrictauth = '')

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Bed AS
 ---get all ward stays and add ranking for person, referral and hospital spell combination ordered by latest month, end date of wardstay and ward stay ID in financial year
 SELECT      w.UniqMonthID
             ,w.Person_ID
             ,w.RecordNumber
             ,w.UniqServReqID
             ,w.UniqHospProvSpellID
             ,w.UniqWardStayID
             ,w.HospitalBedTypeMH
             ,ROW_NUMBER() OVER(PARTITION BY w.Person_ID, w.UniqServReqID, w.UniqHospProvSpellID ORDER BY w.UniqMonthID DESC, w.InactTimeWS DESC, w.EndDateWardStay DESC, w.MHS502UniqID DESC) AS BedRN     
 FROM        $db_output.MHB_MHS502WardStay w

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW Follow AS
 ---get all attended contacts in the financial year unioned with latest person, referral and hospital spell combination in the financial year  
 SELECT      c.Person_ID
             ,c.RecordNumber
             ,c.OrgIdProv
             ,c.ContDate
 FROM        global_temp.Cont c
 WHERE       ((c.ConsMechanismMH IN ('01','02','03','04') and c.UNiqMonthID <= 1458) ---v4.1 data
             OR (c.ConsMechanismMH IN ('01','02','04','11') and c.UNiqMonthID > 1458)) ---v5 data
 UNION ALL
 SELECT      h.Person_ID
             ,h.RecordNumber
             ,h.OrgIdProv
             ,h.StartDateHospProvSpell AS ContDate
 FROM        global_temp.Hosp h
 WHERE       h.HospRN = 1

# COMMAND ----------

 %sql 
 CREATE OR REPLACE GLOBAL TEMP VIEW Onward AS
 ---get all onward referrals in the financial year for all providers
 SELECT DISTINCT   o.Person_ID
                   ,o.UniqServReqID
                   ,o.RecordNumber
                   ,o.OnwardReferDate
                   ,o.OrgIdProv
                   ,CASE 
                       WHEN mp.OrgIDProvider IS NULL THEN NULL
                       WHEN LEFT(o.OrgIDReceiving,1) = '8' THEN o.OrgIDReceiving ---if organisation is independent then use that
                       ELSE LEFT(o.OrgIDReceiving,3) ---else use first 3 characters for receiving organisation
                   END AS OrgIDReceiving
 FROM $db_output.MHB_MHS105OnwardReferral o
 LEFT JOIN (
           SELECT DISTINCT h.OrgIDProvider
           FROM $db_output.MHB_MHS000Header h
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
 GROUP BY           a.Person_ID

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW CCG_LATEST_2months AS
 select distinct    a.Person_ID,
                    CASE WHEN b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
                         WHEN A.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
                         ELSE 'UNKNOWN' END AS IC_Rec_CCG        
 FROM               $db_output.MPI_PROV a
 LEFT JOIN          $db_output.MHB_MHS002GP b 
                    on a.Person_ID = b.Person_ID 
                    and a.UniqMonthID = b.UniqMonthID  
                    and a.recordnumber = b.recordnumber
                    and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
                    and b.EndDateGMPRegistration is null
 INNER JOIN         global_temp.CCG_prep_EIP_2months ccg on a.recordnumber = ccg.recordnumber
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
 LEFT JOIN          $db_output.MHB_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
 WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null)
                    and a.uniqmonthid between '$month_id_start' AND '$month_id_end'

# COMMAND ----------

 %sql 
 CREATE OR REPLACE GLOBAL TEMP VIEW Disch AS
 ---combine discharges and onward referrals with bed type
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
       ,CASE WHEN MethOfDischMHHospProvSpell NOT IN ('4','5') 
             AND DestOfDischHospProvSpell NOT IN ('30','37','38','48','49','50','53','79','84','87') 
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
 LEFT JOIN global_temp.Onward o 
            ON h.Person_ID = o.Person_ID 
            AND h.UniqServReqID = o.UniqServReqID
            AND OnwardReferDate between h.StartDateHospProvSpell and h.DischDateHospProvSpell
 LEFT JOIN global_temp.Bed b 
            ON b.Person_ID = h.Person_ID 
            AND b.UniqServReqID = h.UniqServReqID
            AND b.UniqHospProvSpellID = h.UniqHospProvSpellID 
            AND b.BedRN = 1
 WHERE h.HospRN = 1 
 AND h.DischDateHospProvSpell >= '$rp_startdate' ---discharged after or on start of financial year
 AND h.DischDateHospProvSpell < date_add('$rp_enddate',-3) ---to exclude discharges in the last three days of the financial year

# COMMAND ----------

 %sql 
 CREATE OR REPLACE GLOBAL TEMP VIEW Fup AS
 ---combine discharges and contacts to assess if 72 hour follow up took place
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
       ,CASE WHEN DATEDIFF(c.FirstCont, d.DischDateHospProvSpell)  <= 3 ---follow up was within 72 hours
             THEN 1 ELSE 0 
             END AS FollowedUp3Days
       ,CASE WHEN c.FirstCont IS NULL ---no contact
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
      AND c.ContDate > h.DischDateHospProvSpell ---contacts after discharge only
      AND c.OrgIdProv = h.ResponsibleProv
      GROUP BY     h.Person_ID,  h.UniqServReqID, h.UniqHospProvSpellID
 ) AS c 
 ON d.Person_ID = c.Person_ID 
 AND d.UniqServReqID = c.UniqServReqID 
 AND d.UniqHospProvSpellID = c.UniqHospProvSpellID

# COMMAND ----------

 %sql
 DROP TABLE IF EXISTS $db_output.72h_follow_Master;
 CREATE TABLE         $db_output.72h_follow_Master USING DELTA AS
 ---add geographic levels and demographics to follow up table created above
 SELECT f.UniqMonthID,
        f.person_id,
        f.ResponsibleProv AS ResponsibleProv,       
        COALESCE(ccg.IC_Rec_CCG, 'Unknown') AS CCG,
        COALESCE(mp.STP_code, 'Unknown') AS STP_Code,
        COALESCE(mp.Region_code, 'Unknown') AS Region_Code,
        age_group_lower_chap12,
        age_group_higher_level,
        UpperEthnicity,
        LowerEthnicity,
        case when Der_Gender = '1' then '1'
             when Der_Gender = '2' then '2'
             when Der_Gender = '3' then '3'
             when Der_Gender = '4' then '4'
             when Der_Gender = '9' then '9'
             else 'Unknown' end as Gender,
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
        f.ElgibleDischFlag,
        (CASE WHEN f.DiedBeforeFollowUp = 0 AND f.AcuteBed = 1  THEN f.ElgibleDischFlag ELSE 0 END) AS ElgibleDischFlag_Modified,
        (CASE WHEN f.ElgibleDischFlag = 1 AND f.AcuteBed = 1 AND f.DiedBeforeFollowUp = 0 THEN f.FollowedUp3Days ELSE 0 END) AS FollowedUp3Days
        
 FROM              GLOBAL_TEMP.fup f
 LEFT JOIN         GLOBAL_TEMP.CCG_LATEST_2months ccg ON f.person_id = ccg.person_id 
 INNER JOIN        $db_output.MPI_PROV m ON f.person_id = m.person_id AND f.orgidprov = m.orgidprov 
 LEFT JOIN         $db_output.stp_region_mapping as mp on ccg.IC_Rec_CCG = mp.CCG_code
 LEFT JOIN         $db_output.mha_la la on la.level = m.LADistrictAuth 
 WHERE             DischDateHospProvSpell BETWEEN date_add('$rp_startdate',-4) AND date_add('$rp_enddate',-4)