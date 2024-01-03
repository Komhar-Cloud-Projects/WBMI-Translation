WITH
SQ_claimant_coverage_detail_Type_Bureau AS (
	SELECT CCD.claimant_cov_det_id,
	       CCD.claimant_cov_det_ak_id,
	       CCD.claim_party_occurrence_ak_id,
	       CCD.loc_unit_num,
	       CCD.sub_loc_unit_num,
	       CCD.ins_line,
	       CCD.risk_unit_grp,
	       CCD.risk_unit_grp_seq_num,
	       CCD.risk_unit,
	       CCD.risk_unit_seq_num,
	       CCD.major_peril_code,
	       CCD.major_peril_seq,
	       CCD.pms_type_exposure,
	       CCD.claimant_cov_eff_date,
	       CCD.risk_type_ind,
	       CCD.source_sys_id 
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail  CCD 
	WHERE  CCD.pms_type_bureau_code = 'N/A'
	/*AND CCD.source_sys_id='EXCEED'*/
),
EXP_Values AS (
	SELECT
	claimant_cov_det_id,
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	loc_unit_num,
	-- *INF*: RTRIM(loc_unit_num)
	RTRIM(loc_unit_num) AS loc_unit_num_out,
	sub_loc_unit_num,
	-- *INF*: RTRIM(sub_loc_unit_num)
	RTRIM(sub_loc_unit_num) AS sub_loc_unit_num_out,
	ins_line,
	-- *INF*: RTRIM(ins_line)
	RTRIM(ins_line) AS ins_line1,
	risk_unit_grp,
	-- *INF*: RTRIM(risk_unit_grp)
	RTRIM(risk_unit_grp) AS risk_unit_grp1,
	risk_unit_grp_seq_num,
	-- *INF*: RTRIM(risk_unit_grp_seq_num)
	RTRIM(risk_unit_grp_seq_num) AS risk_unit_grp_seq_num1,
	risk_unit,
	-- *INF*: RTRIM(SUBSTR(risk_unit,1,3))
	-- 
	-- ---RPAD(RTRIM(risk_unit),6,'0')
	-- 
	-- 
	-- ---RTRIM(risk_unit)
	RTRIM(SUBSTR(risk_unit, 1, 3)) AS risk_unit_out,
	risk_unit_seq_num,
	-- *INF*: RTRIM(risk_unit_seq_num)
	RTRIM(risk_unit_seq_num) AS risk_unit_seq_num1,
	major_peril_code,
	-- *INF*: RTRIM(major_peril_code)
	RTRIM(major_peril_code) AS major_peril_code_out,
	major_peril_seq,
	-- *INF*: RTRIM(major_peril_seq)
	RTRIM(major_peril_seq) AS major_peril_seq1,
	pms_type_exposure,
	claimant_cov_eff_date,
	risk_type_ind,
	-- *INF*: LTRIM(RTRIM(risk_unit_seq_num)) || LTRIM(RTRIM(risk_type_ind))
	LTRIM(RTRIM(risk_unit_seq_num)) || LTRIM(RTRIM(risk_type_ind)) AS risk_unit_seq_num_out,
	source_sys_id
	FROM SQ_claimant_coverage_detail_Type_Bureau
),
LKP_Claim_Party_Occurrence AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	claim_occurrence_key,
	pol_ak_id
	FROM (
		SELECT CPO.claim_occurrence_ak_id       AS claim_occurrence_ak_id,
		       RTRIM(CO.claim_occurrence_key)   AS claim_occurrence_key,
		       CPO.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
		       CO.claim_occurrence_type_code    AS claim_occurrence_type_code,
		       P.pol_ak_id	                                             AS pol_ak_id
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO, V2.policy P
		WHERE  CPO.claim_occurrence_ak_id = CO.claim_occurrence_ak_id
		       AND CO.pol_key_ak_id = P.pol_ak_id
		       AND CO.crrnt_snpsht_flag = 1
		       AND CPO.crrnt_snpsht_flag = 1
		       AND P.crrnt_snpsht_flag = 1
		       AND CPO.claim_party_role_code in ('CLMT','CMT')
		ORDER  BY claim_occurrence_key ---
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_party_occurrence_ak_id DESC) = 1
),
LKP_V2_Coverage AS (
	SELECT
	pol_ak_id,
	ins_line,
	loc_unit_num,
	sub_loc_unit_num,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq_num,
	cov_eff_date,
	type_bureau_code
	FROM (
		SELECT  DISTINCT 
		       C.type_bureau_code                 AS type_bureau_code,
		       C.pol_ak_id                                   AS pol_ak_id,
		       RTRIM(C.ins_line)                       AS ins_line,
			 CASE  C.loc_unit_num  WHEN 'N/A' THEN '0000' ELSE RTRIM(C.loc_unit_num)  END AS loc_unit_num,
		       CASE  C.sub_loc_unit_num  WHEN 'N/A' THEN '000' ELSE RTRIM(C.sub_loc_unit_num)   END AS sub_loc_unit_num,
			 RTRIM(C.risk_unit_grp)             AS risk_unit_grp,
		       RTRIM(C.risk_unit_grp_seq_num) AS risk_unit_grp_seq_num,
		       RTRIM(SUBSTRING(C.risk_unit,1,3)) AS risk_unit,
		       CASE C.risk_unit_seq_num   WHEN 'N/A' THEN '0' ELSE  SUBSTRING(C.risk_unit_seq_num,1,1)
		       END                                                    AS  risk_unit_seq_num,
		       RTRIM(C.major_peril_code)      AS major_peril_code,
		       RTRIM(C.major_peril_seq_num)  AS major_peril_seq_num,
		       C.cov_eff_date                               AS cov_eff_date
		FROM   
		       @{pipeline().parameters.SOURCE_TABLE_OWNER}.Coverage C,
		       @{pipeline().parameters.SOURCE_TABLE_OWNER}.Policy P,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Occurrence CO,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Party_Occurrence CPO,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.Claimant_Coverage_Detail CCD
		WHERE  C.pol_ak_id = P.pol_ak_id
		       AND P.pol_key = CO.pol_key
		       AND CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id
		       AND CPO.claim_party_occurrence_ak_id = CCD.claim_party_occurrence_ak_id
		       AND C.crrnt_snpsht_flag = 1
		       AND P.crrnt_snpsht_flag = 1
		       AND CO.crrnt_snpsht_flag = 1 
		       AND CPO.crrnt_snpsht_flag =1 
		       AND CCD.pms_type_bureau_code = 'N/A'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq_num ORDER BY pol_ak_id DESC) = 1
),
EXP_LKP_Values AS (
	SELECT
	EXP_Values.claimant_cov_det_id,
	EXP_Values.claimant_cov_det_ak_id,
	LKP_V2_Coverage.type_bureau_code,
	-- *INF*: IIF(ISNULL(type_bureau_code),'N/A',type_bureau_code)
	IFF(type_bureau_code IS NULL, 'N/A', type_bureau_code) AS type_bureau_code_out,
	LKP_V2_Coverage.pol_ak_id,
	LKP_V2_Coverage.ins_line,
	LKP_V2_Coverage.loc_unit_num,
	LKP_V2_Coverage.sub_loc_unit_num,
	LKP_V2_Coverage.risk_unit_grp,
	LKP_V2_Coverage.risk_unit_grp_seq_num,
	LKP_V2_Coverage.risk_unit,
	LKP_V2_Coverage.risk_unit_seq_num,
	LKP_V2_Coverage.major_peril_code,
	LKP_V2_Coverage.major_peril_seq_num,
	LKP_V2_Coverage.cov_eff_date
	FROM EXP_Values
	LEFT JOIN LKP_V2_Coverage
	ON LKP_V2_Coverage.pol_ak_id = LKP_Claim_Party_Occurrence.pol_ak_id AND LKP_V2_Coverage.ins_line = EXP_Values.ins_line1 AND LKP_V2_Coverage.loc_unit_num = EXP_Values.loc_unit_num_out AND LKP_V2_Coverage.sub_loc_unit_num = EXP_Values.sub_loc_unit_num_out AND LKP_V2_Coverage.risk_unit_grp = EXP_Values.risk_unit_grp1 AND LKP_V2_Coverage.risk_unit_grp_seq_num = EXP_Values.risk_unit_grp_seq_num1 AND LKP_V2_Coverage.risk_unit = EXP_Values.risk_unit_out AND LKP_V2_Coverage.risk_unit_seq_num = EXP_Values.risk_unit_seq_num1 AND LKP_V2_Coverage.major_peril_code = EXP_Values.major_peril_code_out AND LKP_V2_Coverage.major_peril_seq_num = EXP_Values.major_peril_seq1
),
LKP_sup_type_bureau_code AS (
	SELECT
	sup_type_bureau_code_id,
	type_bureau_code
	FROM (
		SELECT 
			sup_type_bureau_code_id,
			type_bureau_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_type_bureau_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_bureau_code ORDER BY sup_type_bureau_code_id) = 1
),
FIL_Invalid_Updates AS (
	SELECT
	EXP_LKP_Values.claimant_cov_det_id, 
	EXP_LKP_Values.type_bureau_code_out, 
	LKP_sup_type_bureau_code.sup_type_bureau_code_id
	FROM EXP_LKP_Values
	LEFT JOIN LKP_sup_type_bureau_code
	ON LKP_sup_type_bureau_code.type_bureau_code = EXP_LKP_Values.type_bureau_code_out
	WHERE IIF(type_bureau_code_out = 'N/A', FALSE, TRUE)
),
UPD_Type_Bureau AS (
	SELECT
	claimant_cov_det_id, 
	type_bureau_code_out, 
	sup_type_bureau_code_id
	FROM FIL_Invalid_Updates
),
claimant_coverage_detail_Update AS (
	MERGE INTO claimant_coverage_detail AS T
	USING UPD_Type_Bureau AS S
	ON T.claimant_cov_det_id = S.claimant_cov_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.pms_type_bureau_code = S.type_bureau_code_out, T.SupTypeBureauCodeID = S.sup_type_bureau_code_id
),
SQ_claimant_coverage_detail_Stat_Cvg AS (
	with AKID
	AS
	(
	select *
	from (Select SC.StatisticalCoverageAKID as StatisticalCoverageAKID, 
	SC.InsuranceReferenceLineOfBusinessAKID as InsuranceReferenceLineOfBusinessAKID, 
	PC.PolicyAKID as PolicyAKID, 
	SC.ProductAKID as ProductAKID ,
	PC.InsuranceLine as InsuranceLine, 
	(CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END) as LocationNumber,
	SC.MajorPerilCode as MajorPerilCode,
	SC.MajorPerilSequenceNumber as MajorPerilSequenceNumber,
	SC.RiskUnit as RiskUnit,
	(CASE WHEN SC.RiskUnitSequenceNumber = '0' then 'N/A' ELSE SC.RiskUnitSequenceNumber END) as RiskUnitSequenceNumber,
	SC.RiskUnitGroup as RiskUnitGroup, 
	SC.RiskUnitGroupSequenceNumber as RiskUnitGroupSequenceNumber , 
	(CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END) as SubLocationUnitNumber,
	PC.TypeBureauCode as TypeBureauCode,
	SC.StatisticalCoverageEffectiveDate as StatisticalCoverageEffectiveDate,
	RANK()over(PARTITION BY 
	PC.PolicyAKID,
	PC.InsuranceLine,
	(CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END),
	(CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END),
	SC.RiskUnitGroup,
	SC.RiskUnitGroupSequenceNumber,
	SC.RiskUnit,
	SC.RiskUnitSequenceNumber,
	SC.MajorPerilCode,
	SC.MajorPerilSequenceNumber ,
	PC.TypeBureauCode,
	SC.StatisticalCoverageEffectiveDate
	order by SC.StatisticalCoverageEffectiveDate desc,CASE WHEN SC.ReinsuranceSectionCode ='2' THEN '2' ELSE '1' END asc, SC.StatisticalCoverageAKID desc) as rn,
	SC.CoverageGUID
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC ,
	@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC,
	@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL,
	V2.policy p
	WHERE SC.PolicyCoverageAKID = PC.PolicyCoverageAKID 
	AND PC.RiskLocationAKID = RL.RiskLocationAKID  
	AND  PC.PolicyAKID = p.pol_ak_id 
	/*AND SC.CurrentSnapshotFlag=1 AND PC.CurrentSnapshotFlag=1 AND RL.CurrentSnapshotFlag=1*/ AND P.crrnt_snpsht_flag=1 
	AND P.source_sys_id='PMS'
	AND  EXISTS (SELECT DISTINCT pol_key_ak_id from dbo.claim_occurrence 
	where crrnt_snpsht_flag = 1 AND PC.PolicyAKID= pol_key_ak_id ))a
	where rn=1
	)
	SELECT 
	claimant_coverage_detail.claimant_cov_det_id, 
	AKID.InsuranceReferenceLineOfBusinessAKID,
	AKID.ProductAKID,
	AKID.StatisticalCoverageAKID,
	AKID.CoverageGUID
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
	ON claim_party_occurrence.claim_party_occurrence_ak_id=claimant_coverage_detail.claim_party_occurrence_ak_id 
	/*and claim_party_occurrence.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'*/ and claim_party_occurrence.crrnt_snpsht_flag=1
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence
	ON claim_occurrence.claim_occurrence_ak_id=claim_party_occurrence.claim_occurrence_ak_id
	/*and claim_occurrence.source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'*/ and claim_occurrence.crrnt_snpsht_flag=1
	LEFT OUTER join AKID
	ON claim_occurrence.pol_key_ak_id=AKID.PolicyAKID
	and LTRIM(RTRIM(claimant_coverage_detail.ins_line))=AKID.InsuranceLine
	and LTRIM(RTRIM(claimant_coverage_detail.loc_unit_num))=AKID.LocationNumber
	and LTRIM(RTRIM(claimant_coverage_detail.sub_loc_unit_num))=AKID.SubLocationUnitNumber
	and LTRIM(RTRIM(claimant_coverage_detail.risk_unit_grp))=AKID.RiskUnitGroup
	and LTRIM(RTRIM(claimant_coverage_detail.risk_unit_grp_seq_num))=AKID.RiskUnitGroupSequenceNumber
	and claim_occurrence.claim_loss_Date >= AKID.StatisticalCoverageEffectiveDate
	/* validate that the Claim Loss Date is after the statisticalcoverageeffectivedate */
	
	and rtrim(claimant_coverage_detail.risk_unit)=AKID.RiskUnit
	
	and (case when LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num))='0' 
	AND LTRIM(RTRIM(claimant_coverage_detail.ins_line))='WC' then '00'
	 when LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num)) in ('0') 
	 AND LTRIM(RTRIM(claimant_coverage_detail.ins_line))<>'WC' 
	 AND LTRIM(RTRIM(claimant_coverage_detail.risk_type_ind))='N/A' then 'N/A'
	 when LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num)) in ('0','1','2','3','4','8') 
	 and LTRIM(RTRIM(claimant_coverage_detail.ins_line))='GL' 
	 then LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num))+LTRIM(RTRIM(claimant_coverage_detail.risk_type_ind))
	 else LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num)) end=AKID.RiskUnitSequenceNumber
	or AKID.RiskUnitSequenceNumber = 'N/A' or AKID.RiskUnitSequenceNumber = '0O'
	or SUBSTRING(case when LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num))='0'
	 AND LTRIM(RTRIM(claimant_coverage_detail.ins_line))='WC' then '00'
	 when LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num)) in ('0') 
	 AND LTRIM(RTRIM(claimant_coverage_detail.ins_line))<>'WC' AND LTRIM(RTRIM(claimant_coverage_detail.risk_type_ind))='N/A' then 'N/A'
	 when LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num)) in ('0','1','2','3','4','8')
	  and LTRIM(RTRIM(claimant_coverage_detail.ins_line))='GL' 
	  then LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num))+LTRIM(RTRIM(claimant_coverage_detail.risk_type_ind))
	 else LTRIM(RTRIM(claimant_coverage_detail.risk_unit_seq_num)) end,1,1) = SUBSTRING(AKID.RiskUnitSequenceNumber,1,1))
	and LTRIM(RTRIM(claimant_coverage_detail.major_peril_code))=AKID.MajorPerilCode
	and LTRIM(RTRIM(claimant_coverage_detail.major_peril_seq))=AKID.MajorPerilSequenceNumber
	and case when claimant_coverage_detail.pms_type_bureau_code is null then 'N/A' 
	else claimant_coverage_detail.pms_type_bureau_code end=AKID.TypeBureauCode
	where /*claimant_coverage_detail.source_sys_id  = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	and*/ claimant_coverage_detail.StatisticalCoverageAKID = -1
	and claimant_coverage_detail.PolicySourceID in ('PMS','ESU')
	ORDER BY claimant_coverage_detail.claimant_cov_det_id,AKID.StatisticalCoverageAKID
),
EXP_EXP_Trim_Expression AS (
	SELECT
	claimant_cov_det_id,
	InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	ProductAKId AS i_ProductAKId,
	StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	CoverageGUID AS i_CoverageGUID,
	-- *INF*: IIF(ISNULL(i_CoverageGUID), 'N/A',i_CoverageGUID)
	IFF(i_CoverageGUID IS NULL, 'N/A', i_CoverageGUID) AS o_CoverageGUID,
	-- *INF*: IIF(ISNULL(i_InsuranceReferenceLineOfBusinessAKId), -1,i_InsuranceReferenceLineOfBusinessAKId)
	IFF(i_InsuranceReferenceLineOfBusinessAKId IS NULL, - 1, i_InsuranceReferenceLineOfBusinessAKId) AS o_InsuranceReferenceLineOfBusinessAKId,
	-- *INF*: IIF(ISNULL(i_ProductAKId), -1,i_ProductAKId)
	IFF(i_ProductAKId IS NULL, - 1, i_ProductAKId) AS o_ProductAKId,
	-- *INF*: IIF(ISNULL(i_StatisticalCoverageAKID), -1,i_StatisticalCoverageAKID)
	IFF(i_StatisticalCoverageAKID IS NULL, - 1, i_StatisticalCoverageAKID) AS o_StatisticalCoverageAKID
	FROM SQ_claimant_coverage_detail_Stat_Cvg
),
FIL_Remove_minus_1_Updates AS (
	SELECT
	claimant_cov_det_id, 
	o_CoverageGUID, 
	o_InsuranceReferenceLineOfBusinessAKId, 
	o_ProductAKId, 
	o_StatisticalCoverageAKID
	FROM EXP_EXP_Trim_Expression
	WHERE o_CoverageGUID <> 'N/A' OR 
o_InsuranceReferenceLineOfBusinessAKId <> -1 OR 
o_ProductAKId <> -1 OR 
o_StatisticalCoverageAKID <> -1
),
UPD_UpdateTarget AS (
	SELECT
	claimant_cov_det_id, 
	o_CoverageGUID AS CoverageGUID, 
	o_InsuranceReferenceLineOfBusinessAKId AS InsuranceReferenceLineOfBusinessAKId, 
	o_ProductAKId AS ProductAKId, 
	o_StatisticalCoverageAKID AS StatisticalCoverageAKID
	FROM FIL_Remove_minus_1_Updates
),
claimant_coverage_detail_update_statisticalcoverageakid AS (
	MERGE INTO claimant_coverage_detail AS T
	USING UPD_UpdateTarget AS S
	ON T.claimant_cov_det_id = S.claimant_cov_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageGUID = S.CoverageGUID, T.StatisticalCoverageAKID = S.StatisticalCoverageAKID, T.InsuranceReferenceLineOfBusinessAKId = S.InsuranceReferenceLineOfBusinessAKId, T.ProductAKId = S.ProductAKId
),
SQ_Claim_Occurrence_PolicyAKID_Change AS (
	with 
	 AKID
	AS
	(
	select distinct *
	from (Select SC.StatisticalCoverageAKID as StatisticalCoverageAKID, 
	SC.InsuranceReferenceLineOfBusinessAKID as InsuranceReferenceLineOfBusinessAKID, 
	PC.PolicyAKID as PolicyAKID, 
	SC.ProductAKID as ProductAKID ,
	PC.InsuranceLine as InsuranceLine, 
	(CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END) as LocationNumber,
	SC.MajorPerilCode as MajorPerilCode,
	SC.MajorPerilSequenceNumber as MajorPerilSequenceNumber,
	SC.RiskUnit as RiskUnit,
	(CASE WHEN SC.RiskUnitSequenceNumber = '0' then 'N/A' ELSE SC.RiskUnitSequenceNumber END) as RiskUnitSequenceNumber,
	(CASE WHEN SC.RiskUnitSequenceNumber = '0' then 'N' ELSE substring(SC.RiskUnitSequenceNumber,1,1) END) as RiskUnitSequenceNumber1_1,
	SC.RiskUnitGroup as RiskUnitGroup, 
	SC.RiskUnitGroupSequenceNumber as RiskUnitGroupSequenceNumber , 
	(CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END) as SubLocationUnitNumber,
	PC.TypeBureauCode as TypeBureauCode,
	SC.StatisticalCoverageEffectiveDate as StatisticalCoverageEffectiveDate,
	RANK()over(PARTITION BY 
	PC.PolicyAKID,
	PC.InsuranceLine,
	(CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END),
	(CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END),
	SC.RiskUnitGroup,
	SC.RiskUnitGroupSequenceNumber,
	SC.RiskUnit,
	SC.RiskUnitSequenceNumber,
	SC.MajorPerilCode,
	SC.MajorPerilSequenceNumber ,
	PC.TypeBureauCode,
	SC.StatisticalCoverageEffectiveDate
	order by SC.StatisticalCoverageEffectiveDate desc,CASE WHEN SC.ReinsuranceSectionCode ='2' THEN '2' ELSE '1' END asc, SC.StatisticalCoverageAKID desc) as rn,
	SC.CoverageGUID
	from 
	StatisticalCoverage SC with (nolock) 
	inner join PolicyCoverage PC with (nolock) on 
		SC.PolicyCoverageAKID = PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	inner join RiskLocation RL with (nolock) on 
		PC.RiskLocationAKID = RL.RiskLocationAKID and RL.CurrentSnapshotFlag=1
	inner join V2.policy p with (nolock) on 
		PC.PolicyAKID = p.pol_ak_id AND 
		P.crrnt_snpsht_flag=1 AND
		P.source_sys_id='PMS'
	inner join 
	
	(select CO.pol_key_ak_id from
	claim_occurrence CO with (nolock)
	inner join claim_party_occurrence CPO with (nolock) on CO.claim_occurrence_ak_id=CPO.claim_occurrence_ak_id and CPO.crrnt_snpsht_flag=1
	inner join claimant_coverage_detail CCD with (nolock) on CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id and CCD.crrnt_snpsht_flag=1
	inner join StatisticalCoverage SC with (nolock) on SC.StatisticalCoverageAKID=CCD.StatisticalCoverageAKID and SC.CurrentSnapshotFlag=1
	inner join PolicyCoverage PC with (nolock) on SC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	where
	CO.crrnt_snpsht_flag=1 and
	CO.pol_key_ak_id != PC.PolicyAKID and
	CO.modified_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	) a on a.pol_key_ak_Id = PC.policyakid
	
	) b
	where rn=1 
	)
	
	,
	ClaimOCC as (
	SELECT distinct
	CCD.claimant_cov_det_id, 
	CCD.StatisticalCoverageAKID,
	CCD.CoverageGUID,
	CCD.ProductAKID,
	CCD.InsuranceReferenceLineOfBusinessAKID,
	CO.pol_key_ak_id,
	PC.PolicyAKID,
	CO.pol_key,
	CO.s3p_claim_num,
	
	LTRIM(RTRIM(CCD.ins_line)) as ins_line,
	LTRIM(RTRIM(CCD.loc_unit_num)) as loc_unit_num,
	LTRIM(RTRIM(CCD.sub_loc_unit_num)) as sub_loc_unit_num,
	LTRIM(RTRIM(CCD.risk_unit_grp)) as risk_unit_grp,
	LTRIM(RTRIM(CCD.risk_unit_grp_seq_num)) as risk_unit_grp_seq_num,
	CO.claim_loss_Date,
	ltrim(rtrim(CCD.risk_unit)) as risk_unit,
	
	case 
		when LTRIM(RTRIM(CCD.risk_unit_seq_num))='0' AND LTRIM(RTRIM(CCD.ins_line))='WC' then '00'
		when LTRIM(RTRIM(CCD.risk_unit_seq_num)) in ('0') 
				AND LTRIM(RTRIM(CCD.ins_line))<>'WC' 
				AND LTRIM(RTRIM(CCD.risk_type_ind))='N/A' then 'N/A'
		when LTRIM(RTRIM(CCD.risk_unit_seq_num)) in ('0','1','2','3','4','8') and LTRIM(RTRIM(CCD.ins_line))='GL' 
		then LTRIM(RTRIM(CCD.risk_unit_seq_num))+LTRIM(RTRIM(CCD.risk_type_ind))
		else LTRIM(RTRIM(CCD.risk_unit_seq_num)) 
	end as RiskUnitSequenceNumber1,
		 
	SUBSTRING(
		case when LTRIM(RTRIM(CCD.risk_unit_seq_num))='0'
				AND LTRIM(RTRIM(CCD.ins_line))='WC' then '00'
			when LTRIM(RTRIM(CCD.risk_unit_seq_num)) in ('0') 
				AND LTRIM(RTRIM(CCD.ins_line))<>'WC' AND LTRIM(RTRIM(CCD.risk_type_ind))='N/A' then 'N/A'
			when LTRIM(RTRIM(CCD.risk_unit_seq_num)) in ('0','1','2','3','4','8')
				and LTRIM(RTRIM(CCD.ins_line))='GL' 
			then LTRIM(RTRIM(CCD.risk_unit_seq_num))+LTRIM(RTRIM(CCD.risk_type_ind))
			else LTRIM(RTRIM(CCD.risk_unit_seq_num)) 
	end,1,1) as RiskUnitSequenceNumber2,
	
	'N/A' as RiskUnitSequenceNumber3,
	'0O' as RiskUnitSequenceNumber4,
	
	LTRIM(RTRIM(CCD.major_peril_code)) as major_peril_code,
	LTRIM(RTRIM(CCD.major_peril_seq)) as major_peril_seq,
	ISNULL(CCD.pms_type_bureau_code,'N/A') as pms_type_bureau_code
	
	FROM 
	claim_occurrence CO with (nolock)
	inner join claim_party_occurrence CPO with (nolock) on CO.claim_occurrence_ak_id=CPO.claim_occurrence_ak_id and CPO.crrnt_snpsht_flag=1
	inner join claimant_coverage_detail CCD with (nolock) on CPO.claim_party_occurrence_ak_id=CCD.claim_party_occurrence_ak_id and CCD.crrnt_snpsht_flag=1
	inner join StatisticalCoverage SC with (nolock) on SC.StatisticalCoverageAKID=CCD.StatisticalCoverageAKID and SC.CurrentSnapshotFlag=1
	inner join PolicyCoverage PC with (nolock) on SC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	
	where 
	CO.crrnt_snpsht_flag=1 and
	CO.pol_key_ak_id != PC.PolicyAKID and
	CCD.PolicySourceID  in ('PMS','ESU') and
	CO.modified_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	)
	
	
	SELECT
	ClaimOCC.claimant_cov_det_id AS claimant_cov_det_id,
	ClaimOCC.StatisticalCoverageAKID AS StatisticalCoverageAKID_Old, 
	AKID.StatisticalCoverageAKID AS StatisticalCoverageAKID_New, 
	AKID.StatisticalCoverageEffectiveDate, 
	ClaimOCC.claim_loss_date, 
	AKID.InsuranceReferenceLineOfBusinessAKID AS InsuranceReferenceLineOfBusinessAKID_New,
	ClaimOcc.InsuranceReferenceLineOfBusinessAKId AS InsuranceReferenceLineOfBusinessAKID_Old,
	AKID.ProductAKID AS ProductAKID_New,
	ClaimOcc.ProductAKId AS ProductAKID_Old,
	AKID.CoverageGuid AS CoverageGuid_New,
	ClaimOcc.CoverageGUID AS CoverageGuid_Old,
	ClaimOCC.s3p_claim_num,
	AKID.PolicyAKID AS PolicyAKID_New,
	ClaimOcc.PolicyAKID AS PolicyAKID_Old
	FROM
	ClaimOCC
	inner join AKID  on 
	Akid.PolicyAKID=ClaimOcc.pol_key_ak_id 
	where
	Akid.InsuranceLine=ClaimOcc.ins_line and
	AKID.locationNumber = ClaimOcc.loc_unit_num and
	AKID.SubLocationUnitNumber=ClaimOcc.sub_loc_unit_num and
	AKID.RiskUnitGroup = claimocc.risk_unit_grp and
	AKID.RiskUnitGroupSequenceNumber = claimocc.risk_unit_grp_seq_num and
	AKID.RiskUnit=ClaimOcc.risk_unit and
	(
	AKID.RiskUnitSequenceNumber in (claimocc.RiskUnitSequenceNumber1, claimocc.RiskUnitSequenceNumber3, claimocc.RiskUnitSequenceNumber4) or
	AKID.RiskUnitSequenceNumber1_1=claimOcc.RiskUnitSequenceNumber2
	) and
	AKID.MajorPerilCode=ClaimOCC.major_peril_code and
	AKID.MajorPerilSequenceNumber=ClaimOcc.major_peril_seq and
	AKID.TypeBureauCode=ClaimOcc.pms_type_bureau_code
),
EXP_Input_PolicyAKID_Change AS (
	SELECT
	claimant_cov_det_id,
	StatisticalCoverageAKID_Old,
	StatisticalCoverageAKID_New,
	StatisticalCoverageEffectiveDate,
	claim_loss_date,
	InsuranceReferenceLineOfBusinessAKID_New,
	InsuranceReferenceLineOfBusinessAKID_Old,
	ProductAKID_New,
	ProductAKID_Old,
	CoverageGuid_New,
	CoverageGuid_Old,
	s3p_claim_num,
	PolicyAKID_New,
	PolicyAKID_Old
	FROM SQ_Claim_Occurrence_PolicyAKID_Change
),
FIL_PolicyAKID_Change AS (
	SELECT
	claimant_cov_det_id, 
	StatisticalCoverageAKID_Old, 
	StatisticalCoverageAKID_New, 
	StatisticalCoverageEffectiveDate, 
	claim_loss_date, 
	InsuranceReferenceLineOfBusinessAKID_New, 
	InsuranceReferenceLineOfBusinessAKID_Old, 
	ProductAKID_New, 
	ProductAKID_Old, 
	CoverageGuid_New, 
	CoverageGuid_Old, 
	s3p_claim_num, 
	PolicyAKID_New, 
	PolicyAKID_Old
	FROM EXP_Input_PolicyAKID_Change
	WHERE IIF(StatisticalCoverageEffectiveDate <= claim_loss_date
AND StatisticalCoverageAKID_New != -1
AND InsuranceReferenceLineOfBusinessAKID_New !=-1
AND ProductAKID_New !=-1, TRUE, FALSE)
),
EXP_Fil_Output AS (
	SELECT
	claimant_cov_det_id,
	StatisticalCoverageAKID_New,
	InsuranceReferenceLineOfBusinessAKID_New,
	ProductAKID_New,
	CoverageGuid_New
	FROM FIL_PolicyAKID_Change
),
UPD_PolicyAKID_Change_Update AS (
	SELECT
	claimant_cov_det_id, 
	StatisticalCoverageAKID_New, 
	InsuranceReferenceLineOfBusinessAKID_New, 
	ProductAKID_New, 
	CoverageGuid_New
	FROM EXP_Fil_Output
),
claimant_coverage_detail_PolicyAKID_Change AS (
	MERGE INTO claimant_coverage_detail AS T
	USING UPD_PolicyAKID_Change_Update AS S
	ON T.claimant_cov_det_id = S.claimant_cov_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CoverageGUID = S.CoverageGuid_New, T.StatisticalCoverageAKID = S.StatisticalCoverageAKID_New, T.InsuranceReferenceLineOfBusinessAKId = S.InsuranceReferenceLineOfBusinessAKID_New, T.ProductAKId = S.ProductAKID_New
),