WITH
SQ_Claimant_Coverage_Sources AS (

------------ PRE SQL ----------
truncate table dbo.work_claimant_coverage

insert dbo.work_claimant_coverage ( CLAIMANT_COV_DET_AK_ID,
                                                                          EFF_FROM_DATE)
SELECT CLAIMANT_COV_DET_AK_ID,EFF_FROM_DATE FROM DBO.CLAIMANT_COVERAGE_DETAIL
WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
UNION
SELECT CLAIMANT_COV_DET_AK_ID,EFF_FROM_DATE FROM DBO.CLAIMANT_COVERAGE_DETAIL_CALCULATION
WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
UNION
SELECT CLAIMANT_COV_DET_AK_ID,EFF_FROM_DATE FROM DBO.CLAIMANT_COVERAGE_DETAIL_RESERVE_CALCULATION
WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
----------------------


	SELECT
	COV_DET.CLAIMANT_COV_DET_ID,
	COV_DET.CLAIMANT_COV_DET_AK_ID,
	RTRIM(COV_DET.S3P_UNIT_TYPE_CODE) AS S3P_UNIT_TYPE_CODE,
	RTRIM(COV_DET.S3P_WC_CLASS_DESCRIPT) AS S3P_WC_CLASS_DESCRIPT,
	ISNULL(COV_DET.LOC_UNIT_NUM,'N/A') AS LOC_UNIT_NUM,
	ISNULL(COV_DET.SUB_LOC_UNIT_NUM,'N/A') AS SUB_LOC_UNIT_NUM,
	ISNULL(COV_DET.INS_LINE,'N/A') AS INS_LINE,
	ISNULL(COV_DET.RISK_UNIT_GRP,'N/A') AS RISK_UNIT_GRP,
	ISNULL(COV_DET.RISK_UNIT_GRP_SEQ_NUM,'N/A') AS RISK_UNIT_GRP_SEQ_NUM,
	ISNULL(COV_DET.RISK_UNIT,'N/A') AS RISK_UNIT,
	ISNULL(COV_DET.RISK_UNIT_SEQ_NUM,'N/A') AS RISK_UNIT_SEQ_NUM,
	ISNULL(RTRIM(COV_DET.MAJOR_PERIL_CODE),'N/A') AS MAJOR_PERIL_CODE,
	ISNULL(COV_DET.MAJOR_PERIL_SEQ,'N/A') AS MAJOR_PERIL_SEQ,
	RTRIM(COV_DET.PMS_LOSS_DISABILITY) AS PMS_LOSS_DISABILITY,
	RTRIM(COV_DET.RESERVE_CTGRY) AS RESERVE_CTGRY,
	RTRIM(COV_DET.CAUSE_OF_LOSS) AS CAUSE_OF_LOSS,
	ISNULL(COV_DET.PMS_TYPE_BUREAU_CODE,'N/A') AS PMS_TYPE_BUREAU_CODE,
	COV_DET.CLAIMANT_COV_EFF_DATE,
	ISNULL(COV_DET.S3P_UNIT_DESCRIPT,'N/A') AS S3P_UNIT_DESCRIPT,
	ISNULL(COV_DET.SPEC_PERS_PROP_USE_CODE,'N/A') AS SPEC_PERS_PROP_USE_CODE,
	ISNULL(COV_DET.PKG_DED_AMT,0) AS PKG_DED_AMT,
	ISNULL(COV_DET.PKG_LMT_AMT,0) AS PKG_LMT_AMT,
	ISNULL(COV_DET.UNIT_VEH_REGISTRATION_STATE_CODE,'N/A') AS UNIT_VEH_REGISTRATION_STATE_CODE,
	ISNULL(COV_DET.UNIT_VEH_STATED_AMT,0) AS UNIT_VEH_STATED_AMT,
	ISNULL(COV_DET.UNIT_DAM_DESCRIPT,'N/A') AS UNIT_DAM_DESCRIPT,
	ISNULL(COV_DET.UNIT_VEH_YR,-1) AS UNIT_VEH_YR,
	ISNULL(COV_DET.UNIT_VEH_MAKE,'N/A') AS UNIT_VEH_MAKE,
	ISNULL(COV_DET.UNIT_VIN_NUM,'N/A') AS UNIT_VIN_NUM,
	ISNULL(COV_DET.AUDIT_ID,-1) AS EDW_CCD_AUDIT_ID,
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE,
	ISNULL(COV_DET.SUPINSURANCELINEID,-1) AS SUPINSURANCELINEID,
	ISNULL(COV_DET.SUP_RISK_UNIT_GRP_ID,-1) AS SUP_RISK_UNIT_GRP_ID,
	ISNULL(COV_DET.SUP_RISK_UNIT_ID,-1) AS SUP_RISK_UNIT_ID,
	ISNULL(COV_DET.SUPMAJORPERILID,-1) AS SUPMAJORPERILID,
	ISNULL(COV_DET.CAUSEOFLOSSID,-1) AS CAUSEOFLOSSID,
	ISNULL(COV_DET.SUPTYPEBUREAUCODEID,-1) AS SUPTYPEBUREAUCODEID,
	COV_DET.POLICYSOURCEID,
	ISNULL(COV_DET.COVERAGEFORM,'N/A') AS COVERAGEFORM,
	ISNULL(COV_DET.RISKTYPE,'N/A') AS RISKTYPE,
	ISNULL(COV_DET.COVERAGETYPE,'N/A') AS COVERAGETYPE,
	ISNULL(COV_DET.COVERAGEVERSION,'N/A') AS COVERAGEVERSION,
	ISNULL(COV_DET.CLASSCODE,'N/A') AS CLASSCODE,
	ISNULL(COV_DET.SUBLINECODE,'N/A') AS SUBLINECODE,
	ISNULL(COV_DET.TypeOfLoss,'N/A') AS TypeOfLoss,
	ISNULL(COV_DET.ClaimTypeCategory,'N/A') AS ClaimTypeCategory,
	ISNULL(COV_DET.ClaimTypeGroup,'N/A') AS ClaimTypeGroup,
	ISNULL(COV_DET.SubrogationEligibleIndicator,'N/A') AS SubrogationEligibleIndicator,
	COV_CALC.CLAIMANT_COV_DET_CALCULATION_ID,
	COV_CALC.CLAIMANT_COV_DATE,
	COV_CALC.CLAIMANT_COV_DATE_TYPE,
	COV_CALC.CLAIMANT_COV_SUPPLEMENTAL_IND,
	COV_CALC.CLAIMANT_COV_FINANCIAL_IND,
	COV_CALC.CLAIMANT_COV_RECOVERY_IND,
	COV_CALC.CLAIMANT_COV_NOTICE_ONLY_IND,
	COV_RES_CALC_D.CLAIMANT_COV_DET_RESERVE_CALCULATION_ID  AS CLAIMANT_COV_DET_RESERVE_CALCULATION_ID_D,
	COV_RES_CALC_D.RESERVE_DATE_TYPE AS CLAIMANT_COV_DIRECT_LOSS_STATUS_TYPE,
	COV_RES_CALC_E.CLAIMANT_COV_DET_RESERVE_CALCULATION_ID AS CLAIMANT_COV_DET_RESERVE_CALCULATION_ID_E,
	COV_RES_CALC_E.RESERVE_DATE_TYPE AS CLAIMANT_COV_EXP_STATUS_TYPE,
	COV_RES_CALC_S.CLAIMANT_COV_DET_RESERVE_CALCULATION_ID AS CLAIMANT_COV_DET_RESERVE_CALCULATION_ID_S,
	COV_RES_CALC_S.RESERVE_DATE_TYPE AS CLAIMANT_COV_SALVAGE_STATUS_TYPE,
	COV_RES_CALC_B.CLAIMANT_COV_DET_RESERVE_CALCULATION_ID AS CLAIMANT_COV_DET_RESERVE_CALCULATION_ID_B,
	COV_RES_CALC_B.RESERVE_DATE_TYPE AS CLAIMANT_COV_SUBROGATION_STATUS_TYPE,
	COV_RES_CALC_R.CLAIMANT_COV_DET_RESERVE_CALCULATION_ID AS CLAIMANT_COV_DET_RESERVE_CALCULATION_ID_R,
	COV_RES_CALC_R.RESERVE_DATE_TYPE AS CLAIMANT_COV_OTHER_RECOVERY_STATUS_TYPE
	FROM
	dbo.work_claimant_coverage AS DISTINCT_EFF_FROM_DATES
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL COV_DET
	ON DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COV_DET.EFF_FROM_DATE AND COV_DET.EFF_TO_DATE
	AND DISTINCT_EFF_FROM_DATES.CLAIMANT_COV_DET_AK_ID = COV_DET.CLAIMANT_COV_DET_AK_ID
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL_CALCULATION COV_CALC
	ON DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COV_CALC.EFF_FROM_DATE AND COV_CALC.EFF_TO_DATE
	AND COV_DET.CLAIMANT_COV_DET_AK_ID = COV_CALC.CLAIMANT_COV_DET_AK_ID
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL_RESERVE_CALCULATION COV_RES_CALC_D ON
	COV_DET.CLAIMANT_COV_DET_AK_ID = COV_RES_CALC_D.CLAIMANT_COV_DET_AK_ID AND COV_RES_CALC_D.FINANCIAL_TYPE_CODE='D'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COV_RES_CALC_D.EFF_FROM_DATE AND COV_RES_CALC_D.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL_RESERVE_CALCULATION COV_RES_CALC_E ON
	COV_DET.CLAIMANT_COV_DET_AK_ID = COV_RES_CALC_E.CLAIMANT_COV_DET_AK_ID AND COV_RES_CALC_E.FINANCIAL_TYPE_CODE='E'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COV_RES_CALC_E.EFF_FROM_DATE AND COV_RES_CALC_E.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL_RESERVE_CALCULATION COV_RES_CALC_S ON
	COV_DET.CLAIMANT_COV_DET_AK_ID = COV_RES_CALC_S.CLAIMANT_COV_DET_AK_ID AND COV_RES_CALC_S.FINANCIAL_TYPE_CODE='S'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COV_RES_CALC_S.EFF_FROM_DATE AND COV_RES_CALC_S.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL_RESERVE_CALCULATION COV_RES_CALC_B ON
	COV_DET.CLAIMANT_COV_DET_AK_ID = COV_RES_CALC_B.CLAIMANT_COV_DET_AK_ID AND COV_RES_CALC_B.FINANCIAL_TYPE_CODE='B'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COV_RES_CALC_B.EFF_FROM_DATE AND COV_RES_CALC_B.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL_RESERVE_CALCULATION COV_RES_CALC_R ON
	COV_DET.CLAIMANT_COV_DET_AK_ID = COV_RES_CALC_R.CLAIMANT_COV_DET_AK_ID AND COV_RES_CALC_R.FINANCIAL_TYPE_CODE='R'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COV_RES_CALC_R.EFF_FROM_DATE AND COV_RES_CALC_R.EFF_TO_DATE
),
LKP_Line_Of_Business AS (
	SELECT
	line_of_business,
	claimant_cov_det_ak_id
	FROM (
		Select
		P.pms_pol_lob_code as line_of_business,
		CCD.claimant_cov_det_ak_id as claimant_cov_det_ak_id 
		from 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail CCD, 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO, 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO, 
		V2.policy P 
		where 
		CCD.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id and 
		CPO.claim_occurrence_ak_id = CO.claim_occurrence_ak_id and 
		CO.pol_key = P.pol_key and
		CO.crrnt_snpsht_flag='1' and
		P.crrnt_snpsht_flag='1' and
		CPO.crrnt_snpsht_flag='1' and 
		CCD.crrnt_snpsht_flag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY line_of_business DESC) = 1
),
LKP_Sup_Claim_Pms_Loss_Disability AS (
	SELECT
	loss_disability_descript,
	in_pms_loss_disability_code,
	loss_disability_code
	FROM (
		SELECT 
			loss_disability_descript,
			in_pms_loss_disability_code,
			loss_disability_code
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_pms_loss_disability
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_disability_code ORDER BY loss_disability_descript) = 1
),
EXP_Claimant_Coverage_Dim AS (
	SELECT
	SQ_Claimant_Coverage_Sources.claimant_cov_det_id AS in_claimant_cov_det_id,
	-- *INF*: IIF(ISNULL(in_claimant_cov_det_id), -1, in_claimant_cov_det_id)
	-- 
	IFF(in_claimant_cov_det_id IS NULL, - 1, in_claimant_cov_det_id) AS claimant_cov_det_id_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_det_calculation_id AS in_claimant_cov_det_calculation_id,
	-- *INF*: IIF(ISNULL(in_claimant_cov_det_calculation_id), -1, in_claimant_cov_det_calculation_id)
	IFF(in_claimant_cov_det_calculation_id IS NULL, - 1, in_claimant_cov_det_calculation_id) AS claimant_cov_det_calculation_id_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_det_reserve_calculation_id_D AS in_claimant_cov_det_reserve_calculation_id_D,
	-- *INF*: IIF(ISNULL(in_claimant_cov_det_reserve_calculation_id_D), -1,in_claimant_cov_det_reserve_calculation_id_D )
	IFF(in_claimant_cov_det_reserve_calculation_id_D IS NULL, - 1, in_claimant_cov_det_reserve_calculation_id_D) AS claimant_cov_det_reserve_calculation_id_D_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_date_type AS in_claimant_cov_date_type,
	-- *INF*: IIF(ISNULL(in_claimant_cov_date_type) OR in_claimant_cov_date_type = 'N/A','N/A',RTRIM(SUBSTR(in_claimant_cov_date_type,2)))
	IFF(in_claimant_cov_date_type IS NULL OR in_claimant_cov_date_type = 'N/A', 'N/A', RTRIM(SUBSTR(in_claimant_cov_date_type, 2))) AS v_claimant_cov_date_type,
	v_claimant_cov_date_type AS claimant_cov_date_type_out1,
	SQ_Claimant_Coverage_Sources.claimant_cov_direct_loss_status_type,
	-- *INF*: IIF(ISNULL(claimant_cov_direct_loss_status_type) OR claimant_cov_direct_loss_status_type = 'N/A','N/A',
	-- RTRIM(SUBSTR(claimant_cov_direct_loss_status_type,2)))
	IFF(claimant_cov_direct_loss_status_type IS NULL OR claimant_cov_direct_loss_status_type = 'N/A', 'N/A', RTRIM(SUBSTR(claimant_cov_direct_loss_status_type, 2))) AS claimant_cov_direct_loss_status_type_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_det_reserve_calculation_id_E AS in_claimant_cov_det_reserve_calculation_id_E,
	-- *INF*: IIF(ISNULL(in_claimant_cov_det_reserve_calculation_id_E), -1,in_claimant_cov_det_reserve_calculation_id_E )
	IFF(in_claimant_cov_det_reserve_calculation_id_E IS NULL, - 1, in_claimant_cov_det_reserve_calculation_id_E) AS claimant_cov_det_reserve_calculation_id_E_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_exp_status_type,
	-- *INF*: IIF(ISNULL(claimant_cov_exp_status_type) OR claimant_cov_exp_status_type = 'N/A','N/A',RTRIM(SUBSTR(claimant_cov_exp_status_type,2)))
	IFF(claimant_cov_exp_status_type IS NULL OR claimant_cov_exp_status_type = 'N/A', 'N/A', RTRIM(SUBSTR(claimant_cov_exp_status_type, 2))) AS claimant_cov_exp_status_type_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_det_reserve_calculation_id_S AS in_claimant_cov_det_reserve_calculation_id_S,
	-- *INF*: IIF(ISNULL(in_claimant_cov_det_reserve_calculation_id_S), -1,in_claimant_cov_det_reserve_calculation_id_S )
	IFF(in_claimant_cov_det_reserve_calculation_id_S IS NULL, - 1, in_claimant_cov_det_reserve_calculation_id_S) AS claimant_cov_det_reserve_calculation_id_S_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_salvage_status_type,
	-- *INF*: IIF(ISNULL(claimant_cov_salvage_status_type) OR claimant_cov_salvage_status_type = 'N/A','N/A',RTRIM(SUBSTR(claimant_cov_salvage_status_type,2)))
	IFF(claimant_cov_salvage_status_type IS NULL OR claimant_cov_salvage_status_type = 'N/A', 'N/A', RTRIM(SUBSTR(claimant_cov_salvage_status_type, 2))) AS claimant_cov_salvage_status_type_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_det_reserve_calculation_id_B AS in_claimant_cov_det_reserve_calculation_id_B,
	-- *INF*: IIF(ISNULL(in_claimant_cov_det_reserve_calculation_id_B), -1, in_claimant_cov_det_reserve_calculation_id_B)
	IFF(in_claimant_cov_det_reserve_calculation_id_B IS NULL, - 1, in_claimant_cov_det_reserve_calculation_id_B) AS claimant_cov_det_reserve_calculation_id_B_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_subgrogation_status_type,
	-- *INF*: IIF(ISNULL(claimant_cov_subgrogation_status_type) OR claimant_cov_subgrogation_status_type = 'N/A','N/A',RTRIM(SUBSTR(claimant_cov_subgrogation_status_type,2)))
	IFF(claimant_cov_subgrogation_status_type IS NULL OR claimant_cov_subgrogation_status_type = 'N/A', 'N/A', RTRIM(SUBSTR(claimant_cov_subgrogation_status_type, 2))) AS claimant_cov_subgrogation_status_type_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_det_reserve_calculation_id_R AS in_claimant_cov_det_reserve_calculation_id_R,
	-- *INF*: IIF(ISNULL(in_claimant_cov_det_reserve_calculation_id_R),-1,in_claimant_cov_det_reserve_calculation_id_R)
	IFF(in_claimant_cov_det_reserve_calculation_id_R IS NULL, - 1, in_claimant_cov_det_reserve_calculation_id_R) AS claimant_cov_det_reserve_calculation_id_R_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_other_recovery_status_type,
	-- *INF*: IIF(ISNULL(claimant_cov_other_recovery_status_type) OR claimant_cov_other_recovery_status_type = 'N/A','N/A',RTRIM(SUBSTR(claimant_cov_other_recovery_status_type,2)))
	IFF(claimant_cov_other_recovery_status_type IS NULL OR claimant_cov_other_recovery_status_type = 'N/A', 'N/A', RTRIM(SUBSTR(claimant_cov_other_recovery_status_type, 2))) AS claimant_cov_other_recovery_status_type_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_supplemental_ind AS in_claimant_cov_supplemental_ind,
	-- *INF*: IIF(ISNULL(in_claimant_cov_supplemental_ind) OR in_claimant_cov_supplemental_ind = 'N/A','N/A',in_claimant_cov_supplemental_ind)
	IFF(in_claimant_cov_supplemental_ind IS NULL OR in_claimant_cov_supplemental_ind = 'N/A', 'N/A', in_claimant_cov_supplemental_ind) AS claimant_cov_supplemental_ind_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_financial_ind AS in_claimant_cov_financial_ind,
	-- *INF*: IIF(ISNULL(in_claimant_cov_financial_ind) OR in_claimant_cov_financial_ind = 'N/A'
	-- ,'N/A',in_claimant_cov_financial_ind)
	IFF(in_claimant_cov_financial_ind IS NULL OR in_claimant_cov_financial_ind = 'N/A', 'N/A', in_claimant_cov_financial_ind) AS claimant_cov_financial_ind_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_recovery_ind AS in_claimant_cov_recovery_ind,
	-- *INF*: IIF(ISNULL(in_claimant_cov_recovery_ind) OR in_claimant_cov_recovery_ind = 'N/A','N/A',in_claimant_cov_recovery_ind)
	IFF(in_claimant_cov_recovery_ind IS NULL OR in_claimant_cov_recovery_ind = 'N/A', 'N/A', in_claimant_cov_recovery_ind) AS claimant_cov_recovery_ind_out,
	SQ_Claimant_Coverage_Sources.claimant_cov_notice_only_ind AS in_claimant_cov_notice_only_ind,
	-- *INF*: IIF(ISNULL(in_claimant_cov_notice_only_ind) OR in_claimant_cov_notice_only_ind = 'N/A','N/A',in_claimant_cov_notice_only_ind)
	IFF(in_claimant_cov_notice_only_ind IS NULL OR in_claimant_cov_notice_only_ind = 'N/A', 'N/A', in_claimant_cov_notice_only_ind) AS claimant_cov_notice_only_ind_out,
	SQ_Claimant_Coverage_Sources.s3p_wc_class_descript AS i_s3p_wc_class_descript,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(i_s3p_wc_class_descript))) OR IS_SPACES(LTRIM(RTRIM(i_s3p_wc_class_descript))),'N/A',LTRIM(RTRIM(i_s3p_wc_class_descript)))
	IFF(LTRIM(RTRIM(i_s3p_wc_class_descript)) IS NULL OR IS_SPACES(LTRIM(RTRIM(i_s3p_wc_class_descript))), 'N/A', LTRIM(RTRIM(i_s3p_wc_class_descript))) AS o_s3p_wc_class_descript,
	LKP_Sup_Claim_Pms_Loss_Disability.in_pms_loss_disability_code,
	-- *INF*: IIF(ISNULL(in_pms_loss_disability_code),'N/A',in_pms_loss_disability_code)
	IFF(in_pms_loss_disability_code IS NULL, 'N/A', in_pms_loss_disability_code) AS o_pms_loss_disability_code,
	LKP_Sup_Claim_Pms_Loss_Disability.loss_disability_descript AS lkp_loss_disability_descript,
	-- *INF*: IIF(ISNULL(lkp_loss_disability_descript) OR lkp_loss_disability_descript = 'N/A','N/A',lkp_loss_disability_descript)
	IFF(lkp_loss_disability_descript IS NULL OR lkp_loss_disability_descript = 'N/A', 'N/A', lkp_loss_disability_descript) AS loss_disability_descript_out,
	SQ_Claimant_Coverage_Sources.s3p_unit_descript AS i_s3p_unit_descript,
	-- *INF*: IIF(ISNULL(i_s3p_unit_descript),'N/A',i_s3p_unit_descript)
	IFF(i_s3p_unit_descript IS NULL, 'N/A', i_s3p_unit_descript) AS o_s3p_unit_descript,
	SQ_Claimant_Coverage_Sources.unit_dam_descript AS i_unit_dam_descript,
	-- *INF*: IIF(ISNULL(i_unit_dam_descript),'N/A',i_unit_dam_descript)
	IFF(i_unit_dam_descript IS NULL, 'N/A', i_unit_dam_descript) AS o_unit_dam_descript,
	SQ_Claimant_Coverage_Sources.claimant_cov_date,
	-- *INF*: IIF(ISNULL(v_claimant_cov_date_type),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_cov_date_type= 'OPEN',claimant_cov_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- ---DECODE(TRUE, 
	-- --v_claimant_cov_date_type = 'N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- --v_claimant_cov_date_type = 'OPEN',claimant_cov_date,
	-- --(v_claimant_cov_date_type != 'N/A' OR v_claimant_cov_date_type != 'OPEN' ) AND (claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id), v_prev_row_claimant_cov_open_date)
	-- 
	-- 
	-- 
	-- 
	IFF(v_claimant_cov_date_type IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IFF(v_claimant_cov_date_type = 'OPEN', claimant_cov_date, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))) AS v_claimant_cov_open_date,
	v_claimant_cov_open_date AS claimant_cov_open_date,
	-- *INF*: IIF(ISNULL(v_claimant_cov_date_type),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_cov_date_type= 'CLOSED',claimant_cov_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- -- DECODE(TRUE, 
	-- -- v_claimant_cov_date_type = 'N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- -- v_claimant_cov_date_type = 'CLOSED',claimant_cov_date,
	-- -- (v_claimant_cov_date_type != 'N/A' OR v_claimant_cov_date_type != 'CLOSED' ) AND (claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id), v_prev_row_claimant_cov_close_date)
	IFF(v_claimant_cov_date_type IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IFF(v_claimant_cov_date_type = 'CLOSED', claimant_cov_date, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))) AS v_claimant_cov_close_date,
	v_claimant_cov_close_date AS claimant_cov_close_date,
	-- *INF*: IIF(ISNULL(v_claimant_cov_date_type),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_cov_date_type= 'REOPEN',claimant_cov_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- -- DECODE(TRUE, 
	-- -- v_claimant_cov_date_type = 'N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- -- v_claimant_cov_date_type = 'REOPEN',claimant_cov_date,
	-- -- (v_claimant_cov_date_type != 'N/A' OR v_claimant_cov_date_type != 'REOPEN' ) AND (claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id), v_prev_row_claimant_cov_reopen_date)
	IFF(v_claimant_cov_date_type IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IFF(v_claimant_cov_date_type = 'REOPEN', claimant_cov_date, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))) AS v_claimant_cov_reopen_date,
	v_claimant_cov_reopen_date AS claimant_cov_reopen_date,
	-- *INF*: IIF(ISNULL(v_claimant_cov_date_type),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_cov_date_type= 'CLOSEDAFTERREOPEN',claimant_cov_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- -- DECODE(TRUE, 
	-- -- v_claimant_cov_date_type = 'N/A',TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- -- v_claimant_cov_date_type = 'CLOSEDAFTERREOPEN',claimant_cov_date,
	-- -- (v_claimant_cov_date_type != 'N/A' OR v_claimant_cov_date_type != 'CLOSEDAFTERREOPEN' ) AND (claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id), v_prev_row_claimant_cov_closed_after_reopen_date)
	-- -- 
	-- -- 
	IFF(v_claimant_cov_date_type IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IFF(v_claimant_cov_date_type = 'CLOSEDAFTERREOPEN', claimant_cov_date, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))) AS v_claimant_cov_closed_after_reopen_date,
	v_claimant_cov_closed_after_reopen_date AS claimant_cov_closed_after_reopen_date,
	-- *INF*: IIF(ISNULL(v_claimant_cov_date_type),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IIF(v_claimant_cov_date_type= 'NOTICEONLY',claimant_cov_date,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')))
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,ISNULL(v_claimant_cov_date_type), TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	--    --                                 v_claimant_cov_date_type = 'N/A',TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),
	--       --                          v_claimant_cov_date_type= 'NOTICEONLY',claimant_cov_date)
	-- 
	-- 
	-- -- DECODE(TRUE, 
	-- -- v_claimant_cov_date_type = 'N/A' ,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- -- v_claimant_cov_date_type = 'NOTICEONLY',claimant_cov_date,
	-- -- (v_claimant_cov_date_type != 'N/A' OR v_claimant_cov_date_type != 'NOTICEONLY' ) AND (claimant_cov_det_ak_id = v_prev_row_claimant_cov_det_ak_id), v_prev_row_claimant_cov_noticeonly_date)
	-- 
	-- 
	-- 
	IFF(v_claimant_cov_date_type IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IFF(v_claimant_cov_date_type = 'NOTICEONLY', claimant_cov_date, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))) AS v_claimant_cov_noticeonly_date,
	v_claimant_cov_noticeonly_date AS claimant_cov_noticeonly_date,
	1 AS crrnt_snpsht_flag,
	-- *INF*: IIF(EDW_CCD_audit_id < 0 , EDW_CCD_audit_id,@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID})
	IFF(EDW_CCD_audit_id < 0, EDW_CCD_audit_id, @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}) AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	SQ_Claimant_Coverage_Sources.s3p_unit_type_code AS i_s3p_unit_type_code,
	-- *INF*: IIF(ISNULL(i_s3p_unit_type_code),'N/A',i_s3p_unit_type_code)
	IFF(i_s3p_unit_type_code IS NULL, 'N/A', i_s3p_unit_type_code) AS o_s3p_unit_type_code,
	SQ_Claimant_Coverage_Sources.reserve_ctgry AS i_pms_reserve_ctgry,
	-- *INF*: IIF(ISNULL(i_pms_reserve_ctgry),'N/A',i_pms_reserve_ctgry)
	IFF(i_pms_reserve_ctgry IS NULL, 'N/A', i_pms_reserve_ctgry) AS o_pms_reserve_ctgry,
	SQ_Claimant_Coverage_Sources.cause_of_loss AS i_pms_loss_cause,
	-- *INF*: IIF(ISNULL(i_pms_loss_cause),'N/A',i_pms_loss_cause)
	IFF(i_pms_loss_cause IS NULL, 'N/A', i_pms_loss_cause) AS o_pms_loss_cause,
	SQ_Claimant_Coverage_Sources.eff_from_date AS new_eff_from_date,
	SQ_Claimant_Coverage_Sources.claimant_cov_det_ak_id AS i_claimant_cov_det_ak_id,
	-- *INF*: IIF(ISNULL(i_claimant_cov_det_ak_id),-1,i_claimant_cov_det_ak_id)
	IFF(i_claimant_cov_det_ak_id IS NULL, - 1, i_claimant_cov_det_ak_id) AS o_claimant_cov_det_ak_id,
	SQ_Claimant_Coverage_Sources.major_peril_code,
	i_claimant_cov_det_ak_id AS v_prev_row_claimant_cov_det_ak_id,
	v_claimant_cov_open_date AS v_prev_row_claimant_cov_open_date,
	v_claimant_cov_close_date AS v_prev_row_claimant_cov_close_date,
	v_claimant_cov_reopen_date AS v_prev_row_claimant_cov_reopen_date,
	v_claimant_cov_closed_after_reopen_date AS v_prev_row_claimant_cov_closed_after_reopen_date,
	v_claimant_cov_noticeonly_date AS v_prev_row_claimant_cov_noticeonly_date,
	SQ_Claimant_Coverage_Sources.EDW_CCD_audit_id,
	LKP_Line_Of_Business.line_of_business,
	SQ_Claimant_Coverage_Sources.CauseOfLossID AS i_CauseOfLossID,
	-- *INF*: TO_BIGINT(i_CauseOfLossID)
	TO_BIGINT(i_CauseOfLossID) AS o_CauseOfLossID,
	SQ_Claimant_Coverage_Sources.PolicySourceID,
	SQ_Claimant_Coverage_Sources.CoverageForm,
	SQ_Claimant_Coverage_Sources.RiskType,
	SQ_Claimant_Coverage_Sources.CoverageType,
	SQ_Claimant_Coverage_Sources.CoverageVersion,
	SQ_Claimant_Coverage_Sources.ClassCode,
	SQ_Claimant_Coverage_Sources.SublineCode,
	SQ_Claimant_Coverage_Sources.ins_line,
	SQ_Claimant_Coverage_Sources.risk_unit_grp,
	SQ_Claimant_Coverage_Sources.risk_unit_grp_seq_num,
	SQ_Claimant_Coverage_Sources.risk_unit,
	SQ_Claimant_Coverage_Sources.risk_unit_seq_num,
	SQ_Claimant_Coverage_Sources.SupInsuranceLineID,
	SQ_Claimant_Coverage_Sources.SupMajorPerilID,
	SQ_Claimant_Coverage_Sources.SupTypeBureauCodeID,
	SQ_Claimant_Coverage_Sources.sup_risk_unit_grp_id,
	SQ_Claimant_Coverage_Sources.sup_risk_unit_id,
	SQ_Claimant_Coverage_Sources.pms_type_bureau_code,
	SQ_Claimant_Coverage_Sources.major_peril_seq,
	SQ_Claimant_Coverage_Sources.claimant_cov_eff_date AS i_claimant_cov_eff_date,
	-- *INF*: IIF(ISNULL(i_claimant_cov_eff_date),TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'),i_claimant_cov_eff_date)
	IFF(i_claimant_cov_eff_date IS NULL, TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'), i_claimant_cov_eff_date) AS o_claimant_cov_eff_date,
	SQ_Claimant_Coverage_Sources.unit_veh_registration_state_code,
	SQ_Claimant_Coverage_Sources.unit_vin_num,
	SQ_Claimant_Coverage_Sources.unit_veh_stated_amt,
	SQ_Claimant_Coverage_Sources.unit_veh_yr,
	SQ_Claimant_Coverage_Sources.unit_veh_make,
	SQ_Claimant_Coverage_Sources.spec_pers_prop_use_code,
	SQ_Claimant_Coverage_Sources.pkg_ded_amt,
	SQ_Claimant_Coverage_Sources.pkg_lmt_amt,
	SQ_Claimant_Coverage_Sources.loc_unit_num,
	SQ_Claimant_Coverage_Sources.sub_loc_unit_num,
	SQ_Claimant_Coverage_Sources.TypeOfLoss,
	SQ_Claimant_Coverage_Sources.ClaimTypeCategory,
	SQ_Claimant_Coverage_Sources.ClaimTypeGroup,
	SQ_Claimant_Coverage_Sources.SubrogationEligibleIndicator
	FROM SQ_Claimant_Coverage_Sources
	LEFT JOIN LKP_Line_Of_Business
	ON LKP_Line_Of_Business.claimant_cov_det_ak_id = SQ_Claimant_Coverage_Sources.claimant_cov_det_ak_id
	LEFT JOIN LKP_Sup_Claim_Pms_Loss_Disability
	ON LKP_Sup_Claim_Pms_Loss_Disability.loss_disability_code = SQ_Claimant_Coverage_Sources.pms_loss_disability
),
EXP_Dup_Flag AS (
	SELECT
	claimant_cov_det_id_out,
	claimant_cov_det_calculation_id_out,
	claimant_cov_det_reserve_calculation_id_D_out,
	claimant_cov_det_reserve_calculation_id_E_out,
	claimant_cov_det_reserve_calculation_id_S_out,
	claimant_cov_det_reserve_calculation_id_B_out,
	claimant_cov_det_reserve_calculation_id_R_out,
	-- *INF*: DECODE(TRUE,claimant_cov_det_id_out=-1 AND claimant_cov_det_calculation_id_out=-1 AND claimant_cov_det_reserve_calculation_id_D_out=-1 AND claimant_cov_det_reserve_calculation_id_E_out=-1 AND claimant_cov_det_reserve_calculation_id_S_out=-1 AND 
	-- claimant_cov_det_reserve_calculation_id_B_out=-1 AND claimant_cov_det_reserve_calculation_id_R_out=-1,'1','0')
	DECODE(TRUE,
	claimant_cov_det_id_out = - 1 AND claimant_cov_det_calculation_id_out = - 1 AND claimant_cov_det_reserve_calculation_id_D_out = - 1 AND claimant_cov_det_reserve_calculation_id_E_out = - 1 AND claimant_cov_det_reserve_calculation_id_S_out = - 1 AND claimant_cov_det_reserve_calculation_id_B_out = - 1 AND claimant_cov_det_reserve_calculation_id_R_out = - 1, '1',
	'0') AS Dup_Flag
	FROM EXP_Claimant_Coverage_Dim
),
FIL_INVALID_COMB_RES_NONRES_STATUS AS (
	SELECT
	EXP_Claimant_Coverage_Dim.claimant_cov_det_id_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_det_calculation_id_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_det_reserve_calculation_id_D_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_date_type_out1 AS claimant_cov_date_type_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_direct_loss_status_type_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_det_reserve_calculation_id_E_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_exp_status_type_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_det_reserve_calculation_id_S_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_salvage_status_type_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_det_reserve_calculation_id_B_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_subgrogation_status_type_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_det_reserve_calculation_id_R_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_other_recovery_status_type_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_supplemental_ind_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_financial_ind_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_recovery_ind_out, 
	EXP_Claimant_Coverage_Dim.claimant_cov_notice_only_ind_out, 
	EXP_Claimant_Coverage_Dim.o_s3p_wc_class_descript AS s3p_wc_class_descript, 
	EXP_Claimant_Coverage_Dim.o_pms_loss_disability_code AS in_pms_loss_disability_code, 
	EXP_Claimant_Coverage_Dim.loss_disability_descript_out, 
	EXP_Claimant_Coverage_Dim.o_s3p_unit_descript AS s3p_unit_descript, 
	EXP_Claimant_Coverage_Dim.o_unit_dam_descript AS unit_dam_descript, 
	EXP_Claimant_Coverage_Dim.claimant_cov_open_date, 
	EXP_Claimant_Coverage_Dim.claimant_cov_close_date, 
	EXP_Claimant_Coverage_Dim.claimant_cov_reopen_date, 
	EXP_Claimant_Coverage_Dim.claimant_cov_closed_after_reopen_date, 
	EXP_Claimant_Coverage_Dim.claimant_cov_noticeonly_date, 
	EXP_Claimant_Coverage_Dim.crrnt_snpsht_flag, 
	EXP_Claimant_Coverage_Dim.audit_id, 
	EXP_Claimant_Coverage_Dim.eff_to_date, 
	EXP_Claimant_Coverage_Dim.created_date, 
	EXP_Claimant_Coverage_Dim.modified_date, 
	EXP_Claimant_Coverage_Dim.o_s3p_unit_type_code AS s3p_unit_type_code1, 
	EXP_Claimant_Coverage_Dim.o_pms_reserve_ctgry AS pms_reserve_ctgry, 
	EXP_Claimant_Coverage_Dim.o_pms_loss_cause AS pms_loss_cause, 
	EXP_Claimant_Coverage_Dim.new_eff_from_date, 
	EXP_Claimant_Coverage_Dim.o_claimant_cov_det_ak_id AS claimant_cov_det_ak_id, 
	EXP_Claimant_Coverage_Dim.major_peril_code, 
	EXP_Claimant_Coverage_Dim.line_of_business, 
	EXP_Claimant_Coverage_Dim.o_CauseOfLossID AS CauseOfLossID, 
	EXP_Claimant_Coverage_Dim.PolicySourceID, 
	EXP_Claimant_Coverage_Dim.CoverageForm, 
	EXP_Claimant_Coverage_Dim.RiskType, 
	EXP_Claimant_Coverage_Dim.CoverageType, 
	EXP_Claimant_Coverage_Dim.CoverageVersion, 
	EXP_Claimant_Coverage_Dim.ClassCode, 
	EXP_Claimant_Coverage_Dim.SublineCode, 
	EXP_Claimant_Coverage_Dim.ins_line, 
	EXP_Claimant_Coverage_Dim.risk_unit_grp, 
	EXP_Claimant_Coverage_Dim.risk_unit_grp_seq_num, 
	EXP_Claimant_Coverage_Dim.risk_unit, 
	EXP_Claimant_Coverage_Dim.risk_unit_seq_num, 
	EXP_Claimant_Coverage_Dim.SupInsuranceLineID, 
	EXP_Claimant_Coverage_Dim.SupMajorPerilID, 
	EXP_Claimant_Coverage_Dim.SupTypeBureauCodeID, 
	EXP_Claimant_Coverage_Dim.sup_risk_unit_grp_id, 
	EXP_Claimant_Coverage_Dim.sup_risk_unit_id, 
	EXP_Claimant_Coverage_Dim.pms_type_bureau_code, 
	EXP_Claimant_Coverage_Dim.major_peril_seq, 
	EXP_Claimant_Coverage_Dim.o_claimant_cov_eff_date AS claimant_cov_eff_date, 
	EXP_Claimant_Coverage_Dim.unit_veh_registration_state_code, 
	EXP_Claimant_Coverage_Dim.unit_vin_num, 
	EXP_Claimant_Coverage_Dim.unit_veh_stated_amt, 
	EXP_Claimant_Coverage_Dim.unit_veh_yr, 
	EXP_Claimant_Coverage_Dim.unit_veh_make, 
	EXP_Claimant_Coverage_Dim.spec_pers_prop_use_code, 
	EXP_Claimant_Coverage_Dim.pkg_ded_amt, 
	EXP_Claimant_Coverage_Dim.pkg_lmt_amt, 
	EXP_Claimant_Coverage_Dim.loc_unit_num, 
	EXP_Claimant_Coverage_Dim.sub_loc_unit_num, 
	EXP_Dup_Flag.Dup_Flag, 
	EXP_Claimant_Coverage_Dim.TypeOfLoss, 
	EXP_Claimant_Coverage_Dim.ClaimTypeCategory, 
	EXP_Claimant_Coverage_Dim.ClaimTypeGroup, 
	EXP_Claimant_Coverage_Dim.SubrogationEligibleIndicator
	FROM EXP_Claimant_Coverage_Dim
	 -- Manually join with EXP_Dup_Flag
	WHERE DECODE(TRUE,
SUBSTR(claimant_cov_date_type_out,1,6) = 'CLOSED' AND 
(claimant_cov_direct_loss_status_type_out = 'OPEN' OR
claimant_cov_exp_status_type_out = 'OPEN' OR
claimant_cov_subgrogation_status_type_out = 'OPEN' OR
claimant_cov_salvage_status_type_out = 'OPEN' OR
claimant_cov_other_recovery_status_type_out = 'OPEN' OR 
claimant_cov_direct_loss_status_type_out = 'REOPEN' OR
claimant_cov_exp_status_type_out = 'REOPEN' OR
claimant_cov_subgrogation_status_type_out = 'REOPEN' OR
claimant_cov_salvage_status_type_out = 'REOPEN' OR
claimant_cov_other_recovery_status_type_out = 'REOPEN'), FALSE,


(SUBSTR(claimant_cov_date_type_out,1,4) = 'OPEN' OR SUBSTR(claimant_cov_date_type_out,1,6) = 'REOPEN' OR SUBSTR(claimant_cov_date_type_out,1,6) = 'CLOSED')
AND
(
SUBSTR(claimant_cov_direct_loss_status_type_out,1,6) = 'N/A' AND
SUBSTR(claimant_cov_exp_status_type_out,1,6) = 'N/A' AND
SUBSTR(claimant_cov_subgrogation_status_type_out,1,6) = 'N/A' AND
SUBSTR(claimant_cov_salvage_status_type_out,1,6) = 'N/A' AND
SUBSTR(claimant_cov_other_recovery_status_type_out,1,6) = 'N/A'
), TRUE,


(SUBSTR(claimant_cov_date_type_out,1,4) = 'OPEN' OR SUBSTR(claimant_cov_date_type_out,1,6) = 'REOPEN')
AND
(
(SUBSTR(claimant_cov_direct_loss_status_type_out,1,6) = 'CLOSED' OR claimant_cov_direct_loss_status_type_out = 'N/A'  OR claimant_cov_direct_loss_status_type_out = 'NOTICEONLY')
AND
(SUBSTR(claimant_cov_exp_status_type_out,1,6) = 'CLOSED' OR claimant_cov_exp_status_type_out = 'N/A' OR claimant_cov_exp_status_type_out = 'NOTICEONLY')
AND
(SUBSTR(claimant_cov_subgrogation_status_type_out,1,6) = 'CLOSED' OR claimant_cov_subgrogation_status_type_out = 'N/A' OR claimant_cov_subgrogation_status_type_out = 'NOTICEONLY')
AND
(SUBSTR(claimant_cov_salvage_status_type_out,1,6) = 'CLOSED' OR claimant_cov_salvage_status_type_out = 'N/A' OR claimant_cov_salvage_status_type_out = 'NOTICEONLY')
AND
(SUBSTR(claimant_cov_other_recovery_status_type_out,1,6) = 'CLOSED' OR claimant_cov_other_recovery_status_type_out = 'N/A' OR claimant_cov_other_recovery_status_type_out = 'NOTICEONLY')
), FALSE,


claimant_cov_date_type_out = 'NOTICEONLY' AND 
(
claimant_cov_direct_loss_status_type_out = 'OPEN' OR 
claimant_cov_direct_loss_status_type_out = 'REOPEN' OR 
SUBSTR(claimant_cov_direct_loss_status_type_out,1,6) = 'CLOSED' OR 
claimant_cov_exp_status_type_out = 'OPEN' OR 
claimant_cov_exp_status_type_out = 'REOPEN' OR 
SUBSTR(claimant_cov_exp_status_type_out,1,6) = 'CLOSED' OR 
claimant_cov_subgrogation_status_type_out = 'OPEN' OR 
claimant_cov_subgrogation_status_type_out = 'REOPEN' OR 
SUBSTR(claimant_cov_subgrogation_status_type_out,1,6) = 'CLOSED' OR 
claimant_cov_salvage_status_type_out = 'OPEN' OR 
claimant_cov_salvage_status_type_out = 'REOPEN' OR 
SUBSTR(claimant_cov_salvage_status_type_out,1,6) = 'CLOSED' OR 
claimant_cov_other_recovery_status_type_out = 'OPEN' OR 
claimant_cov_other_recovery_status_type_out = 'REOPEN' OR 
SUBSTR(claimant_cov_other_recovery_status_type_out,1,6) = 'CLOSED'  
)
,FALSE,

SUBSTR(claimant_cov_date_type_out,1,6) = 'CLOSED' AND
SUBSTR(claimant_cov_direct_loss_status_type_out,1,6) <> 'CLOSED' AND 
SUBSTR(claimant_cov_exp_status_type_out,1,6) <> 'CLOSED' AND 
SUBSTR(claimant_cov_salvage_status_type_out,1,6) <> 'CLOSED' AND 
SUBSTR(claimant_cov_subgrogation_status_type_out,1,6) <> 'CLOSED' AND 
SUBSTR(claimant_cov_other_recovery_status_type_out,1,6) <> 'CLOSED' , FALSE, 
TRUE) AND Dup_Flag='0'

//----------------------------------------------------------------------------------------------------
//This expression eliminates any invalid combination of reserve and non-reserve status.
//If CC Status is CLOSED AND if any of the other fin typ code status is either open or reopen then 
//filter the row out.
// If CC Status is Open or Reopen AND if any of the other fin typ code status is either open or reopen 
//then DON'T filter the row out.
// If CC Status is NOTICEONLY AND all the other fin typ code status also NOTICEONLY or N/A then 
//DON'T filter the row out
//If NR Status is Closed and if none of the other fin type status is closed, then filter the row
//----------------------------------------------------------------------------------------------------
),
LKP_Sup_CauseOfLoss AS (
	SELECT
	CauseOfLossName,
	CauseOfLossId
	FROM (
		SELECT 
			CauseOfLossName,
			CauseOfLossId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_CauseOfLoss
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CauseOfLossId ORDER BY CauseOfLossName DESC) = 1
),
LKP_Sup_Claim_Reserve_Category AS (
	SELECT
	reserve_ctgry_descript,
	reserve_ctgry_code
	FROM (
		SELECT 
			reserve_ctgry_descript,
			reserve_ctgry_code
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_reserve_category
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY reserve_ctgry_code ORDER BY reserve_ctgry_descript) = 1
),
LKP_Sup_Insurance_Line AS (
	SELECT
	StandardInsuranceLineDescription,
	sup_ins_line_id
	FROM (
		SELECT 
			StandardInsuranceLineDescription,
			sup_ins_line_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_insurance_line
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_ins_line_id ORDER BY StandardInsuranceLineDescription) = 1
),
LKP_Sup_Major_Peril AS (
	SELECT
	StandardMajorPerilDescription,
	sup_major_peril_id
	FROM (
		SELECT 
			StandardMajorPerilDescription,
			sup_major_peril_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_major_peril
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_major_peril_id ORDER BY StandardMajorPerilDescription) = 1
),
LKP_Sup_Risk_Unit AS (
	SELECT
	StandardRiskUnitDescription,
	sup_risk_unit_id
	FROM (
		SELECT 
			StandardRiskUnitDescription,
			sup_risk_unit_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_risk_unit
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_risk_unit_id ORDER BY StandardRiskUnitDescription) = 1
),
LKP_Sup_Risk_Unit_Group AS (
	SELECT
	risk_unit_grp_descript,
	IN_sup_risk_unit_grp_id,
	sup_risk_unit_grp_id
	FROM (
		SELECT 
			risk_unit_grp_descript,
			IN_sup_risk_unit_grp_id,
			sup_risk_unit_grp_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_risk_unit_group
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_risk_unit_grp_id ORDER BY risk_unit_grp_descript) = 1
),
LKP_Sup_Type_Bureau_Code AS (
	SELECT
	StandardTypeBureauCode,
	StandardTypeBureauCodeShortDescription,
	StandardTypeBureauCodeLongDescription,
	sup_type_bureau_code_id
	FROM (
		SELECT 
			StandardTypeBureauCode,
			StandardTypeBureauCodeShortDescription,
			StandardTypeBureauCodeLongDescription,
			sup_type_bureau_code_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_type_bureau_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_type_bureau_code_id ORDER BY StandardTypeBureauCode) = 1
),
EXP_Verify_Lookup_Results AS (
	SELECT
	LKP_Sup_CauseOfLoss.CauseOfLossName,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(CauseOfLossName))),'N/A',RTRIM(LTRIM(CauseOfLossName)))
	IFF(RTRIM(LTRIM(CauseOfLossName)) IS NULL, 'N/A', RTRIM(LTRIM(CauseOfLossName))) AS OUT_CauseOfLossName,
	LKP_Sup_Insurance_Line.StandardInsuranceLineDescription,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(StandardInsuranceLineDescription))),'N/A',RTRIM(LTRIM(StandardInsuranceLineDescription)))
	IFF(RTRIM(LTRIM(StandardInsuranceLineDescription)) IS NULL, 'N/A', RTRIM(LTRIM(StandardInsuranceLineDescription))) AS OUT_StandardInsuranceLineDescription,
	LKP_Sup_Major_Peril.StandardMajorPerilDescription,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(StandardMajorPerilDescription))),'N/A',RTRIM(LTRIM(StandardMajorPerilDescription)))
	IFF(RTRIM(LTRIM(StandardMajorPerilDescription)) IS NULL, 'N/A', RTRIM(LTRIM(StandardMajorPerilDescription))) AS OUT_StandardMajorPerilDescription,
	LKP_Sup_Type_Bureau_Code.StandardTypeBureauCode,
	LKP_Sup_Type_Bureau_Code.StandardTypeBureauCodeShortDescription,
	LKP_Sup_Type_Bureau_Code.StandardTypeBureauCodeLongDescription,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(StandardTypeBureauCode))),'N/A',RTRIM(LTRIM(StandardTypeBureauCode)))
	IFF(RTRIM(LTRIM(StandardTypeBureauCode)) IS NULL, 'N/A', RTRIM(LTRIM(StandardTypeBureauCode))) AS OUT_StandardTypeBureauCode,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(StandardTypeBureauCodeShortDescription))),'N/A',RTRIM(LTRIM(StandardTypeBureauCodeShortDescription)))
	IFF(RTRIM(LTRIM(StandardTypeBureauCodeShortDescription)) IS NULL, 'N/A', RTRIM(LTRIM(StandardTypeBureauCodeShortDescription))) AS OUT_StandardTypeBureauCodeShortDescription,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(StandardTypeBureauCodeLongDescription))),'N/A',RTRIM(LTRIM(StandardTypeBureauCodeLongDescription)))
	IFF(RTRIM(LTRIM(StandardTypeBureauCodeLongDescription)) IS NULL, 'N/A', RTRIM(LTRIM(StandardTypeBureauCodeLongDescription))) AS OUT_StandardTypeBureauCodeLongDescription,
	LKP_Sup_Risk_Unit_Group.risk_unit_grp_descript,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(risk_unit_grp_descript))),'N/A',RTRIM(LTRIM(risk_unit_grp_descript)))
	IFF(RTRIM(LTRIM(risk_unit_grp_descript)) IS NULL, 'N/A', RTRIM(LTRIM(risk_unit_grp_descript))) AS OUT_risk_unit_grp_descript,
	LKP_Sup_Risk_Unit.StandardRiskUnitDescription,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(StandardRiskUnitDescription))),'N/A',RTRIM(LTRIM(StandardRiskUnitDescription)))
	IFF(RTRIM(LTRIM(StandardRiskUnitDescription)) IS NULL, 'N/A', RTRIM(LTRIM(StandardRiskUnitDescription))) AS OUT_StandardRiskUnitDescription,
	LKP_Sup_Claim_Reserve_Category.reserve_ctgry_descript,
	-- *INF*: IIF(ISNULL(RTRIM(LTRIM(reserve_ctgry_descript))),'N/A',RTRIM(LTRIM(reserve_ctgry_descript)))
	IFF(RTRIM(LTRIM(reserve_ctgry_descript)) IS NULL, 'N/A', RTRIM(LTRIM(reserve_ctgry_descript))) AS OUT_reserve_ctgry_descript
	FROM 
	LEFT JOIN LKP_Sup_CauseOfLoss
	ON LKP_Sup_CauseOfLoss.CauseOfLossId = FIL_INVALID_COMB_RES_NONRES_STATUS.CauseOfLossID
	LEFT JOIN LKP_Sup_Claim_Reserve_Category
	ON LKP_Sup_Claim_Reserve_Category.reserve_ctgry_code = FIL_INVALID_COMB_RES_NONRES_STATUS.pms_reserve_ctgry
	LEFT JOIN LKP_Sup_Insurance_Line
	ON LKP_Sup_Insurance_Line.sup_ins_line_id = FIL_INVALID_COMB_RES_NONRES_STATUS.SupInsuranceLineID
	LEFT JOIN LKP_Sup_Major_Peril
	ON LKP_Sup_Major_Peril.sup_major_peril_id = FIL_INVALID_COMB_RES_NONRES_STATUS.SupMajorPerilID
	LEFT JOIN LKP_Sup_Risk_Unit
	ON LKP_Sup_Risk_Unit.sup_risk_unit_id = FIL_INVALID_COMB_RES_NONRES_STATUS.sup_risk_unit_id
	LEFT JOIN LKP_Sup_Risk_Unit_Group
	ON LKP_Sup_Risk_Unit_Group.sup_risk_unit_grp_id = FIL_INVALID_COMB_RES_NONRES_STATUS.sup_risk_unit_grp_id
	LEFT JOIN LKP_Sup_Type_Bureau_Code
	ON LKP_Sup_Type_Bureau_Code.sup_type_bureau_code_id = FIL_INVALID_COMB_RES_NONRES_STATUS.SupTypeBureauCodeID
),
LKP_Claim_Total_Loss AS (
	SELECT
	claim_total_loss_ak_id,
	claimant_cov_det_ak_id
	FROM (
		SELECT 
			claim_total_loss_ak_id,
			claimant_cov_det_ak_id
		FROM claim_total_loss
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY claim_total_loss_ak_id) = 1
),
EXP_claim_total_loss_ind AS (
	SELECT
	claim_total_loss_ak_id,
	-- *INF*: IIF(ISNULL(claim_total_loss_ak_id), 'Y', 'N')
	IFF(claim_total_loss_ak_id IS NULL, 'Y', 'N') AS claim_total_loss_ind
	FROM LKP_Claim_Total_Loss
),
LKP_Claimant_Coverage_Dim AS (
	SELECT
	claimant_cov_dim_id,
	edw_claimant_cov_det_pk_id,
	edw_claimant_cov_det_calculation_pk_id,
	edw_claimant_cov_det_reserve_calculation_direct_loss_pk_id,
	edw_claimant_cov_det_reserve_calculation_exp_pk_id,
	edw_claimant_cov_det_reserve_calculation_subrogation_pk_id,
	edw_claimant_cov_det_reserve_calculation_salvage_pk_id,
	edw_claimant_cov_det_reserve_calculation_other_recovery_pk_id
	FROM (
		SELECT 
			claimant_cov_dim_id,
			edw_claimant_cov_det_pk_id,
			edw_claimant_cov_det_calculation_pk_id,
			edw_claimant_cov_det_reserve_calculation_direct_loss_pk_id,
			edw_claimant_cov_det_reserve_calculation_exp_pk_id,
			edw_claimant_cov_det_reserve_calculation_subrogation_pk_id,
			edw_claimant_cov_det_reserve_calculation_salvage_pk_id,
			edw_claimant_cov_det_reserve_calculation_other_recovery_pk_id
		FROM claimant_coverage_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_pk_id,edw_claimant_cov_det_calculation_pk_id,edw_claimant_cov_det_reserve_calculation_direct_loss_pk_id,edw_claimant_cov_det_reserve_calculation_exp_pk_id,edw_claimant_cov_det_reserve_calculation_subrogation_pk_id,edw_claimant_cov_det_reserve_calculation_salvage_pk_id,edw_claimant_cov_det_reserve_calculation_other_recovery_pk_id ORDER BY claimant_cov_dim_id) = 1
),
LKP_Sup_Claim_Cause_Of_Loss AS (
	SELECT
	cause_of_loss_long_descript,
	major_peril_code,
	cause_of_loss_code,
	reserve_ctgry_code
	FROM (
		SELECT 
		A.cause_of_loss_long_descript as cause_of_loss_long_descript, 
		RTRIM(A.major_peril_code) as major_peril_code, 
		RTRIM(A.cause_of_loss_code) as cause_of_loss_code, 
		RTRIM(A.reserve_ctgry_code) as reserve_ctgry_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_cause_of_loss A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril_code,cause_of_loss_code,reserve_ctgry_code ORDER BY cause_of_loss_long_descript) = 1
),
LKP_Unit_Type_Desc AS (
	SELECT
	s3p_unit_type_code1,
	cov_unit_code,
	cov_unit_descript
	FROM (
		SELECT 
		sup_coverage_unit.cov_unit_descript as cov_unit_descript, 
		RTRIM(sup_coverage_unit.cov_unit_code) as cov_unit_code 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_coverage_unit sup_coverage_unit
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cov_unit_code ORDER BY s3p_unit_type_code1) = 1
),
RTR_claimant_coverage_dim AS (
	SELECT
	LKP_Claimant_Coverage_Dim.claimant_cov_dim_id,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_id_out AS in_claimant_cov_det_id,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_calculation_id_out AS in_claimant_cov_det_calculation_id,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_D_out AS in_claimant_cov_det_reserve_calculation_id_D,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_E_out AS in_claimant_cov_det_reserve_calculation_id_E,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_S_out AS in_claimant_cov_det_reserve_calculation_id_S,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_B_out AS in_claimant_cov_det_reserve_calculation_id_B,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_R_out AS in_claimant_cov_det_reserve_calculation_id_R,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_date_type_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_direct_loss_status_type_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_exp_status_type_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_salvage_status_type_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_subgrogation_status_type_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_other_recovery_status_type_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_supplemental_ind_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_financial_ind_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_recovery_ind_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_notice_only_ind_out,
	FIL_INVALID_COMB_RES_NONRES_STATUS.s3p_wc_class_descript,
	FIL_INVALID_COMB_RES_NONRES_STATUS.in_pms_loss_disability_code,
	FIL_INVALID_COMB_RES_NONRES_STATUS.loss_disability_descript_out AS in_loss_disability_descript,
	FIL_INVALID_COMB_RES_NONRES_STATUS.unit_dam_descript,
	FIL_INVALID_COMB_RES_NONRES_STATUS.s3p_unit_descript AS s3p_unit_description,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_open_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_close_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_reopen_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_closed_after_reopen_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_noticeonly_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.crrnt_snpsht_flag,
	FIL_INVALID_COMB_RES_NONRES_STATUS.audit_id,
	FIL_INVALID_COMB_RES_NONRES_STATUS.new_eff_from_date AS eff_from_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.eff_to_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.created_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.modified_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.s3p_unit_type_code1,
	LKP_Unit_Type_Desc.cov_unit_descript,
	FIL_INVALID_COMB_RES_NONRES_STATUS.pms_reserve_ctgry,
	FIL_INVALID_COMB_RES_NONRES_STATUS.pms_loss_cause,
	LKP_Sup_Claim_Cause_Of_Loss.cause_of_loss_long_descript,
	EXP_Verify_Lookup_Results.OUT_reserve_ctgry_descript AS reserve_ctgry_descript,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_ak_id,
	EXP_claim_total_loss_ind.claim_total_loss_ind,
	EXP_Verify_Lookup_Results.OUT_CauseOfLossName AS CauseOfLossName,
	FIL_INVALID_COMB_RES_NONRES_STATUS.PolicySourceID,
	FIL_INVALID_COMB_RES_NONRES_STATUS.CoverageForm,
	FIL_INVALID_COMB_RES_NONRES_STATUS.RiskType,
	FIL_INVALID_COMB_RES_NONRES_STATUS.CoverageType,
	FIL_INVALID_COMB_RES_NONRES_STATUS.CoverageVersion,
	FIL_INVALID_COMB_RES_NONRES_STATUS.ClassCode,
	FIL_INVALID_COMB_RES_NONRES_STATUS.SublineCode,
	FIL_INVALID_COMB_RES_NONRES_STATUS.major_peril_code,
	FIL_INVALID_COMB_RES_NONRES_STATUS.ins_line,
	FIL_INVALID_COMB_RES_NONRES_STATUS.risk_unit_grp,
	FIL_INVALID_COMB_RES_NONRES_STATUS.risk_unit_grp_seq_num,
	FIL_INVALID_COMB_RES_NONRES_STATUS.risk_unit,
	FIL_INVALID_COMB_RES_NONRES_STATUS.risk_unit_seq_num,
	EXP_Verify_Lookup_Results.OUT_StandardInsuranceLineDescription AS StandardInsuranceLineDescription,
	EXP_Verify_Lookup_Results.OUT_StandardMajorPerilDescription AS StandardMajorPerilDescription,
	EXP_Verify_Lookup_Results.OUT_StandardTypeBureauCodeShortDescription AS StandardTypeBureauCodeShortDescription,
	EXP_Verify_Lookup_Results.OUT_StandardTypeBureauCodeLongDescription AS StandardTypeBureauCodeLongDescription,
	EXP_Verify_Lookup_Results.OUT_risk_unit_grp_descript AS StandardRiskUnitGroupDescription,
	EXP_Verify_Lookup_Results.OUT_StandardRiskUnitDescription AS StandardRiskUnitDescription,
	EXP_Verify_Lookup_Results.OUT_StandardTypeBureauCode AS pms_type_bureau_code,
	FIL_INVALID_COMB_RES_NONRES_STATUS.major_peril_seq,
	FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_eff_date,
	FIL_INVALID_COMB_RES_NONRES_STATUS.unit_veh_registration_state_code,
	FIL_INVALID_COMB_RES_NONRES_STATUS.unit_vin_num,
	FIL_INVALID_COMB_RES_NONRES_STATUS.unit_veh_stated_amt,
	FIL_INVALID_COMB_RES_NONRES_STATUS.unit_veh_yr,
	FIL_INVALID_COMB_RES_NONRES_STATUS.unit_veh_make,
	FIL_INVALID_COMB_RES_NONRES_STATUS.spec_pers_prop_use_code,
	FIL_INVALID_COMB_RES_NONRES_STATUS.pkg_ded_amt,
	FIL_INVALID_COMB_RES_NONRES_STATUS.pkg_lmt_amt,
	FIL_INVALID_COMB_RES_NONRES_STATUS.loc_unit_num,
	FIL_INVALID_COMB_RES_NONRES_STATUS.sub_loc_unit_num,
	FIL_INVALID_COMB_RES_NONRES_STATUS.TypeOfLoss,
	FIL_INVALID_COMB_RES_NONRES_STATUS.ClaimTypeCategory,
	FIL_INVALID_COMB_RES_NONRES_STATUS.ClaimTypeGroup,
	FIL_INVALID_COMB_RES_NONRES_STATUS.SubrogationEligibleIndicator
	FROM EXP_Verify_Lookup_Results
	 -- Manually join with EXP_claim_total_loss_ind
	 -- Manually join with FIL_INVALID_COMB_RES_NONRES_STATUS
	LEFT JOIN LKP_Claimant_Coverage_Dim
	ON LKP_Claimant_Coverage_Dim.edw_claimant_cov_det_pk_id = FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_id_out AND LKP_Claimant_Coverage_Dim.edw_claimant_cov_det_calculation_pk_id = FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_calculation_id_out AND LKP_Claimant_Coverage_Dim.edw_claimant_cov_det_reserve_calculation_direct_loss_pk_id = FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_D_out AND LKP_Claimant_Coverage_Dim.edw_claimant_cov_det_reserve_calculation_exp_pk_id = FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_E_out AND LKP_Claimant_Coverage_Dim.edw_claimant_cov_det_reserve_calculation_subrogation_pk_id = FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_B_out AND LKP_Claimant_Coverage_Dim.edw_claimant_cov_det_reserve_calculation_salvage_pk_id = FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_S_out AND LKP_Claimant_Coverage_Dim.edw_claimant_cov_det_reserve_calculation_other_recovery_pk_id = FIL_INVALID_COMB_RES_NONRES_STATUS.claimant_cov_det_reserve_calculation_id_R_out
	LEFT JOIN LKP_Sup_Claim_Cause_Of_Loss
	ON LKP_Sup_Claim_Cause_Of_Loss.major_peril_code = FIL_INVALID_COMB_RES_NONRES_STATUS.major_peril_code AND LKP_Sup_Claim_Cause_Of_Loss.cause_of_loss_code = FIL_INVALID_COMB_RES_NONRES_STATUS.pms_loss_cause AND LKP_Sup_Claim_Cause_Of_Loss.reserve_ctgry_code = FIL_INVALID_COMB_RES_NONRES_STATUS.pms_reserve_ctgry
	LEFT JOIN LKP_Unit_Type_Desc
	ON LKP_Unit_Type_Desc.cov_unit_code = FIL_INVALID_COMB_RES_NONRES_STATUS.s3p_unit_type_code1
),
RTR_claimant_coverage_dim_INSERT AS (SELECT * FROM RTR_claimant_coverage_dim WHERE isnull(claimant_cov_dim_id)),
RTR_claimant_coverage_dim_DEFAULT1 AS (SELECT * FROM RTR_claimant_coverage_dim WHERE NOT ( (isnull(claimant_cov_dim_id)) )),
UPD_claimant_coverage_dim_INSERT AS (
	SELECT
	claimant_cov_dim_id AS claimant_cov_dim_id1, 
	in_claimant_cov_det_id AS in_claimant_cov_det_id1, 
	in_claimant_cov_det_calculation_id AS in_claimant_cov_det_calculation_id1, 
	in_claimant_cov_det_reserve_calculation_id_D AS in_claimant_cov_det_reserve_calculation_id_D1, 
	in_claimant_cov_det_reserve_calculation_id_E AS in_claimant_cov_det_reserve_calculation_id_E1, 
	in_claimant_cov_det_reserve_calculation_id_S AS in_claimant_cov_det_reserve_calculation_id_S1, 
	in_claimant_cov_det_reserve_calculation_id_B AS in_claimant_cov_det_reserve_calculation_id_B1, 
	in_claimant_cov_det_reserve_calculation_id_R AS in_claimant_cov_det_reserve_calculation_id_R1, 
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id1, 
	claimant_cov_date_type_out, 
	claimant_cov_direct_loss_status_type_out, 
	claimant_cov_exp_status_type_out, 
	claimant_cov_salvage_status_type_out, 
	claimant_cov_subgrogation_status_type_out, 
	claimant_cov_other_recovery_status_type_out, 
	claimant_cov_supplemental_ind_out AS claimant_cov_supplemental_ind_out1, 
	claimant_cov_financial_ind_out AS claimant_cov_financial_ind_out1, 
	claimant_cov_recovery_ind_out AS claimant_cov_recovery_ind_out1, 
	claimant_cov_notice_only_ind_out AS claimant_cov_notice_only_ind_out1, 
	s3p_wc_class_descript AS s3p_wc_class_descript1, 
	s3p_unit_type_code AS s3p_unit_type_code11, 
	cov_unit_descript, 
	pms_reserve_ctgry AS pms_reserve_ctgry1, 
	pms_loss_cause AS pms_loss_cause1, 
	cause_of_loss_long_descript AS cause_of_loss_long_descript1, 
	reserve_ctgry_descript AS reserve_ctgry_descript1, 
	in_pms_loss_disability_code AS in_pms_loss_disability_code1, 
	in_loss_disability_descript AS lkp_loss_disability_descript1, 
	unit_dam_descript AS unit_dam_descript1, 
	s3p_unit_description AS s3p_unit_description1, 
	claimant_cov_open_date AS claimant_cov_open_date1, 
	claimant_cov_close_date AS claimant_cov_close_date1, 
	claimant_cov_reopen_date AS claimant_cov_reopen_date1, 
	claimant_cov_closed_after_reopen_date AS claimant_cov_closed_after_reopen_date1, 
	claimant_cov_noticeonly_date, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	claim_total_loss_ind AS claim_total_loss_ind1, 
	CauseOfLossName AS CauseOfLossName1, 
	PolicySourceID AS PolicySourceID1, 
	CoverageForm AS CoverageForm1, 
	RiskType AS RiskType1, 
	CoverageType AS CoverageType1, 
	CoverageVersion AS CoverageVersion1, 
	ClassCode AS ClassCode1, 
	SublineCode AS SublineCode1, 
	major_peril_code AS major_peril_code1, 
	ins_line AS ins_line1, 
	risk_unit_grp AS risk_unit_grp1, 
	risk_unit_grp_seq_num AS risk_unit_grp_seq_num1, 
	risk_unit AS risk_unit1, 
	risk_unit_seq_num AS risk_unit_seq_num1, 
	StandardInsuranceLineDescription AS StandardInsuranceLineDescription1, 
	StandardMajorPerilDescription AS StandardMajorPerilDescription1, 
	StandardTypeBureauCodeShortDescription AS StandardTypeBureauCodeShortDescription1, 
	StandardTypeBureauCodeLongDescription AS StandardTypeBureauCodeLongDescription1, 
	StandardRiskUnitGroupDescription AS StandardRiskUnitGroupDescription1, 
	StandardRiskUnitDescription AS StandardRiskUnitDescription1, 
	pms_type_bureau_code AS pms_type_bureau_code1, 
	major_peril_seq AS major_peril_seq1, 
	claimant_cov_eff_date AS claimant_cov_eff_date1, 
	unit_veh_registration_state_code AS unit_veh_registration_state_code1, 
	unit_vin_num AS unit_vin_num1, 
	unit_veh_stated_amt AS unit_veh_stated_amt1, 
	unit_veh_yr AS unit_veh_yr1, 
	unit_veh_make AS unit_veh_make1, 
	spec_pers_prop_use_code AS spec_pers_prop_use_code1, 
	pkg_ded_amt AS pkg_ded_amt1, 
	pkg_lmt_amt AS pkg_lmt_amt1, 
	loc_unit_num AS loc_unit_num1, 
	sub_loc_unit_num AS sub_loc_unit_num1, 
	TypeOfLoss, 
	ClaimTypeCategory AS ClaimTypeCategory1, 
	ClaimTypeGroup AS ClaimTypeGroup1, 
	SubrogationEligibleIndicator AS SubrogationEligibleIndicator1
	FROM RTR_claimant_coverage_dim_INSERT
),
claimant_coverage_dim_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_dim
	(edw_claimant_cov_det_pk_id, edw_claimant_cov_det_calculation_pk_id, edw_claimant_cov_det_reserve_calculation_direct_loss_pk_id, edw_claimant_cov_det_reserve_calculation_exp_pk_id, edw_claimant_cov_det_reserve_calculation_subrogation_pk_id, edw_claimant_cov_det_reserve_calculation_salvage_pk_id, edw_claimant_cov_det_reserve_calculation_other_recovery_pk_id, edw_claimant_cov_det_ak_id, claimant_cov_status_type, claimant_cov_direct_loss_status_type, claimant_cov_exp_status_type, claimant_cov_salvage_status_type, claimant_cov_subrogation_status_type, claimant_cov_other_recovery_status_type, claimant_cov_financial_ind, claimant_cov_supplemental_ind, claimant_cov_recovery_ind, claimant_cov_notice_only_claim_ind, wc_class_descript, unit_type_code, unit_type_descript, unit_descript, cause_of_loss, cause_of_loss_long_descript, reserve_ctgry, reserve_ctgry_descript, loss_disability, loss_disability_descript, unit_dam_descript, claimant_cov_open_date, claimant_cov_close_date, claimant_cov_reopen_date, claimant_cov_closed_after_reopen_date, claimant_cov_notice_only_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, claim_total_loss_ind, PolicySourceID, CoverageForm, RiskType, CoverageType, CoverageVersion, ClassCode, SublineCode, InsuranceLineCode, InsuranceLineCodeDescription, MajorPerilCode, MajorPerilCodeDescription, MajorPerilSequenceNumber, TypeBureauCode, TypeBureauCodeShortDescription, TypeBureauCodeLongDescription, RiskUnitGroupCode, RiskUnitGroupCodeDescription, RiskUnitGroupSequenceNumber, RiskUnitCode, RiskUnitCodeDescription, RiskUnitSequenceNumber, CoverageEffectiveDate, LocationUnitNumber, SubLocationUnitNumber, UnitVehicleRegistrationStateCode, UnitVinNumber, UnitVehicleStateAmount, UnitVehicleYear, UnitVehicleMake, SpecialPersonalPropertyUseCode, PackageDeductibleAmount, PackageLimitAmount, TypeOfLoss, ClaimTypeCategory, ClaimTypeGroup, SubrogationEligibleIndicator)
	SELECT 
	in_claimant_cov_det_id1 AS EDW_CLAIMANT_COV_DET_PK_ID, 
	in_claimant_cov_det_calculation_id1 AS EDW_CLAIMANT_COV_DET_CALCULATION_PK_ID, 
	in_claimant_cov_det_reserve_calculation_id_D1 AS EDW_CLAIMANT_COV_DET_RESERVE_CALCULATION_DIRECT_LOSS_PK_ID, 
	in_claimant_cov_det_reserve_calculation_id_E1 AS EDW_CLAIMANT_COV_DET_RESERVE_CALCULATION_EXP_PK_ID, 
	in_claimant_cov_det_reserve_calculation_id_B1 AS EDW_CLAIMANT_COV_DET_RESERVE_CALCULATION_SUBROGATION_PK_ID, 
	in_claimant_cov_det_reserve_calculation_id_S1 AS EDW_CLAIMANT_COV_DET_RESERVE_CALCULATION_SALVAGE_PK_ID, 
	in_claimant_cov_det_reserve_calculation_id_R1 AS EDW_CLAIMANT_COV_DET_RESERVE_CALCULATION_OTHER_RECOVERY_PK_ID, 
	claimant_cov_det_ak_id1 AS EDW_CLAIMANT_COV_DET_AK_ID, 
	claimant_cov_date_type_out AS CLAIMANT_COV_STATUS_TYPE, 
	claimant_cov_direct_loss_status_type_out AS CLAIMANT_COV_DIRECT_LOSS_STATUS_TYPE, 
	claimant_cov_exp_status_type_out AS CLAIMANT_COV_EXP_STATUS_TYPE, 
	claimant_cov_salvage_status_type_out AS CLAIMANT_COV_SALVAGE_STATUS_TYPE, 
	claimant_cov_subgrogation_status_type_out AS CLAIMANT_COV_SUBROGATION_STATUS_TYPE, 
	claimant_cov_other_recovery_status_type_out AS CLAIMANT_COV_OTHER_RECOVERY_STATUS_TYPE, 
	claimant_cov_financial_ind_out1 AS CLAIMANT_COV_FINANCIAL_IND, 
	claimant_cov_supplemental_ind_out1 AS CLAIMANT_COV_SUPPLEMENTAL_IND, 
	claimant_cov_recovery_ind_out1 AS CLAIMANT_COV_RECOVERY_IND, 
	claimant_cov_notice_only_ind_out1 AS CLAIMANT_COV_NOTICE_ONLY_CLAIM_IND, 
	s3p_wc_class_descript1 AS WC_CLASS_DESCRIPT, 
	s3p_unit_type_code11 AS UNIT_TYPE_CODE, 
	cov_unit_descript AS UNIT_TYPE_DESCRIPT, 
	s3p_unit_description1 AS UNIT_DESCRIPT, 
	pms_loss_cause1 AS CAUSE_OF_LOSS, 
	CauseOfLossName1 AS CAUSE_OF_LOSS_LONG_DESCRIPT, 
	pms_reserve_ctgry1 AS RESERVE_CTGRY, 
	reserve_ctgry_descript1 AS RESERVE_CTGRY_DESCRIPT, 
	in_pms_loss_disability_code1 AS LOSS_DISABILITY, 
	lkp_loss_disability_descript1 AS LOSS_DISABILITY_DESCRIPT, 
	unit_dam_descript1 AS UNIT_DAM_DESCRIPT, 
	claimant_cov_open_date1 AS CLAIMANT_COV_OPEN_DATE, 
	claimant_cov_close_date1 AS CLAIMANT_COV_CLOSE_DATE, 
	claimant_cov_reopen_date1 AS CLAIMANT_COV_REOPEN_DATE, 
	claimant_cov_closed_after_reopen_date1 AS CLAIMANT_COV_CLOSED_AFTER_REOPEN_DATE, 
	claimant_cov_noticeonly_date AS CLAIMANT_COV_NOTICE_ONLY_DATE, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	claim_total_loss_ind1 AS CLAIM_TOTAL_LOSS_IND, 
	PolicySourceID1 AS POLICYSOURCEID, 
	CoverageForm1 AS COVERAGEFORM, 
	RiskType1 AS RISKTYPE, 
	CoverageType1 AS COVERAGETYPE, 
	CoverageVersion1 AS COVERAGEVERSION, 
	ClassCode1 AS CLASSCODE, 
	SublineCode1 AS SUBLINECODE, 
	ins_line1 AS INSURANCELINECODE, 
	StandardInsuranceLineDescription1 AS INSURANCELINECODEDESCRIPTION, 
	major_peril_code1 AS MAJORPERILCODE, 
	StandardMajorPerilDescription1 AS MAJORPERILCODEDESCRIPTION, 
	major_peril_seq1 AS MAJORPERILSEQUENCENUMBER, 
	pms_type_bureau_code1 AS TYPEBUREAUCODE, 
	StandardTypeBureauCodeShortDescription1 AS TYPEBUREAUCODESHORTDESCRIPTION, 
	StandardTypeBureauCodeLongDescription1 AS TYPEBUREAUCODELONGDESCRIPTION, 
	risk_unit_grp1 AS RISKUNITGROUPCODE, 
	StandardRiskUnitGroupDescription1 AS RISKUNITGROUPCODEDESCRIPTION, 
	risk_unit_grp_seq_num1 AS RISKUNITGROUPSEQUENCENUMBER, 
	risk_unit1 AS RISKUNITCODE, 
	StandardRiskUnitDescription1 AS RISKUNITCODEDESCRIPTION, 
	risk_unit_seq_num1 AS RISKUNITSEQUENCENUMBER, 
	claimant_cov_eff_date1 AS COVERAGEEFFECTIVEDATE, 
	loc_unit_num1 AS LOCATIONUNITNUMBER, 
	sub_loc_unit_num1 AS SUBLOCATIONUNITNUMBER, 
	unit_veh_registration_state_code1 AS UNITVEHICLEREGISTRATIONSTATECODE, 
	unit_vin_num1 AS UNITVINNUMBER, 
	unit_veh_stated_amt1 AS UNITVEHICLESTATEAMOUNT, 
	unit_veh_yr1 AS UNITVEHICLEYEAR, 
	unit_veh_make1 AS UNITVEHICLEMAKE, 
	spec_pers_prop_use_code1 AS SPECIALPERSONALPROPERTYUSECODE, 
	pkg_ded_amt1 AS PACKAGEDEDUCTIBLEAMOUNT, 
	pkg_lmt_amt1 AS PACKAGELIMITAMOUNT, 
	TYPEOFLOSS, 
	ClaimTypeCategory1 AS CLAIMTYPECATEGORY, 
	ClaimTypeGroup1 AS CLAIMTYPEGROUP, 
	SubrogationEligibleIndicator1 AS SUBROGATIONELIGIBLEINDICATOR
	FROM UPD_claimant_coverage_dim_INSERT
),
UPD_claimant_coverage_dim_UPDATE AS (
	SELECT
	claimant_cov_dim_id AS claimant_cov_dim_id2, 
	in_claimant_cov_det_id AS in_claimant_cov_det_id2, 
	in_claimant_cov_det_calculation_id AS in_claimant_coverage_calculation_id2, 
	in_claimant_cov_det_reserve_calculation_id_D AS in_claimant_cov_det_reserve_calculation_id_D2, 
	in_claimant_cov_det_reserve_calculation_id_E AS in_claimant_cov_det_reserve_calculation_id_E2, 
	in_claimant_cov_det_reserve_calculation_id_S AS in_claimant_cov_det_reserve_calculation_id_S2, 
	in_claimant_cov_det_reserve_calculation_id_B AS in_claimant_cov_det_reserve_calculation_id_B2, 
	in_claimant_cov_det_reserve_calculation_id_R AS in_claimant_cov_det_reserve_calculation_id_R2, 
	claimant_cov_date_type_out, 
	claimant_cov_direct_loss_status_type_out, 
	claimant_cov_exp_status_type_out, 
	claimant_cov_salvage_status_type_out, 
	claimant_cov_subgrogation_status_type_out, 
	claimant_cov_other_recovery_status_type_out, 
	claimant_cov_supplemental_ind_out AS claimant_cov_supplemental_ind_out2, 
	claimant_cov_financial_ind_out AS claimant_cov_financial_ind_out2, 
	claimant_cov_recovery_ind_out AS claimant_cov_recovery_ind_out2, 
	claimant_cov_notice_only_ind_out AS claimant_cov_notice_only_ind_out2, 
	s3p_wc_class_descript AS s3p_wc_class_descript2, 
	in_pms_loss_disability_code AS in_pms_loss_disability_code2, 
	in_loss_disability_descript AS lkp_loss_disability_descript2, 
	unit_dam_descript AS unit_dam_descript2, 
	s3p_unit_description AS s3p_unit_description2, 
	claimant_cov_open_date AS claimant_cov_open_date2, 
	claimant_cov_close_date AS claimant_cov_close_date2, 
	claimant_cov_reopen_date AS claimant_cov_reopen_date2, 
	claimant_cov_closed_after_reopen_date AS claimant_cov_closed_after_reopen_date2, 
	claimant_cov_noticeonly_date, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	modified_date AS modified_date2, 
	s3p_unit_type_code1 AS s3p_unit_type_code12, 
	cov_unit_descript, 
	pms_reserve_ctgry AS pms_reserve_ctgry2, 
	pms_loss_cause AS pms_loss_cause2, 
	cause_of_loss_long_descript AS cause_of_loss_long_descript2, 
	reserve_ctgry_descript AS reserve_ctgry_descript2, 
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id2, 
	claim_total_loss_ind AS claim_total_loss_ind2, 
	CauseOfLossName AS CauseOfLossName2, 
	PolicySourceID AS PolicySourceID2, 
	CoverageForm AS CoverageForm2, 
	RiskType AS RiskType2, 
	CoverageType AS CoverageType2, 
	CoverageVersion AS CoverageVersion2, 
	ClassCode AS ClassCode2, 
	SublineCode AS SublineCode2, 
	major_peril_code AS major_peril_code2, 
	ins_line AS ins_line2, 
	risk_unit_grp AS risk_unit_grp2, 
	risk_unit_grp_seq_num AS risk_unit_grp_seq_num2, 
	risk_unit AS risk_unit2, 
	risk_unit_seq_num AS risk_unit_seq_num2, 
	StandardInsuranceLineDescription AS StandardInsuranceLineDescription2, 
	StandardMajorPerilDescription AS StandardMajorPerilDescription2, 
	StandardTypeBureauCodeShortDescription AS StandardTypeBureauCodeShortDescription2, 
	StandardTypeBureauCodeLongDescription AS StandardTypeBureauCodeLongDescription2, 
	StandardRiskUnitGroupDescription AS StandardRiskUnitGroupDescription2, 
	StandardRiskUnitDescription AS StandardRiskUnitDescription2, 
	pms_type_bureau_code AS pms_type_bureau_code2, 
	major_peril_seq AS major_peril_seq2, 
	claimant_cov_eff_date AS claimant_cov_eff_date2, 
	unit_veh_registration_state_code AS unit_veh_registration_state_code2, 
	unit_vin_num AS unit_vin_num2, 
	unit_veh_stated_amt AS unit_veh_stated_amt2, 
	unit_veh_yr AS unit_veh_yr2, 
	unit_veh_make AS unit_veh_make2, 
	spec_pers_prop_use_code AS spec_pers_prop_use_code2, 
	pkg_ded_amt AS pkg_ded_amt2, 
	pkg_lmt_amt AS pkg_lmt_amt2, 
	loc_unit_num AS loc_unit_num2, 
	sub_loc_unit_num AS sub_loc_unit_num2, 
	TypeOfLoss, 
	ClaimTypeCategory AS ClaimTypeCategory2, 
	ClaimTypeGroup AS ClaimTypeGroup2, 
	SubrogationEligibleIndicator AS SubrogationEligibleIndicator2
	FROM RTR_claimant_coverage_dim_DEFAULT1
),
claimant_coverage_dim_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_dim AS T
	USING UPD_claimant_coverage_dim_UPDATE AS S
	ON T.claimant_cov_dim_id = S.claimant_cov_dim_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claimant_cov_det_pk_id = S.in_claimant_cov_det_id2, T.edw_claimant_cov_det_calculation_pk_id = S.in_claimant_coverage_calculation_id2, T.edw_claimant_cov_det_reserve_calculation_direct_loss_pk_id = S.in_claimant_cov_det_reserve_calculation_id_D2, T.edw_claimant_cov_det_reserve_calculation_exp_pk_id = S.in_claimant_cov_det_reserve_calculation_id_E2, T.edw_claimant_cov_det_reserve_calculation_subrogation_pk_id = S.in_claimant_cov_det_reserve_calculation_id_B2, T.edw_claimant_cov_det_reserve_calculation_salvage_pk_id = S.in_claimant_cov_det_reserve_calculation_id_S2, T.edw_claimant_cov_det_reserve_calculation_other_recovery_pk_id = S.in_claimant_cov_det_reserve_calculation_id_R2, T.edw_claimant_cov_det_ak_id = S.claimant_cov_det_ak_id2, T.claimant_cov_status_type = S.claimant_cov_date_type_out, T.claimant_cov_direct_loss_status_type = S.claimant_cov_direct_loss_status_type_out, T.claimant_cov_exp_status_type = S.claimant_cov_exp_status_type_out, T.claimant_cov_salvage_status_type = S.claimant_cov_salvage_status_type_out, T.claimant_cov_subrogation_status_type = S.claimant_cov_subgrogation_status_type_out, T.claimant_cov_other_recovery_status_type = S.claimant_cov_other_recovery_status_type_out, T.claimant_cov_financial_ind = S.claimant_cov_financial_ind_out2, T.claimant_cov_supplemental_ind = S.claimant_cov_supplemental_ind_out2, T.claimant_cov_recovery_ind = S.claimant_cov_recovery_ind_out2, T.claimant_cov_notice_only_claim_ind = S.claimant_cov_notice_only_ind_out2, T.wc_class_descript = S.s3p_wc_class_descript2, T.unit_type_code = S.s3p_unit_type_code12, T.unit_type_descript = S.cov_unit_descript, T.unit_descript = S.s3p_unit_description2, T.cause_of_loss = S.pms_loss_cause2, T.cause_of_loss_long_descript = S.CauseOfLossName2, T.reserve_ctgry = S.pms_reserve_ctgry2, T.reserve_ctgry_descript = S.reserve_ctgry_descript2, T.loss_disability = S.in_pms_loss_disability_code2, T.loss_disability_descript = S.lkp_loss_disability_descript2, T.unit_dam_descript = S.unit_dam_descript2, T.claimant_cov_open_date = S.claimant_cov_open_date2, T.claimant_cov_close_date = S.claimant_cov_close_date2, T.claimant_cov_reopen_date = S.claimant_cov_reopen_date2, T.claimant_cov_closed_after_reopen_date = S.claimant_cov_closed_after_reopen_date2, T.claimant_cov_notice_only_date = S.claimant_cov_noticeonly_date, T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.modified_date = S.modified_date2, T.claim_total_loss_ind = S.claim_total_loss_ind2, T.PolicySourceID = S.PolicySourceID2, T.CoverageForm = S.CoverageForm2, T.RiskType = S.RiskType2, T.CoverageType = S.CoverageType2, T.CoverageVersion = S.CoverageVersion2, T.ClassCode = S.ClassCode2, T.SublineCode = S.SublineCode2, T.InsuranceLineCode = S.ins_line2, T.InsuranceLineCodeDescription = S.StandardInsuranceLineDescription2, T.MajorPerilCode = S.major_peril_code2, T.MajorPerilCodeDescription = S.StandardMajorPerilDescription2, T.MajorPerilSequenceNumber = S.major_peril_seq2, T.TypeBureauCode = S.pms_type_bureau_code2, T.TypeBureauCodeShortDescription = S.StandardTypeBureauCodeShortDescription2, T.TypeBureauCodeLongDescription = S.StandardTypeBureauCodeLongDescription2, T.RiskUnitGroupCode = S.risk_unit_grp2, T.RiskUnitGroupCodeDescription = S.StandardRiskUnitGroupDescription2, T.RiskUnitGroupSequenceNumber = S.risk_unit_grp_seq_num2, T.RiskUnitCode = S.risk_unit2, T.RiskUnitCodeDescription = S.StandardRiskUnitDescription2, T.RiskUnitSequenceNumber = S.risk_unit_seq_num2, T.CoverageEffectiveDate = S.claimant_cov_eff_date2, T.LocationUnitNumber = S.loc_unit_num2, T.SubLocationUnitNumber = S.sub_loc_unit_num2, T.UnitVehicleRegistrationStateCode = S.unit_veh_registration_state_code2, T.UnitVinNumber = S.unit_vin_num2, T.UnitVehicleStateAmount = S.unit_veh_stated_amt2, T.UnitVehicleYear = S.unit_veh_yr2, T.UnitVehicleMake = S.unit_veh_make2, T.SpecialPersonalPropertyUseCode = S.spec_pers_prop_use_code2, T.PackageDeductibleAmount = S.pkg_ded_amt2, T.PackageLimitAmount = S.pkg_lmt_amt2, T.TypeOfLoss = S.TypeOfLoss, T.ClaimTypeCategory = S.ClaimTypeCategory2, T.ClaimTypeGroup = S.ClaimTypeGroup2, T.SubrogationEligibleIndicator = S.SubrogationEligibleIndicator2
),