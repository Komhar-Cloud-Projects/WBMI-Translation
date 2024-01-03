WITH
SQ_Claim_Occurrence_Dim_Sources AS (
	SELECT
	CO.CLAIM_OCCURRENCE_ID AS CLAIM_OCCURRENCE_PK_ID,
	CO.CLAIM_OCCURRENCE_AK_ID AS CLAIM_OCCURRENCE_AK_ID,
	CO.CLAIM_OCCURRENCE_KEY,
	CO.CLAIM_OCCURRENCE_TYPE_CODE,
	CO.SOURCE_CLAIM_OCCURRENCE_STATUS_CODE,
	CO.S3P_CLAIM_CREATED_DATE,
	CO.SOURCE_CLAIM_RPTED_DATE,
	CO.RPT_METHOD,
	CO.HOW_CLAIM_RPTED,
	CO.LOSS_LOC_ADDR,
	CO.LOSS_LOC_CITY,
	CO.LOSS_LOC_COUNTY,
	LTRIM(RTRIM(CO.LOSS_LOC_STATE)) AS LOSS_LOC_STATE ,
	CO.LOSS_LOC_ZIP,
	CO.CLAIM_LOSS_DATE,
	CO.CLAIM_DISCOVERY_DATE,
	CO.CLAIM_CAT_CODE,
	CO.CLAIM_CAT_START_DATE,
	CO.CLAIM_CAT_END_DATE,
	CO.S3P_CLAIM_NUM,
	CO.REINS_NOTIFIED_DATE,
	CO.CLAIM_OCCURRENCE_NUM,
	CO.CLAIM_VOILATION_CITATION_DESCRIPT,
	CO.CLAIM_LOSS_DESCRIPT,
	CO.CLAIM_INSD_AT_FAULT_CODE,
	CO.CLAIM_INSD_DRIVER_NUM,
	CO.CLAIM_INSD_DRIVER_IND,
	CO.CLAIM_LOG_NOTE_LAST_ACT_DATE,
	CO.NEXT_DIARY_DATE,
	CO.NEXT_DIARY_DATE_REP,
	CO.OFFSET_ONSET_IND,
	CO.ERR_FLAG_BAL_TXN,
	CO.ERR_FLAG_BAL_REINS,
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE EFF_FROM_DATE,
	CO.SOURCE_SYS_ID,
	CO.CLAIM_CREATED_BY_KEY,
	COC.CLAIM_OCCURRENCE_CALCULATION_ID  AS CLAIM_OCCURRENCE_PK_ID,
	COC.CLAIM_OCCURRENCE_DATE CLAIM_OCCURRENCE_DATE,
	COC.CLAIM_OCCURRENCE_DATE_TYPE,
	COC.CLAIM_OCCURRENCE_REPORTED_DATE,
	COC.CLAIM_SUPPLEMENTAL_IND,
	COC.CLAIM_FINANCIAL_IND,
	COC.CLAIM_RECOVERY_IND,
	COC.CLAIM_NOTICE_ONLY_IND,
	CORC_D.CLAIM_OCCURRENCE_RESERVE_CALCULATION_ID AS CLAIM_OCCURRENCE_RESERVE_CALCULATION_DIRECT_LOSS_PK_ID ,
	CORC_D.RESERVE_DATE_TYPE AS CLAIM_OCCURRENCE_DIRECT_LOSS_STATUS_CODE,
	CORC_E.CLAIM_OCCURRENCE_RESERVE_CALCULATION_ID AS  CLAIM_OCCURRENCE_RESERVE_CALCULATION_EXP_PK_ID ,
	CORC_E.RESERVE_DATE_TYPE AS CLAIM_OCCURRENCE_EXP_STATUS_CODE ,
	CORC_B.CLAIM_OCCURRENCE_RESERVE_CALCULATION_ID AS  CLAIM_OCCURRENCE_RESERVE_CALCULATION_SUBROGATION_PK_ID ,
	CORC_B.RESERVE_DATE_TYPE AS CLAIM_OCCURRENCE_SUBROGATION_STATUS_CODE,
	CORC_S.CLAIM_OCCURRENCE_RESERVE_CALCULATION_ID AS CLAIM_OCCURRENCE_RESERVE_CALCULATION_SALVAGE_PK_ID ,
	CORC_S.RESERVE_DATE_TYPE AS CLAIM_OCCURRENCE_SALVAGE_STATUS_CODE,
	CORC_R.CLAIM_OCCURRENCE_RESERVE_CALCULATION_ID AS  CLAIM_OCCURRENCE_RESERVE_CALCULATION_OTHER_RECOVERY_PK_ID ,
	CORC_R.RESERVE_DATE_TYPE AS CLAIM_OCCURRENCE_OTHER_RECOVERY_STATUS_CODE,
	CO.WC_CAT_CODE AS WC_CAT_CODE,
	CO.SupStateId AS SupStateId,
	CO.SupClaimInsuredAtFaultCodeId AS SupClaimInsuredAtFaultCodeId,
	CO.PrimaryWorkGroup AS PrimaryWorkGroup,
	CO.SecondaryWorkGroup AS SecondaryWorkGroup,
	CO.ClaimRelationshipKey AS ClaimRelationshipKey
	FROM
	
	(
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_RESERVE_CALCULATION WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_CALCULATION WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_REPRESENTATIVE_OCCURRENCE WHERE MODIFIED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	) AS DISTINCT_EFF_FROM_DATES
	
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE CO ON CO.CLAIM_OCCURRENCE_AK_ID = DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CO.EFF_FROM_DATE AND CO.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_RESERVE_CALCULATION CORC_D ON DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=CORC_D.CLAIM_OCCURRENCE_AK_ID AND CORC_D.FINANCIAL_TYPE_CODE='D'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CORC_D.EFF_FROM_DATE AND CORC_D.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_RESERVE_CALCULATION CORC_E ON DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=CORC_E.CLAIM_OCCURRENCE_AK_ID AND CORC_E.FINANCIAL_TYPE_CODE='E'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CORC_E.EFF_FROM_DATE AND CORC_E.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_RESERVE_CALCULATION CORC_B ON DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=CORC_B.CLAIM_OCCURRENCE_AK_ID AND CORC_B.FINANCIAL_TYPE_CODE='B'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CORC_B.EFF_FROM_DATE AND CORC_B.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_RESERVE_CALCULATION CORC_S ON DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=CORC_S.CLAIM_OCCURRENCE_AK_ID AND CORC_S.FINANCIAL_TYPE_CODE='S'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CORC_S.EFF_FROM_DATE AND CORC_S.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_RESERVE_CALCULATION CORC_R ON DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=CORC_R.CLAIM_OCCURRENCE_AK_ID AND CORC_R.FINANCIAL_TYPE_CODE='R'
	AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CORC_R.EFF_FROM_DATE AND CORC_R.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_CALCULATION COC ON DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=COC.CLAIM_OCCURRENCE_AK_ID AND DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN COC.EFF_FROM_DATE AND COC.EFF_TO_DATE
),
SQ_Claim_Representative_Occurrence AS (
	SELECT
	CASE WHEN CRO_E.CLAIM_REP_OCCURRENCE_ID IS NULL  THEN -1 ELSE CRO_E.CLAIM_REP_OCCURRENCE_ID END,
	
	DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID,
	
	CASE WHEN CRO_E.TRANSFERRED_CLAIM_ADJUSTER_LVL_IND  IS NULL THEN 'N/A' ELSE  CRO_E.TRANSFERRED_CLAIM_ADJUSTER_LVL_IND END ,
	
	CASE  WHEN CRO_E.TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND  IS NULL THEN 'N/A' ELSE CRO_E.TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND END,
	
	CASE  WHEN CRO_E.TRANSFERRED_CLAIM_DEPT_LVL_IND  IS NULL THEN 'N/A' ELSE CRO_E.TRANSFERRED_CLAIM_DEPT_LVL_IND END,
	
	CASE  WHEN CRO_E.TRANSFERRED_CLAIM_DVSN_LVL_IND  IS NULL THEN 'N/A' ELSE  CRO_E.TRANSFERRED_CLAIM_DVSN_LVL_IND END,
	
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE,
	
	CRO_E.EFF_FROM_DATE EFF_FROM_DATE,
	
	CRO_E.EFF_TO_DATE EFF_TO_DATE,
	
	CASE  WHEN CRO_H.CLAIM_REP_OCCURRENCE_ID  IS NULL THEN -1 ELSE CRO_H.CLAIM_REP_OCCURRENCE_ID END ,
	
	CRO_H.EFF_FROM_DATE EFF_FROM_DATE,
	
	CRO_H.EFF_TO_DATE EFF_TO_DATE,
	
	CASE  WHEN CRO_H.TRANSFERRED_CLAIM_ADJUSTER_LVL_IND   IS NULL THEN 'N/A' ELSE CRO_H.TRANSFERRED_CLAIM_ADJUSTER_LVL_IND END,
	
	CASE  WHEN CRO_H.TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND  IS  NULL THEN 'N/A' ELSE CRO_H.TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND END,
	
	CASE  WHEN CRO_H.TRANSFERRED_CLAIM_DEPT_LVL_IND   IS NULL THEN 'N/A' ELSE CRO_H.TRANSFERRED_CLAIM_DEPT_LVL_IND END,
	
	CASE  WHEN CRO_H.TRANSFERRED_CLAIM_DVSN_LVL_IND   IS NULL THEN 'N/A' ELSE CRO_H.TRANSFERRED_CLAIM_DVSN_LVL_IND END
	
	FROM
	
	(
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_RESERVE_CALCULATION WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OCCURRENCE_CALCULATION WHERE CREATED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT CLAIM_OCCURRENCE_AK_ID,EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_REPRESENTATIVE_OCCURRENCE WHERE MODIFIED_DATE>='@{pipeline().parameters.SELECTION_START_TS}'
	) AS DISTINCT_EFF_FROM_DATES
	
	LEFT OUTER JOIN CLAIM_REPRESENTATIVE_OCCURRENCE CRO_E ON 
	DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=CRO_E.CLAIM_OCCURRENCE_AK_ID AND 
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CRO_E.EFF_FROM_DATE AND CRO_E.EFF_TO_DATE AND 
	CRO_E.CLAIM_REP_ROLE_CODE = 'E'
	
	LEFT OUTER JOIN CLAIM_REPRESENTATIVE_OCCURRENCE CRO_H ON 
	DISTINCT_EFF_FROM_DATES.CLAIM_OCCURRENCE_AK_ID=CRO_H.CLAIM_OCCURRENCE_AK_ID AND 
	DISTINCT_EFF_FROM_DATES.EFF_FROM_DATE BETWEEN CRO_H.EFF_FROM_DATE AND CRO_H.EFF_TO_DATE AND 
	CRO_H.CLAIM_REP_ROLE_CODE = 'H'
),
JNR_CRO_EDW AS (SELECT
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_id, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_ak_id, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_key, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_type_code, 
	SQ_Claim_Occurrence_Dim_Sources.source_claim_occurrence_status_code, 
	SQ_Claim_Occurrence_Dim_Sources.s3p_claim_created_date, 
	SQ_Claim_Occurrence_Dim_Sources.source_claim_rpted_date, 
	SQ_Claim_Occurrence_Dim_Sources.rpt_method, 
	SQ_Claim_Occurrence_Dim_Sources.how_claim_rpted, 
	SQ_Claim_Occurrence_Dim_Sources.loss_loc_addr, 
	SQ_Claim_Occurrence_Dim_Sources.loss_loc_city, 
	SQ_Claim_Occurrence_Dim_Sources.loss_loc_county, 
	SQ_Claim_Occurrence_Dim_Sources.loss_loc_state, 
	SQ_Claim_Occurrence_Dim_Sources.loss_loc_zip, 
	SQ_Claim_Occurrence_Dim_Sources.claim_cat_code, 
	SQ_Claim_Occurrence_Dim_Sources.s3p_claim_num, 
	SQ_Claim_Occurrence_Dim_Sources.reins_notified_date, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_num, 
	SQ_Claim_Occurrence_Dim_Sources.claim_voilation_citation_descript, 
	SQ_Claim_Occurrence_Dim_Sources.claim_loss_descript, 
	SQ_Claim_Occurrence_Dim_Sources.claim_insd_at_fault_code, 
	SQ_Claim_Occurrence_Dim_Sources.claim_insd_driver_num, 
	SQ_Claim_Occurrence_Dim_Sources.claim_insd_driver_ind, 
	SQ_Claim_Occurrence_Dim_Sources.claim_log_note_last_act_date, 
	SQ_Claim_Occurrence_Dim_Sources.next_diary_date, 
	SQ_Claim_Occurrence_Dim_Sources.next_diary_date_rep, 
	SQ_Claim_Occurrence_Dim_Sources.offset_onset_ind, 
	SQ_Claim_Occurrence_Dim_Sources.err_flag_bal_txn, 
	SQ_Claim_Occurrence_Dim_Sources.err_flag_bal_reins, 
	SQ_Claim_Occurrence_Dim_Sources.eff_from_date, 
	SQ_Claim_Occurrence_Dim_Sources.source_sys_id, 
	SQ_Claim_Occurrence_Dim_Sources.claim_created_by_key, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_calculation_id, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_date_type, 
	SQ_Claim_Occurrence_Dim_Sources.claim_supplemental_ind, 
	SQ_Claim_Occurrence_Dim_Sources.claim_financial_ind, 
	SQ_Claim_Occurrence_Dim_Sources.claim_recovery_ind, 
	SQ_Claim_Occurrence_Dim_Sources.claim_notice_only_ind, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_reserve_calculation_id, 
	SQ_Claim_Occurrence_Dim_Sources.reserve_date_type, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_reserve_calculation_id1, 
	SQ_Claim_Occurrence_Dim_Sources.reserve_date_type1, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_reserve_calculation_id2, 
	SQ_Claim_Occurrence_Dim_Sources.reserve_date_type2, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_reserve_calculation_id3, 
	SQ_Claim_Occurrence_Dim_Sources.reserve_date_type3, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_reserve_calculation_id4, 
	SQ_Claim_Occurrence_Dim_Sources.reserve_date_type4, 
	SQ_Claim_Occurrence_Dim_Sources.financial_type_status_code4 AS wc_cat_code, 
	SQ_Claim_Occurrence_Dim_Sources.SupStateId, 
	SQ_Claim_Occurrence_Dim_Sources.SupClaimInsuredAtFaultCodeId, 
	SQ_Claim_Representative_Occurrence.claim_rep_occurrence_id AS claim_rep_occurrence_id_E, 
	SQ_Claim_Representative_Occurrence.created_date1 AS eff_from_date_cro, 
	SQ_Claim_Representative_Occurrence.transferred_claim_adjuster_lvl_ind AS transferred_claim_adjuster_lvl_ind_E, 
	SQ_Claim_Representative_Occurrence.transferred_claim_handling_office_lvl_ind AS transferred_claim_handling_office_lvl_ind_E, 
	SQ_Claim_Representative_Occurrence.transferred_claim_dept_lvl_ind AS transferred_claim_dept_lvl_ind_E, 
	SQ_Claim_Representative_Occurrence.transferred_claim_dvsn_lvl_ind AS transferred_claim_dvsn_lvl_ind_E, 
	SQ_Claim_Representative_Occurrence.transferred_claim_adjuster_lvl_ind1 AS transferred_claim_adjuster_lvl_ind_H, 
	SQ_Claim_Representative_Occurrence.transferred_claim_handling_office_lvl_ind1 AS transferred_claim_handling_office_lvl_ind_H, 
	SQ_Claim_Representative_Occurrence.transferred_claim_dept_lvl_ind1 AS transferred_claim_dept_lvl_ind_H, 
	SQ_Claim_Representative_Occurrence.transferred_claim_dvsn_lvl_ind1 AS transferred_claim_dvsn_lvl_ind_H, 
	SQ_Claim_Representative_Occurrence.claim_rep_occurrence_id1 AS claim_rep_occurrence_id_H, 
	SQ_Claim_Representative_Occurrence.claim_occurrence_ak_id AS claim_occurrence_ak_id_cro, 
	SQ_Claim_Occurrence_Dim_Sources.claim_loss_date, 
	SQ_Claim_Occurrence_Dim_Sources.claim_discovery_date, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_reported_date, 
	SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_date, 
	SQ_Claim_Occurrence_Dim_Sources.claim_cat_start_date, 
	SQ_Claim_Occurrence_Dim_Sources.claim_cat_end_date, 
	SQ_Claim_Representative_Occurrence.eff_from_date AS eff_from_date_E, 
	SQ_Claim_Representative_Occurrence.eff_to_date AS eff_to_date_E, 
	SQ_Claim_Representative_Occurrence.eff_from_date1 AS eff_from_date_H, 
	SQ_Claim_Representative_Occurrence.eff_to_date1 AS eff_to_date_H, 
	SQ_Claim_Occurrence_Dim_Sources.PrimaryWorkGroup, 
	SQ_Claim_Occurrence_Dim_Sources.SecondaryWorkGroup, 
	SQ_Claim_Occurrence_Dim_Sources.ClaimRelationshipKey
	FROM SQ_Claim_Occurrence_Dim_Sources
	INNER JOIN SQ_Claim_Representative_Occurrence
	ON SQ_Claim_Representative_Occurrence.claim_occurrence_ak_id = SQ_Claim_Occurrence_Dim_Sources.claim_occurrence_ak_id AND SQ_Claim_Representative_Occurrence.created_date1 = SQ_Claim_Occurrence_Dim_Sources.eff_from_date
),
LKP_ClaimStory AS (
	SELECT
	Catalyst,
	CauseOfDamage,
	DamageCaused,
	ItemDamaged,
	ClaimOccurrenceKey
	FROM (
		SELECT 
			Catalyst,
			CauseOfDamage,
			DamageCaused,
			ItemDamaged,
			ClaimOccurrenceKey
		FROM ClaimStory
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimOccurrenceKey ORDER BY Catalyst) = 1
),
LKP_SupClaimReportedMethodDescription AS (
	SELECT
	ClaimReportedMethodDescription,
	ClaimReportedMethodCode
	FROM (
		select ClaimReportedMethodCode as ClaimReportedMethodCode, 
			ClaimReportedMethodDescription as ClaimReportedMethodDescription
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupClaimReportedMethodDescription
		where CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimReportedMethodCode ORDER BY ClaimReportedMethodDescription) = 1
),
LKP_Sup_Claim_Insured_At_Fault_Code AS (
	SELECT
	claim_insd_at_fault_descript,
	sup_claim_insd_at_fault_code_id
	FROM (
		SELECT 
			claim_insd_at_fault_descript,
			sup_claim_insd_at_fault_code_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_insured_at_fault_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_claim_insd_at_fault_code_id ORDER BY claim_insd_at_fault_descript) = 1
),
LKP_Sup_State AS (
	SELECT
	state_descript,
	state_code
	FROM (
		SELECT 
			state_descript,
			state_code
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_descript) = 1
),
EXP_Set_Default_Value AS (
	SELECT
	JNR_CRO_EDW.claim_occurrence_id AS edw_claim_occurrence_pk_id,
	-- *INF*: IIF(ISNULL(edw_claim_occurrence_pk_id), -1, edw_claim_occurrence_pk_id)
	-- 
	IFF(edw_claim_occurrence_pk_id IS NULL, - 1, edw_claim_occurrence_pk_id) AS edw_claim_occurrence_pk_id_out,
	JNR_CRO_EDW.claim_occurrence_calculation_id AS edw_claim_occurrence_calculation_pk_id,
	-- *INF*: IIF(ISNULL(edw_claim_occurrence_calculation_pk_id),-1,edw_claim_occurrence_calculation_pk_id)
	IFF(edw_claim_occurrence_calculation_pk_id IS NULL, - 1, edw_claim_occurrence_calculation_pk_id) AS edw_claim_occurrence_calculation_pk_id_out,
	JNR_CRO_EDW.claim_occurrence_reserve_calculation_id AS edw_claim_occurrence_reserve_calculation_direct_loss_pk_id,
	-- *INF*: IIF(ISNULL(edw_claim_occurrence_reserve_calculation_direct_loss_pk_id),-1,edw_claim_occurrence_reserve_calculation_direct_loss_pk_id)
	IFF(edw_claim_occurrence_reserve_calculation_direct_loss_pk_id IS NULL, - 1, edw_claim_occurrence_reserve_calculation_direct_loss_pk_id) AS edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out,
	JNR_CRO_EDW.claim_occurrence_reserve_calculation_id1 AS edw_claim_occurrence_reserve_calculation_exp_pk_id,
	-- *INF*: IIF(ISNULL(edw_claim_occurrence_reserve_calculation_exp_pk_id),-1,edw_claim_occurrence_reserve_calculation_exp_pk_id)
	IFF(edw_claim_occurrence_reserve_calculation_exp_pk_id IS NULL, - 1, edw_claim_occurrence_reserve_calculation_exp_pk_id) AS edw_claim_occurrence_reserve_calculation_exp_pk_id_out,
	JNR_CRO_EDW.claim_occurrence_reserve_calculation_id2 AS edw_claim_occurrence_reserve_calculation_subrogation_pk_id,
	-- *INF*: IIF(ISNULL(edw_claim_occurrence_reserve_calculation_subrogation_pk_id),-1,edw_claim_occurrence_reserve_calculation_subrogation_pk_id)
	IFF(edw_claim_occurrence_reserve_calculation_subrogation_pk_id IS NULL, - 1, edw_claim_occurrence_reserve_calculation_subrogation_pk_id) AS edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out,
	JNR_CRO_EDW.claim_occurrence_reserve_calculation_id3 AS edw_claim_occurrence_reserve_calculation_salvage_pk_id,
	-- *INF*: IIF(ISNULL(edw_claim_occurrence_reserve_calculation_salvage_pk_id),-1,edw_claim_occurrence_reserve_calculation_salvage_pk_id)
	IFF(edw_claim_occurrence_reserve_calculation_salvage_pk_id IS NULL, - 1, edw_claim_occurrence_reserve_calculation_salvage_pk_id) AS edw_claim_occurrence_reserve_calculation_salvage_pk_id_out,
	JNR_CRO_EDW.claim_occurrence_reserve_calculation_id4 AS edw_claim_occurrence_reserve_calculation_other_recovery_pk_id,
	-- *INF*: IIF(ISNULL(edw_claim_occurrence_reserve_calculation_other_recovery_pk_id),-1,edw_claim_occurrence_reserve_calculation_other_recovery_pk_id)
	IFF(edw_claim_occurrence_reserve_calculation_other_recovery_pk_id IS NULL, - 1, edw_claim_occurrence_reserve_calculation_other_recovery_pk_id) AS edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out,
	JNR_CRO_EDW.claim_occurrence_key,
	JNR_CRO_EDW.claim_occurrence_date_type AS in_claim_occurrence_status_code,
	-- *INF*: iif(isnull(in_claim_occurrence_status_code) OR LENGTH(in_claim_occurrence_status_code)=0 OR in_claim_occurrence_status_code = 'N/A' ,
	-- 'N/A',
	-- substr(in_claim_occurrence_status_code,2))
	IFF(in_claim_occurrence_status_code IS NULL OR LENGTH(in_claim_occurrence_status_code) = 0 OR in_claim_occurrence_status_code = 'N/A', 'N/A', substr(in_claim_occurrence_status_code, 2)) AS v_claim_occurrence_status_code,
	v_claim_occurrence_status_code AS out_claim_occurrence_status_code,
	JNR_CRO_EDW.reserve_date_type AS in_claim_occurrence_direct_loss_status_code,
	-- *INF*: iif(isnull(in_claim_occurrence_direct_loss_status_code) OR in_claim_occurrence_direct_loss_status_code='N/A',
	-- 'N/A',
	-- substr(in_claim_occurrence_direct_loss_status_code,2))
	IFF(in_claim_occurrence_direct_loss_status_code IS NULL OR in_claim_occurrence_direct_loss_status_code = 'N/A', 'N/A', substr(in_claim_occurrence_direct_loss_status_code, 2)) AS claim_occurrence_direct_loss_status_code_out,
	JNR_CRO_EDW.reserve_date_type1 AS in_claim_occurrence_exp_status_code,
	-- *INF*: iif(isnull(in_claim_occurrence_exp_status_code) OR in_claim_occurrence_exp_status_code = 'N/A'
	-- ,'N/A',
	-- substr(in_claim_occurrence_exp_status_code,2))
	IFF(in_claim_occurrence_exp_status_code IS NULL OR in_claim_occurrence_exp_status_code = 'N/A', 'N/A', substr(in_claim_occurrence_exp_status_code, 2)) AS claim_occurrence_exp_status_code_out,
	JNR_CRO_EDW.reserve_date_type2 AS in_claim_occurrence_subrogation_status_code,
	-- *INF*: iif(isnull(in_claim_occurrence_subrogation_status_code) OR in_claim_occurrence_subrogation_status_code ='N/A'
	-- ,'N/A',
	-- substr(in_claim_occurrence_subrogation_status_code,2))
	IFF(in_claim_occurrence_subrogation_status_code IS NULL OR in_claim_occurrence_subrogation_status_code = 'N/A', 'N/A', substr(in_claim_occurrence_subrogation_status_code, 2)) AS claim_occurrence_subrogation_status_code_out,
	JNR_CRO_EDW.reserve_date_type3 AS in_claim_occurrence_salvage_status_code,
	-- *INF*: iif(isnull(in_claim_occurrence_salvage_status_code) OR in_claim_occurrence_salvage_status_code='N/A'
	-- ,'N/A',
	-- substr(in_claim_occurrence_salvage_status_code,2))
	IFF(in_claim_occurrence_salvage_status_code IS NULL OR in_claim_occurrence_salvage_status_code = 'N/A', 'N/A', substr(in_claim_occurrence_salvage_status_code, 2)) AS claim_occurrence_salvage_status_code_out,
	JNR_CRO_EDW.reserve_date_type4 AS in_claim_occurrence_other_recovery_status_code,
	-- *INF*: iif(isnull(in_claim_occurrence_other_recovery_status_code) OR in_claim_occurrence_other_recovery_status_code = 'N/A'
	-- ,'N/A',
	-- substr(in_claim_occurrence_other_recovery_status_code,2))
	IFF(in_claim_occurrence_other_recovery_status_code IS NULL OR in_claim_occurrence_other_recovery_status_code = 'N/A', 'N/A', substr(in_claim_occurrence_other_recovery_status_code, 2)) AS claim_occurrence_other_recovery_status_code_out,
	JNR_CRO_EDW.wc_cat_code AS in_wc_cat_code,
	-- *INF*: iif(isnull(in_wc_cat_code)
	-- ,'N/A',
	-- in_wc_cat_code)
	IFF(in_wc_cat_code IS NULL, 'N/A', in_wc_cat_code) AS wc_cat_code_out,
	-- *INF*: iif(isnull(in_claim_occurrence_reopen_ind),'N/A',in_claim_occurrence_reopen_ind)
	IFF(in_claim_occurrence_reopen_ind IS NULL, 'N/A', in_claim_occurrence_reopen_ind) AS claim_occurrence_reopen_ind_out,
	JNR_CRO_EDW.claim_financial_ind AS in_claim_occurrence_financial_ind,
	-- *INF*: iif(isnull(in_claim_occurrence_financial_ind),'N/A',in_claim_occurrence_financial_ind)
	IFF(in_claim_occurrence_financial_ind IS NULL, 'N/A', in_claim_occurrence_financial_ind) AS claim_occurrence_financial_ind_out,
	JNR_CRO_EDW.claim_supplemental_ind AS in_claim_occurrence_supplemental_ind,
	-- *INF*: iif(isnull(in_claim_occurrence_supplemental_ind),'N/A',in_claim_occurrence_supplemental_ind)
	IFF(in_claim_occurrence_supplemental_ind IS NULL, 'N/A', in_claim_occurrence_supplemental_ind) AS claim_occurrence_supplemental_ind_out,
	JNR_CRO_EDW.claim_recovery_ind AS in_claim_occurrence_recovery_ind,
	-- *INF*: iif(isnull(in_claim_occurrence_recovery_ind),'N/A',in_claim_occurrence_recovery_ind)
	IFF(in_claim_occurrence_recovery_ind IS NULL, 'N/A', in_claim_occurrence_recovery_ind) AS claim_occurrence_recovery_ind_out,
	JNR_CRO_EDW.claim_notice_only_ind AS in_claim_occurrence_notice_only_claim_ind,
	-- *INF*: iif(isnull(in_claim_occurrence_notice_only_claim_ind),'N/A',in_claim_occurrence_notice_only_claim_ind)
	IFF(in_claim_occurrence_notice_only_claim_ind IS NULL, 'N/A', in_claim_occurrence_notice_only_claim_ind) AS claim_occurrence_notice_only_claim_ind_out,
	JNR_CRO_EDW.claim_occurrence_type_code AS claim_occurrence_type,
	JNR_CRO_EDW.source_claim_occurrence_status_code,
	JNR_CRO_EDW.s3p_claim_created_date,
	JNR_CRO_EDW.source_claim_rpted_date,
	JNR_CRO_EDW.rpt_method,
	JNR_CRO_EDW.how_claim_rpted,
	JNR_CRO_EDW.loss_loc_zip,
	JNR_CRO_EDW.loss_loc_state AS i_state_code,
	-- *INF*: IIF(ISNULL(i_state_code) OR RTRIM(LTRIM(i_state_code))='',
	-- 'N/A',
	-- RTRIM(LTRIM(i_state_code)))
	IFF(i_state_code IS NULL OR RTRIM(LTRIM(i_state_code)) = '', 'N/A', RTRIM(LTRIM(i_state_code))) AS loss_loc_state,
	JNR_CRO_EDW.loss_loc_county,
	JNR_CRO_EDW.loss_loc_city,
	JNR_CRO_EDW.loss_loc_addr,
	JNR_CRO_EDW.claim_cat_code,
	JNR_CRO_EDW.s3p_claim_num AS in_claim_num,
	-- *INF*: IIF(ISNULL(in_claim_num) OR RTRIM(LTRIM(in_claim_num)) = '',
	-- 'N/A',
	-- RTRIM(LTRIM(in_claim_num)))
	IFF(in_claim_num IS NULL OR RTRIM(LTRIM(in_claim_num)) = '', 'N/A', RTRIM(LTRIM(in_claim_num))) AS o_claim_num,
	JNR_CRO_EDW.reins_notified_date,
	JNR_CRO_EDW.claim_occurrence_num,
	JNR_CRO_EDW.claim_voilation_citation_descript AS claim_violation_citation_descript,
	JNR_CRO_EDW.claim_loss_descript,
	JNR_CRO_EDW.claim_insd_at_fault_code,
	LKP_Sup_Claim_Insured_At_Fault_Code.claim_insd_at_fault_descript AS i_claim_insd_at_fault_descript,
	-- *INF*: IIF(ISNULL(i_claim_insd_at_fault_descript),'N/A',i_claim_insd_at_fault_descript)
	IFF(i_claim_insd_at_fault_descript IS NULL, 'N/A', i_claim_insd_at_fault_descript) AS o_claim_insd_at_fault_code_descript,
	JNR_CRO_EDW.claim_insd_driver_num AS claim_insd_drvr_num,
	JNR_CRO_EDW.claim_insd_driver_ind AS claim_insd_drvr_ind,
	JNR_CRO_EDW.offset_onset_ind AS claim_offset_onset_ind,
	JNR_CRO_EDW.err_flag_bal_txn,
	JNR_CRO_EDW.err_flag_bal_reins,
	JNR_CRO_EDW.source_sys_id,
	JNR_CRO_EDW.claim_created_by_key,
	JNR_CRO_EDW.claim_log_note_last_act_date,
	JNR_CRO_EDW.next_diary_date,
	JNR_CRO_EDW.next_diary_date_rep,
	-- *INF*: IIF(isnull(claim_rep_role_code), '-1', claim_rep_role_code)
	IFF(claim_rep_role_code IS NULL, '-1', claim_rep_role_code) AS claim_rep_role_code_out,
	-- *INF*: iif(isnull(in_claim_rep_role_code_descript),'N/A',in_claim_rep_role_code_descript)
	IFF(in_claim_rep_role_code_descript IS NULL, 'N/A', in_claim_rep_role_code_descript) AS claim_rep_role_code_descript_out,
	JNR_CRO_EDW.transferred_claim_dvsn_lvl_ind_E,
	-- *INF*: IIF(ISNULL(transferred_claim_dvsn_lvl_ind_E), '-1', transferred_claim_dvsn_lvl_ind_E)
	IFF(transferred_claim_dvsn_lvl_ind_E IS NULL, '-1', transferred_claim_dvsn_lvl_ind_E) AS transferred_claim_dvsn_lvl_ind_out_E,
	JNR_CRO_EDW.transferred_claim_dept_lvl_ind_E,
	-- *INF*: IIF(ISNULL(transferred_claim_dept_lvl_ind_E), '-1', transferred_claim_dept_lvl_ind_E)
	IFF(transferred_claim_dept_lvl_ind_E IS NULL, '-1', transferred_claim_dept_lvl_ind_E) AS transferred_claim_dept_lvl_ind_out_E,
	JNR_CRO_EDW.transferred_claim_handling_office_lvl_ind_E,
	-- *INF*: IIF(ISNULL(transferred_claim_handling_office_lvl_ind_E), '-1', transferred_claim_handling_office_lvl_ind_E)
	IFF(transferred_claim_handling_office_lvl_ind_E IS NULL, '-1', transferred_claim_handling_office_lvl_ind_E) AS transferred_claim_handling_office_lvl_ind_out_E,
	JNR_CRO_EDW.transferred_claim_adjuster_lvl_ind_E,
	-- *INF*: IIF(ISNULL(transferred_claim_adjuster_lvl_ind_E), '-1', transferred_claim_adjuster_lvl_ind_E)
	IFF(transferred_claim_adjuster_lvl_ind_E IS NULL, '-1', transferred_claim_adjuster_lvl_ind_E) AS transferred_claim_adjuster_lvl_ind_out_E,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	JNR_CRO_EDW.eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	JNR_CRO_EDW.claim_occurrence_ak_id,
	JNR_CRO_EDW.transferred_claim_adjuster_lvl_ind_H,
	JNR_CRO_EDW.transferred_claim_handling_office_lvl_ind_H,
	JNR_CRO_EDW.transferred_claim_dept_lvl_ind_H,
	JNR_CRO_EDW.transferred_claim_dvsn_lvl_ind_H,
	JNR_CRO_EDW.claim_rep_occurrence_id_E,
	JNR_CRO_EDW.claim_rep_occurrence_id_H,
	JNR_CRO_EDW.claim_loss_date,
	JNR_CRO_EDW.claim_discovery_date,
	JNR_CRO_EDW.claim_occurrence_date,
	-- *INF*: DECODE(TRUE, v_claim_occurrence_status_code = 'OPEN', claim_occurrence_date, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	-- 
	-- -- If this evaluates to 1/1/1800, the value will be overwritten in the post-session update that runs in a target of this mapping.
	DECODE(TRUE,
	v_claim_occurrence_status_code = 'OPEN', claim_occurrence_date,
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')) AS v_claim_open_date,
	v_claim_open_date AS claim_open_date,
	-- *INF*: DECODE(TRUE, v_claim_occurrence_status_code = 'CLOSED', claim_occurrence_date, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	-- 
	-- -- If this evaluates to 1/1/1800, the value will be overwritten in the post-session update that runs in a target of this mapping.
	DECODE(TRUE,
	v_claim_occurrence_status_code = 'CLOSED', claim_occurrence_date,
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')) AS v_claim_closed_date,
	v_claim_closed_date AS claim_closed_date,
	-- *INF*: DECODE(TRUE,v_claim_occurrence_status_code = 'REOPEN',claim_occurrence_date,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	-- 
	-- -- If this evaluates to 1/1/1800, the value will be overwritten in the post-session update that runs in a target of this mapping.
	DECODE(TRUE,
	v_claim_occurrence_status_code = 'REOPEN', claim_occurrence_date,
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')) AS v_claim_reopen_date,
	v_claim_reopen_date AS claim_reopen_date,
	-- *INF*: DECODE(TRUE,v_claim_occurrence_status_code = 'CLOSEDAFTERREOPEN', claim_occurrence_date,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	-- 
	-- -- If this evaluates to 1/1/1800, the value will be overwritten in the post-session update that runs in a target of this mapping.
	DECODE(TRUE,
	v_claim_occurrence_status_code = 'CLOSEDAFTERREOPEN', claim_occurrence_date,
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')) AS v_claim_closed_after_reopen_date,
	v_claim_closed_after_reopen_date AS claim_closed_after_reopen_date,
	-- *INF*: DECODE(TRUE,v_claim_occurrence_status_code = 'NOTICEONLY',claim_occurrence_date,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	-- 
	-- -- If this evaluates to 1/1/1800, the value will be overwritten in the post-session update that runs in a target of this mapping.
	DECODE(TRUE,
	v_claim_occurrence_status_code = 'NOTICEONLY', claim_occurrence_date,
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')) AS v_claim_notice_only_date,
	v_claim_notice_only_date AS claim_notice_only_date,
	JNR_CRO_EDW.claim_occurrence_reported_date,
	JNR_CRO_EDW.claim_cat_start_date,
	JNR_CRO_EDW.claim_cat_end_date,
	JNR_CRO_EDW.eff_from_date_E,
	eff_from_date_E AS eff_from_date_E1,
	JNR_CRO_EDW.eff_to_date_E,
	eff_to_date_E AS eff_to_date_E1,
	JNR_CRO_EDW.eff_from_date_H,
	eff_from_date_H AS eff_from_date_H1,
	JNR_CRO_EDW.eff_to_date_H,
	eff_to_date_H AS eff_to_date_H1,
	-- *INF*: TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS default_date,
	'N/A' AS default_NA,
	claim_occurrence_ak_id AS v_prev_row_claim_occurrence_ak_id,
	v_claim_open_date AS v_prev_row_claim_open_date,
	v_claim_closed_date AS v_prev_row_claim_closed_date,
	v_claim_reopen_date AS v_prev_row_claim_reopen_date,
	v_claim_closed_after_reopen_date AS v_prev_row_claim_closed_after_reopen_date,
	v_claim_notice_only_date AS v_prev_row_claim_notice_only_date,
	LKP_Sup_State.state_descript AS i_state_descript,
	-- *INF*: IIF(ISNULL(i_state_descript) OR RTRIM(LTRIM(i_state_descript))='','N/A',i_state_descript)
	-- 
	IFF(i_state_descript IS NULL OR RTRIM(LTRIM(i_state_descript)) = '', 'N/A', i_state_descript) AS o_LossLocationStateDescription,
	JNR_CRO_EDW.PrimaryWorkGroup AS in_PrimaryWorkGroup,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_PrimaryWorkGroup)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_PrimaryWorkGroup) AS out_PrimaryWorkGroup,
	JNR_CRO_EDW.SecondaryWorkGroup AS in_SecondaryWorkGroup,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_SecondaryWorkGroup)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_SecondaryWorkGroup) AS out_SecondaryWorkGroup,
	JNR_CRO_EDW.ClaimRelationshipKey AS in_ClaimRelationshipKey,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_ClaimRelationshipKey)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_ClaimRelationshipKey) AS out_ClaimRelationshipKey,
	LKP_SupClaimReportedMethodDescription.ClaimReportedMethodDescription AS in_ClaimReportedMethodDescription,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_ClaimReportedMethodDescription)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_ClaimReportedMethodDescription) AS out_ClaimReportedMethodDescription,
	LKP_ClaimStory.Catalyst AS in_Catalyst,
	-- *INF*: IIF(ISNULL(in_Catalyst),'N/A',in_Catalyst)
	IFF(in_Catalyst IS NULL, 'N/A', in_Catalyst) AS out_Catalyst,
	LKP_ClaimStory.CauseOfDamage AS in_CauseOfDamage,
	-- *INF*: IIF(ISNULL(in_CauseOfDamage),'N/A',in_CauseOfDamage)
	IFF(in_CauseOfDamage IS NULL, 'N/A', in_CauseOfDamage) AS out_CauseOfDamage,
	LKP_ClaimStory.DamageCaused AS in_DamageCaused,
	-- *INF*: IIF(ISNULL(in_DamageCaused),'N/A',in_DamageCaused)
	IFF(in_DamageCaused IS NULL, 'N/A', in_DamageCaused) AS out_DamageCaused,
	LKP_ClaimStory.ItemDamaged AS in_ItemDamaged,
	-- *INF*: IIF(ISNULL(in_ItemDamaged),'N/A',in_ItemDamaged)
	IFF(in_ItemDamaged IS NULL, 'N/A', in_ItemDamaged) AS out_ItemDamaged
	FROM JNR_CRO_EDW
	LEFT JOIN LKP_ClaimStory
	ON LKP_ClaimStory.ClaimOccurrenceKey = JNR_CRO_EDW.claim_occurrence_key
	LEFT JOIN LKP_SupClaimReportedMethodDescription
	ON LKP_SupClaimReportedMethodDescription.ClaimReportedMethodCode = JNR_CRO_EDW.rpt_method
	LEFT JOIN LKP_Sup_Claim_Insured_At_Fault_Code
	ON LKP_Sup_Claim_Insured_At_Fault_Code.sup_claim_insd_at_fault_code_id = JNR_CRO_EDW.SupClaimInsuredAtFaultCodeId
	LEFT JOIN LKP_Sup_State
	ON LKP_Sup_State.state_code = JNR_CRO_EDW.loss_loc_state
),
FIL_Invalid_Record AS (
	SELECT
	edw_claim_occurrence_pk_id_out, 
	edw_claim_occurrence_calculation_pk_id_out, 
	edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out, 
	edw_claim_occurrence_reserve_calculation_exp_pk_id_out, 
	edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out, 
	edw_claim_occurrence_reserve_calculation_salvage_pk_id_out, 
	edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out, 
	claim_occurrence_key, 
	out_claim_occurrence_status_code, 
	claim_occurrence_direct_loss_status_code_out, 
	claim_occurrence_exp_status_code_out, 
	claim_occurrence_subrogation_status_code_out, 
	claim_occurrence_salvage_status_code_out, 
	claim_occurrence_other_recovery_status_code_out, 
	wc_cat_code_out AS claim_occurrence_wc_cat_code, 
	claim_occurrence_reopen_ind_out, 
	claim_occurrence_financial_ind_out, 
	claim_occurrence_supplemental_ind_out, 
	claim_occurrence_recovery_ind_out, 
	claim_occurrence_notice_only_claim_ind_out, 
	claim_occurrence_type, 
	source_claim_occurrence_status_code, 
	claim_created_by, 
	s3p_claim_created_date, 
	source_claim_rpted_date, 
	rpt_method, 
	how_claim_rpted, 
	loss_loc_zip, 
	loss_loc_state, 
	loss_loc_county, 
	loss_loc_city, 
	loss_loc_addr, 
	claim_cat_code, 
	o_claim_num AS claim_num, 
	reins_notified_date, 
	claim_occurrence_num, 
	claim_violation_citation_descript, 
	claim_loss_descript, 
	claim_insd_at_fault_code, 
	o_claim_insd_at_fault_code_descript AS claim_insd_at_fault_code_descript_out, 
	claim_insd_drvr_num, 
	claim_insd_drvr_ind, 
	claim_offset_onset_ind, 
	err_flag_bal_txn, 
	err_flag_bal_reins, 
	source_sys_id, 
	claim_created_by_key, 
	claim_log_note_last_act_date, 
	next_diary_date, 
	next_diary_date_rep, 
	claim_rep_role_code_out, 
	claim_rep_role_code_descript_out, 
	transferred_claim_dvsn_lvl_ind_out_E, 
	transferred_claim_dept_lvl_ind_out_E, 
	transferred_claim_handling_office_lvl_ind_out_E, 
	transferred_claim_adjuster_lvl_ind_out_E, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	claim_occurrence_ak_id, 
	transferred_claim_adjuster_lvl_ind_H, 
	transferred_claim_handling_office_lvl_ind_H, 
	transferred_claim_dept_lvl_ind_H, 
	transferred_claim_dvsn_lvl_ind_H, 
	claim_rep_occurrence_id_E, 
	claim_rep_occurrence_id_H, 
	claim_loss_date, 
	claim_discovery_date, 
	claim_open_date, 
	claim_closed_date, 
	claim_reopen_date, 
	claim_closed_after_reopen_date, 
	claim_notice_only_date, 
	claim_occurrence_reported_date, 
	claim_cat_start_date, 
	claim_cat_end_date, 
	eff_from_date_E1, 
	eff_to_date_E1, 
	eff_from_date_H1, 
	eff_to_date_H1, 
	default_date, 
	default_NA, 
	o_LossLocationStateDescription AS LossLocationStateDescription, 
	out_PrimaryWorkGroup AS PrimaryWorkGroup, 
	out_SecondaryWorkGroup AS SecondaryWorkGroup, 
	out_ClaimRelationshipKey AS ClaimRelationshipKey, 
	out_ClaimReportedMethodDescription AS ClaimReportedMethodDescription, 
	out_Catalyst AS Catalyst, 
	out_CauseOfDamage AS CauseOfDamage, 
	out_DamageCaused AS DamageCaused, 
	out_ItemDamaged AS ItemDamaged
	FROM EXP_Set_Default_Value
	WHERE DECODE(TRUE, SUBSTR(out_claim_occurrence_status_code,1,6) = 'CLOSED' AND  (claim_occurrence_direct_loss_status_code_out = 'OPEN' OR claim_occurrence_exp_status_code_out = 'OPEN' OR claim_occurrence_subrogation_status_code_out = 'OPEN' OR claim_occurrence_salvage_status_code_out = 'OPEN' OR claim_occurrence_other_recovery_status_code_out = 'OPEN' OR  claim_occurrence_direct_loss_status_code_out = 'REOPEN' OR claim_occurrence_exp_status_code_out = 'REOPEN' OR claim_occurrence_subrogation_status_code_out = 'REOPEN' OR claim_occurrence_salvage_status_code_out = 'REOPEN' OR claim_occurrence_other_recovery_status_code_out = 'REOPEN'), FALSE,  (SUBSTR(out_claim_occurrence_status_code,1,4) = 'OPEN' OR SUBSTR(out_claim_occurrence_status_code,1,6) = 'REOPEN' OR SUBSTR(out_claim_occurrence_status_code,1,6) = 'CLOSED') AND SUBSTR(claim_occurrence_direct_loss_status_code_out,1,6) = 'N/A' AND SUBSTR(claim_occurrence_exp_status_code_out,1,6) = 'N/A' AND SUBSTR(claim_occurrence_subrogation_status_code_out,1,6) = 'N/A' AND SUBSTR(claim_occurrence_salvage_status_code_out,1,6) = 'N/A' AND SUBSTR(claim_occurrence_other_recovery_status_code_out,1,6) = 'N/A', TRUE,    (SUBSTR(out_claim_occurrence_status_code,1,4) = 'OPEN' OR SUBSTR(out_claim_occurrence_status_code,1,6) = 'REOPEN') AND ( (SUBSTR(claim_occurrence_direct_loss_status_code_out,1,6) = 'CLOSED' OR claim_occurrence_direct_loss_status_code_out = 'N/A' or claim_occurrence_direct_loss_status_code_out = 'NOTICEONLY') AND (SUBSTR(claim_occurrence_exp_status_code_out,1,6) = 'CLOSED' OR claim_occurrence_exp_status_code_out = 'N/A'  or claim_occurrence_exp_status_code_out = 'NOTICEONLY') AND (SUBSTR(claim_occurrence_subrogation_status_code_out,1,6) = 'CLOSED' OR claim_occurrence_subrogation_status_code_out = 'N/A'  or claim_occurrence_subrogation_status_code_out = 'NOTICEONLY') AND (SUBSTR(claim_occurrence_salvage_status_code_out,1,6) = 'CLOSED' OR claim_occurrence_salvage_status_code_out = 'N/A'  or claim_occurrence_salvage_status_code_out = 'NOTICEONLY') AND (SUBSTR(claim_occurrence_other_recovery_status_code_out,1,6) = 'CLOSED' OR claim_occurrence_other_recovery_status_code_out = 'N/A'  or claim_occurrence_other_recovery_status_code_out = 'NOTICEONLY') ), FALSE,   out_claim_occurrence_status_code = 'NOTICEONLY' AND  ( claim_occurrence_direct_loss_status_code_out = 'OPEN' OR  claim_occurrence_direct_loss_status_code_out = 'REOPEN' OR  SUBSTR(claim_occurrence_direct_loss_status_code_out,1,6) = 'CLOSED' OR  claim_occurrence_exp_status_code_out = 'OPEN' OR  claim_occurrence_exp_status_code_out = 'REOPEN' OR  SUBSTR(claim_occurrence_exp_status_code_out,1,6) = 'CLOSED' OR  claim_occurrence_subrogation_status_code_out = 'OPEN' OR  claim_occurrence_subrogation_status_code_out = 'REOPEN' OR  SUBSTR(claim_occurrence_subrogation_status_code_out,1,6) = 'CLOSED' OR  claim_occurrence_salvage_status_code_out = 'OPEN' OR  claim_occurrence_salvage_status_code_out = 'REOPEN' OR  SUBSTR(claim_occurrence_salvage_status_code_out,1,6) = 'CLOSED' OR  claim_occurrence_other_recovery_status_code_out = 'OPEN' OR  claim_occurrence_other_recovery_status_code_out = 'REOPEN' OR  SUBSTR(claim_occurrence_other_recovery_status_code_out,1,6) = 'CLOSED'   ) ,FALSE,  SUBSTR(out_claim_occurrence_status_code,1,6) = 'CLOSED' AND  SUBSTR(claim_occurrence_direct_loss_status_code_out,1,6) <> 'CLOSED' AND  SUBSTR(claim_occurrence_exp_status_code_out,1,6)  <> 'CLOSED' AND  SUBSTR(claim_occurrence_subrogation_status_code_out,1,6) <> 'CLOSED' AND  SUBSTR(claim_occurrence_salvage_status_code_out,1,6)  <> 'CLOSED' AND  SUBSTR(claim_occurrence_other_recovery_status_code_out,1,6)  <> 'CLOSED'  , FALSE,   TRUE)  //---------------------------------------------------------------------------------------------------- 
//This expression eliminates any invalid combination of reserve and non-reserve status. 
//If CO Status is CLOSED AND if any of the other fin typ code status is either open or reopen then  //filter the row out. 
// If CO Status is Open or Reopen AND if any of the other fin typ code status is either open or reopen  //then DON'T filter the row out. 
// If CO Status is NOTICEONLY AND all the other fin typ code status also NOTICEONLY or N/A then  //DON'T filter the row out 
//If NR Status is Open/Openedinerror/Openwithnofinancial and all fin types are N/A then DON'T filter //If NR Status is Closed and if none of the other fin type status is closed, then filter the row //----------------------------------------------------------------------------------------------------
),
LKP_Claim_Occurrence_Dim AS (
	SELECT
	claim_occurrence_dim_id,
	edw_claim_occurrence_pk_id_out,
	edw_claim_occurrence_pk_id,
	edw_claim_occurrence_calculation_pk_id_out,
	edw_claim_occurrence_calculation_pk_id,
	edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out,
	edw_claim_occurrence_reserve_calculation_direct_loss_pk_id,
	edw_claim_occurrence_reserve_calculation_exp_pk_id_out,
	edw_claim_occurrence_reserve_calculation_exp_pk_id,
	edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out,
	edw_claim_occurrence_reserve_calculation_subrogation_pk_id,
	edw_claim_occurrence_reserve_calculation_salvage_pk_id_out,
	edw_claim_occurrence_reserve_calculation_salvage_pk_id,
	edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out,
	edw_claim_occurrence_reserve_calculation_other_recovery_pk_id,
	claim_rep_occurrence_id_H,
	edw_claim_rep_occurrence_pk_id_prim_claim_rep,
	claim_rep_occurrence_id_E,
	edw_claim_rep_occurrence_pk_id_examiner
	FROM (
		SELECT 
			claim_occurrence_dim_id,
			edw_claim_occurrence_pk_id_out,
			edw_claim_occurrence_pk_id,
			edw_claim_occurrence_calculation_pk_id_out,
			edw_claim_occurrence_calculation_pk_id,
			edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out,
			edw_claim_occurrence_reserve_calculation_direct_loss_pk_id,
			edw_claim_occurrence_reserve_calculation_exp_pk_id_out,
			edw_claim_occurrence_reserve_calculation_exp_pk_id,
			edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out,
			edw_claim_occurrence_reserve_calculation_subrogation_pk_id,
			edw_claim_occurrence_reserve_calculation_salvage_pk_id_out,
			edw_claim_occurrence_reserve_calculation_salvage_pk_id,
			edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out,
			edw_claim_occurrence_reserve_calculation_other_recovery_pk_id,
			claim_rep_occurrence_id_H,
			edw_claim_rep_occurrence_pk_id_prim_claim_rep,
			claim_rep_occurrence_id_E,
			edw_claim_rep_occurrence_pk_id_examiner
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_pk_id,edw_claim_occurrence_calculation_pk_id,edw_claim_occurrence_reserve_calculation_direct_loss_pk_id,edw_claim_occurrence_reserve_calculation_exp_pk_id,edw_claim_occurrence_reserve_calculation_subrogation_pk_id,edw_claim_occurrence_reserve_calculation_salvage_pk_id,edw_claim_occurrence_reserve_calculation_other_recovery_pk_id,edw_claim_rep_occurrence_pk_id_prim_claim_rep,edw_claim_rep_occurrence_pk_id_examiner ORDER BY claim_occurrence_dim_id) = 1
),
RTR_Claim_Occurrence_Dim AS (
	SELECT
	LKP_Claim_Occurrence_Dim.claim_occurrence_dim_id,
	FIL_Invalid_Record.edw_claim_occurrence_pk_id_out AS in_edw_claim_occurrence_pk_id,
	FIL_Invalid_Record.edw_claim_occurrence_calculation_pk_id_out AS in_edw_claim_occurrence_calculation_pk_id_out,
	FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out,
	FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_exp_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_exp_pk_id_out,
	FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out,
	FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_salvage_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_salvage_pk_id_out,
	FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out,
	FIL_Invalid_Record.claim_occurrence_key,
	FIL_Invalid_Record.out_claim_occurrence_status_code AS claim_occurrence_status_code,
	FIL_Invalid_Record.claim_occurrence_direct_loss_status_code_out,
	FIL_Invalid_Record.claim_occurrence_exp_status_code_out,
	FIL_Invalid_Record.claim_occurrence_subrogation_status_code_out,
	FIL_Invalid_Record.claim_occurrence_salvage_status_code_out,
	FIL_Invalid_Record.claim_occurrence_other_recovery_status_code_out,
	FIL_Invalid_Record.claim_occurrence_wc_cat_code AS claim_occurrence_wc_cat_code_out,
	FIL_Invalid_Record.claim_occurrence_reopen_ind_out,
	FIL_Invalid_Record.claim_occurrence_financial_ind_out,
	FIL_Invalid_Record.claim_occurrence_supplemental_ind_out,
	FIL_Invalid_Record.claim_occurrence_recovery_ind_out,
	FIL_Invalid_Record.claim_occurrence_notice_only_claim_ind_out,
	FIL_Invalid_Record.claim_occurrence_type,
	FIL_Invalid_Record.source_claim_occurrence_status_code,
	FIL_Invalid_Record.claim_created_by,
	FIL_Invalid_Record.s3p_claim_created_date,
	FIL_Invalid_Record.source_claim_rpted_date,
	FIL_Invalid_Record.rpt_method,
	FIL_Invalid_Record.how_claim_rpted,
	FIL_Invalid_Record.loss_loc_zip,
	FIL_Invalid_Record.loss_loc_state,
	FIL_Invalid_Record.loss_loc_county,
	FIL_Invalid_Record.loss_loc_city,
	FIL_Invalid_Record.loss_loc_addr,
	FIL_Invalid_Record.claim_cat_code,
	FIL_Invalid_Record.claim_num,
	FIL_Invalid_Record.reins_notified_date,
	FIL_Invalid_Record.claim_occurrence_num,
	FIL_Invalid_Record.claim_violation_citation_descript,
	FIL_Invalid_Record.claim_loss_descript,
	FIL_Invalid_Record.claim_insd_at_fault_code,
	FIL_Invalid_Record.claim_insd_at_fault_code_descript_out AS claim_insd_at_fault_descript,
	FIL_Invalid_Record.claim_insd_drvr_num,
	FIL_Invalid_Record.claim_insd_drvr_ind,
	FIL_Invalid_Record.claim_offset_onset_ind,
	FIL_Invalid_Record.err_flag_bal_txn,
	FIL_Invalid_Record.err_flag_bal_reins,
	FIL_Invalid_Record.source_sys_id,
	FIL_Invalid_Record.claim_created_by_key,
	FIL_Invalid_Record.claim_log_note_last_act_date,
	FIL_Invalid_Record.next_diary_date,
	FIL_Invalid_Record.next_diary_date_rep,
	FIL_Invalid_Record.claim_rep_role_code_out AS claim_rep_role_code,
	FIL_Invalid_Record.claim_rep_role_code_descript_out AS lkp_claim_rep_role_code_descript,
	FIL_Invalid_Record.transferred_claim_dvsn_lvl_ind_out_E AS transferred_claim_dvsn_lvl_ind,
	FIL_Invalid_Record.transferred_claim_dept_lvl_ind_out_E AS transferred_claim_dept_lvl_ind,
	FIL_Invalid_Record.transferred_claim_handling_office_lvl_ind_out_E AS transferred_claim_handling_office_lvl_ind,
	FIL_Invalid_Record.transferred_claim_adjuster_lvl_ind_out_E AS transferred_claim_adjuster_lvl_ind,
	FIL_Invalid_Record.crrnt_snpsht_flag,
	FIL_Invalid_Record.audit_id,
	FIL_Invalid_Record.eff_from_date,
	FIL_Invalid_Record.eff_to_date,
	FIL_Invalid_Record.created_date,
	FIL_Invalid_Record.modified_date,
	FIL_Invalid_Record.claim_occurrence_ak_id,
	FIL_Invalid_Record.claim_loss_date,
	FIL_Invalid_Record.claim_discovery_date,
	FIL_Invalid_Record.claim_occurrence_reported_date,
	FIL_Invalid_Record.claim_open_date,
	FIL_Invalid_Record.claim_closed_date,
	FIL_Invalid_Record.claim_reopen_date,
	FIL_Invalid_Record.claim_closed_after_reopen_date,
	FIL_Invalid_Record.claim_notice_only_date,
	FIL_Invalid_Record.claim_cat_start_date,
	FIL_Invalid_Record.claim_cat_end_date,
	FIL_Invalid_Record.transferred_claim_adjuster_lvl_ind_H,
	FIL_Invalid_Record.transferred_claim_handling_office_lvl_ind_H,
	FIL_Invalid_Record.transferred_claim_dept_lvl_ind_H,
	FIL_Invalid_Record.transferred_claim_dvsn_lvl_ind_H,
	FIL_Invalid_Record.eff_from_date_E1 AS eff_from_date_E,
	FIL_Invalid_Record.eff_to_date_E1 AS eff_to_date_E,
	FIL_Invalid_Record.eff_from_date_H1 AS eff_from_date_H,
	FIL_Invalid_Record.eff_to_date_H1 AS eff_to_date_H,
	FIL_Invalid_Record.claim_rep_occurrence_id_E,
	FIL_Invalid_Record.claim_rep_occurrence_id_H,
	FIL_Invalid_Record.default_date,
	FIL_Invalid_Record.default_NA,
	FIL_Invalid_Record.LossLocationStateDescription,
	FIL_Invalid_Record.PrimaryWorkGroup,
	FIL_Invalid_Record.SecondaryWorkGroup,
	FIL_Invalid_Record.ClaimRelationshipKey,
	FIL_Invalid_Record.ClaimReportedMethodDescription,
	FIL_Invalid_Record.Catalyst,
	FIL_Invalid_Record.CauseOfDamage,
	FIL_Invalid_Record.DamageCaused,
	FIL_Invalid_Record.ItemDamaged
	FROM FIL_Invalid_Record
	LEFT JOIN LKP_Claim_Occurrence_Dim
	ON LKP_Claim_Occurrence_Dim.edw_claim_occurrence_pk_id = FIL_Invalid_Record.edw_claim_occurrence_pk_id_out AND LKP_Claim_Occurrence_Dim.edw_claim_occurrence_calculation_pk_id = FIL_Invalid_Record.edw_claim_occurrence_calculation_pk_id_out AND LKP_Claim_Occurrence_Dim.edw_claim_occurrence_reserve_calculation_direct_loss_pk_id = FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out AND LKP_Claim_Occurrence_Dim.edw_claim_occurrence_reserve_calculation_exp_pk_id = FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_exp_pk_id_out AND LKP_Claim_Occurrence_Dim.edw_claim_occurrence_reserve_calculation_subrogation_pk_id = FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out AND LKP_Claim_Occurrence_Dim.edw_claim_occurrence_reserve_calculation_salvage_pk_id = FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_salvage_pk_id_out AND LKP_Claim_Occurrence_Dim.edw_claim_occurrence_reserve_calculation_other_recovery_pk_id = FIL_Invalid_Record.edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out AND LKP_Claim_Occurrence_Dim.edw_claim_rep_occurrence_pk_id_prim_claim_rep = FIL_Invalid_Record.claim_rep_occurrence_id_H AND LKP_Claim_Occurrence_Dim.edw_claim_rep_occurrence_pk_id_examiner = FIL_Invalid_Record.claim_rep_occurrence_id_E
),
RTR_Claim_Occurrence_Dim_INSERT AS (SELECT * FROM RTR_Claim_Occurrence_Dim WHERE isnull(claim_occurrence_dim_id)),
RTR_Claim_Occurrence_Dim_DEFAULT1 AS (SELECT * FROM RTR_Claim_Occurrence_Dim WHERE NOT ( (isnull(claim_occurrence_dim_id)) )),
UPD_Claim_Occurrence_Dim_UPDATE AS (
	SELECT
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	in_edw_claim_occurrence_pk_id AS in_edw_claim_occurrence_pk_id1, 
	in_edw_claim_occurrence_calculation_pk_id_out AS in_edw_claim_occurrence_calculation_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_exp_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_exp_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_salvage_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_salvage_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out1, 
	claim_occurrence_key AS claim_occurrence_key1, 
	claim_occurrence_status_code AS claim_occurrence_status_code1, 
	claim_occurrence_direct_loss_status_code_out AS claim_occurrence_direct_loss_status_code_out1, 
	claim_occurrence_exp_status_code_out AS claim_occurrence_exp_status_code_out1, 
	claim_occurrence_subrogation_status_code_out AS claim_occurrence_subrogation_status_code_out1, 
	claim_occurrence_salvage_status_code_out AS claim_occurrence_salvage_status_code_out1, 
	claim_occurrence_other_recovery_status_code_out AS claim_occurrence_other_recovery_status_code_out1, 
	claim_occurrence_wc_cat_code_out AS claim_occurrence_wc_cat_code_out1, 
	claim_occurrence_reopen_ind_out AS claim_occurrence_reopen_ind_out1, 
	claim_occurrence_financial_ind_out AS claim_occurrence_financial_ind_out1, 
	claim_occurrence_supplemental_ind_out AS claim_occurrence_supplemental_ind_out1, 
	claim_occurrence_recovery_ind_out AS claim_occurrence_recovery_ind_out1, 
	claim_occurrence_notice_only_claim_ind_out AS claim_occurrence_notice_only_claim_ind_out1, 
	claim_occurrence_type AS claim_occurrence_type1, 
	claim_created_by AS claim_created_by1, 
	source_claim_occurrence_status_code, 
	s3p_claim_created_date AS s3p_claim_created_date1, 
	source_claim_rpted_date, 
	rpt_method AS rpt_method1, 
	how_claim_rpted, 
	loss_loc_zip AS loss_loc_zip1, 
	loss_loc_state AS loss_loc_state1, 
	loss_loc_county AS loss_loc_county1, 
	loss_loc_city AS loss_loc_city1, 
	loss_loc_addr AS loss_loc_addr1, 
	claim_cat_code AS claim_cat_code1, 
	claim_num AS claim_num1, 
	reins_notified_date AS reins_notified_date1, 
	claim_occurrence_num AS claim_occurrence_num1, 
	claim_violation_citation_descript AS claim_violation_citation_descript1, 
	claim_loss_descript AS claim_loss_descript1, 
	claim_insd_at_fault_code AS claim_insd_at_fault_code1, 
	claim_insd_at_fault_descript, 
	claim_insd_drvr_num AS claim_insd_drvr_num1, 
	claim_insd_drvr_ind AS claim_insd_drvr_ind1, 
	claim_offset_onset_ind AS claim_offset_onset_ind1, 
	err_flag_bal_txn AS err_flag_bal_txn2, 
	err_flag_bal_reins AS err_flag_bal_reins2, 
	claim_log_note_last_act_date AS claim_log_note_last_act_date1, 
	next_diary_date AS next_diary_date1, 
	next_diary_date_rep AS next_diary_date_rep1, 
	claim_rep_role_code AS claim_rep_role_code1, 
	lkp_claim_rep_role_code_descript AS lkp_claim_rep_role_code_descript1, 
	transferred_claim_dvsn_lvl_ind AS transferred_claim_dvsn_lvl_ind1, 
	transferred_claim_dept_lvl_ind AS transferred_claim_dept_lvl_ind1, 
	transferred_claim_handling_office_lvl_ind AS transferred_claim_handling_office_lvl_ind1, 
	transferred_claim_adjuster_lvl_ind AS transferred_claim_adjuster_lvl_ind1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	claim_occurrence_ak_id AS claim_occurrence_ak_id1, 
	claim_loss_date AS claim_loss_date1, 
	claim_discovery_date AS claim_discovery_date1, 
	claim_occurrence_reported_date AS claim_occurrence_reported_date1, 
	claim_open_date AS claim_open_date1, 
	claim_closed_date AS claim_closed_date1, 
	claim_reopen_date AS claim_reopen_date1, 
	claim_closed_after_reopen_date AS claim_closed_after_reopen_date1, 
	claim_notice_only_date AS claim_notice_only_date2, 
	claim_cat_start_date AS claim_cat_start_date1, 
	claim_cat_end_date AS claim_cat_end_date1, 
	transferred_claim_adjuster_lvl_ind_H AS transferred_claim_adjuster_lvl_ind_H1, 
	transferred_claim_handling_office_lvl_ind_H AS transferred_claim_handling_office_lvl_ind_H1, 
	transferred_claim_dept_lvl_ind_H AS transferred_claim_dept_lvl_ind_H1, 
	transferred_claim_dvsn_lvl_ind_H AS transferred_claim_dvsn_lvl_ind_H1, 
	eff_from_date_E AS eff_from_date_E1, 
	eff_to_date_E AS eff_to_date_E1, 
	eff_from_date_H AS eff_from_date_H1, 
	eff_to_date_H AS eff_to_date_H1, 
	claim_rep_occurrence_id_E AS claim_rep_occurrence_id_E1, 
	claim_rep_occurrence_id_H AS claim_rep_occurrence_id_H1, 
	default_date AS default_date1, 
	default_NA AS default_NA1, 
	source_sys_id AS source_sys_id2, 
	claim_created_by_key AS claim_created_by_key2, 
	LossLocationStateDescription AS LossLocationStateDescription2, 
	PrimaryWorkGroup, 
	SecondaryWorkGroup, 
	ClaimRelationshipKey, 
	ClaimReportedMethodDescription, 
	Catalyst, 
	CauseOfDamage, 
	DamageCaused, 
	ItemDamaged
	FROM RTR_Claim_Occurrence_Dim_DEFAULT1
),
Claim_Occurrence_Dim_Update AS (
	MERGE INTO claim_occurrence_dim AS T
	USING UPD_Claim_Occurrence_Dim_UPDATE AS S
	ON T.claim_occurrence_dim_id = S.claim_occurrence_dim_id1
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claim_occurrence_pk_id = S.in_edw_claim_occurrence_pk_id1, T.edw_claim_occurrence_calculation_pk_id = S.in_edw_claim_occurrence_calculation_pk_id_out1, T.edw_claim_occurrence_reserve_calculation_direct_loss_pk_id = S.in_edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out1, T.edw_claim_occurrence_reserve_calculation_exp_pk_id = S.in_edw_claim_occurrence_reserve_calculation_exp_pk_id_out1, T.edw_claim_occurrence_reserve_calculation_subrogation_pk_id = S.in_edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out1, T.edw_claim_occurrence_reserve_calculation_salvage_pk_id = S.in_edw_claim_occurrence_reserve_calculation_salvage_pk_id_out1, T.edw_claim_occurrence_reserve_calculation_other_recovery_pk_id = S.in_edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out1, T.edw_claim_rep_occurrence_pk_id_prim_claim_rep = S.claim_rep_occurrence_id_H1, T.edw_claim_rep_occurrence_pk_id_examiner = S.claim_rep_occurrence_id_E1, T.edw_claim_occurrence_ak_id = S.claim_occurrence_ak_id1, T.claim_occurrence_key = S.claim_occurrence_key1, T.source_claim_occurrence_status_code = S.source_claim_occurrence_status_code, T.claim_occurrence_status_type = S.claim_occurrence_status_code1, T.claim_occurrence_direct_loss_status_type = S.claim_occurrence_direct_loss_status_code_out1, T.claim_occurrence_exp_status_type = S.claim_occurrence_exp_status_code_out1, T.claim_occurrence_salvage_status_type = S.claim_occurrence_salvage_status_code_out1, T.claim_occurrence_subrogation_status_type = S.claim_occurrence_subrogation_status_code_out1, T.claim_occurrence_other_recovery_status_type = S.claim_occurrence_other_recovery_status_code_out1, T.claim_occurrence_financial_ind = S.claim_occurrence_financial_ind_out1, T.claim_occurrence_supplemental_ind = S.claim_occurrence_supplemental_ind_out1, T.claim_occurrence_recovery_ind = S.claim_occurrence_recovery_ind_out1, T.claim_occurrence_notice_only_claim_ind = S.claim_occurrence_notice_only_claim_ind_out1, T.claim_occurrence_type = S.claim_occurrence_type1, T.rpt_method = S.rpt_method1, T.how_claim_rpted = S.how_claim_rpted, T.claim_scripted_date = S.s3p_claim_created_date1, T.source_claim_rpted_date = S.source_claim_rpted_date, T.claim_rpted_date = S.claim_occurrence_reported_date1, T.loss_loc_zip = S.loss_loc_zip1, T.loss_loc_state = S.loss_loc_state1, T.loss_loc_county = S.loss_loc_county1, T.loss_loc_city = S.loss_loc_city1, T.loss_loc_addr = S.loss_loc_addr1, T.claim_loss_date = S.claim_loss_date1, T.claim_discovery_date = S.claim_discovery_date1, T.claim_open_date = S.claim_open_date1, T.claim_close_date = S.claim_closed_date1, T.claim_reopen_date = S.claim_reopen_date1, T.claim_closed_after_reopen_date = S.claim_closed_after_reopen_date1, T.claim_notice_only_date = S.claim_notice_only_date2, T.claim_cat_code = S.claim_cat_code1, T.claim_cat_start_date = S.claim_cat_start_date1, T.claim_cat_end_date = S.claim_cat_end_date1, T.claim_num = S.claim_num1, T.reins_notified_date = S.reins_notified_date1, T.claim_occurrence_num = S.claim_occurrence_num1, T.claim_violation_citation_descript = S.claim_violation_citation_descript1, T.claim_loss_descript = S.claim_loss_descript1, T.claim_insd_at_fault_code = S.claim_insd_at_fault_code1, T.claim_insd_at_fault_code_descript = S.claim_insd_at_fault_descript, T.claim_insd_driver_num = S.claim_insd_drvr_num1, T.claim_insd_driver_ind = S.claim_insd_drvr_ind1, T.claim_offset_onset_ind = S.claim_offset_onset_ind1, T.claim_log_note_last_act_date = S.claim_log_note_last_act_date1, T.next_diary_date = S.next_diary_date1, T.next_diary_date_rep = S.next_diary_date_rep1, T.prim_claim_rep_assigned_date = S.eff_from_date_H1, T.prim_claim_rep_transferred_claim_dvsn_lvl_ind = S.transferred_claim_dvsn_lvl_ind_H1, T.prim_claim_rep_transferred_claim_dept_lvl_ind = S.transferred_claim_dept_lvl_ind_H1, T.prim_claim_rep_transferred_claim_handling_office_lvl_ind = S.transferred_claim_handling_office_lvl_ind_H1, T.prim_claim_rep_transferred_claim_adjuster_lvl_ind = S.transferred_claim_adjuster_lvl_ind_H1, T.examiner_assigned_date = S.eff_from_date_E1, T.examiner_transferred_claim_dvsn_lvl_ind = S.transferred_claim_dvsn_lvl_ind1, T.examiner_transferred_claim_dept_lvl_ind = S.transferred_claim_dept_lvl_ind1, T.examiner_transferred_claim_handling_office_lvl_ind = S.transferred_claim_handling_office_lvl_ind1, T.examiner_transferred_claim_adjuster_lvl_ind = S.transferred_claim_adjuster_lvl_ind1, T.err_flag_bal_txn = S.err_flag_bal_txn2, T.err_flag_bal_reins = S.err_flag_bal_reins2, T.audit_id = S.audit_id1, T.modified_date = S.modified_date1, T.claim_created_by_key = S.claim_created_by_key2, T.wc_cat_code = S.claim_occurrence_wc_cat_code_out1, T.PrimaryWorkGroup = S.PrimaryWorkGroup, T.SecondaryWorkGroup = S.SecondaryWorkGroup, T.LossLocationStateDescription = S.LossLocationStateDescription2, T.ClaimRelationshipKey = S.ClaimRelationshipKey, T.ClaimReportedMethodDescription = S.ClaimReportedMethodDescription, T.Catalyst = S.Catalyst, T.CauseOfDamage = S.CauseOfDamage, T.DamageCaused = S.DamageCaused, T.ItemDamaged = S.ItemDamaged, T.SourceSystemId = S.source_sys_id2
),
UPD_Claim_Occurrence_Dim_INSERT AS (
	SELECT
	claim_occurrence_dim_id AS claim_occurrence_dim_id1, 
	in_edw_claim_occurrence_pk_id AS in_edw_claim_occurrence_pk_id1, 
	in_edw_claim_occurrence_calculation_pk_id_out AS in_edw_claim_occurrence_calculation_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_exp_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_exp_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_salvage_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_salvage_pk_id_out1, 
	in_edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out AS in_edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out1, 
	claim_occurrence_key AS claim_occurrence_key1, 
	claim_occurrence_status_code AS claim_occurrence_status_code1, 
	claim_occurrence_direct_loss_status_code_out AS claim_occurrence_direct_loss_status_code_out1, 
	claim_occurrence_exp_status_code_out AS claim_occurrence_exp_status_code_out1, 
	claim_occurrence_subrogation_status_code_out AS claim_occurrence_subrogation_status_code_out1, 
	claim_occurrence_salvage_status_code_out AS claim_occurrence_salvage_status_code_out1, 
	claim_occurrence_other_recovery_status_code_out AS claim_occurrence_other_recovery_status_code_out1, 
	claim_occurrence_wc_cat_code_out AS claim_occurrence_wc_cat_code_out1, 
	claim_occurrence_reopen_ind_out AS claim_occurrence_reopen_ind_out1, 
	claim_occurrence_financial_ind_out AS claim_occurrence_financial_ind_out1, 
	claim_occurrence_supplemental_ind_out AS claim_occurrence_supplemental_ind_out1, 
	claim_occurrence_recovery_ind_out AS claim_occurrence_recovery_ind_out1, 
	claim_occurrence_notice_only_claim_ind_out AS claim_occurrence_notice_only_claim_ind_out1, 
	claim_occurrence_type AS claim_occurrence_type1, 
	source_claim_occurrence_status_code, 
	claim_created_by AS claim_created_by1, 
	s3p_claim_created_date, 
	source_claim_rpted_date, 
	rpt_method AS rpt_method1, 
	how_claim_rpted, 
	loss_loc_zip AS loss_loc_zip1, 
	loss_loc_state AS loss_loc_state1, 
	loss_loc_county AS loss_loc_county1, 
	loss_loc_city AS loss_loc_city1, 
	loss_loc_addr AS loss_loc_addr1, 
	claim_cat_code AS claim_cat_code1, 
	claim_num AS claim_num1, 
	reins_notified_date AS reins_notified_date1, 
	claim_occurrence_num AS claim_occurrence_num1, 
	claim_violation_citation_descript AS claim_violation_citation_descript1, 
	claim_loss_descript AS claim_loss_descript1, 
	claim_insd_at_fault_code AS claim_insd_at_fault_code1, 
	claim_insd_at_fault_descript, 
	claim_insd_drvr_num AS claim_insd_drvr_num1, 
	claim_insd_drvr_ind AS claim_insd_drvr_ind1, 
	claim_offset_onset_ind AS claim_offset_onset_ind1, 
	err_flag_bal_txn AS err_flag_bal_txn1, 
	err_flag_bal_reins AS err_flag_bal_reins1, 
	source_sys_id AS source_sys_id1, 
	claim_created_by_key AS claim_created_by_key1, 
	claim_log_note_last_act_date AS claim_log_note_last_act_date1, 
	next_diary_date AS next_diary_date1, 
	next_diary_date_rep AS next_diary_date_rep1, 
	transferred_claim_dvsn_lvl_ind AS transferred_claim_dvsn_lvl_ind1, 
	transferred_claim_dept_lvl_ind AS transferred_claim_dept_lvl_ind1, 
	transferred_claim_handling_office_lvl_ind AS transferred_claim_handling_office_lvl_ind1, 
	transferred_claim_adjuster_lvl_ind AS transferred_claim_adjuster_lvl_ind1, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag1, 
	audit_id AS audit_id1, 
	eff_from_date AS eff_from_date1, 
	eff_to_date AS eff_to_date1, 
	created_date AS created_date1, 
	modified_date AS modified_date1, 
	claim_occurrence_ak_id AS claim_occurrence_ak_id1, 
	claim_loss_date AS claim_loss_date1, 
	claim_discovery_date AS claim_discovery_date1, 
	claim_occurrence_reported_date AS claim_occurrence_reported_date1, 
	claim_open_date AS claim_open_date1, 
	claim_closed_date AS claim_closed_date1, 
	claim_reopen_date AS claim_reopen_date1, 
	claim_closed_after_reopen_date AS claim_closed_after_reopen_date1, 
	claim_notice_only_date AS claim_notice_only_date1, 
	claim_cat_start_date AS claim_cat_start_date1, 
	claim_cat_end_date AS claim_cat_end_date1, 
	transferred_claim_adjuster_lvl_ind_H AS transferred_claim_adjuster_lvl_ind_H1, 
	transferred_claim_handling_office_lvl_ind_H AS transferred_claim_handling_office_lvl_ind_H1, 
	transferred_claim_dept_lvl_ind_H AS transferred_claim_dept_lvl_ind_H1, 
	transferred_claim_dvsn_lvl_ind_H AS transferred_claim_dvsn_lvl_ind_H1, 
	eff_from_date_E AS eff_from_date_E1, 
	eff_to_date_E AS eff_to_date_E1, 
	eff_from_date_H AS eff_from_date_H1, 
	eff_to_date_H AS eff_to_date_H1, 
	claim_rep_occurrence_id_E AS claim_rep_occurrence_id_E1, 
	claim_rep_occurrence_id_H AS claim_rep_occurrence_id_H1, 
	default_date AS default_date1, 
	default_NA AS default_NA1, 
	LossLocationStateDescription AS LossLocationStateDescription1, 
	PrimaryWorkGroup, 
	SecondaryWorkGroup, 
	ClaimRelationshipKey, 
	ClaimReportedMethodDescription, 
	Catalyst, 
	CauseOfDamage, 
	DamageCaused, 
	ItemDamaged
	FROM RTR_Claim_Occurrence_Dim_INSERT
),
Claim_Occurrence_Dim_Insert AS (
	INSERT INTO claim_occurrence_dim
	(edw_claim_occurrence_pk_id, edw_claim_occurrence_calculation_pk_id, edw_claim_occurrence_reserve_calculation_direct_loss_pk_id, edw_claim_occurrence_reserve_calculation_exp_pk_id, edw_claim_occurrence_reserve_calculation_subrogation_pk_id, edw_claim_occurrence_reserve_calculation_salvage_pk_id, edw_claim_occurrence_reserve_calculation_other_recovery_pk_id, edw_claim_rep_occurrence_pk_id_prim_claim_rep, edw_claim_rep_occurrence_pk_id_examiner, edw_claim_occurrence_ak_id, claim_occurrence_key, source_claim_occurrence_status_code, claim_occurrence_status_type, claim_occurrence_direct_loss_status_type, claim_occurrence_exp_status_type, claim_occurrence_salvage_status_type, claim_occurrence_subrogation_status_type, claim_occurrence_other_recovery_status_type, claim_occurrence_financial_ind, claim_occurrence_supplemental_ind, claim_occurrence_recovery_ind, claim_occurrence_notice_only_claim_ind, claim_occurrence_type, rpt_method, how_claim_rpted, claim_scripted_date, source_claim_rpted_date, claim_rpted_date, loss_loc_zip, loss_loc_state, loss_loc_county, loss_loc_city, loss_loc_addr, claim_loss_date, claim_discovery_date, claim_open_date, claim_close_date, claim_reopen_date, claim_closed_after_reopen_date, claim_notice_only_date, claim_cat_code, claim_cat_start_date, claim_cat_end_date, claim_num, reins_notified_date, claim_occurrence_num, claim_violation_citation_descript, claim_loss_descript, claim_insd_at_fault_code, claim_insd_at_fault_code_descript, claim_insd_driver_num, claim_insd_driver_ind, claim_offset_onset_ind, claim_log_note_last_act_date, next_diary_date, next_diary_date_rep, prim_claim_rep_assigned_date, prim_claim_rep_transferred_claim_dvsn_lvl_ind, prim_claim_rep_transferred_claim_dept_lvl_ind, prim_claim_rep_transferred_claim_handling_office_lvl_ind, prim_claim_rep_transferred_claim_adjuster_lvl_ind, examiner_assigned_date, examiner_transferred_claim_dvsn_lvl_ind, examiner_transferred_claim_dept_lvl_ind, examiner_transferred_claim_handling_office_lvl_ind, examiner_transferred_claim_adjuster_lvl_ind, err_flag_bal_txn, err_flag_bal_reins, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, claim_created_by_key, wc_cat_code, PrimaryWorkGroup, SecondaryWorkGroup, LossLocationStateDescription, ClaimRelationshipKey, ClaimReportedMethodDescription, Catalyst, CauseOfDamage, DamageCaused, ItemDamaged, SourceSystemId)
	SELECT 
	in_edw_claim_occurrence_pk_id1 AS EDW_CLAIM_OCCURRENCE_PK_ID, 
	in_edw_claim_occurrence_calculation_pk_id_out1 AS EDW_CLAIM_OCCURRENCE_CALCULATION_PK_ID, 
	in_edw_claim_occurrence_reserve_calculation_direct_loss_pk_id_out1 AS EDW_CLAIM_OCCURRENCE_RESERVE_CALCULATION_DIRECT_LOSS_PK_ID, 
	in_edw_claim_occurrence_reserve_calculation_exp_pk_id_out1 AS EDW_CLAIM_OCCURRENCE_RESERVE_CALCULATION_EXP_PK_ID, 
	in_edw_claim_occurrence_reserve_calculation_subrogation_pk_id_out1 AS EDW_CLAIM_OCCURRENCE_RESERVE_CALCULATION_SUBROGATION_PK_ID, 
	in_edw_claim_occurrence_reserve_calculation_salvage_pk_id_out1 AS EDW_CLAIM_OCCURRENCE_RESERVE_CALCULATION_SALVAGE_PK_ID, 
	in_edw_claim_occurrence_reserve_calculation_other_recovery_pk_id_out1 AS EDW_CLAIM_OCCURRENCE_RESERVE_CALCULATION_OTHER_RECOVERY_PK_ID, 
	claim_rep_occurrence_id_H1 AS EDW_CLAIM_REP_OCCURRENCE_PK_ID_PRIM_CLAIM_REP, 
	claim_rep_occurrence_id_E1 AS EDW_CLAIM_REP_OCCURRENCE_PK_ID_EXAMINER, 
	claim_occurrence_ak_id1 AS EDW_CLAIM_OCCURRENCE_AK_ID, 
	claim_occurrence_key1 AS CLAIM_OCCURRENCE_KEY, 
	SOURCE_CLAIM_OCCURRENCE_STATUS_CODE, 
	claim_occurrence_status_code1 AS CLAIM_OCCURRENCE_STATUS_TYPE, 
	claim_occurrence_direct_loss_status_code_out1 AS CLAIM_OCCURRENCE_DIRECT_LOSS_STATUS_TYPE, 
	claim_occurrence_exp_status_code_out1 AS CLAIM_OCCURRENCE_EXP_STATUS_TYPE, 
	claim_occurrence_salvage_status_code_out1 AS CLAIM_OCCURRENCE_SALVAGE_STATUS_TYPE, 
	claim_occurrence_subrogation_status_code_out1 AS CLAIM_OCCURRENCE_SUBROGATION_STATUS_TYPE, 
	claim_occurrence_other_recovery_status_code_out1 AS CLAIM_OCCURRENCE_OTHER_RECOVERY_STATUS_TYPE, 
	claim_occurrence_financial_ind_out1 AS CLAIM_OCCURRENCE_FINANCIAL_IND, 
	claim_occurrence_supplemental_ind_out1 AS CLAIM_OCCURRENCE_SUPPLEMENTAL_IND, 
	claim_occurrence_recovery_ind_out1 AS CLAIM_OCCURRENCE_RECOVERY_IND, 
	claim_occurrence_notice_only_claim_ind_out1 AS CLAIM_OCCURRENCE_NOTICE_ONLY_CLAIM_IND, 
	claim_occurrence_type1 AS CLAIM_OCCURRENCE_TYPE, 
	rpt_method1 AS RPT_METHOD, 
	HOW_CLAIM_RPTED, 
	s3p_claim_created_date AS CLAIM_SCRIPTED_DATE, 
	SOURCE_CLAIM_RPTED_DATE, 
	claim_occurrence_reported_date1 AS CLAIM_RPTED_DATE, 
	loss_loc_zip1 AS LOSS_LOC_ZIP, 
	loss_loc_state1 AS LOSS_LOC_STATE, 
	loss_loc_county1 AS LOSS_LOC_COUNTY, 
	loss_loc_city1 AS LOSS_LOC_CITY, 
	loss_loc_addr1 AS LOSS_LOC_ADDR, 
	claim_loss_date1 AS CLAIM_LOSS_DATE, 
	claim_discovery_date1 AS CLAIM_DISCOVERY_DATE, 
	claim_open_date1 AS CLAIM_OPEN_DATE, 
	claim_closed_date1 AS CLAIM_CLOSE_DATE, 
	claim_reopen_date1 AS CLAIM_REOPEN_DATE, 
	claim_closed_after_reopen_date1 AS CLAIM_CLOSED_AFTER_REOPEN_DATE, 
	claim_notice_only_date1 AS CLAIM_NOTICE_ONLY_DATE, 
	claim_cat_code1 AS CLAIM_CAT_CODE, 
	claim_cat_start_date1 AS CLAIM_CAT_START_DATE, 
	claim_cat_end_date1 AS CLAIM_CAT_END_DATE, 
	claim_num1 AS CLAIM_NUM, 
	reins_notified_date1 AS REINS_NOTIFIED_DATE, 
	claim_occurrence_num1 AS CLAIM_OCCURRENCE_NUM, 
	claim_violation_citation_descript1 AS CLAIM_VIOLATION_CITATION_DESCRIPT, 
	claim_loss_descript1 AS CLAIM_LOSS_DESCRIPT, 
	claim_insd_at_fault_code1 AS CLAIM_INSD_AT_FAULT_CODE, 
	claim_insd_at_fault_descript AS CLAIM_INSD_AT_FAULT_CODE_DESCRIPT, 
	claim_insd_drvr_num1 AS CLAIM_INSD_DRIVER_NUM, 
	claim_insd_drvr_ind1 AS CLAIM_INSD_DRIVER_IND, 
	claim_offset_onset_ind1 AS CLAIM_OFFSET_ONSET_IND, 
	claim_log_note_last_act_date1 AS CLAIM_LOG_NOTE_LAST_ACT_DATE, 
	next_diary_date1 AS NEXT_DIARY_DATE, 
	next_diary_date_rep1 AS NEXT_DIARY_DATE_REP, 
	eff_from_date_H1 AS PRIM_CLAIM_REP_ASSIGNED_DATE, 
	transferred_claim_dvsn_lvl_ind_H1 AS PRIM_CLAIM_REP_TRANSFERRED_CLAIM_DVSN_LVL_IND, 
	transferred_claim_dept_lvl_ind_H1 AS PRIM_CLAIM_REP_TRANSFERRED_CLAIM_DEPT_LVL_IND, 
	transferred_claim_handling_office_lvl_ind_H1 AS PRIM_CLAIM_REP_TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND, 
	transferred_claim_adjuster_lvl_ind_H1 AS PRIM_CLAIM_REP_TRANSFERRED_CLAIM_ADJUSTER_LVL_IND, 
	eff_from_date_E1 AS EXAMINER_ASSIGNED_DATE, 
	transferred_claim_dvsn_lvl_ind1 AS EXAMINER_TRANSFERRED_CLAIM_DVSN_LVL_IND, 
	transferred_claim_dept_lvl_ind1 AS EXAMINER_TRANSFERRED_CLAIM_DEPT_LVL_IND, 
	transferred_claim_handling_office_lvl_ind1 AS EXAMINER_TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND, 
	transferred_claim_adjuster_lvl_ind1 AS EXAMINER_TRANSFERRED_CLAIM_ADJUSTER_LVL_IND, 
	err_flag_bal_txn1 AS ERR_FLAG_BAL_TXN, 
	err_flag_bal_reins1 AS ERR_FLAG_BAL_REINS, 
	crrnt_snpsht_flag1 AS CRRNT_SNPSHT_FLAG, 
	audit_id1 AS AUDIT_ID, 
	eff_from_date1 AS EFF_FROM_DATE, 
	eff_to_date1 AS EFF_TO_DATE, 
	created_date1 AS CREATED_DATE, 
	modified_date1 AS MODIFIED_DATE, 
	claim_created_by_key1 AS CLAIM_CREATED_BY_KEY, 
	claim_occurrence_wc_cat_code_out1 AS WC_CAT_CODE, 
	PRIMARYWORKGROUP, 
	SECONDARYWORKGROUP, 
	LossLocationStateDescription1 AS LOSSLOCATIONSTATEDESCRIPTION, 
	CLAIMRELATIONSHIPKEY, 
	CLAIMREPORTEDMETHODDESCRIPTION, 
	CATALYST, 
	CAUSEOFDAMAGE, 
	DAMAGECAUSED, 
	ITEMDAMAGED, 
	source_sys_id1 AS SOURCESYSTEMID
	FROM UPD_Claim_Occurrence_Dim_INSERT
),
SQ_claim_occurrence AS (
	select  claim_occurrence_id as claim_occurrence_pk_id,
	claim_occurrence_ak_id as claim_occurrence_ak_id,
	next_diary_date , next_diary_date_rep
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence
	where modified_date>'@{pipeline().parameters.SELECTION_START_TS}'
	and created_date<'@{pipeline().parameters.SELECTION_START_TS}'
),
Exp_Collect_values AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_ak_id,
	next_diary_date,
	next_diary_date_rep,
	1 AS o_crrnt_Flag,
	SYSDATE AS O_Modified_date
	FROM SQ_claim_occurrence
),
lkp_claim_occurrence_dim_diary AS (
	SELECT
	claim_occurrence_dim_id,
	in_crrnt_flg,
	edw_claim_occurrence_ak_id,
	edw_claim_occurrence_pk_id,
	crrnt_snpsht_flag
	FROM (
		SELECT 
			claim_occurrence_dim_id,
			in_crrnt_flg,
			edw_claim_occurrence_ak_id,
			edw_claim_occurrence_pk_id,
			crrnt_snpsht_flag
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,edw_claim_occurrence_pk_id,crrnt_snpsht_flag ORDER BY claim_occurrence_dim_id) = 1
),
UPD_Claim_occurrence_dim_diary AS (
	SELECT
	lkp_claim_occurrence_dim_diary.claim_occurrence_dim_id, 
	Exp_Collect_values.next_diary_date, 
	Exp_Collect_values.next_diary_date_rep, 
	Exp_Collect_values.O_Modified_date AS Modified_date
	FROM Exp_Collect_values
	LEFT JOIN lkp_claim_occurrence_dim_diary
	ON lkp_claim_occurrence_dim_diary.edw_claim_occurrence_ak_id = Exp_Collect_values.claim_occurrence_ak_id AND lkp_claim_occurrence_dim_diary.edw_claim_occurrence_pk_id = Exp_Collect_values.claim_occurrence_id AND lkp_claim_occurrence_dim_diary.crrnt_snpsht_flag = Exp_Collect_values.o_crrnt_Flag
),
claim_occurrence_dim_UpdateDiary AS (
	MERGE INTO claim_occurrence_dim AS T
	USING UPD_Claim_occurrence_dim_diary AS S
	ON T.claim_occurrence_dim_id = S.claim_occurrence_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.next_diary_date = S.next_diary_date, T.next_diary_date_rep = S.next_diary_date_rep, T.modified_date = S.Modified_date
),
SQ_Claim_Occurrence_Dim AS (
	SELECT 
	CLAIM_OCCURRENCE_DIM.CLAIM_OCCURRENCE_DIM_ID, 
	CLAIM_OCCURRENCE_DIM.EFF_FROM_DATE, 
	CLAIM_OCCURRENCE_DIM.EFF_TO_DATE, 
	CLAIM_OCCURRENCE_DIM.EDW_CLAIM_OCCURRENCE_AK_ID 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_OCCURRENCE_DIM CLAIM_OCCURRENCE_DIM
	WHERE 
	EFF_FROM_DATE <> EFF_TO_DATE AND 
	EXISTS
	(
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_OCCURRENCE_DIM CLAIM_OCCURRENCE_DIM2 
	WHERE CRRNT_SNPSHT_FLAG = 1 AND 
	CLAIM_OCCURRENCE_DIM.EDW_CLAIM_OCCURRENCE_AK_ID = CLAIM_OCCURRENCE_DIM2.EDW_CLAIM_OCCURRENCE_AK_ID
	GROUP BY CLAIM_OCCURRENCE_DIM2.EDW_CLAIM_OCCURRENCE_AK_ID HAVING COUNT(*) > 1
	)
	ORDER BY 
	CLAIM_OCCURRENCE_DIM.EDW_CLAIM_OCCURRENCE_AK_ID, CLAIM_OCCURRENCE_DIM.EFF_FROM_DATE DESC
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	claim_occurrence_dim_id,
	edw_claim_occurrence_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_occurrence_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	edw_claim_occurrence_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_occurrence_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_Claim_Occurrence_Dim
),
FIL_Expired_Row AS (
	SELECT
	claim_occurrence_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Eff_To_Date AS (
	SELECT
	claim_occurrence_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_Expired_Row
),
Claim_Occurrence_Dim_Expire_E AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_dim AS T
	USING UPD_Eff_To_Date AS S
	ON T.claim_occurrence_dim_id = S.claim_occurrence_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
SQ_ClaimStory AS (
	select
	COD.claim_occurrence_dim_id as claim_occurrence_dim_id,
	CS.ClaimOccurrenceKey as ClaimOccurrenceKey,
	CS.Catalyst as Catalyst,
	CS.CauseOfDamage as CauseOfDamage,
	CS.DamageCaused as DamageCaused,
	CS.ItemDamaged as ItemDamaged,
	CS.ModifiedDate as ModifiedDate
	from 
	@{pipeline().parameters.SOURCE_DATABASE}.dbo.ClaimStory CS
	inner join dbo.claim_occurrence_dim COD
	on CS.ClaimOccurrenceKey=COD.claim_occurrence_key 
	-- and COD.crrnt_snpsht_flag=1  -- all records must update not just current
	where   
	(
	COD.Catalyst != CS.Catalyst OR
	COD.CauseOfDamage != CS.CauseOfDamage OR
	COD.DamageCaused != CS.DamageCaused OR
	COD.ItemDamaged != CS.ItemDamaged
	)
	and CS.ModifiedDate > '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_claim_story_output AS (
	SELECT
	claim_occurrence_dim_id,
	ClaimOccurrenceKey,
	Catalyst,
	CauseOfDamage,
	DamageCaused,
	ItemDamaged,
	CURRENT_TIMESTAMP AS SessionStartTime,
	ModifiedDate
	FROM SQ_ClaimStory
),
UPD_UpdateFourWordStory AS (
	SELECT
	claim_occurrence_dim_id, 
	Catalyst, 
	CauseOfDamage, 
	DamageCaused, 
	ItemDamaged, 
	SessionStartTime
	FROM EXP_claim_story_output
),
claim_occurrence_dim_UPDATE_ClaimStory AS (
	MERGE INTO claim_occurrence_dim AS T
	USING UPD_UpdateFourWordStory AS S
	ON T.claim_occurrence_dim_id = S.claim_occurrence_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.modified_date = S.SessionStartTime, T.Catalyst = S.Catalyst, T.CauseOfDamage = S.CauseOfDamage, T.DamageCaused = S.DamageCaused, T.ItemDamaged = S.ItemDamaged
),