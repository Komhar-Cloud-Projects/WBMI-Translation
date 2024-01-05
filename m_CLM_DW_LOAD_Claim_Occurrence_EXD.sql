WITH
SQ_CLAIM_TAB_STAGE AS (
	SELECT 
	CLAIM_TAB_STAGE.CLM_CLAIM_NBR,
	CONVERT(VARCHAR,CLAIM_TAB_STAGE.CLM_LOSS_DT,101) as CLM_CAUSE_LOSS_CD,
	CLAIM_TAB_STAGE.CLM_CSR_CLAIM_NBR, 
	CLAIM_TAB_STAGE.CLM_CREATE_TS, 
	CLAIM_TAB_STAGE.CLM_ENTRY_OPR_ID,
	CLAIM_TAB_STAGE.CLM_OCCURRENCE_ID, 
	CLAIM_TAB_STAGE.CLM_REPORTED_DT, CLAIM_TAB_STAGE.CLM_STATUS_CD,
	CONVERT(VARCHAR,CLAIM_TAB_STAGE.CLM_LOSS_TM,108) AS CLM_UPDATE_OPR_ID, 
	CLAIM_TAB_STAGE.CLM_UPD_TS, CLAIM_TAB_STAGE.CLM_POSTAL_CD, 
	CLAIM_TAB_STAGE.CLM_DISCOVERY_DT, 
	 CLAIM_TAB_STAGE.CLM_LOSS_CITY, 
	CLAIM_TAB_STAGE.CLM_LOSS_COUNTY, CLAIM_TAB_STAGE.CLM_LOSS_DES_ID, 
	CLAIM_TAB_STAGE.CLM_LOSS_STATE_CD, CLAIM_TAB_STAGE.CLM_NOT_CLAIM_IND, 
	CLAIM_TAB_STAGE.CLM_TYPE_CD, CLAIM_TAB_STAGE.CLM_LOSS_PLACE_ID, 
	CLAIM_TAB_STAGE.CLM_REI_NOTIFY_DT, CLAIM_TAB_STAGE.CLM_AT_FAULT_CD, 
	CLAIM_TAB_STAGE.CLM_DRIVER_NBR, CLAIM_TAB_STAGE.CLM_DRV_SAME_IND, 
	CLAIM_TAB_STAGE.CLM_VIOL_CIT_DESC, CLAIM_TAB_STAGE.CLM_HOW_CLM_RPTD, 
	CLAIM_TAB_STAGE.CLM_METHOD_RPTD,
	CLAIM_TAB_STAGE.CLM_WC_CAT_CODE,
	CLAIM_TAB_STAGE.clm_primary_loc_code,
	CLAIM_TAB_STAGE.clm_secondary_dept_code
	FROM
	CLAIM_TAB_STAGE
	/*where exists
	( select 1 from
	(select
	ltrim(rtrim(b.cob_claim_nbr)) as claim_nbr
	,ltrim(rtrim(c.cct_client_id)) as client_id
	,ltrim(rtrim(c.cct_client_role_cd)) as client_role_cd
	,LTRIM(RTRIM(a.ccp_object_seq_nbr)) as object_seq_nbr
	,ltrim(rtrim(a.ccp_object_type_cd)) as object_type_cd
	from WC_Stage_CO.dbo.clm_cov_pkg_stage a
	join WC_Stage_CO.dbo.claim_object_stage b
	on ltrim(rtrim(a.ccp_claim_nbr))=ltrim(rtrim(b.cob_claim_nbr))
	and ltrim(rtrim(a.ccp_object_seq_nbr))=ltrim(rtrim(b.cob_object_seq_nbr))
	and ltrim(rtrim(a.ccp_object_type_cd))=ltrim(rtrim(b.cob_object_type_cd))
	join WC_Stage_CO.dbo.claim_object_clt_stage c
	on ltrim(rtrim(a.ccp_claim_nbr))=ltrim(rtrim(c.cct_claim_nbr))
	join WC_Stage_CO.dbo.claim_claimant_nbr_stage d
	on ltrim(rtrim(d.ccn_claim_nbr))=ltrim(rtrim(b.cob_claim_nbr))
	join wc_stage_co.dbo.claim_transaction_stage e
	on ltrim(rtrim(e.ctx_claim_nbr))=ltrim(rtrim(b.cob_claim_nbr))
	and ltrim(rtrim(e.ctx_client_id))=ltrim(rtrim(c.cct_client_id))
	join WC_Stage_CO.dbo.claim_draft_stage f
	on ltrim(rtrim(e.ctx_draft_nbr))=ltrim(rtrim(f.dft_draft_nbr))
	) t2
	where ltrim(rtrim(CLAIM_TAB_STAGE.clm_claim_nbr))=t2.claim_nbr
	)*/
),
EXP_Values AS (
	SELECT
	CLM_CLAIM_NBR,
	CLM_CSR_CLAIM_NBR,
	CLM_POSTAL_CD,
	CLM_DISCOVERY_DT,
	CLM_CAUSE_LOSS_CD AS CLM_LOSS_DT,
	CLM_LOSS_CITY,
	CLM_LOSS_COUNTY,
	CLM_LOSS_STATE_CD,
	CLM_TYPE_CD,
	CLM_REI_NOTIFY_DT AS in_CLM_REI_NOTIFY_DT,
	-- *INF*: IIF(isnull(in_CLM_REI_NOTIFY_DT), 
	-- to_date('1800-01-01', 'YYYY-MM-DD'),
	-- to_date(in_CLM_REI_NOTIFY_DT, 'YYYY-MM-DD'))
	IFF(in_CLM_REI_NOTIFY_DT IS NULL, to_date('1800-01-01', 'YYYY-MM-DD'), to_date(in_CLM_REI_NOTIFY_DT, 'YYYY-MM-DD')) AS CLM_REI_NOTIFY_DT,
	CLM_METHOD_RPTD,
	CLM_HOW_CLM_RPTD,
	CLM_VIOL_CIT_DESC,
	CLM_LOSS_PLACE_ID,
	CLM_OCCURRENCE_ID,
	-- *INF*: IIF(CLM_OCCURRENCE_ID= '99999999999999999999' OR CLM_OCCURRENCE_ID = '88888888888888888888', 
	-- 'N/A',
	-- LPAD(rtrim(SUBSTR(CLM_OCCURRENCE_ID,19,2)),3,'0'))
	IFF(CLM_OCCURRENCE_ID = '99999999999999999999' OR CLM_OCCURRENCE_ID = '88888888888888888888', 'N/A', LPAD(rtrim(SUBSTR(CLM_OCCURRENCE_ID, 19, 2)), 3, '0')) AS CLM_CATASTROPHE_CODE,
	CLM_STATUS_CD,
	CLM_CREATE_TS,
	CLM_AT_FAULT_CD,
	CLM_DRIVER_NBR,
	CLM_DRV_SAME_IND,
	CLM_LOSS_DES_ID,
	CLM_NOT_CLAIM_IND,
	CLM_UPD_TS,
	CLM_REPORTED_DT,
	CLM_UPDATE_OPR_ID AS CLM_LOSS_TM,
	-- *INF*: IIF(ISNULL(CLM_LOSS_TM) , '00:00:00' ,CLM_LOSS_TM)
	IFF(CLM_LOSS_TM IS NULL, '00:00:00', CLM_LOSS_TM) AS V_CLM_LOSS_TM,
	-- *INF*: (CLM_LOSS_DT  || ' '  ||  V_CLM_LOSS_TM)
	( CLM_LOSS_DT || ' ' || V_CLM_LOSS_TM ) AS v_CLM_LOSS_DATE_TIMESTAMP1,
	-- *INF*: TO_DATE( v_CLM_LOSS_DATE_TIMESTAMP1,'MM/DD/YYYY HH24:MI:SS')
	TO_DATE(v_CLM_LOSS_DATE_TIMESTAMP1, 'MM/DD/YYYY HH24:MI:SS') AS CLM_LOSS_DATE_TIMESTAMP1,
	-- *INF*: --GET_DATE_PART(CLM_LOSS_TM,'HH24')
	'' AS V_loss_date_timestamp,
	-- *INF*: ---GET_DATE_PART(CLM_LOSS_TM,'MI')
	'' AS V_loss_date_timestamp_min,
	-- *INF*: ---SET_DATE_PART(CLM_LOSS_DT,'HH24',V_loss_date_timestamp)
	'' AS V_CLM_LOSS_DATE_HOUR,
	-- *INF*: ---SET_DATE_PART(V_CLM_LOSS_DATE_HOUR,'MI',V_loss_date_timestamp_min)
	'' AS v_CLM_LOSS_DATE_TIMESTAMP,
	-- *INF*: ---v_CLM_LOSS_DATE_TIMESTAMP
	'' AS CLM_LOSS_DATE_TIMESTAMP,
	CLM_ENTRY_OPR_ID,
	clm_wc_cat_code,
	-- *INF*: LTRIM(RTRIM(CLM_LOSS_STATE_CD))
	LTRIM(RTRIM(CLM_LOSS_STATE_CD)) AS o_CLM_LOSS_STATE_CD,
	clm_primary_loc_code,
	clm_secondary_dept_code
	FROM SQ_CLAIM_TAB_STAGE
),
LKP_Adjuster_Tab_Stage AS (
	SELECT
	CAJ_EMP_CLIENT_ID,
	CAJ_USER_ID
	FROM (
		SELECT 
			CAJ_EMP_CLIENT_ID,
			CAJ_USER_ID
		FROM ADJUSTER_TAB_STAGE
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CAJ_USER_ID ORDER BY CAJ_EMP_CLIENT_ID) = 1
),
LKP_ClaimRelationShipStage AS (
	SELECT
	RelationshipId,
	TchClaimNbr
	FROM (
		SELECT 
			RelationshipId,
			TchClaimNbr
		FROM ClaimRelationshipStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TchClaimNbr ORDER BY RelationshipId) = 1
),
LKP_Claim_Coverage_Stage AS (
	SELECT
	in_CLM_CLAIM_NBR,
	cvr_pol_mod_nbr,
	cvr_policy_src_id,
	cvr_policy_id,
	cvr_pol_nbr,
	cvr_claim_nbr
	FROM (
		SELECT 
			in_CLM_CLAIM_NBR,
			cvr_pol_mod_nbr,
			cvr_policy_src_id,
			cvr_policy_id,
			cvr_pol_nbr,
			cvr_claim_nbr
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_coverage_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cvr_claim_nbr ORDER BY in_CLM_CLAIM_NBR) = 1
),
LKP_Clm_Comments_Stage AS (
	SELECT
	TCC_COMMENT_TXT,
	FOLDER_KEY,
	COMMENT_ITEM_NBR
	FROM (
		SELECT comment.TCC_COMMENT_TXT as TCC_COMMENT_TXT, 
		comment.FOLDER_KEY as FOLDER_KEY, 
		comment.COMMENT_ITEM_NBR as COMMENT_ITEM_NBR 
		FROM CLM_COMMENTS_STAGE comment
		INNER JOIN  claim_tab_stage claim_Tab
		ON comment.folder_key = claim_Tab.CLM_CLAIM_NBR
		AND  comment.comment_item_nbr = claim_tab.CLM_LOSS_PLACE_ID
		ORDER BY FOLDER_KEY,COMMENT_ITEM_NBR
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FOLDER_KEY,COMMENT_ITEM_NBR ORDER BY TCC_COMMENT_TXT) = 1
),
LKP_Clm_Comments_Stage_Loss_Description AS (
	SELECT
	TCC_COMMENT_TXT,
	FOLDER_KEY,
	COMMENT_ITEM_NBR
	FROM (
		SELECT comment.TCC_COMMENT_TXT as TCC_COMMENT_TXT, 
		comment.FOLDER_KEY as FOLDER_KEY, 
		comment.COMMENT_ITEM_NBR as COMMENT_ITEM_NBR 
		FROM CLM_COMMENTS_STAGE comment
		INNER JOIN  claim_tab_stage claim_Tab
		ON comment.folder_key = claim_Tab.CLM_CLAIM_NBR
		AND  comment.comment_item_nbr = claim_tab.CLM_LOSS_DES_ID
		ORDER BY FOLDER_KEY,COMMENT_ITEM_NBR
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FOLDER_KEY,COMMENT_ITEM_NBR ORDER BY TCC_COMMENT_TXT) = 1
),
LKP_Clm_Log_Notes_Stage AS (
	SELECT
	create_date,
	claim_nbr
	FROM (
		SELECT 
		MAX(a.create_date) as create_date, 
		a.claim_nbr as claim_nbr 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_log_notes_stage a
		WHERE  a.deleted_date is NULL
		GROUP BY a.claim_nbr
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_nbr ORDER BY create_date) = 1
),
LKP_Clm_Occurrence_Nbr_Stage AS (
	SELECT
	in_clm_claim_nbr,
	con_claim_nbr,
	con_policy_id,
	con_occurrence_nbr
	FROM (
		SELECT 
		SUBSTRING(clm_occurrence_nbr_stage.con_policy_id,1,12) as con_policy_id, 
		clm_occurrence_nbr_stage.con_occurrence_nbr as con_occurrence_nbr, 
		clm_occurrence_nbr_stage.con_claim_nbr as con_claim_nbr 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_occurrence_nbr_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY con_claim_nbr ORDER BY in_clm_claim_nbr) = 1
),
LKP_Sup_Claim_Catastrophe_Code AS (
	SELECT
	cat_start_date,
	cat_end_date,
	cat_code
	FROM (
		SELECT 
		a.cat_start_date as cat_start_date, 
		a.cat_end_date as cat_end_date, 
		rtrim(ltrim(a.cat_code)) as cat_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_catastrophe_code a
		WHERE
		source_system_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cat_code ORDER BY cat_start_date) = 1
),
LKP_Sup_State AS (
	SELECT
	sup_state_id,
	state_code
	FROM (
		SELECT 
			sup_state_id,
			state_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY sup_state_id) = 1
),
mplt_claim_occurrence_next_diary AS (WITH
	INPUT AS (
		
	),
	LKP_Task_NextDueDiary_ByClaim AS (
		SELECT
		DueDate,
		ClaimId
		FROM (
			select MIN(T.DueDate) as DueDate, T.ClaimId as ClaimId 
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T 
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			group by T.ClaimId
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId ORDER BY DueDate DESC) = 1
	),
	LKP_Task_DiaryByClaimAndDueDate AS (
		SELECT
		DueDate,
		AssignedUserName,
		ClaimId
		FROM (
			select T.DueDate as DueDate, T.AssignedUserName as AssignedUserName, T.ClaimId as ClaimId
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId,DueDate ORDER BY DueDate DESC) = 1
	),
	OUTPUT AS (
		SELECT
		DueDate, 
		AssignedUserName
		FROM LKP_Task_DiaryByClaimAndDueDate
	),
),
EXP_Lkp_Values AS (
	SELECT
	EXP_Values.CLM_CLAIM_NBR,
	EXP_Values.CLM_CSR_CLAIM_NBR AS in_CLM_CSR_CLAIM_NBR,
	-- *INF*: IIF(ISNULL(in_CLM_CSR_CLAIM_NBR),'N/A',
	--    IIF(IS_SPACES(in_CLM_CSR_CLAIM_NBR),'N/A',
	--     rtrim(in_CLM_CSR_CLAIM_NBR)))
	IFF(in_CLM_CSR_CLAIM_NBR IS NULL, 'N/A', IFF(IS_SPACES(in_CLM_CSR_CLAIM_NBR), 'N/A', rtrim(in_CLM_CSR_CLAIM_NBR))) AS CLM_CSR_CLAIM_NBR,
	EXP_Values.CLM_POSTAL_CD AS in_CLM_POSTAL_CD,
	-- *INF*: iif(isnull(in_CLM_POSTAL_CD),'N/A',
	--    iif(is_spaces(in_CLM_POSTAL_CD),'N/A',
	--    rtrim( in_CLM_POSTAL_CD)))
	IFF(in_CLM_POSTAL_CD IS NULL, 'N/A', IFF(is_spaces(in_CLM_POSTAL_CD), 'N/A', rtrim(in_CLM_POSTAL_CD))) AS CLM_POSTAL_CD,
	EXP_Values.CLM_DISCOVERY_DT AS in_CLM_DISCOVERY_DT,
	-- *INF*: iif(isnull(in_CLM_DISCOVERY_DT),
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_CLM_DISCOVERY_DT)
	IFF(in_CLM_DISCOVERY_DT IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_CLM_DISCOVERY_DT) AS CLM_DISCOVERY_DT,
	EXP_Values.CLM_LOSS_DATE_TIMESTAMP1 AS in_CLM_LOSS_DT,
	-- *INF*: iif(isnull(in_CLM_LOSS_DT),
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_CLM_LOSS_DT)
	IFF(in_CLM_LOSS_DT IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_CLM_LOSS_DT) AS CLM_LOSS_DT,
	EXP_Values.CLM_LOSS_CITY AS in_CLM_LOSS_CITY,
	-- *INF*: iif(isnull(in_CLM_LOSS_CITY),'N/A',
	--    iif(is_spaces(in_CLM_LOSS_CITY),'N/A',
	--    rtrim(in_CLM_LOSS_CITY)))
	-- 
	IFF(in_CLM_LOSS_CITY IS NULL, 'N/A', IFF(is_spaces(in_CLM_LOSS_CITY), 'N/A', rtrim(in_CLM_LOSS_CITY))) AS CLM_LOSS_CITY,
	EXP_Values.CLM_LOSS_COUNTY AS in_CLM_LOSS_COUNTY,
	-- *INF*: iif(isnull(in_CLM_LOSS_COUNTY),'N/A',
	--    iif(is_spaces(in_CLM_LOSS_COUNTY),'N/A',
	--    rtrim( in_CLM_LOSS_COUNTY)))
	IFF(in_CLM_LOSS_COUNTY IS NULL, 'N/A', IFF(is_spaces(in_CLM_LOSS_COUNTY), 'N/A', rtrim(in_CLM_LOSS_COUNTY))) AS CLM_LOSS_COUNTY,
	EXP_Values.CLM_LOSS_STATE_CD AS in_CLM_LOSS_STATE_CD,
	-- *INF*: iif(isnull(in_CLM_LOSS_STATE_CD),'N/A',
	--    iif(is_spaces(in_CLM_LOSS_STATE_CD),'N/A',
	--     rtrim(in_CLM_LOSS_STATE_CD)))
	IFF(in_CLM_LOSS_STATE_CD IS NULL, 'N/A', IFF(is_spaces(in_CLM_LOSS_STATE_CD), 'N/A', rtrim(in_CLM_LOSS_STATE_CD))) AS CLM_LOSS_STATE_CD,
	EXP_Values.CLM_TYPE_CD AS in_CLM_TYPE_CD,
	-- *INF*: iif(isnull(in_CLM_TYPE_CD),'N/A',
	--    iif(is_spaces(in_CLM_TYPE_CD),'N/A',
	--     rtrim(in_CLM_TYPE_CD)))
	IFF(in_CLM_TYPE_CD IS NULL, 'N/A', IFF(is_spaces(in_CLM_TYPE_CD), 'N/A', rtrim(in_CLM_TYPE_CD))) AS CLM_TYPE_CD,
	EXP_Values.CLM_REI_NOTIFY_DT AS in_CLM_REI_NOTIFY_DT,
	-- *INF*: iif(isnull(in_CLM_REI_NOTIFY_DT),
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_CLM_REI_NOTIFY_DT)
	IFF(in_CLM_REI_NOTIFY_DT IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_CLM_REI_NOTIFY_DT) AS CLM_REI_NOTIFY_DT,
	EXP_Values.CLM_METHOD_RPTD AS in_CLM_METHOD_RPTD,
	-- *INF*: iif(isnull(in_CLM_METHOD_RPTD),'N/A',
	--    iif(is_spaces(in_CLM_METHOD_RPTD),'N/A',
	--     rtrim(in_CLM_METHOD_RPTD)))
	-- 
	-- 
	IFF(in_CLM_METHOD_RPTD IS NULL, 'N/A', IFF(is_spaces(in_CLM_METHOD_RPTD), 'N/A', rtrim(in_CLM_METHOD_RPTD))) AS CLM_METHOD_RPTD,
	EXP_Values.CLM_HOW_CLM_RPTD AS in_CLM_HOW_CLM_RPTD,
	-- *INF*: iif(isnull(in_CLM_HOW_CLM_RPTD),'N/A',
	--    iif(is_spaces(in_CLM_HOW_CLM_RPTD),'N/A',
	--     rtrim(in_CLM_HOW_CLM_RPTD)))
	IFF(in_CLM_HOW_CLM_RPTD IS NULL, 'N/A', IFF(is_spaces(in_CLM_HOW_CLM_RPTD), 'N/A', rtrim(in_CLM_HOW_CLM_RPTD))) AS CLM_HOW_CLM_RPTD,
	EXP_Values.CLM_VIOL_CIT_DESC AS in_CLM_VIOL_CIT_DESC,
	-- *INF*: IIF(ISNULL(in_CLM_VIOL_CIT_DESC),'N/A',
	--    IIF(IS_SPACES(in_CLM_VIOL_CIT_DESC),'N/A',
	--    rtrim( in_CLM_VIOL_CIT_DESC)))
	IFF(in_CLM_VIOL_CIT_DESC IS NULL, 'N/A', IFF(IS_SPACES(in_CLM_VIOL_CIT_DESC), 'N/A', rtrim(in_CLM_VIOL_CIT_DESC))) AS CLM_VIOL_CIT_DESC,
	LKP_Clm_Occurrence_Nbr_Stage.con_occurrence_nbr AS in_CON_OCCURRENCE_NBR,
	-- *INF*: IIF(ISNULL(in_CON_OCCURRENCE_NBR),'N/A',
	--    IIF(IS_SPACES(in_CON_OCCURRENCE_NBR),'N/A',
	--    rtrim( in_CON_OCCURRENCE_NBR)))
	IFF(in_CON_OCCURRENCE_NBR IS NULL, 'N/A', IFF(IS_SPACES(in_CON_OCCURRENCE_NBR), 'N/A', rtrim(in_CON_OCCURRENCE_NBR))) AS CON_OCCURRENCE_NBR,
	EXP_Values.CLM_OCCURRENCE_ID,
	EXP_Values.CLM_CATASTROPHE_CODE AS in_CLM_CATASTROPHE_CODE,
	v_CLM_CATASTROPHE_CODE AS CLM_CATASTROPHE_CODE,
	-- *INF*: iif(isnull(in_CLM_CATASTROPHE_CODE),'N/A',
	--    iif(is_spaces(in_CLM_CATASTROPHE_CODE),'N/A',
	--     rtrim(in_CLM_CATASTROPHE_CODE)))
	IFF(in_CLM_CATASTROPHE_CODE IS NULL, 'N/A', IFF(is_spaces(in_CLM_CATASTROPHE_CODE), 'N/A', rtrim(in_CLM_CATASTROPHE_CODE))) AS v_CLM_CATASTROPHE_CODE,
	LKP_Clm_Comments_Stage.TCC_COMMENT_TXT AS in_TCC_COMMENT_TXT,
	-- *INF*: iif(isnull(in_TCC_COMMENT_TXT),'N/A',
	--    iif(is_spaces(in_TCC_COMMENT_TXT),'N/A',
	--    rtrim(in_TCC_COMMENT_TXT)))
	IFF(in_TCC_COMMENT_TXT IS NULL, 'N/A', IFF(is_spaces(in_TCC_COMMENT_TXT), 'N/A', rtrim(in_TCC_COMMENT_TXT))) AS TCC_COMMENT_TXT,
	LKP_Sup_Claim_Catastrophe_Code.cat_start_date AS in_COC_START_DT,
	-- *INF*: IIF(v_CLM_CATASTROPHE_CODE = 'N/A', TO_DATE('1/1/1800', 'MM/DD/YYYY'), 
	-- IIF(ISNULL(in_COC_START_DT),TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_COC_START_DT))
	IFF(v_CLM_CATASTROPHE_CODE = 'N/A', TO_DATE('1/1/1800', 'MM/DD/YYYY'), IFF(in_COC_START_DT IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_COC_START_DT)) AS COC_START_DT,
	LKP_Sup_Claim_Catastrophe_Code.cat_end_date AS in_COC_END_DT,
	-- *INF*: IIF(v_CLM_CATASTROPHE_CODE = 'N/A', TO_DATE('12/31/2100', 'MM/DD/YYYY'), 
	-- IIF(ISNULL(in_COC_END_DT),TO_DATE('12/31/2100','MM/DD/YYYY'),
	-- in_COC_END_DT))
	IFF(v_CLM_CATASTROPHE_CODE = 'N/A', TO_DATE('12/31/2100', 'MM/DD/YYYY'), IFF(in_COC_END_DT IS NULL, TO_DATE('12/31/2100', 'MM/DD/YYYY'), in_COC_END_DT)) AS COC_END_DT,
	EXP_Values.CLM_STATUS_CD AS in_CLM_STATUS_CD,
	-- *INF*: iif(isnull(in_CLM_STATUS_CD),'N/A',
	--    iif(is_spaces(in_CLM_STATUS_CD),'N/A',
	--      iif(in_CLM_STATUS_CD = 'O' and in_CLM_NOT_CLAIM_IND = 'N','NO',
	-- 	rtrim(in_CLM_STATUS_CD))))
	-- 
	-- 
	-- 
	-- 
	IFF(in_CLM_STATUS_CD IS NULL, 'N/A', IFF(is_spaces(in_CLM_STATUS_CD), 'N/A', IFF(in_CLM_STATUS_CD = 'O' AND in_CLM_NOT_CLAIM_IND = 'N', 'NO', rtrim(in_CLM_STATUS_CD)))) AS CLM_STATUS_CD,
	EXP_Values.CLM_CREATE_TS AS in_CLM_CREATE_TS,
	-- *INF*: iif(isnull(in_CLM_CREATE_TS), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--    in_CLM_CREATE_TS)
	-- 
	IFF(in_CLM_CREATE_TS IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_CLM_CREATE_TS) AS CLM_CREATE_TS,
	EXP_Values.CLM_AT_FAULT_CD AS in_CLM_AT_FAULT_CD,
	-- *INF*: iif(isnull(in_CLM_AT_FAULT_CD),'N/A',
	--    iif(is_spaces(in_CLM_AT_FAULT_CD),'N/A',
	--    rtrim(in_CLM_AT_FAULT_CD)))
	IFF(in_CLM_AT_FAULT_CD IS NULL, 'N/A', IFF(is_spaces(in_CLM_AT_FAULT_CD), 'N/A', rtrim(in_CLM_AT_FAULT_CD))) AS CLM_AT_FAULT_CD,
	EXP_Values.CLM_DRIVER_NBR AS in_CLM_DRIVER_NBR,
	-- *INF*: IIF(
	-- (ISNULL(in_CLM_DRIVER_NBR) OR LENGTH(TO_CHAR(in_CLM_DRIVER_NBR))=0),
	-- -1,
	-- in_CLM_DRIVER_NBR)
	IFF(( in_CLM_DRIVER_NBR IS NULL OR LENGTH(TO_CHAR(in_CLM_DRIVER_NBR)) = 0 ), - 1, in_CLM_DRIVER_NBR) AS CLM_DRIVER_NBR,
	EXP_Values.CLM_DRV_SAME_IND AS in_CLM_DRV_SAME_IND,
	-- *INF*: iif(isnull(in_CLM_DRV_SAME_IND),'N/A',
	--    iif(is_spaces(in_CLM_DRV_SAME_IND),'N/A',
	--    rtrim(in_CLM_DRV_SAME_IND)))
	IFF(in_CLM_DRV_SAME_IND IS NULL, 'N/A', IFF(is_spaces(in_CLM_DRV_SAME_IND), 'N/A', rtrim(in_CLM_DRV_SAME_IND))) AS CLM_DRV_SAME_IND,
	LKP_Clm_Log_Notes_Stage.create_date AS in_LOG_NOTE_LAST_ACTIVITY_DATE,
	-- *INF*: iif(isnull(in_LOG_NOTE_LAST_ACTIVITY_DATE), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--    in_LOG_NOTE_LAST_ACTIVITY_DATE)
	IFF(in_LOG_NOTE_LAST_ACTIVITY_DATE IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_LOG_NOTE_LAST_ACTIVITY_DATE) AS LOG_NOTE_LAST_ACTIVITY_DATE,
	mplt_claim_occurrence_next_diary.DueDate AS in_CLAIM_NEXT_DIARY_DATE,
	-- *INF*: iif(isnull(in_CLAIM_NEXT_DIARY_DATE), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--   in_CLAIM_NEXT_DIARY_DATE)
	IFF(in_CLAIM_NEXT_DIARY_DATE IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_CLAIM_NEXT_DIARY_DATE) AS CLAIM_NEXT_DIARY_DATE,
	-- *INF*: iif(isnull(in_ADJUSTER_NEXT_DIARY_DATE), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--   in_ADJUSTER_NEXT_DIARY_DATE)
	IFF(in_ADJUSTER_NEXT_DIARY_DATE IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_ADJUSTER_NEXT_DIARY_DATE) AS ADJUSTER_NEXT_DIARY_DATE,
	LKP_Clm_Comments_Stage_Loss_Description.TCC_COMMENT_TXT AS in_LOSS_DESCRIPTION,
	-- *INF*: iif(isnull(in_LOSS_DESCRIPTION),'N/A',
	--    iif(is_spaces(in_LOSS_DESCRIPTION),'N/A',
	--    rtrim(in_LOSS_DESCRIPTION)))
	IFF(in_LOSS_DESCRIPTION IS NULL, 'N/A', IFF(is_spaces(in_LOSS_DESCRIPTION), 'N/A', rtrim(in_LOSS_DESCRIPTION))) AS LOSS_DESCRIPTION,
	'N/A' AS OFFSET_ONSET_INDICATOR,
	0 AS LOGICAL_FLAG,
	EXP_Values.CLM_NOT_CLAIM_IND AS in_CLM_NOT_CLAIM_IND,
	-- *INF*: iif((isnull(in_CLM_NOT_CLAIM_IND) OR is_spaces(in_CLM_NOT_CLAIM_IND) OR LENGTH(in_CLM_NOT_CLAIM_IND) = 0),'N/A',
	--    rtrim(in_CLM_NOT_CLAIM_IND))
	IFF(( in_CLM_NOT_CLAIM_IND IS NULL OR is_spaces(in_CLM_NOT_CLAIM_IND) OR LENGTH(in_CLM_NOT_CLAIM_IND) = 0 ), 'N/A', rtrim(in_CLM_NOT_CLAIM_IND)) AS CLM_NOT_CLAIM_IND,
	EXP_Values.CLM_UPD_TS AS in_CLM_UPD_TS,
	-- *INF*: iif(isnull(in_CLM_UPD_TS), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--    in_CLM_UPD_TS)
	IFF(in_CLM_UPD_TS IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_CLM_UPD_TS) AS CLM_UPD_TS,
	EXP_Values.CLM_REPORTED_DT AS in_CLM_REPORTED_DT,
	-- *INF*: iif(isnull(in_CLM_REPORTED_DT), TO_DATE('1/1/1800','MM/DD/YYYY'),
	--    in_CLM_REPORTED_DT)
	IFF(in_CLM_REPORTED_DT IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), in_CLM_REPORTED_DT) AS CLM_REPORTED_DT,
	LKP_Adjuster_Tab_Stage.CAJ_EMP_CLIENT_ID,
	-- *INF*: IIF(ISNULL(CAJ_EMP_CLIENT_ID),'N/A',LTRIM(RTRIM(CAJ_EMP_CLIENT_ID)))
	IFF(CAJ_EMP_CLIENT_ID IS NULL, 'N/A', LTRIM(RTRIM(CAJ_EMP_CLIENT_ID))) AS v_claim_created_by_key,
	v_claim_created_by_key AS out_claim_created_by_key,
	mplt_claim_occurrence_next_diary.AssignedUserName AS claim_rep_full_name,
	-- *INF*: IIF(ISNULL(claim_rep_full_name),'N/A',claim_rep_full_name)
	IFF(claim_rep_full_name IS NULL, 'N/A', claim_rep_full_name) AS claim_rep_full_name_out,
	EXP_Values.clm_wc_cat_code,
	-- *INF*: iif((isnull(clm_wc_cat_code) OR is_spaces(clm_wc_cat_code) OR LENGTH(clm_wc_cat_code) = 0),'N/A',
	--    rtrim(clm_wc_cat_code))
	IFF(( clm_wc_cat_code IS NULL OR is_spaces(clm_wc_cat_code) OR LENGTH(clm_wc_cat_code) = 0 ), 'N/A', rtrim(clm_wc_cat_code)) AS clm_wc_cat_code_out,
	LKP_Sup_State.sup_state_id AS in_sup_state_id,
	-- *INF*: IIF(ISNULL(in_sup_state_id),'-1',in_sup_state_id)
	-- 
	IFF(in_sup_state_id IS NULL, '-1', in_sup_state_id) AS sup_state_id,
	-- *INF*: iif (isnull(in_pol_ak_id) ,
	-- -1, 
	-- in_pol_ak_id)
	-- 
	-- 
	IFF(in_pol_ak_id IS NULL, - 1, in_pol_ak_id) AS pol_ak_id,
	LKP_Clm_Occurrence_Nbr_Stage.con_policy_id AS in_con_policy_id,
	-- *INF*: iif((isnull(in_con_policy_id) OR is_spaces(in_con_policy_id) OR LENGTH(in_con_policy_id) = 0),'N/A',
	--    rtrim(in_con_policy_id))
	IFF(( in_con_policy_id IS NULL OR is_spaces(in_con_policy_id) OR LENGTH(in_con_policy_id) = 0 ), 'N/A', rtrim(in_con_policy_id)) AS v_con_policy_id,
	LKP_Claim_Coverage_Stage.cvr_pol_mod_nbr AS in_cvr_pol_mod_nbr,
	LKP_Claim_Coverage_Stage.cvr_policy_src_id AS in_cvr_policy_src_id,
	LKP_Claim_Coverage_Stage.cvr_policy_id AS in_cvr_policy_id,
	LKP_Claim_Coverage_Stage.cvr_pol_nbr AS in_civr_pol_nbr,
	-- *INF*: LTRIM(RTRIM(in_civr_pol_nbr)) ||LPAD(LTRIM(RTRIM(in_cvr_pol_mod_nbr)),2,'0')
	LTRIM(RTRIM(in_civr_pol_nbr)) || LPAD(LTRIM(RTRIM(in_cvr_pol_mod_nbr)), 2, '0') AS v_cvr_policy_key,
	-- *INF*: IIF(ISNULL(v_cvr_policy_key),'N/A', v_cvr_policy_key)
	IFF(v_cvr_policy_key IS NULL, 'N/A', v_cvr_policy_key) AS v_coverage_policy_key,
	-- *INF*: DECODE(LTRIM(RTRIM(in_cvr_policy_src_id)),
	-- 'PMS',v_con_policy_id,
	-- 'ESU',v_con_policy_id,
	-- 'DUC',v_coverage_policy_key,
	-- 'PDC',v_coverage_policy_key,
	-- 'N/A')
	-- 
	DECODE(LTRIM(RTRIM(in_cvr_policy_src_id)),
	'PMS', v_con_policy_id,
	'ESU', v_con_policy_id,
	'DUC', v_coverage_policy_key,
	'PDC', v_coverage_policy_key,
	'N/A') AS o_POLICY_KEY,
	EXP_Values.clm_primary_loc_code AS in_clm_primary_loc_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_clm_primary_loc_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_clm_primary_loc_code) AS out_clm_primary_loc_code,
	EXP_Values.clm_secondary_dept_code AS in_clm_secondary_dept_code,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_clm_secondary_dept_code)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(in_clm_secondary_dept_code) AS out_clm_secondary_dept_code,
	LKP_ClaimRelationShipStage.RelationshipId AS in_RelationshipId,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_RelationshipId))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(in_RelationshipId)) AS out_RelationshipId
	FROM EXP_Values
	 -- Manually join with mplt_claim_occurrence_next_diary
	LEFT JOIN LKP_Adjuster_Tab_Stage
	ON LKP_Adjuster_Tab_Stage.CAJ_USER_ID = EXP_Values.CLM_ENTRY_OPR_ID
	LEFT JOIN LKP_ClaimRelationShipStage
	ON LKP_ClaimRelationShipStage.TchClaimNbr = EXP_Values.CLM_CLAIM_NBR
	LEFT JOIN LKP_Claim_Coverage_Stage
	ON LKP_Claim_Coverage_Stage.cvr_claim_nbr = EXP_Values.CLM_CLAIM_NBR
	LEFT JOIN LKP_Clm_Comments_Stage
	ON LKP_Clm_Comments_Stage.FOLDER_KEY = EXP_Values.CLM_CLAIM_NBR AND LKP_Clm_Comments_Stage.COMMENT_ITEM_NBR = EXP_Values.CLM_LOSS_PLACE_ID
	LEFT JOIN LKP_Clm_Comments_Stage_Loss_Description
	ON LKP_Clm_Comments_Stage_Loss_Description.FOLDER_KEY = EXP_Values.CLM_CLAIM_NBR AND LKP_Clm_Comments_Stage_Loss_Description.COMMENT_ITEM_NBR = EXP_Values.CLM_LOSS_DES_ID
	LEFT JOIN LKP_Clm_Log_Notes_Stage
	ON LKP_Clm_Log_Notes_Stage.claim_nbr = EXP_Values.CLM_CLAIM_NBR
	LEFT JOIN LKP_Clm_Occurrence_Nbr_Stage
	ON LKP_Clm_Occurrence_Nbr_Stage.con_claim_nbr = EXP_Values.CLM_CLAIM_NBR
	LEFT JOIN LKP_Sup_Claim_Catastrophe_Code
	ON LKP_Sup_Claim_Catastrophe_Code.cat_code = EXP_Values.CLM_CATASTROPHE_CODE
	LEFT JOIN LKP_Sup_State
	ON LKP_Sup_State.state_code = EXP_Values.o_CLM_LOSS_STATE_CD
),
LKP_Claim_Occurrence AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_ak_id,
	pol_key_ak_id,
	claim_occurrence_type_code,
	source_claim_occurrence_status_code,
	notice_claim_ind,
	s3p_claim_created_date,
	source_claim_rpted_date,
	s3p_claim_updated_date,
	rpt_method,
	how_claim_rpted,
	loss_loc_addr,
	loss_loc_city,
	loss_loc_county,
	loss_loc_state,
	loss_loc_zip,
	claim_loss_date,
	claim_discovery_date,
	claim_cat_code,
	claim_cat_start_date,
	claim_cat_end_date,
	s3p_claim_num,
	reins_notified_date,
	claim_occurrence_num,
	claim_voilation_citation_descript,
	claim_loss_descript,
	claim_insd_at_fault_code,
	claim_insd_driver_num,
	claim_insd_driver_ind,
	claim_log_note_last_act_date,
	next_diary_date,
	next_diary_date_rep,
	offset_onset_ind,
	claim_created_by_key,
	wc_cat_code,
	PrimaryWorkGroup,
	SecondaryWorkGroup,
	ClaimRelationshipKey,
	claim_occurrence_key
	FROM (
		SELECT 
		a.claim_occurrence_id as claim_occurrence_id, 
		a.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		a.pol_key_ak_id as pol_key_ak_id, 
		a.claim_occurrence_type_code as claim_occurrence_type_code, 
		a.source_claim_occurrence_status_code as source_claim_occurrence_status_code, 
		a.notice_claim_ind as notice_claim_ind,
		a.s3p_claim_created_date as s3p_claim_created_date, 
		a.source_claim_rpted_date as source_claim_rpted_date,
		a.s3p_claim_updated_date as s3p_claim_updated_date,
		a.rpt_method as rpt_method, 
		a.how_claim_rpted as how_claim_rpted, 
		a.loss_loc_addr as loss_loc_addr, 
		a.loss_loc_city as loss_loc_city, 
		a.loss_loc_county as loss_loc_county, 
		a.loss_loc_state as loss_loc_state, 
		a.loss_loc_zip as loss_loc_zip, 
		a.claim_loss_date as claim_loss_date, 
		a.claim_discovery_date as claim_discovery_date, 
		a.claim_cat_code as claim_cat_code, 
		a.claim_cat_start_date as claim_cat_start_date,
		a.claim_cat_end_date as claim_cat_end_date, 
		a.s3p_claim_num as s3p_claim_num, 
		a.reins_notified_date as reins_notified_date, 
		a.claim_occurrence_num as claim_occurrence_num, 
		a.claim_voilation_citation_descript as claim_voilation_citation_descript, 
		a.claim_loss_descript as claim_loss_descript, 
		a.claim_insd_at_fault_code as claim_insd_at_fault_code, 
		a.claim_insd_driver_num as claim_insd_driver_num,
		a.claim_insd_driver_ind as claim_insd_driver_ind, 
		a.claim_log_note_last_act_date as claim_log_note_last_act_date, 
		a.next_diary_date as next_diary_date, 
		a.next_diary_date_rep as next_diary_date_rep, 
		a.offset_onset_ind as offset_onset_ind, 
		a.claim_created_by_key as claim_created_by_key,
		a.claim_occurrence_key as claim_occurrence_key ,
		a.wc_cat_code as wc_cat_code ,
		a.PrimaryWorkGroup as PrimaryWorkGroup,
		a.SecondaryWorkGroup as SecondaryWorkGroup,
		a.ClaimRelationshipKey as ClaimRelationshipKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence a ,@{pipeline().parameters.SOURCE_DATABASE}.dbo.claim_tab_stage b
		WHERE a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND a.crrnt_snpsht_flag = 1
		AND RTRIM(a.claim_occurrence_key) = b.clm_claim_nbr
		ORDER BY claim_occurrence_key --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_id) = 1
),
LKP_Sup_Claim_Insured_At_Fault_Code AS (
	SELECT
	sup_claim_insd_at_fault_code_id,
	claim_insd_at_fault_code
	FROM (
		SELECT 
			sup_claim_insd_at_fault_code_id,
			claim_insd_at_fault_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_insured_at_fault_code
		WHERE crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_insd_at_fault_code ORDER BY sup_claim_insd_at_fault_code_id) = 1
),
LKP_V2_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
		a.pol_ak_id as pol_ak_id, 
		a.pol_key as pol_key 
		FROM V2.policy a
		WHERE a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.CLM_CLAIM_NBR,
	EXP_Lkp_Values.CLM_CSR_CLAIM_NBR,
	EXP_Lkp_Values.CLM_POSTAL_CD,
	EXP_Lkp_Values.CLM_DISCOVERY_DT,
	EXP_Lkp_Values.CLM_LOSS_DT,
	EXP_Lkp_Values.CLM_LOSS_CITY,
	EXP_Lkp_Values.CLM_LOSS_COUNTY,
	EXP_Lkp_Values.CLM_LOSS_STATE_CD,
	EXP_Lkp_Values.CLM_TYPE_CD,
	EXP_Lkp_Values.CLM_REI_NOTIFY_DT,
	EXP_Lkp_Values.CLM_METHOD_RPTD,
	EXP_Lkp_Values.CLM_HOW_CLM_RPTD,
	EXP_Lkp_Values.CLM_VIOL_CIT_DESC,
	EXP_Lkp_Values.CON_OCCURRENCE_NBR,
	EXP_Lkp_Values.CLM_CATASTROPHE_CODE,
	EXP_Lkp_Values.TCC_COMMENT_TXT,
	EXP_Lkp_Values.COC_START_DT,
	EXP_Lkp_Values.COC_END_DT,
	EXP_Lkp_Values.CLM_STATUS_CD,
	EXP_Lkp_Values.CLM_CREATE_TS,
	EXP_Lkp_Values.CLM_UPD_TS,
	EXP_Lkp_Values.LOSS_DESCRIPTION,
	EXP_Lkp_Values.CLM_AT_FAULT_CD,
	EXP_Lkp_Values.CLM_DRIVER_NBR,
	EXP_Lkp_Values.CLM_DRV_SAME_IND,
	EXP_Lkp_Values.LOG_NOTE_LAST_ACTIVITY_DATE,
	EXP_Lkp_Values.CLAIM_NEXT_DIARY_DATE,
	EXP_Lkp_Values.ADJUSTER_NEXT_DIARY_DATE,
	EXP_Lkp_Values.OFFSET_ONSET_INDICATOR,
	EXP_Lkp_Values.CLM_NOT_CLAIM_IND,
	EXP_Lkp_Values.CLM_REPORTED_DT,
	LKP_V2_Policy.pol_ak_id AS in_POL_AK_ID,
	-- *INF*: IIF(ISNULL(in_POL_AK_ID),-1,in_POL_AK_ID)
	IFF(in_POL_AK_ID IS NULL, - 1, in_POL_AK_ID) AS v_POL_AK_ID,
	v_POL_AK_ID AS POL_AK_ID,
	EXP_Lkp_Values.out_claim_created_by_key AS Out_claim_created_by_key,
	LKP_Claim_Occurrence.claim_occurrence_id,
	LKP_Claim_Occurrence.claim_occurrence_ak_id AS LKP_claim_occurrence_ak_id,
	LKP_Claim_Occurrence.pol_key_ak_id AS pol_key_dim_id,
	LKP_Claim_Occurrence.claim_occurrence_type_code,
	LKP_Claim_Occurrence.source_claim_occurrence_status_code AS s3p_claim_occurrence_status_code,
	LKP_Claim_Occurrence.s3p_claim_created_date,
	LKP_Claim_Occurrence.source_claim_rpted_date,
	LKP_Claim_Occurrence.s3p_claim_updated_date AS s3p_claim_update_date,
	LKP_Claim_Occurrence.rpt_method,
	LKP_Claim_Occurrence.how_claim_rpted,
	LKP_Claim_Occurrence.loss_loc_addr,
	LKP_Claim_Occurrence.loss_loc_city,
	LKP_Claim_Occurrence.loss_loc_county,
	LKP_Claim_Occurrence.loss_loc_state,
	LKP_Claim_Occurrence.loss_loc_zip,
	LKP_Claim_Occurrence.claim_loss_date,
	LKP_Claim_Occurrence.claim_discovery_date,
	LKP_Claim_Occurrence.claim_cat_code,
	LKP_Claim_Occurrence.claim_cat_start_date,
	LKP_Claim_Occurrence.claim_cat_end_date,
	LKP_Claim_Occurrence.s3p_claim_num,
	LKP_Claim_Occurrence.reins_notified_date,
	LKP_Claim_Occurrence.claim_occurrence_num,
	LKP_Claim_Occurrence.claim_voilation_citation_descript,
	LKP_Claim_Occurrence.claim_loss_descript,
	LKP_Claim_Occurrence.claim_insd_at_fault_code,
	LKP_Claim_Occurrence.claim_insd_driver_num AS claim_insd_drvr_num,
	LKP_Claim_Occurrence.claim_insd_driver_ind AS claim_insd_drvr_ind,
	LKP_Claim_Occurrence.claim_log_note_last_act_date,
	LKP_Claim_Occurrence.next_diary_date_rep,
	LKP_Claim_Occurrence.next_diary_date,
	LKP_Claim_Occurrence.offset_onset_ind,
	LKP_Claim_Occurrence.notice_claim_ind AS s3p_not_claim_ind,
	LKP_Claim_Occurrence.claim_created_by_key,
	LKP_Claim_Occurrence.wc_cat_code,
	LKP_Claim_Occurrence.PrimaryWorkGroup,
	LKP_Claim_Occurrence.SecondaryWorkGroup,
	LKP_Claim_Occurrence.ClaimRelationshipKey,
	EXP_Lkp_Values.LOGICAL_FLAG,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: iif(isnull(claim_occurrence_id),'NEW',
	-- 	iif (
	-- 	(ltrim(rtrim(CLM_CSR_CLAIM_NBR)) <> ltrim(rtrim(s3p_claim_num))) or
	-- 	(ltrim(rtrim(CLM_POSTAL_CD)) <> ltrim(rtrim(loss_loc_zip))) or
	-- 	(CLM_DISCOVERY_DT <> claim_discovery_date ) or
	-- 	(CLM_LOSS_DT  <> claim_loss_date ) or
	-- 	(ltrim(rtrim(CLM_LOSS_CITY)) <> ltrim(rtrim(loss_loc_city))) or
	-- 	(ltrim(rtrim(CLM_LOSS_COUNTY)) <> ltrim(rtrim(loss_loc_county) )) or
	-- 	(ltrim(rtrim(CLM_LOSS_STATE_CD)) <>  ltrim(rtrim(loss_loc_state))) or
	-- --
	-- (ltrim(rtrim(clm_wc_cat_code_out)) <> ltrim(rtrim(wc_cat_code))) or
	-- ---
	-- 	(ltrim(rtrim(CLM_TYPE_CD)) <> ltrim(rtrim(claim_occurrence_type_code))) or
	-- 	(CLM_REI_NOTIFY_DT <> reins_notified_date) or
	-- 	(ltrim(rtrim(CLM_METHOD_RPTD)) <> ltrim(rtrim(rpt_method))) or
	-- 	(ltrim(rtrim(CLM_HOW_CLM_RPTD)) <> ltrim(rtrim(how_claim_rpted))) or
	-- 	(ltrim(rtrim(CLM_VIOL_CIT_DESC)) <> ltrim(rtrim(claim_voilation_citation_descript))) or
	-- 	(ltrim(rtrim(CON_OCCURRENCE_NBR)) <> ltrim(rtrim(claim_occurrence_num))) or
	-- 	(ltrim(rtrim(CLM_CATASTROPHE_CODE)) <> ltrim(rtrim(claim_cat_code))) or
	-- 	(ltrim(rtrim(TCC_COMMENT_TXT)) <>  ltrim(rtrim(loss_loc_addr))) or
	-- 	(COC_START_DT <> claim_cat_start_date) or
	-- 	(COC_END_DT <> claim_cat_end_date) or
	-- 	(ltrim(rtrim(CLM_STATUS_CD))  <> ltrim(rtrim(s3p_claim_occurrence_status_code))) or
	-- 	( CLM_CREATE_TS <> s3p_claim_created_date) or
	-- 	----(CLM_UPD_TS <> s3p_claim_update_date) or
	-- 	(ltrim(rtrim( LOSS_DESCRIPTION)) <> ltrim(rtrim(claim_loss_descript))) or
	-- 	(ltrim(rtrim(CLM_AT_FAULT_CD))  <> ltrim(rtrim(claim_insd_at_fault_code))) or
	-- 	( CLM_DRIVER_NBR <> claim_insd_drvr_num) or
	-- 	(ltrim(rtrim( CLM_DRV_SAME_IND)) <> ltrim(rtrim(claim_insd_drvr_ind))) or
	-- 	(LOG_NOTE_LAST_ACTIVITY_DATE  <> claim_log_note_last_act_date) or
	-- 	(claim_rep_full_name_out  <> next_diary_date_rep) or
	-- 	( CLAIM_NEXT_DIARY_DATE <> next_diary_date) or
	-- 	(ltrim(rtrim(OFFSET_ONSET_INDICATOR))  <> ltrim(rtrim(offset_onset_ind))) or
	--       (ltrim(rtrim(CLM_NOT_CLAIM_IND))  <> ltrim(rtrim(s3p_not_claim_ind))) or
	--       CLM_REPORTED_DT <> source_claim_rpted_date or 
	-- 	v_POL_AK_ID <>  pol_key_dim_id or
	-- 	PrimaryWorkGroup <> clm_primary_loc_code or 
	-- 	SecondaryWorkGroup <> clm_secondary_dept_code or
	--       ltrim(rtrim(Out_claim_created_by_key)) <> ltrim(rtrim(claim_created_by_key)) or ltrim(rtrim(ClaimRelationshipKey)) <> ltrim(rtrim(RelationshipId)),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(claim_occurrence_id IS NULL, 'NEW', IFF(( ltrim(rtrim(CLM_CSR_CLAIM_NBR)) <> ltrim(rtrim(s3p_claim_num)) ) OR ( ltrim(rtrim(CLM_POSTAL_CD)) <> ltrim(rtrim(loss_loc_zip)) ) OR ( CLM_DISCOVERY_DT <> claim_discovery_date ) OR ( CLM_LOSS_DT <> claim_loss_date ) OR ( ltrim(rtrim(CLM_LOSS_CITY)) <> ltrim(rtrim(loss_loc_city)) ) OR ( ltrim(rtrim(CLM_LOSS_COUNTY)) <> ltrim(rtrim(loss_loc_county)) ) OR ( ltrim(rtrim(CLM_LOSS_STATE_CD)) <> ltrim(rtrim(loss_loc_state)) ) OR ( ltrim(rtrim(clm_wc_cat_code_out)) <> ltrim(rtrim(wc_cat_code)) ) OR ( ltrim(rtrim(CLM_TYPE_CD)) <> ltrim(rtrim(claim_occurrence_type_code)) ) OR ( CLM_REI_NOTIFY_DT <> reins_notified_date ) OR ( ltrim(rtrim(CLM_METHOD_RPTD)) <> ltrim(rtrim(rpt_method)) ) OR ( ltrim(rtrim(CLM_HOW_CLM_RPTD)) <> ltrim(rtrim(how_claim_rpted)) ) OR ( ltrim(rtrim(CLM_VIOL_CIT_DESC)) <> ltrim(rtrim(claim_voilation_citation_descript)) ) OR ( ltrim(rtrim(CON_OCCURRENCE_NBR)) <> ltrim(rtrim(claim_occurrence_num)) ) OR ( ltrim(rtrim(CLM_CATASTROPHE_CODE)) <> ltrim(rtrim(claim_cat_code)) ) OR ( ltrim(rtrim(TCC_COMMENT_TXT)) <> ltrim(rtrim(loss_loc_addr)) ) OR ( COC_START_DT <> claim_cat_start_date ) OR ( COC_END_DT <> claim_cat_end_date ) OR ( ltrim(rtrim(CLM_STATUS_CD)) <> ltrim(rtrim(s3p_claim_occurrence_status_code)) ) OR ( CLM_CREATE_TS <> s3p_claim_created_date ) OR ( ltrim(rtrim(LOSS_DESCRIPTION)) <> ltrim(rtrim(claim_loss_descript)) ) OR ( ltrim(rtrim(CLM_AT_FAULT_CD)) <> ltrim(rtrim(claim_insd_at_fault_code)) ) OR ( CLM_DRIVER_NBR <> claim_insd_drvr_num ) OR ( ltrim(rtrim(CLM_DRV_SAME_IND)) <> ltrim(rtrim(claim_insd_drvr_ind)) ) OR ( LOG_NOTE_LAST_ACTIVITY_DATE <> claim_log_note_last_act_date ) OR ( claim_rep_full_name_out <> next_diary_date_rep ) OR ( CLAIM_NEXT_DIARY_DATE <> next_diary_date ) OR ( ltrim(rtrim(OFFSET_ONSET_INDICATOR)) <> ltrim(rtrim(offset_onset_ind)) ) OR ( ltrim(rtrim(CLM_NOT_CLAIM_IND)) <> ltrim(rtrim(s3p_not_claim_ind)) ) OR CLM_REPORTED_DT <> source_claim_rpted_date OR v_POL_AK_ID <> pol_key_dim_id OR PrimaryWorkGroup <> clm_primary_loc_code OR SecondaryWorkGroup <> clm_secondary_dept_code OR ltrim(rtrim(Out_claim_created_by_key)) <> ltrim(rtrim(claim_created_by_key)) OR ltrim(rtrim(ClaimRelationshipKey)) <> ltrim(rtrim(RelationshipId)), 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	EXP_Lkp_Values.o_POLICY_KEY AS POLICY_KEY,
	0 AS err_flag,
	EXP_Lkp_Values.claim_rep_full_name_out,
	EXP_Lkp_Values.clm_wc_cat_code_out,
	EXP_Lkp_Values.sup_state_id,
	LKP_Sup_Claim_Insured_At_Fault_Code.sup_claim_insd_at_fault_code_id AS in_sup_claim_insd_at_fault_code_id,
	-- *INF*: IIF(ISNULL(in_sup_claim_insd_at_fault_code_id),-1, in_sup_claim_insd_at_fault_code_id)
	IFF(in_sup_claim_insd_at_fault_code_id IS NULL, - 1, in_sup_claim_insd_at_fault_code_id) AS sup_claim_insd_at_fault_code_id,
	EXP_Lkp_Values.out_clm_primary_loc_code AS clm_primary_loc_code,
	EXP_Lkp_Values.out_clm_secondary_dept_code AS clm_secondary_dept_code,
	EXP_Lkp_Values.out_RelationshipId AS RelationshipId
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_Claim_Occurrence
	ON LKP_Claim_Occurrence.claim_occurrence_key = EXP_Lkp_Values.CLM_CLAIM_NBR
	LEFT JOIN LKP_Sup_Claim_Insured_At_Fault_Code
	ON LKP_Sup_Claim_Insured_At_Fault_Code.claim_insd_at_fault_code = EXP_Lkp_Values.CLM_AT_FAULT_CD
	LEFT JOIN LKP_V2_Policy
	ON LKP_V2_Policy.pol_key = EXP_Lkp_Values.o_POLICY_KEY
),
FIL_Insert AS (
	SELECT
	LKP_claim_occurrence_ak_id, 
	CLM_CLAIM_NBR, 
	CLM_CSR_CLAIM_NBR, 
	CLM_POSTAL_CD, 
	CLM_DISCOVERY_DT, 
	CLM_LOSS_DT, 
	CLM_LOSS_CITY, 
	CLM_LOSS_COUNTY, 
	CLM_LOSS_STATE_CD, 
	CLM_TYPE_CD, 
	CLM_REI_NOTIFY_DT, 
	CLM_METHOD_RPTD, 
	CLM_HOW_CLM_RPTD, 
	CLM_VIOL_CIT_DESC, 
	CON_OCCURRENCE_NBR, 
	CLM_CATASTROPHE_CODE, 
	TCC_COMMENT_TXT, 
	COC_START_DT, 
	COC_END_DT, 
	POL_AK_ID, 
	CLM_STATUS_CD, 
	CLM_CREATE_TS, 
	CLM_UPD_TS, 
	LOSS_DESCRIPTION, 
	CLM_AT_FAULT_CD, 
	CLM_DRIVER_NBR, 
	CLM_DRV_SAME_IND, 
	LOG_NOTE_LAST_ACTIVITY_DATE, 
	CLAIM_NEXT_DIARY_DATE, 
	claim_rep_full_name_out, 
	OFFSET_ONSET_INDICATOR, 
	CLM_NOT_CLAIM_IND, 
	CLM_REPORTED_DT, 
	Out_claim_created_by_key, 
	LOGICAL_FLAG, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	POLICY_KEY, 
	err_flag, 
	clm_wc_cat_code_out AS wc_cat_code, 
	sup_state_id, 
	sup_claim_insd_at_fault_code_id, 
	clm_primary_loc_code, 
	clm_secondary_dept_code, 
	RelationshipId
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_occurrence AS (
	CREATE SEQUENCE SEQ_claim_occurrence
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	LKP_claim_occurrence_ak_id,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, LKP_claim_occurrence_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, LKP_claim_occurrence_ak_id) AS Out_claim_occurrence_ak_id,
	CLM_CLAIM_NBR,
	CLM_CSR_CLAIM_NBR,
	CLM_POSTAL_CD,
	CLM_DISCOVERY_DT,
	CLM_LOSS_DT,
	CLM_LOSS_CITY,
	CLM_LOSS_COUNTY,
	CLM_LOSS_STATE_CD,
	CLM_TYPE_CD,
	CLM_REI_NOTIFY_DT,
	CLM_METHOD_RPTD,
	CLM_HOW_CLM_RPTD,
	CLM_VIOL_CIT_DESC,
	CON_OCCURRENCE_NBR,
	CLM_CATASTROPHE_CODE,
	TCC_COMMENT_TXT,
	COC_START_DT,
	COC_END_DT,
	POL_AK_ID,
	CLM_STATUS_CD,
	CLM_CREATE_TS,
	CLM_UPD_TS,
	LOSS_DESCRIPTION,
	CLM_AT_FAULT_CD,
	CLM_DRIVER_NBR,
	CLM_DRV_SAME_IND,
	LOG_NOTE_LAST_ACTIVITY_DATE,
	CLAIM_NEXT_DIARY_DATE,
	claim_rep_full_name_out,
	OFFSET_ONSET_INDICATOR,
	CLM_NOT_CLAIM_IND,
	CLM_REPORTED_DT,
	Out_claim_created_by_key,
	LOGICAL_FLAG,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	Changed_Flag,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	POLICY_KEY,
	err_flag,
	SEQ_claim_occurrence.NEXTVAL,
	wc_cat_code,
	sup_state_id,
	sup_claim_insd_at_fault_code_id,
	clm_primary_loc_code,
	clm_secondary_dept_code,
	RelationshipId
	FROM FIL_Insert
),
Claim_Occurrence_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence
	(claim_occurrence_ak_id, pol_key_ak_id, pol_key, claim_occurrence_key, claim_occurrence_type_code, source_claim_occurrence_status_code, notice_claim_ind, s3p_claim_created_date, source_claim_rpted_date, s3p_claim_updated_date, rpt_method, how_claim_rpted, loss_loc_addr, loss_loc_city, loss_loc_county, loss_loc_state, loss_loc_zip, claim_loss_date, claim_discovery_date, claim_cat_code, claim_cat_start_date, claim_cat_end_date, s3p_claim_num, reins_notified_date, claim_occurrence_num, claim_voilation_citation_descript, claim_loss_descript, claim_insd_at_fault_code, claim_insd_driver_num, claim_insd_driver_ind, claim_log_note_last_act_date, err_flag_bal_txn, next_diary_date, next_diary_date_rep, offset_onset_ind, err_flag_bal_reins, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_created_by_key, wc_cat_code, SupStateId, SupClaimInsuredAtFaultCodeId, PrimaryWorkGroup, SecondaryWorkGroup, ClaimRelationshipKey)
	SELECT 
	Out_claim_occurrence_ak_id AS CLAIM_OCCURRENCE_AK_ID, 
	POL_AK_ID AS POL_KEY_AK_ID, 
	POLICY_KEY AS POL_KEY, 
	CLM_CLAIM_NBR AS CLAIM_OCCURRENCE_KEY, 
	CLM_TYPE_CD AS CLAIM_OCCURRENCE_TYPE_CODE, 
	CLM_STATUS_CD AS SOURCE_CLAIM_OCCURRENCE_STATUS_CODE, 
	CLM_NOT_CLAIM_IND AS NOTICE_CLAIM_IND, 
	CLM_CREATE_TS AS S3P_CLAIM_CREATED_DATE, 
	CLM_REPORTED_DT AS SOURCE_CLAIM_RPTED_DATE, 
	CLM_UPD_TS AS S3P_CLAIM_UPDATED_DATE, 
	CLM_METHOD_RPTD AS RPT_METHOD, 
	CLM_HOW_CLM_RPTD AS HOW_CLAIM_RPTED, 
	TCC_COMMENT_TXT AS LOSS_LOC_ADDR, 
	CLM_LOSS_CITY AS LOSS_LOC_CITY, 
	CLM_LOSS_COUNTY AS LOSS_LOC_COUNTY, 
	CLM_LOSS_STATE_CD AS LOSS_LOC_STATE, 
	CLM_POSTAL_CD AS LOSS_LOC_ZIP, 
	CLM_LOSS_DT AS CLAIM_LOSS_DATE, 
	CLM_DISCOVERY_DT AS CLAIM_DISCOVERY_DATE, 
	CLM_CATASTROPHE_CODE AS CLAIM_CAT_CODE, 
	COC_START_DT AS CLAIM_CAT_START_DATE, 
	COC_END_DT AS CLAIM_CAT_END_DATE, 
	CLM_CSR_CLAIM_NBR AS S3P_CLAIM_NUM, 
	CLM_REI_NOTIFY_DT AS REINS_NOTIFIED_DATE, 
	CON_OCCURRENCE_NBR AS CLAIM_OCCURRENCE_NUM, 
	CLM_VIOL_CIT_DESC AS CLAIM_VOILATION_CITATION_DESCRIPT, 
	LOSS_DESCRIPTION AS CLAIM_LOSS_DESCRIPT, 
	CLM_AT_FAULT_CD AS CLAIM_INSD_AT_FAULT_CODE, 
	CLM_DRIVER_NBR AS CLAIM_INSD_DRIVER_NUM, 
	CLM_DRV_SAME_IND AS CLAIM_INSD_DRIVER_IND, 
	LOG_NOTE_LAST_ACTIVITY_DATE AS CLAIM_LOG_NOTE_LAST_ACT_DATE, 
	err_flag AS ERR_FLAG_BAL_TXN, 
	CLAIM_NEXT_DIARY_DATE AS NEXT_DIARY_DATE, 
	claim_rep_full_name_out AS NEXT_DIARY_DATE_REP, 
	OFFSET_ONSET_INDICATOR AS OFFSET_ONSET_IND, 
	err_flag AS ERR_FLAG_BAL_REINS, 
	LOGICAL_FLAG AS LOGICAL_FLAG, 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	Out_claim_created_by_key AS CLAIM_CREATED_BY_KEY, 
	WC_CAT_CODE, 
	sup_state_id AS SUPSTATEID, 
	sup_claim_insd_at_fault_code_id AS SUPCLAIMINSUREDATFAULTCODEID, 
	clm_primary_loc_code AS PRIMARYWORKGROUP, 
	clm_secondary_dept_code AS SECONDARYWORKGROUP, 
	RelationshipId AS CLAIMRELATIONSHIPKEY
	FROM EXP_Determine_AK
),
SQ_claim_occurrence_UpdateClaimRelationship AS (
	SELECT 
	co.claim_occurrence_ak_id, 
	co.pol_key_ak_id, 
	co.pol_key, 
	co.claim_occurrence_key, 
	co.claim_occurrence_type_code, 
	co.source_claim_occurrence_status_code, 
	co.notice_claim_ind, 
	co.s3p_claim_created_date, 
	co.source_claim_rpted_date, 
	co.s3p_claim_updated_date, 
	co.rpt_method, 
	co.how_claim_rpted, 
	co.loss_loc_addr, 
	co.loss_loc_city, 
	co.loss_loc_county, 
	co.loss_loc_state, 
	co.loss_loc_zip, 
	co.claim_loss_date, 
	co.claim_discovery_date, 
	co.claim_cat_code, 
	co.claim_cat_start_date, 
	co.claim_cat_end_date, 
	co.s3p_claim_num, 
	co.reins_notified_date, 
	co.claim_occurrence_num, 
	co.claim_voilation_citation_descript, 
	co.claim_loss_descript, 
	co.claim_insd_at_fault_code, 
	co.claim_insd_driver_num, 
	co.claim_insd_driver_ind, 
	co.claim_log_note_last_act_date, 
	co.err_flag_bal_txn, 
	co.next_diary_date, 
	co.next_diary_date_rep,
	co.offset_onset_ind, 
	co.err_flag_bal_reins, 
	co.claim_created_by_key, 
	co.wc_cat_code, 
	co.SupStateId, 
	co.SupClaimInsuredAtFaultCodeId, 
	co.PrimaryWorkGroup, 
	co.SecondaryWorkGroup, 
	ISNULL(convert(varchar,crs.RelationshipId),'N/A') as ClaimRelationshipKey
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence co
	inner hash join @{pipeline().parameters.SOURCE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ClaimRelationshipStage crs
	on ltrim(rtrim(crs.TchClaimNbr))=ltrim(rtrim(co.claim_occurrence_key))
	and co.ClaimRelationshipKey<>convert(varchar,crs.RelationshipId)
	and co.crrnt_snpsht_flag = 1
	and not exists (select 1 
					from @{pipeline().parameters.SOURCE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_TAB_STAGE
					where CLM_CLAIM_NBR = crs.TchClaimNbr)
	union
	SELECT 
	co.claim_occurrence_ak_id, 
	co.pol_key_ak_id, 
	co.pol_key, 
	co.claim_occurrence_key, 
	co.claim_occurrence_type_code, 
	co.source_claim_occurrence_status_code, 
	co.notice_claim_ind, 
	co.s3p_claim_created_date, 
	co.source_claim_rpted_date, 
	co.s3p_claim_updated_date, 
	co.rpt_method, 
	co.how_claim_rpted, 
	co.loss_loc_addr, 
	co.loss_loc_city, 
	co.loss_loc_county, 
	co.loss_loc_state, 
	co.loss_loc_zip, 
	co.claim_loss_date, 
	co.claim_discovery_date, 
	co.claim_cat_code, 
	co.claim_cat_start_date, 
	co.claim_cat_end_date, 
	co.s3p_claim_num, 
	co.reins_notified_date, 
	co.claim_occurrence_num, 
	co.claim_voilation_citation_descript, 
	co.claim_loss_descript, 
	co.claim_insd_at_fault_code, 
	co.claim_insd_driver_num, 
	co.claim_insd_driver_ind, 
	co.claim_log_note_last_act_date, 
	co.err_flag_bal_txn, 
	co.next_diary_date, 
	co.next_diary_date_rep,
	co.offset_onset_ind, 
	co.err_flag_bal_reins, 
	co.claim_created_by_key, 
	co.wc_cat_code, 
	co.SupStateId, 
	co.SupClaimInsuredAtFaultCodeId, 
	co.PrimaryWorkGroup, 
	co.SecondaryWorkGroup, 
	'N/A' as ClaimRelationshipKey
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence co
	where co.ClaimRelationshipKey <> 'N/A' and co.crrnt_snpsht_flag = 1
	and not exists (select 1 
					from  @{pipeline().parameters.SOURCE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ClaimRelationshipStage
					where co.claim_occurrence_key = TchClaimNbr)
),
EXP_ClaimRelationship AS (
	SELECT
	claim_occurrence_ak_id,
	pol_key_ak_id,
	pol_key,
	claim_occurrence_key,
	claim_occurrence_type_code,
	source_claim_occurrence_status_code,
	notice_claim_ind,
	s3p_claim_created_date,
	source_claim_rpted_date,
	s3p_claim_updated_date,
	rpt_method,
	how_claim_rpted,
	loss_loc_addr,
	loss_loc_city,
	loss_loc_county,
	loss_loc_state,
	loss_loc_zip,
	claim_loss_date,
	claim_discovery_date,
	claim_cat_code,
	claim_cat_start_date,
	claim_cat_end_date,
	s3p_claim_num,
	reins_notified_date,
	claim_occurrence_num,
	claim_voilation_citation_descript,
	claim_loss_descript,
	claim_insd_at_fault_code,
	claim_insd_driver_num,
	claim_insd_driver_ind,
	claim_log_note_last_act_date,
	err_flag_bal_txn,
	next_diary_date,
	next_diary_date_rep,
	offset_onset_ind,
	err_flag_bal_reins,
	0 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	claim_created_by_key,
	wc_cat_code,
	SupStateId,
	SupClaimInsuredAtFaultCodeId,
	PrimaryWorkGroup,
	SecondaryWorkGroup,
	ClaimRelationshipKey
	FROM SQ_claim_occurrence_UpdateClaimRelationship
),
claim_occurrence_InsertNewClaimRelationship AS (
	INSERT INTO claim_occurrence
	(claim_occurrence_ak_id, pol_key_ak_id, pol_key, claim_occurrence_key, claim_occurrence_type_code, source_claim_occurrence_status_code, notice_claim_ind, s3p_claim_created_date, source_claim_rpted_date, s3p_claim_updated_date, rpt_method, how_claim_rpted, loss_loc_addr, loss_loc_city, loss_loc_county, loss_loc_state, loss_loc_zip, claim_loss_date, claim_discovery_date, claim_cat_code, claim_cat_start_date, claim_cat_end_date, s3p_claim_num, reins_notified_date, claim_occurrence_num, claim_voilation_citation_descript, claim_loss_descript, claim_insd_at_fault_code, claim_insd_driver_num, claim_insd_driver_ind, claim_log_note_last_act_date, err_flag_bal_txn, next_diary_date, next_diary_date_rep, offset_onset_ind, err_flag_bal_reins, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_created_by_key, wc_cat_code, SupStateId, SupClaimInsuredAtFaultCodeId, PrimaryWorkGroup, SecondaryWorkGroup, ClaimRelationshipKey)
	SELECT 
	CLAIM_OCCURRENCE_AK_ID, 
	POL_KEY_AK_ID, 
	POL_KEY, 
	CLAIM_OCCURRENCE_KEY, 
	CLAIM_OCCURRENCE_TYPE_CODE, 
	SOURCE_CLAIM_OCCURRENCE_STATUS_CODE, 
	NOTICE_CLAIM_IND, 
	S3P_CLAIM_CREATED_DATE, 
	SOURCE_CLAIM_RPTED_DATE, 
	S3P_CLAIM_UPDATED_DATE, 
	RPT_METHOD, 
	HOW_CLAIM_RPTED, 
	LOSS_LOC_ADDR, 
	LOSS_LOC_CITY, 
	LOSS_LOC_COUNTY, 
	LOSS_LOC_STATE, 
	LOSS_LOC_ZIP, 
	CLAIM_LOSS_DATE, 
	CLAIM_DISCOVERY_DATE, 
	CLAIM_CAT_CODE, 
	CLAIM_CAT_START_DATE, 
	CLAIM_CAT_END_DATE, 
	S3P_CLAIM_NUM, 
	REINS_NOTIFIED_DATE, 
	CLAIM_OCCURRENCE_NUM, 
	CLAIM_VOILATION_CITATION_DESCRIPT, 
	CLAIM_LOSS_DESCRIPT, 
	CLAIM_INSD_AT_FAULT_CODE, 
	CLAIM_INSD_DRIVER_NUM, 
	CLAIM_INSD_DRIVER_IND, 
	CLAIM_LOG_NOTE_LAST_ACT_DATE, 
	ERR_FLAG_BAL_TXN, 
	NEXT_DIARY_DATE, 
	NEXT_DIARY_DATE_REP, 
	OFFSET_ONSET_IND, 
	ERR_FLAG_BAL_REINS, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	CLAIM_CREATED_BY_KEY, 
	WC_CAT_CODE, 
	SUPSTATEID, 
	SUPCLAIMINSUREDATFAULTCODEID, 
	PRIMARYWORKGROUP, 
	SECONDARYWORKGROUP, 
	CLAIMRELATIONSHIPKEY
	FROM EXP_ClaimRelationship
),
SQ_Claim_Occurrence AS (
	SELECT 
		a.claim_occurrence_id, 
		a.claim_occurrence_key, 
		a.eff_from_date, 
		a.eff_to_date 
	FROM
	 	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND  
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_occurrence_key = b.claim_occurrence_key
			GROUP BY claim_occurrence_key
			HAVING COUNT(*) > 1)
	ORDER BY claim_occurrence_key, eff_from_date  DESC
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_occurrence_key = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	claim_occurrence_key = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_occurrence_key AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_Claim_Occurrence
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_occurrence_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Claim_Occurrence AS (
	SELECT
	claim_occurrence_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
Claim_Occurrence_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence AS T
	USING UPD_Claim_Occurrence AS S
	ON T.claim_occurrence_id = S.claim_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
SQ_TaskStage AS (
	select distinct ClaimId
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage
	where SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
	and (CreatedDate > '@{pipeline().parameters.SELECTION_START_TS}' or ModifiedDate > '@{pipeline().parameters.SELECTION_START_TS}')
	and not exists (select 1 
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_TAB_STAGE
					where CLM_CLAIM_NBR = ClaimId)
),
EXP_Collect AS (
	SELECT
	ClaimId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS OUT_source_sys_id,
	'T' AS OUT_crrnt_snpsht_flag,
	SYSDATE AS OUT_modified_date
	FROM SQ_TaskStage
),
mplt_claim_occurrence_next_diary1 AS (WITH
	INPUT AS (
		
	),
	LKP_Task_NextDueDiary_ByClaim AS (
		SELECT
		DueDate,
		ClaimId
		FROM (
			select MIN(T.DueDate) as DueDate, T.ClaimId as ClaimId 
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T 
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
			group by T.ClaimId
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId ORDER BY DueDate DESC) = 1
	),
	LKP_Task_DiaryByClaimAndDueDate AS (
		SELECT
		DueDate,
		AssignedUserName,
		ClaimId
		FROM (
			select T.DueDate as DueDate, T.AssignedUserName as AssignedUserName, T.ClaimId as ClaimId
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.TaskStage T
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupTaskStatusStage STS on T.SupTaskStatusId = STS.SupTaskStatusId and STS.Description = 'Open' 
			where T.SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimId,DueDate ORDER BY DueDate DESC) = 1
	),
	OUTPUT AS (
		SELECT
		DueDate, 
		AssignedUserName
		FROM LKP_Task_DiaryByClaimAndDueDate
	),
),
EXP_Default_Values AS (
	SELECT
	DueDate AS IN_DueDate,
	-- *INF*: IIF(ISNULL(IN_DueDate),
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- IN_DueDate
	-- )
	IFF(IN_DueDate IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), IN_DueDate) AS OUT_DueDate,
	AssignedUserName AS IN_AssignedUserName,
	-- *INF*: IIF(ISNULL(IN_AssignedUserName),
	-- 'N/A',
	-- IN_AssignedUserName)
	IFF(IN_AssignedUserName IS NULL, 'N/A', IN_AssignedUserName) AS OUT_AssignedUserName
	FROM mplt_claim_occurrence_next_diary1
),
LKP_claim_occurrence_active_record AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_ak_id,
	pol_key_ak_id,
	pol_key,
	claim_occurrence_key,
	claim_occurrence_type_code,
	source_claim_occurrence_status_code,
	notice_claim_ind,
	s3p_claim_created_date,
	source_claim_rpted_date,
	s3p_claim_updated_date,
	rpt_method,
	how_claim_rpted,
	loss_loc_addr,
	loss_loc_city,
	loss_loc_county,
	loss_loc_state,
	loss_loc_zip,
	claim_loss_date,
	claim_discovery_date,
	claim_cat_code,
	claim_cat_start_date,
	claim_cat_end_date,
	s3p_claim_num,
	reins_notified_date,
	claim_occurrence_num,
	claim_voilation_citation_descript,
	claim_loss_descript,
	claim_insd_at_fault_code,
	claim_insd_driver_num,
	claim_insd_driver_ind,
	claim_log_note_last_act_date,
	err_flag_bal_txn,
	next_diary_date,
	next_diary_date_rep,
	offset_onset_ind,
	err_flag_bal_reins,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	claim_created_by_key,
	wc_cat_code,
	SupStateId,
	SupClaimInsuredAtFaultCodeId,
	PrimaryWorkGroup,
	SecondaryWorkGroup
	FROM (
		SELECT 
			claim_occurrence_id,
			claim_occurrence_ak_id,
			pol_key_ak_id,
			pol_key,
			claim_occurrence_key,
			claim_occurrence_type_code,
			source_claim_occurrence_status_code,
			notice_claim_ind,
			s3p_claim_created_date,
			source_claim_rpted_date,
			s3p_claim_updated_date,
			rpt_method,
			how_claim_rpted,
			loss_loc_addr,
			loss_loc_city,
			loss_loc_county,
			loss_loc_state,
			loss_loc_zip,
			claim_loss_date,
			claim_discovery_date,
			claim_cat_code,
			claim_cat_start_date,
			claim_cat_end_date,
			s3p_claim_num,
			reins_notified_date,
			claim_occurrence_num,
			claim_voilation_citation_descript,
			claim_loss_descript,
			claim_insd_at_fault_code,
			claim_insd_driver_num,
			claim_insd_driver_ind,
			claim_log_note_last_act_date,
			err_flag_bal_txn,
			next_diary_date,
			next_diary_date_rep,
			offset_onset_ind,
			err_flag_bal_reins,
			logical_flag,
			crrnt_snpsht_flag,
			audit_id,
			eff_from_date,
			eff_to_date,
			source_sys_id,
			created_date,
			modified_date,
			claim_created_by_key,
			wc_cat_code,
			SupStateId,
			SupClaimInsuredAtFaultCodeId,
			PrimaryWorkGroup,
			SecondaryWorkGroup
		FROM claim_occurrence
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key,source_sys_id,crrnt_snpsht_flag ORDER BY claim_occurrence_id) = 1
),
UPD_claim_occurrence_diary AS (
	SELECT
	LKP_claim_occurrence_active_record.claim_occurrence_id, 
	EXP_Default_Values.OUT_DueDate AS DueDate, 
	EXP_Default_Values.OUT_AssignedUserName AS AssignedUserName, 
	EXP_Collect.OUT_modified_date AS modified_date
	FROM EXP_Collect
	 -- Manually join with EXP_Default_Values
	LEFT JOIN LKP_claim_occurrence_active_record
	ON LKP_claim_occurrence_active_record.claim_occurrence_key = EXP_Collect.ClaimId AND LKP_claim_occurrence_active_record.source_sys_id = EXP_Collect.OUT_source_sys_id AND LKP_claim_occurrence_active_record.crrnt_snpsht_flag = EXP_Collect.OUT_crrnt_snpsht_flag
),
claim_occurrence_update_diary AS (
	MERGE INTO claim_occurrence AS T
	USING UPD_claim_occurrence_diary AS S
	ON T.claim_occurrence_id = S.claim_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.next_diary_date = S.DueDate, T.next_diary_date_rep = S.AssignedUserName, T.modified_date = S.modified_date
),