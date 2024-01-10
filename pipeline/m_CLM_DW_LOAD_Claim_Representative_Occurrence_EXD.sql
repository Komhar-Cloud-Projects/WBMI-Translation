WITH
LKP_CLAIM_OCCURRENCE AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key
	FROM (
		SELECT 
		   claim_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, 
		   claim_occurrence.claim_occurrence_key as claim_occurrence_key 
		FROM 
		   claim_occurrence
		WHERE
		   source_sys_id = '@{pipeline().parameters.SOURCE_SYS_ID}' AND crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_ak_id) = 1
),
LKP_Claim_Rep_AK_id AS (
	SELECT
	claim_rep_ak_id,
	claim_rep_key
	FROM (
		SELECT 
		a.claim_rep_ak_id as claim_rep_ak_id
		, ltrim(rtrim(a.claim_rep_key)) as claim_rep_key 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
		where a.crrnt_snpsht_flag = 1
		and a.source_sys_id	 = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key ORDER BY claim_rep_ak_id) = 1
),
LKP_Claim_Rep_Office_Code AS (
	SELECT
	handling_office_code,
	claim_rep_ak_id
	FROM (
		SELECT 
		ltrim(rtrim(a.handling_office_code)) as handling_office_code
		, a.claim_rep_ak_id as claim_rep_ak_id
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
		where a.crrnt_snpsht_flag = 1
		and a.source_sys_id	 = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id ORDER BY handling_office_code) = 1
),
LKP_Claim_Rep_Dept_Code AS (
	SELECT
	dept_name,
	claim_rep_ak_id
	FROM (
		SELECT 
		ltrim(rtrim(a.dept_name)) as dept_name
		, a.claim_rep_ak_id as claim_rep_ak_id
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
		where a.crrnt_snpsht_flag = 1
		and a.source_sys_id	 = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id ORDER BY dept_name) = 1
),
LKP_Claim_Rep_Dvsn_Code AS (
	SELECT
	dvsn_code,
	claim_rep_ak_id
	FROM (
		SELECT 
		ltrim(rtrim(a.dvsn_code)) as dvsn_code
		,a.claim_rep_ak_id as claim_rep_ak_id
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
		where a.crrnt_snpsht_flag = 1
		and a.source_sys_id	 = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id ORDER BY dvsn_code) = 1
),
SQ_CLAIM_ADJUSTER_STAGE AS (
	SELECT 
	ltrim(rtrim(CAI_CLAIM_NBR))
	,ltrim(rtrim(CAI_CLM_HDL_ID))
	,ltrim(rtrim(CAI_ADJ_ROLE_CD))
	,CAI_DATE_ASSIGNED
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_ADJUSTER_STAGE
),
EXP_Claim_Adjuster_Stage AS (
	SELECT
	CAI_CLAIM_NBR,
	-- *INF*: IIF(IS_SPACES(CAI_CLAIM_NBR) OR ISNULL(CAI_CLAIM_NBR)
	-- ,'N/A'
	-- ,CAI_CLAIM_NBR)
	IFF(LENGTH(CAI_CLAIM_NBR)>0 AND TRIM(CAI_CLAIM_NBR)='' 
		OR CAI_CLAIM_NBR IS NULL,
		'N/A',
		CAI_CLAIM_NBR
	) AS out_CAI_CLAIM_NBR,
	CAI_CLM_HDL_ID,
	-- *INF*: IIF(IS_SPACES(CAI_CLM_HDL_ID) OR ISNULL(CAI_CLM_HDL_ID)
	-- ,'N/A'
	-- ,CAI_CLM_HDL_ID)
	IFF(LENGTH(CAI_CLM_HDL_ID)>0 AND TRIM(CAI_CLM_HDL_ID)='' 
		OR CAI_CLM_HDL_ID IS NULL,
		'N/A',
		CAI_CLM_HDL_ID
	) AS out_CAI_CLM_HDL_ID,
	CAI_ADJ_ROLE_CD,
	-- *INF*: IIF(IS_SPACES(CAI_ADJ_ROLE_CD) OR ISNULL(CAI_ADJ_ROLE_CD)
	-- ,'N/A'
	-- ,CAI_ADJ_ROLE_CD)
	IFF(LENGTH(CAI_ADJ_ROLE_CD)>0 AND TRIM(CAI_ADJ_ROLE_CD)='' 
		OR CAI_ADJ_ROLE_CD IS NULL,
		'N/A',
		CAI_ADJ_ROLE_CD
	) AS out_CAI_ADJ_ROLE_CD,
	CAI_DATE_ASSIGNED
	FROM SQ_CLAIM_ADJUSTER_STAGE
),
LKP_Claim_Occurrence_EDW AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key
	FROM (
		SELECT
		a.claim_occurrence_ak_id as claim_occurrence_ak_id
		,ltrim(rtrim(a.claim_occurrence_key)) as claim_occurrence_key
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence a
		where  a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_ak_id) = 1
),
LKP_Claim_Rep_Occurrence_EDW AS (
	SELECT
	claim_rep_occurrence_ak_id,
	claim_rep_ak_id,
	claim_assigned_date,
	claim_occurrence_ak_id,
	claim_rep_role_code
	FROM (
		SELECT 
		a.claim_rep_occurrence_ak_id as claim_rep_occurrence_ak_id
		, a.claim_assigned_date as claim_assigned_date
		, a.claim_occurrence_ak_id as claim_occurrence_ak_id
		, a.claim_rep_ak_id as claim_rep_ak_id
		, a.claim_rep_role_code as claim_rep_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence a
		where 
		a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and a.claim_rep_occurrence_id in (select max(b.claim_rep_occurrence_id)
			                                                                   from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence b
		                                                                         where b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and b.crrnt_snpsht_flag = 1
		                                                                         group by b.claim_rep_ak_id, b.claim_occurrence_ak_id,b.claim_rep_role_code)
		order by a.claim_rep_ak_id, a.claim_occurrence_ak_id, a.claim_rep_role_code --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_rep_role_code ORDER BY claim_rep_occurrence_ak_id) = 1
),
Exp_Determine_Hierarchy_Existing_Rep AS (
	SELECT
	claim_rep_ak_id AS old_claim_rep_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_REP_OFFICE_CODE(old_claim_rep_ak_id)
	LKP_CLAIM_REP_OFFICE_CODE_old_claim_rep_ak_id.handling_office_code AS old_handling_office_code,
	-- *INF*: :LKP.LKP_CLAIM_REP_DEPT_CODE(old_claim_rep_ak_id)
	LKP_CLAIM_REP_DEPT_CODE_old_claim_rep_ak_id.dept_name AS old_dept_name,
	-- *INF*: :LKP.LKP_CLAIM_REP_DVSN_CODE(old_claim_rep_ak_id)
	LKP_CLAIM_REP_DVSN_CODE_old_claim_rep_ak_id.dvsn_code AS old_dvsn_code
	FROM LKP_Claim_Rep_Occurrence_EDW
	LEFT JOIN LKP_CLAIM_REP_OFFICE_CODE LKP_CLAIM_REP_OFFICE_CODE_old_claim_rep_ak_id
	ON LKP_CLAIM_REP_OFFICE_CODE_old_claim_rep_ak_id.claim_rep_ak_id = old_claim_rep_ak_id

	LEFT JOIN LKP_CLAIM_REP_DEPT_CODE LKP_CLAIM_REP_DEPT_CODE_old_claim_rep_ak_id
	ON LKP_CLAIM_REP_DEPT_CODE_old_claim_rep_ak_id.claim_rep_ak_id = old_claim_rep_ak_id

	LEFT JOIN LKP_CLAIM_REP_DVSN_CODE LKP_CLAIM_REP_DVSN_CODE_old_claim_rep_ak_id
	ON LKP_CLAIM_REP_DVSN_CODE_old_claim_rep_ak_id.claim_rep_ak_id = old_claim_rep_ak_id

),
Exp_Determine_Hierarchy_Incoming_Rep AS (
	SELECT
	out_CAI_CLM_HDL_ID AS claim_rep_key,
	-- *INF*: :LKP.LKP_CLAIM_REP_AK_ID(claim_rep_key)
	LKP_CLAIM_REP_AK_ID_claim_rep_key.claim_rep_ak_id AS lkp_claim_rep_ak_id,
	lkp_claim_rep_ak_id AS new_claim_rep_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_REP_OFFICE_CODE(lkp_claim_rep_ak_id)
	LKP_CLAIM_REP_OFFICE_CODE_lkp_claim_rep_ak_id.handling_office_code AS new_handling_office_code,
	-- *INF*: :LKP.LKP_CLAIM_REP_DEPT_CODE(lkp_claim_rep_ak_id)
	LKP_CLAIM_REP_DEPT_CODE_lkp_claim_rep_ak_id.dept_name AS new_dept_name,
	-- *INF*: :LKP.LKP_CLAIM_REP_DVSN_CODE(lkp_claim_rep_ak_id)
	LKP_CLAIM_REP_DVSN_CODE_lkp_claim_rep_ak_id.dvsn_code AS new_dvsn_code
	FROM EXP_Claim_Adjuster_Stage
	LEFT JOIN LKP_CLAIM_REP_AK_ID LKP_CLAIM_REP_AK_ID_claim_rep_key
	ON LKP_CLAIM_REP_AK_ID_claim_rep_key.claim_rep_key = claim_rep_key

	LEFT JOIN LKP_CLAIM_REP_OFFICE_CODE LKP_CLAIM_REP_OFFICE_CODE_lkp_claim_rep_ak_id
	ON LKP_CLAIM_REP_OFFICE_CODE_lkp_claim_rep_ak_id.claim_rep_ak_id = lkp_claim_rep_ak_id

	LEFT JOIN LKP_CLAIM_REP_DEPT_CODE LKP_CLAIM_REP_DEPT_CODE_lkp_claim_rep_ak_id
	ON LKP_CLAIM_REP_DEPT_CODE_lkp_claim_rep_ak_id.claim_rep_ak_id = lkp_claim_rep_ak_id

	LEFT JOIN LKP_CLAIM_REP_DVSN_CODE LKP_CLAIM_REP_DVSN_CODE_lkp_claim_rep_ak_id
	ON LKP_CLAIM_REP_DVSN_CODE_lkp_claim_rep_ak_id.claim_rep_ak_id = lkp_claim_rep_ak_id

),
EXP_Detect_Changes AS (
	SELECT
	LKP_Claim_Rep_Occurrence_EDW.claim_rep_occurrence_ak_id,
	LKP_Claim_Rep_Occurrence_EDW.claim_rep_ak_id AS old_claim_rep_ak_id,
	LKP_Claim_Rep_Occurrence_EDW.claim_assigned_date AS old_claim_assigned_date,
	Exp_Determine_Hierarchy_Incoming_Rep.new_claim_rep_ak_id,
	Exp_Determine_Hierarchy_Incoming_Rep.new_handling_office_code,
	Exp_Determine_Hierarchy_Incoming_Rep.new_dept_name,
	Exp_Determine_Hierarchy_Incoming_Rep.new_dvsn_code,
	Exp_Determine_Hierarchy_Existing_Rep.old_handling_office_code,
	Exp_Determine_Hierarchy_Existing_Rep.old_dept_name,
	Exp_Determine_Hierarchy_Existing_Rep.old_dvsn_code,
	LKP_Claim_Occurrence_EDW.claim_occurrence_ak_id,
	-- *INF*: IIF(ISNULL(claim_rep_occurrence_ak_id)
	-- , 'NEW'
	-- ,  IIF(old_claim_assigned_date != CAI_DATE_ASSIGNED or new_claim_rep_ak_id != old_claim_rep_ak_id
	-- , 'UPDATE'
	-- , 'NOCHANGE'))
	IFF(claim_rep_occurrence_ak_id IS NULL,
		'NEW',
		IFF(old_claim_assigned_date != CAI_DATE_ASSIGNED 
			OR new_claim_rep_ak_id != old_claim_rep_ak_id,
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	-- *INF*: iif(isnull(claim_rep_occurrence_ak_id)
	--    ,0
	--    ,iif((new_claim_rep_ak_id != old_claim_rep_ak_id)
	--        ,2
	--        ,0
	--        )
	--    )
	IFF(claim_rep_occurrence_ak_id IS NULL,
		0,
		IFF(( new_claim_rep_ak_id != old_claim_rep_ak_id 
			),
			2,
			0
		)
	) AS transferred_claim_adjuster_lvl_ind,
	-- *INF*: iif(isnull(claim_rep_occurrence_ak_id)
	--    ,0
	--    ,iif((new_claim_rep_ak_id != old_claim_rep_ak_id) and (new_handling_office_code != old_handling_office_code)
	--        ,2
	--        ,0
	--        )
	--    )
	IFF(claim_rep_occurrence_ak_id IS NULL,
		0,
		IFF(( new_claim_rep_ak_id != old_claim_rep_ak_id 
			) 
			AND ( new_handling_office_code != old_handling_office_code 
			),
			2,
			0
		)
	) AS transferred_claim_handling_office_lvl_ind,
	-- *INF*: iif(isnull(claim_rep_occurrence_ak_id)
	--    ,0
	--    ,iif((new_claim_rep_ak_id != old_claim_rep_ak_id) and (new_dept_name != old_dept_name)
	--        ,2
	--        ,0
	--        )
	--    ) 
	IFF(claim_rep_occurrence_ak_id IS NULL,
		0,
		IFF(( new_claim_rep_ak_id != old_claim_rep_ak_id 
			) 
			AND ( new_dept_name != old_dept_name 
			),
			2,
			0
		)
	) AS transferred_claim_dept_lvl_ind,
	-- *INF*: iif(isnull(claim_rep_occurrence_ak_id)
	--    ,0
	--    ,iif((new_claim_rep_ak_id != old_claim_rep_ak_id) and (new_dvsn_code != old_dvsn_code)
	--        ,2
	--        ,0
	--        )
	--    )
	IFF(claim_rep_occurrence_ak_id IS NULL,
		0,
		IFF(( new_claim_rep_ak_id != old_claim_rep_ak_id 
			) 
			AND ( new_dvsn_code != old_dvsn_code 
			),
			2,
			0
		)
	) AS transferred_claim_dvsn_lvl_ind,
	EXP_Claim_Adjuster_Stage.CAI_DATE_ASSIGNED,
	EXP_Claim_Adjuster_Stage.out_CAI_ADJ_ROLE_CD AS CAI_ADJ_ROLE_CD,
	0 AS logical_flag,
	1 AS Crrnt_SnapSht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	v_changed_flag AS changed_flag,
	sysdate AS created_date,
	sysdate AS modified_date
	FROM EXP_Claim_Adjuster_Stage
	 -- Manually join with Exp_Determine_Hierarchy_Existing_Rep
	 -- Manually join with Exp_Determine_Hierarchy_Incoming_Rep
	LEFT JOIN LKP_Claim_Occurrence_EDW
	ON LKP_Claim_Occurrence_EDW.claim_occurrence_key = EXP_Claim_Adjuster_Stage.out_CAI_CLAIM_NBR
	LEFT JOIN LKP_Claim_Rep_Occurrence_EDW
	ON LKP_Claim_Rep_Occurrence_EDW.claim_occurrence_ak_id = LKP_Claim_Occurrence_EDW.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_EDW.claim_rep_role_code = EXP_Claim_Adjuster_Stage.out_CAI_ADJ_ROLE_CD
),
FIL_Insert AS (
	SELECT
	claim_rep_occurrence_ak_id, 
	new_claim_rep_ak_id, 
	claim_occurrence_ak_id, 
	transferred_claim_adjuster_lvl_ind, 
	transferred_claim_handling_office_lvl_ind, 
	transferred_claim_dept_lvl_ind, 
	transferred_claim_dvsn_lvl_ind, 
	CAI_DATE_ASSIGNED, 
	CAI_ADJ_ROLE_CD, 
	logical_flag, 
	Crrnt_SnapSht_Flag, 
	AUDIT_ID, 
	SOURCE_SYSTEM_ID, 
	eff_from_date, 
	eff_to_date, 
	changed_flag, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
SEQ_Claim_Rep_Occurrence_AK AS (
	CREATE SEQUENCE SEQ_Claim_Rep_Occurrence_AK
	START = 0
	INCREMENT = 1;
),
EXP_TRANS_Determine_AK AS (
	SELECT
	claim_rep_occurrence_ak_id,
	SEQ_Claim_Rep_Occurrence_AK.NEXTVAL,
	-- *INF*: iif(isnull(claim_rep_occurrence_ak_id)
	-- ,NEXTVAL
	-- ,claim_rep_occurrence_ak_id)
	IFF(claim_rep_occurrence_ak_id IS NULL,
		NEXTVAL,
		claim_rep_occurrence_ak_id
	) AS out_claim_rep_occurrence_ak_id,
	new_claim_rep_ak_id,
	claim_occurrence_ak_id,
	transferred_claim_adjuster_lvl_ind,
	transferred_claim_handling_office_lvl_ind,
	transferred_claim_dept_lvl_ind,
	transferred_claim_dvsn_lvl_ind,
	CAI_DATE_ASSIGNED,
	CAI_ADJ_ROLE_CD,
	logical_flag,
	Crrnt_SnapSht_Flag,
	AUDIT_ID,
	SOURCE_SYSTEM_ID,
	eff_from_date,
	eff_to_date,
	changed_flag,
	created_date,
	modified_date
	FROM FIL_Insert
),
claim_representative_occurrence_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence
	(claim_rep_occurrence_ak_id, claim_rep_ak_id, claim_occurrence_ak_id, claim_assigned_date, claim_rep_role_code, transferred_claim_adjuster_lvl_ind, transferred_claim_handling_office_lvl_ind, transferred_claim_dept_lvl_ind, transferred_claim_dvsn_lvl_ind, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	out_claim_rep_occurrence_ak_id AS CLAIM_REP_OCCURRENCE_AK_ID, 
	new_claim_rep_ak_id AS CLAIM_REP_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CAI_DATE_ASSIGNED AS CLAIM_ASSIGNED_DATE, 
	CAI_ADJ_ROLE_CD AS CLAIM_REP_ROLE_CODE, 
	TRANSFERRED_CLAIM_ADJUSTER_LVL_IND, 
	TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND, 
	TRANSFERRED_CLAIM_DEPT_LVL_IND, 
	TRANSFERRED_CLAIM_DVSN_LVL_IND, 
	LOGICAL_FLAG, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_TRANS_Determine_AK
),
SQ_clm_case_manage_stage AS (
	SELECT  DISTINCT clm_case_manage_stage.tch_claim_nbr, clm_case_manage_stage.prim_lit_handler 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_case_manage_stage
),
EXP_default AS (
	SELECT
	tch_claim_nbr,
	prim_lit_handler,
	-- *INF*: :LKP.LKP_CLAIM_OCCURRENCE(tch_claim_nbr)
	LKP_CLAIM_OCCURRENCE_tch_claim_nbr.claim_occurrence_ak_id AS v_claim_occurrence_ak_id,
	v_claim_occurrence_ak_id AS claim_occurrence_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_REP_AK_ID(prim_lit_handler)
	LKP_CLAIM_REP_AK_ID_prim_lit_handler.claim_rep_ak_id AS v_claim_rep_ak_id,
	v_claim_rep_ak_id AS claim_rep_ak_id,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS claim_assigned_date,
	'PLH' AS claim_rep_role_code,
	0 AS transferred_claim_adjuster_lvl_ind,
	0 AS transferred_claim_handling_office_lvl_ind,
	0 AS transferred_claim_dept_lvl_ind,
	0 AS transferred_claim_dvsn_lvl_ind,
	0 AS logical_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM SQ_clm_case_manage_stage
	LEFT JOIN LKP_CLAIM_OCCURRENCE LKP_CLAIM_OCCURRENCE_tch_claim_nbr
	ON LKP_CLAIM_OCCURRENCE_tch_claim_nbr.claim_occurrence_key = tch_claim_nbr

	LEFT JOIN LKP_CLAIM_REP_AK_ID LKP_CLAIM_REP_AK_ID_prim_lit_handler
	ON LKP_CLAIM_REP_AK_ID_prim_lit_handler.claim_rep_key = prim_lit_handler

),
FIL_Claim_Rep_ak_id AS (
	SELECT
	claim_occurrence_ak_id, 
	claim_rep_ak_id, 
	claim_assigned_date, 
	claim_rep_role_code, 
	transferred_claim_adjuster_lvl_ind, 
	transferred_claim_handling_office_lvl_ind, 
	transferred_claim_dept_lvl_ind, 
	transferred_claim_dvsn_lvl_ind, 
	logical_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_default
	WHERE IIF(ISNULL(claim_rep_ak_id),FALSE,TRUE)
),
LKP_Claim_Rep_Occ_id AS (
	SELECT
	claim_rep_occurrence_id,
	claim_rep_ak_id,
	claim_occurrence_ak_id
	FROM (
		SELECT claim_representative_occurrence.claim_rep_occurrence_id as claim_rep_occurrence_id, 
		claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, 
		claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence
		WHERE claim_rep_role_code = 'PLH'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_ak_id,claim_occurrence_ak_id ORDER BY claim_rep_occurrence_id DESC) = 1
),
FIL_Claim_Rep_Occurrence_id AS (
	SELECT
	LKP_Claim_Rep_Occ_id.claim_rep_occurrence_id, 
	FIL_Claim_Rep_ak_id.claim_occurrence_ak_id, 
	FIL_Claim_Rep_ak_id.claim_rep_ak_id, 
	FIL_Claim_Rep_ak_id.claim_assigned_date, 
	FIL_Claim_Rep_ak_id.claim_rep_role_code, 
	FIL_Claim_Rep_ak_id.transferred_claim_adjuster_lvl_ind, 
	FIL_Claim_Rep_ak_id.transferred_claim_handling_office_lvl_ind, 
	FIL_Claim_Rep_ak_id.transferred_claim_dept_lvl_ind, 
	FIL_Claim_Rep_ak_id.transferred_claim_dvsn_lvl_ind, 
	FIL_Claim_Rep_ak_id.logical_flag, 
	FIL_Claim_Rep_ak_id.crrnt_snpsht_flag, 
	FIL_Claim_Rep_ak_id.audit_id, 
	FIL_Claim_Rep_ak_id.eff_from_date, 
	FIL_Claim_Rep_ak_id.eff_to_date, 
	FIL_Claim_Rep_ak_id.source_sys_id, 
	FIL_Claim_Rep_ak_id.created_date, 
	FIL_Claim_Rep_ak_id.modified_date
	FROM FIL_Claim_Rep_ak_id
	LEFT JOIN LKP_Claim_Rep_Occ_id
	ON LKP_Claim_Rep_Occ_id.claim_rep_ak_id = FIL_Claim_Rep_ak_id.claim_rep_ak_id AND LKP_Claim_Rep_Occ_id.claim_occurrence_ak_id = FIL_Claim_Rep_ak_id.claim_occurrence_ak_id
	WHERE ISNULL(claim_rep_occurrence_id)
),
claim_representative_occurrence_insert_PLH AS (
	INSERT INTO claim_representative_occurrence
	(claim_rep_occurrence_ak_id, claim_rep_ak_id, claim_occurrence_ak_id, claim_assigned_date, claim_rep_role_code, transferred_claim_adjuster_lvl_ind, transferred_claim_handling_office_lvl_ind, transferred_claim_dept_lvl_ind, transferred_claim_dvsn_lvl_ind, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Claim_Rep_Occurrence_AK.NEXTVAL AS CLAIM_REP_OCCURRENCE_AK_ID, 
	CLAIM_REP_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_ASSIGNED_DATE, 
	CLAIM_REP_ROLE_CODE, 
	TRANSFERRED_CLAIM_ADJUSTER_LVL_IND, 
	TRANSFERRED_CLAIM_HANDLING_OFFICE_LVL_IND, 
	TRANSFERRED_CLAIM_DEPT_LVL_IND, 
	TRANSFERRED_CLAIM_DVSN_LVL_IND, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_Claim_Rep_Occurrence_id
),
SQ_claim_representative_occurrence AS (
	SELECT 
	a.claim_rep_occurrence_id
	, a.claim_rep_ak_id
	, a.claim_occurrence_ak_id
	, a.claim_rep_role_code
	, a.transferred_claim_adjuster_lvl_ind
	, a.transferred_claim_handling_office_lvl_ind
	, a.transferred_claim_dept_lvl_ind
	, a.transferred_claim_dvsn_lvl_ind
	, a.eff_from_date
	, a.eff_to_date
	, a.source_sys_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence b
		WHERE b.crrnt_snpsht_flag = 1
		 and a.claim_rep_occurrence_ak_id = b.claim_rep_occurrence_ak_id
		GROUP BY b.claim_rep_occurrence_ak_id
		HAVING COUNT(*) > 1)
	ORDER BY  a.claim_rep_occurrence_ak_id,a.eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of crrnt_snpsht_flag = 1 and AK ID
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK ID and eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Claim_Rep_Occurrence_Expire_Row AS (
	SELECT
	claim_rep_occurrence_id,
	claim_rep_ak_id,
	claim_occurrence_ak_id,
	claim_rep_role_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	transferred_claim_adjuster_lvl_ind,
	transferred_claim_handling_office_lvl_ind,
	transferred_claim_dept_lvl_ind,
	transferred_claim_dvsn_lvl_ind,
	-- *INF*: DECODE (TRUE, claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_id 
	-- and
	-- claim_rep_role_code = v_PREV_ROW_claim_rep_role_code
	-- and 
	-- source_sys_id = v_PREV_ROW_source_sys_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_id 
		AND claim_rep_role_code = v_PREV_ROW_claim_rep_role_code 
		AND source_sys_id = v_PREV_ROW_source_sys_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_rep_ak_id AS v_PREV_ROW_claim_rep_id,
	claim_occurrence_ak_id AS v_PREV_ROW_claim_occurrence_id,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	claim_rep_role_code AS v_PREV_ROW_claim_rep_role_code,
	-- *INF*: iif(v_PREV_ROW_transferred_claim_adjuster_lvl_ind='2' and transferred_claim_adjuster_lvl_ind='0'
	--    ,'1'
	--    ,iif(v_PREV_ROW_transferred_claim_adjuster_lvl_ind='2' and transferred_claim_adjuster_lvl_ind='2'
	--        ,'3'
	--        ,transferred_claim_adjuster_lvl_ind)
	--    )
	IFF(v_PREV_ROW_transferred_claim_adjuster_lvl_ind = '2' 
		AND transferred_claim_adjuster_lvl_ind = '0',
		'1',
		IFF(v_PREV_ROW_transferred_claim_adjuster_lvl_ind = '2' 
			AND transferred_claim_adjuster_lvl_ind = '2',
			'3',
			transferred_claim_adjuster_lvl_ind
		)
	) AS v_transferred_claim_adjuster_lvl_ind,
	v_transferred_claim_adjuster_lvl_ind AS out_transferred_claim_adjuster_lvl_ind,
	-- *INF*: iif(v_PREV_ROW_transferred_claim_handling_office_lvl_ind='2' and transferred_claim_handling_office_lvl_ind='0'
	--    ,'1'
	--    ,iif(v_PREV_ROW_transferred_claim_handling_office_lvl_ind='2' and transferred_claim_handling_office_lvl_ind='2'
	--        ,'3'
	--        ,transferred_claim_handling_office_lvl_ind)
	--    )
	IFF(v_PREV_ROW_transferred_claim_handling_office_lvl_ind = '2' 
		AND transferred_claim_handling_office_lvl_ind = '0',
		'1',
		IFF(v_PREV_ROW_transferred_claim_handling_office_lvl_ind = '2' 
			AND transferred_claim_handling_office_lvl_ind = '2',
			'3',
			transferred_claim_handling_office_lvl_ind
		)
	) AS v_transferred_claim_handling_office_lvl_ind,
	v_transferred_claim_handling_office_lvl_ind AS out_transferred_claim_handling_office_lvl_ind,
	-- *INF*: iif(v_PREV_ROW_transferred_claim_dept_lvl_ind='2' and transferred_claim_dept_lvl_ind='0'
	--    ,'1'
	--    ,iif(v_PREV_ROW_transferred_claim_dept_lvl_ind='2' and transferred_claim_dept_lvl_ind='2'
	--        ,'3'
	--        ,transferred_claim_dept_lvl_ind)
	--    )
	IFF(v_PREV_ROW_transferred_claim_dept_lvl_ind = '2' 
		AND transferred_claim_dept_lvl_ind = '0',
		'1',
		IFF(v_PREV_ROW_transferred_claim_dept_lvl_ind = '2' 
			AND transferred_claim_dept_lvl_ind = '2',
			'3',
			transferred_claim_dept_lvl_ind
		)
	) AS v_transferred_claim_dept_lvl_ind,
	v_transferred_claim_dept_lvl_ind AS out_transferred_claim_dept_lvl_ind,
	-- *INF*: iif(v_PREV_ROW_transferred_claim_dvsn_lvl_ind='2' and transferred_claim_dvsn_lvl_ind='0'
	--    ,'1'
	--    ,iif(v_PREV_ROW_transferred_claim_dvsn_lvl_ind='2' and transferred_claim_dvsn_lvl_ind='2'
	--        ,'3'
	--        ,transferred_claim_dvsn_lvl_ind)
	--    )
	IFF(v_PREV_ROW_transferred_claim_dvsn_lvl_ind = '2' 
		AND transferred_claim_dvsn_lvl_ind = '0',
		'1',
		IFF(v_PREV_ROW_transferred_claim_dvsn_lvl_ind = '2' 
			AND transferred_claim_dvsn_lvl_ind = '2',
			'3',
			transferred_claim_dvsn_lvl_ind
		)
	) AS v_transferred_claim_dvsn_lvl_ind,
	v_transferred_claim_dvsn_lvl_ind AS out_transferred_claim_dvsn_lvl_ind,
	transferred_claim_adjuster_lvl_ind AS v_PREV_ROW_transferred_claim_adjuster_lvl_ind,
	transferred_claim_handling_office_lvl_ind AS v_PREV_ROW_transferred_claim_handling_office_lvl_ind,
	transferred_claim_dept_lvl_ind AS v_PREV_ROW_transferred_claim_dept_lvl_ind,
	transferred_claim_dvsn_lvl_ind AS v_PREV_ROW_transferred_claim_dvsn_lvl_ind,
	0 AS crrnt_Snpsht_flag,
	sysdate AS modified_date
	FROM SQ_claim_representative_occurrence
),
FIL_Claim_Rep_Occurrence_Upd AS (
	SELECT
	claim_rep_occurrence_id, 
	out_transferred_claim_adjuster_lvl_ind, 
	out_transferred_claim_handling_office_lvl_ind, 
	out_transferred_claim_dept_lvl_ind, 
	out_transferred_claim_dvsn_lvl_ind, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM EXP_Claim_Rep_Occurrence_Expire_Row
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Claim_Rep_Occurrence AS (
	SELECT
	claim_rep_occurrence_id, 
	out_transferred_claim_adjuster_lvl_ind, 
	out_transferred_claim_handling_office_lvl_ind, 
	out_transferred_claim_dept_lvl_ind, 
	out_transferred_claim_dvsn_lvl_ind, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM FIL_Claim_Rep_Occurrence_Upd
),
claim_representative_occurrence_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence AS T
	USING UPD_Claim_Rep_Occurrence AS S
	ON T.claim_rep_occurrence_id = S.claim_rep_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.transferred_claim_adjuster_lvl_ind = S.out_transferred_claim_adjuster_lvl_ind, T.transferred_claim_handling_office_lvl_ind = S.out_transferred_claim_handling_office_lvl_ind, T.transferred_claim_dept_lvl_ind = S.out_transferred_claim_dept_lvl_ind, T.transferred_claim_dvsn_lvl_ind = S.out_transferred_claim_dvsn_lvl_ind, T.crrnt_snpsht_flag = S.crrnt_Snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),