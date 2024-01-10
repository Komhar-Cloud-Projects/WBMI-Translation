WITH
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
SQ_pif_42gp_stage AS (
	SELECT 
	ltrim(rtrim(a.pif_symbol))
	, ltrim(rtrim(a.pif_policy_number))
	, ltrim(rtrim(a.pif_module))
	, ltrim(rtrim(a.ipfcgp_year_of_loss))
	, ltrim(rtrim(a.ipfcgp_month_of_loss))
	, ltrim(rtrim(a.ipfcgp_day_of_loss))
	, ltrim(rtrim(a.ipfcgp_loss_occurence))
	, ltrim(rtrim(a.ipfcgp_year_process))
	, ltrim(rtrim(a.ipfcgp_month_process))
	, ltrim(rtrim(a.ipfcgp_day_process))
	, ltrim(rtrim(a.ipfcgp_loss_adjustor_no))
	, ltrim(rtrim(a.ipfcgp_loss_examiner ))
	, ltrim(rtrim(a.logical_flag)) 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_42gp_stage a
	where a.logical_flag in ('0','1')
),
EXP_Claim_Adjuster_Stage AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	ipfcgp_year_of_loss,
	-- *INF*: TO_CHAR(ipfcgp_year_of_loss)
	TO_CHAR(ipfcgp_year_of_loss
	) AS v_ipfcgp_year_of_loss,
	ipfcgp_month_of_loss,
	-- *INF*: TO_CHAR(ipfcgp_month_of_loss)
	TO_CHAR(ipfcgp_month_of_loss
	) AS v_ipfcgp_month_of_loss,
	ipfcgp_day_of_loss,
	-- *INF*: TO_CHAR(ipfcgp_day_of_loss)
	TO_CHAR(ipfcgp_day_of_loss
	) AS v_ipfcgp_day_of_loss,
	ipfcgp_loss_occurence,
	-- *INF*: TO_INTEGER(v_ipfcgp_loss_occurence)
	CAST(v_ipfcgp_loss_occurence AS INTEGER) AS v_ipfcgp_loss_occurence,
	pif_symbol || pif_policy_number || pif_module AS v_sym_num_mod,
	-- *INF*: IIF ( LENGTH(v_ipfcgp_month_of_loss) = 1, '0' || v_ipfcgp_month_of_loss, v_ipfcgp_month_of_loss)
	-- ||  
	-- IIF ( LENGTH(v_ipfcgp_day_of_loss) = 1, '0' || v_ipfcgp_day_of_loss, v_ipfcgp_day_of_loss )
	-- ||  
	-- v_ipfcgp_year_of_loss
	-- 
	IFF(LENGTH(v_ipfcgp_month_of_loss
		) = 1,
		'0' || v_ipfcgp_month_of_loss,
		v_ipfcgp_month_of_loss
	) || IFF(LENGTH(v_ipfcgp_day_of_loss
		) = 1,
		'0' || v_ipfcgp_day_of_loss,
		v_ipfcgp_day_of_loss
	) || v_ipfcgp_year_of_loss AS v_claim_loss_date,
	v_sym_num_mod || v_claim_loss_date || ipfcgp_loss_occurence AS v_claim_occurrence,
	-- *INF*: v_claim_occurrence
	-- 
	-- 
	-- --pif_symbol || pif_policy_number || pif_module || lpad(to_char(ipfcgp_month_of_loss),2,'0') || lpad(to_char(ipfcgp_day_of_loss),2,'0') || to_char(ipfcgp_year_of_loss) || ipfcgp_loss_occurence
	-- 
	-- 
	v_claim_occurrence AS claim_occurrence,
	ipfcgp_loss_adjustor_no,
	ipfcgp_loss_examiner,
	ipfcgp_month_process,
	ipfcgp_day_process,
	ipfcgp_year_process,
	-- *INF*: IIF(ipfcgp_year_process = 0 or isnull(ipfcgp_year_process), to_date('01/01/1800','mm/dd/yyyy'),
	-- to_date(to_char(ipfcgp_month_process) || '/' ||  to_char(ipfcgp_day_process) || '/' ||  to_char(ipfcgp_year_process), 'mm/dd/yyyy'))
	IFF(ipfcgp_year_process = 0 
		OR ipfcgp_year_process IS NULL,
		to_date('01/01/1800', 'mm/dd/yyyy'
		),
		to_date(to_char(ipfcgp_month_process
			) || '/' || to_char(ipfcgp_day_process
			) || '/' || to_char(ipfcgp_year_process
			), 'mm/dd/yyyy'
		)
	) AS date_assigned,
	'H' AS Adjuster_Type,
	'E' AS Examiner_Type,
	logical_flag
	FROM SQ_pif_42gp_stage
),
Union_Split_Record AS (
	SELECT claim_occurrence, ipfcgp_loss_adjustor_no AS Examiner_Or_Adjustor_key, Adjuster_Type AS Examiner_Or_Adjustor, date_assigned, logical_flag
	FROM 
	UNION
	SELECT claim_occurrence, ipfcgp_loss_examiner AS Examiner_Or_Adjustor_key, Examiner_Type AS Examiner_Or_Adjustor, date_assigned, logical_flag
	FROM 
),
Exp_Split_Records AS (
	SELECT
	claim_occurrence,
	Examiner_Or_Adjustor_key,
	-- *INF*: ltrim(rtrim(Examiner_Or_Adjustor_key))
	ltrim(rtrim(Examiner_Or_Adjustor_key
		)
	) AS Out_Examiner_Or_Adjustor_key,
	Examiner_Or_Adjustor,
	date_assigned,
	logical_flag
	FROM Union_Split_Record
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
Exp_Determine_Hierarchy_Incoming_Rep AS (
	SELECT
	Exp_Split_Records.Out_Examiner_Or_Adjustor_key,
	-- *INF*: :LKP.LKP_CLAIM_REP_AK_ID(Out_Examiner_Or_Adjustor_key)
	LKP_CLAIM_REP_AK_ID_Out_Examiner_Or_Adjustor_key.claim_rep_ak_id AS lkp_claim_rep_ak_id,
	Exp_Split_Records.Examiner_Or_Adjustor,
	-- *INF*: IIF(ISNULL(lkp_claim_rep_ak_id)
	-- 	,IIF(Examiner_Or_Adjustor = 'E'
	-- 		,-1
	-- 		,0)
	-- 	,lkp_claim_rep_ak_id)
	IFF(lkp_claim_rep_ak_id IS NULL,
		IFF(Examiner_Or_Adjustor = 'E',
			- 1,
			0
		),
		lkp_claim_rep_ak_id
	) AS new_claim_rep_ak_id,
	-- *INF*: :LKP.LKP_CLAIM_REP_OFFICE_CODE(lkp_claim_rep_ak_id)
	LKP_CLAIM_REP_OFFICE_CODE_lkp_claim_rep_ak_id.handling_office_code AS new_handling_office_code,
	-- *INF*: :LKP.LKP_CLAIM_REP_DVSN_CODE(lkp_claim_rep_ak_id)
	LKP_CLAIM_REP_DVSN_CODE_lkp_claim_rep_ak_id.dvsn_code AS new_dvsn_code,
	-- *INF*: :LKP.LKP_CLAIM_REP_DEPT_CODE(lkp_claim_rep_ak_id)
	LKP_CLAIM_REP_DEPT_CODE_lkp_claim_rep_ak_id.dept_name AS new_dept_name,
	LKP_Claim_Occurrence_EDW.claim_occurrence_ak_id,
	Exp_Split_Records.date_assigned,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	Exp_Split_Records.logical_flag
	FROM Exp_Split_Records
	LEFT JOIN LKP_Claim_Occurrence_EDW
	ON LKP_Claim_Occurrence_EDW.claim_occurrence_key = Exp_Split_Records.claim_occurrence
	LEFT JOIN LKP_CLAIM_REP_AK_ID LKP_CLAIM_REP_AK_ID_Out_Examiner_Or_Adjustor_key
	ON LKP_CLAIM_REP_AK_ID_Out_Examiner_Or_Adjustor_key.claim_rep_key = Out_Examiner_Or_Adjustor_key

	LEFT JOIN LKP_CLAIM_REP_OFFICE_CODE LKP_CLAIM_REP_OFFICE_CODE_lkp_claim_rep_ak_id
	ON LKP_CLAIM_REP_OFFICE_CODE_lkp_claim_rep_ak_id.claim_rep_ak_id = lkp_claim_rep_ak_id

	LEFT JOIN LKP_CLAIM_REP_DVSN_CODE LKP_CLAIM_REP_DVSN_CODE_lkp_claim_rep_ak_id
	ON LKP_CLAIM_REP_DVSN_CODE_lkp_claim_rep_ak_id.claim_rep_ak_id = lkp_claim_rep_ak_id

	LEFT JOIN LKP_CLAIM_REP_DEPT_CODE LKP_CLAIM_REP_DEPT_CODE_lkp_claim_rep_ak_id
	ON LKP_CLAIM_REP_DEPT_CODE_lkp_claim_rep_ak_id.claim_rep_ak_id = lkp_claim_rep_ak_id

),
LKP_Claim_Rep_Occurrence_EDW AS (
	SELECT
	claim_rep_occurrence_ak_id,
	claim_rep_ak_id,
	claim_assigned_date,
	claim_occurrence_ak_id,
	claim_rep_role_code
	FROM (
		SELECT claim_representative_occurrence.claim_rep_occurrence_ak_id as claim_rep_occurrence_ak_id, claim_representative_occurrence.claim_rep_ak_id as claim_rep_ak_id, claim_representative_occurrence.claim_assigned_date as claim_assigned_date, claim_representative_occurrence.claim_occurrence_ak_id as claim_occurrence_ak_id, claim_representative_occurrence.claim_rep_role_code as claim_rep_role_code FROM claim_representative_occurrence
		where claim_representative_occurrence.crrnt_snpsht_flag = 1
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
EXP_Detect_Changes AS (
	SELECT
	LKP_Claim_Rep_Occurrence_EDW.claim_rep_occurrence_ak_id,
	LKP_Claim_Rep_Occurrence_EDW.claim_rep_ak_id AS old_claim_rep_ak_id,
	LKP_Claim_Rep_Occurrence_EDW.claim_assigned_date AS old_claim_assigned_date,
	Exp_Determine_Hierarchy_Incoming_Rep.claim_occurrence_ak_id,
	Exp_Determine_Hierarchy_Incoming_Rep.new_claim_rep_ak_id,
	Exp_Determine_Hierarchy_Incoming_Rep.date_assigned,
	-- *INF*: IIF(ISNULL(claim_rep_occurrence_ak_id)
	-- , 'NEW'
	-- ,  IIF(old_claim_assigned_date != date_assigned or old_claim_rep_ak_id  != new_claim_rep_ak_id
	-- , 'UPDATE'
	-- , 'NOCHANGE'))
	IFF(claim_rep_occurrence_ak_id IS NULL,
		'NEW',
		IFF(old_claim_assigned_date != date_assigned 
			OR old_claim_rep_ak_id != new_claim_rep_ak_id,
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	Exp_Determine_Hierarchy_Incoming_Rep.Examiner_Or_Adjustor,
	Exp_Determine_Hierarchy_Incoming_Rep.logical_flag,
	1 AS Crrnt_SnapSht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	Exp_Determine_Hierarchy_Incoming_Rep.SOURCE_SYSTEM_ID,
	Exp_Determine_Hierarchy_Existing_Rep.old_handling_office_code,
	Exp_Determine_Hierarchy_Existing_Rep.old_dept_name,
	Exp_Determine_Hierarchy_Existing_Rep.old_dvsn_code,
	Exp_Determine_Hierarchy_Incoming_Rep.new_handling_office_code,
	Exp_Determine_Hierarchy_Incoming_Rep.new_dvsn_code,
	Exp_Determine_Hierarchy_Incoming_Rep.new_dept_name,
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
	FROM Exp_Determine_Hierarchy_Existing_Rep
	 -- Manually join with Exp_Determine_Hierarchy_Incoming_Rep
	LEFT JOIN LKP_Claim_Rep_Occurrence_EDW
	ON LKP_Claim_Rep_Occurrence_EDW.claim_occurrence_ak_id = Exp_Determine_Hierarchy_Incoming_Rep.claim_occurrence_ak_id AND LKP_Claim_Rep_Occurrence_EDW.claim_rep_role_code = Exp_Determine_Hierarchy_Incoming_Rep.Examiner_Or_Adjustor
),
FIL_Insert AS (
	SELECT
	claim_rep_occurrence_ak_id, 
	new_claim_rep_ak_id, 
	claim_occurrence_ak_id, 
	date_assigned, 
	Examiner_Or_Adjustor, 
	logical_flag, 
	Crrnt_SnapSht_Flag, 
	AUDIT_ID, 
	SOURCE_SYSTEM_ID, 
	transferred_claim_adjuster_lvl_ind, 
	transferred_claim_handling_office_lvl_ind, 
	transferred_claim_dept_lvl_ind, 
	transferred_claim_dvsn_lvl_ind, 
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
EXP_Determine_AK AS (
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
	date_assigned,
	Examiner_Or_Adjustor,
	logical_flag,
	Crrnt_SnapSht_Flag,
	AUDIT_ID,
	SOURCE_SYSTEM_ID,
	transferred_claim_adjuster_lvl_ind,
	transferred_claim_handling_office_lvl_ind,
	transferred_claim_dept_lvl_ind,
	transferred_claim_dvsn_lvl_ind,
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
	date_assigned AS CLAIM_ASSIGNED_DATE, 
	Examiner_Or_Adjustor AS CLAIM_REP_ROLE_CODE, 
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
	FROM EXP_Determine_AK
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
	--WHERE clause is always made up of eff_to_date='12/31/2100 23:59:59' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Claim_Rep_Occurrence_Expire_Row AS (
	SELECT
	claim_rep_occurrence_id,
	claim_rep_ak_id,
	claim_occurrence_ak_id,
	claim_rep_role_code,
	transferred_claim_adjuster_lvl_ind,
	transferred_claim_handling_office_lvl_ind,
	transferred_claim_dept_lvl_ind,
	transferred_claim_dvsn_lvl_ind,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, 
	-- claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id 
	-- and 
	-- claim_rep_role_code = v_PREV_ROW_claim_rep_role_code
	-- and
	-- source_sys_id = v_PREV_ROW_source_sys_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		claim_occurrence_ak_id = v_PREV_ROW_claim_occurrence_ak_id 
		AND claim_rep_role_code = v_PREV_ROW_claim_rep_role_code 
		AND source_sys_id = v_PREV_ROW_source_sys_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_rep_ak_id AS v_PREV_ROW_claim_rep_ak_id,
	claim_occurrence_ak_id AS v_PREV_ROW_claim_occurrence_ak_id,
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
	orig_eff_to_date, 
	eff_to_date, 
	out_transferred_claim_adjuster_lvl_ind, 
	out_transferred_claim_handling_office_lvl_ind, 
	out_transferred_claim_dept_lvl_ind, 
	out_transferred_claim_dvsn_lvl_ind, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM EXP_Claim_Rep_Occurrence_Expire_Row
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Claim_Rep_Occurrence AS (
	SELECT
	claim_rep_occurrence_id, 
	orig_eff_to_date, 
	eff_to_date, 
	out_transferred_claim_adjuster_lvl_ind, 
	out_transferred_claim_handling_office_lvl_ind, 
	out_transferred_claim_dept_lvl_ind, 
	out_transferred_claim_dvsn_lvl_ind, 
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