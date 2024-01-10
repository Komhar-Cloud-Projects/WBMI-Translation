WITH
LKP_RSM_Territory_Name_Rule AS (
	SELECT
	rsm_terr_name_rule_id,
	rsm_terr_sym
	FROM (
		SELECT rsm_territory_name_rule.rsm_terr_name_rule_id as rsm_terr_name_rule_id,
		rsm_territory_name_rule.rsm_terr_sym as rsm_terr_sym FROM rsm_territory_name_rule
		where crrnt_rule_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsm_terr_sym ORDER BY rsm_terr_name_rule_id) = 1
),
SQ_agency_stage AS (
	SELECT
		AGENCY_STAGE_ID,
		STATE_CODE,
		AGENCY_NUM,
		APPOINTED_DATE,
		AGENCY_STATUS,
		AGENCY_FULL_NAME,
		AGENCY_DBA_NAME,
		AGENCY_ABBREV_NAME,
		AGENCY_SORT_NAME,
		TERRITORY_CODE,
		TELEPHONE,
		FAX,
		EMAIL_ADDRESS,
		EMAIL_RETRIEVAL,
		WEB_ADDRESS,
		AUTHORITY_LEVEL,
		COMPARATIVE_RATER,
		LINE_APPOINTED,
		TAX_ID,
		TAX_ID_TYPE,
		TAX_LOCATION,
		WEBSITE_TYPE,
		SUBAGENT,
		EMAIL_HTML_FLAG,
		SR22_AUTHORITY,
		KEY_AGENT_FLG,
		TAX_REPORTABLE_FLG,
		ELECTRONIC_RPT_FLG,
		ADVISORY_BOARD,
		WBC_STEER_COMM,
		CONTGNT_GUARANTEED,
		DIRCONN_PER_DATE,
		REINSTATEMENT_DATE,
		COMMENT_ID,
		DIRCONN_COMM_DATE,
		AGENCY_CODE,
		MGMT_SYSTEM_ID,
		CHOICEPOINT_ACCT,
		INTERNET_CONN_ID,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM agency_stage
),
EXP_Values AS (
	SELECT
	AGENCY_STAGE_ID AS AGENCY_stage_ID,
	STATE_CODE,
	AGENCY_NUM,
	APPOINTED_DATE,
	AGENCY_STATUS,
	AGENCY_FULL_NAME,
	AGENCY_DBA_NAME,
	AGENCY_ABBREV_NAME,
	AGENCY_SORT_NAME,
	TERRITORY_CODE,
	TELEPHONE,
	FAX,
	EMAIL_ADDRESS,
	EMAIL_RETRIEVAL,
	WEB_ADDRESS,
	AUTHORITY_LEVEL,
	COMPARATIVE_RATER,
	LINE_APPOINTED,
	TAX_ID,
	TAX_ID_TYPE,
	TAX_LOCATION,
	WEBSITE_TYPE,
	SUBAGENT,
	EMAIL_HTML_FLAG,
	SR22_AUTHORITY,
	KEY_AGENT_FLG,
	TAX_REPORTABLE_FLG,
	ELECTRONIC_RPT_FLG,
	ADVISORY_BOARD,
	WBC_STEER_COMM,
	CONTGNT_GUARANTEED,
	DIRCONN_PER_DATE,
	REINSTATEMENT_DATE,
	COMMENT_ID,
	DIRCONN_COMM_DATE,
	AGENCY_CODE,
	MGMT_SYSTEM_ID,
	CHOICEPOINT_ACCT,
	INTERNET_CONN_ID,
	SOURCE_SYSTEM_ID
	FROM SQ_agency_stage
),
LKP_Territory_Stage AS (
	SELECT
	RSM_ID,
	TERRITORY_CODE,
	STATE_CODE,
	TERRITORY_SYMBOL,
	TERRITORY_NAME
	FROM (
		SELECT territory_stage.RSM_ID as RSM_ID, territory_stage.TERRITORY_SYMBOL as TERRITORY_SYMBOL, territory_stage.TERRITORY_NAME as TERRITORY_NAME, territory_stage.TERRITORY_CODE as TERRITORY_CODE, territory_stage.STATE_CODE as STATE_CODE 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.territory_stage territory_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TERRITORY_CODE,STATE_CODE ORDER BY RSM_ID) = 1
),
LKP_RSM_Stage AS (
	SELECT
	RSM_FIRST_NAME,
	RSM_LAST_NAME,
	RSM_ID
	FROM (
		SELECT R.RSM_FIRST_NAME as RSM_FIRST_NAME, R.RSM_LAST_NAME as RSM_LAST_NAME, R.RSM_ID as RSM_ID FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.RSM_stage R
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RSM_ID ORDER BY RSM_FIRST_NAME) = 1
),
LKP_State_Sup AS (
	SELECT
	state_abbrev,
	state_descript,
	state_code
	FROM (
		SELECT dbo.State_Sup.state_abbrev as state_abbrev, dbo.State_Sup.state_descript as state_descript, dbo.State_Sup.state_code as state_code FROM dbo.State_Sup
		where crrnt_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
),
EXP_Lkp_Values AS (
	SELECT
	EXP_Values.STATE_CODE,
	LKP_Territory_Stage.TERRITORY_SYMBOL AS in_rsm_terr_sym,
	-- *INF*: iif(isnull(in_rsm_terr_sym),'N/A',
	-- iif(is_spaces(in_rsm_terr_sym),'N/A',in_rsm_terr_sym))
	IFF(in_rsm_terr_sym IS NULL,
		'N/A',
		IFF(LENGTH(in_rsm_terr_sym)>0 AND TRIM(in_rsm_terr_sym)='',
			'N/A',
			in_rsm_terr_sym
		)
	) AS out_rsm_TERR_SYM,
	LKP_Territory_Stage.TERRITORY_NAME AS in_rsm_TERR_NAME,
	-- *INF*: iif(isnull(in_rsm_TERR_NAME),'Not Found',iif(is_spaces(in_rsm_TERR_NAME),'Not Found',ltrim(rtrim(in_rsm_TERR_NAME))))
	IFF(in_rsm_TERR_NAME IS NULL,
		'Not Found',
		IFF(LENGTH(in_rsm_TERR_NAME)>0 AND TRIM(in_rsm_TERR_NAME)='',
			'Not Found',
			ltrim(rtrim(in_rsm_TERR_NAME
				)
			)
		)
	) AS out_rsm_TERR_NAME,
	LKP_Territory_Stage.TERRITORY_CODE AS in_rsm_TERR_CODE,
	-- *INF*: iif(isnull(TO_CHAR(in_rsm_TERR_CODE)),'N/A',TO_CHAR(in_rsm_TERR_CODE))
	IFF(TO_CHAR(in_rsm_TERR_CODE
		) IS NULL,
		'N/A',
		TO_CHAR(in_rsm_TERR_CODE
		)
	) AS out_rsm_TERR_CODE,
	-- *INF*: iif(isnull(STATE_CODE),'N/A',iif(is_spaces(STATE_CODE),'N/A',STATE_CODE))
	IFF(STATE_CODE IS NULL,
		'N/A',
		IFF(LENGTH(STATE_CODE)>0 AND TRIM(STATE_CODE)='',
			'N/A',
			STATE_CODE
		)
	) AS v_STATE_CODE,
	v_STATE_CODE AS agency_state_code,
	LKP_State_Sup.state_abbrev AS in_agency_state_abbrev,
	-- *INF*: iif(isnull(in_agency_state_abbrev),'N/A',iif(IS_SPACES(in_agency_state_abbrev),'N/A',in_agency_state_abbrev))
	-- 
	-- 
	IFF(in_agency_state_abbrev IS NULL,
		'N/A',
		IFF(LENGTH(in_agency_state_abbrev)>0 AND TRIM(in_agency_state_abbrev)='',
			'N/A',
			in_agency_state_abbrev
		)
	) AS agency_state_abbrev,
	LKP_State_Sup.state_descript AS in_agency_state_descript,
	-- *INF*: iif(isnull(in_agency_state_descript),'Not Available',
	-- iif(is_spaces(in_agency_state_descript),'Not Available',in_agency_state_descript))
	IFF(in_agency_state_descript IS NULL,
		'Not Available',
		IFF(LENGTH(in_agency_state_descript)>0 AND TRIM(in_agency_state_descript)='',
			'Not Available',
			in_agency_state_descript
		)
	) AS out_agency_state_descript,
	EXP_Values.AGENCY_NUM AS in_AGENCY_NUM,
	-- *INF*: iif(isnull(in_AGENCY_NUM),'N/A',
	-- iif(is_spaces(in_AGENCY_NUM),'N/A',in_AGENCY_NUM))
	IFF(in_AGENCY_NUM IS NULL,
		'N/A',
		IFF(LENGTH(in_AGENCY_NUM)>0 AND TRIM(in_AGENCY_NUM)='',
			'N/A',
			in_AGENCY_NUM
		)
	) AS out_AGENCY_NUM,
	EXP_Values.AGENCY_ABBREV_NAME AS in_AGENCY_NAME,
	-- *INF*: iif(isnull(in_AGENCY_NAME),'Not Available',
	-- iif(is_spaces(in_AGENCY_NAME),'Not Available',ltrim(rtrim(in_AGENCY_NAME))))
	IFF(in_AGENCY_NAME IS NULL,
		'Not Available',
		IFF(LENGTH(in_AGENCY_NAME)>0 AND TRIM(in_AGENCY_NAME)='',
			'Not Available',
			ltrim(rtrim(in_AGENCY_NAME
				)
			)
		)
	) AS out_AGENCY_NAME,
	-- *INF*: :LKP.LKP_RSM_TERRITORY_NAME_RULE(in_rsm_terr_sym)
	LKP_RSM_TERRITORY_NAME_RULE_in_rsm_terr_sym.rsm_terr_name_rule_id AS v_Audit_Terr_Name_Rule_id,
	-- *INF*: iif(isnull(v_Audit_Terr_Name_Rule_id),0,v_Audit_Terr_Name_Rule_id)
	IFF(v_Audit_Terr_Name_Rule_id IS NULL,
		0,
		v_Audit_Terr_Name_Rule_id
	) AS out_v_Audit_Terr_Name_Rule_id,
	EXP_Values.DIRCONN_PER_DATE AS in_DIRCONN_PER_DATE,
	-- *INF*: iif(isnull(in_DIRCONN_PER_DATE),TO_DATE('1/1/1800','MM/DD/YYYY'),in_DIRCONN_PER_DATE)
	IFF(in_DIRCONN_PER_DATE IS NULL,
		TO_DATE('1/1/1800', 'MM/DD/YYYY'
		),
		in_DIRCONN_PER_DATE
	) AS out_DIRCONN_PER_DATE,
	EXP_Values.DIRCONN_COMM_DATE AS in_DIRCONN_COMM_DATE,
	-- *INF*: iif(isnull(in_DIRCONN_COMM_DATE),TO_DATE('1/1/1800','MM/DD/YYYY'),in_DIRCONN_COMM_DATE)
	IFF(in_DIRCONN_COMM_DATE IS NULL,
		TO_DATE('1/1/1800', 'MM/DD/YYYY'
		),
		in_DIRCONN_COMM_DATE
	) AS out_DIRCONN_COMM_DATE,
	LKP_RSM_Stage.RSM_LAST_NAME,
	-- *INF*: iif(isnull(RSM_LAST_NAME),'Not Available',iif(is_spaces(RSM_LAST_NAME),'Not Available',(ltrim(rtrim(RSM_LAST_NAME)))))
	IFF(RSM_LAST_NAME IS NULL,
		'Not Available',
		IFF(LENGTH(RSM_LAST_NAME)>0 AND TRIM(RSM_LAST_NAME)='',
			'Not Available',
			( ltrim(rtrim(RSM_LAST_NAME
					)
				) 
			)
		)
	) AS v_RSM_Last_Name,
	v_RSM_Last_Name AS out_rsm_last_name,
	LKP_RSM_Stage.RSM_FIRST_NAME,
	-- *INF*: iif(isnull(RSM_FIRST_NAME),'N/A',iif(is_spaces(RSM_FIRST_NAME),'N/A',(ltrim(rtrim(RSM_FIRST_NAME)))))
	IFF(RSM_FIRST_NAME IS NULL,
		'N/A',
		IFF(LENGTH(RSM_FIRST_NAME)>0 AND TRIM(RSM_FIRST_NAME)='',
			'N/A',
			( ltrim(rtrim(RSM_FIRST_NAME
					)
				) 
			)
		)
	) AS v_RSM_FIRST_NAME,
	-- *INF*: iif(isnull
	-- (ltrim(rtrim(RSM_FIRST_NAME))
	-- ||' ' || (ltrim(rtrim(RSM_LAST_NAME)))),'Not Available',(ltrim(rtrim(RSM_FIRST_NAME)))||' ' || (ltrim(rtrim(RSM_LAST_NAME))))
	-- 
	IFF(ltrim(rtrim(RSM_FIRST_NAME
			)
		) || ' ' || ( ltrim(rtrim(RSM_LAST_NAME
				)
			) 
		) IS NULL,
		'Not Available',
		( ltrim(rtrim(RSM_FIRST_NAME
				)
			) 
		) || ' ' || ( ltrim(rtrim(RSM_LAST_NAME
				)
			) 
		)
	) AS out_rsm_full_name,
	STATE_CODE || in_AGENCY_NUM AS v_Agency_key,
	-- *INF*: iif(isnull(v_Agency_key), 'N/A',v_Agency_key)
	IFF(v_Agency_key IS NULL,
		'N/A',
		v_Agency_key
	) AS out_Agency_Key,
	1 AS crrnt_snpsht_flag,
	EXP_Values.SOURCE_SYSTEM_ID
	FROM EXP_Values
	LEFT JOIN LKP_RSM_Stage
	ON LKP_RSM_Stage.RSM_ID = LKP_Territory_Stage.RSM_ID
	LEFT JOIN LKP_State_Sup
	ON LKP_State_Sup.state_code = EXP_Values.STATE_CODE
	LEFT JOIN LKP_Territory_Stage
	ON LKP_Territory_Stage.TERRITORY_CODE = EXP_Values.TERRITORY_CODE AND LKP_Territory_Stage.STATE_CODE = EXP_Values.STATE_CODE
	LEFT JOIN LKP_RSM_TERRITORY_NAME_RULE LKP_RSM_TERRITORY_NAME_RULE_in_rsm_terr_sym
	ON LKP_RSM_TERRITORY_NAME_RULE_in_rsm_terr_sym.rsm_terr_sym = in_rsm_terr_sym

),
LKP_Related_Agency_Stage_Prim_Agency_key AS (
	SELECT
	REL_RSN_CODE,
	PRIM_STATE_CODE,
	PRIM_AGENCY_NUM,
	PRIM_AGENCY_ABBREV_NAME,
	REL_STATE_CODE,
	REL_AGENCY_NUM
	FROM (
		SELECT 
		
		related_agency_Stage.REL_STATE_CODE as REL_STATE_CODE, 
		related_agency_Stage.REL_AGENCY_NUM as REL_AGENCY_NUM,
		related_agency_Stage.REL_RSN_CODE as REL_RSN_CODE,
		related_agency_Stage.STATE_CODE as PRIM_STATE_CODE,
		related_agency_Stage.AGENCY_NUM as PRIM_AGENCY_NUM,
		agency_stage.AGENCY_ABBREV_NAME as PRIM_AGENCY_ABBREV_NAME
		
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.related_agency_Stage  related_agency_Stage,
		              @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency_stage agency_stage
		
		Where
		agency_stage.state_code = related_agency_Stage.STATE_CODE
		AND agency_stage.AGENCY_NUM = related_agency_Stage.AGENCY_NUM
		AND related_agency_Stage.REL_EFF_DATE <= getdate()
		AND related_agency_Stage.REL_RSN_CODE <> 'D'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY REL_STATE_CODE,REL_AGENCY_NUM ORDER BY REL_RSN_CODE) = 1
),
LKP_State_Sup1 AS (
	SELECT
	state_abbrev,
	state_descript,
	state_code
	FROM (
		SELECT 
			state_abbrev,
			state_descript,
			state_code
		FROM dbo.State_Sup
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY state_abbrev) = 1
),
LKP_agency AS (
	SELECT
	agency_id,
	agency_ak_id,
	rsm_terr_sym,
	rsm_terr_code,
	rsm_terr_name,
	rsm_full_name,
	rsm_last_name,
	prim_agency_state_code,
	prim_agency_state_abbrev,
	prim_agency_state_descript,
	prim_agency_num,
	prim_agency_key,
	prim_agency_name,
	agency_state_code,
	agency_state_abbrev,
	agency_state_descript,
	agency_num,
	agency_key,
	agency_name,
	rsm_terr_name_rule_id,
	dirconn_per_date,
	dirconn_comm_date,
	agency_pay_code,
	agency_pay_code_eff_to_date,
	agency_pay_code_eff_from_date,
	out_Agency_Key
	FROM (
		SELECT 
		a.agency_id as agency_id, 
		a.agency_ak_id as agency_ak_id,
		a.rsm_terr_sym as rsm_terr_sym, 
		a.rsm_terr_code as rsm_terr_code, 
		a.rsm_terr_name as rsm_terr_name, 
		a.rsm_full_name as rsm_full_name, 
		a.rsm_last_name as rsm_last_name, 
		a.prim_agency_state_code as prim_agency_state_code, 
		a.prim_agency_state_abbrev as prim_agency_state_abbrev, 
		a.prim_agency_state_descript as prim_agency_state_descript, 
		a.prim_agency_num as prim_agency_num, 
		a.prim_agency_key as prim_agency_key,
		a.prim_agency_name as prim_agency_name, 
		a.agency_state_abbrev as agency_state_abbrev, 
		a.agency_state_descript as agency_state_descript, 
		a.agency_key as agency_key, 
		a.agency_name as agency_name, 
		a.rsm_terr_name_rule_id as rsm_terr_name_rule_id, 
		a.dirconn_per_date as dirconn_per_date, 
		a.dirconn_comm_date as dirconn_comm_date, 
		a.agency_state_code as agency_state_code, 
		a.agency_num as agency_num,
		a.agency_pay_code as agency_pay_code, 
		a.agency_pay_code_eff_to_date as agency_pay_code_eff_to_date, 
		a.agency_pay_code_eff_from_date as agency_pay_code_eff_from_date 
		FROM 
			@{pipeline().parameters.SOURCE_TABLE_OWNER}.agency a
		WHERE  a.agency_id IN (SELECT MAX(b.agency_id)
			from 
				@{pipeline().parameters.SOURCE_TABLE_OWNER}.agency b
			WHERE crrnt_snpsht_flag=1
			GROUP BY b.agency_state_code,
			b.agency_num)
		ORDER BY a.agency_state_code,
			a.agency_num
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statemen
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY agency_id) = 1
),
LKP_pay_code_stage AS (
	SELECT
	pay_code_stage_id,
	state_code,
	agency_num,
	pay_code,
	pay_code_exp_date,
	pay_code_eff_date,
	agency_code,
	agency_num_IN,
	agency_key_IN
	FROM (
		SELECT 
		PC.pay_code_stage_id as pay_code_stage_id, 
		PC.pay_code as pay_code, 
		PC.pay_code_exp_date as pay_code_exp_date, 
		PC.pay_code_eff_date as pay_code_eff_date, 
		PC.state_code as state_code, 
		PC.agency_num as agency_num, 
		PC.agency_code as agency_code 
		
		FROM 
		pay_code_stage PC
		order by pay_code_exp_date desc --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code,agency_num,agency_code ORDER BY pay_code_stage_id) = 1
),
SEQ_Agency AS (
	CREATE SEQUENCE SEQ_Agency
	START = 0
	INCREMENT = 1;
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.out_rsm_TERR_SYM AS rsm_TERR_SYM,
	EXP_Lkp_Values.out_rsm_TERR_CODE AS in_rsm_TERR_CODE,
	EXP_Lkp_Values.out_rsm_TERR_NAME AS rsm_TERR_NAME,
	EXP_Lkp_Values.out_rsm_full_name,
	EXP_Lkp_Values.out_rsm_last_name,
	LKP_Related_Agency_Stage_Prim_Agency_key.PRIM_STATE_CODE AS in_prim_agency_state_code,
	-- *INF*: IIF(ISNULL(in_prim_agency_state_code),agency_state_code,in_prim_agency_state_code)
	IFF(in_prim_agency_state_code IS NULL,
		agency_state_code,
		in_prim_agency_state_code
	) AS v_prim_agency_state_code,
	v_prim_agency_state_code || '  ' AS prim_agency_state_code,
	LKP_State_Sup1.state_abbrev AS in_prim_agency_state_abbrev,
	-- *INF*: IIF(ISNULL(in_prim_agency_state_abbrev),agency_state_abbrev,in_prim_agency_state_abbrev)
	IFF(in_prim_agency_state_abbrev IS NULL,
		agency_state_abbrev,
		in_prim_agency_state_abbrev
	) AS v_prim_agency_state_abbrev,
	v_prim_agency_state_abbrev AS prim_agency_state_abbrev,
	LKP_State_Sup1.state_descript AS in_prim_agency_state_descript,
	-- *INF*: IIF(ISNULL(in_prim_agency_state_descript),agency_state_descript,in_prim_agency_state_descript)
	IFF(in_prim_agency_state_descript IS NULL,
		agency_state_descript,
		in_prim_agency_state_descript
	) AS v_prim_agency_state_descript,
	v_prim_agency_state_descript AS prim_agency_state_descript,
	LKP_Related_Agency_Stage_Prim_Agency_key.PRIM_AGENCY_NUM AS in_prim_AGENCY_NUM,
	-- *INF*: IIF(ISNULL(in_prim_AGENCY_NUM),AGENCY_NUM,in_prim_AGENCY_NUM)
	IFF(in_prim_AGENCY_NUM IS NULL,
		AGENCY_NUM,
		in_prim_AGENCY_NUM
	) AS v_prim_AGENCY_NUM,
	v_prim_AGENCY_NUM AS prim_AGENCY_NUM,
	v_prim_agency_state_code || v_prim_AGENCY_NUM AS V_out_prim_agency_key,
	V_out_prim_agency_key AS OUT_PRIM_AGENCY_KEY,
	LKP_Related_Agency_Stage_Prim_Agency_key.PRIM_AGENCY_ABBREV_NAME AS in_prim_AGENCY_NAME,
	-- *INF*: RTRIM(IIF(ISNULL(in_prim_AGENCY_NAME),AGENCY_NAME,in_prim_AGENCY_NAME))
	RTRIM(IFF(in_prim_AGENCY_NAME IS NULL,
			AGENCY_NAME,
			in_prim_AGENCY_NAME
		)
	) AS v_prim_AGENCY_NAME,
	v_prim_AGENCY_NAME AS prim_AGENCY_NAME,
	EXP_Lkp_Values.agency_state_code,
	EXP_Lkp_Values.agency_state_abbrev,
	EXP_Lkp_Values.out_agency_state_descript AS agency_state_descript,
	EXP_Lkp_Values.out_AGENCY_NUM AS AGENCY_NUM,
	EXP_Lkp_Values.out_Agency_Key,
	EXP_Lkp_Values.out_AGENCY_NAME AS AGENCY_NAME,
	EXP_Lkp_Values.out_v_Audit_Terr_Name_Rule_id AS out_Audit_Terr_Name_Rule_id,
	EXP_Lkp_Values.out_DIRCONN_PER_DATE AS DIRCONN_PER_DATE,
	EXP_Lkp_Values.out_DIRCONN_COMM_DATE AS DIRCONN_COMM_DATE,
	LKP_pay_code_stage.pay_code AS in_pay_code,
	-- *INF*: IIF(isnull(in_pay_code),'N/A',in_pay_code)
	IFF(in_pay_code IS NULL,
		'N/A',
		in_pay_code
	) AS v_pay_code,
	v_pay_code AS pay_code,
	LKP_pay_code_stage.pay_code_exp_date AS in_pay_code_exp_date,
	-- *INF*: iif(isnull(in_pay_code_exp_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_pay_code_exp_date)
	IFF(in_pay_code_exp_date IS NULL,
		TO_DATE('1/1/1800', 'MM/DD/YYYY'
		),
		in_pay_code_exp_date
	) AS v_pay_code_exp_date,
	v_pay_code_exp_date AS pay_code_exp_date,
	LKP_pay_code_stage.pay_code_eff_date AS in_pay_code_eff_date,
	-- *INF*: iif(isnull(in_pay_code_eff_date),TO_DATE('1/1/1800','MM/DD/YYYY'),in_pay_code_eff_date)
	IFF(in_pay_code_eff_date IS NULL,
		TO_DATE('1/1/1800', 'MM/DD/YYYY'
		),
		in_pay_code_eff_date
	) AS v_pay_code_eff_date,
	v_pay_code_eff_date AS pay_code_eff_date,
	LKP_agency.agency_ak_id,
	EXP_Lkp_Values.crrnt_snpsht_flag,
	LKP_agency.rsm_terr_sym AS rsm_terr_sym_old,
	LKP_agency.rsm_terr_code AS rsm_terr_code_old,
	LKP_agency.rsm_terr_name AS rsm_terr_name_old,
	LKP_agency.rsm_full_name AS rsm_full_name_old,
	LKP_agency.rsm_last_name AS rsm_last_name_old,
	LKP_agency.prim_agency_state_code AS prim_agency_state_code_old,
	LKP_agency.prim_agency_state_abbrev AS prim_agency_state_abbrev_old,
	LKP_agency.prim_agency_state_descript AS prim_agency_state_descript_old,
	LKP_agency.prim_agency_num AS prim_agency_num_old,
	LKP_agency.prim_agency_key AS prim_agency_key_old,
	LKP_agency.prim_agency_name AS prim_agency_name_old,
	LKP_agency.agency_state_code AS agency_state_code_old,
	LKP_agency.agency_state_abbrev AS agency_state_abbrev_old,
	LKP_agency.agency_state_descript AS agency_state_descript_old,
	LKP_agency.agency_num AS agency_num_old,
	LKP_agency.agency_key AS agency_key_old,
	LKP_agency.agency_name AS agency_name_old,
	LKP_agency.rsm_terr_name_rule_id AS rsm_terr_name_rule_id_old,
	LKP_agency.dirconn_per_date AS dirconn_per_date_old,
	LKP_agency.dirconn_comm_date AS dirconn_comm_date_old,
	LKP_agency.agency_pay_code,
	LKP_agency.agency_pay_code_eff_to_date,
	LKP_agency.agency_pay_code_eff_from_date,
	-- *INF*:  iif(isnull(agency_id_old),'NEW',
	-- iif((rsm_TERR_SYM <> rsm_terr_sym_old ) or
	-- (in_rsm_TERR_CODE  <> RTrim (rsm_terr_code_old, '  ') ) or
	-- (rsm_TERR_NAME <> rsm_terr_name_old) or
	-- (out_rsm_full_name<> rsm_full_name_old) or
	-- (out_rsm_last_name <> rsm_last_name_old ) or
	-- (LPAD(v_prim_agency_state_code,2) <> LPAD (prim_agency_state_code_old,2))  or
	-- (v_prim_agency_state_abbrev<> prim_agency_state_abbrev_old ) or
	-- (v_prim_agency_state_descript<>prim_agency_state_descript_old ) or
	-- (v_prim_AGENCY_NUM<> prim_agency_num_old ) or
	-- (V_out_prim_agency_key<> prim_agency_key_old ) or
	-- (v_prim_AGENCY_NAME<> prim_agency_name_old ) or
	-- (LPAD(agency_state_code,2) <> LPAD(agency_state_code_old,2)) or
	-- (agency_state_abbrev<> agency_state_abbrev_old ) or
	-- (agency_state_descript<>agency_state_descript_old ) or
	-- (AGENCY_NUM<>agency_num_old ) or
	-- (out_Agency_Key<>agency_key_old ) or
	-- (AGENCY_NAME<> agency_name_old ) or
	-- (out_Audit_Terr_Name_Rule_id<> rsm_terr_name_rule_id_old ) or
	-- (DIRCONN_PER_DATE<> dirconn_per_date_old) or
	-- (DIRCONN_COMM_DATE<> dirconn_comm_date_old ) or
	-- (v_pay_code <> agency_pay_code) or
	-- (v_pay_code_exp_date <> agency_pay_code_eff_to_date) or
	-- (v_pay_code_eff_date <> agency_pay_code_eff_from_date)
	-- ,
	-- 'UPDATE',
	-- 'NOCHANGE'))
	IFF(agency_id_old IS NULL,
		'NEW',
		IFF(( rsm_TERR_SYM <> rsm_terr_sym_old 
			) 
			OR ( in_rsm_TERR_CODE <> RTrim(rsm_terr_code_old, '  '
				) 
			) 
			OR ( rsm_TERR_NAME <> rsm_terr_name_old 
			) 
			OR ( out_rsm_full_name <> rsm_full_name_old 
			) 
			OR ( out_rsm_last_name <> rsm_last_name_old 
			) 
			OR ( LPAD(v_prim_agency_state_code, 2
				) <> LPAD(prim_agency_state_code_old, 2
				) 
			) 
			OR ( v_prim_agency_state_abbrev <> prim_agency_state_abbrev_old 
			) 
			OR ( v_prim_agency_state_descript <> prim_agency_state_descript_old 
			) 
			OR ( v_prim_AGENCY_NUM <> prim_agency_num_old 
			) 
			OR ( V_out_prim_agency_key <> prim_agency_key_old 
			) 
			OR ( v_prim_AGENCY_NAME <> prim_agency_name_old 
			) 
			OR ( LPAD(agency_state_code, 2
				) <> LPAD(agency_state_code_old, 2
				) 
			) 
			OR ( agency_state_abbrev <> agency_state_abbrev_old 
			) 
			OR ( agency_state_descript <> agency_state_descript_old 
			) 
			OR ( AGENCY_NUM <> agency_num_old 
			) 
			OR ( out_Agency_Key <> agency_key_old 
			) 
			OR ( AGENCY_NAME <> agency_name_old 
			) 
			OR ( out_Audit_Terr_Name_Rule_id <> rsm_terr_name_rule_id_old 
			) 
			OR ( DIRCONN_PER_DATE <> dirconn_per_date_old 
			) 
			OR ( DIRCONN_COMM_DATE <> dirconn_comm_date_old 
			) 
			OR ( v_pay_code <> agency_pay_code 
			) 
			OR ( v_pay_code_exp_date <> agency_pay_code_eff_to_date 
			) 
			OR ( v_pay_code_eff_date <> agency_pay_code_eff_from_date 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
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
	LKP_agency.agency_id AS agency_id_old,
	EXP_Lkp_Values.SOURCE_SYSTEM_ID,
	sysdate AS created_date,
	sysdate AS modified_date,
	SEQ_Agency.NEXTVAL,
	-- *INF*: IIF(v_changed_flag='NEW',
	-- NEXTVAL,
	-- agency_ak_id)
	IFF(v_changed_flag = 'NEW',
		NEXTVAL,
		agency_ak_id
	) AS out_agency_ak_id
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_Related_Agency_Stage_Prim_Agency_key
	ON LKP_Related_Agency_Stage_Prim_Agency_key.REL_STATE_CODE = EXP_Lkp_Values.agency_state_code AND LKP_Related_Agency_Stage_Prim_Agency_key.REL_AGENCY_NUM = EXP_Lkp_Values.out_AGENCY_NUM
	LEFT JOIN LKP_State_Sup1
	ON LKP_State_Sup1.state_code = LKP_Related_Agency_Stage_Prim_Agency_key.PRIM_STATE_CODE
	LEFT JOIN LKP_agency
	ON LKP_agency.agency_key = EXP_Lkp_Values.out_Agency_Key
	LEFT JOIN LKP_pay_code_stage
	ON LKP_pay_code_stage.state_code = EXP_Lkp_Values.agency_state_code AND LKP_pay_code_stage.agency_num = EXP_Lkp_Values.out_AGENCY_NUM AND LKP_pay_code_stage.agency_code = EXP_Lkp_Values.out_Agency_Key
),
FLT_Insert AS (
	SELECT
	EXP_Detect_Changes.out_agency_ak_id, 
	EXP_Detect_Changes.rsm_TERR_SYM, 
	EXP_Detect_Changes.in_rsm_TERR_CODE AS rsm_TERR_CODE, 
	EXP_Detect_Changes.rsm_TERR_NAME, 
	EXP_Detect_Changes.out_rsm_full_name, 
	EXP_Detect_Changes.out_rsm_last_name, 
	EXP_Detect_Changes.prim_agency_state_code, 
	EXP_Detect_Changes.prim_agency_state_abbrev, 
	EXP_Detect_Changes.prim_agency_state_descript, 
	EXP_Detect_Changes.prim_AGENCY_NUM, 
	EXP_Detect_Changes.OUT_PRIM_AGENCY_KEY AS out_prim_agency_key, 
	EXP_Detect_Changes.prim_AGENCY_NAME, 
	EXP_Detect_Changes.agency_state_code, 
	EXP_Detect_Changes.agency_state_abbrev, 
	EXP_Detect_Changes.agency_state_descript, 
	EXP_Detect_Changes.AGENCY_NUM, 
	EXP_Detect_Changes.out_Agency_Key, 
	EXP_Detect_Changes.AGENCY_NAME, 
	EXP_Detect_Changes.out_Audit_Terr_Name_Rule_id, 
	EXP_Detect_Changes.DIRCONN_PER_DATE, 
	EXP_Detect_Changes.DIRCONN_COMM_DATE, 
	EXP_Detect_Changes.crrnt_snpsht_flag, 
	EXP_Detect_Changes.audit_id, 
	EXP_Detect_Changes.eff_from_date, 
	EXP_Detect_Changes.eff_to_date, 
	EXP_Detect_Changes.changed_flag, 
	EXP_Detect_Changes.SOURCE_SYSTEM_ID, 
	EXP_Detect_Changes.created_date, 
	EXP_Detect_Changes.modified_date, 
	LKP_Related_Agency_Stage_Prim_Agency_key.REL_RSN_CODE, 
	LKP_Territory_Stage.STATE_CODE AS Terr_STATE_CODE, 
	EXP_Detect_Changes.pay_code, 
	EXP_Detect_Changes.pay_code_exp_date, 
	EXP_Detect_Changes.pay_code_eff_date
	FROM EXP_Detect_Changes
	LEFT JOIN LKP_Related_Agency_Stage_Prim_Agency_key
	ON LKP_Related_Agency_Stage_Prim_Agency_key.REL_STATE_CODE = EXP_Lkp_Values.agency_state_code AND LKP_Related_Agency_Stage_Prim_Agency_key.REL_AGENCY_NUM = EXP_Lkp_Values.out_AGENCY_NUM
	LEFT JOIN LKP_Territory_Stage
	ON LKP_Territory_Stage.TERRITORY_CODE = EXP_Values.TERRITORY_CODE AND LKP_Territory_Stage.STATE_CODE = EXP_Values.STATE_CODE
	WHERE (changed_flag='NEW' or changed_flag='UPDATE')

AND REL_RSN_CODE <> 'D'

AND Terr_STATE_CODE = agency_state_code
),
agency_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency
	(agency_ak_id, rsm_terr_sym, rsm_terr_code, rsm_terr_name, rsm_full_name, rsm_last_name, prim_agency_state_code, prim_agency_state_abbrev, prim_agency_state_descript, prim_agency_num, prim_agency_key, prim_agency_name, agency_state_code, agency_state_abbrev, agency_state_descript, agency_num, agency_key, agency_name, rsm_terr_name_rule_id, dirconn_per_date, dirconn_comm_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date, agency_pay_code, agency_pay_code_eff_from_date, agency_pay_code_eff_to_date)
	SELECT 
	out_agency_ak_id AS AGENCY_AK_ID, 
	rsm_TERR_SYM AS RSM_TERR_SYM, 
	rsm_TERR_CODE AS RSM_TERR_CODE, 
	rsm_TERR_NAME AS RSM_TERR_NAME, 
	out_rsm_full_name AS RSM_FULL_NAME, 
	out_rsm_last_name AS RSM_LAST_NAME, 
	PRIM_AGENCY_STATE_CODE, 
	PRIM_AGENCY_STATE_ABBREV, 
	PRIM_AGENCY_STATE_DESCRIPT, 
	prim_AGENCY_NUM AS PRIM_AGENCY_NUM, 
	out_prim_agency_key AS PRIM_AGENCY_KEY, 
	prim_AGENCY_NAME AS PRIM_AGENCY_NAME, 
	AGENCY_STATE_CODE, 
	AGENCY_STATE_ABBREV, 
	AGENCY_STATE_DESCRIPT, 
	AGENCY_NUM AS AGENCY_NUM, 
	out_Agency_Key AS AGENCY_KEY, 
	AGENCY_NAME AS AGENCY_NAME, 
	out_Audit_Terr_Name_Rule_id AS RSM_TERR_NAME_RULE_ID, 
	DIRCONN_PER_DATE AS DIRCONN_PER_DATE, 
	DIRCONN_COMM_DATE AS DIRCONN_COMM_DATE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	pay_code AS AGENCY_PAY_CODE, 
	pay_code_eff_date AS AGENCY_PAY_CODE_EFF_FROM_DATE, 
	pay_code_exp_date AS AGENCY_PAY_CODE_EFF_TO_DATE
	FROM FLT_Insert
),
SQ_agency AS (
	SELECT a.agency_id, 
	a.agency_state_code, 
	a.agency_num, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.agency a
	WHERE EXISTS(SELECT 1			
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency b
		WHERE eff_to_date = '12/31/2100 23:59:59'
		and a.agency_state_code  = b.agency_state_code
		and a.agency_num = b.agency_num
		GROUP BY agency_state_code, 
		agency_num
		HAVING COUNT(*) > 1)
	ORDER BY agency_state_code,agency_num,eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	agency_id,
	agency_state_code,
	agency_num,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	agency_state_code=v_PREV_ROW_agency_state_code and agency_num=v_PREV_ROW_agency_num, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	-- 
	-- 
	-- 
	DECODE(TRUE,
		agency_state_code = v_PREV_ROW_agency_state_code 
		AND agency_num = v_PREV_ROW_agency_num, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	agency_state_code AS v_PREV_ROW_agency_state_code,
	agency_num AS v_PREV_ROW_agency_num,
	sysdate AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_agency
),
FIL_FirstRowInAKGroup AS (
	SELECT
	agency_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Agency AS (
	SELECT
	agency_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
agency_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency AS T
	USING UPD_Agency AS S
	ON T.agency_id = S.agency_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),