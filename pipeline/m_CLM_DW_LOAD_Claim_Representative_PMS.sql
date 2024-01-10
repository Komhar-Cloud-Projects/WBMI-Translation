WITH
LKP_Sup_Claim_Staff_Email AS (
	SELECT
	EMAIL,
	WBCONNECT_USER_ID
	FROM (
		SELECT
		ltrim(rtrim(a.EMAIL)) as EMAIL
		,ltrim(rtrim(a.WBCONNECT_USER_ID)) as WBCONNECT_USER_ID 
		FROM 
		sup_claim_staff_stage a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WBCONNECT_USER_ID ORDER BY EMAIL) = 1
),
SQ_PMS_ADJUSTER_MASTER_STAGE AS (
	SELECT 
	ltrim(rtrim(a.ADNM_ADJUSTOR_NBR))
	, ltrim(rtrim(a.ADNM_NAME))
	,ltrim(rtrim(a.ADNM_ADJUSTOR_BRANCH_NUMBER))
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PMS_ADJUSTER_MASTER_STAGE a
	where a.ADNM_TYPE_ADJUSTOR = 'H'
),
EXP_Source AS (
	SELECT
	ADNM_ADJUSTOR_NBR,
	ADNM_NAME,
	ADNM_ADJUSTOR_BRANCH_NUMBER,
	-- *INF*: IIF(IS_SPACES(ADNM_NAME) OR ISNULL(ADNM_NAME)
	-- ,'N/A'
	-- ,ADNM_NAME)
	IFF(LENGTH(ADNM_NAME)>0 AND TRIM(ADNM_NAME)='' 
		OR ADNM_NAME IS NULL,
		'N/A',
		ADNM_NAME
	) AS out_ADNM_NAME
	FROM SQ_PMS_ADJUSTER_MASTER_STAGE
),
LKP_Sup_Claim_Adjuster_EDW AS (
	SELECT
	wbconnect_user_id,
	adjuster_code
	FROM (
		SELECT 
		ltrim(rtrim(a.wbconnect_user_id)) as wbconnect_user_id
		, ltrim(rtrim(a.adjuster_code)) as adjuster_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_adjuster a
		WHERE a.crrnt_snpsht_flag  = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adjuster_code ORDER BY wbconnect_user_id) = 1
),
LKP_Claim_Rep_handling_office AS (
	SELECT
	handling_office_code,
	claim_rep_wbconnect_user_id
	FROM (
		SELECT 
		claim_representative.handling_office_code as handling_office_code, 
		claim_representative.claim_rep_wbconnect_user_id as claim_rep_wbconnect_user_id 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative
		WHERE source_sys_id  <> '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_wbconnect_user_id ORDER BY handling_office_code) = 1
),
EXP_Adjustor_Claim_Exists AS (
	SELECT
	handling_office_code AS ipfcgp_loss_handling_office,
	-- *INF*: iif(isnull(ipfcgp_loss_handling_office) or is_spaces(ipfcgp_loss_handling_office) or ipfcgp_loss_handling_office ='' OR LENGTH( ipfcgp_loss_handling_office) = 0
	-- ,'N/A'
	-- ,ipfcgp_loss_handling_office)
	IFF(ipfcgp_loss_handling_office IS NULL 
		OR LENGTH(ipfcgp_loss_handling_office)>0 AND TRIM(ipfcgp_loss_handling_office)='' 
		OR ipfcgp_loss_handling_office = '' 
		OR LENGTH(ipfcgp_loss_handling_office
		) = 0,
		'N/A',
		ipfcgp_loss_handling_office
	) AS ipfcgp_loss_handling_office_v,
	ipfcgp_loss_handling_office_v AS ipfcgp_loss_handling_office_out,
	-- *INF*: iif(ipfcgp_loss_handling_office_v = 'N/A'
	-- ,'N'
	-- ,'Y')
	IFF(ipfcgp_loss_handling_office_v = 'N/A',
		'N',
		'Y'
	) AS v_Exists_At_Claim_Level,
	v_Exists_At_Claim_Level AS Exists_At_Claim_level
	FROM LKP_Claim_Rep_handling_office
),
LKP_Claim_Representative_Hierachy_EDW AS (
	SELECT
	dvsn_code,
	dvsn_descript,
	dept_descript,
	dept_name,
	dept_mgr,
	handling_office_descript,
	handling_office_mgr,
	handling_office_code
	FROM (
		SELECT distinct 
		LTRIM(RTRIM(a.dvsn_code)) as dvsn_code
		,LTRIM(RTRIM(a.dvsn_descript)) as dvsn_descript
		, LTRIM(RTRIM(a.dept_descript)) as dept_descript
		, LTRIM(RTRIM(a.dept_name)) as dept_name
		, LTRIM(RTRIM(a.dept_mgr)) as dept_mgr
		, LTRIM(RTRIM(a.handling_office_descript)) as handling_office_descript
		, LTRIM(RTRIM(a.handling_office_mgr)) as handling_office_mgr
		,LTRIM(RTRIM(a.handling_office_code)) as handling_office_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
		where a.crrnt_snpsht_flag = 1
		and a.source_sys_id <> '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		
		
		--the last filter is applied as the hierachy of the PMS adjustor is built upon that of EXCEED system.
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY handling_office_code ORDER BY dvsn_code) = 1
),
LKP_Sup_Claim_Report_Office_Stage AS (
	SELECT
	CLAIM_MANAGER_CODE,
	REPORT_OFFICE_NAME,
	DIRECTOR_CODE,
	DEPT_CODE,
	REPORT_OFFICE_CODE
	FROM (
		SELECT 
		ltrim(rtrim(a.CLAIM_MANAGER_CODE)) as CLAIM_MANAGER_CODE
		, ltrim(rtrim(a.REPORT_OFFICE_NAME)) as REPORT_OFFICE_NAME
		, ltrim(rtrim(a.DIRECTOR_CODE)) as DIRECTOR_CODE
		, ltrim(rtrim(a.DEPT_CODE)) as DEPT_CODE
		, ltrim(rtrim(a.REPORT_OFFICE_CODE)) as REPORT_OFFICE_CODE 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SUP_REPORT_OFFICE_STAGE a
		
		--there is no filter needed on source_sys_id as this table is only loaded with EXCEED source
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY REPORT_OFFICE_CODE ORDER BY CLAIM_MANAGER_CODE) = 1
),
LKP_Sup_Claim_Adjuster_EDW_HANDLING_OFFICE_MGR AS (
	SELECT
	wbconnect_user_id,
	adjuster_code
	FROM (
		SELECT 
		ltrim(rtrim(a.wbconnect_user_id)) as wbconnect_user_id
		, ltrim(rtrim(a.adjuster_code)) as adjuster_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_adjuster a
		WHERE a.crrnt_snpsht_flag  = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adjuster_code ORDER BY wbconnect_user_id) = 1
),
LKP_gtam_wbadj_stage AS (
	SELECT
	Cost_Center_Number,
	Adjuster_Code
	FROM (
		SELECT 
			Cost_Center_Number,
			Adjuster_Code
		FROM gtam_wbadj_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Adjuster_Code ORDER BY Cost_Center_Number DESC) = 1
),
EXP_TARGET AS (
	SELECT
	EXP_Source.ADNM_ADJUSTOR_NBR,
	EXP_Source.out_ADNM_NAME AS ADNM_NAME,
	'WBMI' AS CO_Description,
	LKP_Claim_Representative_Hierachy_EDW.dvsn_code,
	LKP_Claim_Representative_Hierachy_EDW.dvsn_descript,
	LKP_Claim_Representative_Hierachy_EDW.dept_descript,
	LKP_Claim_Representative_Hierachy_EDW.dept_name,
	LKP_Claim_Representative_Hierachy_EDW.dept_mgr,
	EXP_Adjustor_Claim_Exists.ipfcgp_loss_handling_office_out AS office_id,
	LKP_Claim_Representative_Hierachy_EDW.handling_office_descript,
	LKP_Claim_Representative_Hierachy_EDW.handling_office_mgr,
	LKP_Sup_Claim_Adjuster_EDW.wbconnect_user_id AS Adjuster_User_id,
	-- *INF*: iif(isnull(:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(Adjuster_User_id))
	-- ,'N/A'
	-- ,:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(Adjuster_User_id)
	-- )
	IFF(LKP_SUP_CLAIM_STAFF_EMAIL_Adjuster_User_id.EMAIL IS NULL,
		'N/A',
		LKP_SUP_CLAIM_STAFF_EMAIL_Adjuster_User_id.EMAIL
	) AS Claim_Rep_Email_ID,
	LKP_Sup_Claim_Adjuster_EDW_HANDLING_OFFICE_MGR.wbconnect_user_id AS wbconnect_user_id_handling_office_mgr,
	-- *INF*: iif(isnull(:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(wbconnect_user_id_handling_office_mgr))
	-- ,'N/A'
	-- ,:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(wbconnect_user_id_handling_office_mgr)
	-- )
	-- 
	IFF(LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_office_mgr.EMAIL IS NULL,
		'N/A',
		LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_office_mgr.EMAIL
	) AS Handling_Office_Mgr_Email_ID,
	EXP_Source.ADNM_ADJUSTOR_BRANCH_NUMBER AS adnm_adjustor_branch_number,
	-- *INF*: lpad(:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(adnm_adjustor_branch_number)),3,'0')
	lpad(:UDF.DEFAULT_VALUE_FOR_STRINGS(TO_CHAR(adnm_adjustor_branch_number
			)
		), 3, '0'
	) AS adnm_adjustor_branch_number_Out,
	LKP_gtam_wbadj_stage.Cost_Center_Number,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(Cost_Center_Number)
	-- --rtrim(ltrim(Cost_Center_Number))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(Cost_Center_Number
	) AS cost_center_number_Out
	FROM EXP_Adjustor_Claim_Exists
	 -- Manually join with EXP_Source
	LEFT JOIN LKP_Claim_Representative_Hierachy_EDW
	ON LKP_Claim_Representative_Hierachy_EDW.handling_office_code = EXP_Adjustor_Claim_Exists.ipfcgp_loss_handling_office_out
	LEFT JOIN LKP_Sup_Claim_Adjuster_EDW
	ON LKP_Sup_Claim_Adjuster_EDW.adjuster_code = EXP_Source.ADNM_ADJUSTOR_NBR
	LEFT JOIN LKP_Sup_Claim_Adjuster_EDW_HANDLING_OFFICE_MGR
	ON LKP_Sup_Claim_Adjuster_EDW_HANDLING_OFFICE_MGR.adjuster_code = LKP_Sup_Claim_Report_Office_Stage.CLAIM_MANAGER_CODE
	LEFT JOIN LKP_gtam_wbadj_stage
	ON LKP_gtam_wbadj_stage.Adjuster_Code = EXP_Source.ADNM_ADJUSTOR_NBR
	LEFT JOIN LKP_SUP_CLAIM_STAFF_EMAIL LKP_SUP_CLAIM_STAFF_EMAIL_Adjuster_User_id
	ON LKP_SUP_CLAIM_STAFF_EMAIL_Adjuster_User_id.WBCONNECT_USER_ID = Adjuster_User_id

	LEFT JOIN LKP_SUP_CLAIM_STAFF_EMAIL LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_office_mgr
	ON LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_office_mgr.WBCONNECT_USER_ID = wbconnect_user_id_handling_office_mgr

),
LKP_Claim_Representative_EDW AS (
	SELECT
	claim_rep_id,
	claim_rep_ak_id,
	claim_rep_key,
	claim_rep_full_name,
	dvsn_code,
	dvsn_descript,
	dept_descript,
	dept_name,
	dept_mgr,
	handling_office_code,
	handling_office_descript,
	handling_office_mgr,
	claim_rep_wbconnect_user_id,
	claim_rep_email,
	claim_rep_direct_automatic_pay_lmt,
	claim_rep_direct_automatic_reserve_lmt,
	handling_office_mgr_email,
	handling_office_mgr_direct_automatic_pay_lmt,
	handling_office_mgr_direct_automatic_reserve_lmt,
	cost_center,
	claim_rep_branch_num,
	ADNM_ADJUSTOR_NBR
	FROM (
		SELECT 
		a.claim_rep_id as claim_rep_id
		, a.claim_rep_ak_id as claim_rep_ak_id
		, ltrim(rtrim(a.claim_rep_full_name)) as claim_rep_full_name
		, ltrim(rtrim(a.dvsn_code)) as dvsn_code
		, ltrim(rtrim(a.dvsn_descript)) as dvsn_descript
		, ltrim(rtrim(a.dept_descript)) as dept_descript
		, ltrim(rtrim(a.dept_name)) as dept_name
		, ltrim(rtrim(a.dept_mgr)) as dept_mgr
		, ltrim(rtrim(a.handling_office_code)) as handling_office_code
		, ltrim(rtrim(a.handling_office_descript)) as handling_office_descript
		, ltrim(rtrim(a.handling_office_mgr)) as handling_office_mgr
		, ltrim(rtrim(a.claim_rep_wbconnect_user_id)) as claim_rep_wbconnect_user_id
		, ltrim(rtrim(a.claim_rep_email)) AS claim_rep_email
		, a.claim_rep_direct_automatic_pay_lmt  AS claim_rep_direct_automatic_pay_lmt
		, a.claim_rep_direct_automatic_reserve_lmt  AS claim_rep_direct_automatic_reserve_lmt
		, ltrim(rtrim(a.handling_office_mgr_email)) AS handling_office_mgr_email
		, a.handling_office_mgr_direct_automatic_pay_lmt     AS handling_office_mgr_direct_automatic_pay_lmt
		, a.handling_office_mgr_direct_automatic_reserve_lmt AS handling_office_mgr_direct_automatic_reserve_lmt
		,a.cost_center as cost_center
		,a.claim_rep_branch_num as claim_rep_branch_num
		, a.claim_rep_key as claim_rep_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
		where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and
		a.claim_rep_id in (select max(b.claim_rep_id) from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative b
		where b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and b.crrnt_snpsht_flag = 1
		group by b.claim_rep_key)
		order by a.claim_rep_key
		
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100 23:59:59'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer 
		--generate default ORDER BY statement
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key ORDER BY claim_rep_id DESC) = 1
),
LKP_Get_Limit_Reserve_Amt_For_Claim_Rep AS (
	SELECT
	CAJ_DIR_AUT_RES,
	CAJ_DIR_AUT_PMT,
	WBCONNCT_USER_ID_CLAIM_REP,
	CAJ_USER_ID
	FROM (
		SELECT 
		ltrim(rtrim(a.CAJ_DIR_AUT_RES)) as CAJ_DIR_AUT_RES
		,ltrim(rtrim(a.CAJ_DIR_AUT_PMT)) as CAJ_DIR_AUT_PMT
		, ltrim(rtrim(a.CAJ_USER_ID)) as CAJ_USER_ID 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.ADJUSTER_TAB_STAGE a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CAJ_USER_ID ORDER BY CAJ_DIR_AUT_RES) = 1
),
LKP_Get_Limit_Reserve_Amt_For_handling_office_mgr AS (
	SELECT
	CAJ_DIR_AUT_RES,
	CAJ_DIR_AUT_PMT,
	WBCONNCT_USER_ID_HANDLING_OFFICE_MGR,
	CAJ_USER_ID
	FROM (
		SELECT 
		ltrim(rtrim(a.CAJ_DIR_AUT_RES)) as CAJ_DIR_AUT_RES
		,ltrim(rtrim(a.CAJ_DIR_AUT_PMT)) as CAJ_DIR_AUT_PMT
		, ltrim(rtrim(a.CAJ_USER_ID)) as CAJ_USER_ID 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.ADJUSTER_TAB_STAGE a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CAJ_USER_ID ORDER BY CAJ_DIR_AUT_RES) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Claim_Representative_EDW.claim_rep_id AS lkp_claim_rep_id,
	LKP_Claim_Representative_EDW.claim_rep_ak_id AS lkp_claim_rep_ak_id,
	LKP_Claim_Representative_EDW.claim_rep_full_name AS lkp_claim_rep_name,
	LKP_Claim_Representative_EDW.dvsn_code AS lkp_dvsn_code,
	LKP_Claim_Representative_EDW.dvsn_descript AS lkp_dvsn_descript,
	LKP_Claim_Representative_EDW.dept_descript AS lkp_dept_descript,
	LKP_Claim_Representative_EDW.dept_name AS lkp_dept_name,
	LKP_Claim_Representative_EDW.dept_mgr AS lkp_dept_mgr,
	LKP_Claim_Representative_EDW.handling_office_code AS lkp_handling_office_code,
	LKP_Claim_Representative_EDW.handling_office_descript AS lkp_handling_office_descript,
	LKP_Claim_Representative_EDW.handling_office_mgr AS lkp_handling_office_mgr,
	LKP_Claim_Representative_EDW.claim_rep_wbconnect_user_id AS lkp_webconnect_user_id,
	LKP_Claim_Representative_EDW.claim_rep_email AS lkp_claim_rep_email,
	LKP_Claim_Representative_EDW.claim_rep_direct_automatic_pay_lmt AS lkp_claim_rep_direct_automatic_pay_lmt,
	LKP_Claim_Representative_EDW.claim_rep_direct_automatic_reserve_lmt AS lkp_claim_rep_direct_automatic_reserve_lmt,
	LKP_Claim_Representative_EDW.handling_office_mgr_email AS lkp_handling_office_mgr_email,
	LKP_Claim_Representative_EDW.handling_office_mgr_direct_automatic_pay_lmt AS lkp_handling_office_mgr_direct_automatic_pay_lmt,
	LKP_Claim_Representative_EDW.handling_office_mgr_direct_automatic_reserve_lmt AS lkp_handling_office_mgr_direct_automatic_reserve_lmt,
	LKP_Claim_Representative_EDW.cost_center AS lkp_cost_center,
	LKP_Claim_Representative_EDW.claim_rep_branch_num AS lkp_claim_rep_branch_num,
	EXP_TARGET.ADNM_NAME AS claim_rep_name,
	EXP_TARGET.dvsn_code AS DIVISION_CODE,
	EXP_TARGET.dvsn_descript AS DIVISION_DESC,
	EXP_TARGET.dept_descript AS DEPT_DESC,
	EXP_TARGET.dept_name AS DEPT_CODE,
	EXP_TARGET.dept_mgr AS DEPT_MGR,
	EXP_TARGET.office_id AS OFFICE_ID,
	EXP_TARGET.handling_office_descript AS REPORT_OFFICE_NAME,
	EXP_TARGET.Adjuster_User_id AS USER_ID,
	EXP_TARGET.handling_office_mgr AS Handling_Office_Mgr,
	EXP_Adjustor_Claim_Exists.Exists_At_Claim_level,
	EXP_TARGET.ADNM_ADJUSTOR_NBR AS Adjuster_No,
	EXP_TARGET.CO_Description,
	EXP_TARGET.Claim_Rep_Email_ID,
	LKP_Get_Limit_Reserve_Amt_For_Claim_Rep.CAJ_DIR_AUT_RES AS Claim_Rep_DIR_AUT_RES,
	LKP_Get_Limit_Reserve_Amt_For_Claim_Rep.CAJ_DIR_AUT_PMT AS Claim_Rep_DIR_AUT_PMT,
	EXP_TARGET.Handling_Office_Mgr_Email_ID,
	LKP_Get_Limit_Reserve_Amt_For_handling_office_mgr.CAJ_DIR_AUT_RES AS Handling_Office_Mgr_DIR_AUT_RES,
	LKP_Get_Limit_Reserve_Amt_For_handling_office_mgr.CAJ_DIR_AUT_PMT AS Handling_Office_Mgr_DIR_AUT_PMT,
	EXP_TARGET.adnm_adjustor_branch_number_Out AS adnm_adjustor_branch_number,
	EXP_TARGET.cost_center_number_Out AS cost_center_number,
	-- *INF*: iif(isnull(lkp_claim_rep_id)
	-- ,'NEW'
	-- ,iif(Exists_At_Claim_level = 'Y' and (lkp_claim_rep_name != claim_rep_name OR 
	-- lkp_dvsn_code != DIVISION_CODE OR 
	-- lkp_dvsn_descript != DIVISION_DESC OR 
	-- lkp_dept_descript != DEPT_DESC OR 
	-- lkp_dept_name != DEPT_CODE OR 
	-- lkp_dept_mgr != DEPT_MGR OR 
	-- lkp_handling_office_code != OFFICE_ID OR 
	-- lkp_handling_office_descript != REPORT_OFFICE_NAME OR 
	-- lkp_handling_office_mgr != Handling_Office_Mgr OR
	-- lkp_webconnect_user_id != USER_ID OR
	-- lkp_claim_rep_email != Claim_Rep_Email_ID OR 
	-- lkp_handling_office_mgr_email != Handling_Office_Mgr_Email_ID OR 
	-- lkp_claim_rep_direct_automatic_pay_lmt != Claim_Rep_DIR_AUT_PMT OR 
	-- lkp_claim_rep_direct_automatic_reserve_lmt != Claim_Rep_DIR_AUT_RES OR 
	-- lkp_handling_office_mgr_direct_automatic_pay_lmt != Handling_Office_Mgr_DIR_AUT_PMT OR 
	-- lkp_handling_office_mgr_direct_automatic_reserve_lmt != Handling_Office_Mgr_DIR_AUT_RES OR
	-- rtrim(ltrim(lkp_cost_center)) != rtrim(ltrim(cost_center_number)) OR
	-- lkp_claim_rep_branch_num != adnm_adjustor_branch_number
	-- )
	-- ,'UPDATE'
	-- ,'NO CHANGE'))
	IFF(lkp_claim_rep_id IS NULL,
		'NEW',
		IFF(Exists_At_Claim_level = 'Y' 
			AND ( lkp_claim_rep_name != claim_rep_name 
				OR lkp_dvsn_code != DIVISION_CODE 
				OR lkp_dvsn_descript != DIVISION_DESC 
				OR lkp_dept_descript != DEPT_DESC 
				OR lkp_dept_name != DEPT_CODE 
				OR lkp_dept_mgr != DEPT_MGR 
				OR lkp_handling_office_code != OFFICE_ID 
				OR lkp_handling_office_descript != REPORT_OFFICE_NAME 
				OR lkp_handling_office_mgr != Handling_Office_Mgr 
				OR lkp_webconnect_user_id != USER_ID 
				OR lkp_claim_rep_email != Claim_Rep_Email_ID 
				OR lkp_handling_office_mgr_email != Handling_Office_Mgr_Email_ID 
				OR lkp_claim_rep_direct_automatic_pay_lmt != Claim_Rep_DIR_AUT_PMT 
				OR lkp_claim_rep_direct_automatic_reserve_lmt != Claim_Rep_DIR_AUT_RES 
				OR lkp_handling_office_mgr_direct_automatic_pay_lmt != Handling_Office_Mgr_DIR_AUT_PMT 
				OR lkp_handling_office_mgr_direct_automatic_reserve_lmt != Handling_Office_Mgr_DIR_AUT_RES 
				OR rtrim(ltrim(lkp_cost_center
					)
				) != rtrim(ltrim(cost_center_number
					)
				) 
				OR lkp_claim_rep_branch_num != adnm_adjustor_branch_number 
			),
			'UPDATE',
			'NO CHANGE'
		)
	) AS v_changed_flag,
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
	sysdate AS modified_date,
	'N/A' AS Not_Available_Field,
	0 AS ExceedAuthorityFlag,
	'N/A' AS ClaimsDesktopAuthorityType
	FROM EXP_Adjustor_Claim_Exists
	 -- Manually join with EXP_TARGET
	LEFT JOIN LKP_Claim_Representative_EDW
	ON LKP_Claim_Representative_EDW.claim_rep_key = EXP_TARGET.ADNM_ADJUSTOR_NBR
	LEFT JOIN LKP_Get_Limit_Reserve_Amt_For_Claim_Rep
	ON LKP_Get_Limit_Reserve_Amt_For_Claim_Rep.CAJ_USER_ID = EXP_TARGET.Adjuster_User_id
	LEFT JOIN LKP_Get_Limit_Reserve_Amt_For_handling_office_mgr
	ON LKP_Get_Limit_Reserve_Amt_For_handling_office_mgr.CAJ_USER_ID = EXP_TARGET.wbconnect_user_id_handling_office_mgr
),
FIL_Insert AS (
	SELECT
	lkp_claim_rep_id AS claim_rep_id, 
	lkp_claim_rep_ak_id AS claim_rep_ak_id, 
	Adjuster_No AS CAJ_EMP_CLIENT_ID, 
	claim_rep_name AS CICL_FULL_NM, 
	Not_Available_Field AS CICL_FST_NM, 
	Not_Available_Field AS CICL_MDL_NM, 
	Not_Available_Field AS CICL_LST_NM, 
	Not_Available_Field AS NM_PFX, 
	Not_Available_Field AS NM_SFX, 
	CO_Description, 
	DIVISION_CODE, 
	DIVISION_DESC, 
	DEPT_DESC, 
	DEPT_CODE, 
	DEPT_MGR, 
	OFFICE_ID AS CAJ_OFFICE_ID, 
	REPORT_OFFICE_NAME, 
	USER_ID, 
	Handling_Office_Mgr, 
	Claim_Rep_Email_ID, 
	Claim_Rep_DIR_AUT_RES, 
	Claim_Rep_DIR_AUT_PMT, 
	Handling_Office_Mgr_Email_ID, 
	Handling_Office_Mgr_DIR_AUT_RES, 
	Handling_Office_Mgr_DIR_AUT_PMT, 
	Crrnt_SnapSht_Flag, 
	AUDIT_ID, 
	SOURCE_SYSTEM_ID, 
	eff_from_date, 
	eff_to_date, 
	changed_flag, 
	created_date, 
	modified_date, 
	Not_Available_Field AS Division_Manager, 
	adnm_adjustor_branch_number, 
	cost_center_number, 
	ExceedAuthorityFlag, 
	ClaimsDesktopAuthorityType
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
SEQ_Claim_Representative_AK AS (
	CREATE SEQUENCE SEQ_Claim_Representative_AK
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	claim_rep_ak_id,
	SEQ_Claim_Representative_AK.NEXTVAL,
	-- *INF*: iif(isnull(claim_rep_ak_id)
	-- ,NEXTVAL
	-- ,claim_rep_ak_id)
	IFF(claim_rep_ak_id IS NULL,
		NEXTVAL,
		claim_rep_ak_id
	) AS out_claim_rep_ak_id,
	CAJ_EMP_CLIENT_ID,
	CICL_FULL_NM,
	CICL_FST_NM,
	CICL_LST_NM,
	CICL_MDL_NM,
	NM_PFX,
	NM_SFX,
	CO_Description,
	DIVISION_CODE,
	DIVISION_DESC,
	DEPT_DESC,
	DEPT_CODE,
	DEPT_MGR,
	CAJ_OFFICE_ID,
	REPORT_OFFICE_NAME,
	USER_ID,
	Handling_Office_Mgr,
	Claim_Rep_Email_ID,
	Claim_Rep_DIR_AUT_RES,
	Claim_Rep_DIR_AUT_PMT,
	Handling_Office_Mgr_Email_ID,
	Handling_Office_Mgr_DIR_AUT_RES,
	Handling_Office_Mgr_DIR_AUT_PMT,
	Crrnt_SnapSht_Flag,
	AUDIT_ID,
	SOURCE_SYSTEM_ID,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	Division_Manager,
	adnm_adjustor_branch_number,
	cost_center_number,
	ExceedAuthorityFlag,
	ClaimsDesktopAuthorityType
	FROM FIL_Insert
),
claim_representative_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative
	(claim_rep_ak_id, claim_rep_key, claim_rep_full_name, claim_rep_first_name, claim_rep_last_name, claim_rep_mid_name, claim_rep_name_prfx, claim_rep_name_sfx, co_descript, dvsn_code, dvsn_descript, dvsn_mgr, dept_descript, dept_name, dept_mgr, handling_office_code, handling_office_descript, handling_office_mgr, claim_rep_wbconnect_user_id, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_rep_email, handling_office_mgr_email, claim_rep_direct_automatic_pay_lmt, claim_rep_direct_automatic_reserve_lmt, handling_office_mgr_direct_automatic_pay_lmt, handling_office_mgr_direct_automatic_reserve_lmt, cost_center, claim_rep_branch_num, claim_rep_num, ExceedAuthorityFlag, ClaimsDesktopAuthorityType)
	SELECT 
	out_claim_rep_ak_id AS CLAIM_REP_AK_ID, 
	CAJ_EMP_CLIENT_ID AS CLAIM_REP_KEY, 
	CICL_FULL_NM AS CLAIM_REP_FULL_NAME, 
	CICL_FST_NM AS CLAIM_REP_FIRST_NAME, 
	CICL_LST_NM AS CLAIM_REP_LAST_NAME, 
	CICL_MDL_NM AS CLAIM_REP_MID_NAME, 
	NM_PFX AS CLAIM_REP_NAME_PRFX, 
	NM_SFX AS CLAIM_REP_NAME_SFX, 
	CO_Description AS CO_DESCRIPT, 
	DIVISION_CODE AS DVSN_CODE, 
	DIVISION_DESC AS DVSN_DESCRIPT, 
	Division_Manager AS DVSN_MGR, 
	DEPT_DESC AS DEPT_DESCRIPT, 
	DEPT_CODE AS DEPT_NAME, 
	DEPT_MGR AS DEPT_MGR, 
	CAJ_OFFICE_ID AS HANDLING_OFFICE_CODE, 
	REPORT_OFFICE_NAME AS HANDLING_OFFICE_DESCRIPT, 
	Handling_Office_Mgr AS HANDLING_OFFICE_MGR, 
	USER_ID AS CLAIM_REP_WBCONNECT_USER_ID, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	Claim_Rep_Email_ID AS CLAIM_REP_EMAIL, 
	Handling_Office_Mgr_Email_ID AS HANDLING_OFFICE_MGR_EMAIL, 
	Claim_Rep_DIR_AUT_PMT AS CLAIM_REP_DIRECT_AUTOMATIC_PAY_LMT, 
	Claim_Rep_DIR_AUT_RES AS CLAIM_REP_DIRECT_AUTOMATIC_RESERVE_LMT, 
	Handling_Office_Mgr_DIR_AUT_PMT AS HANDLING_OFFICE_MGR_DIRECT_AUTOMATIC_PAY_LMT, 
	Handling_Office_Mgr_DIR_AUT_RES AS HANDLING_OFFICE_MGR_DIRECT_AUTOMATIC_RESERVE_LMT, 
	cost_center_number AS COST_CENTER, 
	adnm_adjustor_branch_number AS CLAIM_REP_BRANCH_NUM, 
	CAJ_EMP_CLIENT_ID AS CLAIM_REP_NUM, 
	EXCEEDAUTHORITYFLAG, 
	CLAIMSDESKTOPAUTHORITYTYPE
	FROM EXP_Determine_AK
),
SQ_claim_representative AS (
	SELECT
	 a.claim_rep_id
	, a.claim_rep_key
	, a.eff_from_date
	, a.eff_to_date
	, a.source_sys_id 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative b
		WHERE b.crrnt_snpsht_flag = 1
		AND a.claim_rep_key = b.claim_rep_key
	      and a.source_sys_id = b.source_sys_id
		GROUP BY b.claim_rep_key, b.source_sys_id
		HAVING COUNT(*) > 1)
	ORDER BY a.claim_rep_key, a.source_sys_id, a.eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of eff_to_date='12/31/2100 23:59:59' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Expire_Rows AS (
	SELECT
	claim_rep_id,
	claim_rep_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, claim_rep_key = v_PREV_ROW_claim_rep_key and source_sys_id = v_PREV_ROW_source_sys_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		claim_rep_key = v_PREV_ROW_claim_rep_key 
		AND source_sys_id = v_PREV_ROW_source_sys_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_rep_key AS v_PREV_ROW_claim_rep_key,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	0 AS crrnt_Snpsht_flag,
	sysdate AS modified_date
	FROM SQ_claim_representative
),
FIL_Claim_Rep_Occurrence_Upd AS (
	SELECT
	claim_rep_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Claim_Rep_Occurrence AS (
	SELECT
	claim_rep_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_Snpsht_flag, 
	modified_date
	FROM FIL_Claim_Rep_Occurrence_Upd
),
claim_representative_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative AS T
	USING UPD_Claim_Rep_Occurrence AS S
	ON T.claim_rep_id = S.claim_rep_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_Snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),