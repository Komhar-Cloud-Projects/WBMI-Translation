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
LKP_gtam_wbadj_stage_cost_center AS (
	SELECT
	Cost_Center_Number,
	Adjuster_Code
	FROM (
		SELECT 
			Cost_Center_Number,
			Adjuster_Code
		FROM gtam_wbadj_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Adjuster_Code ORDER BY Cost_Center_Number) = 1
),
LKP_pms_adjuster_master_stage_branch_number AS (
	SELECT
	adnm_adjustor_branch_number,
	adnm_adjustor_nbr
	FROM (
		SELECT 
			adnm_adjustor_branch_number,
			adnm_adjustor_nbr
		FROM pms_adjuster_master_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adnm_adjustor_nbr ORDER BY adnm_adjustor_branch_number DESC) = 1
),
LKP_clt_ref_relation_client_id AS (
	SELECT
	cirf_ref_id,
	client_id,
	ref_typ_cd
	FROM (
		SELECT 
			cirf_ref_id,
			client_id,
			ref_typ_cd
		FROM clt_ref_relation_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id,ref_typ_cd ORDER BY cirf_ref_id DESC) = 1
),
SQ_ADJUSTER_TAB_STAGE AS (
	SELECT 
	ltrim(rtrim(a.CAJ_EMP_CLIENT_ID))
	, ltrim(rtrim(a.CAJ_USER_ID))
	, a.CAJ_DIR_AUT_RES
	, a.CAJ_DIR_AUT_PMT
	 , ltrim(rtrim(a.CAJ_OFFICE_ID))
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.ADJUSTER_TAB_STAGE a
),
EXP_Source AS (
	SELECT
	CAJ_EMP_CLIENT_ID,
	-- *INF*: IIF(IS_SPACES(CAJ_EMP_CLIENT_ID) OR ISNULL(CAJ_EMP_CLIENT_ID)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(CAJ_EMP_CLIENT_ID))
	-- )
	-- 
	IFF(LENGTH(CAJ_EMP_CLIENT_ID)>0 AND TRIM(CAJ_EMP_CLIENT_ID)='' 
		OR CAJ_EMP_CLIENT_ID IS NULL,
		'N/A',
		LTRIM(RTRIM(CAJ_EMP_CLIENT_ID
			)
		)
	) AS SOURCE_CAJ_EMP_CLIENT_ID,
	CAJ_USER_ID,
	-- *INF*: IIF(IS_SPACES(CAJ_USER_ID) OR ISNULL(CAJ_USER_ID)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(CAJ_USER_ID))
	-- )
	IFF(LENGTH(CAJ_USER_ID)>0 AND TRIM(CAJ_USER_ID)='' 
		OR CAJ_USER_ID IS NULL,
		'N/A',
		LTRIM(RTRIM(CAJ_USER_ID
			)
		)
	) AS SOURCE_CAJ_USER_ID,
	CAJ_OFFICE_ID,
	-- *INF*: IIF(IS_SPACES(CAJ_OFFICE_ID) OR ISNULL(CAJ_OFFICE_ID)  OR CAJ_OFFICE_ID = ''
	-- ,'N/A'
	-- ,LTRIM(RTRIM(CAJ_OFFICE_ID))
	-- )
	IFF(LENGTH(CAJ_OFFICE_ID)>0 AND TRIM(CAJ_OFFICE_ID)='' 
		OR CAJ_OFFICE_ID IS NULL 
		OR CAJ_OFFICE_ID = '',
		'N/A',
		LTRIM(RTRIM(CAJ_OFFICE_ID
			)
		)
	) AS SOURCE_CAJ_OFFICE_ID,
	CAJ_DIR_AUT_RES,
	CAJ_DIR_AUT_PMT
	FROM SQ_ADJUSTER_TAB_STAGE
),
EXP_DeterminePmsId AS (
	SELECT
	SOURCE_CAJ_EMP_CLIENT_ID,
	-- *INF*: IIF(SOURCE_CAJ_EMP_CLIENT_ID = 'N/A',SOURCE_CAJ_EMP_CLIENT_ID,:LKP.LKP_CLT_REF_RELATION_CLIENT_ID(SOURCE_CAJ_EMP_CLIENT_ID,'AJO '))
	-- -- if N/A do return N/A else perform lookup
	IFF(SOURCE_CAJ_EMP_CLIENT_ID = 'N/A',
		SOURCE_CAJ_EMP_CLIENT_ID,
		LKP_CLT_REF_RELATION_CLIENT_ID_SOURCE_CAJ_EMP_CLIENT_ID_AJO.cirf_ref_id
	) AS var_Ref_Id,
	-- *INF*: IIF(ISNULL(var_Ref_Id),'N/A',IIF(rtrim(ltrim(var_Ref_Id))='','N/A',var_Ref_Id))
	-- -- if N/A or blank return N/A else return the refid
	IFF(var_Ref_Id IS NULL,
		'N/A',
		IFF(rtrim(ltrim(var_Ref_Id
				)
			) = '',
			'N/A',
			var_Ref_Id
		)
	) AS var_Ref_Id_Response,
	-- *INF*: IIF(var_Ref_Id_Response='N/A','N/A',:LKP.LKP_GTAM_WBADJ_STAGE_COST_CENTER(var_Ref_Id_Response))
	IFF(var_Ref_Id_Response = 'N/A',
		'N/A',
		LKP_GTAM_WBADJ_STAGE_COST_CENTER_var_Ref_Id_Response.Cost_Center_Number
	) AS var_cost_center,
	-- *INF*: IIF(ISNULL(var_cost_center),'N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(var_cost_center))
	-- -- need to decrease lenght from 5 to 4
	IFF(var_cost_center IS NULL,
		'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(var_cost_center
		)
	) AS cost_center_out,
	-- *INF*: IIF(var_Ref_Id_Response='N/A' ,'N/A',TO_CHAR(:LKP.LKP_PMS_ADJUSTER_MASTER_STAGE_BRANCH_NUMBER(var_Ref_Id_Response)))
	IFF(var_Ref_Id_Response = 'N/A',
		'N/A',
		TO_CHAR(LKP_PMS_ADJUSTER_MASTER_STAGE_BRANCH_NUMBER_var_Ref_Id_Response.adnm_adjustor_branch_number
		)
	) AS var_branch_number,
	-- *INF*: IIF(isnull(var_branch_number),'N/A',LPAD(:UDF.DEFAULT_VALUE_FOR_STRINGS(var_branch_number),3,'0'))
	IFF(var_branch_number IS NULL,
		'N/A',
		LPAD(:UDF.DEFAULT_VALUE_FOR_STRINGS(var_branch_number
			), 3, '0'
		)
	) AS branch_number_out,
	var_Ref_Id_Response AS claim_rep_number
	FROM EXP_Source
	LEFT JOIN LKP_CLT_REF_RELATION_CLIENT_ID LKP_CLT_REF_RELATION_CLIENT_ID_SOURCE_CAJ_EMP_CLIENT_ID_AJO
	ON LKP_CLT_REF_RELATION_CLIENT_ID_SOURCE_CAJ_EMP_CLIENT_ID_AJO.client_id = SOURCE_CAJ_EMP_CLIENT_ID
	AND LKP_CLT_REF_RELATION_CLIENT_ID_SOURCE_CAJ_EMP_CLIENT_ID_AJO.ref_typ_cd = 'AJO '

	LEFT JOIN LKP_GTAM_WBADJ_STAGE_COST_CENTER LKP_GTAM_WBADJ_STAGE_COST_CENTER_var_Ref_Id_Response
	ON LKP_GTAM_WBADJ_STAGE_COST_CENTER_var_Ref_Id_Response.Adjuster_Code = var_Ref_Id_Response

	LEFT JOIN LKP_PMS_ADJUSTER_MASTER_STAGE_BRANCH_NUMBER LKP_PMS_ADJUSTER_MASTER_STAGE_BRANCH_NUMBER_var_Ref_Id_Response
	ON LKP_PMS_ADJUSTER_MASTER_STAGE_BRANCH_NUMBER_var_Ref_Id_Response.adnm_adjustor_nbr = var_Ref_Id_Response

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
LKP_Sup_Claim_Adjuster_Manager AS (
	SELECT
	wbconnect_user_id,
	claim_manager_code,
	adjuster_code
	FROM (
		SELECT 
		ltrim(rtrim(a.wbconnect_user_id)) as wbconnect_user_id
		, ltrim(rtrim(a.adjuster_code)) as adjuster_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_adjuster a
		where a.SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adjuster_code ORDER BY wbconnect_user_id) = 1
),
LKP_Adjuster_Tab_Stage AS (
	SELECT
	CAJ_EMP_CLIENT_ID,
	CAJ_DIR_AUT_RES,
	CAJ_DIR_AUT_PMT,
	wbconnect_user_id,
	CAJ_USER_ID
	FROM (
		SELECT 
		ltrim(rtrim(a.CAJ_EMP_CLIENT_ID)) as CAJ_EMP_CLIENT_ID
		, a.CAJ_DIR_AUT_RES as CAJ_DIR_AUT_RES
		, a.CAJ_DIR_AUT_PMT as CAJ_DIR_AUT_PMT
		, LTRIM(RTRIM(a.CAJ_USER_ID)) as CAJ_USER_ID 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.ADJUSTER_TAB_STAGE a
		where a.SOURCE_SYSTEM_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CAJ_USER_ID ORDER BY CAJ_EMP_CLIENT_ID) = 1
),
LKP_Claim_Party1_EDW AS (
	SELECT
	claim_party_first_name,
	claim_party_last_name,
	claim_party_mid_name,
	claim_party_name_prfx,
	claim_party_name_sfx,
	wbconnect_user_id_handling_office_mgr,
	CAJ_DIR_AUT_PMT,
	CAJ_DIR_AUT_RES,
	claim_party_key
	FROM (
		SELECT 
		ltrim(rtrim(a.claim_party_full_name)) as claim_party_full_name
		,ltrim(rtrim(a.claim_party_first_name)) as claim_party_first_name
		, ltrim(rtrim(a.claim_party_last_name)) as claim_party_last_name
		, ltrim(rtrim(a.claim_party_mid_name)) as claim_party_mid_name
		, ltrim(rtrim(a.claim_party_name_prfx)) as claim_party_name_prfx
		, ltrim(rtrim(a.claim_party_name_sfx)) as claim_party_name_sfx
		, a.claim_party_key as claim_party_key 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party a
		inner join @{pipeline().parameters.STAGING_DATABASE}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ADJUSTER_TAB_STAGE adj
		ON ltrim(rtrim(adj.CAJ_EMP_CLIENT_ID)) = a.claim_party_key 
		where a.crrnt_snpsht_flag = 1
		AND SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_first_name) = 1
),
LKP_Claim_Party_EDW AS (
	SELECT
	claim_party_full_name,
	claim_party_first_name,
	claim_party_last_name,
	claim_party_mid_name,
	claim_party_name_prfx,
	claim_party_name_sfx,
	claim_party_key
	FROM (
		SELECT 
		ltrim(rtrim(a.claim_party_full_name)) as claim_party_full_name
		,ltrim(rtrim(a.claim_party_first_name)) as claim_party_first_name
		, ltrim(rtrim(a.claim_party_last_name)) as claim_party_last_name
		, ltrim(rtrim(a.claim_party_mid_name)) as claim_party_mid_name
		, ltrim(rtrim(a.claim_party_name_prfx)) as claim_party_name_prfx
		, ltrim(rtrim(a.claim_party_name_sfx)) as claim_party_name_sfx
		, a.claim_party_key as claim_party_key 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party a
		inner join @{pipeline().parameters.STAGING_DATABASE}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ADJUSTER_TAB_STAGE adj
		ON ltrim(rtrim(adj.CAJ_EMP_CLIENT_ID)) = a.claim_party_key 
		where a.crrnt_snpsht_flag = 1
		AND SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_full_name) = 1
),
LKP_PMS_Adjuster_Master_Stage AS (
	SELECT
	adnm_name,
	adnm_adjustor_nbr
	FROM (
		SELECT 
			adnm_name,
			adnm_adjustor_nbr
		FROM pms_adjuster_master_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY adnm_adjustor_nbr ORDER BY adnm_name DESC) = 1
),
LKP_SecUsrsStage AS (
	SELECT
	SecUsrsStageId,
	SecUsrId,
	SOURCE_CAJ_USER_ID
	FROM (
		SELECT 
		SecUsrsStage.SecUsrsStageId as SecUsrsStageId, 
		ltrim(rtrim(SecUsrsStage.SecUsrId)) as SecUsrId 
		FROM 
		SecUsrsStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SecUsrId ORDER BY SecUsrsStageId) = 1
),
LKP_Sup_Claim_Dept_Stage AS (
	SELECT
	DEPT_CODE,
	DIVISION_CODE,
	DEPT_DESC
	FROM (
		SELECT 
		ltrim(rtrim(a.DIVISION_CODE)) as DIVISION_CODE
		, ltrim(rtrim(a.DEPT_DESC)) as DEPT_DESC
		, ltrim(rtrim(a.DEPT_CODE)) as DEPT_CODE 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SUP_CLAIM_DEPT_STAGE a
		where a.SOURCE_SYSTEM_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DEPT_CODE ORDER BY DEPT_CODE) = 1
),
LKP_Sup_Claim_Division_Stage AS (
	SELECT
	DIVISION_DESC,
	DIVISION_CODE
	FROM (
		SELECT 
		a.DIVISION_DESC as DIVISION_DESC
		, a.DIVISION_CODE as DIVISION_CODE 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SUP_CLAIM_DIVISION_STAGE a
		where a.SOURCE_SYSTEM_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DIVISION_CODE ORDER BY DIVISION_DESC) = 1
),
LKP_claims_desktop_access AS (
	SELECT
	MemberId,
	UserId,
	GroupSecurityId,
	GroupType,
	GroupName,
	SOURCE_CAJ_USER_ID
	FROM (
		select 
		A.memberid as MemberId, 
		ltrim(rtrim(A.userId)) as UserId, 
		B.groupsecurityid as GroupSecurityId, 
		C.grouptype as GroupType, 
		C.GroupName as GroupName
		from 
		AdmMembersStage A 
		inner join AdmMbrSecurityStage B on A.memberid=B.memberid
		inner join AdmSecurityGrpsStage C on B.groupsecurityid = C.groupsecurityid
		where C.grouptype in ('cr','cm','cu','cq','cts','ca')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY UserId ORDER BY MemberId) = 1
),
EXP_Target AS (
	SELECT
	EXP_Source.SOURCE_CAJ_EMP_CLIENT_ID AS CAJ_EMP_CLIENT_ID,
	LKP_Claim_Party_EDW.claim_party_full_name AS CICL_FULL_NAME,
	LKP_Claim_Party_EDW.claim_party_first_name AS CICL_FST_NM,
	LKP_Claim_Party_EDW.claim_party_mid_name AS CICL_MDL_NM,
	LKP_Claim_Party_EDW.claim_party_last_name AS CICL_LST_NM,
	LKP_Claim_Party_EDW.claim_party_name_prfx AS NM_PFX,
	LKP_Claim_Party_EDW.claim_party_name_sfx AS NM_SFX,
	'WBMI' AS CO_Description,
	LKP_Sup_Claim_Dept_Stage.DIVISION_CODE,
	LKP_Sup_Claim_Division_Stage.DIVISION_DESC,
	LKP_Sup_Claim_Dept_Stage.DEPT_DESC,
	LKP_Sup_Claim_Dept_Stage.DEPT_CODE,
	EXP_Source.SOURCE_CAJ_OFFICE_ID AS CAJ_OFFICE_ID,
	LKP_Sup_Claim_Report_Office_Stage.REPORT_OFFICE_NAME,
	EXP_Source.SOURCE_CAJ_USER_ID,
	LKP_Claim_Party1_EDW.claim_party_first_name AS Manager_first_name,
	-- *INF*: IIF(Manager_first_name = 'N/A'
	--       ,' '
	--       ,Manager_first_name)
	IFF(Manager_first_name = 'N/A',
		' ',
		Manager_first_name
	) AS v_Manager_first_name,
	LKP_Claim_Party1_EDW.claim_party_last_name AS Manager_last_name,
	-- *INF*: iif(Manager_last_name = 'N/A'
	--      ,' '
	--      ,Manager_last_name)
	IFF(Manager_last_name = 'N/A',
		' ',
		Manager_last_name
	) AS v_Manager_last_name,
	LKP_Claim_Party1_EDW.claim_party_mid_name AS Manager_mid_name,
	-- *INF*: iif(Manager_mid_name = 'N/A'
	--      ,' '
	--      ,Manager_mid_name)
	IFF(Manager_mid_name = 'N/A',
		' ',
		Manager_mid_name
	) AS v_Manager_mid_name,
	LKP_Claim_Party1_EDW.claim_party_name_prfx AS Manager_name_prfx,
	LKP_Claim_Party1_EDW.claim_party_name_sfx AS Manager_name_sfx,
	-- *INF*: IIF(IS_SPACES(v_Manager_first_name || v_Manager_mid_name || v_Manager_last_name) = 1
	--        ,'N/A',IIF(Manager_mid_name ='N/A',LTRIM(RTRIM(v_Manager_first_name || v_Manager_mid_name || v_Manager_last_name)),
	--        LTRIM(RTRIM(v_Manager_first_name || ' ' || v_Manager_mid_name || ' ' || v_Manager_last_name))))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --IIF(IS_SPACES(v_Manager_first_name || v_Manager_mid_name || v_Manager_last_name) = 1
	--    --    ,'N/A'
	--       -- ,LTRIM(RTRIM(v_Manager_first_name || ' ' || v_Manager_mid_name || ' ' || v_Manager_last_name)))
	IFF(LENGTH(v_Manager_first_name || v_Manager_mid_name || v_Manager_last_name)>0 AND TRIM(v_Manager_first_name || v_Manager_mid_name || v_Manager_last_name)='' = 1,
		'N/A',
		IFF(Manager_mid_name = 'N/A',
			LTRIM(RTRIM(v_Manager_first_name || v_Manager_mid_name || v_Manager_last_name
				)
			),
			LTRIM(RTRIM(v_Manager_first_name || ' ' || v_Manager_mid_name || ' ' || v_Manager_last_name
				)
			)
		)
	) AS CICL_LNG_NM,
	LKP_PMS_Adjuster_Master_Stage.adnm_name AS in_adnm_name,
	-- *INF*: IIF(ISNULL(in_adnm_name),'N/A',LTRIM(RTRIM(in_adnm_name)))
	-- 
	-- --converting to variable and routing through a diff output port so the length will match target length and not force unnecessary updates.
	IFF(in_adnm_name IS NULL,
		'N/A',
		LTRIM(RTRIM(in_adnm_name
			)
		)
	) AS v_dept_mgr,
	v_dept_mgr AS dept_mgr_out,
	EXP_Source.SOURCE_CAJ_USER_ID AS wbconnect_user_id_claim_rep,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(wbconnect_user_id_claim_rep))
	-- ,'N/A'
	-- ,:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(wbconnect_user_id_claim_rep))
	IFF(LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_claim_rep.EMAIL IS NULL,
		'N/A',
		LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_claim_rep.EMAIL
	) AS Claim_Rep_Email,
	LKP_Claim_Party1_EDW.wbconnect_user_id_handling_office_mgr AS wbconnect_user_id_handling_officer_mgr,
	-- *INF*: IIF(ISNULL(:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(wbconnect_user_id_handling_officer_mgr))
	-- ,'N/A'
	-- ,:LKP.LKP_SUP_CLAIM_STAFF_EMAIL(wbconnect_user_id_handling_officer_mgr))
	IFF(LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_officer_mgr.EMAIL IS NULL,
		'N/A',
		LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_officer_mgr.EMAIL
	) AS Handling_Office_Mgr_Email,
	EXP_Source.CAJ_DIR_AUT_RES AS CLAIM_REP_DIR_AUT_RES,
	EXP_Source.CAJ_DIR_AUT_PMT AS CLAIM_REP_DIR_AUT_PMT,
	LKP_Claim_Party1_EDW.CAJ_DIR_AUT_PMT AS MGR_DIR_AUT_PMT,
	LKP_Claim_Party1_EDW.CAJ_DIR_AUT_RES AS MGR_DIR_AUT_RES,
	-- *INF*: rtrim(ltrim(:LKP.LKP_CLT_REF_RELATION_CLIENT_ID(CAJ_EMP_CLIENT_ID,'AJO')))
	rtrim(ltrim(LKP_CLT_REF_RELATION_CLIENT_ID_CAJ_EMP_CLIENT_ID_AJO.cirf_ref_id
		)
	) AS v_adjustor_ref_id,
	EXP_DeterminePmsId.cost_center_out AS cost_center,
	EXP_DeterminePmsId.branch_number_out AS branch_number,
	EXP_DeterminePmsId.claim_rep_number,
	LKP_SecUsrsStage.SecUsrsStageId,
	-- *INF*: IIF(ISNULL(SecUsrsStageId),'F','T')
	IFF(SecUsrsStageId IS NULL,
		'F',
		'T'
	) AS SecUsrsStageId_out,
	LKP_claims_desktop_access.GroupType,
	-- *INF*: DECODE( TRUE,
	-- ltrim(rtrim(GroupType))='cr','Read Only',
	-- ltrim(rtrim(GroupType))='cm','Manager',
	-- ltrim(rtrim(GroupType))='cu','Change',
	-- ltrim(rtrim(GroupType))='cq','Quest',
	-- ltrim(rtrim(GroupType))='cts','Transformation Station',
	-- ltrim(rtrim(GroupType))='ca','Agent'
	-- ,'N/A'
	-- )
	-- 
	-- --Determine and persist the authority type based on the group type:
	-- --cr = Read Only
	-- --cm = Manager
	-- --cu = Change
	-- --cq = Quest
	-- --cts = Transformation Station
	-- --ca = Agent
	DECODE(TRUE,
		ltrim(rtrim(GroupType
			)
		) = 'cr', 'Read Only',
		ltrim(rtrim(GroupType
			)
		) = 'cm', 'Manager',
		ltrim(rtrim(GroupType
			)
		) = 'cu', 'Change',
		ltrim(rtrim(GroupType
			)
		) = 'cq', 'Quest',
		ltrim(rtrim(GroupType
			)
		) = 'cts', 'Transformation Station',
		ltrim(rtrim(GroupType
			)
		) = 'ca', 'Agent',
		'N/A'
	) AS v_GroupType,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_GroupType)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(v_GroupType
	) AS GroupName_out
	FROM EXP_DeterminePmsId
	 -- Manually join with EXP_Source
	LEFT JOIN LKP_Claim_Party1_EDW
	ON LKP_Claim_Party1_EDW.claim_party_key = LKP_Adjuster_Tab_Stage.CAJ_EMP_CLIENT_ID
	LEFT JOIN LKP_Claim_Party_EDW
	ON LKP_Claim_Party_EDW.claim_party_key = EXP_Source.SOURCE_CAJ_EMP_CLIENT_ID
	LEFT JOIN LKP_PMS_Adjuster_Master_Stage
	ON LKP_PMS_Adjuster_Master_Stage.adnm_adjustor_nbr = LKP_Sup_Claim_Report_Office_Stage.DIRECTOR_CODE
	LEFT JOIN LKP_SecUsrsStage
	ON LKP_SecUsrsStage.SecUsrId = EXP_Source.SOURCE_CAJ_USER_ID
	LEFT JOIN LKP_Sup_Claim_Dept_Stage
	ON LKP_Sup_Claim_Dept_Stage.DEPT_CODE = LKP_Sup_Claim_Report_Office_Stage.DEPT_CODE
	LEFT JOIN LKP_Sup_Claim_Division_Stage
	ON LKP_Sup_Claim_Division_Stage.DIVISION_CODE = LKP_Sup_Claim_Dept_Stage.DIVISION_CODE
	LEFT JOIN LKP_Sup_Claim_Report_Office_Stage
	ON LKP_Sup_Claim_Report_Office_Stage.REPORT_OFFICE_CODE = EXP_Source.SOURCE_CAJ_OFFICE_ID
	LEFT JOIN LKP_claims_desktop_access
	ON LKP_claims_desktop_access.UserId = EXP_Source.SOURCE_CAJ_USER_ID
	LEFT JOIN LKP_SUP_CLAIM_STAFF_EMAIL LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_claim_rep
	ON LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_claim_rep.WBCONNECT_USER_ID = wbconnect_user_id_claim_rep

	LEFT JOIN LKP_SUP_CLAIM_STAFF_EMAIL LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_officer_mgr
	ON LKP_SUP_CLAIM_STAFF_EMAIL_wbconnect_user_id_handling_officer_mgr.WBCONNECT_USER_ID = wbconnect_user_id_handling_officer_mgr

	LEFT JOIN LKP_CLT_REF_RELATION_CLIENT_ID LKP_CLT_REF_RELATION_CLIENT_ID_CAJ_EMP_CLIENT_ID_AJO
	ON LKP_CLT_REF_RELATION_CLIENT_ID_CAJ_EMP_CLIENT_ID_AJO.client_id = CAJ_EMP_CLIENT_ID
	AND LKP_CLT_REF_RELATION_CLIENT_ID_CAJ_EMP_CLIENT_ID_AJO.ref_typ_cd = 'AJO'

),
LKP_Claim_Representative_EDW AS (
	SELECT
	claim_rep_id,
	claim_rep_ak_id,
	claim_rep_full_name,
	claim_rep_first_name,
	claim_rep_last_name,
	claim_rep_mid_name,
	claim_rep_name_prfx,
	claim_rep_name_sfx,
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
	handling_office_mgr_email,
	claim_rep_direct_automatic_pay_lmt,
	claim_rep_direct_automatic_reserve_lmt,
	handling_office_mgr_direct_automatic_pay_lmt,
	handling_office_mgr_direct_automatic_reserver_lmt,
	cost_center,
	claim_rep_branch_num,
	claim_rep_num,
	ExceedAuthorityFlag,
	ClaimsDesktopAuthorityType,
	CAJ_EMP_CLIENT_ID,
	claim_rep_key
	FROM (
		SELECT 
		a.claim_rep_id as claim_rep_id
		, a.claim_rep_ak_id as claim_rep_ak_id
		, ltrim(rtrim(a.claim_rep_full_name)) as claim_rep_full_name
		, ltrim(rtrim(a.claim_rep_first_name)) as claim_rep_first_name
		, ltrim(rtrim(a.claim_rep_last_name)) as claim_rep_last_name
		, ltrim(rtrim(a.claim_rep_mid_name)) as claim_rep_mid_name
		, ltrim(rtrim(a.claim_rep_name_prfx)) as claim_rep_name_prfx
		, ltrim(rtrim(a.claim_rep_name_sfx)) as claim_rep_name_sfx
		, ltrim(rtrim(a.dvsn_code)) as dvsn_code
		, ltrim(rtrim(a.dvsn_descript)) as dvsn_descript
		, ltrim(rtrim(a.dept_descript)) as dept_descript
		, ltrim(rtrim(a.dept_name)) as dept_name
		, ltrim(rtrim(a.dept_mgr)) as dept_mgr
		, ltrim(rtrim(a.handling_office_code)) as handling_office_code
		, ltrim(rtrim(a.handling_office_descript)) as handling_office_descript
		, ltrim(rtrim(a.handling_office_mgr)) as handling_office_mgr
		, ltrim(rtrim(a.claim_rep_wbconnect_user_id)) as claim_rep_wbconnect_user_id
		, ltrim(rtrim(a.claim_rep_email))  AS claim_rep_email
		, ltrim(rtrim(a.handling_office_mgr_email)) AS handling_office_mgr_email
		, a.claim_rep_direct_automatic_pay_lmt  AS claim_rep_direct_automatic_pay_lmt
		, a.claim_rep_direct_automatic_reserve_lmt  AS claim_rep_direct_automatic_reserve_lmt
		, a.handling_office_mgr_direct_automatic_pay_lmt  AS handling_office_mgr_direct_automatic_pay_lmt
		, a.handling_office_mgr_direct_automatic_reserve_lmt AS handling_office_mgr_direct_automatic_reserver_lmt
		,a.cost_center as cost_center
		,a.claim_rep_branch_num as claim_rep_branch_num
		,a.claim_rep_num as claim_rep_num
		, a.claim_rep_key    AS claim_rep_key
		,a.ExceedAuthorityFlag as ExceedAuthorityFlag
		,a.ClaimsDesktopAuthorityType as ClaimsDesktopAuthorityType
		FROM claim_representative a
		where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and	
		a.claim_rep_id in (select max(b.claim_rep_id) from claim_representative b
		where b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and b.crrnt_snpsht_flag = 1
		group by b.claim_rep_key)
		order by a.claim_rep_key
		
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100 23:59:59'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key ORDER BY claim_rep_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Claim_Representative_EDW.claim_rep_id,
	LKP_Claim_Representative_EDW.claim_rep_ak_id,
	LKP_Claim_Representative_EDW.claim_rep_full_name AS lkp_claim_rep_full_name,
	LKP_Claim_Representative_EDW.claim_rep_first_name AS lkp_claim_rep_first_name,
	LKP_Claim_Representative_EDW.claim_rep_last_name AS lkp_claim_rep_last_name,
	LKP_Claim_Representative_EDW.claim_rep_mid_name AS lkp_claim_rep_mid_name,
	LKP_Claim_Representative_EDW.claim_rep_name_prfx AS lkp_claim_rep_name_prfx,
	LKP_Claim_Representative_EDW.claim_rep_name_sfx AS lkp_claim_rep_name_sfx,
	LKP_Claim_Representative_EDW.dvsn_code AS lkp_dvsn_code,
	LKP_Claim_Representative_EDW.dvsn_descript AS lkp_dvsn_descript,
	LKP_Claim_Representative_EDW.dept_descript AS lkp_dept_descript,
	LKP_Claim_Representative_EDW.dept_name AS lkp_dept_name,
	LKP_Claim_Representative_EDW.dept_mgr AS lkp_dept_mgr,
	LKP_Claim_Representative_EDW.handling_office_code,
	LKP_Claim_Representative_EDW.handling_office_descript,
	LKP_Claim_Representative_EDW.handling_office_mgr,
	LKP_Claim_Representative_EDW.claim_rep_wbconnect_user_id,
	LKP_Claim_Representative_EDW.claim_rep_email AS claim_rep_email1,
	LKP_Claim_Representative_EDW.claim_rep_direct_automatic_pay_lmt,
	LKP_Claim_Representative_EDW.claim_rep_direct_automatic_reserve_lmt,
	LKP_Claim_Representative_EDW.handling_office_mgr_email,
	LKP_Claim_Representative_EDW.handling_office_mgr_direct_automatic_pay_lmt,
	LKP_Claim_Representative_EDW.handling_office_mgr_direct_automatic_reserver_lmt,
	LKP_Claim_Representative_EDW.cost_center AS lkp_cost_center,
	LKP_Claim_Representative_EDW.claim_rep_branch_num AS lkp_claim_rep_branch_num,
	LKP_Claim_Representative_EDW.claim_rep_num AS lkp_claim_rep_num,
	LKP_Claim_Representative_EDW.ExceedAuthorityFlag AS lkp_ExceedAuthorityFlag,
	LKP_Claim_Representative_EDW.ClaimsDesktopAuthorityType AS lkp_ClaimsDesktopAuthorityType,
	EXP_Target.CICL_FULL_NAME,
	EXP_Target.CICL_FST_NM,
	EXP_Target.CICL_MDL_NM,
	EXP_Target.CICL_LST_NM,
	EXP_Target.NM_PFX,
	EXP_Target.NM_SFX,
	EXP_Target.DIVISION_CODE,
	EXP_Target.DIVISION_DESC,
	EXP_Target.DEPT_DESC,
	EXP_Target.DEPT_CODE,
	EXP_Target.dept_mgr_out AS DEPT_MGR,
	EXP_Target.CAJ_OFFICE_ID,
	EXP_Target.REPORT_OFFICE_NAME,
	EXP_Target.SOURCE_CAJ_USER_ID,
	EXP_Target.CICL_LNG_NM,
	EXP_Target.Claim_Rep_Email,
	EXP_Target.Handling_Office_Mgr_Email AS Handling_Office_Mgr_Email1,
	EXP_Target.CLAIM_REP_DIR_AUT_RES,
	EXP_Target.CLAIM_REP_DIR_AUT_PMT,
	EXP_Target.MGR_DIR_AUT_PMT,
	EXP_Target.MGR_DIR_AUT_RES,
	EXP_Target.cost_center,
	EXP_Target.branch_number AS claim_rep_branch_num,
	EXP_Target.claim_rep_number AS claim_rep_num,
	EXP_Target.SecUsrsStageId_out AS ExceedAuthorityFlag,
	EXP_Target.GroupName_out AS ClaimsDesktopAuthorityType,
	-- *INF*: iif(isnull(claim_rep_id)
	-- ,'NEW'
	-- ,iif(ltrim(rtrim(lkp_claim_rep_first_name)) != ltrim(rtrim(CICL_FST_NM))  
	-- OR ltrim(rtrim(lkp_claim_rep_last_name)) != ltrim(rtrim(CICL_LST_NM)) 
	-- OR ltrim(rtrim(lkp_claim_rep_mid_name)) != ltrim(rtrim(CICL_MDL_NM)) 
	-- OR ltrim(rtrim(lkp_claim_rep_name_prfx)) != ltrim(rtrim(NM_PFX)) 
	-- OR ltrim(rtrim(lkp_claim_rep_name_sfx)) != ltrim(rtrim(NM_SFX))
	-- OR ltrim(rtrim(lkp_dvsn_code)) != ltrim(rtrim(DIVISION_CODE)) 
	-- OR ltrim(rtrim(lkp_dvsn_descript)) != ltrim(rtrim(DIVISION_DESC))
	-- OR ltrim(rtrim(lkp_dept_descript)) != ltrim(rtrim(DEPT_DESC))
	-- OR ltrim(rtrim(lkp_dept_name)) != ltrim(rtrim(DEPT_CODE))
	-- OR ltrim(rtrim(lkp_dept_mgr)) != ltrim(rtrim(DEPT_MGR))
	-- OR ltrim(rtrim(handling_office_code)) != ltrim(rtrim(CAJ_OFFICE_ID))  
	-- OR ltrim(rtrim(handling_office_descript)) != ltrim(rtrim(REPORT_OFFICE_NAME))
	-- OR ltrim(rtrim(handling_office_mgr)) != ltrim(rtrim(CICL_LNG_NM))
	-- OR ltrim(rtrim(claim_rep_wbconnect_user_id)) != ltrim(rtrim(SOURCE_CAJ_USER_ID))
	-- or  ltrim(rtrim(claim_rep_email1))  !=  ltrim(rtrim(Claim_Rep_Email))
	-- or  ltrim(rtrim(handling_office_mgr_email)) !=  ltrim(rtrim(Handling_Office_Mgr_Email1))
	-- or claim_rep_direct_automatic_pay_lmt != CLAIM_REP_DIR_AUT_PMT
	--  OR claim_rep_direct_automatic_reserve_lmt != CLAIM_REP_DIR_AUT_RES
	--  OR handling_office_mgr_direct_automatic_pay_lmt !=MGR_DIR_AUT_PMT
	--  OR handling_office_mgr_direct_automatic_reserver_lmt != MGR_DIR_AUT_RES
	-- OR ltrim(rtrim(lkp_cost_center)) != ltrim(rtrim(cost_center))
	-- OR ltrim(rtrim(lkp_claim_rep_branch_num)) != ltrim(rtrim(claim_rep_branch_num))
	-- OR ltrim(rtrim(lkp_claim_rep_num)) != ltrim(rtrim(claim_rep_num))
	-- OR ltrim(rtrim(lkp_ExceedAuthorityFlag)) !=ltrim(rtrim(ExceedAuthorityFlag))
	-- OR ltrim(rtrim(lkp_ClaimsDesktopAuthorityType)) !=ltrim(rtrim(ClaimsDesktopAuthorityType))
	-- ,'UPDATE'
	-- ,'NOCHANGE'))
	IFF(claim_rep_id IS NULL,
		'NEW',
		IFF(ltrim(rtrim(lkp_claim_rep_first_name
				)
			) != ltrim(rtrim(CICL_FST_NM
				)
			) 
			OR ltrim(rtrim(lkp_claim_rep_last_name
				)
			) != ltrim(rtrim(CICL_LST_NM
				)
			) 
			OR ltrim(rtrim(lkp_claim_rep_mid_name
				)
			) != ltrim(rtrim(CICL_MDL_NM
				)
			) 
			OR ltrim(rtrim(lkp_claim_rep_name_prfx
				)
			) != ltrim(rtrim(NM_PFX
				)
			) 
			OR ltrim(rtrim(lkp_claim_rep_name_sfx
				)
			) != ltrim(rtrim(NM_SFX
				)
			) 
			OR ltrim(rtrim(lkp_dvsn_code
				)
			) != ltrim(rtrim(DIVISION_CODE
				)
			) 
			OR ltrim(rtrim(lkp_dvsn_descript
				)
			) != ltrim(rtrim(DIVISION_DESC
				)
			) 
			OR ltrim(rtrim(lkp_dept_descript
				)
			) != ltrim(rtrim(DEPT_DESC
				)
			) 
			OR ltrim(rtrim(lkp_dept_name
				)
			) != ltrim(rtrim(DEPT_CODE
				)
			) 
			OR ltrim(rtrim(lkp_dept_mgr
				)
			) != ltrim(rtrim(DEPT_MGR
				)
			) 
			OR ltrim(rtrim(handling_office_code
				)
			) != ltrim(rtrim(CAJ_OFFICE_ID
				)
			) 
			OR ltrim(rtrim(handling_office_descript
				)
			) != ltrim(rtrim(REPORT_OFFICE_NAME
				)
			) 
			OR ltrim(rtrim(handling_office_mgr
				)
			) != ltrim(rtrim(CICL_LNG_NM
				)
			) 
			OR ltrim(rtrim(claim_rep_wbconnect_user_id
				)
			) != ltrim(rtrim(SOURCE_CAJ_USER_ID
				)
			) 
			OR ltrim(rtrim(claim_rep_email1
				)
			) != ltrim(rtrim(Claim_Rep_Email
				)
			) 
			OR ltrim(rtrim(handling_office_mgr_email
				)
			) != ltrim(rtrim(Handling_Office_Mgr_Email1
				)
			) 
			OR claim_rep_direct_automatic_pay_lmt != CLAIM_REP_DIR_AUT_PMT 
			OR claim_rep_direct_automatic_reserve_lmt != CLAIM_REP_DIR_AUT_RES 
			OR handling_office_mgr_direct_automatic_pay_lmt != MGR_DIR_AUT_PMT 
			OR handling_office_mgr_direct_automatic_reserver_lmt != MGR_DIR_AUT_RES 
			OR ltrim(rtrim(lkp_cost_center
				)
			) != ltrim(rtrim(cost_center
				)
			) 
			OR ltrim(rtrim(lkp_claim_rep_branch_num
				)
			) != ltrim(rtrim(claim_rep_branch_num
				)
			) 
			OR ltrim(rtrim(lkp_claim_rep_num
				)
			) != ltrim(rtrim(claim_rep_num
				)
			) 
			OR ltrim(rtrim(lkp_ExceedAuthorityFlag
				)
			) != ltrim(rtrim(ExceedAuthorityFlag
				)
			) 
			OR ltrim(rtrim(lkp_ClaimsDesktopAuthorityType
				)
			) != ltrim(rtrim(ClaimsDesktopAuthorityType
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	EXP_Target.CAJ_EMP_CLIENT_ID,
	EXP_Target.CO_Description,
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
	'N/A' AS Division_Manager
	FROM EXP_Target
	LEFT JOIN LKP_Claim_Representative_EDW
	ON LKP_Claim_Representative_EDW.claim_rep_key = EXP_Target.CAJ_EMP_CLIENT_ID
),
FIL_Insert AS (
	SELECT
	claim_rep_id, 
	claim_rep_ak_id, 
	CAJ_EMP_CLIENT_ID, 
	CICL_FULL_NAME, 
	CICL_FST_NM, 
	CICL_MDL_NM, 
	CICL_LST_NM, 
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
	SOURCE_CAJ_USER_ID, 
	CICL_LNG_NM, 
	Claim_Rep_Email, 
	Handling_Office_Mgr_Email1 AS Handling_Office_Mgr_Email, 
	CLAIM_REP_DIR_AUT_RES, 
	CLAIM_REP_DIR_AUT_PMT, 
	MGR_DIR_AUT_PMT, 
	MGR_DIR_AUT_RES, 
	Crrnt_SnapSht_Flag, 
	AUDIT_ID, 
	SOURCE_SYSTEM_ID, 
	eff_from_date, 
	eff_to_date, 
	changed_flag, 
	created_date, 
	modified_date, 
	Division_Manager, 
	cost_center, 
	claim_rep_branch_num, 
	claim_rep_num, 
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
	CICL_FULL_NAME,
	CICL_FST_NM,
	CICL_MDL_NM,
	CICL_LST_NM,
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
	SOURCE_CAJ_USER_ID,
	CICL_LNG_NM,
	Claim_Rep_Email,
	Handling_Office_Mgr_Email,
	CLAIM_REP_DIR_AUT_RES,
	CLAIM_REP_DIR_AUT_PMT,
	MGR_DIR_AUT_PMT,
	MGR_DIR_AUT_RES,
	Crrnt_SnapSht_Flag,
	AUDIT_ID,
	SOURCE_SYSTEM_ID,
	eff_from_date,
	eff_to_date,
	changed_flag,
	created_date,
	modified_date,
	Division_Manager,
	cost_center,
	claim_rep_branch_num,
	claim_rep_num,
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
	CICL_FULL_NAME AS CLAIM_REP_FULL_NAME, 
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
	CICL_LNG_NM AS HANDLING_OFFICE_MGR, 
	SOURCE_CAJ_USER_ID AS CLAIM_REP_WBCONNECT_USER_ID, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	Claim_Rep_Email AS CLAIM_REP_EMAIL, 
	Handling_Office_Mgr_Email AS HANDLING_OFFICE_MGR_EMAIL, 
	CLAIM_REP_DIR_AUT_PMT AS CLAIM_REP_DIRECT_AUTOMATIC_PAY_LMT, 
	CLAIM_REP_DIR_AUT_RES AS CLAIM_REP_DIRECT_AUTOMATIC_RESERVE_LMT, 
	MGR_DIR_AUT_PMT AS HANDLING_OFFICE_MGR_DIRECT_AUTOMATIC_PAY_LMT, 
	MGR_DIR_AUT_RES AS HANDLING_OFFICE_MGR_DIRECT_AUTOMATIC_RESERVE_LMT, 
	COST_CENTER, 
	CLAIM_REP_BRANCH_NUM, 
	CLAIM_REP_NUM, 
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