WITH
LKP_Underwriter AS (
	SELECT
	uw_ak_id,
	uw_code
	FROM (
		SELECT u.uw_ak_id as uw_ak_id, 
		u.uw_code as uw_code FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter u
		Where u.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY uw_code ORDER BY uw_ak_id) = 1
),
LKP_Underwriter_Manager AS (
	SELECT
	uw_mgr_ak_id,
	source_uw_mgr_id
	FROM (
		SELECT u.uw_mgr_ak_id as uw_mgr_ak_id, u.source_uw_mgr_id as source_uw_mgr_id 
		FROM 
		dbo.underwriter_manager u
		Where u.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY source_uw_mgr_id ORDER BY uw_mgr_ak_id) = 1
),
SQ_underwriter_terr_stage AS (
	select A.territory_code as territory_code, A.uw_code as uw_code, A.uw_mgr_id as uw_mgr_id, A.SOURCE_SYSTEM_ID as SOURCE_SYSTEM_ID 
	FROM
	(
	 SELECT  underwriter_terr_stage.territory_code, underwriter_terr_stage.uw_code, underwriter_terr_stage.uw_mgr_id, underwriter_terr_stage.SOURCE_SYSTEM_ID 
	, row_number() over (partition by underwriter_terr_stage.territory_code, underwriter_terr_stage.uw_code,underwriter_terr_stage.uw_mgr_id order by underwriter_terr_stage.uw_mgr_id) as rn
	FROM
	 underwriter_terr_stage) A
	 where A.rn=1
),
EXP_DefaultValues AS (
	SELECT
	territory_code AS in_territory_code,
	uw_code AS in_uw_code,
	uw_mgr_id AS in_uw_mgr_id,
	-- *INF*: iif(isnull(in_rsm_id),'N/A',iif(IS_SPACES(in_rsm_id),'N/A',in_rsm_id))
	IFF(in_rsm_id IS NULL,
		'N/A',
		IFF(LENGTH(in_rsm_id)>0 AND TRIM(in_rsm_id)='',
			'N/A',
			in_rsm_id
		)
	) AS rsm_id,
	-- *INF*: to_char(to_integer(in_territory_code))
	-- --substr(in_territory_code,1,(INSTR(in_territory_code,'.',1,1)-1))
	-- 
	-- 
	-- 
	to_char(CAST(in_territory_code AS INTEGER)
	) AS territory_code,
	-- *INF*: iif(isnull(in_uw_code),'N/A',iif(IS_SPACES(in_uw_code),'N/A',in_uw_code))
	IFF(in_uw_code IS NULL,
		'N/A',
		IFF(LENGTH(in_uw_code)>0 AND TRIM(in_uw_code)='',
			'N/A',
			in_uw_code
		)
	) AS uw_code,
	-- *INF*: iif(isnull(in_uw_mgr_id),'N/A',iif(IS_SPACES(in_uw_mgr_id),'N/A',in_uw_mgr_id))
	IFF(in_uw_mgr_id IS NULL,
		'N/A',
		IFF(LENGTH(in_uw_mgr_id)>0 AND TRIM(in_uw_mgr_id)='',
			'N/A',
			in_uw_mgr_id
		)
	) AS uw_mgr_id,
	SOURCE_SYSTEM_ID
	FROM SQ_underwriter_terr_stage
),
EXP_lookupvalues AS (
	SELECT
	territory_code AS in_territory_code,
	-- *INF*: in_territory_code || '  '
	-- --Concat  spaces to have a correct lookup to underwriter_terr_table
	-- 
	-- 
	in_territory_code || '  ' AS v_territory_code,
	v_territory_code AS territory_code,
	uw_code,
	-- *INF*: :LKP.LKP_UNDERWRITER(uw_code)
	LKP_UNDERWRITER_uw_code.uw_ak_id AS out_uw_ak_id,
	uw_mgr_id AS source_uw_mgr_id,
	-- *INF*: TO_INTEGER(source_uw_mgr_id)
	CAST(source_uw_mgr_id AS INTEGER) AS lkp_uw_mgr_id,
	-- *INF*: :LKP.LKP_UNDERWRITER_MANAGER(lkp_uw_mgr_id)
	LKP_UNDERWRITER_MANAGER_lkp_uw_mgr_id.uw_mgr_ak_id AS out_uw_mgr_ak_id,
	SOURCE_SYSTEM_ID
	FROM EXP_DefaultValues
	LEFT JOIN LKP_UNDERWRITER LKP_UNDERWRITER_uw_code
	ON LKP_UNDERWRITER_uw_code.uw_code = uw_code

	LEFT JOIN LKP_UNDERWRITER_MANAGER LKP_UNDERWRITER_MANAGER_lkp_uw_mgr_id
	ON LKP_UNDERWRITER_MANAGER_lkp_uw_mgr_id.source_uw_mgr_id = lkp_uw_mgr_id

),
EXP_Detectchanges AS (
	SELECT
	territory_code,
	out_uw_ak_id,
	out_uw_mgr_ak_id,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	SOURCE_SYSTEM_ID,
	sysdate AS created_date,
	sysdate AS modified_date,
	-- *INF*: IIF(ISNULL(v_uw_terr_ak_id) OR v_uw_terr_ak_id=0,1,v_uw_terr_ak_id +1)
	IFF(v_uw_terr_ak_id IS NULL 
		OR v_uw_terr_ak_id = 0,
		1,
		v_uw_terr_ak_id + 1
	) AS v_uw_terr_ak_id,
	v_uw_terr_ak_id AS out_uw_terr_ak_id
	FROM EXP_lookupvalues
),
underwriter_territory_Insert AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.underwriter_territory;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.underwriter_territory
	(uw_terr_ak_id, uw_ak_id, uw_mgr_ak_id, terr_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date)
	SELECT 
	out_uw_terr_ak_id AS UW_TERR_AK_ID, 
	out_uw_ak_id AS UW_AK_ID, 
	out_uw_mgr_ak_id AS UW_MGR_AK_ID, 
	territory_code AS TERR_CODE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_Detectchanges
),