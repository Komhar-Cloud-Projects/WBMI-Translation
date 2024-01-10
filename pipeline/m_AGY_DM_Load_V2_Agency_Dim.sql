WITH
SQ_agency AS (

------------ PRE SQL ----------
truncate table dbo.work_v2_agency

insert dbo.work_v2_agency (agency_ak_id ,
                    EFF_FROM_DATE)
SELECT AGENCY_AK_ID, EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY WHERE CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
UNION
SELECT AGENCY_AK_ID, EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY_ADDRESS WHERE CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
UNION
SELECT AGENCY_AK_ID, EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY_UNDERWRITER WHERE CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
UNION
SELECT B.AGENCY_AK_ID,A.EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER A, @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY_UNDERWRITER B
WHERE A.UW_AK_ID = B.UW_AK_ID AND A.CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
UNION
SELECT C.AGENCY_AK_ID, A.EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER_TERRITORY A, @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER B,@{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY_UNDERWRITER C
WHERE A.UW_AK_ID = B.UW_AK_ID AND B.UW_AK_ID = C.UW_AK_ID AND A.CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
UNION
SELECT C.AGENCY_AK_ID, A.EFF_FROM_DATE FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER_MANAGER A,@{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER_TERRITORY B, @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY_UNDERWRITER C
WHERE A.UW_MGR_AK_ID = B.UW_MGR_AK_ID AND C.UW_AK_ID = B.UW_AK_ID
AND A.CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
----------------------


	SELECT
	
	AGENCY.AGENCY_ID,
	AGENCY.AGENCY_AK_ID,
	AGENCY.RSM_TERR_SYM,
	ISNULL(AGENCY.RSM_TERR_CODE,0) RSM_TERR_CODE,
	AGENCY.RSM_TERR_NAME,
	AGENCY.RSM_FULL_NAME,
	AGENCY.PRIM_AGENCY_STATE_CODE,
	AGENCY.PRIM_AGENCY_STATE_ABBREV,
	AGENCY.PRIM_AGENCY_STATE_DESCRIPT,
	AGENCY.PRIM_AGENCY_NUM,
	AGENCY.PRIM_AGENCY_KEY,
	AGENCY.PRIM_AGENCY_NAME,
	AGENCY.AGENCY_STATE_CODE,
	AGENCY.AGENCY_STATE_ABBREV,
	AGENCY.AGENCY_STATE_DESCRIPT,
	AGENCY.AGENCY_NUM,
	AGENCY.AGENCY_KEY,
	AGENCY.AGENCY_NAME,
	AGENCY.AGENCY_PAY_CODE,
	AGENCY.AGENCY_PAY_CODE_EFF_FROM_DATE,
	AGENCY.AGENCY_PAY_CODE_EFF_TO_DATE,
	AGENCY.DIRCONN_PER_DATE,
	AGENCY.DIRCONN_COMM_DATE,
	DISTINCT_EFF_DATES.EFF_FROM_DATE,
	
	CASE WHEN AGENCY_UNDERWRITER.AGENCY_UW_ID IS NULL THEN -1 ELSE AGENCY_UNDERWRITER.AGENCY_UW_ID END AS AGENCY_UW_ID,
	
	CASE WHEN UNDERWRITER.UW_ID IS NULL THEN -1 ELSE UNDERWRITER.UW_ID END AS UW_ID,
	
	CASE WHEN UNDERWRITER_TERRITORY.UW_TERR_ID IS NULL THEN -1 ELSE UNDERWRITER_TERRITORY.UW_TERR_ID END AS UW_TERR_ID,
	
	CASE WHEN UNDERWRITER_MANAGER.UW_MGR_ID IS NULL THEN -1 ELSE UNDERWRITER_MANAGER.UW_MGR_ID END AS UW_MGR_ID,
	
	CASE WHEN AGENCY_ADDRESS.AGENCY_ADDRESS_ID IS NULL THEN -1 ELSE AGENCY_ADDRESS.AGENCY_ADDRESS_ID END AS AGENCY_ADDRESS_ID,
	
	CASE WHEN UNDERWRITER_TERRITORY.TERR_CODE IS NULL THEN 'N/A' ELSE RTRIM(UNDERWRITER_TERRITORY.TERR_CODE) END AS TERR_CODE,
	
	CASE WHEN UNDERWRITER.UW_CODE IS NULL THEN 'N/A' ELSE UW_CODE END AS UW_CODE,
	
	CASE WHEN RTRIM(UNDERWRITER.UW_FIRST_NAME) IS NULL THEN 'N/A' ELSE RTRIM(UNDERWRITER.UW_FIRST_NAME) END AS UW_FIRST_NAME,
	
	CASE WHEN RTRIM(UNDERWRITER.UW_LAST_NAME) IS NULL THEN 'N/A' ELSE RTRIM(UNDERWRITER.UW_LAST_NAME) END AS UW_LAST_NAME,
	
	CASE WHEN UNDERWRITER_MANAGER.SOURCE_UW_MGR_ID IS NULL THEN -1 ELSE UNDERWRITER_MANAGER.SOURCE_UW_MGR_ID END AS SOURCE_UW_MGR_ID,
	
	CASE WHEN RTRIM(UNDERWRITER_MANAGER.UW_MGR_FIRST_NAME) IS NULL THEN 'N/A' ELSE RTRIM(UNDERWRITER_MANAGER.UW_MGR_FIRST_NAME) END AS UW_MGR_FIRST_NAME,
	
	CASE WHEN RTRIM(UNDERWRITER_MANAGER.UW_MGR_LAST_NAME) IS NULL THEN 'N/A' ELSE RTRIM(UNDERWRITER_MANAGER.UW_MGR_LAST_NAME) END AS UW_MGR_LAST_NAME,
	
	CASE WHEN RTRIM(AGENCY_ADDRESS.AGENCY_ADDRESS) IS NULL THEN 'N/A' ELSE RTRIM(AGENCY_ADDRESS.AGENCY_ADDRESS) END AS AGENCY_ADDRESS,
	
	CASE WHEN RTRIM(AGENCY_ADDRESS.CITY) IS NULL THEN 'N/A' ELSE RTRIM(AGENCY_ADDRESS.CITY) END AS CITY,
	
	CASE WHEN RTRIM(AGENCY_ADDRESS.POSTAL_CODE) IS NULL THEN 'N/A' ELSE RTRIM(AGENCY_ADDRESS.POSTAL_CODE) END AS POSTAL_CODE
	
	FROM
	
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.work_v2_agency DISTINCT_EFF_DATES
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY AGENCY ON DISTINCT_EFF_DATES.AGENCY_AK_ID = AGENCY.AGENCY_AK_ID
	AND DISTINCT_EFF_DATES.EFF_FROM_DATE BETWEEN AGENCY.EFF_FROM_DATE AND AGENCY.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY_UNDERWRITER AGENCY_UNDERWRITER
	ON AGENCY.AGENCY_AK_ID = AGENCY_UNDERWRITER.AGENCY_AK_ID
	AND DISTINCT_EFF_DATES.EFF_FROM_DATE BETWEEN AGENCY_UNDERWRITER.EFF_FROM_DATE AND AGENCY_UNDERWRITER.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER UNDERWRITER
	ON AGENCY_UNDERWRITER.UW_AK_ID = UNDERWRITER.UW_AK_ID
	AND DISTINCT_EFF_DATES.EFF_FROM_DATE BETWEEN UNDERWRITER.EFF_FROM_DATE AND UNDERWRITER.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER_TERRITORY UNDERWRITER_TERRITORY
	ON UNDERWRITER.UW_AK_ID = UNDERWRITER_TERRITORY.UW_AK_ID
	AND DISTINCT_EFF_DATES.EFF_FROM_DATE BETWEEN UNDERWRITER_TERRITORY.EFF_FROM_DATE AND UNDERWRITER_TERRITORY.EFF_TO_DATE
	
	--LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER_MANAGER UNDERWRITER_MANAGER
	---ON UNDERWRITER_TERRITORY.UW_MGR_AK_ID = UNDERWRITER_MANAGER.UW_MGR_AK_ID
	---AND DISTINCT_EFF_DATES.EFF_FROM_DATE BETWEEN -------UNDERWRITER_MANAGER.EFF_FROM_DATE AND UNDERWRITER_MANAGER.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.UNDERWRITER_MANAGER UNDERWRITER_MANAGER
	ON UNDERWRITER_TERRITORY.UW_MGR_AK_ID = UNDERWRITER_MANAGER.UW_MGR_AK_ID
	AND UNDERWRITER_TERRITORY.UW_AK_ID = AGENCY_UNDERWRITER.UW_AK_ID   --
	AND AGENCY_UNDERWRITER.insurance_line IN ('C','D')
	AND DISTINCT_EFF_DATES.EFF_FROM_DATE BETWEEN UNDERWRITER_MANAGER.EFF_FROM_DATE 
	AND UNDERWRITER_MANAGER.EFF_TO_DATE
	
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.AGENCY_ADDRESS AGENCY_ADDRESS
	ON AGENCY.AGENCY_AK_ID = AGENCY_ADDRESS.AGENCY_AK_ID
	AND DISTINCT_EFF_DATES.EFF_FROM_DATE BETWEEN AGENCY_ADDRESS.EFF_FROM_DATE AND AGENCY_ADDRESS.EFF_TO_DATE
	 
	 
	ORDER BY 1,2
),
EXPTRANS AS (
	SELECT
	agency_id,
	agency_uw_id,
	-1 AS producer_code_id,
	uw_id,
	uw_terr_id,
	uw_mgr_id,
	agency_address_id,
	rsm_terr_code,
	rsm_terr_name,
	rsm_full_name,
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
	agency_pay_code,
	agency_pay_code_eff_from_date,
	agency_pay_code_eff_to_date,
	'N/A' AS prdcr_code,
	terr_code,
	uw_code,
	uw_first_name,
	uw_last_name,
	uw_first_name || ' ' || uw_last_name AS uw_full_name,
	source_uw_mgr_id,
	uw_mgr_first_name,
	uw_mgr_last_name,
	uw_mgr_first_name || ' ' || uw_mgr_last_name AS uw_mgr_full_name,
	rsm_terr_sym,
	dirconn_per_date,
	dirconn_comm_date,
	agency_address,
	city,
	postal_code,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	eff_from_date AS eff_from_dt,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_dt,
	sysdate AS cr_mod_dt,
	agency_ak_id,
	'N/A' AS DEFAULT_STR,
	-- *INF*: TO_DATE('1/1/1800','MM/DD/YYYY')
	TO_DATE('1/1/1800', 'MM/DD/YYYY'
	) AS DEFAULT_DATE
	FROM SQ_agency
),
LKP_agency_underwriter AS (
	SELECT
	agency_uw_id,
	insurance_line,
	uw_code,
	agency_ak_id
	FROM (
		SELECT      
		    aa.agency_uw_id as agency_uw_id
		, RTRIM(LTRIM(aa.insurance_line)) as insurance_line 
		  ,aa.agency_ak_id as agency_ak_id
		    ,a.uw_code as uw_code    
		  FROM      @{pipeline().parameters.SOURCE_TABLE_OWNER}.underwriter a,
		     @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency_underwriter aa
		  where a.uw_ak_id = aa.uw_ak_id
		   and  RTRIM(LTRIM(aa.insurance_line)) IN ( 'C','D')  --- Commercial Lines/NSI
		   and  a.crrnt_snpsht_flag = 1
		   and  aa.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY uw_code,agency_ak_id ORDER BY agency_uw_id DESC) = 1
),
LKP_sup_underwriter_manager_region AS (
	SELECT
	agency_num,
	agency_state,
	bus_unit_ind,
	uw_mgr_region,
	uw_mgr
	FROM (
		SELECT b.agency_num as agency_num
		                 ,b.agency_state as agency_state
		                  ,b.bus_unit_ind as bus_unit_ind 
		                 ,b.uw_mgr_region    as uw_mgr_region    
		                  ,b.uw_mgr    as uw_mgr    
		FROM (
		SELECT   
		    a.agency_num as agency_num
		   ,a.agency_state as agency_state
		   ,a.bus_unit_ind as bus_unit_ind 
		   ,CASE WHEN RTRIM(a.uw_mgr_region) IS NULL THEN 'N/A' ELSE RTRIM(a.uw_mgr_region) END as uw_mgr_region  
		   ,CASE WHEN RTRIM(a.uw_mgr) IS NULL THEN 'N/A' ELSE RTRIM(a.uw_mgr) END as uw_mgr
		  FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_underwriter_manager_region a
		  where a.crrnt_snpsht_flag = 1) b
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY agency_num,agency_state ORDER BY agency_num DESC) = 1
),
EXP_DET_MRG_REGION AS (
	SELECT
	LKP_sup_underwriter_manager_region.bus_unit_ind AS LKP_bus_unit_ind,
	LKP_sup_underwriter_manager_region.uw_mgr_region AS LKP_uw_mgr_region,
	LKP_sup_underwriter_manager_region.uw_mgr AS LKP_uw_mgr,
	EXPTRANS.uw_mgr_full_name AS in_uw_mgr_full_name,
	LKP_agency_underwriter.agency_uw_id AS LKP_agency_uw_id,
	LKP_agency_underwriter.insurance_line AS LKP_insurance_line,
	-- *INF*: IIF(ISNULL(LKP_agency_uw_id),'N/A',IIF(  LKP_insurance_line = 'D',in_uw_mgr_full_name,LKP_uw_mgr))
	-- 
	--  
	--  
	IFF(LKP_agency_uw_id IS NULL,
		'N/A',
		IFF(LKP_insurance_line = 'D',
			in_uw_mgr_full_name,
			LKP_uw_mgr
		)
	) AS v_uw_mgr_full_name,
	-- *INF*: IIF(ISNULL(LKP_agency_uw_id) OR LKP_insurance_line = 'D'  ,'N/A',LKP_uw_mgr_region)
	IFF(LKP_agency_uw_id IS NULL 
		OR LKP_insurance_line = 'D',
		'N/A',
		LKP_uw_mgr_region
	) AS v_uw_mgr_region,
	-- *INF*: IIF(ISNULL(LKP_agency_uw_id) OR LKP_insurance_line = 'D' ,'N/A',LKP_bus_unit_ind)
	IFF(LKP_agency_uw_id IS NULL 
		OR LKP_insurance_line = 'D',
		'N/A',
		LKP_bus_unit_ind
	) AS v_bus_unit_ind,
	v_uw_mgr_full_name AS o_uw_mgr,
	v_uw_mgr_region AS o_uw_mgr_region,
	v_bus_unit_ind AS o_bus_unit_ind
	FROM EXPTRANS
	LEFT JOIN LKP_agency_underwriter
	ON LKP_agency_underwriter.uw_code = EXPTRANS.uw_code AND LKP_agency_underwriter.agency_ak_id = EXPTRANS.agency_ak_id
	LEFT JOIN LKP_sup_underwriter_manager_region
	ON LKP_sup_underwriter_manager_region.agency_num = EXPTRANS.agency_num AND LKP_sup_underwriter_manager_region.agency_state = EXPTRANS.agency_state_code
),
LKP_AGENCY_DIM AS (
	SELECT
	agency_dim_id,
	edw_agency_pk_id,
	edw_agency_uw_pk_id,
	edw_prdcr_code_pk_id,
	edw_uw_pk_id,
	edw_uw_mgr_pk_id,
	edw_agency_addr_pk_id,
	terr_code,
	in_agency_id,
	in_agency_uw_id,
	in_producer_code_id,
	in_uw_id1,
	in_uw_terr_id,
	in_uw_mgr_id1,
	in_agency_address_id,
	in_rsm_terr_code1,
	in_rsm_terr_name,
	in_rsm_full_name1,
	in_prim_agency_state_code1,
	in_prim_agency_state_abbrev1,
	in_prim_agency_state_descript1,
	in_prim_agency_num1,
	in_prim_agency_key1,
	in_prim_agency_name1,
	in_agency_state_code1,
	in_agency_state_abbrev1,
	in_agency_state_descript1,
	in_agency_num1,
	in_agency_key1,
	in_agency_name1,
	in_prdcr_code1,
	in_terr_code1,
	in_uw_code1,
	in_uw_full_name1,
	in_source_uw_mgr_id1,
	in_uw_mgr_full_name1,
	in_rsm_terr_sym1,
	in_dirconn_per_date,
	in_dirconn_comm_date1,
	in_agency_address,
	in_city,
	in_postal_code,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_dt,
	eff_to_dt,
	cr_mod_dt,
	agency_ak_id,
	in_agency_pay_code,
	in_agency_pay_code_eff_from_date,
	in_agency_pay_code_eff_to_date
	FROM (
		SELECT 
		agency_dim.agency_dim_id as agency_dim_id, 
		agency_dim.edw_agency_pk_id as edw_agency_pk_id, 
		agency_dim.edw_agency_uw_pk_id as edw_agency_uw_pk_id, 
		agency_dim.edw_prdcr_code_pk_id as edw_prdcr_code_pk_id, 
		agency_dim.edw_uw_pk_id as edw_uw_pk_id, 
		agency_dim.edw_uw_mgr_pk_id as edw_uw_mgr_pk_id, 
		agency_dim.edw_agency_addr_pk_id as edw_agency_addr_pk_id, 
		rtrim(agency_dim.terr_code) as terr_code 
		FROM 
		V2.agency_dim agency_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_agency_pk_id,edw_agency_uw_pk_id,edw_prdcr_code_pk_id,edw_uw_pk_id,edw_uw_mgr_pk_id,edw_agency_addr_pk_id,terr_code ORDER BY agency_dim_id) = 1
),
FIL_EXISTING_ROWS AS (
	SELECT
	LKP_AGENCY_DIM.agency_dim_id AS in_agency_dim_id, 
	LKP_AGENCY_DIM.in_agency_id, 
	LKP_AGENCY_DIM.in_agency_uw_id, 
	LKP_AGENCY_DIM.in_producer_code_id, 
	LKP_AGENCY_DIM.in_uw_id1, 
	LKP_AGENCY_DIM.in_uw_terr_id, 
	LKP_AGENCY_DIM.in_uw_mgr_id1, 
	LKP_AGENCY_DIM.in_agency_address_id, 
	LKP_AGENCY_DIM.in_rsm_terr_code1, 
	LKP_AGENCY_DIM.in_rsm_terr_name, 
	LKP_AGENCY_DIM.in_rsm_full_name1, 
	LKP_AGENCY_DIM.in_prim_agency_state_code1, 
	LKP_AGENCY_DIM.in_prim_agency_state_abbrev1, 
	LKP_AGENCY_DIM.in_prim_agency_state_descript1, 
	LKP_AGENCY_DIM.in_prim_agency_num1, 
	LKP_AGENCY_DIM.in_prim_agency_key1, 
	LKP_AGENCY_DIM.in_prim_agency_name1, 
	LKP_AGENCY_DIM.in_agency_state_code1, 
	LKP_AGENCY_DIM.in_agency_state_abbrev1, 
	LKP_AGENCY_DIM.in_agency_state_descript1, 
	LKP_AGENCY_DIM.in_agency_num1, 
	LKP_AGENCY_DIM.in_agency_key1, 
	LKP_AGENCY_DIM.in_agency_name1, 
	LKP_AGENCY_DIM.in_prdcr_code1, 
	LKP_AGENCY_DIM.in_terr_code1, 
	LKP_AGENCY_DIM.in_uw_code1, 
	LKP_AGENCY_DIM.in_uw_full_name1, 
	LKP_AGENCY_DIM.in_source_uw_mgr_id1, 
	EXP_DET_MRG_REGION.o_uw_mgr AS in_uw_mgr_full_name1, 
	LKP_AGENCY_DIM.in_rsm_terr_sym1, 
	LKP_AGENCY_DIM.in_dirconn_per_date, 
	LKP_AGENCY_DIM.in_dirconn_comm_date1, 
	LKP_AGENCY_DIM.in_agency_address, 
	LKP_AGENCY_DIM.in_city, 
	LKP_AGENCY_DIM.in_postal_code, 
	LKP_AGENCY_DIM.crrnt_snpsht_flag, 
	LKP_AGENCY_DIM.audit_id, 
	LKP_AGENCY_DIM.eff_from_dt, 
	LKP_AGENCY_DIM.eff_to_dt, 
	LKP_AGENCY_DIM.cr_mod_dt, 
	LKP_AGENCY_DIM.agency_ak_id, 
	EXP_DET_MRG_REGION.o_bus_unit_ind AS bus_unit_ind, 
	EXP_DET_MRG_REGION.o_uw_mgr_region AS uw_mgr_region, 
	LKP_sup_underwriter_manager_region.uw_mgr AS LKP_uw_mgr, 
	LKP_AGENCY_DIM.in_agency_pay_code, 
	LKP_AGENCY_DIM.in_agency_pay_code_eff_from_date, 
	LKP_AGENCY_DIM.in_agency_pay_code_eff_to_date, 
	EXPTRANS.DEFAULT_STR, 
	EXPTRANS.DEFAULT_DATE
	FROM EXPTRANS
	 -- Manually join with EXP_DET_MRG_REGION
	LEFT JOIN LKP_AGENCY_DIM
	ON LKP_AGENCY_DIM.edw_agency_pk_id = EXPTRANS.agency_id AND LKP_AGENCY_DIM.edw_agency_uw_pk_id = EXPTRANS.agency_uw_id AND LKP_AGENCY_DIM.edw_prdcr_code_pk_id = EXPTRANS.producer_code_id AND LKP_AGENCY_DIM.edw_uw_pk_id = EXPTRANS.uw_id AND LKP_AGENCY_DIM.edw_uw_mgr_pk_id = EXPTRANS.uw_mgr_id AND LKP_AGENCY_DIM.edw_agency_addr_pk_id = EXPTRANS.agency_address_id AND LKP_AGENCY_DIM.terr_code = EXPTRANS.terr_code
	LEFT JOIN LKP_sup_underwriter_manager_region
	ON LKP_sup_underwriter_manager_region.agency_num = EXPTRANS.agency_num AND LKP_sup_underwriter_manager_region.agency_state = EXPTRANS.agency_state_code
	WHERE ISNULL(in_agency_dim_id)
),
agency_dim_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_dim
	(edw_agency_pk_id, edw_agency_uw_pk_id, edw_prdcr_code_pk_id, edw_uw_pk_id, edw_uw_terr_pk_id, edw_uw_mgr_pk_id, edw_agency_addr_pk_id, edw_agency_ak_id, rsm_terr_code, rsm_terr_descript, rsm_full_name, prim_agency_state_code, prim_agency_state_abbrev, prim_agency_state_descript, prim_agency_num, prim_agency_key, prim_agency_name, agency_state_code, agency_state_abbrev, agency_state_descript, agency_num, agency_key, agency_name, prdcr_code, terr_code, uw_code, uw_full_name, source_uw_mgr_id, uw_mgr_full_name, rsm_terr_sym, dirconnect_per_date, dirconn_comm_date, agency_addr, agency_city, agency_postal_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, agency_pay_code, agency_pay_code_eff_from_date, agency_pay_code_eff_to_date, bus_unit_ind, uw_mgr_region)
	SELECT 
	in_agency_id AS EDW_AGENCY_PK_ID, 
	in_agency_uw_id AS EDW_AGENCY_UW_PK_ID, 
	in_producer_code_id AS EDW_PRDCR_CODE_PK_ID, 
	in_uw_id1 AS EDW_UW_PK_ID, 
	in_uw_terr_id AS EDW_UW_TERR_PK_ID, 
	in_uw_mgr_id1 AS EDW_UW_MGR_PK_ID, 
	in_agency_address_id AS EDW_AGENCY_ADDR_PK_ID, 
	agency_ak_id AS EDW_AGENCY_AK_ID, 
	in_rsm_terr_code1 AS RSM_TERR_CODE, 
	in_rsm_terr_name AS RSM_TERR_DESCRIPT, 
	in_rsm_full_name1 AS RSM_FULL_NAME, 
	in_prim_agency_state_code1 AS PRIM_AGENCY_STATE_CODE, 
	in_prim_agency_state_abbrev1 AS PRIM_AGENCY_STATE_ABBREV, 
	in_prim_agency_state_descript1 AS PRIM_AGENCY_STATE_DESCRIPT, 
	in_prim_agency_num1 AS PRIM_AGENCY_NUM, 
	in_prim_agency_key1 AS PRIM_AGENCY_KEY, 
	in_prim_agency_name1 AS PRIM_AGENCY_NAME, 
	in_agency_state_code1 AS AGENCY_STATE_CODE, 
	in_agency_state_abbrev1 AS AGENCY_STATE_ABBREV, 
	in_agency_state_descript1 AS AGENCY_STATE_DESCRIPT, 
	in_agency_num1 AS AGENCY_NUM, 
	in_agency_key1 AS AGENCY_KEY, 
	in_agency_name1 AS AGENCY_NAME, 
	in_prdcr_code1 AS PRDCR_CODE, 
	in_terr_code1 AS TERR_CODE, 
	in_uw_code1 AS UW_CODE, 
	in_uw_full_name1 AS UW_FULL_NAME, 
	in_source_uw_mgr_id1 AS SOURCE_UW_MGR_ID, 
	in_uw_mgr_full_name1 AS UW_MGR_FULL_NAME, 
	in_rsm_terr_sym1 AS RSM_TERR_SYM, 
	in_dirconn_per_date AS DIRCONNECT_PER_DATE, 
	in_dirconn_comm_date1 AS DIRCONN_COMM_DATE, 
	in_agency_address AS AGENCY_ADDR, 
	in_city AS AGENCY_CITY, 
	in_postal_code AS AGENCY_POSTAL_CODE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	eff_from_dt AS EFF_FROM_DATE, 
	eff_to_dt AS EFF_TO_DATE, 
	cr_mod_dt AS CREATED_DATE, 
	cr_mod_dt AS MODIFIED_DATE, 
	in_agency_pay_code AS AGENCY_PAY_CODE, 
	in_agency_pay_code_eff_from_date AS AGENCY_PAY_CODE_EFF_FROM_DATE, 
	in_agency_pay_code_eff_to_date AS AGENCY_PAY_CODE_EFF_TO_DATE, 
	BUS_UNIT_IND, 
	UW_MGR_REGION
	FROM FIL_EXISTING_ROWS
),
SQ_agency_dim AS (
	SELECT
	agency_dim.agency_dim_id,
	agency_dim.edw_agency_ak_id,
	agency_dim.eff_from_date
	FROM
	V2.agency_dim agency_dim
	WHERE EXISTS
	(
	SELECT 1 FROM V2.agency_dim agency_dim2
	where agency_dim.edw_agency_ak_id = agency_dim2.edw_agency_ak_id
	and agency_dim2.crrnt_snpsht_flag = 1
	group by agency_dim2.edw_agency_ak_id having count(distinct eff_from_date) > 1
	)
	and agency_dim.crrnt_snpsht_flag = 1
	
	Order by 2,3
),
LKP_AGENCY_DIM_GET_TO_DATE AS (
	SELECT
	eff_from_date,
	eff_from_date1,
	eff_to_date,
	edw_agency_ak_id,
	edw_agency_ak_id1
	FROM (
		SELECT
		min(B.EFF_FROM_DATE) as eff_to_date,
		A.EFF_FROM_DATE as eff_from_date,
		A.edw_agency_ak_id as edw_agency_ak_id
		
		FROM
		(
		SELECT
		edw_agency_ak_id,
		EFF_FROM_DATE
		FROM
		v2.agency_dim
		where crrnt_snpsht_flag=1
		) AS A,
		(
		SELECT
		edw_agency_ak_id,
		EFF_FROM_DATE
		FROM
		v2.agency_dim
		where crrnt_snpsht_flag =1
		) AS B
		WHERE A.edw_agency_ak_id = B.edw_agency_ak_id
		AND B.EFF_FROM_DATE > A.EFF_FROM_DATE
		
		group by
		A.edw_agency_ak_id,
		A.EFF_FROM_DATE
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY eff_from_date,edw_agency_ak_id ORDER BY eff_from_date) = 1
),
EXP_EFF_TO_DATE AS (
	SELECT
	SQ_agency_dim.agency_dim_id,
	LKP_AGENCY_DIM_GET_TO_DATE.eff_to_date,
	-- *INF*: iif(not isnull(eff_to_date),add_to_date(eff_to_date,'SS',-1))
	IFF(eff_to_date IS NOT NULL,
		DATEADD(SECOND,- 1,eff_to_date)
	) AS out_eff_to_date,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date,
	LKP_AGENCY_DIM_GET_TO_DATE.edw_agency_ak_id AS exists_edw_agency_ak_id
	FROM SQ_agency_dim
	LEFT JOIN LKP_AGENCY_DIM_GET_TO_DATE
	ON LKP_AGENCY_DIM_GET_TO_DATE.eff_from_date = SQ_agency_dim.eff_from_date AND LKP_AGENCY_DIM_GET_TO_DATE.edw_agency_ak_id = SQ_agency_dim.edw_agency_ak_id
),
FIL_NULL_EFF_TO_DATE AS (
	SELECT
	agency_dim_id, 
	exists_edw_agency_ak_id, 
	out_eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_EFF_TO_DATE
	WHERE not isnull(exists_edw_agency_ak_id)
),
UPD_AGENCY_DIM_EXPIRE AS (
	SELECT
	agency_dim_id, 
	out_eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_NULL_EFF_TO_DATE
),
agency_dim_expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.agency_dim AS T
	USING UPD_AGENCY_DIM_EXPIRE AS S
	ON T.agency_dim_id = S.agency_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.out_eff_to_date, T.modified_date = S.modified_date
),