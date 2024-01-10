WITH
SQ_gtam_wbimcls_stage AS (
	Select inland_marine_class_code
	       ,inland_marine_class_description
	 FROM gtam_wbimcls_stage
),
EXP_values_wbimcls AS (
	SELECT
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_default_end_date,
	'N/A' AS v_default_str,
	inland_marine_class_code AS IN_inland_marine_class_code,
	-- *INF*: IIF(ISNULL(IN_inland_marine_class_code) OR IS_SPACES(IN_inland_marine_class_code) OR LENGTH(IN_inland_marine_class_code)=0
	-- ,'N/A'
	-- ,ltrim(rtrim(IN_inland_marine_class_code)))
	IFF(IN_inland_marine_class_code IS NULL 
		OR LENGTH(IN_inland_marine_class_code)>0 AND TRIM(IN_inland_marine_class_code)='' 
		OR LENGTH(IN_inland_marine_class_code
		) = 0,
		'N/A',
		ltrim(rtrim(IN_inland_marine_class_code
			)
		)
	) AS class_code,
	inland_marine_class_description AS IN_inland_marine_class_description,
	-- *INF*: IIF(ISNULL(IN_inland_marine_class_description) OR IS_SPACES(IN_inland_marine_class_description) OR
	-- LENGTH(IN_inland_marine_class_description)=0,'N/A',ltrim(rtrim(IN_inland_marine_class_description)))
	IFF(IN_inland_marine_class_description IS NULL 
		OR LENGTH(IN_inland_marine_class_description)>0 AND TRIM(IN_inland_marine_class_description)='' 
		OR LENGTH(IN_inland_marine_class_description
		) = 0,
		'N/A',
		ltrim(rtrim(IN_inland_marine_class_description
			)
		)
	) AS OUT_inland_marine_class_description,
	v_default_str AS OUT_class_loc_code,
	v_default_str AS OUT_class_descript_ind,
	v_default_str AS OUT_mco,
	v_default_str AS OUT_class_loc_state,
	v_default_str AS OUT_subline,
	v_default_end_date AS OUT_gtam_exp_date,
	v_default_str AS OUT_class_descript_num_seq,
	v_default_str AS OUT_crime_ind
	FROM SQ_gtam_wbimcls_stage
),
LKP_sup_class_code_wbimcls AS (
	SELECT
	sup_class_code_id,
	class_code_descript,
	class_code,
	class_loc_code,
	class_descript_ind,
	mco,
	class_loc_state,
	subline,
	gtam_exp_date,
	class_descript_num_seq,
	crime_ind
	FROM (
		SELECT 
			tl.sup_class_code_id as sup_class_code_id, 	
		     RTRIM(LTRIM(	tl.class_code_descript)) as class_code_descript, 	
		 RTRIM(LTRIM(	tl.crime_ind)) as crime_ind,
		       RTRIM(LTRIM( tl.class_code  )) as class_code,
			  RTRIM(LTRIM(tl.class_loc_code )) as class_loc_code ,
			  RTRIM(LTRIM(tl.class_descript_ind))  as class_descript_ind,
		  RTRIM(LTRIM(	tl.mco))  as mco,
			  RTRIM(LTRIM(tl.class_loc_state))  as class_loc_state,
		  RTRIM(LTRIM(	tl.subline))  as subline,
			tl.gtam_exp_date  as gtam_exp_date ,
			  RTRIM(LTRIM(tl.class_descript_num_seq))  as class_descript_num_seq		
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code,class_loc_code,class_descript_ind,mco,class_loc_state,subline,gtam_exp_date,class_descript_num_seq,crime_ind ORDER BY sup_class_code_id) = 1
),
EXP_Detect_Changes_wbimcls AS (
	SELECT
	EXP_values_wbimcls.OUT_inland_marine_class_description AS inland_marine_class_description,
	LKP_sup_class_code_wbimcls.sup_class_code_id AS LKP_sup_class_code_id,
	LKP_sup_class_code_wbimcls.class_code_descript AS LKP_class_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_class_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(inland_marine_class_description)) <> ltrim(rtrim(LKP_class_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_class_code_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(inland_marine_class_description
					)
				) <> ltrim(rtrim(LKP_class_code_descript
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	EXP_values_wbimcls.class_code,
	EXP_values_wbimcls.OUT_class_loc_code AS class_loc_code,
	EXP_values_wbimcls.OUT_class_descript_ind AS class_descript_ind,
	EXP_values_wbimcls.OUT_mco AS mco,
	EXP_values_wbimcls.OUT_class_loc_state AS class_loc_state,
	EXP_values_wbimcls.OUT_subline AS subline,
	EXP_values_wbimcls.OUT_gtam_exp_date AS gtam_exp_date,
	EXP_values_wbimcls.OUT_class_descript_num_seq AS class_descript_num_seq,
	EXP_values_wbimcls.OUT_crime_ind AS crime_ind
	FROM EXP_values_wbimcls
	LEFT JOIN LKP_sup_class_code_wbimcls
	ON LKP_sup_class_code_wbimcls.class_code = EXP_values_wbimcls.class_code AND LKP_sup_class_code_wbimcls.class_loc_code = EXP_values_wbimcls.OUT_class_loc_code AND LKP_sup_class_code_wbimcls.class_descript_ind = EXP_values_wbimcls.OUT_class_descript_ind AND LKP_sup_class_code_wbimcls.mco = EXP_values_wbimcls.OUT_mco AND LKP_sup_class_code_wbimcls.class_loc_state = EXP_values_wbimcls.OUT_class_loc_state AND LKP_sup_class_code_wbimcls.subline = EXP_values_wbimcls.OUT_subline AND LKP_sup_class_code_wbimcls.gtam_exp_date = EXP_values_wbimcls.OUT_gtam_exp_date AND LKP_sup_class_code_wbimcls.class_descript_num_seq = EXP_values_wbimcls.OUT_class_descript_num_seq AND LKP_sup_class_code_wbimcls.crime_ind = EXP_values_wbimcls.OUT_crime_ind
),
FIL_insert_wbimcls AS (
	SELECT
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date, 
	class_code, 
	class_loc_code, 
	class_descript_ind, 
	mco, 
	class_loc_state, 
	subline, 
	gtam_exp_date, 
	class_descript_num_seq, 
	inland_marine_class_description AS class_code_descript, 
	crime_ind
	FROM EXP_Detect_Changes_wbimcls
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_class_code_wbimcls AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, class_code, class_loc_code, class_descript_ind, mco, class_loc_state, subline, gtam_exp_date, class_descript_num_seq, crime_ind, class_code_descript)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CLASS_CODE, 
	CLASS_LOC_CODE, 
	CLASS_DESCRIPT_IND, 
	MCO, 
	CLASS_LOC_STATE, 
	SUBLINE, 
	GTAM_EXP_DATE, 
	CLASS_DESCRIPT_NUM_SEQ, 
	CRIME_IND, 
	CLASS_CODE_DESCRIPT
	FROM FIL_insert_wbimcls
),
SQ_gtam_tl07rx_stage1 AS (
	SELECT
	 class_code
	,class_description_indicator
	,state
	,class_description_sequence      
	 ,class_description
	FROM gtam_tl07rx_stage
),
EXP_values_tl07rx AS (
	SELECT
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_default_end_date,
	'N/A' AS v_default_str,
	class_code AS IN_class_code,
	class_description_indicator AS IN_class_description_indicator,
	state AS IN_location_state,
	class_description_sequence AS IN_class_description_seq,
	class_description AS IN_class_description,
	-- *INF*: IIF(ISNULL(IN_class_code) OR IS_SPACES(IN_class_code) OR LENGTH(IN_class_code)=0
	-- ,'N/A'
	-- ,ltrim(rtrim(IN_class_code)))
	IFF(IN_class_code IS NULL 
		OR LENGTH(IN_class_code)>0 AND TRIM(IN_class_code)='' 
		OR LENGTH(IN_class_code
		) = 0,
		'N/A',
		ltrim(rtrim(IN_class_code
			)
		)
	) AS OUT_class_code,
	v_default_str AS OUT_class_loc_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description_indicator )))  ,v_default_str,ltrim(rtrim(IN_class_description_indicator)))
	IFF(RTRIM(LTRIM(IN_class_description_indicator
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description_indicator
			)
		)
	) AS OUT_class_descript_ind,
	v_default_str AS OUT_mco,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_location_state  )))  ,v_default_str,ltrim(rtrim(IN_location_state)))
	IFF(RTRIM(LTRIM(IN_location_state
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_location_state
			)
		)
	) AS OUT_class_loc_state,
	v_default_str AS OUT_subline,
	v_default_end_date AS OUT_gtam_exp_date,
	-- *INF*: IIF(ISNULL(IN_class_description_seq) OR IS_SPACES(IN_class_description_seq) OR
	-- LENGTH(IN_class_description_seq)=0,v_default_str,ltrim(rtrim(IN_class_description_seq)))
	IFF(IN_class_description_seq IS NULL 
		OR LENGTH(IN_class_description_seq)>0 AND TRIM(IN_class_description_seq)='' 
		OR LENGTH(IN_class_description_seq
		) = 0,
		v_default_str,
		ltrim(rtrim(IN_class_description_seq
			)
		)
	) AS OUT_class_descript_num_seq,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description  )))  ,v_default_str,ltrim(rtrim(IN_class_description)))
	IFF(RTRIM(LTRIM(IN_class_description
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description
			)
		)
	) AS OUT_class_description,
	v_default_str AS OUT_crime_ind
	FROM SQ_gtam_tl07rx_stage1
),
LKP_sup_class_code_tl07rx AS (
	SELECT
	sup_class_code_id,
	class_code_descript,
	class_code,
	class_loc_code,
	class_descript_ind,
	mco,
	class_loc_state,
	subline,
	gtam_exp_date,
	class_descript_num_seq,
	crime_ind
	FROM (
		SELECT 
			tl.sup_class_code_id as sup_class_code_id, 	
		     RTRIM(LTRIM(	tl.class_code_descript)) as class_code_descript, 	
		 RTRIM(LTRIM(	tl.crime_ind)) as crime_ind,
		       RTRIM(LTRIM( tl.class_code  )) as class_code,
			  RTRIM(LTRIM(tl.class_loc_code )) as class_loc_code ,
			  RTRIM(LTRIM(tl.class_descript_ind))  as class_descript_ind,
		  RTRIM(LTRIM(	tl.mco))  as mco,
			  RTRIM(LTRIM(tl.class_loc_state))  as class_loc_state,
		  RTRIM(LTRIM(	tl.subline))  as subline,
			tl.gtam_exp_date  as gtam_exp_date ,
			  RTRIM(LTRIM(tl.class_descript_num_seq))  as class_descript_num_seq		
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code,class_loc_code,class_descript_ind,mco,class_loc_state,subline,gtam_exp_date,class_descript_num_seq,crime_ind ORDER BY sup_class_code_id) = 1
),
EXP_Detect_Changes_tl07rx AS (
	SELECT
	EXP_values_tl07rx.OUT_class_description AS class_description,
	LKP_sup_class_code_tl07rx.sup_class_code_id AS LKP_sup_class_code_id,
	LKP_sup_class_code_tl07rx.class_code_descript AS LKP_class_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_class_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(class_description)) <> ltrim(rtrim(LKP_class_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_class_code_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(class_description
					)
				) <> ltrim(rtrim(LKP_class_code_descript
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	EXP_values_tl07rx.OUT_class_code AS class_code,
	EXP_values_tl07rx.OUT_class_loc_code AS class_loc_code,
	EXP_values_tl07rx.OUT_class_descript_ind AS class_descript_ind,
	EXP_values_tl07rx.OUT_mco AS mco,
	EXP_values_tl07rx.OUT_class_loc_state AS class_loc_state,
	EXP_values_tl07rx.OUT_subline AS subline,
	EXP_values_tl07rx.OUT_gtam_exp_date AS gtam_exp_date,
	EXP_values_tl07rx.OUT_class_descript_num_seq AS class_descript_num_seq,
	EXP_values_tl07rx.OUT_crime_ind AS crime_ind
	FROM EXP_values_tl07rx
	LEFT JOIN LKP_sup_class_code_tl07rx
	ON LKP_sup_class_code_tl07rx.class_code = EXP_values_tl07rx.OUT_class_code AND LKP_sup_class_code_tl07rx.class_loc_code = EXP_values_tl07rx.OUT_class_loc_code AND LKP_sup_class_code_tl07rx.class_descript_ind = EXP_values_tl07rx.OUT_class_descript_ind AND LKP_sup_class_code_tl07rx.mco = EXP_values_tl07rx.OUT_mco AND LKP_sup_class_code_tl07rx.class_loc_state = EXP_values_tl07rx.OUT_class_loc_state AND LKP_sup_class_code_tl07rx.subline = EXP_values_tl07rx.OUT_subline AND LKP_sup_class_code_tl07rx.gtam_exp_date = EXP_values_tl07rx.OUT_gtam_exp_date AND LKP_sup_class_code_tl07rx.class_descript_num_seq = EXP_values_tl07rx.OUT_class_descript_num_seq AND LKP_sup_class_code_tl07rx.crime_ind = EXP_values_tl07rx.OUT_crime_ind
),
FIL_insert_tl07rx AS (
	SELECT
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date, 
	class_code, 
	class_loc_code, 
	class_descript_ind, 
	mco, 
	class_loc_state, 
	subline, 
	gtam_exp_date, 
	class_descript_num_seq, 
	class_description AS class_code_descript, 
	crime_ind
	FROM EXP_Detect_Changes_tl07rx
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_class_code_tl07rx AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, class_code, class_loc_code, class_descript_ind, mco, class_loc_state, subline, gtam_exp_date, class_descript_num_seq, crime_ind, class_code_descript)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CLASS_CODE, 
	CLASS_LOC_CODE, 
	CLASS_DESCRIPT_IND, 
	MCO, 
	CLASS_LOC_STATE, 
	SUBLINE, 
	GTAM_EXP_DATE, 
	CLASS_DESCRIPT_NUM_SEQ, 
	CRIME_IND, 
	CLASS_CODE_DESCRIPT
	FROM FIL_insert_tl07rx
),
SQ_gtam_tl63y_stage AS (
	SELECT location
	      ,master_company_number
	      ,location_state
	      ,subline
	      ,class_description_code       
	      ,crime_ind
	      ,eff_date
	      ,class_desc_num_seq
	      ,one_line_of_class_desc
	FROM gtam_tl63y_stage
),
EXP_values_tl63y AS (
	SELECT
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_default_end_date,
	'N/A' AS v_default_str,
	location AS IN_class_loc_code,
	master_company_number AS IN_mco_code,
	location_state AS IN_location_state,
	subline AS IN_subline,
	class_description_code AS IN_class_description_indicator,
	crime_ind AS IN_crime_ind,
	eff_date AS IN_exp_date,
	class_desc_num_seq AS IN_class_description_seq,
	one_line_of_class_desc AS IN_class_description,
	v_default_str AS OUT_class_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_loc_code )))  ,v_default_str,ltrim(rtrim(IN_class_loc_code)))
	IFF(RTRIM(LTRIM(IN_class_loc_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_loc_code
			)
		)
	) AS OUT_class_loc_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description_indicator )))  ,v_default_str,ltrim(rtrim(IN_class_description_indicator)))
	IFF(RTRIM(LTRIM(IN_class_description_indicator
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description_indicator
			)
		)
	) AS OUT_class_descript_ind,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_mco_code )))  ,v_default_str,ltrim(rtrim(IN_mco_code)))
	IFF(RTRIM(LTRIM(IN_mco_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_mco_code
			)
		)
	) AS OUT_mco,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_location_state  )))  ,v_default_str,ltrim(rtrim(IN_location_state)))
	IFF(RTRIM(LTRIM(IN_location_state
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_location_state
			)
		)
	) AS OUT_class_loc_state,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_subline  )))  ,v_default_str,ltrim(rtrim(IN_subline)))
	IFF(RTRIM(LTRIM(IN_subline
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_subline
			)
		)
	) AS OUT_subline,
	-- *INF*: DECODE(  RTRIM(LTRIM( IN_exp_date )),
	--     NULL , v_default_end_date,      
	--     '99999999', v_default_end_date ,
	--     
	-- IIF(IS_DATE(IN_exp_date,'YYYYMMDD') = 1, 
	-- ADD_TO_DATE(
	-- TO_DATE(IN_exp_date,'YYYYMMDD') ,'DD',1) ,
	-- 
	-- ---
	-- ADD_TO_DATE(LAST_DAY(
	-- TO_DATE(
	-- CONCAT(     SUBSTR(IN_exp_date,1,6)  ,'01')
	-- 
	-- ,'YYYYMMDD')),'DD',1) 
	-- 
	-- ))
	--          
	DECODE(RTRIM(LTRIM(IN_exp_date
			)
		),
		NULL, v_default_end_date,
		'99999999', v_default_end_date,
		IFF(IS_DATE(IN_exp_date, 'YYYYMMDD'
			) = 1,
			DATEADD(DAY,1,TO_DATE(IN_exp_date, 'YYYYMMDD'
			)),
			DATEADD(DAY,1,LAST_DAY(TO_DATE(CONCAT(SUBSTR(IN_exp_date, 1, 6
						), '01'
					), 'YYYYMMDD'
				)
			))
		)
	) AS v_exp_date,
	v_exp_date AS OUT_exp_date,
	-- *INF*: IIF(ISNULL(IN_class_description_seq) OR IS_SPACES(IN_class_description_seq) OR
	-- LENGTH(IN_class_description_seq)=0,v_default_str,ltrim(rtrim(IN_class_description_seq)))
	IFF(IN_class_description_seq IS NULL 
		OR LENGTH(IN_class_description_seq)>0 AND TRIM(IN_class_description_seq)='' 
		OR LENGTH(IN_class_description_seq
		) = 0,
		v_default_str,
		ltrim(rtrim(IN_class_description_seq
			)
		)
	) AS OUT_class_descript_num_seq,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description  )))  ,v_default_str,ltrim(rtrim(IN_class_description)))
	IFF(RTRIM(LTRIM(IN_class_description
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description
			)
		)
	) AS OUT_class_description,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_crime_ind  ))) OR IS_SPACES(IN_crime_ind)
	-- OR LENGTH( IN_crime_ind) = 0
	-- 
	--   ,v_default_str,ltrim(rtrim(IN_crime_ind)))
	IFF(RTRIM(LTRIM(IN_crime_ind
			)
		) IS NULL 
		OR LENGTH(IN_crime_ind)>0 AND TRIM(IN_crime_ind)='' 
		OR LENGTH(IN_crime_ind
		) = 0,
		v_default_str,
		ltrim(rtrim(IN_crime_ind
			)
		)
	) AS OUT_crime_ind
	FROM SQ_gtam_tl63y_stage
),
LKP_sup_class_code_tl63y AS (
	SELECT
	sup_class_code_id,
	class_code_descript,
	class_code,
	class_loc_code,
	class_descript_ind,
	mco,
	class_loc_state,
	subline,
	gtam_exp_date,
	class_descript_num_seq,
	crime_ind
	FROM (
		SELECT 
			tl.sup_class_code_id as sup_class_code_id, 	
		     RTRIM(LTRIM(	tl.class_code_descript)) as class_code_descript, 	
		 RTRIM(LTRIM(	tl.crime_ind)) as crime_ind,
		       RTRIM(LTRIM( tl.class_code  )) as class_code,
			  RTRIM(LTRIM(tl.class_loc_code )) as class_loc_code ,
			  RTRIM(LTRIM(tl.class_descript_ind))  as class_descript_ind,
		  RTRIM(LTRIM(	tl.mco))  as mco,
			  RTRIM(LTRIM(tl.class_loc_state))  as class_loc_state,
		  RTRIM(LTRIM(	tl.subline))  as subline,
			tl.gtam_exp_date  as gtam_exp_date ,
			  RTRIM(LTRIM(tl.class_descript_num_seq))  as class_descript_num_seq		
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code,class_loc_code,class_descript_ind,mco,class_loc_state,subline,gtam_exp_date,class_descript_num_seq,crime_ind ORDER BY sup_class_code_id) = 1
),
EXP_Detect_Changes_tl63y AS (
	SELECT
	EXP_values_tl63y.OUT_class_description AS class_description,
	LKP_sup_class_code_tl63y.sup_class_code_id AS LKP_sup_class_code_id,
	LKP_sup_class_code_tl63y.class_code_descript AS LKP_class_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_class_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(class_description)) <> ltrim(rtrim(LKP_class_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_class_code_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(class_description
					)
				) <> ltrim(rtrim(LKP_class_code_descript
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	EXP_values_tl63y.OUT_class_code AS class_code,
	EXP_values_tl63y.OUT_class_loc_code AS class_loc_code,
	EXP_values_tl63y.OUT_class_descript_ind AS class_descript_ind,
	EXP_values_tl63y.OUT_mco AS mco,
	EXP_values_tl63y.OUT_class_loc_state AS class_loc_state,
	EXP_values_tl63y.OUT_subline AS subline,
	EXP_values_tl63y.OUT_exp_date AS gtam_exp_date,
	EXP_values_tl63y.OUT_class_descript_num_seq AS class_descript_num_seq,
	EXP_values_tl63y.OUT_crime_ind AS crime_ind
	FROM EXP_values_tl63y
	LEFT JOIN LKP_sup_class_code_tl63y
	ON LKP_sup_class_code_tl63y.class_code = EXP_values_tl63y.OUT_class_code AND LKP_sup_class_code_tl63y.class_loc_code = EXP_values_tl63y.OUT_class_loc_code AND LKP_sup_class_code_tl63y.class_descript_ind = EXP_values_tl63y.OUT_class_descript_ind AND LKP_sup_class_code_tl63y.mco = EXP_values_tl63y.OUT_mco AND LKP_sup_class_code_tl63y.class_loc_state = EXP_values_tl63y.OUT_class_loc_state AND LKP_sup_class_code_tl63y.subline = EXP_values_tl63y.OUT_subline AND LKP_sup_class_code_tl63y.gtam_exp_date = EXP_values_tl63y.OUT_exp_date AND LKP_sup_class_code_tl63y.class_descript_num_seq = EXP_values_tl63y.OUT_class_descript_num_seq AND LKP_sup_class_code_tl63y.crime_ind = EXP_values_tl63y.OUT_crime_ind
),
FIL_insert_tl63y AS (
	SELECT
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date, 
	class_code, 
	class_loc_code, 
	class_descript_ind, 
	mco, 
	class_loc_state, 
	subline, 
	gtam_exp_date, 
	class_descript_num_seq, 
	class_description AS class_code_descript, 
	crime_ind
	FROM EXP_Detect_Changes_tl63y
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_class_code_tl63y AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, class_code, class_loc_code, class_descript_ind, mco, class_loc_state, subline, gtam_exp_date, class_descript_num_seq, crime_ind, class_code_descript)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CLASS_CODE, 
	CLASS_LOC_CODE, 
	CLASS_DESCRIPT_IND, 
	MCO, 
	CLASS_LOC_STATE, 
	SUBLINE, 
	GTAM_EXP_DATE, 
	CLASS_DESCRIPT_NUM_SEQ, 
	CRIME_IND, 
	CLASS_CODE_DESCRIPT
	FROM FIL_insert_tl63y
),
SQ_gtam_tl63x_stage AS (
	SELECT  
	      location
	      ,master_company_number
	      ,location_state
	      ,class_description_code   
	      ,crime_ind
	      ,eff_date
	      ,class_desc_num_seq       
	      ,one_line_of_class_desc      
	  FROM  gtam_tl63x_stage
),
EXP_values_tl63x AS (
	SELECT
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_default_end_date,
	'N/A' AS v_default_str,
	location AS IN_class_loc_code,
	master_company_number AS IN_mco_code,
	location_state AS IN_location_state,
	class_description_code AS IN_class_description_indicator,
	crime_ind AS IN_crime_ind,
	eff_date AS IN_exp_date,
	class_desc_num_seq AS IN_class_description_seq,
	one_line_of_class_desc AS IN_class_description,
	v_default_str AS OUT_class_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_loc_code )))  ,v_default_str,ltrim(rtrim(IN_class_loc_code)))
	IFF(RTRIM(LTRIM(IN_class_loc_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_loc_code
			)
		)
	) AS OUT_class_loc_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description_indicator )))  ,v_default_str,ltrim(rtrim(IN_class_description_indicator)))
	IFF(RTRIM(LTRIM(IN_class_description_indicator
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description_indicator
			)
		)
	) AS OUT_class_descript_ind,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_mco_code )))  ,v_default_str,ltrim(rtrim(IN_mco_code)))
	IFF(RTRIM(LTRIM(IN_mco_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_mco_code
			)
		)
	) AS OUT_mco,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_location_state  )))  ,v_default_str,ltrim(rtrim(IN_location_state)))
	IFF(RTRIM(LTRIM(IN_location_state
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_location_state
			)
		)
	) AS OUT_class_loc_state,
	v_default_str AS OUT_subline,
	-- *INF*: DECODE(  RTRIM(LTRIM( IN_exp_date )),
	--     NULL , v_default_end_date,      
	--     '99999999', v_default_end_date ,
	--     
	-- IIF(IS_DATE(IN_exp_date,'YYYYMMDD') = 1, 
	-- ADD_TO_DATE(
	-- TO_DATE(IN_exp_date,'YYYYMMDD') ,'DD',1) ,
	-- 
	-- ---
	-- ADD_TO_DATE(LAST_DAY(
	-- TO_DATE(
	-- CONCAT(     SUBSTR(IN_exp_date,1,6)  ,'01')
	-- 
	-- ,'YYYYMMDD')),'DD',1) 
	-- 
	-- ))
	--          
	DECODE(RTRIM(LTRIM(IN_exp_date
			)
		),
		NULL, v_default_end_date,
		'99999999', v_default_end_date,
		IFF(IS_DATE(IN_exp_date, 'YYYYMMDD'
			) = 1,
			DATEADD(DAY,1,TO_DATE(IN_exp_date, 'YYYYMMDD'
			)),
			DATEADD(DAY,1,LAST_DAY(TO_DATE(CONCAT(SUBSTR(IN_exp_date, 1, 6
						), '01'
					), 'YYYYMMDD'
				)
			))
		)
	) AS v_exp_date,
	v_exp_date AS OUT_exp_date,
	-- *INF*: IIF(ISNULL(IN_class_description_seq) OR IS_SPACES(IN_class_description_seq) OR
	-- LENGTH(IN_class_description_seq)=0,v_default_str,ltrim(rtrim(IN_class_description_seq)))
	IFF(IN_class_description_seq IS NULL 
		OR LENGTH(IN_class_description_seq)>0 AND TRIM(IN_class_description_seq)='' 
		OR LENGTH(IN_class_description_seq
		) = 0,
		v_default_str,
		ltrim(rtrim(IN_class_description_seq
			)
		)
	) AS OUT_class_descript_num_seq,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description  )))  ,v_default_str,ltrim(rtrim(IN_class_description)))
	IFF(RTRIM(LTRIM(IN_class_description
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description
			)
		)
	) AS OUT_class_description,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_crime_ind  ))) OR IS_SPACES(IN_crime_ind)
	-- OR LENGTH( IN_crime_ind) = 0
	-- 
	--   ,v_default_str,ltrim(rtrim(IN_crime_ind)))
	-- 
	-- 
	--  
	IFF(RTRIM(LTRIM(IN_crime_ind
			)
		) IS NULL 
		OR LENGTH(IN_crime_ind)>0 AND TRIM(IN_crime_ind)='' 
		OR LENGTH(IN_crime_ind
		) = 0,
		v_default_str,
		ltrim(rtrim(IN_crime_ind
			)
		)
	) AS OUT_crime_ind
	FROM SQ_gtam_tl63x_stage
),
LKP_sup_class_code_tl63x AS (
	SELECT
	sup_class_code_id,
	class_code_descript,
	class_code,
	class_loc_code,
	class_descript_ind,
	mco,
	class_loc_state,
	subline,
	gtam_exp_date,
	class_descript_num_seq,
	crime_ind
	FROM (
		SELECT 
			tl.sup_class_code_id as sup_class_code_id, 	
		     RTRIM(LTRIM(	tl.class_code_descript)) as class_code_descript, 	
		 RTRIM(LTRIM(	tl.crime_ind)) as crime_ind,
		       RTRIM(LTRIM( tl.class_code  )) as class_code,
			  RTRIM(LTRIM(tl.class_loc_code )) as class_loc_code ,
			  RTRIM(LTRIM(tl.class_descript_ind))  as class_descript_ind,
		  RTRIM(LTRIM(	tl.mco))  as mco,
			  RTRIM(LTRIM(tl.class_loc_state))  as class_loc_state,
		  RTRIM(LTRIM(	tl.subline))  as subline,
			tl.gtam_exp_date  as gtam_exp_date ,
			  RTRIM(LTRIM(tl.class_descript_num_seq))  as class_descript_num_seq		
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code,class_loc_code,class_descript_ind,mco,class_loc_state,subline,gtam_exp_date,class_descript_num_seq,crime_ind ORDER BY sup_class_code_id) = 1
),
EXP_Detect_Changes_tl63x AS (
	SELECT
	EXP_values_tl63x.OUT_class_description AS class_description,
	LKP_sup_class_code_tl63x.sup_class_code_id AS LKP_sup_class_code_id,
	LKP_sup_class_code_tl63x.class_code_descript AS LKP_class_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_class_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(class_description)) <> ltrim(rtrim(LKP_class_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_class_code_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(class_description
					)
				) <> ltrim(rtrim(LKP_class_code_descript
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	EXP_values_tl63x.OUT_class_code AS class_code,
	EXP_values_tl63x.OUT_class_loc_code AS class_loc_code,
	EXP_values_tl63x.OUT_class_descript_ind AS class_descript_ind,
	EXP_values_tl63x.OUT_mco AS mco,
	EXP_values_tl63x.OUT_class_loc_state AS class_loc_state,
	EXP_values_tl63x.OUT_subline AS subline,
	EXP_values_tl63x.OUT_exp_date AS gtam_exp_date,
	EXP_values_tl63x.OUT_class_descript_num_seq AS class_descript_num_seq,
	EXP_values_tl63x.OUT_crime_ind AS crime_ind
	FROM EXP_values_tl63x
	LEFT JOIN LKP_sup_class_code_tl63x
	ON LKP_sup_class_code_tl63x.class_code = EXP_values_tl63x.OUT_class_code AND LKP_sup_class_code_tl63x.class_loc_code = EXP_values_tl63x.OUT_class_loc_code AND LKP_sup_class_code_tl63x.class_descript_ind = EXP_values_tl63x.OUT_class_descript_ind AND LKP_sup_class_code_tl63x.mco = EXP_values_tl63x.OUT_mco AND LKP_sup_class_code_tl63x.class_loc_state = EXP_values_tl63x.OUT_class_loc_state AND LKP_sup_class_code_tl63x.subline = EXP_values_tl63x.OUT_subline AND LKP_sup_class_code_tl63x.gtam_exp_date = EXP_values_tl63x.OUT_exp_date AND LKP_sup_class_code_tl63x.class_descript_num_seq = EXP_values_tl63x.OUT_class_descript_num_seq AND LKP_sup_class_code_tl63x.crime_ind = EXP_values_tl63x.OUT_crime_ind
),
FIL_insert_tl63x AS (
	SELECT
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date, 
	class_code, 
	class_loc_code, 
	class_descript_ind, 
	mco, 
	class_loc_state, 
	subline, 
	gtam_exp_date, 
	class_descript_num_seq, 
	class_description AS class_code_descript, 
	crime_ind
	FROM EXP_Detect_Changes_tl63x
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_class_code_tl63x AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, class_code, class_loc_code, class_descript_ind, mco, class_loc_state, subline, gtam_exp_date, class_descript_num_seq, crime_ind, class_code_descript)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CLASS_CODE, 
	CLASS_LOC_CODE, 
	CLASS_DESCRIPT_IND, 
	MCO, 
	CLASS_LOC_STATE, 
	SUBLINE, 
	GTAM_EXP_DATE, 
	CLASS_DESCRIPT_NUM_SEQ, 
	CRIME_IND, 
	CLASS_CODE_DESCRIPT
	FROM FIL_insert_tl63x
),
SQ_gtam_tm530x_stage AS (
	SELECT a.location
	      ,a.master_company_number
	      ,a.state
	      ,a.class_description_code
	      ,a.crime_indicator
	      ,a.expiration_date
	     ,a.screen_class_descr 
	FROM gtam_tm530x_stage a
),
EXP_values_tm530x AS (
	SELECT
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_default_end_date,
	'N/A' AS v_default_str,
	location AS IN_class_loc_code,
	master_company_number AS IN_mco_code,
	state AS IN_location_state,
	class_description_code AS IN_class_description_indicator,
	crime_indicator AS IN_crime_ind,
	expiration_date AS IN_exp_date,
	screen_class_descr AS IN_class_description,
	v_default_str AS OUT_class_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_loc_code )))  ,v_default_str,ltrim(rtrim(IN_class_loc_code)))
	IFF(RTRIM(LTRIM(IN_class_loc_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_loc_code
			)
		)
	) AS OUT_class_loc_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description_indicator )))  ,v_default_str,ltrim(rtrim(IN_class_description_indicator)))
	IFF(RTRIM(LTRIM(IN_class_description_indicator
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description_indicator
			)
		)
	) AS OUT_class_descript_ind,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_mco_code )))  ,v_default_str,ltrim(rtrim(IN_mco_code)))
	IFF(RTRIM(LTRIM(IN_mco_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_mco_code
			)
		)
	) AS OUT_mco,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_location_state  )))  ,v_default_str,ltrim(rtrim(IN_location_state)))
	IFF(RTRIM(LTRIM(IN_location_state
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_location_state
			)
		)
	) AS OUT_class_loc_state,
	v_default_str AS OUT_subline,
	-- *INF*: DECODE(  RTRIM(LTRIM( IN_exp_date )),
	--     NULL , v_default_end_date,      
	--     '99999999', v_default_end_date ,
	--     
	-- IIF(IS_DATE(IN_exp_date,'YYYYMMDD') = 1, 
	-- ADD_TO_DATE(
	-- TO_DATE(IN_exp_date,'YYYYMMDD') ,'DD',1) ,
	-- 
	-- ---
	-- ADD_TO_DATE(LAST_DAY(
	-- TO_DATE(
	-- CONCAT(     SUBSTR(IN_exp_date,1,6)  ,'01')
	-- 
	-- ,'YYYYMMDD')),'DD',1) 
	-- 
	-- ))
	--          
	DECODE(RTRIM(LTRIM(IN_exp_date
			)
		),
		NULL, v_default_end_date,
		'99999999', v_default_end_date,
		IFF(IS_DATE(IN_exp_date, 'YYYYMMDD'
			) = 1,
			DATEADD(DAY,1,TO_DATE(IN_exp_date, 'YYYYMMDD'
			)),
			DATEADD(DAY,1,LAST_DAY(TO_DATE(CONCAT(SUBSTR(IN_exp_date, 1, 6
						), '01'
					), 'YYYYMMDD'
				)
			))
		)
	) AS v_exp_date,
	v_exp_date AS OUT_exp_date,
	v_default_str AS OUT_class_descript_num_seq,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description  )))  ,v_default_str,ltrim(rtrim(IN_class_description)))
	IFF(RTRIM(LTRIM(IN_class_description
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description
			)
		)
	) AS OUT_class_description,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_crime_ind  ))) OR IS_SPACES(IN_crime_ind)
	-- OR LENGTH( IN_crime_ind) = 0
	-- 
	--   ,v_default_str,ltrim(rtrim(IN_crime_ind)))
	IFF(RTRIM(LTRIM(IN_crime_ind
			)
		) IS NULL 
		OR LENGTH(IN_crime_ind)>0 AND TRIM(IN_crime_ind)='' 
		OR LENGTH(IN_crime_ind
		) = 0,
		v_default_str,
		ltrim(rtrim(IN_crime_ind
			)
		)
	) AS OUT_crime_ind
	FROM SQ_gtam_tm530x_stage
),
LKP_sup_class_code_tm530x AS (
	SELECT
	sup_class_code_id,
	class_code_descript,
	class_code,
	class_loc_code,
	class_descript_ind,
	mco,
	class_loc_state,
	subline,
	gtam_exp_date,
	class_descript_num_seq,
	crime_ind
	FROM (
		SELECT 
			tl.sup_class_code_id as sup_class_code_id, 	
		     RTRIM(LTRIM(	tl.class_code_descript)) as class_code_descript, 	
		 RTRIM(LTRIM(	tl.crime_ind)) as crime_ind,
		       RTRIM(LTRIM( tl.class_code  )) as class_code,
			  RTRIM(LTRIM(tl.class_loc_code )) as class_loc_code ,
			  RTRIM(LTRIM(tl.class_descript_ind))  as class_descript_ind,
		  RTRIM(LTRIM(	tl.mco))  as mco,
			  RTRIM(LTRIM(tl.class_loc_state))  as class_loc_state,
		  RTRIM(LTRIM(	tl.subline))  as subline,
			tl.gtam_exp_date  as gtam_exp_date ,
			  RTRIM(LTRIM(tl.class_descript_num_seq))  as class_descript_num_seq		
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code,class_loc_code,class_descript_ind,mco,class_loc_state,subline,gtam_exp_date,class_descript_num_seq,crime_ind ORDER BY sup_class_code_id) = 1
),
EXP_Detect_Changes_tm530x AS (
	SELECT
	EXP_values_tm530x.OUT_class_description AS class_description,
	LKP_sup_class_code_tm530x.sup_class_code_id AS LKP_sup_class_code_id,
	LKP_sup_class_code_tm530x.class_code_descript AS LKP_class_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_class_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(class_description)) <> ltrim(rtrim(LKP_class_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_class_code_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(class_description
					)
				) <> ltrim(rtrim(LKP_class_code_descript
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	EXP_values_tm530x.OUT_class_code AS class_code,
	EXP_values_tm530x.OUT_class_loc_code AS class_loc_code,
	EXP_values_tm530x.OUT_class_descript_ind AS class_descript_ind,
	EXP_values_tm530x.OUT_mco AS mco,
	EXP_values_tm530x.OUT_class_loc_state AS class_loc_state,
	EXP_values_tm530x.OUT_subline AS subline,
	EXP_values_tm530x.OUT_exp_date AS gtam_exp_date,
	EXP_values_tm530x.OUT_class_descript_num_seq AS class_descript_num_seq,
	EXP_values_tm530x.OUT_crime_ind AS crime_ind
	FROM EXP_values_tm530x
	LEFT JOIN LKP_sup_class_code_tm530x
	ON LKP_sup_class_code_tm530x.class_code = EXP_values_tm530x.OUT_class_code AND LKP_sup_class_code_tm530x.class_loc_code = EXP_values_tm530x.OUT_class_loc_code AND LKP_sup_class_code_tm530x.class_descript_ind = EXP_values_tm530x.OUT_class_descript_ind AND LKP_sup_class_code_tm530x.mco = EXP_values_tm530x.OUT_mco AND LKP_sup_class_code_tm530x.class_loc_state = EXP_values_tm530x.OUT_class_loc_state AND LKP_sup_class_code_tm530x.subline = EXP_values_tm530x.OUT_subline AND LKP_sup_class_code_tm530x.gtam_exp_date = EXP_values_tm530x.OUT_exp_date AND LKP_sup_class_code_tm530x.class_descript_num_seq = EXP_values_tm530x.OUT_class_descript_num_seq AND LKP_sup_class_code_tm530x.crime_ind = EXP_values_tm530x.OUT_crime_ind
),
FIL_insert_tm530x AS (
	SELECT
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date, 
	class_code, 
	class_loc_code, 
	class_descript_ind, 
	mco, 
	class_loc_state, 
	subline, 
	gtam_exp_date, 
	class_descript_num_seq, 
	crime_ind, 
	class_description AS class_code_descript
	FROM EXP_Detect_Changes_tm530x
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_class_code_tm530x AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, class_code, class_loc_code, class_descript_ind, mco, class_loc_state, subline, gtam_exp_date, class_descript_num_seq, crime_ind, class_code_descript)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CLASS_CODE, 
	CLASS_LOC_CODE, 
	CLASS_DESCRIPT_IND, 
	MCO, 
	CLASS_LOC_STATE, 
	SUBLINE, 
	GTAM_EXP_DATE, 
	CLASS_DESCRIPT_NUM_SEQ, 
	CRIME_IND, 
	CLASS_CODE_DESCRIPT
	FROM FIL_insert_tm530x
),
SQ_gtam_tm530xe_seq1_stage AS (
	SELECT location
	      ,policy_company
	      ,state
	      ,business_classification_code    
	      ,expiration_date    
	      ,long_desc  
	 FROM gtam_tm530xe_seq1_stage
),
EXP_values_tm530xe_seq1 AS (
	SELECT
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS v_default_end_date,
	'N/A' AS v_default_str,
	location AS IN_class_loc_code,
	policy_company AS IN_mco_code,
	state AS IN_location_state,
	business_classification_code AS IN_class_code,
	expiration_date AS IN_exp_date,
	long_desc AS IN_class_description,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_code )))  ,v_default_str,ltrim(rtrim(IN_class_code)))
	IFF(RTRIM(LTRIM(IN_class_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_code
			)
		)
	) AS OUT_class_code,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_loc_code )))  ,v_default_str,ltrim(rtrim(IN_class_loc_code)))
	IFF(RTRIM(LTRIM(IN_class_loc_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_loc_code
			)
		)
	) AS OUT_class_loc_code,
	v_default_str AS OUT_class_descript_ind,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_mco_code )))  ,v_default_str,ltrim(rtrim(IN_mco_code)))
	IFF(RTRIM(LTRIM(IN_mco_code
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_mco_code
			)
		)
	) AS OUT_mco,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_location_state  )))  ,v_default_str,ltrim(rtrim(IN_location_state)))
	IFF(RTRIM(LTRIM(IN_location_state
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_location_state
			)
		)
	) AS OUT_class_loc_state,
	v_default_str AS OUT_subline,
	-- *INF*: DECODE(  RTRIM(LTRIM( IN_exp_date )),
	--     NULL , v_default_end_date,      
	--     '99999999', v_default_end_date ,
	--     
	-- IIF(IS_DATE(IN_exp_date,'YYYYMMDD') = 1, 
	-- ADD_TO_DATE(
	-- TO_DATE(IN_exp_date,'YYYYMMDD') ,'DD',1) ,
	-- 
	-- ---
	-- ADD_TO_DATE(LAST_DAY(
	-- TO_DATE(
	-- CONCAT(     SUBSTR(IN_exp_date,1,6)  ,'01')
	-- 
	-- ,'YYYYMMDD')),'DD',1) 
	-- 
	-- ))
	--          
	DECODE(RTRIM(LTRIM(IN_exp_date
			)
		),
		NULL, v_default_end_date,
		'99999999', v_default_end_date,
		IFF(IS_DATE(IN_exp_date, 'YYYYMMDD'
			) = 1,
			DATEADD(DAY,1,TO_DATE(IN_exp_date, 'YYYYMMDD'
			)),
			DATEADD(DAY,1,LAST_DAY(TO_DATE(CONCAT(SUBSTR(IN_exp_date, 1, 6
						), '01'
					), 'YYYYMMDD'
				)
			))
		)
	) AS v_exp_date,
	v_exp_date AS OUT_exp_date,
	v_default_str AS OUT_class_descript_num_seq,
	-- *INF*: IIF(ISNULL( RTRIM(LTRIM( IN_class_description  )))  ,v_default_str,ltrim(rtrim(IN_class_description)))
	IFF(RTRIM(LTRIM(IN_class_description
			)
		) IS NULL,
		v_default_str,
		ltrim(rtrim(IN_class_description
			)
		)
	) AS OUT_class_description,
	v_default_str AS OUT_crime_ind
	FROM SQ_gtam_tm530xe_seq1_stage
),
LKP_sup_class_code_tm530xe_seq1 AS (
	SELECT
	sup_class_code_id,
	class_code_descript,
	class_code,
	class_loc_code,
	class_descript_ind,
	mco,
	class_loc_state,
	subline,
	gtam_exp_date,
	class_descript_num_seq,
	crime_ind
	FROM (
		SELECT 
			tl.sup_class_code_id as sup_class_code_id, 	
		     RTRIM(LTRIM(	tl.class_code_descript)) as class_code_descript, 	
		 RTRIM(LTRIM(	tl.crime_ind)) as crime_ind,
		       RTRIM(LTRIM( tl.class_code  )) as class_code,
			  RTRIM(LTRIM(tl.class_loc_code )) as class_loc_code ,
			  RTRIM(LTRIM(tl.class_descript_ind))  as class_descript_ind,
		  RTRIM(LTRIM(	tl.mco))  as mco,
			  RTRIM(LTRIM(tl.class_loc_state))  as class_loc_state,
		  RTRIM(LTRIM(	tl.subline))  as subline,
			tl.gtam_exp_date  as gtam_exp_date ,
			  RTRIM(LTRIM(tl.class_descript_num_seq))  as class_descript_num_seq		
		FROM  
			@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tl
		WHERE 
			tl.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY class_code,class_loc_code,class_descript_ind,mco,class_loc_state,subline,gtam_exp_date,class_descript_num_seq,crime_ind ORDER BY sup_class_code_id) = 1
),
EXP_Detect_Changes_tm530xe_seq1 AS (
	SELECT
	EXP_values_tm530xe_seq1.OUT_class_description AS class_description,
	LKP_sup_class_code_tm530xe_seq1.sup_class_code_id AS LKP_sup_class_code_id,
	LKP_sup_class_code_tm530xe_seq1.class_code_descript AS LKP_class_code_descript,
	1 AS crrnt_snapshot_flag,
	-- *INF*: IIF(ISNULL(LKP_sup_class_code_id),'NEW',
	-- 	IIF(
	-- 	(ltrim(rtrim(class_description)) <> ltrim(rtrim(LKP_class_code_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(LKP_sup_class_code_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(class_description
					)
				) <> ltrim(rtrim(LKP_class_code_descript
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'
			), 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_System_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	EXP_values_tm530xe_seq1.OUT_class_code AS class_code,
	EXP_values_tm530xe_seq1.OUT_class_loc_code AS class_loc_code,
	EXP_values_tm530xe_seq1.OUT_class_descript_ind AS class_descript_ind,
	EXP_values_tm530xe_seq1.OUT_mco AS mco,
	EXP_values_tm530xe_seq1.OUT_class_loc_state AS class_loc_state,
	EXP_values_tm530xe_seq1.OUT_subline AS subline,
	EXP_values_tm530xe_seq1.OUT_exp_date AS gtam_exp_date,
	EXP_values_tm530xe_seq1.OUT_class_descript_num_seq AS class_descript_num_seq,
	EXP_values_tm530xe_seq1.OUT_crime_ind AS crime_ind
	FROM EXP_values_tm530xe_seq1
	LEFT JOIN LKP_sup_class_code_tm530xe_seq1
	ON LKP_sup_class_code_tm530xe_seq1.class_code = EXP_values_tm530xe_seq1.OUT_class_code AND LKP_sup_class_code_tm530xe_seq1.class_loc_code = EXP_values_tm530xe_seq1.OUT_class_loc_code AND LKP_sup_class_code_tm530xe_seq1.class_descript_ind = EXP_values_tm530xe_seq1.OUT_class_descript_ind AND LKP_sup_class_code_tm530xe_seq1.mco = EXP_values_tm530xe_seq1.OUT_mco AND LKP_sup_class_code_tm530xe_seq1.class_loc_state = EXP_values_tm530xe_seq1.OUT_class_loc_state AND LKP_sup_class_code_tm530xe_seq1.subline = EXP_values_tm530xe_seq1.OUT_subline AND LKP_sup_class_code_tm530xe_seq1.gtam_exp_date = EXP_values_tm530xe_seq1.OUT_exp_date AND LKP_sup_class_code_tm530xe_seq1.class_descript_num_seq = EXP_values_tm530xe_seq1.OUT_class_descript_num_seq AND LKP_sup_class_code_tm530xe_seq1.crime_ind = EXP_values_tm530xe_seq1.OUT_crime_ind
),
FIL_insert_tm530xe_seq1 AS (
	SELECT
	crrnt_snapshot_flag, 
	Audit_ID, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	Source_System_ID, 
	Created_Date, 
	Modified_Date, 
	class_code, 
	class_loc_code, 
	class_descript_ind, 
	mco, 
	class_loc_state, 
	subline, 
	gtam_exp_date, 
	class_descript_num_seq, 
	crime_ind, 
	class_description AS class_code_descript
	FROM EXP_Detect_Changes_tm530xe_seq1
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
INS_sup_class_code_tm530xe_seq1 AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, class_code, class_loc_code, class_descript_ind, mco, class_loc_state, subline, gtam_exp_date, class_descript_num_seq, crime_ind, class_code_descript)
	SELECT 
	crrnt_snapshot_flag AS CRRNT_SNPSHT_FLAG, 
	Audit_ID AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	Source_System_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CLASS_CODE, 
	CLASS_LOC_CODE, 
	CLASS_DESCRIPT_IND, 
	MCO, 
	CLASS_LOC_STATE, 
	SUBLINE, 
	GTAM_EXP_DATE, 
	CLASS_DESCRIPT_NUM_SEQ, 
	CRIME_IND, 
	CLASS_CODE_DESCRIPT
	FROM FIL_insert_tm530xe_seq1
),
SQ_sup_class_code_UPD AS (
	SELECT 
		tl.sup_class_code_id, 
		tl.eff_from_date, 
		tl.eff_to_date, 
		tl.source_sys_id,
		tl.class_code,
		tl.class_loc_code,
		tl.class_descript_ind,
		tl.mco,
		tl.class_loc_state,
		tl.subline,
		tl.gtam_exp_date,
		tl.class_descript_num_seq,
	      tl.crime_ind
	FROM
	 	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tl
	WHERE EXISTS 
		   (SELECT 'X' FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code tll
	           WHERE tll.crrnt_snpsht_flag = 1 
	           and tll.class_code = tl.class_code
		       and tll.class_loc_code = tl.class_loc_code
		 and tll.class_descript_ind = tl.class_descript_ind
		 and tll.mco = tl.mco 
		 and tll.class_loc_state = tl.class_loc_state
		 and tll.subline = tl.subline
		 and tll.gtam_exp_date = tl.gtam_exp_date 
		 and tll.class_descript_num_seq   = tl.class_descript_num_seq           
	   and    tll.crime_ind =  tl.crime_ind
	           GROUP BY tll.source_sys_id,
		tll.class_code,
		tll.class_loc_code,
		tll.class_descript_ind,
		tll.mco,
		tll.class_loc_state,
		tll.subline,
		tll.gtam_exp_date,
		tll.class_descript_num_seq ,
	      tll.crime_ind
	           HAVING count(*) > 1)
	ORDER BY  
	tl.source_sys_id,
		tl.class_code,
		tl.class_loc_code,
		tl.class_descript_ind,
		tl.mco,
		tl.class_loc_state,
		tl.subline,
		tl.gtam_exp_date,
		tl.class_descript_num_seq,
	      tl.crime_ind,
	    tl.eff_from_date  DESC
),
EXP_Lag_Eff_dates AS (
	SELECT
	sup_class_code_id,
	source_sys_id,
	class_code,
	class_loc_code,
	class_descript_ind,
	mco,
	class_loc_state,
	subline,
	gtam_exp_date,
	class_descript_num_seq,
	crime_ind,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	source_sys_id = v_PREV_ROW_source_sys_id AND
	--     class_code = v_PREV_ROW_class_code AND
	--   class_loc_code  =  v_PREV_ROW_class_loc_code AND
	--    class_descript_ind  =  v_PREV_ROW_class_descript_ind AND
	--    mco =   v_PREV_ROW_mco AND
	--    class_loc_state =   v_PREV_ROW_class_loc_state AND
	--    subline  =   v_PREV_ROW_subline AND
	--    gtam_exp_date  =  v_PREV_ROW_gtam_exp_date AND
	-- class_descript_num_seq = v_PREV_ROW_class_descript_num_seq AND
	-- crime_ind = v_PREV_ROW_crime_ind
	-- , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		source_sys_id = v_PREV_ROW_source_sys_id 
		AND class_code = v_PREV_ROW_class_code 
		AND class_loc_code = v_PREV_ROW_class_loc_code 
		AND class_descript_ind = v_PREV_ROW_class_descript_ind 
		AND mco = v_PREV_ROW_mco 
		AND class_loc_state = v_PREV_ROW_class_loc_state 
		AND subline = v_PREV_ROW_subline 
		AND gtam_exp_date = v_PREV_ROW_gtam_exp_date 
		AND class_descript_num_seq = v_PREV_ROW_class_descript_num_seq 
		AND crime_ind = v_PREV_ROW_crime_ind, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	class_code AS v_PREV_ROW_class_code,
	class_loc_code AS v_PREV_ROW_class_loc_code,
	class_descript_ind AS v_PREV_ROW_class_descript_ind,
	mco AS v_PREV_ROW_mco,
	class_loc_state AS v_PREV_ROW_class_loc_state,
	subline AS v_PREV_ROW_subline,
	gtam_exp_date AS v_PREV_ROW_gtam_exp_date,
	class_descript_num_seq AS v_PREV_ROW_class_descript_num_seq,
	crime_ind AS v_PREV_ROW_crime_ind,
	SYSDATE AS modified_date,
	0 AS crrnt_snapshot_flag
	FROM SQ_sup_class_code_UPD
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_class_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snapshot_flag
	FROM EXP_Lag_Eff_dates
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_Sup_Class_Code AS (
	SELECT
	sup_class_code_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snapshot_flag
	FROM FIL_FirstRowInAKGroup
),
UPD_sup_class_code AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_class_code AS T
	USING UPD_Sup_Class_Code AS S
	ON T.sup_class_code_id = S.sup_class_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),