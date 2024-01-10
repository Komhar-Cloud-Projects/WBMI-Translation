WITH
SQ_gtam_xtsa01_stage AS (
	select c.field_label as major_peril_code,
	             c.major_peril_translation as major_peril_description  
	       from      
	      (
	      SELECT 
	      CASE LEN(LTRIM(RTRIM( a.field_label)))
	         WHEN 13 then SUBSTRING( RTRIM(LTRIM(a.field_label)),11,3)
	         ELSE a.field_label          
	      END  as field_label,
	      a.major_peril_translation as major_peril_translation         
	      FROM gtam_xtsa01_stage a  ) c
	     
	
	--- GET the last 3 characters of the column for the major_peril_code
),
EXP_Default_Values_xtsa01 AS (
	SELECT
	field_label,
	-- *INF*: IIF(ISNULL( rtrim(ltrim(field_label))), 'N/A', rtrim(ltrim( field_label)))
	IFF(rtrim(ltrim(field_label
			)
		) IS NULL,
		'N/A',
		rtrim(ltrim(field_label
			)
		)
	) AS MAJOR_PERIL_OUT,
	major_peril_translation,
	-- *INF*: IIF(ISNULL(rtrim(ltrim(major_peril_translation))), 'N/A', rtrim(ltrim(major_peril_translation)))
	IFF(rtrim(ltrim(major_peril_translation
			)
		) IS NULL,
		'N/A',
		rtrim(ltrim(major_peril_translation
			)
		)
	) AS LONG_ALPHABETIC_DESCRIPTION_OUT
	FROM SQ_gtam_xtsa01_stage
),
LKP_sup_major_peril_xtsa01 AS (
	SELECT
	sup_major_peril_id,
	major_peril_descript,
	major_peril_code
	FROM (
		SELECT sup_major_peril.sup_major_peril_id as sup_major_peril_id, 
		sup_major_peril.major_peril_descript as major_peril_descript, 
		ltrim(rtrim(sup_major_peril.major_peril_code)) as major_peril_code
		 FROM sup_major_peril where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril_code ORDER BY sup_major_peril_id) = 1
),
EXP_detect_changes_xtsa01 AS (
	SELECT
	LKP_sup_major_peril_xtsa01.sup_major_peril_id AS LKP_sup_major_peril_id,
	LKP_sup_major_peril_xtsa01.major_peril_descript AS LKP_major_peril_descript,
	EXP_Default_Values_xtsa01.MAJOR_PERIL_OUT,
	EXP_Default_Values_xtsa01.LONG_ALPHABETIC_DESCRIPTION_OUT,
	-- *INF*: IIF(ISNULL(LKP_sup_major_peril_id), 'NEW', IIF(LTRIM(RTRIM(LKP_major_peril_descript)) <> (LTRIM(RTRIM(LONG_ALPHABETIC_DESCRIPTION_OUT))), 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(LKP_sup_major_peril_id IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(LKP_major_peril_descript
				)
			) <> ( LTRIM(RTRIM(LONG_ALPHABETIC_DESCRIPTION_OUT
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_CHANGED_FLAG,
	v_CHANGED_FLAG AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_CHANGED_FLAG = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values_xtsa01
	LEFT JOIN LKP_sup_major_peril_xtsa01
	ON LKP_sup_major_peril_xtsa01.major_peril_code = EXP_Default_Values_xtsa01.MAJOR_PERIL_OUT
),
FIL_sup_insurance_line_INS_xtsa01 AS (
	SELECT
	MAJOR_PERIL_OUT, 
	LONG_ALPHABETIC_DESCRIPTION_OUT, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id
	FROM EXP_detect_changes_xtsa01
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
INS_sup_major_peril_xtsa01 AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_major_peril
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, major_peril_code, major_peril_descript, StandardMajorPerilCode, StandardMajorPerilDescription)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	MAJOR_PERIL_OUT AS MAJOR_PERIL_CODE, 
	LONG_ALPHABETIC_DESCRIPTION_OUT AS MAJOR_PERIL_DESCRIPT, 
	MAJOR_PERIL_OUT AS STANDARDMAJORPERILCODE, 
	LONG_ALPHABETIC_DESCRIPTION_OUT AS STANDARDMAJORPERILDESCRIPTION
	FROM FIL_sup_insurance_line_INS_xtsa01
),
SQ_sup_major_peril AS (
	SELECT a.sup_major_peril_id, 
	a.eff_from_date, 
	a.eff_to_date ,
	a.major_peril_code
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_major_peril a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_major_peril b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.major_peril_code = b.major_peril_code
	             GROUP BY b.major_peril_code
			HAVING COUNT(*) > 1)
	ORDER BY a.major_peril_code ,a.eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_major_peril_id,
	major_peril_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	major_peril_code= v_Prev_row_major_peril_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		major_peril_code = v_Prev_row_major_peril_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	major_peril_code AS v_Prev_row_major_peril_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_major_peril
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_major_peril_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_major_peril AS (
	SELECT
	sup_major_peril_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
UPD_sup_major_peril AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_major_peril AS T
	USING UPD_sup_major_peril AS S
	ON T.sup_major_peril_id = S.sup_major_peril_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),