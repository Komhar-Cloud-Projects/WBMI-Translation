WITH
SQ_gtam_wbclb_stage AS (
	SELECT
		gtam_wbclb_stage_id,
		prog_code,
		prog_description,
		prog_type,
		inactive_ind,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbclb_stage
),
LKP_arch_gtam_wbclb_stage AS (
	SELECT
	arch_gtam_wbclb_stage_id,
	gtam_wbclb_stage_id,
	prog_description,
	prog_type,
	inactive_ind,
	in_prog_code,
	prog_code
	FROM (
		SELECT arch_gtam_wbclb_stage.arch_gtam_wbclb_stage_id as arch_gtam_wbclb_stage_id, arch_gtam_wbclb_stage.gtam_wbclb_stage_id as gtam_wbclb_stage_id, arch_gtam_wbclb_stage.prog_description as prog_description, arch_gtam_wbclb_stage.prog_type as prog_type, arch_gtam_wbclb_stage.inactive_ind as inactive_ind, arch_gtam_wbclb_stage.prog_code as prog_code FROM arch_gtam_wbclb_stage
		where 	arch_gtam_wbclb_stage.arch_gtam_wbclb_stage_id In
			(Select max(arch_gtam_wbclb_stage_id) from arch_gtam_wbclb_stage b
			group by b.prog_code)
		order by arch_gtam_wbclb_stage.prog_code--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prog_code ORDER BY arch_gtam_wbclb_stage_id) = 1
),
EXP_arch_wbclb_stage AS (
	SELECT
	SQ_gtam_wbclb_stage.gtam_wbclb_stage_id,
	SQ_gtam_wbclb_stage.prog_code,
	SQ_gtam_wbclb_stage.prog_description,
	SQ_gtam_wbclb_stage.prog_type,
	SQ_gtam_wbclb_stage.inactive_ind,
	SQ_gtam_wbclb_stage.extract_date AS EXTRACT_DATE,
	SQ_gtam_wbclb_stage.as_of_date AS AS_OF_DATE,
	SQ_gtam_wbclb_stage.record_count AS RECORD_COUNT,
	SQ_gtam_wbclb_stage.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_wbclb_stage.arch_gtam_wbclb_stage_id AS old_arch_gtam_wbclb_stage_id,
	LKP_arch_gtam_wbclb_stage.prog_description AS old_prog_description,
	LKP_arch_gtam_wbclb_stage.prog_type AS old_prog_type,
	LKP_arch_gtam_wbclb_stage.inactive_ind AS old_inactive_ind,
	-- *INF*: iif(isnull(old_arch_gtam_wbclb_stage_id) , 'NEW',
	--      iif((old_prog_description<>prog_description) OR
	--           (old_prog_type <> prog_type) OR
	--           (old_inactive_ind <> inactive_ind), 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(
	    old_arch_gtam_wbclb_stage_id IS NULL, 'NEW',
	    IFF(
	        (old_prog_description <> prog_description)
	        or (old_prog_type <> prog_type)
	        or (old_inactive_ind <> inactive_ind),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_wbclb_stage
	LEFT JOIN LKP_arch_gtam_wbclb_stage
	ON LKP_arch_gtam_wbclb_stage.prog_code = SQ_gtam_wbclb_stage.prog_code
),
FIL_Inserts AS (
	SELECT
	gtam_wbclb_stage_id, 
	prog_code, 
	prog_description, 
	prog_type, 
	inactive_ind, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_wbclb_stage
	WHERE Changed_Flag = 'NEW' OR Changed_Flag = 'UPDATE'
),
arch_gtam_wbclb_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_wbclb_stage
	(gtam_wbclb_stage_id, prog_code, prog_description, prog_type, inactive_ind, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_WBCLB_STAGE_ID, 
	PROG_CODE, 
	PROG_DESCRIPTION, 
	PROG_TYPE, 
	INACTIVE_IND, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),