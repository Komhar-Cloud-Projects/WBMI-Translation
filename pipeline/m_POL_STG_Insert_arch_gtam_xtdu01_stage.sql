WITH
SQ_gtam_xtdu01_stage AS (
	SELECT
		gtam_xtdu01_stage_id,
		field_heading,
		xtdu01_code,
		verbal_description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_xtdu01_stage
),
LKP_arch_gtam_xtud01_stage AS (
	SELECT
	arch_gtam_xtdu01_stage_id,
	verbal_description,
	xtdu01_code,
	field_heading
	FROM (
		SELECT 
		arch_gtam_xtdu01_stage.arch_gtam_xtdu01_stage_id as arch_gtam_xtdu01_stage_id, 
		arch_gtam_xtdu01_stage.verbal_description as verbal_description, 
		arch_gtam_xtdu01_stage.xtdu01_code as xtdu01_code, 
		arch_gtam_xtdu01_stage.field_heading as field_heading 
		FROM 
		arch_gtam_xtdu01_stage
		where arch_gtam_xtdu01_stage_id in (
		select max(arch_gtam_xtdu01_stage_id)
		from arch_gtam_xtdu01_stage b
		group by 
		b.xtdu01_code,
		b.field_heading)
		order by 
		arch_gtam_xtdu01_stage.xtdu01_code,
		arch_gtam_xtdu01_stage.field_heading --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY xtdu01_code,field_heading ORDER BY arch_gtam_xtdu01_stage_id) = 1
),
EXP_arch_gtam_xtdu01_stage AS (
	SELECT
	LKP_arch_gtam_xtud01_stage.arch_gtam_xtdu01_stage_id AS lkp_arch_gtam_xtdu01_stage_id,
	LKP_arch_gtam_xtud01_stage.verbal_description AS lkp_verbal_description,
	SQ_gtam_xtdu01_stage.gtam_xtdu01_stage_id,
	SQ_gtam_xtdu01_stage.field_heading,
	SQ_gtam_xtdu01_stage.xtdu01_code,
	SQ_gtam_xtdu01_stage.verbal_description,
	-- *INF*: iif(isnull(lkp_arch_gtam_xtdu01_stage_id),'NEW',IIF(lkp_verbal_description != verbal_description,'UPDATE','NOCHANGE'))
	IFF(
	    lkp_arch_gtam_xtdu01_stage_id IS NULL, 'NEW',
	    IFF(
	        lkp_verbal_description != verbal_description, 'UPDATE', 'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	SYSDATE AS extract_date,
	SYSDATE AS as_of_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_gtam_xtdu01_stage
	LEFT JOIN LKP_arch_gtam_xtud01_stage
	ON LKP_arch_gtam_xtud01_stage.xtdu01_code = SQ_gtam_xtdu01_stage.xtdu01_code AND LKP_arch_gtam_xtud01_stage.field_heading = SQ_gtam_xtdu01_stage.field_heading
),
FIL_arch_gtam_xtdu01_stage AS (
	SELECT
	gtam_xtdu01_stage_id, 
	field_heading, 
	xtdu01_code, 
	verbal_description, 
	changed_flag, 
	extract_date, 
	as_of_date, 
	source_system_id, 
	audit_id
	FROM EXP_arch_gtam_xtdu01_stage
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
TGT_arch_gtam_xtdu01_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_xtdu01_stage
	(gtam_xtdu01_stage_id, field_heading, xtdu01_code, verbal_description, extract_date, as_of_date, source_system_id, audit_id)
	SELECT 
	GTAM_XTDU01_STAGE_ID, 
	FIELD_HEADING, 
	XTDU01_CODE, 
	VERBAL_DESCRIPTION, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM FIL_arch_gtam_xtdu01_stage
),