WITH
SQ_gtam_wbmtmrkt_stage AS (
	SELECT
		gtam_wbmtmrkt_stage_id,
		abbreviation_of_target_mkt,
		date_field1,
		date_field2,
		description_of_target_mkt,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbmtmrkt_stage
),
LKP_arch_gtam_wbmtmrkt_stage AS (
	SELECT
	arch_wbmtmrkt_stage_id,
	gtam_wbmtmrkt_stage_id,
	date_field1,
	date_field2,
	description_of_target_mkt,
	in_abbreviation_of_target_mkt,
	abbreviation_of_target_mkt
	FROM (
		SELECT arch_gtam_wbmtmrkt_stage.arch_wbmtmrkt_stage_id as arch_wbmtmrkt_stage_id, arch_gtam_wbmtmrkt_stage.gtam_wbmtmrkt_stage_id as gtam_wbmtmrkt_stage_id, arch_gtam_wbmtmrkt_stage.date_field1 as date_field1, arch_gtam_wbmtmrkt_stage.date_field2 as date_field2, arch_gtam_wbmtmrkt_stage.description_of_target_mkt as description_of_target_mkt, arch_gtam_wbmtmrkt_stage.abbreviation_of_target_mkt as abbreviation_of_target_mkt FROM arch_gtam_wbmtmrkt_stage
		where 	arch_gtam_wbmtmrkt_stage.arch_wbmtmrkt_stage_id In
			(Select max(arch_wbmtmrkt_stage_id) from arch_gtam_wbmtmrkt_stage b
			group by  b.abbreviation_of_target_mkt)
		order by  arch_gtam_wbmtmrkt_stage.abbreviation_of_target_mkt--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY abbreviation_of_target_mkt ORDER BY arch_wbmtmrkt_stage_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_gtam_wbmtmrkt_stage.gtam_wbmtmrkt_stage_id,
	SQ_gtam_wbmtmrkt_stage.abbreviation_of_target_mkt,
	SQ_gtam_wbmtmrkt_stage.date_field1,
	SQ_gtam_wbmtmrkt_stage.date_field2,
	SQ_gtam_wbmtmrkt_stage.description_of_target_mkt,
	SQ_gtam_wbmtmrkt_stage.extract_date,
	SQ_gtam_wbmtmrkt_stage.as_of_date,
	SQ_gtam_wbmtmrkt_stage.record_count,
	SQ_gtam_wbmtmrkt_stage.source_system_id,
	LKP_arch_gtam_wbmtmrkt_stage.arch_wbmtmrkt_stage_id AS old_arch_wbmtmrkt_stage_id,
	LKP_arch_gtam_wbmtmrkt_stage.date_field1 AS old_date_field1,
	LKP_arch_gtam_wbmtmrkt_stage.date_field2 AS old_date_field2,
	LKP_arch_gtam_wbmtmrkt_stage.description_of_target_mkt AS old_description_of_target_mkt,
	-- *INF*: iif(isnull(old_arch_wbmtmrkt_stage_id),'NEW',
	-- 	iif((old_date_field1 <> date_field1) OR
	--              (old_date_field2 <> date_field2) OR
	--              (old_description_of_target_mkt <> description_of_target_mkt),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(
	    old_arch_wbmtmrkt_stage_id IS NULL, 'NEW',
	    IFF(
	        (old_date_field1 <> date_field1)
	        or (old_date_field2 <> date_field2)
	        or (old_description_of_target_mkt <> description_of_target_mkt),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID
	FROM SQ_gtam_wbmtmrkt_stage
	LEFT JOIN LKP_arch_gtam_wbmtmrkt_stage
	ON LKP_arch_gtam_wbmtmrkt_stage.abbreviation_of_target_mkt = SQ_gtam_wbmtmrkt_stage.abbreviation_of_target_mkt
),
FIL_Inserts AS (
	SELECT
	gtam_wbmtmrkt_stage_id, 
	abbreviation_of_target_mkt, 
	date_field1, 
	date_field2, 
	description_of_target_mkt, 
	extract_date, 
	as_of_date, 
	record_count, 
	source_system_id, 
	Changed_Flag, 
	Audit_ID
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW' or Changed_Flag='UPDATE'
),
arch_gtam_wbmtmrkt_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_wbmtmrkt_stage
	(gtam_wbmtmrkt_stage_id, abbreviation_of_target_mkt, date_field1, date_field2, description_of_target_mkt, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_WBMTMRKT_STAGE_ID, 
	ABBREVIATION_OF_TARGET_MKT, 
	DATE_FIELD1, 
	DATE_FIELD2, 
	DESCRIPTION_OF_TARGET_MKT, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Audit_ID AS AUDIT_ID
	FROM FIL_Inserts
),