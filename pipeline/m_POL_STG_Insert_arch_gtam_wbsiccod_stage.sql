WITH
SQ_gtam_wbsiccod_stage AS (
	SELECT
		gtam_wbsiccod_stage_id,
		sic_code_number,
		sic_code_description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbsiccod_stage
),
LKP_arch_gtam_wbsiccod_stage AS (
	SELECT
	arch_gtam_wbsiccod_stage_id,
	gtam_wbsiccod_stage_id,
	sic_code_description,
	in_sic_code_number,
	sic_code_number
	FROM (
		SELECT arch_gtam_wbsiccod_stage.arch_gtam_wbsiccod_stage_id as arch_gtam_wbsiccod_stage_id, arch_gtam_wbsiccod_stage.gtam_wbsiccod_stage_id as gtam_wbsiccod_stage_id, arch_gtam_wbsiccod_stage.sic_code_description as sic_code_description, arch_gtam_wbsiccod_stage.sic_code_number as sic_code_number FROM arch_gtam_wbsiccod_stage
		where 	arch_gtam_wbsiccod_stage.arch_gtam_wbsiccod_stage_id In
			(Select max(arch_gtam_wbsiccod_stage_id) from arch_gtam_wbsiccod_stage b
			group by b.sic_code_number)
		order by arch_gtam_wbsiccod_stage.sic_code_number--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sic_code_number ORDER BY arch_gtam_wbsiccod_stage_id) = 1
),
EXP_arch_wbsiccod_stage AS (
	SELECT
	SQ_gtam_wbsiccod_stage.gtam_wbsiccod_stage_id,
	SQ_gtam_wbsiccod_stage.sic_code_number,
	SQ_gtam_wbsiccod_stage.sic_code_description,
	SQ_gtam_wbsiccod_stage.extract_date AS EXTRACT_DATE,
	SQ_gtam_wbsiccod_stage.as_of_date AS AS_OF_DATE,
	SQ_gtam_wbsiccod_stage.record_count AS RECORD_COUNT,
	SQ_gtam_wbsiccod_stage.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_wbsiccod_stage.arch_gtam_wbsiccod_stage_id AS old_arch_gtam_wbsiccod_stage_id,
	LKP_arch_gtam_wbsiccod_stage.sic_code_description AS old_sic_code_description,
	-- *INF*: iif(isnull(old_arch_gtam_wbsiccod_stage_id),'NEW',
	--     iif((old_sic_code_description<>sic_code_description),'UPDATE','NOCHANGE'))
	IFF(
	    old_arch_gtam_wbsiccod_stage_id IS NULL, 'NEW',
	    IFF(
	        (old_sic_code_description <> sic_code_description), 'UPDATE', 'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_flag AS Changed_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_wbsiccod_stage
	LEFT JOIN LKP_arch_gtam_wbsiccod_stage
	ON LKP_arch_gtam_wbsiccod_stage.sic_code_number = SQ_gtam_wbsiccod_stage.sic_code_number
),
FIL_Inserts AS (
	SELECT
	gtam_wbsiccod_stage_id, 
	sic_code_number, 
	sic_code_description, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_flag, 
	AUDIT_ID
	FROM EXP_arch_wbsiccod_stage
	WHERE Changed_flag = 'NEW' or Changed_flag = 'UPDATE'
),
arch_gtam_wbsiccod_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_wbsiccod_stage
	(gtam_wbsiccod_stage_id, sic_code_number, sic_code_description, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_WBSICCOD_STAGE_ID, 
	SIC_CODE_NUMBER, 
	SIC_CODE_DESCRIPTION, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),