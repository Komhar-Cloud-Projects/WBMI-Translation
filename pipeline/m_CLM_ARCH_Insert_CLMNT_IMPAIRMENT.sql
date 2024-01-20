WITH
SQ_clmnt_impairment_stage AS (
	SELECT
		clmnt_impairment_stage_id,
		claim_nbr,
		client_id,
		seq_nbr,
		body_part_code,
		impair_percentage,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clmnt_impairment_stage
),
EXP_CLMNT_IMPAIRMENT_STAGE AS (
	SELECT
	claim_nbr,
	client_id,
	seq_nbr,
	body_part_code,
	impair_percentage,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_clmnt_impairment_stage
),
arch_clmnt_impairment_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_clmnt_impairment_stage
	(claim_nbr, client_id, seq_nbr, body_part_code, impair_percentage, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	CLAIM_NBR, 
	CLIENT_ID, 
	SEQ_NBR, 
	BODY_PART_CODE, 
	IMPAIR_PERCENTAGE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID
	FROM EXP_CLMNT_IMPAIRMENT_STAGE
),