WITH
SQ_gtam_tl79_stage AS (
	SELECT
		gtam_tl79_stage_id,
		location,
		master_company_number,
		lineof_business,
		legal_entity,
		legal_entity_literal,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tl79_stage
),
LKP_arch_gtam_tl79_stage AS (
	SELECT
	arch_gtam_tl79_stage_id,
	legal_entity_literal,
	location,
	master_company_number,
	lineof_business,
	legal_entity
	FROM (
		SELECT 
		arch_gtam_tl79_stage.arch_gtam_tl79_stage_id as arch_gtam_tl79_stage_id,
		 arch_gtam_tl79_stage.legal_entity_literal as legal_entity_literal, 
		arch_gtam_tl79_stage.location as location, 
		arch_gtam_tl79_stage.master_company_number as master_company_number, 
		arch_gtam_tl79_stage.lineof_business as lineof_business, 
		arch_gtam_tl79_stage.legal_entity as legal_entity 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_tl79_stage
		where arch_gtam_tl79_stage.arch_gtam_tl79_stage_id in 
		(select max(arch_gtam_tl79_stage_id)
		from arch_gtam_tl79_stage b
		group by 
		b.location,
		b.master_company_number,
		b.lineof_business,
		b.legal_entity)
		order by 
		arch_gtam_tl79_stage.location, 
		arch_gtam_tl79_stage.master_company_number, 
		arch_gtam_tl79_stage.lineof_business,
		arch_gtam_tl79_stage.legal_entity --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY location,master_company_number,lineof_business,legal_entity ORDER BY arch_gtam_tl79_stage_id) = 1
),
EXP_arch_gtam_tl79_stage AS (
	SELECT
	LKP_arch_gtam_tl79_stage.arch_gtam_tl79_stage_id AS lkp_arch_gtam_tl79_stage_id,
	LKP_arch_gtam_tl79_stage.legal_entity_literal AS lkp_legal_entity_literal,
	SQ_gtam_tl79_stage.gtam_tl79_stage_id,
	SQ_gtam_tl79_stage.location,
	SQ_gtam_tl79_stage.master_company_number,
	SQ_gtam_tl79_stage.lineof_business,
	SQ_gtam_tl79_stage.legal_entity,
	SQ_gtam_tl79_stage.legal_entity_literal,
	-- *INF*: iif(isnull(lkp_arch_gtam_tl79_stage_id),'NEW',
	-- 	iif(ltrim(rtrim(lkp_legal_entity_literal)) <> ltrim(rtrim(legal_entity_literal)) ,
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(
	    lkp_arch_gtam_tl79_stage_id IS NULL, 'NEW',
	    IFF(
	        ltrim(rtrim(lkp_legal_entity_literal)) <> ltrim(rtrim(legal_entity_literal)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	SYSDATE AS extract_date,
	SYSDATE AS as_of_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_gtam_tl79_stage
	LEFT JOIN LKP_arch_gtam_tl79_stage
	ON LKP_arch_gtam_tl79_stage.location = SQ_gtam_tl79_stage.location AND LKP_arch_gtam_tl79_stage.master_company_number = SQ_gtam_tl79_stage.master_company_number AND LKP_arch_gtam_tl79_stage.lineof_business = SQ_gtam_tl79_stage.lineof_business AND LKP_arch_gtam_tl79_stage.legal_entity = SQ_gtam_tl79_stage.legal_entity
),
FIL_arch_gtam_tl79_stage AS (
	SELECT
	gtam_tl79_stage_id, 
	location, 
	master_company_number, 
	lineof_business, 
	legal_entity, 
	legal_entity_literal, 
	changed_flag, 
	extract_date, 
	as_of_date, 
	source_system_id, 
	audit_id
	FROM EXP_arch_gtam_tl79_stage
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
TGT_arch_gtam_tl79_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_tl79_stage
	(gtam_tl79_stage_id, location, master_company_number, lineof_business, legal_entity, legal_entity_literal, extract_date, as_of_date, source_system_id, audit_id)
	SELECT 
	GTAM_TL79_STAGE_ID, 
	LOCATION, 
	MASTER_COMPANY_NUMBER, 
	LINEOF_BUSINESS, 
	LEGAL_ENTITY, 
	LEGAL_ENTITY_LITERAL, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM FIL_arch_gtam_tl79_stage
),