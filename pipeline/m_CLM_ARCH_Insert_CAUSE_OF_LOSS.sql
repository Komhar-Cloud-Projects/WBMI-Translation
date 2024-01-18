WITH
SQ_CAUSE_OF_LOSS_STAGE1 AS (
	SELECT CAUSE_OF_LOSS_STAGE.CAUSE_OF_LOSS_ID, CAUSE_OF_LOSS_STAGE.LINE_OF_BUSINESS, CAUSE_OF_LOSS_STAGE.MAJOR_PERIL, CAUSE_OF_LOSS_STAGE.CAUSE_OF_LOSS, CAUSE_OF_LOSS_STAGE.NUM_CAUSE_OF_LOSS, CAUSE_OF_LOSS_STAGE.ALPH_CAUSE_OF_LOSS, CAUSE_OF_LOSS_STAGE.ABBR_CAUSE_OF_LOSS, CAUSE_OF_LOSS_STAGE.CAUSE_OF_LOSS_NM, CAUSE_OF_LOSS_STAGE.BUREAU_NAME1, CAUSE_OF_LOSS_STAGE.BUR_CAUSE_OF_LOSS1, CAUSE_OF_LOSS_STAGE.BUR_TYPE_OF_LOSS1, CAUSE_OF_LOSS_STAGE.BUREAU_NAME2, CAUSE_OF_LOSS_STAGE.BUR_CAUSE_OF_LOSS2, CAUSE_OF_LOSS_STAGE.BUR_TYPE_OF_LOSS2, CAUSE_OF_LOSS_STAGE.BUREAU_NAME3, CAUSE_OF_LOSS_STAGE.BUR_CAUSE_OF_LOSS3, CAUSE_OF_LOSS_STAGE.BUR_TYPE_OF_LOSS3, CAUSE_OF_LOSS_STAGE.BUREAU_NAME4, CAUSE_OF_LOSS_STAGE.BUR_CAUSE_OF_LOSS4, CAUSE_OF_LOSS_STAGE.BUR_TYPE_OF_LOSS4, CAUSE_OF_LOSS_STAGE.BUREAU_NAME5, CAUSE_OF_LOSS_STAGE.BUR_CAUSE_OF_LOSS5, CAUSE_OF_LOSS_STAGE.BUR_TYPE_OF_LOSS5, CAUSE_OF_LOSS_STAGE.EXTRACT_DATE, CAUSE_OF_LOSS_STAGE.AS_OF_DATE, CAUSE_OF_LOSS_STAGE.RECORD_COUNT, CAUSE_OF_LOSS_STAGE.SOURCE_SYSTEM_ID ,
	CAUSE_OF_LOSS_STAGE.COV_CATEGORY_CODE 
	FROM
	 CAUSE_OF_LOSS_STAGE
),
EXP_CAUSE_OF_LOSS_STAGE AS (
	SELECT
	CAUSE_OF_LOSS_ID,
	LINE_OF_BUSINESS,
	MAJOR_PERIL,
	CAUSE_OF_LOSS,
	NUM_CAUSE_OF_LOSS,
	ALPH_CAUSE_OF_LOSS,
	ABBR_CAUSE_OF_LOSS,
	CAUSE_OF_LOSS_NM,
	BUREAU_NAME1,
	BUR_CAUSE_OF_LOSS1,
	BUR_TYPE_OF_LOSS1,
	BUREAU_NAME2,
	BUR_CAUSE_OF_LOSS2,
	BUR_TYPE_OF_LOSS2,
	BUREAU_NAME3,
	BUR_CAUSE_OF_LOSS3,
	BUR_TYPE_OF_LOSS3,
	BUREAU_NAME4,
	BUR_CAUSE_OF_LOSS4,
	BUR_TYPE_OF_LOSS4,
	BUREAU_NAME5,
	BUR_CAUSE_OF_LOSS5,
	BUR_TYPE_OF_LOSS5,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	cov_category_code
	FROM SQ_CAUSE_OF_LOSS_STAGE1
),
ARCH_CAUSE_OF_LOSS_STAGE AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ARCH_CAUSE_OF_LOSS_STAGE
	(cause_of_loss_id, line_of_business, major_peril, cause_of_loss, num_cause_of_loss, alph_cause_of_loss, abbr_cause_of_loss, cause_of_loss_nm, bureau_name1, bur_cause_of_loss1, bur_type_of_loss1, bureau_name2, bur_cause_of_loss2, bur_type_of_loss2, bureau_name3, bur_cause_of_loss3, bur_type_of_loss3, bureau_name4, bur_cause_of_loss4, bur_type_of_loss4, bureau_name5, bur_cause_of_loss5, bur_type_of_loss5, extract_date, as_of_date, record_count, source_system_id, audit_id, cov_category_code)
	SELECT 
	CAUSE_OF_LOSS_ID AS CAUSE_OF_LOSS_ID, 
	LINE_OF_BUSINESS AS LINE_OF_BUSINESS, 
	MAJOR_PERIL AS MAJOR_PERIL, 
	CAUSE_OF_LOSS AS CAUSE_OF_LOSS, 
	NUM_CAUSE_OF_LOSS AS NUM_CAUSE_OF_LOSS, 
	ALPH_CAUSE_OF_LOSS AS ALPH_CAUSE_OF_LOSS, 
	ABBR_CAUSE_OF_LOSS AS ABBR_CAUSE_OF_LOSS, 
	CAUSE_OF_LOSS_NM AS CAUSE_OF_LOSS_NM, 
	BUREAU_NAME1 AS BUREAU_NAME1, 
	BUR_CAUSE_OF_LOSS1 AS BUR_CAUSE_OF_LOSS1, 
	BUR_TYPE_OF_LOSS1 AS BUR_TYPE_OF_LOSS1, 
	BUREAU_NAME2 AS BUREAU_NAME2, 
	BUR_CAUSE_OF_LOSS2 AS BUR_CAUSE_OF_LOSS2, 
	BUR_TYPE_OF_LOSS2 AS BUR_TYPE_OF_LOSS2, 
	BUREAU_NAME3 AS BUREAU_NAME3, 
	BUR_CAUSE_OF_LOSS3 AS BUR_CAUSE_OF_LOSS3, 
	BUR_TYPE_OF_LOSS3 AS BUR_TYPE_OF_LOSS3, 
	BUREAU_NAME4 AS BUREAU_NAME4, 
	BUR_CAUSE_OF_LOSS4 AS BUR_CAUSE_OF_LOSS4, 
	BUR_TYPE_OF_LOSS4 AS BUR_TYPE_OF_LOSS4, 
	BUREAU_NAME5 AS BUREAU_NAME5, 
	BUR_CAUSE_OF_LOSS5 AS BUR_CAUSE_OF_LOSS5, 
	BUR_TYPE_OF_LOSS5 AS BUR_TYPE_OF_LOSS5, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID, 
	COV_CATEGORY_CODE
	FROM EXP_CAUSE_OF_LOSS_STAGE
),