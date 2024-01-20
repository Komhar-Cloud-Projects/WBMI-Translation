WITH
SQ_WorkControlRules AS (
	SELECT
		WorkControlRulesId,
		CreatedDate,
		ModifiedDate,
		UserName,
		ControlRules,
		Comments
	FROM WorkControlRules
	WHERE WorkControlRules.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}' and WorkControlRules.CreatedDate=(select max(CreatedDate) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkControlRules)
),
EXP_values AS (
	SELECT
	WorkControlRulesId,
	CreatedDate,
	ModifiedDate,
	UserName,
	ControlRules,
	Comments
	FROM SQ_WorkControlRules
),
SQL_control_rules AS (-- SQL_control_rules

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_date AS (
	SELECT
	CreatedDate_output,
	ModifiedDate_output,
	o_controlkeyvalue AS ControlKeyValue,
	out_key AS keytype,
	Comments_output AS Comments
	FROM SQL_control_rules
),
FLT_ControlRules AS (
	SELECT
	CreatedDate_output AS createdDate, 
	ModifiedDate_output AS modifiedDate, 
	ControlKeyValue, 
	keytype, 
	Comments
	FROM EXP_date
	WHERE not isnull(ControlKeyValue )
),
TGT_WorkControlKey AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkControlKey;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkControlKey
	(CreatedDate, ModifiedDate, ControlKeyValue, KeyType, Comments)
	SELECT 
	createdDate AS CREATEDDATE, 
	modifiedDate AS MODIFIEDDATE, 
	CONTROLKEYVALUE, 
	keytype AS KEYTYPE, 
	COMMENTS
	FROM FLT_ControlRules
),