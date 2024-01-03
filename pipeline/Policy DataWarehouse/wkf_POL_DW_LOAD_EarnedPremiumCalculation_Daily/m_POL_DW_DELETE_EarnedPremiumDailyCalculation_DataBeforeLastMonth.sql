WITH
SQ_EarnedPremiumDailyCalculation AS (
	select 1 as EarnedPremiumDailyCalculationID,
	'@{pipeline().parameters.SOURCE_TABLE_OWNER}' as SOURCE_TABLE_OWNER
),
SQL_DeleteDataBeforeLastMonth_EarnedPremiumDailyCalculation AS (-- SQL_DeleteDataBeforeLastMonth_EarnedPremiumDailyCalculation

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
SQL_DeleteDataBeforeLastMonth_WorkEarnedPremiumCoverageDaily AS (-- SQL_DeleteDataBeforeLastMonth_WorkEarnedPremiumCoverageDaily

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
SQL_DeleteDataBeforeLastMonth_WorkFirstAuditDaily AS (-- SQL_DeleteDataBeforeLastMonth_WorkFirstAuditDaily

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
Union_Default AS (
	SELECT EarnedPremiumDailyCalculationID_output AS EarnedPremiumDailyCalculationId
	FROM SQL_DeleteDataBeforeLastMonth_EarnedPremiumDailyCalculation
	UNION
	SELECT EarnedPremiumDailyCalculationID_output AS EarnedPremiumDailyCalculationId
	FROM SQL_DeleteDataBeforeLastMonth_WorkEarnedPremiumCoverageDaily
	UNION
	SELECT EarnedPremiumDailyCalculationID_output AS EarnedPremiumDailyCalculationId
	FROM SQL_DeleteDataBeforeLastMonth_WorkFirstAuditDaily
),
FIL_All AS (
	SELECT
	EarnedPremiumDailyCalculationId
	FROM Union_Default
	WHERE FALSE
),
TGT_EarnedPremiumDailyCalculation_Default AS (
	INSERT INTO EarnedPremiumDailyCalculation
	(EarnedPremiumDailyCalculationID)
	SELECT 
	EarnedPremiumDailyCalculationId AS EARNEDPREMIUMDAILYCALCULATIONID
	FROM FIL_All
),