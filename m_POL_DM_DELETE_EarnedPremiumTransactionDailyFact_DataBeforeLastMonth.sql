WITH
SQ_EarnedPremiumDailyCalculation AS (
	select 1 as EarnedPremiumDailyCalculationID,
	'@{pipeline().parameters.TARGET_TABLE_OWNER}' as SOURCE_TABLE_OWNER
),
SQL_DeleteDataBeforeLastMonth_EarnedPremiumTransactionDailyFact AS (-- SQL_DeleteDataBeforeLastMonth_EarnedPremiumTransactionDailyFact

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
FIL_All AS (
	SELECT
	EarnedPremiumDailyCalculationID_output AS EarnedPremiumDailyCalculationId
	FROM SQL_DeleteDataBeforeLastMonth_EarnedPremiumTransactionDailyFact
	WHERE FALSE
),
EarnedPremiumTransactionDailyFact_default AS (
	INSERT INTO EarnedPremiumTransactionDailyFact
	(EDWEarnedPremiumDailyCalculationPKID)
	SELECT 
	EarnedPremiumDailyCalculationId AS EDWEARNEDPREMIUMDAILYCALCULATIONPKID
	FROM FIL_All
),