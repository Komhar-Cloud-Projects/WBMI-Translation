WITH
SQ_WB_BIL_DisbursementActivity AS (
	SELECT
		DisbursementActivityId,
		ModifiedUserId,
		ModifiedDate,
		DisbursementId,
		CheckNumber,
		TransactionDate,
		DisbursementStatusCode,
		ProcessedStatusCode,
		ErrorDescription
	FROM WB_BIL_DisbursementActivity
),
EXP_Batch_Web_Service AS (
	SELECT
	DisbursementActivityId,
	-- *INF*: IIF(ISNULL(var_SequenceNumber), 0, var_SequenceNumber + 1)
	IFF(var_SequenceNumber IS NULL, 0, var_SequenceNumber + 1) AS var_SequenceNumber,
	-- *INF*: IIF(ISNULL(var_BatchNumber),
	--  1, 
	--  IIF(MOD(var_SequenceNumber, @{pipeline().parameters.DUCKCREEKFACADESERVICE_BATCHSIZE})  != 1,
	--   var_BatchNumber,
	--   var_BatchNumber + 1))
	IFF(
	    var_BatchNumber IS NULL, 1,
	    IFF(
	        MOD(var_SequenceNumber, @{pipeline().parameters.DUCKCREEKFACADESERVICE_BATCHSIZE}) != 1, var_BatchNumber,
	        var_BatchNumber + 1
	    )
	) AS var_BatchNumber,
	var_SequenceNumber AS out_SequenceNumber,
	var_BatchNumber AS out_BatchNumber
	FROM SQ_WB_BIL_DisbursementActivity
),
AGG_Batch_Web_Service AS (
	SELECT
	out_BatchNumber AS BatchNumber
	FROM EXP_Batch_Web_Service
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BatchNumber ORDER BY NULL) = 1
),
ProcessDisbursementActivity AS (-- ProcessDisbursementActivity

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
FIL_ErrorMessage AS (
	SELECT
	tns2_ErrorMessage AS ServiceErrorMessage
	FROM ProcessDisbursementActivity
	WHERE NOT ISNULL(ServiceErrorMessage)
),
EXP_RegisterWorkFlowError AS (
	SELECT
	ServiceErrorMessage,
	-- *INF*: ERROR(ServiceErrorMessage)
	ERROR(ServiceErrorMessage) AS WorkFlowError
	FROM FIL_ErrorMessage
),
ServiceError_Not_Used AS (
	INSERT INTO ServiceError
	(ErrorMessage)
	SELECT 
	ServiceErrorMessage AS ERRORMESSAGE
	FROM EXP_RegisterWorkFlowError
),
ServiceError AS (
	INSERT INTO ServiceError
	(ErrorMessage)
	SELECT 
	tns2_ErrorMessage AS ERRORMESSAGE
	FROM ProcessDisbursementActivity
),