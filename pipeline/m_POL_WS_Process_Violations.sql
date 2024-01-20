WITH
SQ_DriverStage AS (
	SELECT
		DriverStageId,
		CreatedDate,
		ModifiedDate,
		FileCreationDate,
		FileIDCode,
		SenderName,
		LicenseState,
		LicenseNumber,
		LastName,
		FirstName,
		MiddleName,
		StreetAddress,
		City,
		StateCode,
		ZipCode,
		BirthDate,
		Gender,
		BodyWeight,
		EyeColor,
		PolicyNumber,
		PolicyExpirationDate,
		QuoteBackPolicyNumber,
		QuoteBackAgencyNumber,
		QuoteBackLineOfBusiness,
		QuoteBackDriverId,
		QuoteBackState,
		QuoteBackUnderwriterNumber,
		QuoteBack,
		InsuranceIndicator,
		AccountNumber,
		RejectSource,
		RejectReason,
		RejectCount,
		SupProcessStatusId,
		ErrorDescription,
		IncidentSequence,
		Height,
		PreviousLicense
	FROM DriverStage
	WHERE DriverStage.SupProcessStatusId IN (SELECT SupProcessStatusId FROM DCTtoWBStaging.DBO.SupProcessStatus 
	WHERE ProcessStatus = 'Request')
),
EXP_Violations AS (
	SELECT
	DriverStageId,
	CreatedDate,
	ModifiedDate,
	FileCreationDate,
	FileIDCode,
	SenderName,
	LicenseState,
	LicenseNumber,
	LastName,
	FirstName,
	MiddleName,
	StreetAddress,
	City,
	StateCode,
	ZipCode,
	BirthDate,
	Gender,
	BodyWeight,
	EyeColor,
	PolicyNumber,
	PolicyExpirationDate,
	QuoteBackPolicyNumber,
	QuoteBackAgencyNumber,
	QuoteBackLineOfBusiness,
	QuoteBackDriverId,
	QuoteBackState,
	QuoteBackUnderwriterNumber,
	QuoteBack,
	InsuranceIndicator,
	AccountNumber,
	RejectSource,
	RejectReason,
	RejectCount,
	SupProcessStatusId,
	ErrorDescription,
	IncidentSequence,
	Height,
	PreviousLicense,
	-- *INF*: SYSTIMESTAMP() || 'batchnumber' || TO_CHAR(v_batch_number)
	CURRENT_TIMESTAMP() || 'batchnumber' || TO_CHAR(v_batch_number) AS out_EARSBatch,
	-- *INF*: IIF(v_SeqNumber = 0,@{pipeline().parameters.BATCHSIZE},v_SeqNumber + 1)
	IFF(v_SeqNumber = 0, @{pipeline().parameters.BATCHSIZE}, v_SeqNumber + 1) AS v_SeqNumber,
	-- *INF*: TRUNC(v_batch_number/ @{pipeline().parameters.BATCHSIZE},0)
	TRUNC(v_batch_number / @{pipeline().parameters.BATCHSIZE},0) AS v_batch_number,
	v_batch_number AS out_BatchNumber,
	v_SeqNumber AS out_SeqNumber
	FROM SQ_DriverStage
),
AGGTRANS AS (
	SELECT
	out_BatchNumber,
	out_EARSBatch,
	out_SeqNumber
	FROM EXP_Violations
	QUALIFY ROW_NUMBER() OVER (PARTITION BY out_BatchNumber ORDER BY NULL) = 1
),
SEQ_DriverID AS (
	CREATE SEQUENCE SEQ_DriverID
	START = 0
	INCREMENT = 1;
),
wsc_DuckCreekFacadeService_AddEARSEntities AS (-- wsc_DuckCreekFacadeService_AddEARSEntities

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
FIL_ErrorMessage AS (
	SELECT
	tns2_ErrorMessage AS ServiceErrorMessage
	FROM wsc_DuckCreekFacadeService_AddEARSEntities
	WHERE NOT ISNULL(ServiceErrorMessage)
),
ServiceError AS (
	INSERT INTO ServiceError
	(ErrorMessage)
	SELECT 
	ServiceErrorMessage AS ERRORMESSAGE
	FROM FIL_ErrorMessage
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