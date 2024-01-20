WITH
LKP_ProcessStatus AS (
	SELECT
	SupProcessStatusId,
	ProcessStatus
	FROM (
		SELECT 
			SupProcessStatusId,
			ProcessStatus
		FROM SupProcessStatus
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProcessStatus ORDER BY SupProcessStatusId) = 1
),
SQ_DCTClaimClientStage AS (
	SELECT DCTClaimClientStage.DCTClaimClientStageId, DCTClaimClientStage.CreatedDate, DCTClaimClientStage.ModifiedDate, DCTClaimClientStage.PolicyNumber, DCTClaimClientStage.PolicyVersion, DCTClaimClientStage.AgreementPartyId, DCTClaimClientStage.FirstName, DCTClaimClientStage.LastName, DCTClaimClientStage.Street1, DCTClaimClientStage.Street2, DCTClaimClientStage.City, DCTClaimClientStage.StateCode, DCTClaimClientStage.ZipCode, DCTClaimClientStage.WorkPhoneNumber, DCTClaimClientStage.ErrorDescription, DCTClaimClientStage.SupProcessStatusId, DCTClaimClientStage.FaxNumber, DCTClaimClientStage.BirthDate, DCTClaimClientStage.TaxId ,
	SupProcessStatus.SupProcessStatusId, SupProcessStatus.ProcessStatus, SupProcessStatus.CreatedDate, SupProcessStatus.CreatedUserId, SupProcessStatus.ModifiedDate, SupProcessStatus.ModifiedUserId
	FROM
	DCTClaimClientStage
	JOIN SupProcessStatus ON DCTClaimClientStage.SupProcessStatusId = SupProcessStatus.SupProcessStatusId
	WHERE SupProcessStatus.ProcessStatus = 'Request'
),
EXP_DCTClaimClientStage AS (
	SELECT
	DCTClaimClientStageId,
	CreatedDate,
	ModifiedDate,
	PolicyNumber,
	PolicyVersion,
	AgreementPartyId,
	FirstName,
	LastName,
	Street1,
	Street2,
	City,
	StateCode,
	ZipCode,
	WorkPhoneNumber,
	ErrorDescription,
	SupProcessStatusId,
	FaxNumber,
	BirthDate,
	TaxId
	FROM SQ_DCTClaimClientStage
),
WEB_UpdatePolicyClient AS (-- WEB_UpdatePolicyClient

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_ClaimDataProcessor_Response AS (
	SELECT
	tns1_StageId0 AS StageId,
	tns1_StatusCode0 AS StatusCode,
	tns1_ErrorDescription0 AS ErrorDescription,
	-- *INF*: SYSTIMESTAMP()
	CURRENT_TIMESTAMP() AS out_ChangedDate,
	-- *INF*: IIF(StatusCode = 'Success', :LKP.LKP_PROCESSSTATUS('Complete'), :LKP.LKP_PROCESSSTATUS('Error'))
	IFF(
	    StatusCode = 'Success', LKP_PROCESSSTATUS__Complete.SupProcessStatusId,
	    LKP_PROCESSSTATUS__Error.SupProcessStatusId
	) AS out_ProcessStatusId,
	-- *INF*: IIF(StatusCode != 'Success', SETVARIABLE(@{pipeline().parameters.ERRORFLAG}, 1))
	IFF(StatusCode != 'Success', SETVARIABLE(@{pipeline().parameters.ERRORFLAG}, 1)) AS v_set_ErrorFlag
	FROM WEB_UpdatePolicyClient
	LEFT JOIN LKP_PROCESSSTATUS LKP_PROCESSSTATUS__Complete
	ON LKP_PROCESSSTATUS__Complete.ProcessStatus = 'Complete'

	LEFT JOIN LKP_PROCESSSTATUS LKP_PROCESSSTATUS__Error
	ON LKP_PROCESSSTATUS__Error.ProcessStatus = 'Error'

),
UPD_DCTClaimClientStage AS (
	SELECT
	StageId, 
	ErrorDescription, 
	out_ChangedDate, 
	out_ProcessStatusId AS SupProcessStatusId
	FROM EXP_ClaimDataProcessor_Response
),
DCTClaimClientStage1 AS (
	MERGE INTO DCTClaimClientStage AS T
	USING UPD_DCTClaimClientStage AS S
	ON T.DCTClaimClientStageId = S.StageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.out_ChangedDate, T.ErrorDescription = S.ErrorDescription, T.SupProcessStatusId = S.SupProcessStatusId
),