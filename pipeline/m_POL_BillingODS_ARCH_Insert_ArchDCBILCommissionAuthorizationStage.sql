WITH
SQ_DCBILCommissionAuthorizationStage AS (
	SELECT
		DCBILCommissionAuthorizationStageId,
		ExtractDate,
		SourceSystemId,
		CommissionAuthorizationId,
		AgentReference,
		AccountId,
		PolicyTermId,
		BillItemId,
		AuthorizedAmount,
		CommissionSchemeReference,
		AuthorizationDate,
		CommissionType,
		CommissionPercent,
		CurrencyCulture,
		LastUpdatedTimestamp,
		LastUpdatedUserId,
		CommissionAuthorizationLockingTS,
		AgencyRollupReference,
		AuthorizationDateTime,
		AuthorizationReason,
		AuthorizationTypeCode,
		TierAmount,
		Activity,
		TransactionTypeCode,
		PlanId,
		TransactionGUID
	FROM DCBILCommissionAuthorizationStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	DCBILCommissionAuthorizationStageId,
	ExtractDate,
	SourceSystemId,
	CommissionAuthorizationId,
	AgentReference,
	AccountId,
	PolicyTermId,
	BillItemId,
	AuthorizedAmount,
	CommissionSchemeReference,
	AuthorizationDate,
	CommissionType,
	CommissionPercent,
	CurrencyCulture,
	LastUpdatedTimestamp,
	LastUpdatedUserId,
	CommissionAuthorizationLockingTS,
	AgencyRollupReference,
	AuthorizationDateTime,
	AuthorizationReason,
	AuthorizationTypeCode,
	TierAmount,
	Activity,
	TransactionTypeCode,
	PlanId,
	TransactionGUID
	FROM SQ_DCBILCommissionAuthorizationStage
),
LKP_ArchExist AS (
	SELECT
	ArchDCBILCommissionAuthorizationStageId,
	CommissionAuthorizationId
	FROM (
		SELECT 
			ArchDCBILCommissionAuthorizationStageId,
			CommissionAuthorizationId
		FROM ArchDCBILCommissionAuthorizationStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CommissionAuthorizationId ORDER BY ArchDCBILCommissionAuthorizationStageId) = 1
),
FIL_Exist AS (
	SELECT
	LKP_ArchExist.ArchDCBILCommissionAuthorizationStageId AS lkp_ArchDCBILCommissionAuthorizationStageId, 
	EXP_Metadata.o_AuditId AS AuditId, 
	EXP_Metadata.DCBILCommissionAuthorizationStageId, 
	EXP_Metadata.ExtractDate, 
	EXP_Metadata.SourceSystemId, 
	EXP_Metadata.CommissionAuthorizationId, 
	EXP_Metadata.AgentReference, 
	EXP_Metadata.AccountId, 
	EXP_Metadata.PolicyTermId, 
	EXP_Metadata.BillItemId, 
	EXP_Metadata.AuthorizedAmount, 
	EXP_Metadata.CommissionSchemeReference, 
	EXP_Metadata.AuthorizationDate, 
	EXP_Metadata.CommissionType, 
	EXP_Metadata.CommissionPercent, 
	EXP_Metadata.CurrencyCulture, 
	EXP_Metadata.LastUpdatedTimestamp, 
	EXP_Metadata.LastUpdatedUserId, 
	EXP_Metadata.CommissionAuthorizationLockingTS, 
	EXP_Metadata.AgencyRollupReference, 
	EXP_Metadata.AuthorizationDateTime, 
	EXP_Metadata.AuthorizationReason, 
	EXP_Metadata.AuthorizationTypeCode, 
	EXP_Metadata.TierAmount, 
	EXP_Metadata.Activity, 
	EXP_Metadata.TransactionTypeCode, 
	EXP_Metadata.PlanId, 
	EXP_Metadata.TransactionGUID
	FROM EXP_Metadata
	LEFT JOIN LKP_ArchExist
	ON LKP_ArchExist.CommissionAuthorizationId = EXP_Metadata.CommissionAuthorizationId
	WHERE ISNULL(lkp_ArchDCBILCommissionAuthorizationStageId)
),
ArchDCBILCommissionAuthorizationStage AS (
	INSERT INTO ArchDCBILCommissionAuthorizationStage
	(ExtractDate, SourceSystemId, AuditId, DCBILCommissionAuthorizationStageId, CommissionAuthorizationId, AgentReference, AccountId, PolicyTermId, BillItemId, AuthorizedAmount, CommissionSchemeReference, AuthorizationDate, CommissionType, CommissionPercent, CurrencyCulture, LastUpdatedTimestamp, LastUpdatedUserId, CommissionAuthorizationLockingTS, AgencyRollupReference, AuthorizationDateTime, AuthorizationReason, AuthorizationTypeCode, TierAmount, Activity, TransactionTypeCode, PlanId, TransactionGUID)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	DCBILCOMMISSIONAUTHORIZATIONSTAGEID, 
	COMMISSIONAUTHORIZATIONID, 
	AGENTREFERENCE, 
	ACCOUNTID, 
	POLICYTERMID, 
	BILLITEMID, 
	AUTHORIZEDAMOUNT, 
	COMMISSIONSCHEMEREFERENCE, 
	AUTHORIZATIONDATE, 
	COMMISSIONTYPE, 
	COMMISSIONPERCENT, 
	CURRENCYCULTURE, 
	LASTUPDATEDTIMESTAMP, 
	LASTUPDATEDUSERID, 
	COMMISSIONAUTHORIZATIONLOCKINGTS, 
	AGENCYROLLUPREFERENCE, 
	AUTHORIZATIONDATETIME, 
	AUTHORIZATIONREASON, 
	AUTHORIZATIONTYPECODE, 
	TIERAMOUNT, 
	ACTIVITY, 
	TRANSACTIONTYPECODE, 
	PLANID, 
	TRANSACTIONGUID
	FROM FIL_Exist
),