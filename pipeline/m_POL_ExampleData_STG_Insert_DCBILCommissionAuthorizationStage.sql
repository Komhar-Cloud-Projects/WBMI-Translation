WITH
SQ_DC_BIL_CommissionAuthorization AS (
	SELECT CA.CommissionAuthorizationId, CA.AgentReference, CA.AccountId, CA.PolicyTermId, CA.BillItemId, CA.AuthorizedAmount, CA.CommissionSchemeReference, CA.AuthorizationDate, CA.CommissionType, 
	CA.CommissionPercent, CA.CurrencyCulture, CA.LastUpdatedTimestamp, CA.LastUpdatedUserId, null as CommissionAuthorizationLockingTS, CA.AgencyRollupReference, CA.AuthorizationDateTime, CA.AuthorizationReason, 
	CA.AuthorizationTypeCode, CA.TierAmount, CA.Activity, CA.TransactionTypeCode, CA.PlanId, null as TransactionGUID 
	FROM
	 DC_BIL_CommissionAuthorization CA with(nolock)
	WHERE
	CA.AuthorizationDate >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	CA.CommissionAuthorizationId % @{pipeline().parameters.NUM_OF_PARTITIONS}=1
	
	UNION ALL
	SELECT CA.CommissionAuthorizationId, CA.AgentReference, CA.AccountId, CA.PolicyTermId, CA.BillItemId, CA.AuthorizedAmount, CA.CommissionSchemeReference, CA.AuthorizationDate, CA.CommissionType, 
	CA.CommissionPercent, CA.CurrencyCulture, CA.LastUpdatedTimestamp, CA.LastUpdatedUserId, null as CommissionAuthorizationLockingTS, CA.AgencyRollupReference, CA.AuthorizationDateTime, CA.AuthorizationReason, 
	CA.AuthorizationTypeCode, CA.TierAmount, CA.Activity, CA.TransactionTypeCode, CA.PlanId, null as TransactionGUID 
	FROM
	 DC_BIL_CommissionAuthorization CA with(nolock)
	WHERE
	CA.AuthorizationDate >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	CA.CommissionAuthorizationId % @{pipeline().parameters.NUM_OF_PARTITIONS}=2
	
	UNION ALL
	SELECT CA.CommissionAuthorizationId, CA.AgentReference, CA.AccountId, CA.PolicyTermId, CA.BillItemId, CA.AuthorizedAmount, CA.CommissionSchemeReference, CA.AuthorizationDate, CA.CommissionType, 
	CA.CommissionPercent, CA.CurrencyCulture, CA.LastUpdatedTimestamp, CA.LastUpdatedUserId, null as CommissionAuthorizationLockingTS, CA.AgencyRollupReference, CA.AuthorizationDateTime, CA.AuthorizationReason, 
	CA.AuthorizationTypeCode, CA.TierAmount, CA.Activity, CA.TransactionTypeCode, CA.PlanId, null as TransactionGUID 
	FROM
	 DC_BIL_CommissionAuthorization CA with(nolock)
	WHERE
	CA.AuthorizationDate >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	CA.CommissionAuthorizationId % @{pipeline().parameters.NUM_OF_PARTITIONS}=3
	
	UNION ALL
	SELECT CA.CommissionAuthorizationId, CA.AgentReference, CA.AccountId, CA.PolicyTermId, CA.BillItemId, CA.AuthorizedAmount, CA.CommissionSchemeReference, CA.AuthorizationDate, CA.CommissionType, 
	CA.CommissionPercent, CA.CurrencyCulture, CA.LastUpdatedTimestamp, CA.LastUpdatedUserId, null as CommissionAuthorizationLockingTS, CA.AgencyRollupReference, CA.AuthorizationDateTime, CA.AuthorizationReason, 
	CA.AuthorizationTypeCode, CA.TierAmount, CA.Activity, CA.TransactionTypeCode, CA.PlanId, null as TransactionGUID 
	FROM
	 DC_BIL_CommissionAuthorization CA with(nolock)
	WHERE
	CA.AuthorizationDate >= DATEADD(DD,@{pipeline().parameters.NO_OF_DAYS},'@{pipeline().parameters.SELECTION_START_TS}')
	AND
	CA.CommissionAuthorizationId % @{pipeline().parameters.NUM_OF_PARTITIONS}=4
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_DC_BIL_CommissionAuthorization
),
DCBILCommissionAuthorizationStage AS (
	TRUNCATE TABLE DCBILCommissionAuthorizationStage;
	INSERT INTO DCBILCommissionAuthorizationStage
	(ExtractDate, SourceSystemId, CommissionAuthorizationId, AgentReference, AccountId, PolicyTermId, BillItemId, AuthorizedAmount, CommissionSchemeReference, AuthorizationDate, CommissionType, CommissionPercent, CurrencyCulture, LastUpdatedTimestamp, LastUpdatedUserId, CommissionAuthorizationLockingTS, AgencyRollupReference, AuthorizationDateTime, AuthorizationReason, AuthorizationTypeCode, TierAmount, Activity, TransactionTypeCode, PlanId, TransactionGUID)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
	FROM EXP_Metadata
),