WITH
SQ_WBPOLBatchRenewalPolicyStage AS (
	SELECT
		WBPOLBatchRenewalPolicyStageId,
		ExtractDate,
		SourceSystemid,
		HistoryId,
		ModifiedUserId,
		ModifiedDate,
		QuoteId,
		HistoryIdRenewalPolicyVersion,
		PolicyQualifiedAutomaticRenewalIndicator,
		CustomerQualifiedAutomaticRenewalIndicator,
		CustomerBatchProcessedIndicator,
		AutoRenewedIndicator,
		TransactionDate,
		BusinessDivision,
		CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM WBPOLBatchRenewalPolicyStage
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemid AS SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	HistoryId,
	ModifiedUserId,
	ModifiedDate,
	QuoteId,
	HistoryIdRenewalPolicyVersion,
	PolicyQualifiedAutomaticRenewalIndicator AS i_PolicyQualifiedAutomaticRenewalIndicator,
	-- *INF*: DECODE(i_PolicyQualifiedAutomaticRenewalIndicator,
	-- 'T','1',
	-- 'F','0',
	-- NULL)
	DECODE(
	    i_PolicyQualifiedAutomaticRenewalIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_PolicyQualifiedAutomaticRenewalIndicator,
	CustomerQualifiedAutomaticRenewalIndicator AS i_CustomerQualifiedAutomaticRenewalIndicator,
	-- *INF*: DECODE(i_CustomerQualifiedAutomaticRenewalIndicator,
	-- 'T','1',
	-- 'F','0',
	-- NULL)
	DECODE(
	    i_CustomerQualifiedAutomaticRenewalIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_CustomerQualifiedAutomaticRenewalIndicator,
	CustomerBatchProcessedIndicator AS i_CustomerBatchProcessedIndicator,
	-- *INF*: DECODE(i_CustomerBatchProcessedIndicator,
	-- 'T','1',
	-- 'F','0',
	-- NULL)
	DECODE(
	    i_CustomerBatchProcessedIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_CustomerBatchProcessedIndicator,
	AutoRenewedIndicator AS i_AutoRenewedIndicator,
	-- *INF*: DECODE(i_AutoRenewedIndicator,
	-- 'T','1',
	-- 'F','0',
	-- NULL)
	DECODE(
	    i_AutoRenewedIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AutoRenewedIndicator,
	TransactionDate,
	BusinessDivision,
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator,
	-- *INF*: DECODE(CustomerCarePolicyQualifiedAutomaticRenewalIndicator, 'T','1', 'F','0', NULL)
	DECODE(
	    CustomerCarePolicyQualifiedAutomaticRenewalIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM SQ_WBPOLBatchRenewalPolicyStage
),
ArchWBPOLBatchRenewalPolicyStage AS (
	INSERT INTO ArchWBPOLBatchRenewalPolicyStage
	(ExtractDate, SourceSystemid, AuditId, HistoryId, ModifiedUserId, ModifiedDate, QuoteId, HistoryIdRenewalPolicyVersion, PolicyQualifiedAutomaticRenewalIndicator, CustomerQualifiedAutomaticRenewalIndicator, CustomerBatchProcessedIndicator, AutoRenewedIndicator, TransactionDate, BusinessDivision, CustomerCarePolicyQualifiedAutomaticRenewalIndicator)
	SELECT 
	EXTRACTDATE, 
	SourceSystemId AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	HISTORYID, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	QUOTEID, 
	HISTORYIDRENEWALPOLICYVERSION, 
	o_PolicyQualifiedAutomaticRenewalIndicator AS POLICYQUALIFIEDAUTOMATICRENEWALINDICATOR, 
	o_CustomerQualifiedAutomaticRenewalIndicator AS CUSTOMERQUALIFIEDAUTOMATICRENEWALINDICATOR, 
	o_CustomerBatchProcessedIndicator AS CUSTOMERBATCHPROCESSEDINDICATOR, 
	o_AutoRenewedIndicator AS AUTORENEWEDINDICATOR, 
	TRANSACTIONDATE, 
	BUSINESSDIVISION, 
	o_CustomerCarePolicyQualifiedAutomaticRenewalIndicator AS CUSTOMERCAREPOLICYQUALIFIEDAUTOMATICRENEWALINDICATOR
	FROM EXP_Metadata
),