WITH
SQ_WB_POL_BatchRenewalPolicy AS (
	SELECT WB_POL_BatchRenewalPolicy.HistoryId, WB_POL_BatchRenewalPolicy.ModifiedUserId, WB_POL_BatchRenewalPolicy.ModifiedDate, WB_POL_BatchRenewalPolicy.QuoteId, WB_POL_BatchRenewalPolicy.HistoryIdRenewalPolicyVersion, WB_POL_BatchRenewalPolicy.PolicyQualifiedAutomaticRenewalIndicator, WB_POL_BatchRenewalPolicy.CustomerQualifiedAutomaticRenewalIndicator, WB_POL_BatchRenewalPolicy.CustomerBatchProcessedIndicator, WB_POL_BatchRenewalPolicy.AutoRenewedIndicator, WB_POL_BatchRenewalPolicy.TransactionDate, WB_POL_BatchRenewalPolicy.BusinessDivision,
	WB_POL_BatchRenewalPolicy.CustomerCarePolicyQualifiedAutomaticRenewalIndicator 
	FROM WBExampleData.dbo.WB_POL_BatchRenewalPolicy 
	WHERE ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	HistoryId,
	ModifiedUserId,
	ModifiedDate,
	QuoteId,
	HistoryIdRenewalPolicyVersion,
	PolicyQualifiedAutomaticRenewalIndicator AS i_PolicyQualifiedAutomaticRenewalIndicator,
	-- *INF*: IIF(i_PolicyQualifiedAutomaticRenewalIndicator='T','1','0')
	IFF(i_PolicyQualifiedAutomaticRenewalIndicator = 'T', '1', '0') AS o_PolicyQualifiedAutomaticRenewalIndicator,
	CustomerQualifiedAutomaticRenewalIndicator AS i_CustomerQualifiedAutomaticRenewalIndicator,
	-- *INF*: IIF(i_CustomerQualifiedAutomaticRenewalIndicator='T','1','0')
	IFF(i_CustomerQualifiedAutomaticRenewalIndicator = 'T', '1', '0') AS o_CustomerQualifiedAutomaticRenewalIndicator,
	CustomerBatchProcessedIndicator AS i_CustomerBatchProcessedIndicator,
	-- *INF*: IIF(i_CustomerBatchProcessedIndicator='T','1','0')
	IFF(i_CustomerBatchProcessedIndicator = 'T', '1', '0') AS o_CustomerBatchProcessedIndicator,
	AutoRenewedIndicator AS i_AutoRenewedIndicator,
	-- *INF*: IIF(i_AutoRenewedIndicator='T','1','0')
	IFF(i_AutoRenewedIndicator = 'T', '1', '0') AS o_AutoRenewedIndicator,
	TransactionDate,
	BusinessDivision,
	CustomerCarePolicyQualifiedAutomaticRenewalIndicator,
	-- *INF*: iif(CustomerCarePolicyQualifiedAutomaticRenewalIndicator='T','1','0')
	IFF(CustomerCarePolicyQualifiedAutomaticRenewalIndicator = 'T', '1', '0') AS o_CustomerCarePolicyQualifiedAutomaticRenewalIndicator
	FROM SQ_WB_POL_BatchRenewalPolicy
),
WBPOLBatchRenewalPolicyStage AS (
	TRUNCATE TABLE WBPOLBatchRenewalPolicyStage;
	INSERT INTO WBPOLBatchRenewalPolicyStage
	(ExtractDate, SourceSystemid, HistoryId, ModifiedUserId, ModifiedDate, QuoteId, HistoryIdRenewalPolicyVersion, PolicyQualifiedAutomaticRenewalIndicator, CustomerQualifiedAutomaticRenewalIndicator, CustomerBatchProcessedIndicator, AutoRenewedIndicator, TransactionDate, BusinessDivision, CustomerCarePolicyQualifiedAutomaticRenewalIndicator)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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