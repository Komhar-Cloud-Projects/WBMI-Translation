WITH
SQ_WB_EDWBalancingRuleResults1 AS (
	SELECT
		PolicyNumber,
		PolicyVersion,
		HistoryID,
		Description,
		SourceResults,
		TargetResults,
		OutOfBalanceAmount,
		BlackListDate,
		ComparisonType,
		RuleExecutionDate,
		Purpose,
		SessionId
	FROM WB_EDWBalancingRuleResults
),
EXP_Default_Values AS (
	SELECT
	@{pipeline().parameters.AUDITID} AS o_AuditId,
	@{pipeline().parameters.SOURCESYSTEMID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	PolicyNumber AS i_PolicyNumber,
	PolicyVersion AS i_PolicyVersion,
	HistoryID AS i_HistoryID,
	Description AS i_Description,
	SourceResults AS i_SourceResults,
	TargetResults AS i_TargetResults,
	OutOfBalanceAmount AS i_OutOfBalanceAmount,
	BlackListDate,
	ComparisonType AS i_ComparisonType,
	RuleExecutionDate,
	Purpose AS i_Purpose,
	SessionId AS i_SessionId,
	-- *INF*: IIF(NOT ISNULL(i_PolicyNumber), i_PolicyNumber, '0')
	IFF(i_PolicyNumber IS NOT NULL,
		i_PolicyNumber,
		'0'
	) AS o_PolicyNumber,
	-- *INF*: IIF(NOT ISNULL(i_PolicyVersion), i_PolicyVersion, '0')
	IFF(i_PolicyVersion IS NOT NULL,
		i_PolicyVersion,
		'0'
	) AS o_PolicyVersion,
	-- *INF*: IIF(NOT ISNULL(i_HistoryID), i_HistoryID, 0)
	IFF(i_HistoryID IS NOT NULL,
		i_HistoryID,
		0
	) AS o_HistoryID,
	-- *INF*: IIF(NOT ISNULL(i_Description), i_Description, '0')
	IFF(i_Description IS NOT NULL,
		i_Description,
		'0'
	) AS o_Description,
	-- *INF*: IIF(NOT ISNULL(i_SourceResults), i_SourceResults, 0)
	IFF(i_SourceResults IS NOT NULL,
		i_SourceResults,
		0
	) AS o_SourceResults,
	-- *INF*: IIF(NOT ISNULL(i_TargetResults), i_TargetResults, 0)
	IFF(i_TargetResults IS NOT NULL,
		i_TargetResults,
		0
	) AS o_TargetResults,
	-- *INF*: IIF(NOT ISNULL(i_OutOfBalanceAmount), i_OutOfBalanceAmount, 0)
	IFF(i_OutOfBalanceAmount IS NOT NULL,
		i_OutOfBalanceAmount,
		0
	) AS o_OutOfBalanceAmount,
	-- *INF*: IIF(NOT ISNULL(i_ComparisonType), i_ComparisonType, '0')
	IFF(i_ComparisonType IS NOT NULL,
		i_ComparisonType,
		'0'
	) AS o_ComparisonType,
	-- *INF*: IIF(NOT ISNULL(i_Purpose), i_Purpose, '0')
	IFF(i_Purpose IS NOT NULL,
		i_Purpose,
		'0'
	) AS o_Purpose,
	-- *INF*: IIF(NOT ISNULL(i_SessionId), i_SessionId, 0)
	IFF(i_SessionId IS NOT NULL,
		i_SessionId,
		0
	) AS o_SessionId
	FROM SQ_WB_EDWBalancingRuleResults1
),
WBEDWBalancingRuleResults AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBEDWBalancingRuleResults;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBEDWBalancingRuleResults
	(AuditId, SourceSystemId, CreatedDate, ModifiedDate, PolicyNumber, PolicyVersion, HistoryID, Description, SourceResults, TargetResults, OutOfBalanceAmount, BlackListDate, ComparisonType, RuleExecutionDate, Purpose, SessionId)
	SELECT 
	o_AuditId AS AUDITID, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyVersion AS POLICYVERSION, 
	o_HistoryID AS HISTORYID, 
	o_Description AS DESCRIPTION, 
	o_SourceResults AS SOURCERESULTS, 
	o_TargetResults AS TARGETRESULTS, 
	o_OutOfBalanceAmount AS OUTOFBALANCEAMOUNT, 
	BLACKLISTDATE, 
	o_ComparisonType AS COMPARISONTYPE, 
	RULEEXECUTIONDATE, 
	o_Purpose AS PURPOSE, 
	o_SessionId AS SESSIONID
	FROM EXP_Default_Values
),