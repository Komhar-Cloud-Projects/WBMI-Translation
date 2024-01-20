WITH
SQ_WB_EDWIncrementalDataQualitySessions AS (
	SELECT A.PolicyNumber, A.PolicyVersion, A.HistoryID, A.Purpose, A.SessionId, A.SourceAccountingDate, A.ModifiedDate, A.Indicator, A.Autoshred 
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions A
	where A.ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' AND '@{pipeline().parameters.SELECTION_END_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_METADATA AS (
	SELECT
	SYSDATE AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	PolicyNumber,
	PolicyVersion,
	HistoryId AS HistoryID,
	Purpose,
	SessionId,
	SourceAccountingDate,
	ModifiedDate,
	Indicator,
	Autoshred
	FROM SQ_WB_EDWIncrementalDataQualitySessions
),
WBEDWIncrementalDataQualitySessions AS (
	TRUNCATE TABLE WBEDWIncrementalDataQualitySessions;
	INSERT INTO WBEDWIncrementalDataQualitySessions
	(ExtractDate, SourceSystemid, PolicyNumber, PolicyVersion, HistoryID, Purpose, SessionID, SourceAccountingDate, SourceModifiedDate, Indicator, Autoshred)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	POLICYNUMBER, 
	POLICYVERSION, 
	HISTORYID, 
	PURPOSE, 
	SessionId AS SESSIONID, 
	SOURCEACCOUNTINGDATE, 
	ModifiedDate AS SOURCEMODIFIEDDATE, 
	INDICATOR, 
	AUTOSHRED
	FROM EXP_METADATA
),