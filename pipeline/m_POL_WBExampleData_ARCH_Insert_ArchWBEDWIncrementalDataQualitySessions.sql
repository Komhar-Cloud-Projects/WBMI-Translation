WITH
SQ_WBEDWIncrementalDataQualitySessions AS (
	SELECT
		WBEDWIncrementalDataQualitySessionsId,
		ExtractDate,
		SourceSystemid,
		PolicyNumber,
		PolicyVersion,
		HistoryID,
		Purpose,
		SessionID,
		SourceAccountingDate,
		SourceModifiedDate,
		Indicator,
		Autoshred
	FROM WBEDWIncrementalDataQualitySessions
),
EXP_Metadata AS (
	SELECT
	WBEDWIncrementalDataQualitySessionsId,
	ExtractDate,
	SourceSystemid,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	PolicyNumber,
	PolicyVersion,
	HistoryID,
	Purpose,
	SessionID,
	SourceAccountingDate,
	SourceModifiedDate,
	Indicator,
	Autoshred
	FROM SQ_WBEDWIncrementalDataQualitySessions
),
ArchWBEDWIncrementalDataQualitySessions AS (
	INSERT INTO ArchWBEDWIncrementalDataQualitySessions
	(ExtractDate, SourceSystemId, AuditId, WBEDWIncrementalDataQualitySessionsId, PolicyNumber, PolicyVersion, HistoryID, Purpose, SessionID, SourceAccountingDate, SourceModifiedDate, Indicator, Autoshred)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	AuditID AS AUDITID, 
	WBEDWINCREMENTALDATAQUALITYSESSIONSID, 
	POLICYNUMBER, 
	POLICYVERSION, 
	HISTORYID, 
	PURPOSE, 
	SESSIONID, 
	SOURCEACCOUNTINGDATE, 
	SOURCEMODIFIEDDATE, 
	INDICATOR, 
	AUTOSHRED
	FROM EXP_Metadata
),