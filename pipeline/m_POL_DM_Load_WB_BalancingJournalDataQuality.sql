WITH
SQ_WB_BalancingJournalDataQuality1 AS (
	SELECT
		HistoryId,
		Purpose,
		DataQualityFailedDate,
		DataQualityRestoredDate,
		ModifiedDate
	FROM WB_BalancingJournalDataQuality1
),
EXP_Default_Value1 AS (
	SELECT
	@{pipeline().parameters.AUDITID} AS o_AduitId,
	@{pipeline().parameters.SOURCESYSTEMID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_SourceModififedDate,
	HistoryId AS i_HistoryId,
	Purpose AS i_Purpose,
	DataQualityFailedDate,
	DataQualityRestoredDate,
	ModifiedDate,
	-- *INF*: IIF(NOT ISNULL(i_HistoryId), i_HistoryId, 0)
	IFF(i_HistoryId IS NOT NULL, i_HistoryId, 0) AS o_HistoryId,
	-- *INF*: IIF(NOT ISNULL(i_Purpose), i_Purpose, '0')
	IFF(i_Purpose IS NOT NULL, i_Purpose, '0') AS o_Purpose
	FROM SQ_WB_BalancingJournalDataQuality1
),
WBBalancingJournalDataQuality AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBalancingJournalDataQuality;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBalancingJournalDataQuality
	(AuditId, SourceSystemId, CreatedDate, ModifiedDate, HistoryId, Purpose, DataQualityFailedDate, DataQualityRestoredDate, SourceModifiedDate)
	SELECT 
	o_AduitId AS AUDITID, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	MODIFIEDDATE, 
	o_HistoryId AS HISTORYID, 
	o_Purpose AS PURPOSE, 
	DATAQUALITYFAILEDDATE, 
	DATAQUALITYRESTOREDDATE, 
	o_SourceModififedDate AS SOURCEMODIFIEDDATE
	FROM EXP_Default_Value1
),