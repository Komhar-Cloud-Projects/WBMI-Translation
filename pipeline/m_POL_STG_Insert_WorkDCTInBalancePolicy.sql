WITH
SQ_WBEDWIncrementalDataQualitySessions AS (
	SELECT A.ExtractDate, A.SourceSystemid, A.PolicyNumber, A.HistoryID, A.Purpose, A.SessionID, A.SourceAccountingDate  
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WBEDWIncrementalDataQualitySessions A 
	where A.Indicator=1
	and A.Autoshred<> '1'
),
EXP_MetaData AS (
	SELECT
	ExtractDate,
	SourceSystemid,
	PolicyNumber,
	HistoryID,
	Purpose,
	SessionID,
	SourceAccountingDate
	FROM SQ_WBEDWIncrementalDataQualitySessions
),
LKP_WorkDCTInBalancePolicy AS (
	SELECT
	HistoryID,
	Purpose
	FROM (
		SELECT 
			HistoryID,
			Purpose
		FROM WorkDCTInBalancePolicy
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,Purpose ORDER BY HistoryID) = 1
),
EXP_AccountingDate AS (
	SELECT
	LKP_WorkDCTInBalancePolicy.HistoryID AS LKP_HistoryID,
	LKP_WorkDCTInBalancePolicy.Purpose AS LKP_Purpose,
	EXP_MetaData.ExtractDate,
	EXP_MetaData.SourceSystemid,
	CURRENT_TIMESTAMP AS o_CreatedDate,
	'InformS' AS o_CreatedUserID,
	EXP_MetaData.PolicyNumber,
	EXP_MetaData.HistoryID,
	EXP_MetaData.SessionID,
	EXP_MetaData.Purpose,
	EXP_MetaData.SourceAccountingDate,
	-- *INF*: DECODE(TRUE,
	-- TO_CHAR(SESSSTARTTIME,'DD')='01' AND UPPER(TO_CHAR(ADD_TO_DATE(SESSSTARTTIME,'D',-1),'DAY'))='SUNDAY',ADD_TO_DATE(SESSSTARTTIME,'D',-1),
	-- SESSSTARTTIME
	-- )
	DECODE(
	    TRUE,
	    TO_CHAR(SESSSTARTTIME, 'DD') = '01' AND UPPER(TO_CHAR(DATEADD(DAY,- 1,SESSSTARTTIME), 'DAY')) = 'SUNDAY', DATEADD(DAY,- 1,SESSSTARTTIME),
	    SESSSTARTTIME
	) AS v_SessStartTime,
	-- *INF*: LAST_DAY(SourceAccountingDate)
	LAST_DAY(SourceAccountingDate) AS v_SourceAccountingDate,
	-- *INF*: SET_DATE_PART(
	-- SET_DATE_PART(
	-- SET_DATE_PART(
	-- TRUNC(v_SourceAccountingDate,'DAY'), 'HH24', 23),
	-- 'MI',59),
	-- 'SS',59)
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0))),CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0)))),DATEADD(HOUR,23-DATE_PART(HOUR,CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0))),CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0))))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0))),CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0)))),DATEADD(HOUR,23-DATE_PART(HOUR,CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0))),CAST(TRUNC(v_SourceAccountingDate, 'DAY') AS TIMESTAMP_NTZ(0))))) AS o_SourceAccountingDate,
	-- *INF*: DECODE( TRUE,
	-- NOT ISNULL(LKP_HistoryID),1,
	-- LTRIM(RTRIM(Purpose))='Onset',0,
	-- 1)
	DECODE(
	    TRUE,
	    LKP_HistoryID IS NOT NULL, 1,
	    LTRIM(RTRIM(Purpose)) = 'Onset', 0,
	    1
	) AS v_ProcessedFlag,
	v_ProcessedFlag AS ProcessedFlag
	FROM EXP_MetaData
	LEFT JOIN LKP_WorkDCTInBalancePolicy
	ON LKP_WorkDCTInBalancePolicy.HistoryID = EXP_MetaData.HistoryID AND LKP_WorkDCTInBalancePolicy.Purpose = EXP_MetaData.Purpose
),
WorkDCTInBalancePolicy AS (
	INSERT INTO WorkDCTInBalancePolicy
	(ExtractDate, SourceSystemid, CreatedDate, CreatedUserID, PolicyNumber, HistoryID, SessionID, Purpose, AccountingDate, ProcessedFlag)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_CreatedUserID AS CREATEDUSERID, 
	POLICYNUMBER, 
	HISTORYID, 
	SESSIONID, 
	PURPOSE, 
	o_SourceAccountingDate AS ACCOUNTINGDATE, 
	PROCESSEDFLAG
	FROM EXP_AccountingDate
),