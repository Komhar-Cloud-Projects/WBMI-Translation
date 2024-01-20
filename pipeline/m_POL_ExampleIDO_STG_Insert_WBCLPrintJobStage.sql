WITH
SQ_WB_CL_PrintJob AS (
	WITH cte_WBCLPrintJob(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PolicyId, 
	X.WB_CL_PrintJobId, 
	X.SessionId, 
	X.Manuscript, 
	X.PrintJob 
	FROM
	WB_CL_PrintJob X
	inner join
	cte_WBCLPrintJob Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	WB_CL_PrintJobId,
	SessionId,
	Manuscript,
	PrintJob,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID
	FROM SQ_WB_CL_PrintJob
),
WBCLPrintJobStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPrintJobStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCLPrintJobStage
	(PolicyId, WBCLPrintJobId, SessionId, Manuscript, PrintJob, ExtractDate, SourceSystemId)
	SELECT 
	POLICYID, 
	WB_CL_PrintJobId AS WBCLPRINTJOBID, 
	SESSIONID, 
	MANUSCRIPT, 
	PRINTJOB, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemID AS SOURCESYSTEMID
	FROM EXP_Metadata
),