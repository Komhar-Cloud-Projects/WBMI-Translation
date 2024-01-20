WITH
SQ_DC_Line AS (
	WITH cte_DCLine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PolicyId, 
	X.LineId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.HonorRates, 
	X.HonoredRateEffectiveDate, 
	X.AssignmentDate, 
	X.AuditPeriod 
	FROM
	DC_Line X
	inner join
	cte_DCLine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	PolicyId,
	LineId,
	SessionId,
	Id,
	Type,
	HonorRates,
	HonoredRateEffectiveDate,
	AssignmentDate,
	AuditPeriod,
	-- *INF*: DECODE(HonorRates,'T',1,'F',0,NULL)
	DECODE(
	    HonorRates,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HonorRates,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_Line
),
DCLineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLineStaging
	(PolicyId, LineId, SessionId, Id, Type, HonorRates, HonoredRateEffectiveDate, AssignmentDate, AuditPeriod, ExtractDate, SourceSystemId)
	SELECT 
	POLICYID, 
	LINEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	o_HonorRates AS HONORRATES, 
	HONOREDRATEEFFECTIVEDATE, 
	ASSIGNMENTDATE, 
	AUDITPERIOD, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),