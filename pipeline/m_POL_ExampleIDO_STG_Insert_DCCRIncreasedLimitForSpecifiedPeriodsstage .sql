WITH
SQ_DC_CR_IncreasedLimitForSpecifiedPeriods AS (
	WITH cte_DCCRIncreasedLimitForSpecifiedPeriods(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CR_EndorsementId, 
	X.CR_BuildingId, 
	X.CR_IncreasedLimitForSpecifiedPeriodsId, 
	X.SessionId, 
	X.Id, 
	X.CR_BuildingXmlId, 
	X.IncreaseLimitForSpecifiedPeriodsEffectiveDate, 
	X.IncreaseLimitForSpecifiedPeriodsExpirationDate, 
	X.IncreaseLimitForSpecifiedPeriodsNumberOfPremises, 
	X.Deleted 
	FROM
	DC_CR_IncreasedLimitForSpecifiedPeriods X
	inner join
	cte_DCCRIncreasedLimitForSpecifiedPeriods Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CR_EndorsementId,
	CR_BuildingId,
	CR_IncreasedLimitForSpecifiedPeriodsId,
	SessionId,
	Id,
	CR_BuildingXmlId,
	IncreaseLimitForSpecifiedPeriodsEffectiveDate,
	IncreaseLimitForSpecifiedPeriodsExpirationDate,
	IncreaseLimitForSpecifiedPeriodsNumberOfPremises,
	Deleted,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CR_IncreasedLimitForSpecifiedPeriods
),
DCCRIncreasedLimitForSpecifiedPeriodsStage1 AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCRIncreasedLimitForSpecifiedPeriodsStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCRIncreasedLimitForSpecifiedPeriodsStage
	(CREndorsementId, CRBuildingId, CRIncreasedLimitForSpecifiedPeriodsId, SessionId, Id, Deleted, CRBuildingXmlId, IncreaseLimitForSpecifiedPeriodsEffectiveDate, IncreaseLimitForSpecifiedPeriodsExpirationDate, IncreaseLimitForSpecifiedPeriodsNumberOfPremises, ExtractDate, SourceSystemId)
	SELECT 
	CR_EndorsementId AS CRENDORSEMENTID, 
	CR_BuildingId AS CRBUILDINGID, 
	CR_IncreasedLimitForSpecifiedPeriodsId AS CRINCREASEDLIMITFORSPECIFIEDPERIODSID, 
	SESSIONID, 
	ID, 
	DELETED, 
	CR_BuildingXmlId AS CRBUILDINGXMLID, 
	INCREASELIMITFORSPECIFIEDPERIODSEFFECTIVEDATE, 
	INCREASELIMITFORSPECIFIEDPERIODSEXPIRATIONDATE, 
	INCREASELIMITFORSPECIFIEDPERIODSNUMBEROFPREMISES, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),