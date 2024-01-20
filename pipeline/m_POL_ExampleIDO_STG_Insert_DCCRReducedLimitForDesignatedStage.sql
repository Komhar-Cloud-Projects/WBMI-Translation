WITH
SQ_DC_CR_ReducedLimitForDesignated AS (
	WITH cte_DCCRReducedLimitForDesignated(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CR_EndorsementId, 
	X.CR_BuildingId, 
	X.CR_ReducedLimitForDesignatedId, 
	X.SessionId, 
	X.Id, 
	X.CR_BuildingXmlId, 
	X.EndorsementReducedLimitForDesignatedNumberOfPremises, 
	X.Deleted 
	FROM
	DC_CR_ReducedLimitForDesignated X
	inner join
	cte_DCCRReducedLimitForDesignated Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CR_EndorsementId,
	CR_BuildingId,
	CR_ReducedLimitForDesignatedId,
	SessionId,
	Id,
	CR_BuildingXmlId,
	EndorsementReducedLimitForDesignatedNumberOfPremises,
	Deleted,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CR_ReducedLimitForDesignated
),
DCCRReducedLimitForDesignatedStage4 AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCRReducedLimitForDesignatedStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCRReducedLimitForDesignatedStage
	(CREndorsementId, CRBuildingId, CRReducedLimitForDesignatedId, SessionId, Id, Deleted, CRBuildingXmlId, EndorsementReducedLimitForDesignatedNumberOfPremises, ExtractDate, SourceSystemId)
	SELECT 
	CR_EndorsementId AS CRENDORSEMENTID, 
	CR_BuildingId AS CRBUILDINGID, 
	CR_ReducedLimitForDesignatedId AS CRREDUCEDLIMITFORDESIGNATEDID, 
	SESSIONID, 
	ID, 
	DELETED, 
	CR_BuildingXmlId AS CRBUILDINGXMLID, 
	ENDORSEMENTREDUCEDLIMITFORDESIGNATEDNUMBEROFPREMISES, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),