WITH
SQ_DC_IndividualsIncludedExcluded AS (
	WITH cte_DCIndividualsIncludedExcluded(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.IndividualsIncludedExcludedId, 
	X.SessionId, 
	X.Id, 
	X.IncludedExcluded, 
	X.OwnershipPercentage, 
	X.Duties, 
	X.RemunerationPayroll, 
	X.TitleRelationship 
	FROM
	DC_IndividualsIncludedExcluded X
	inner join
	cte_DCIndividualsIncludedExcluded Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	IndividualsIncludedExcludedId,
	SessionId,
	Id,
	IncludedExcluded,
	OwnershipPercentage,
	-- *INF*: IIF(IS_NUMBER(OwnershipPercentage)=1,TO_FLOAT(OwnershipPercentage) ,NULL)
	IFF(REGEXP_LIKE(OwnershipPercentage, '^[0-9]+$') = 1, TO_FLOAT(OwnershipPercentage), NULL) AS o_OwnershipPercentage,
	Duties,
	RemunerationPayroll,
	TitleRelationship,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_IndividualsIncludedExcluded
),
DCIndividualsIncludedExcludedStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIndividualsIncludedExcludedStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIndividualsIncludedExcludedStage
	(LineId, IndividualsIncludedExcludedId, SessionId, Id, IncludedExcluded, OwnershipPercentage, Duties, RemunerationPayroll, TitleRelationship, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	INDIVIDUALSINCLUDEDEXCLUDEDID, 
	SESSIONID, 
	ID, 
	INCLUDEDEXCLUDED, 
	o_OwnershipPercentage AS OWNERSHIPPERCENTAGE, 
	DUTIES, 
	REMUNERATIONPAYROLL, 
	TITLERELATIONSHIP, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),