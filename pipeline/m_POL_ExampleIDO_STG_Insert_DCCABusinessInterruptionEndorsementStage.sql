WITH
SQ_DC_CA_BusinessInterruptionEndorsement AS (
	WITH cte_DCCABusIntEndo(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId,
	X.CA_BusinessInterruptionEndorsementId, 
	X.SessionId, 
	X.Id, 
	X.Deleted,
	X.CollisionCoverage,
	X.DescriptionOfBusinessActivities,
	X.DurationOfWaitingPeriod,
	X.ExtendedAdditionalCoverage,
	X.ExtraExpenseCoverage,
	X.FormSelection,
	X.OTCCausesOfLoss
	FROM
	DC_CA_BusinessInterruptionEndorsement X
	inner join
	cte_DCCABusIntEndo Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LineId,
	CA_BusinessInterruptionEndorsementId,
	SessionId,
	Id,
	Deleted,
	CollisionCoverage,
	DescriptionOfBusinessActivities,
	DurationOfWaitingPeriod,
	ExtendedAdditionalCoverage,
	ExtraExpenseCoverage,
	FormSelection,
	OTCCausesOfLoss
	FROM SQ_DC_CA_BusinessInterruptionEndorsement
),
DCCABusinessInterruptionEndorsementStage AS (
	TRUNCATE TABLE DCCABusinessInterruptionEndorsementStage;
	INSERT INTO DCCABusinessInterruptionEndorsementStage
	(ExtractDate, SourceSystemid, LineId, CA_BusinessInterruptionEndorsementId, SessionId, Id, Deleted, CollisionCoverage, DescriptionOfBusinessActivities, DurationOfWaitingPeriod, ExtendedAdditionalCoverage, ExtraExpenseCoverage, FormSelection, OTCCausesOfLoss)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CA_BUSINESSINTERRUPTIONENDORSEMENTID, 
	SESSIONID, 
	ID, 
	DELETED, 
	COLLISIONCOVERAGE, 
	DESCRIPTIONOFBUSINESSACTIVITIES, 
	DURATIONOFWAITINGPERIOD, 
	EXTENDEDADDITIONALCOVERAGE, 
	EXTRAEXPENSECOVERAGE, 
	FORMSELECTION, 
	OTCCAUSESOFLOSS
	FROM EXP_MetaData
),