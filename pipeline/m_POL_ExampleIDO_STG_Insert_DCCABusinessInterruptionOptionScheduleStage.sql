WITH
SQ_DC_CA_BusinessInterruptionOptionSchedule AS (
	WITH cte_DCCABusIntOptSch(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_BusinessInterruptionOptionId, 
	X.CA_BusinessInterruptionOptionScheduleId, 
	X.SessionId, 
	X.Id, 
	X.DescriptionOfScheduledProperty
	FROM
	DC_CA_BusinessInterruptionOptionSchedule X
	inner join
	cte_DCCABusIntOptSch Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_BusinessInterruptionOptionId,
	CA_BusinessInterruptionOptionScheduleId,
	SessionId,
	Id,
	DescriptionOfScheduledProperty
	FROM SQ_DC_CA_BusinessInterruptionOptionSchedule
),
DCCABusinessInterruptionOptionScheduleStage AS (
	TRUNCATE TABLE DCCABusinessInterruptionOptionScheduleStage;
	INSERT INTO DCCABusinessInterruptionOptionScheduleStage
	(ExtractDate, SourceSystemid, CA_BusinessInterruptionOptionId, CA_BusinessInterruptionOptionScheduleId, SessionId, Id, DescriptionOfScheduledProperty)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_BUSINESSINTERRUPTIONOPTIONID, 
	CA_BUSINESSINTERRUPTIONOPTIONSCHEDULEID, 
	SESSIONID, 
	ID, 
	DESCRIPTIONOFSCHEDULEDPROPERTY
	FROM EXP_MetaData
),