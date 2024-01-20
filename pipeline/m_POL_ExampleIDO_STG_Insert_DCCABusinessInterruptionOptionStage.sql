WITH
SQ_DC_CA_BusinessInterruptionOption AS (
	WITH cte_DCCABusIntOpt(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_BusinessInterruptionEndorsementId, 
	X.CA_BusinessInterruptionOptionId, 
	X.SessionId, 
	X.Id, 
	X.Deleted,
	X.OptionType,
	X.OptionDescription,
	X.TotalExposureOptionB
	FROM
	DC_CA_BusinessInterruptionOption X
	inner join
	cte_DCCABusIntOpt Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_BusinessInterruptionEndorsementId,
	CA_BusinessInterruptionOptionId,
	SessionId,
	Id,
	Deleted,
	OptionType,
	OptionDescription,
	TotalExposureOptionB
	FROM SQ_DC_CA_BusinessInterruptionOption
),
DCCABusinessInterruptionOptionStage AS (
	TRUNCATE TABLE DCCABusinessInterruptionOptionStage;
	INSERT INTO DCCABusinessInterruptionOptionStage
	(ExtractDate, SourceSystemid, CA_BusinessInterruptionEndorsementId, CA_BusinessInterruptionOptionId, SessionId, Id, Deleted, OptionType, OptionDescription, TotalExposureOptionB)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_BUSINESSINTERRUPTIONENDORSEMENTID, 
	CA_BUSINESSINTERRUPTIONOPTIONID, 
	SESSIONID, 
	ID, 
	DELETED, 
	OPTIONTYPE, 
	OPTIONDESCRIPTION, 
	TOTALEXPOSUREOPTIONB
	FROM EXP_MetaData
),