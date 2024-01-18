WITH
SQ_DC_CA_Public AS (
	WITH cte_dccapublic(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_RiskId, 
	X.CA_PublicId, 
	X.SessionId,
	X.Id, 
	X.Auditable, 
	X.CharterRegPlates, 
	X.IndividuallyOwnedLimousine, 
	X.Jitneys, 
	X.MigrantFarmWorkersTransportation, 
	X.MileageAudit, 
	X.MileageEstimate, 
	X.MileageTotal, 
	X.MileageTotalAudit, 
	X.MileageTotalEstimate,
	X.PublicGroupType, 
	X.PublicType, 
	X.RideSharing, 
	X.RiskPublicInputNYBlackCar, 
	X.SeatingCapacity,
	X.MechanicalLift
	FROM
	DC_CA_Public X
	inner join
	cte_dccapublic Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_TRANS AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CA_RiskId,
	CA_PublicId,
	SessionId,
	Id,
	Auditable,
	CharterRegPlates,
	IndividuallyOwnedLimousine,
	Jitneys,
	MigrantFarmWorkersTransportation,
	MileageAudit,
	MileageEstimate,
	MileageTotal,
	MileageTotalAudit,
	MileageTotalEstimate,
	PublicGroupType,
	PublicType,
	RideSharing,
	RiskPublicInputNYBlackCar,
	SeatingCapacity,
	MechanicalLift
	FROM SQ_DC_CA_Public
),
DCCAPublicStage AS (
	TRUNCATE TABLE DCCAPublicStage;
	INSERT INTO DCCAPublicStage
	(ExtractDate, SourceSystemid, CA_RiskId, CA_PublicId, SessionId, Id, Auditable, CharterRegPlates, IndividuallyOwnedLimousine, Jitneys, MigrantFarmWorkersTransportation, MileageAudit, MileageEstimate, MileageTotal, MileageTotalAudit, MileageTotalEstimate, PublicGroupType, PublicType, RideSharing, RiskPublicInputNYBlackCar, SeatingCapacity, MechanicalLift)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_RISKID, 
	CA_PUBLICID, 
	SESSIONID, 
	ID, 
	AUDITABLE, 
	CHARTERREGPLATES, 
	INDIVIDUALLYOWNEDLIMOUSINE, 
	JITNEYS, 
	MIGRANTFARMWORKERSTRANSPORTATION, 
	MILEAGEAUDIT, 
	MILEAGEESTIMATE, 
	MILEAGETOTAL, 
	MILEAGETOTALAUDIT, 
	MILEAGETOTALESTIMATE, 
	PUBLICGROUPTYPE, 
	PUBLICTYPE, 
	RIDESHARING, 
	RISKPUBLICINPUTNYBLACKCAR, 
	SEATINGCAPACITY, 
	MECHANICALLIFT
	FROM EXP_TRANS
),