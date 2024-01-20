WITH
SQ_DC_CA_CoverageCollision AS (
	WITH cte_DCCACoverageCollision(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.dbo.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from DC_Session A Inner join DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	
	SELECT 
	X.CoverageId, 
	X.CA_CoverageCollisionId, 
	X.SessionId, 
	X.AcceptCollisionCoverage, 
	X.AutoDealersTotalCollisionRatingUnits, 
	X.BroadenedCollision, 
	X.CollisionType, 
	X.LimitedFullCoverageCollision, 
	X.NumberNamedDrivers, 
	X.NumberOfSafetyFeatures, 
	X.UnderwriterOverride, 
	X.ExperienceRatingBasicLimitPremium, 
	X.ExperienceRatingBasicLimitPremiumGarage 
	FROM
	 DC_CA_CoverageCollision X
	Inner join
	cte_DCCACoverageCollision Y
	on X.sessionid=Y.sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	CoverageId,
	CA_CoverageCollisionId,
	SessionId,
	AcceptCollisionCoverage,
	AutoDealersTotalCollisionRatingUnits,
	BroadenedCollision,
	CollisionType,
	LimitedFullCoverageCollision,
	NumberNamedDrivers,
	NumberOfSafetyFeatures,
	UnderwriterOverride,
	ExperienceRatingBasicLimitPremium,
	ExperienceRatingBasicLimitPremiumGarage,
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID
	FROM SQ_DC_CA_CoverageCollision
),
DCCACoverageCollisionStaging AS (
	TRUNCATE TABLE DCCACoverageCollisionStaging;
	INSERT INTO DCCACoverageCollisionStaging
	(CoverageId, CA_CoverageCollisionId, SessionId, AcceptCollisionCoverage, AutoDealersTotalCollisionRatingUnits, BroadenedCollision, CollisionType, LimitedFullCoverageCollision, NumberNamedDrivers, NumberOfSafetyFeatures, UnderwriterOverride, ExperienceRatingBasicLimitPremium, ExperienceRatingBasicLimitPremiumGarage, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	CA_COVERAGECOLLISIONID, 
	SESSIONID, 
	ACCEPTCOLLISIONCOVERAGE, 
	AUTODEALERSTOTALCOLLISIONRATINGUNITS, 
	BROADENEDCOLLISION, 
	COLLISIONTYPE, 
	LIMITEDFULLCOVERAGECOLLISION, 
	NUMBERNAMEDDRIVERS, 
	NUMBEROFSAFETYFEATURES, 
	UNDERWRITEROVERRIDE, 
	EXPERIENCERATINGBASICLIMITPREMIUM, 
	EXPERIENCERATINGBASICLIMITPREMIUMGARAGE, 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID
	FROM EXPTRANS
),