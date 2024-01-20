WITH
SQ_DCCACoverageCollisionStaging AS (
	SELECT
		DCCACoverageCollisionStagingId,
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
		ExtractDate,
		SourceSystemId
	FROM DCCACoverageCollisionStaging
),
EXPTRANS AS (
	SELECT
	DCCACoverageCollisionStagingId,
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_DCCACoverageCollisionStaging
),
archDCCACoverageCollisionStaging AS (
	INSERT INTO archDCCACoverageCollisionStaging
	(DCCACoverageCollisionStagingId, CoverageId, CA_CoverageCollisionId, SessionId, AcceptCollisionCoverage, AutoDealersTotalCollisionRatingUnits, BroadenedCollision, CollisionType, LimitedFullCoverageCollision, NumberNamedDrivers, NumberOfSafetyFeatures, UnderwriterOverride, ExperienceRatingBasicLimitPremium, ExperienceRatingBasicLimitPremiumGarage, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCCACOVERAGECOLLISIONSTAGINGID, 
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
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),