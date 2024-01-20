WITH
SQ_SilverCircle AS (
	SELECT
		SilverCircle.AgencyID,
		SilverCircle.SilverCircleYear,
		SilverCircle.SilverCircleLevelDescription,
		SilverCircle.SourceSystemID,
		SilverCircle.HashKey,
		SilverCircle.ModifiedUserId,
		SilverCircle.ModifiedDate,
		Agency.AgencyCode
	FROM Agency
	INNER JOIN SilverCircle
	ON Agency.AgencyID = SilverCircle.AgencyID
),
EXPTRANS AS (
	SELECT
	AgencyID,
	SilverCircleYear,
	SilverCircleLevelDescription,
	SourceSystemID,
	HashKey,
	ModifiedUserId,
	ModifiedDate,
	AgencyCode,
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.SOURCESYSTEMID} AS SourceSystemID_1
	FROM SQ_SilverCircle
),
AgencySilverCircleTierStaging AS (
	TRUNCATE TABLE AgencySilverCircleTierStaging;
	INSERT INTO AgencySilverCircleTierStaging
	(AgencyCode, SilverCircleYear, SilverCircleLevelDescription, HashKey, ModifiedUserId, ModifiedDate, ExtractDate, SourceSystemId)
	SELECT 
	AGENCYCODE, 
	SILVERCIRCLEYEAR, 
	SILVERCIRCLELEVELDESCRIPTION, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	EXTRACTDATE, 
	SourceSystemID_1 AS SOURCESYSTEMID
	FROM EXPTRANS
),