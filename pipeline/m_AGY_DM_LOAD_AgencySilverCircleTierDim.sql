WITH
SQ_AgencySilverCircleTier AS (
	SELECT
		AgencySilverCircleTierId,
		AgencyCode,
		SilverCircleYear,
		SilverCircleLevelDescription,
		HashKey,
		ModifiedUserId,
		ModifiedDate,
		CreatedDate,
		SourceSystemId,
		AuditId
	FROM AgencySilverCircleTier
),
EXPTRANS AS (
	SELECT
	AgencySilverCircleTierId,
	AgencyCode,
	SilverCircleYear,
	SilverCircleLevelDescription,
	HashKey,
	ModifiedUserId,
	ModifiedDate,
	CreatedDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_AgencySilverCircleTier
),
AgencySilverCircleTierDim AS (
	TRUNCATE TABLE AgencySilverCircleTierDim;
	INSERT INTO AgencySilverCircleTierDim
	(EDWAgencySilverCircleTierPKId, AgencyCode, SilverCircleYear, SilverCircleLevelDescription, HashKey, ModifiedUserId, ModifiedDate, CreatedDate, SourceSystemId, AuditId)
	SELECT 
	AgencySilverCircleTierId AS EDWAGENCYSILVERCIRCLETIERPKID, 
	AGENCYCODE, 
	SILVERCIRCLEYEAR, 
	SILVERCIRCLELEVELDESCRIPTION, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	CREATEDDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),