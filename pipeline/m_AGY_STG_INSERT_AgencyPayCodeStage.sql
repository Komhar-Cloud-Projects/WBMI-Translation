WITH
SQ_AgencyPayCode AS (
	SELECT
		AgencyPayCodeID,
		SourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		PayCode,
		CommissionScheduleCode,
		PayCodeEffectiveDate,
		PayCodeExpirationDate
	FROM AgencyPayCode
),
EXP_GetData AS (
	SELECT
	AgencyPayCodeID,
	SourceSystemID AS AgencyODSSourceSystemID,
	HashKey,
	ModifiedUserID,
	ModifiedDate,
	AgencyID,
	AgencyCode,
	PayCode,
	CommissionScheduleCode,
	PayCodeEffectiveDate,
	PayCodeExpirationDate,
	sysdate AS CurrentDate,
	1 AS RecordCount,
	@{pipeline().parameters.SOURCESYSTEMID} AS SourceSystemID
	FROM SQ_AgencyPayCode
),
AgencyPayCodeStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyPayCodeStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.AgencyPayCodeStage
	(AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, PayCode, CommissionScheduleCode, PayCodeEffectiveDate, PayCodeExpirationDate, ExtractDate, AsOfDate, RecordCount, SourceSystemID)
	SELECT 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	AGENCYID, 
	AGENCYCODE, 
	PAYCODE, 
	COMMISSIONSCHEDULECODE, 
	PAYCODEEFFECTIVEDATE, 
	PAYCODEEXPIRATIONDATE, 
	CurrentDate AS EXTRACTDATE, 
	CurrentDate AS ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID
	FROM EXP_GetData
),