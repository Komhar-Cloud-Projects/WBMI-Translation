WITH
SQ_AgencyPayCodeStage AS (
	SELECT
		AgencyPayCodeStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AgencyID,
		AgencyCode,
		PayCode,
		CommissionScheduleCode,
		PayCodeEffectiveDate,
		PayCodeExpirationDate,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID
	FROM AgencyPayCodeStage
),
LKP_Existing AS (
	SELECT
	HashKey,
	AgencyID,
	PayCodeEffectiveDate
	FROM (
		select	a.ModifiedDate as ModifiedDate,
				a.HashKey as HashKey,
				a.AgencyID as AgencyID,
			      a.PayCodeEffectiveDate as PayCodeEffectiveDate
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyPayCodeStage a
		inner join (
					select AgencyID, PayCodeEffectiveDate, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchAgencyPayCodeStage
					group by  AgencyID, PayCodeEffectiveDate) b
		on  a.AgencyID = b.AgencyID
		and a.PayCodeEffectiveDate = b.PayCodeEffectiveDate
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyID,PayCodeEffectiveDate ORDER BY HashKey) = 1
),
EXP_GetData AS (
	SELECT
	SQ_AgencyPayCodeStage.AgencyPayCodeStageID,
	SQ_AgencyPayCodeStage.AgencyODSSourceSystemID,
	SQ_AgencyPayCodeStage.HashKey,
	SQ_AgencyPayCodeStage.ModifiedUserID,
	SQ_AgencyPayCodeStage.ModifiedDate,
	SQ_AgencyPayCodeStage.AgencyID,
	SQ_AgencyPayCodeStage.AgencyCode,
	SQ_AgencyPayCodeStage.PayCode,
	SQ_AgencyPayCodeStage.CommissionScheduleCode,
	SQ_AgencyPayCodeStage.PayCodeEffectiveDate,
	SQ_AgencyPayCodeStage.PayCodeExpirationDate,
	SQ_AgencyPayCodeStage.ExtractDate,
	SQ_AgencyPayCodeStage.AsOfDate,
	SQ_AgencyPayCodeStage.RecordCount,
	SQ_AgencyPayCodeStage.SourceSystemID,
	LKP_Existing.HashKey AS lkp_HashKey,
	-- *INF*: Decode(true,
	-- HashKey = lkp_HashKey, 'IGNORE',
	-- IsNull(lkp_HashKey), 'INSERT',
	-- 'UPDATE')
	Decode(
	    true,
	    HashKey = lkp_HashKey, 'IGNORE',
	    lkp_HashKey IS NULL, 'INSERT',
	    'UPDATE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID
	FROM SQ_AgencyPayCodeStage
	LEFT JOIN LKP_Existing
	ON LKP_Existing.AgencyID = SQ_AgencyPayCodeStage.AgencyID AND LKP_Existing.PayCodeEffectiveDate = SQ_AgencyPayCodeStage.PayCodeEffectiveDate
),
FIL_ChangesOnly AS (
	SELECT
	AgencyPayCodeStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AgencyID, 
	AgencyCode, 
	PayCode, 
	CommissionScheduleCode, 
	PayCodeEffectiveDate, 
	PayCodeExpirationDate, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	o_AuditID AS AuditID, 
	o_ChangeFlag AS ChangeFlag
	FROM EXP_GetData
	WHERE ChangeFlag = 'INSERT' OR ChangeFlag = 'UPDATE'
),
ArchAgencyPayCodeStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchAgencyPayCodeStage
	(AgencyPayCodeStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AgencyID, AgencyCode, PayCode, CommissionScheduleCode, PayCodeEffectiveDate, PayCodeExpirationDate, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID)
	SELECT 
	AGENCYPAYCODESTAGEID, 
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
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	AUDITID
	FROM FIL_ChangesOnly
),