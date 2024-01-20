WITH
SQ_UnderwriterProductRelationshipStage AS (
	SELECT
		UnderwriterProductRelationshipStageID,
		AgencyODSSourceSystemID,
		HashKey,
		ModifiedUserID,
		ModifiedDate,
		AssociateID,
		WestBendAssociateID,
		StrategicProfitCenterCode,
		PolicyOfferingCode,
		ProgramCode,
		PolicyAmountMinimum,
		PolicyAmountMaximum,
		AgencyODSRelationshipId,
		ExtractDate,
		AsOfDate,
		RecordCount,
		SourceSystemID,
		InsuranceSegmentCode,
		BondCategory
	FROM UnderwriterProductRelationshipStage
),
LKP_Existing AS (
	SELECT
	in_AgencyODSRelationshipId,
	AgencyODSRelationshipId,
	HashKey,
	InsuranceSegmentCode,
	BondCategory
	FROM (
		select	a.ModifiedDate as ModifiedDate,
				a.HashKey as HashKey,
				a.AgencyODSRelationshipId as AgencyODSRelationshipId,
				a.InsuranceSegmentCode as InsuranceSegmentCode,
				a.BondCategory as BondCategory
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwriterProductRelationshipStage a
		inner join (
					select AgencyODSRelationshipId, max(ModifiedDate) as ModifiedDate
					from @{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchUnderwriterProductRelationshipStage
					group by AgencyODSRelationshipId) b
		on  a.AgencyODSRelationshipId = b.AgencyODSRelationshipId
		and a.ModifiedDate = b.ModifiedDate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY AgencyODSRelationshipId ORDER BY in_AgencyODSRelationshipId) = 1
),
EXP_AddAuditID AS (
	SELECT
	SQ_UnderwriterProductRelationshipStage.UnderwriterProductRelationshipStageID,
	SQ_UnderwriterProductRelationshipStage.AgencyODSSourceSystemID,
	SQ_UnderwriterProductRelationshipStage.HashKey,
	SQ_UnderwriterProductRelationshipStage.ModifiedUserID,
	SQ_UnderwriterProductRelationshipStage.ModifiedDate,
	SQ_UnderwriterProductRelationshipStage.AssociateID,
	SQ_UnderwriterProductRelationshipStage.WestBendAssociateID,
	SQ_UnderwriterProductRelationshipStage.StrategicProfitCenterCode,
	SQ_UnderwriterProductRelationshipStage.PolicyOfferingCode,
	SQ_UnderwriterProductRelationshipStage.ProgramCode,
	SQ_UnderwriterProductRelationshipStage.PolicyAmountMinimum,
	SQ_UnderwriterProductRelationshipStage.PolicyAmountMaximum,
	SQ_UnderwriterProductRelationshipStage.AgencyODSRelationshipId,
	SQ_UnderwriterProductRelationshipStage.ExtractDate,
	SQ_UnderwriterProductRelationshipStage.AsOfDate,
	SQ_UnderwriterProductRelationshipStage.RecordCount,
	SQ_UnderwriterProductRelationshipStage.SourceSystemID,
	SQ_UnderwriterProductRelationshipStage.InsuranceSegmentCode,
	SQ_UnderwriterProductRelationshipStage.BondCategory,
	LKP_Existing.HashKey AS lkp_HashKey,
	LKP_Existing.InsuranceSegmentCode AS lkp_InsuranceSegmentCode,
	LKP_Existing.BondCategory AS lkp_BondCategory,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: Decode(true,
	-- HashKey = lkp_HashKey
	-- AND InsuranceSegmentCode=lkp_InsuranceSegmentCode
	-- AND BondCategory=lkp_BondCategory, 'IGNORE',
	-- IsNull(lkp_HashKey), 'INSERT',
	-- 'UPDATE')
	Decode(
	    true,
	    HashKey = lkp_HashKey AND InsuranceSegmentCode = lkp_InsuranceSegmentCode AND BondCategory = lkp_BondCategory, 'IGNORE',
	    lkp_HashKey IS NULL, 'INSERT',
	    'UPDATE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM SQ_UnderwriterProductRelationshipStage
	LEFT JOIN LKP_Existing
	ON LKP_Existing.AgencyODSRelationshipId = SQ_UnderwriterProductRelationshipStage.AgencyODSRelationshipId
),
FIL_ChangesOnly AS (
	SELECT
	UnderwriterProductRelationshipStageID, 
	AgencyODSSourceSystemID, 
	HashKey, 
	ModifiedUserID, 
	ModifiedDate, 
	AssociateID, 
	WestBendAssociateID, 
	StrategicProfitCenterCode, 
	PolicyOfferingCode, 
	ProgramCode, 
	PolicyAmountMinimum, 
	PolicyAmountMaximum, 
	AgencyODSRelationshipId, 
	ExtractDate, 
	AsOfDate, 
	RecordCount, 
	SourceSystemID, 
	InsuranceSegmentCode, 
	BondCategory, 
	o_AuditID, 
	o_ChangeFlag
	FROM EXP_AddAuditID
	WHERE o_ChangeFlag = 'INSERT' OR o_ChangeFlag = 'UPDATE'
),
ArchUnderwriterProductRelationshipStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchUnderwriterProductRelationshipStage
	(UnderwritingProductRelationshipStageID, AgencyODSSourceSystemID, HashKey, ModifiedUserID, ModifiedDate, AssociateID, WestBendAssociateID, StrategicProfitCenterCode, PolicyOfferingCode, ProgramCode, PolicyAmountMinimum, PolicyAmountMaximum, AgencyODSRelationshipId, ExtractDate, AsOfDate, RecordCount, SourceSystemID, AuditID, InsuranceSegmentCode, BondCategory)
	SELECT 
	UnderwriterProductRelationshipStageID AS UNDERWRITINGPRODUCTRELATIONSHIPSTAGEID, 
	AGENCYODSSOURCESYSTEMID, 
	HASHKEY, 
	MODIFIEDUSERID, 
	MODIFIEDDATE, 
	ASSOCIATEID, 
	WESTBENDASSOCIATEID, 
	STRATEGICPROFITCENTERCODE, 
	POLICYOFFERINGCODE, 
	PROGRAMCODE, 
	POLICYAMOUNTMINIMUM, 
	POLICYAMOUNTMAXIMUM, 
	AGENCYODSRELATIONSHIPID, 
	EXTRACTDATE, 
	ASOFDATE, 
	RECORDCOUNT, 
	SOURCESYSTEMID, 
	o_AuditID AS AUDITID, 
	INSURANCESEGMENTCODE, 
	BONDCATEGORY
	FROM FIL_ChangesOnly
),