WITH
SQ_DCDeductibleStaging AS (
	WITH PCoverage as (
	select A.SessionId, A.ObjectId AS ObjectId, A.ObjectName AS ObjectName, A.coverageID as PCoverageid,A.CoverageId, A.Id AS CoverageGUID, A.Type as CoverageType, 
	CASE WHEN A.ObjectName = 'DC_Line' THEN L.Type 
	ELSE 
	CASE substring(A.ObjectName,4,3) 
	when 'CF_' then 'Property'
	when 'GL_' then 'GeneralLiability'
	when 'WC_' then 'WorkersCompensation'
	when 'BP_' then 'BusinessOwners'
	when 'CR_' then 'Crime'
	when 'IM_' then 'InlandMarine'
	when 'EXL' then 'ExcessLiability'
	when 'CU_' then 'CommercialUmbrella'
	when 'CA_' then 'CommercialAuto'
	when 'CDO' then 'DirectorsAndOffsCondos'
	when 'EPL' then 'EmploymentPracticesLiab'
	when 'HIO' then 'HoleInOne'
	ELSE 'N/A' 
	END END AS InsuranceLine 
	from DCCoverageStaging A
	left join DCLineStaging L on L.sessionid = A.sessionid and A.objectid = L.lineID
	where A.ObjectName <> 'DC_Coverage'
	
	union all
	
	select B.SessionId, B.ObjectId AS ObjectId, B.ObjectName AS ObjectName,B.coverageID as PCoverageid, A.CoverageId, A.Id AS CoverageGUID,A.Type as CoverageType, 
	CASE WHEN B.ObjectName = 'DC_Line' THEN L.Type
	ELSE
	case substring(B.ObjectName,4,3) 
	when 'CF_' then 'Property'
	when 'GL_' then 'GeneralLiability'
	when 'WC_' then 'WorkersCompensation'
	when 'BP_' then 'BusinessOwners'
	when 'CR_' then 'Crime'
	when 'IM_' then 'InlandMarine'
	when 'EXL' then 'ExcessLiability'
	when 'CU_' then 'CommercialUmbrella'
	when 'CA_' then 'CommercialAuto'
	when 'CDO' then 'DirectorsAndOffsCondos'
	when 'EPL' then 'EmploymentPracticesLiab'
	when 'HIO' then 'HoleInOne'
	else 'N/A' 
	END END AS  InsuranceLine
	from DCCoverageStaging A
	inner join DCCoverageStaging B on A.SessionId=B.SessionId  and A.ObjectId=B.CoverageId and A.ObjectName='DC_Coverage'
	left join DCLineStaging L on L.sessionid = B.sessionid and B.objectid = L.lineID
	where B.ObjectName <> 'DC_Coverage'
	)
	
	select C.Coveragetype,C.CoverageGuid, C.CoverageId, DLT.Type as DeductibleType, DLT.Value as DeductibleValue, C.InsuranceLine as Insuranceline
	from DCDeductibleStaging DLT
	inner join PCoverage C on DLT.SessionId=C.SessionId and DLT.ObjectId=C.PCoverageId and  DLT.ObjectName='DC_Coverage'
	where DLT.Type is not null and DLT.Value is not null
	AND case when isnull(DLT.value,0) in ('N/A','0') then 0 else 1 end>0
	
	UNION    ---- By using UNION we are elimination duplicates coming from above and below queries
	
	---- Below query is used to pull the deductible values from DCLimit tables if deductibles are tied to child coverage rather than parent, this is to cover odd ----scenarios in data
	
	select C.CoverageType,C.CoverageGuid, C.CoverageId, DLT.Type as DeductibleType, DLT.Value as DeductibleValue, C.InsuranceLine as Insuranceline
	from DCDeductibleStaging DLT
	inner join PCoverage C on DLT.SessionId=C.SessionId and DLT.ObjectId=C.CoverageId and  DLT.ObjectName='DC_Coverage'
	where DLT.Type is not null and DLT.Value is not null
	AND case when isnull(DLT.value,0) in ('N/A','0') then 0 else 1 end>0
),
EXP_DataCollectSRC AS (
	SELECT
	CoverageType,
	CoverageGuid AS CoverageGuId,
	-- *INF*: IIF(ISNULL(CoverageGuId) OR IS_SPACES(CoverageGuId) OR LENGTH(CoverageGuId)=0, 'N/A', LTRIM(RTRIM(CoverageGuId)))
	IFF(CoverageGuId IS NULL OR IS_SPACES(CoverageGuId) OR LENGTH(CoverageGuId) = 0, 'N/A', LTRIM(RTRIM(CoverageGuId))) AS O_CoverageGUID,
	CoverageId,
	Type AS DeductibleType,
	Value,
	InsuranceLine,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCDeductibleStaging
),
SQ_WorkPremiumTransaction AS (
	SELECT wpt.PremiumTransactionAKId, wpt.PremiumTransactionStageId, pt.PremiumTransactionAKID, rc.CoverageType, rc.CoverageGUID, PC.InsuranceLine, rc.SubCoverageTypeCode
	FROM dbo.WorkPremiumTransaction wpt WITH (NOLOCK)
	JOIN dbo.PremiumTransaction pt WITH (NOLOCK) ON pt.PremiumTransactionAKID = wpt.PremiumTransactionAKId
	and pt.PremiumTransactionID not in (select WPTOL.PremiumTransactionID from dbo.WorkPremiumTransactionOffsetLineage WPTOL with (nolock) WHERE WPTOL.UpdateAttributeFlag = 1)
	JOIN dbo.RatingCoverage rc WITH (NOLOCK) ON pt.RatingCoverageAKId = rc.RatingCoverageAKID
		AND pt.EffectiveDate = rc.EffectiveDate
	INNER JOIN dbo.PolicyCoverage PC on PC.PolicyCoverageAKID = rc.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	--WHERE wpt.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
),
LKP_SupDeductibleTypeExclude AS (
	SELECT
	SupDeductibleTypeExcludeId,
	InsuranceLine,
	CoverageType,
	SubCoverageTypeCode
	FROM (
		SELECT 
			SupDeductibleTypeExcludeId,
			InsuranceLine,
			CoverageType,
			SubCoverageTypeCode
		FROM SupDeductibleTypeExclude
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLine,CoverageType,SubCoverageTypeCode ORDER BY SupDeductibleTypeExcludeId) = 1
),
FIL_RemoveExcludedCoverages AS (
	SELECT
	SQ_WorkPremiumTransaction.PremiumTransactionAKId, 
	SQ_WorkPremiumTransaction.PremiumTransactionStageId, 
	SQ_WorkPremiumTransaction.PT_PremiumTransactionAKID, 
	SQ_WorkPremiumTransaction.CoverageType, 
	SQ_WorkPremiumTransaction.CoverageGUID, 
	LKP_SupDeductibleTypeExclude.SupDeductibleTypeExcludeId
	FROM SQ_WorkPremiumTransaction
	LEFT JOIN LKP_SupDeductibleTypeExclude
	ON LKP_SupDeductibleTypeExclude.InsuranceLine = SQ_WorkPremiumTransaction.InsuranceLine AND LKP_SupDeductibleTypeExclude.CoverageType = SQ_WorkPremiumTransaction.CoverageType AND LKP_SupDeductibleTypeExclude.SubCoverageTypeCode = SQ_WorkPremiumTransaction.SubCoverageTypeCode
	WHERE ISNULL(SupDeductibleTypeExcludeId)
),
JNR_AKID_STAGEID AS (SELECT
	FIL_RemoveExcludedCoverages.PremiumTransactionAKId, 
	FIL_RemoveExcludedCoverages.PremiumTransactionStageId, 
	FIL_RemoveExcludedCoverages.PT_PremiumTransactionAKID, 
	FIL_RemoveExcludedCoverages.CoverageType, 
	FIL_RemoveExcludedCoverages.CoverageGUID, 
	EXP_DataCollectSRC.CoverageId, 
	EXP_DataCollectSRC.DeductibleType, 
	EXP_DataCollectSRC.Value, 
	EXP_DataCollectSRC.InsuranceLine, 
	EXP_DataCollectSRC.o_AuditId AS AuditId, 
	EXP_DataCollectSRC.O_CoverageGUID
	FROM EXP_DataCollectSRC
	INNER JOIN FIL_RemoveExcludedCoverages
	ON FIL_RemoveExcludedCoverages.PremiumTransactionStageId = EXP_DataCollectSRC.CoverageId AND FIL_RemoveExcludedCoverages.CoverageGUID = EXP_DataCollectSRC.O_CoverageGUID
),
mplt_Load_Deductibles_IL_Layer_DCT AS (WITH
	SEQ_CoverageDeductibleID AS (
		CREATE SEQUENCE SEQ_CoverageDeductibleID
		START = 0
		INCREMENT = 1;
	),
	INPUT AS (
		
	),
	EXP_Input AS (
		SELECT
		PremiumTransactionAKId,
		CoverageType,
		DeductibleType,
		DeductibleValue,
		InsuranceLine,
		AuditId
		FROM INPUT
	),
	LKP_Valid_Deductibles AS (
		SELECT
		StandardDeductibleType,
		DeductibleLevel,
		IN_InsuranceLine,
		IN_CoverageType,
		IN_DeductibleType,
		InsuranceLine,
		CoverageType,
		DeductibleType
		FROM (
			SELECT 
				StandardDeductibleType,
				DeductibleLevel,
				IN_InsuranceLine,
				IN_CoverageType,
				IN_DeductibleType,
				InsuranceLine,
				CoverageType,
				DeductibleType
			FROM SupDeductibleType
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLine,CoverageType,DeductibleType ORDER BY StandardDeductibleType) = 1
	),
	FIL_UnnecessaryDeductibles AS (
		SELECT
		LKP_Valid_Deductibles.DeductibleLevel, 
		EXP_Input.DeductibleValue AS Value, 
		EXP_Input.PremiumTransactionAKId, 
		LKP_Valid_Deductibles.StandardDeductibleType, 
		EXP_Input.AuditId
		FROM EXP_Input
		LEFT JOIN LKP_Valid_Deductibles
		ON LKP_Valid_Deductibles.InsuranceLine = EXP_Input.InsuranceLine AND LKP_Valid_Deductibles.CoverageType = EXP_Input.CoverageType AND LKP_Valid_Deductibles.DeductibleType = EXP_Input.DeductibleType
		WHERE Not ISNULL(DeductibleLevel)
	),
	EXP_Default_Values AS (
		SELECT
		StandardDeductibleType AS i_Type,
		Value AS i_Value,
		PremiumTransactionAKId,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_Type)
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Type) AS o_CoverageLimitType,
		-- *INF*: TO_CHAR(TO_DECIMAL(i_Value))
		TO_CHAR(TO_DECIMAL(i_Value)) AS o_CoverageLimitValue,
		AuditId
		FROM FIL_UnnecessaryDeductibles
	),
	AGG_VLAUE_TYPE AS (
		SELECT
		o_CoverageLimitType AS CoverageDeductibleType,
		o_CoverageLimitValue AS CoverageDeductibleValue,
		AuditId
		FROM EXP_Default_Values
		QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDeductibleType, CoverageDeductibleValue ORDER BY NULL) = 1
	),
	SRt_bridege_type_value AS (
		SELECT
		o_CoverageLimitType AS CoverageDeductibleType, 
		o_CoverageLimitValue AS CoverageDeductibleValue, 
		PremiumTransactionAKId
		FROM EXP_Default_Values
		ORDER BY CoverageDeductibleType ASC, CoverageDeductibleValue ASC
	),
	LKP_COVERAGEDEDUCTIBLEID AS (
		SELECT
		CoverageDeductibleId,
		CoverageDeductibleType,
		CoverageDeductibleValue,
		i_CoverageDeductibleType,
		i_CoverageDeductibleValue
		FROM (
			SELECT 
				CoverageDeductibleId,
				CoverageDeductibleType,
				CoverageDeductibleValue,
				i_CoverageDeductibleType,
				i_CoverageDeductibleValue
			FROM CoverageDeductible
			WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDeductibleType,CoverageDeductibleValue ORDER BY CoverageDeductibleId) = 1
	),
	EXP_Set_CoverageDeductibleID AS (
		SELECT
		LKP_COVERAGEDEDUCTIBLEID.CoverageDeductibleId AS lkp_CoverageDeductibleId,
		SEQ_CoverageDeductibleID.NEXTVAL AS i_NEXTVAL,
		AGG_VLAUE_TYPE.CoverageDeductibleType,
		AGG_VLAUE_TYPE.CoverageDeductibleValue,
		-- *INF*: IIF(ISNULL(lkp_CoverageDeductibleId),i_NEXTVAL,lkp_CoverageDeductibleId)
		IFF(lkp_CoverageDeductibleId IS NULL, i_NEXTVAL, lkp_CoverageDeductibleId) AS CoverageDeductibleId,
		AGG_VLAUE_TYPE.AuditId
		FROM AGG_VLAUE_TYPE
		LEFT JOIN LKP_COVERAGEDEDUCTIBLEID
		ON LKP_COVERAGEDEDUCTIBLEID.CoverageDeductibleType = AGG_VLAUE_TYPE.CoverageDeductibleType AND LKP_COVERAGEDEDUCTIBLEID.CoverageDeductibleValue = AGG_VLAUE_TYPE.CoverageDeductibleValue
	),
	SRT_type_vlaue AS (
		SELECT
		CoverageDeductibleType, 
		CoverageDeductibleValue, 
		CoverageDeductibleId, 
		AuditId
		FROM EXP_Set_CoverageDeductibleID
		ORDER BY CoverageDeductibleType ASC, CoverageDeductibleValue ASC
	),
	FIL_Insert_CoverageDeductible AS (
		SELECT
		lkp_CoverageDeductibleId, 
		CoverageDeductibleId, 
		CoverageDeductibleType, 
		CoverageDeductibleValue, 
		AuditId
		FROM EXP_Set_CoverageDeductibleID
		WHERE ISNULL(lkp_CoverageDeductibleId)
	),
	EXP_HANDE AS (
		SELECT
		CoverageDeductibleType AS i_CoverageDeductibleType,
		CoverageDeductibleValue AS i_CoverageDeductibleValue,
		CoverageDeductibleId AS i_CoverageDeductibleid,
		i_CoverageDeductibleid AS o_CoverageDeductibleid,
		AuditId,
		@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_sourceSystemid,
		sysdate AS o_date,
		i_CoverageDeductibleType AS o_CoverageDeductibleType,
		-- *INF*: IIF(NOT ISNULL(i_CoverageDeductibleValue),i_CoverageDeductibleValue,'0')
		IFF(NOT i_CoverageDeductibleValue IS NULL, i_CoverageDeductibleValue, '0') AS o_CoverageDeductibleVlaue
		FROM FIL_Insert_CoverageDeductible
	),
	JNR_type_value AS (SELECT
		SRt_bridege_type_value.CoverageDeductibleType AS i_CoverageDeductibleType_bridge, 
		SRt_bridege_type_value.CoverageDeductibleValue AS i_CoverageDeductibleValue_bridege, 
		SRt_bridege_type_value.PremiumTransactionAKId AS i_PremiumTransactionAKId, 
		SRT_type_vlaue.CoverageDeductibleType AS i_CoverageDeductibleType, 
		SRT_type_vlaue.CoverageDeductibleValue AS i_CoverageDeductibleValue, 
		SRT_type_vlaue.CoverageDeductibleId, 
		SRT_type_vlaue.AuditId
		FROM SRT_type_vlaue
		INNER JOIN SRt_bridege_type_value
		ON SRt_bridege_type_value.CoverageDeductibleType = SRT_type_vlaue.CoverageDeductibleType AND SRt_bridege_type_value.CoverageDeductibleValue = SRT_type_vlaue.CoverageDeductibleValue
	),
	LKP_CoverageDeductiblebridgeid AS (
		SELECT
		CoverageDeductibleBridgeId,
		CoverageDeductibleId,
		PremiumTransactionAKId
		FROM (
			SELECT CDB.CoverageDeductibleBridgeId as CoverageDeductibleBridgeId, 
			CDB.CoverageDeductibleId as CoverageDeductibleId, 
			CDB.PremiumTransactionAKId as PremiumTransactionAKId 
			FROM CoverageDeductibleBridge CDB 
			INNER JOIN dbo.WorkPremiumTransaction wpt WITH (NOLOCK)
			on  CDB.PremiumTransactionAKId = wpt.PremiumTransactionAKID 
			WHERE CDB.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDeductibleId,PremiumTransactionAKId ORDER BY CoverageDeductibleBridgeId) = 1
	),
	OUTPUT_CoverageDeductible_Insert AS (
		SELECT
		o_CoverageDeductibleid AS CoverageDeductibleId, 
		AuditId, 
		o_sourceSystemid AS SourceSystemId, 
		o_date AS CurrentDate, 
		o_CoverageDeductibleType AS CoverageDeductibleType, 
		o_CoverageDeductibleVlaue AS CoverageDeductibleValue
		FROM EXP_HANDE
	),
	FIL_Insert_CoverageDeductibleBridge AS (
		SELECT
		LKP_CoverageDeductiblebridgeid.CoverageDeductibleBridgeId AS i_CoverageDeductibleBridgeId, 
		JNR_type_value.i_PremiumTransactionAKId AS PremiumTransactionAKId, 
		JNR_type_value.CoverageDeductibleId, 
		JNR_type_value.AuditId
		FROM JNR_type_value
		LEFT JOIN LKP_CoverageDeductiblebridgeid
		ON LKP_CoverageDeductiblebridgeid.CoverageDeductibleId = JNR_type_value.CoverageDeductibleId AND LKP_CoverageDeductiblebridgeid.PremiumTransactionAKId = JNR_type_value.i_PremiumTransactionAKId
		WHERE ISNULL(i_CoverageDeductibleBridgeId) AND PremiumTransactionAKId<>-1
	),
	AGG_bridge_count AS (
		SELECT
		CoverageDeductibleId AS i_CoverageDeductibleId,
		PremiumTransactionAKId,
		-- *INF*: count(1)
		count(1) AS o_CoverageDeductibleIdCount,
		AuditId
		FROM FIL_Insert_CoverageDeductibleBridge
		GROUP BY i_CoverageDeductibleId, PremiumTransactionAKId
	),
	EXP_bridge_target AS (
		SELECT
		AuditId,
		@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_sourceSystemid,
		sysdate AS o_credatedate,
		PremiumTransactionAKId AS o_PremiumTransactionAKId,
		i_CoverageDeductibleId AS o_CoverageDeductibleId,
		o_CoverageDeductibleIdCount,
		'N/A' AS o_CoverageDeductibleControl
		FROM AGG_bridge_count
	),
	OUTPUT_CoverageDeductibleBridge_Insert AS (
		SELECT
		AuditId, 
		o_sourceSystemid AS SourceSystemId, 
		o_credatedate AS CurrentDate, 
		o_PremiumTransactionAKId AS PremiumTransactionAKId, 
		o_CoverageDeductibleId AS CoverageDeductibleId, 
		o_CoverageDeductibleIdCount AS CoverageDeductibleIdCount, 
		o_CoverageDeductibleControl AS CoverageDeductibleControl
		FROM EXP_bridge_target
	),
),
CoverageDeductibleBridge AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductibleBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageDeductibleId, CoverageDeductibleIdCount, CoverageDeductibleControl)
	SELECT 
	AuditId1 AS AUDITID, 
	SourceSystemId AS SOURCESYSTEMID, 
	CurrentDate AS CREATEDDATE, 
	PremiumTransactionAKId1 AS PREMIUMTRANSACTIONAKID, 
	COVERAGEDEDUCTIBLEID, 
	COVERAGEDEDUCTIBLEIDCOUNT, 
	COVERAGEDEDUCTIBLECONTROL
	FROM mplt_Load_Deductibles_IL_Layer_DCT
),
UPDTRANS AS (
	SELECT
	CoverageDeductibleId1 AS CoverageDeductibleId, 
	AuditId2 AS O_AUDIT, 
	SourceSystemId1 AS o_soureSystemid, 
	CurrentDate1 AS o_createdate, 
	CoverageDeductibleType AS o_CoverageDeductibletype, 
	CoverageDeductibleValue AS o_CoverageDeductiblevalue
	FROM mplt_Load_Deductibles_IL_Layer_DCT
),
CoverageDeductible AS (
	SET IDENTITY_INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductible  ON
	INSERT @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDeductible (CoverageDeductibleId ,AuditID,SourceSystemID,CreatedDate,CoverageDeductibleType,CoverageDeductibleValue) 
	SELECT S.CoverageDeductibleId,S.AuditID,S.SourceSystemID, S.CreatedDate,S.CoverageDeductibleType, S.CoverageDeductibleValue
	FROM UPDTRANS S
),
SQ_CoverageDeductibleBridge_Offsets AS (
	SELECT CDB.CoverageDeductibleId, CDB.CoverageDeductibleIdCount, CDB.CoverageDeductibleControl, WPTOL.PremiumTransactionAKID 
	FROM
	 WorkPremiumTransactionOffsetLineage WPTOL
	 inner join CoveragedeductibleBridge CDB on
	WPTOL.PreviousPremiumTransactionAKID = CDB.PremiumTransactionAKId
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
	WPTOL.premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Offset'
	where
	WPTOL.UpdateAttributeFlag = 1
),
EXP_CoverageDeductibleBridge_PassThrough AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	CoverageDeductibleId,
	CoverageDeductibleIdCount,
	CoverageDeductibleControl,
	PremiumTransactionAKID
	FROM SQ_CoverageDeductibleBridge_Offsets
),
LKP_CoverageDeductibleBridge_Offsets AS (
	SELECT
	CoverageDeductibleBridgeId,
	PremiumTransactionAKId,
	CoverageDeductibleId
	FROM (
		SELECT CDB.CoverageDeductibleBridgeId as CoverageDeductibleBridgeId, 
		CDB.PremiumTransactionAKId as PremiumTransactionAKId ,
		CDB.CoverageDeductibleId as CoverageDeductibleId
		FROM CoverageDeductibleBridge CDB 
		INNER JOIN (SELECT distinct PreviousPremiumTransactionAKID as PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage 
		UNION 
		SELECT DISTINCT PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage ) A
		on CDB.PremiumTransactionAKId = A.PremiumTransactionAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageDeductibleId ORDER BY CoverageDeductibleBridgeId) = 1
),
FIL_Offset_Inserts_CoverageDeductibleBridge AS (
	SELECT
	LKP_CoverageDeductibleBridge_Offsets.CoverageDeductibleBridgeId, 
	EXP_CoverageDeductibleBridge_PassThrough.AuditID, 
	EXP_CoverageDeductibleBridge_PassThrough.SourceSystemID, 
	EXP_CoverageDeductibleBridge_PassThrough.CreatedDate, 
	EXP_CoverageDeductibleBridge_PassThrough.CoverageDeductibleId, 
	EXP_CoverageDeductibleBridge_PassThrough.CoverageDeductibleIdCount, 
	EXP_CoverageDeductibleBridge_PassThrough.CoverageDeductibleControl, 
	EXP_CoverageDeductibleBridge_PassThrough.PremiumTransactionAKID
	FROM EXP_CoverageDeductibleBridge_PassThrough
	LEFT JOIN LKP_CoverageDeductibleBridge_Offsets
	ON LKP_CoverageDeductibleBridge_Offsets.PremiumTransactionAKId = EXP_CoverageDeductibleBridge_PassThrough.PremiumTransactionAKID AND LKP_CoverageDeductibleBridge_Offsets.CoverageDeductibleId = EXP_CoverageDeductibleBridge_PassThrough.CoverageDeductibleId
	WHERE ISNULL(CoverageDeductibleBridgeId)
),
TGT_CoverageDeductibleBridge_Offsets_Insert AS (
	INSERT INTO CoverageDeductibleBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageDeductibleId, CoverageDeductibleIdCount, CoverageDeductibleControl)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	COVERAGEDEDUCTIBLEID, 
	COVERAGEDEDUCTIBLEIDCOUNT, 
	COVERAGEDEDUCTIBLECONTROL
	FROM FIL_Offset_Inserts_CoverageDeductibleBridge
),
SQ_CoverageDeductibleBridge_Deprecated AS (
	SELECT CDB.CoverageDeductibleId, CDB.CoverageDeductibleIdCount, CDB.CoverageDeductibleControl, WPTOL.PremiumTransactionAKID 
	FROM
	 WorkPremiumTransactionOffsetLineage WPTOL
	 inner join CoveragedeductibleBridge CDB on
	WPTOL.PreviousPremiumTransactionAKID = CDB.PremiumTransactionAKId
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
	WPTOL.premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Deprecated'
	where
	WPTOL.UpdateAttributeFlag = 1
),
EXP_CoverageDeductibleBridge_PassThrough_Deprecated AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	CoverageDeductibleId,
	CoverageDeductibleIdCount,
	CoverageDeductibleControl,
	PremiumTransactionAKID
	FROM SQ_CoverageDeductibleBridge_Deprecated
),
LKP_CoverageDeductibleBridge_Deprecated AS (
	SELECT
	CoverageDeductibleBridgeId,
	PremiumTransactionAKId,
	CoverageDeductibleId
	FROM (
		SELECT CDB.CoverageDeductibleBridgeId as CoverageDeductibleBridgeId, 
		CDB.PremiumTransactionAKId as PremiumTransactionAKId ,
		CDB.CoverageDeductibleId as CoverageDeductibleId
		FROM CoverageDeductibleBridge CDB 
		INNER JOIN (SELECT distinct PreviousPremiumTransactionAKID as PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage 
		UNION 
		SELECT DISTINCT PremiumTransactionAKID FROM  dbo. WorkPremiumTransactionOffsetLineage ) A
		on CDB.PremiumTransactionAKId = A.PremiumTransactionAKID
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionAKId,CoverageDeductibleId ORDER BY CoverageDeductibleBridgeId) = 1
),
FIL_Offset_Inserts_CoverageDeductibleBridge_Deprecated AS (
	SELECT
	LKP_CoverageDeductibleBridge_Deprecated.CoverageDeductibleBridgeId, 
	EXP_CoverageDeductibleBridge_PassThrough_Deprecated.AuditID, 
	EXP_CoverageDeductibleBridge_PassThrough_Deprecated.SourceSystemID, 
	EXP_CoverageDeductibleBridge_PassThrough_Deprecated.CreatedDate, 
	EXP_CoverageDeductibleBridge_PassThrough_Deprecated.CoverageDeductibleId, 
	EXP_CoverageDeductibleBridge_PassThrough_Deprecated.CoverageDeductibleIdCount, 
	EXP_CoverageDeductibleBridge_PassThrough_Deprecated.CoverageDeductibleControl, 
	EXP_CoverageDeductibleBridge_PassThrough_Deprecated.PremiumTransactionAKID
	FROM EXP_CoverageDeductibleBridge_PassThrough_Deprecated
	LEFT JOIN LKP_CoverageDeductibleBridge_Deprecated
	ON LKP_CoverageDeductibleBridge_Deprecated.PremiumTransactionAKId = EXP_CoverageDeductibleBridge_PassThrough_Deprecated.PremiumTransactionAKID AND LKP_CoverageDeductibleBridge_Deprecated.CoverageDeductibleId = EXP_CoverageDeductibleBridge_PassThrough_Deprecated.CoverageDeductibleId
	WHERE ISNULL(CoverageDeductibleBridgeId)
),
TGT_CoverageDeductibleBridge_Deprecated_Insert AS (
	INSERT INTO CoverageDeductibleBridge
	(AuditID, SourceSystemID, CreatedDate, PremiumTransactionAKId, CoverageDeductibleId, CoverageDeductibleIdCount, CoverageDeductibleControl)
	SELECT 
	AUDITID, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	PremiumTransactionAKID AS PREMIUMTRANSACTIONAKID, 
	COVERAGEDEDUCTIBLEID, 
	COVERAGEDEDUCTIBLEIDCOUNT, 
	COVERAGEDEDUCTIBLECONTROL
	FROM FIL_Offset_Inserts_CoverageDeductibleBridge_Deprecated
),