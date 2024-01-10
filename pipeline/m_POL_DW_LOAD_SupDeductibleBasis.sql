WITH
SQ_GTAMX401Stage AS (
	SELECT DISTINCT 
	LocationCode, 
	MasterCompanyNumber, 
	TypeBureauCode, 
	MajorPerilCode, 
	CoverageCode, 
	BureauCoverageCode, 
	DecutibleType, 
	DecutibleAmount, 
	SublineCode 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME}
),
EXP_Default AS (
	SELECT
	LocationCode AS i_LocationCode,
	MasterCompanyNumber AS i_MasterCompanyNumber,
	TypeBureauCode AS i_TypeBureauCode,
	MajorPerilCode AS i_MajorPerilCode,
	CoverageCode AS i_CoverageCode,
	BureauCoverageCode AS i_BureauCoverageCode,
	DecutibleType AS i_DecutibleType,
	DecutibleAmount AS i_DecutibleAmount,
	SublineCode AS i_SublineCode,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_SyeDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_LocationCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_LocationCode) AS o_LocationCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MasterCompanyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_MasterCompanyNumber) AS o_MasterCompanyNumber,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_TypeBureauCode) AS o_TypeBureauCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_MajorPerilCode) AS o_MajorPerilCode,
	-- *INF*: IIF(ISNULL(i_CoverageCode) OR IS_SPACES(i_CoverageCode) OR LENGTH(i_CoverageCode)=0, 
	-- '000', 
	-- LTRIM(RTRIM(i_CoverageCode))
	-- )
	IFF(i_CoverageCode IS NULL OR IS_SPACES(i_CoverageCode) OR LENGTH(i_CoverageCode) = 0, '000', LTRIM(RTRIM(i_CoverageCode))) AS o_CoverageCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BureauCoverageCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_BureauCoverageCode) AS o_BureauCoverageCode,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_DecutibleType)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_DecutibleType) AS o_DeductibleBasis,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_DecutibleAmount)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_DecutibleAmount) AS o_DecutibleAmount,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SublineCode)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_SublineCode) AS o_SublineCode,
	-- *INF*: DECODE(:UDF.DEFAULT_VALUE_FOR_STRINGS(i_DecutibleType),
	-- 'D','Flat Dollar Deductible',
	-- 'F','Full coverage Glass Deductible',
	-- 'P','Percentage Deductible',
	-- 'N/A'
	-- )
	DECODE(:UDF.DEFAULT_VALUE_FOR_STRINGS(i_DecutibleType),
		'D', 'Flat Dollar Deductible',
		'F', 'Full coverage Glass Deductible',
		'P', 'Percentage Deductible',
		'N/A') AS o_DeductibleBasisDescription
	FROM SQ_GTAMX401Stage
),
LKP_SupDeductibleBasis AS (
	SELECT
	SupDeductibleBasisId,
	DeductibleBasisDescription,
	LocationCode,
	MasterCompanyNumber,
	TypeBureauCode,
	MajorPerilCode,
	CoverageCode,
	BureauCoverageCode,
	DeductibleBasis,
	DecutibleAmount,
	SublineCode
	FROM (
		SELECT 
			SupDeductibleBasisId,
			DeductibleBasisDescription,
			LocationCode,
			MasterCompanyNumber,
			TypeBureauCode,
			MajorPerilCode,
			CoverageCode,
			BureauCoverageCode,
			DeductibleBasis,
			DecutibleAmount,
			SublineCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleBasis
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LocationCode,MasterCompanyNumber,TypeBureauCode,MajorPerilCode,CoverageCode,BureauCoverageCode,DeductibleBasis,DecutibleAmount,SublineCode ORDER BY SupDeductibleBasisId) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_SupDeductibleBasis.SupDeductibleBasisId AS lkp_SupDeductibleBasisId,
	LKP_SupDeductibleBasis.DeductibleBasisDescription AS lkp_DeductibleBasisDescription,
	EXP_Default.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_Default.o_AuditId AS AuditId,
	EXP_Default.o_EffectiveDate AS EffectiveDate,
	EXP_Default.o_ExpirationDate AS ExpirationDate,
	EXP_Default.o_SourceSystemId AS SourceSystemId,
	EXP_Default.o_SyeDate AS SystemDate,
	EXP_Default.o_LocationCode AS LocationCode,
	EXP_Default.o_MasterCompanyNumber AS MasterCompanyNumber,
	EXP_Default.o_TypeBureauCode AS TypeBureauCode,
	EXP_Default.o_MajorPerilCode AS MajorPerilCode,
	EXP_Default.o_CoverageCode AS CoverageCode,
	EXP_Default.o_BureauCoverageCode AS BureauCoverageCode,
	EXP_Default.o_DeductibleBasis AS DeductibleBasis,
	EXP_Default.o_DecutibleAmount AS DecutibleAmount,
	EXP_Default.o_SublineCode AS SublineCode,
	EXP_Default.o_DeductibleBasisDescription AS DeductibleBasisDescription
	FROM EXP_Default
	LEFT JOIN LKP_SupDeductibleBasis
	ON LKP_SupDeductibleBasis.LocationCode = EXP_Default.o_LocationCode AND LKP_SupDeductibleBasis.MasterCompanyNumber = EXP_Default.o_MasterCompanyNumber AND LKP_SupDeductibleBasis.TypeBureauCode = EXP_Default.o_TypeBureauCode AND LKP_SupDeductibleBasis.MajorPerilCode = EXP_Default.o_MajorPerilCode AND LKP_SupDeductibleBasis.CoverageCode = EXP_Default.o_CoverageCode AND LKP_SupDeductibleBasis.BureauCoverageCode = EXP_Default.o_BureauCoverageCode AND LKP_SupDeductibleBasis.DeductibleBasis = EXP_Default.o_DeductibleBasis AND LKP_SupDeductibleBasis.DecutibleAmount = EXP_Default.o_DecutibleAmount AND LKP_SupDeductibleBasis.SublineCode = EXP_Default.o_SublineCode
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ISNULL(lkp_SupDeductibleBasisId)),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE NOT ISNULL(lkp_SupDeductibleBasisId) AND lkp_DeductibleBasisDescription<>DeductibleBasisDescription),
UPD_SupDeductibleBasis AS (
	SELECT
	lkp_SupDeductibleBasisId AS SupDeductibleBasisId, 
	SystemDate, 
	DeductibleBasisDescription
	FROM RTR_Insert_Update_UPDATE
),
SupDeductibleBasis_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleBasis AS T
	USING UPD_SupDeductibleBasis AS S
	ON T.SupDeductibleBasisId = S.SupDeductibleBasisId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.SystemDate, T.DeductibleBasisDescription = S.DeductibleBasisDescription
),
SupDeductibleBasis_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDeductibleBasis
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LocationCode, MasterCompanyNumber, TypeBureauCode, MajorPerilCode, CoverageCode, BureauCoverageCode, DeductibleBasis, DecutibleAmount, SublineCode, DeductibleBasisDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	SystemDate AS CREATEDDATE, 
	SystemDate AS MODIFIEDDATE, 
	LOCATIONCODE, 
	MASTERCOMPANYNUMBER, 
	TYPEBUREAUCODE, 
	MAJORPERILCODE, 
	COVERAGECODE, 
	BUREAUCOVERAGECODE, 
	DEDUCTIBLEBASIS, 
	DECUTIBLEAMOUNT, 
	SUBLINECODE, 
	DEDUCTIBLEBASISDESCRIPTION
	FROM RTR_Insert_Update_INSERT
),