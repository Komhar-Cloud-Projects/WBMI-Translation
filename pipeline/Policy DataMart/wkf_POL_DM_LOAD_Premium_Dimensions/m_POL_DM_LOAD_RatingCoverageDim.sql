WITH
SQ_RatingCoverage AS (
	SELECT
		RatingCoverageId,
		EffectiveDate,
		ExpirationDate,
		RatingCoverageAKID,
		StatisticalCoverageAKID,
		PolicyCoverageAKID,
		RatingCoverageKey,
		CoverageForm,
		ClassCode,
		RiskType,
		CoverageType,
		Exposure,
		RatingCoverageEffectiveDate,
		RatingCoverageExpirationDate
	FROM RatingCoverage
	WHERE CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_MetaData AS (
	SELECT
	RatingCoverageId,
	EffectiveDate,
	ExpirationDate,
	RatingCoverageAKID,
	StatisticalCoverageAKID,
	PolicyCoverageAKID,
	RatingCoverageKey,
	CoverageForm,
	ClassCode,
	RiskType,
	CoverageType,
	Exposure,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate
	FROM SQ_RatingCoverage
),
LKP_RatingCoverageDim AS (
	SELECT
	EDWRatingCoveragePKId
	FROM (
		SELECT 
			EDWRatingCoveragePKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverageDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWRatingCoveragePKId ORDER BY EDWRatingCoveragePKId) = 1
),
EXP_CalculationValue AS (
	SELECT
	LKP_RatingCoverageDim.EDWRatingCoveragePKId AS lkp_EDWRatingCoveragePKID,
	EXP_MetaData.RatingCoverageId AS i_RatingCoverageId,
	EXP_MetaData.EffectiveDate AS i_EffectiveDate,
	EXP_MetaData.ExpirationDate AS i_ExpirationDate,
	EXP_MetaData.RatingCoverageAKID AS i_RatingCoverageAKID,
	EXP_MetaData.StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	EXP_MetaData.PolicyCoverageAKID AS i_PolicyCoverageAKID,
	EXP_MetaData.RatingCoverageKey AS i_RatingCoverageKey,
	EXP_MetaData.CoverageForm AS i_CoverageForm,
	EXP_MetaData.ClassCode AS i_ClassCode,
	EXP_MetaData.RiskType AS i_RiskType,
	EXP_MetaData.CoverageType AS i_CoverageType,
	EXP_MetaData.Exposure AS i_Exposure,
	EXP_MetaData.RatingCoverageEffectiveDate AS i_RatingCoverageEffectiveDate,
	EXP_MetaData.RatingCoverageExpirationDate AS i_RatingCoverageExpirationDate,
	EXP_MetaData.o_CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	EXP_MetaData.o_AuditID AS i_AuditID,
	EXP_MetaData.o_CreatedDate AS i_CreatedDate,
	EXP_MetaData.o_ModifiedDate AS i_ModifiedDate,
	i_CoverageType||'-'||i_RiskType AS v_CoverageTypeCode,
	i_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	i_AuditID AS o_AuditID,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	i_CreatedDate AS o_CreatedDate,
	i_ModifiedDate AS o_ModifiedDate,
	i_RatingCoverageId AS o_RatingCoverageId,
	i_RatingCoverageAKID AS o_RatingCoverageAKID,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	i_PolicyCoverageAKID AS o_PolicyCoverageAKID,
	i_RatingCoverageKey AS o_RatingCoverageKey,
	i_CoverageForm AS o_CoverageForm,
	i_ClassCode AS o_ClassCode,
	'N/A' AS o_ClassCodeDescription,
	v_CoverageTypeCode AS o_CoverageTypeCode,
	'N/A' AS o_RiskPerilCode,
	i_Exposure AS o_Exposure,
	i_RatingCoverageEffectiveDate AS o_RatingCoverageEffectiveDate,
	i_RatingCoverageExpirationDate AS o_RatingCoverageExpirationDate
	FROM EXP_MetaData
	LEFT JOIN LKP_RatingCoverageDim
	ON LKP_RatingCoverageDim.EDWRatingCoveragePKId = EXP_MetaData.RatingCoverageId
),
FLT_RatingCoverageDim AS (
	SELECT
	lkp_EDWRatingCoveragePKID, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_AuditID AS AuditID, 
	o_EffectiveDate AS EffectiveDate, 
	o_ExpirationDate AS ExpirationDate, 
	o_CreatedDate AS CreatedDate, 
	o_ModifiedDate AS ModifiedDate, 
	o_RatingCoverageId AS RatingCoverageId, 
	o_RatingCoverageAKID AS RatingCoverageAKID, 
	o_StatisticalCoverageAKID AS StatisticalCoverageAKID, 
	o_PolicyCoverageAKID AS PolicyCoverageAKID, 
	o_RatingCoverageKey AS RatingCoverageKey, 
	o_CoverageForm AS CoverageForm, 
	o_ClassCode AS ClassCode, 
	o_ClassCodeDescription AS ClassCodeDescription, 
	o_CoverageTypeCode AS CoverageTypeCode, 
	o_RiskPerilCode AS RiskPerilCode, 
	o_Exposure AS Exposure, 
	o_RatingCoverageEffectiveDate AS RatingCoverageEffectiveDate, 
	o_RatingCoverageExpirationDate AS RatingCoverageExpirationDate
	FROM EXP_CalculationValue
	WHERE ISNULL(lkp_EDWRatingCoveragePKID)
),
RatingCoverageDim_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverageDim
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, EDWRatingCoveragePKId, EDWRatingCoverageAKId, EDWStatisticalCoverageAKId, EDWPolicyCoverageAKId, RatingCoverageKey, CoverageFormCode, ClassCode, ClassCodeDescription, CoverageTypeCode, RiskPerilCode, Exposure, RatingCoverageEffectiveDate, RatingCoverageExpirationDate)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	RatingCoverageId AS EDWRATINGCOVERAGEPKID, 
	RatingCoverageAKID AS EDWRATINGCOVERAGEAKID, 
	StatisticalCoverageAKID AS EDWSTATISTICALCOVERAGEAKID, 
	PolicyCoverageAKID AS EDWPOLICYCOVERAGEAKID, 
	RATINGCOVERAGEKEY, 
	CoverageForm AS COVERAGEFORMCODE, 
	CLASSCODE, 
	CLASSCODEDESCRIPTION, 
	COVERAGETYPECODE, 
	RISKPERILCODE, 
	EXPOSURE, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE
	FROM FLT_RatingCoverageDim
),
SQ_RatingCoverage_Logical_Delete AS (
	SELECT 
	RC.RatingCoverageID,
	RC.RatingCoverageExpirationDate
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
	WHERE
	RC.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	AND
	RC.CurrentSnapshotFlag='0'
),
EXP_RatingCoverageDim AS (
	SELECT
	RatingCoverageId AS RatingCoverageID,
	'0' AS o_CurrentSnapshotFlag,
	SYSDATE AS o_ModifiedDate
	FROM SQ_RatingCoverage_Logical_Delete
),
LKP_RatingCoverageDim_UPD AS (
	SELECT
	RatingCoverageDimId,
	EDWRatingCoveragePKId
	FROM (
		SELECT 
			RatingCoverageDimId,
			EDWRatingCoveragePKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverageDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWRatingCoveragePKId ORDER BY RatingCoverageDimId) = 1
),
FLT_RatingCoverageDimUPD AS (
	SELECT
	LKP_RatingCoverageDim_UPD.EDWRatingCoveragePKId AS i_EDWRatingCoveragePKID, 
	LKP_RatingCoverageDim_UPD.RatingCoverageDimId AS RatingCoverageDimID, 
	EXP_RatingCoverageDim.o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	EXP_RatingCoverageDim.o_ModifiedDate AS ModifiedDate, 
	SQ_RatingCoverage_Logical_Delete.RatingCoverageExpirationDate
	FROM EXP_RatingCoverageDim
	 -- Manually join with SQ_RatingCoverage_Logical_Delete
	LEFT JOIN LKP_RatingCoverageDim_UPD
	ON LKP_RatingCoverageDim_UPD.EDWRatingCoveragePKId = EXP_RatingCoverageDim.RatingCoverageID
	WHERE NOT ISNULL(i_EDWRatingCoveragePKID)
),
UPD_RatingCoverageDim AS (
	SELECT
	RatingCoverageDimID, 
	CurrentSnapshotFlag, 
	ModifiedDate, 
	RatingCoverageExpirationDate
	FROM FLT_RatingCoverageDimUPD
),
RatingCoverageDim_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverageDim AS T
	USING UPD_RatingCoverageDim AS S
	ON T.RatingCoverageDimId = S.RatingCoverageDimID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ModifiedDate = S.ModifiedDate, T.RatingCoverageExpirationDate = S.RatingCoverageExpirationDate
),