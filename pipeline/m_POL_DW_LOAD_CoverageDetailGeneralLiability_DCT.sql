WITH
LKP_WBNDOCoverageDirectorsAndOfficersNFPStage AS (
	SELECT
	RetroactiveDate,
	CoverageId
	FROM (
		SELECT 
			RetroactiveDate,
			CoverageId
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBNDOCoverageDirectorsAndOfficersNFPStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY RetroactiveDate) = 1
),
LKP_SupClassificationGeneralLiability AS (
	SELECT
	lkp_result,
	ClassCode,
	SublineCode,
	RatingStateCode
	FROM (
		SELECT ClassCode as ClassCode,
		SublineCode as SublineCode,
		RatingStateCode as RatingStateCode,
		ISOGeneralLiabilityClassSummary+'@1'
		       +ISOGeneralLiabilityClassGroupCode+'@2'
			     as lkp_result
		  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationGeneralLiability
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,SublineCode,RatingStateCode ORDER BY lkp_result) = 1
),
SQ_DCTWorkTable AS (
	SELECT
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageId,
		WorkDCTTransactionInsuranceLineLocationBridge.CoverageVersion,
		WorkDCTCoverageTransaction.CoverageGUID,
		WorkDCTTransactionInsuranceLineLocationBridge.RetroactiveDate,
		WorkDCTInsuranceLine.LineType,
		WorkDCTInsuranceLine.LineId
	FROM WorkDCTTransactionInsuranceLineLocationBridge
	INNER JOIN WorkDCTCoverageTransaction
	INNER JOIN WorkDCTInsuranceLine
	ON WorkDCTInsuranceLine.LineId=WorkDCTTransactionInsuranceLineLocationBridge.LineId
	and
	WorkDCTInsuranceLine.LineType in ('GeneralLiability','SBOPGeneralLiability','DirectorsAndOfficersNFP', 'DirectorsAndOffsCondos', 'EmploymentPracticesLiab')
	and
	WorkDCTCoverageTransaction.CoverageId=WorkDCTTransactionInsuranceLineLocationBridge.CoverageId
),
EXP_Stage AS (
	SELECT
	RetroactiveDate AS i_RetroactiveDate,
	LineType,
	CoverageId,
	CoverageVersion,
	CoverageGUID,
	-- *INF*: IIF(LineType='DirectorsAndOfficersNFP',
	-- :LKP.LKP_WBNDOCOVERAGEDIRECTORSANDOFFICERSNFPSTAGE(CoverageId),
	-- i_RetroactiveDate
	-- )
	IFF(LineType = 'DirectorsAndOfficersNFP',
		LKP_WBNDOCOVERAGEDIRECTORSANDOFFICERSNFPSTAGE_CoverageId.RetroactiveDate,
		i_RetroactiveDate
	) AS o_RetroactiveDate
	FROM SQ_DCTWorkTable
	LEFT JOIN LKP_WBNDOCOVERAGEDIRECTORSANDOFFICERSNFPSTAGE LKP_WBNDOCOVERAGEDIRECTORSANDOFFICERSNFPSTAGE_CoverageId
	ON LKP_WBNDOCOVERAGEDIRECTORSANDOFFICERSNFPSTAGE_CoverageId.CoverageId = CoverageId

),
SQ_EDW_DCT AS (
	SELECT DISTINCT PT.PremiumTransactionID AS PremiumTransactionID
	,WPT.PremiumTransactionStageId AS PremiumTransactionStageId
	,RC.ClassCode AS ClassCode
	,RC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC ON PT.RatingCoverageAKId = RC.RatingCoverageAKId and pt.EffectiveDate=rc.EffectiveDate
							   AND PT.SourceSystemID = 'DCT' AND RC.SourceSystemID = 'DCT' 
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT ON PT.PremiumTransactionAKId = WPT.PremiumTransactionAKId
	INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
							   AND PC.SourceSystemID = 'DCT' AND PC.CurrentSnapshotFlag=1
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID AND RL.CurrentSnapshotFlag=1
	WHERE PT.CreatedDate >= '@{pipeline().parameters.SELECTION_START_TS}' 
	AND PC.InsuranceLine IN ('GeneralLiability','SBOPGeneralLiability','DirectorsAndOfficersNFP', 'DirectorsAndOffsCondos', 'EmploymentPracticesLiab')
	@{pipeline().parameters.WHERE_CLAUSE}
),
JNR_GL AS (SELECT
	SQ_EDW_DCT.PremiumTransactionID, 
	SQ_EDW_DCT.PremiumTransactionStageId, 
	SQ_EDW_DCT.ClassCode, 
	SQ_EDW_DCT.SublineCode, 
	SQ_EDW_DCT.StateCode, 
	SQ_EDW_DCT.ClassCodeOrganizationCode, 
	EXP_Stage.LineType, 
	EXP_Stage.CoverageId, 
	EXP_Stage.CoverageVersion, 
	EXP_Stage.o_RetroactiveDate AS RetroactiveDate, 
	EXP_Stage.CoverageGUID
	FROM SQ_EDW_DCT
	INNER JOIN EXP_Stage
	ON EXP_Stage.CoverageId = SQ_EDW_DCT.PremiumTransactionStageId
),
AGG_DuplicateRemove AS (
	SELECT
	PremiumTransactionID,
	PremiumTransactionStageId,
	CoverageVersion,
	RetroactiveDate,
	CoverageGUID,
	LineType,
	ClassCode,
	SublineCode,
	StateCode,
	ClassCodeOrganizationCode
	FROM JNR_GL
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY NULL) = 1
),
EXP_Valuate AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CoverageVersion AS i_CoverageVersion,
	RetroactiveDate AS i_RetroactiveDate,
	CoverageGUID AS i_CoverageGUID,
	ClassCode AS i_ClassCode,
	SublineCode AS i_SublineCode,
	StateCode AS i_RatingStateCode,
	ClassCodeOrganizationCode AS i_ClassCodeOrganizationCode,
	LineType,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: RTRIM(LTRIM(i_CoverageGUID))
	RTRIM(LTRIM(i_CoverageGUID
		)
	) AS o_CoverageGUID,
	i_RetroactiveDate AS o_RetroactiveDate,
	-- *INF*: IIF(ISNULL(i_CoverageVersion) OR IS_SPACES(i_CoverageVersion) OR LENGTH(i_CoverageVersion)=0, 'N/A', i_CoverageVersion)
	IFF(i_CoverageVersion IS NULL 
		OR LENGTH(i_CoverageVersion)>0 AND TRIM(i_CoverageVersion)='' 
		OR LENGTH(i_CoverageVersion
		) = 0,
		'N/A',
		i_CoverageVersion
	) AS o_CoverageVersion,
	-- *INF*: LTRIM(RTRIM(i_ClassCode))
	LTRIM(RTRIM(i_ClassCode
		)
	) AS o_ClassCode,
	-- *INF*: LTRIM(RTRIM(i_SublineCode))
	LTRIM(RTRIM(i_SublineCode
		)
	) AS o_SublineCode,
	-- *INF*: RTRIM(LTRIM(i_RatingStateCode))
	RTRIM(LTRIM(i_RatingStateCode
		)
	) AS o_RatingStateCode,
	-- *INF*: LTRIM(RTRIM(i_ClassCodeOrganizationCode))
	LTRIM(RTRIM(i_ClassCodeOrganizationCode
		)
	) AS o_ClassCodeOrganizationCode
	FROM AGG_DuplicateRemove
),
EXP_Metadata AS (
	SELECT
	LineType AS i_LineType,
	o_PremiumTransactionID AS i_PremiumTransactionID,
	o_CoverageGUID AS i_CoverageGUID,
	o_RetroactiveDate AS i_RetroactiveDate,
	o_CoverageVersion AS i_LiabilityFormCode,
	o_ClassCode AS i_ClassCode,
	o_SublineCode AS i_SublineCode,
	o_RatingStateCode AS i_RatingStateCode,
	o_ClassCodeOrganizationCode AS i_OriginatingOrganizationCode,
	-- *INF*: DECODE(true,
	-- IN(i_LineType,'GeneralLiability','SBOPGeneralLiability') AND NOT ISNULL(:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,i_RatingStateCode)),:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,i_RatingStateCode),
	-- IN(i_LineType,'GeneralLiability','SBOPGeneralLiability') AND NOT ISNULL(:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,'99')),:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,'99'),
	-- 'N/A')
	DECODE(true,
		i_LineType IN ('GeneralLiability','SBOPGeneralLiability') 
		AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.lkp_result IS NOT NULL, LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.lkp_result,
		i_LineType IN ('GeneralLiability','SBOPGeneralLiability') 
		AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.lkp_result IS NOT NULL, LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.lkp_result,
		'N/A'
	) AS v_lkp_result,
	-- *INF*: IIF(ISNULL(i_RetroactiveDate),
	-- TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),
	-- i_RetroactiveDate
	-- )
	IFF(i_RetroactiveDate IS NULL,
		TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		i_RetroactiveDate
	) AS v_RetroactiveDate,
	-- *INF*: DECODE(TRUE,
	-- i_LineType<>'DirectorsAndOfficersNFP' and i_LiabilityFormCode='OCCURRENCE', '3',
	-- (i_LiabilityFormCode='CLAIMSMADE' or IN(i_LineType, 'DirectorsAndOfficersNFP', 'DirectorsAndOffsCondos', 'EmploymentPracticesLiab')) and ISNULL(i_RetroactiveDate), '4',
	-- (i_LiabilityFormCode='CLAIMSMADE' or IN(i_LineType, 'DirectorsAndOfficersNFP', 'DirectorsAndOffsCondos', 'EmploymentPracticesLiab')) and NOT ISNULL(i_RetroactiveDate), '1',
	-- 'N/A'
	-- )
	DECODE(TRUE,
		i_LineType <> 'DirectorsAndOfficersNFP' 
		AND i_LiabilityFormCode = 'OCCURRENCE', '3',
		( i_LiabilityFormCode = 'CLAIMSMADE' 
			OR i_LineType IN ('DirectorsAndOfficersNFP','DirectorsAndOffsCondos','EmploymentPracticesLiab') 
		) 
		AND i_RetroactiveDate IS NULL, '4',
		( i_LiabilityFormCode = 'CLAIMSMADE' 
			OR i_LineType IN ('DirectorsAndOfficersNFP','DirectorsAndOffsCondos','EmploymentPracticesLiab') 
		) 
		AND i_RetroactiveDate IS NOT NULL, '1',
		'N/A'
	) AS v_LiabilityFormCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('1800-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US')
	TO_DATE('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.US'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CoverageGUID)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_CoverageGUID
	) AS o_CoverageGUID,
	v_RetroactiveDate AS o_RetroactiveDate,
	v_LiabilityFormCode AS o_LiabilityFormCode,
	-- *INF*: IIF(v_lkp_result='N/A' OR ISNULL(v_lkp_result), 'N/A',
	--     IIF(ISNULL(SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1)) OR LENGTH(SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1))=0 ,'N/A',SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1))
	-- )
	IFF(v_lkp_result = 'N/A' 
		OR v_lkp_result IS NULL,
		'N/A',
		IFF(SUBSTR(v_lkp_result, 1, REGEXP_INSTR(v_lkp_result, '@1'
				) - 1
			) IS NULL 
			OR LENGTH(SUBSTR(v_lkp_result, 1, REGEXP_INSTR(v_lkp_result, '@1'
					) - 1
				)
			) = 0,
			'N/A',
			SUBSTR(v_lkp_result, 1, REGEXP_INSTR(v_lkp_result, '@1'
				) - 1
			)
		)
	) AS o_ClassSummary,
	-- *INF*: IIF(v_lkp_result='N/A' OR ISNULL(v_lkp_result), 'N/A',
	-- IIF(ISNULL(SUBSTR(v_lkp_result,(instr(v_lkp_result,'@1')+2),(instr(v_lkp_result,'@2')-(instr(v_lkp_result,'@1')+2)))) OR LENGTH(SUBSTR(v_lkp_result,(instr(v_lkp_result,'@1')+2),(instr(v_lkp_result,'@2')-(instr(v_lkp_result,'@1')+2)))) =0 , 'N/A'  ,SUBSTR(v_lkp_result,(instr(v_lkp_result,'@1')+2),(instr(v_lkp_result,'@2')-(instr(v_lkp_result,'@1')+2))))
	-- )
	IFF(v_lkp_result = 'N/A' 
		OR v_lkp_result IS NULL,
		'N/A',
		IFF(SUBSTR(v_lkp_result, ( REGEXP_INSTR(v_lkp_result, '@1'
					) + 2 
				), ( REGEXP_INSTR(v_lkp_result, '@2'
					) - ( REGEXP_INSTR(v_lkp_result, '@1'
						) + 2 
					) 
				)
			) IS NULL 
			OR LENGTH(SUBSTR(v_lkp_result, ( REGEXP_INSTR(v_lkp_result, '@1'
						) + 2 
					), ( REGEXP_INSTR(v_lkp_result, '@2'
						) - ( REGEXP_INSTR(v_lkp_result, '@1'
							) + 2 
						) 
					)
				)
			) = 0,
			'N/A',
			SUBSTR(v_lkp_result, ( REGEXP_INSTR(v_lkp_result, '@1'
					) + 2 
				), ( REGEXP_INSTR(v_lkp_result, '@2'
					) - ( REGEXP_INSTR(v_lkp_result, '@1'
						) + 2 
					) 
				)
			)
		)
	) AS o_ClassGroup
	FROM EXP_Valuate
	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.SublineCode = i_SublineCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.RatingStateCode = i_RatingStateCode

	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.SublineCode = i_SublineCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.RatingStateCode = '99'

),
LKP_CoverageDetailGeneralLiability AS (
	SELECT
	PremiumTransactionID,
	RetroactiveDate,
	LiabilityFormCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode
	FROM (
		SELECT 
			PremiumTransactionID,
			RetroactiveDate,
			LiabilityFormCode,
			ISOGeneralLiabilityClassSummary,
			ISOGeneralLiabilityClassGroupCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailGeneralLiability
		WHERE SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and PremiumTransactionID  in (select pt.PremiumTransactionID from
		PremiumTransaction pt
		inner join WorkPremiumTransaction wpt
		on pt.PremiumTransactionAKID=wpt.PremiumTransactionAKId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CoverageDetailGeneralLiability.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailGeneralLiability.RetroactiveDate AS lkp_RetroactiveDate,
	LKP_CoverageDetailGeneralLiability.LiabilityFormCode AS lkp_LiabilityFormCode,
	LKP_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassSummary AS lkp_ClassSummary,
	LKP_CoverageDetailGeneralLiability.ISOGeneralLiabilityClassGroupCode AS lkp_ClassGroupCode,
	EXP_Metadata.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_Metadata.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_Metadata.o_AuditID AS AuditID,
	EXP_Metadata.o_EffectiveDate AS EffectiveDate,
	EXP_Metadata.o_ExpirationDate AS ExpirationDate,
	EXP_Metadata.o_SourceSystemID AS SourceSystemID,
	EXP_Metadata.o_CreatedDate AS CreatedDate,
	EXP_Metadata.o_ModifiedDate AS ModifiedDate,
	EXP_Metadata.o_CoverageGUID AS CoverageGUID,
	EXP_Metadata.o_RetroactiveDate AS RetroactiveDate,
	EXP_Metadata.o_LiabilityFormCode AS LiabilityFormCode,
	EXP_Metadata.o_ClassSummary AS ClassSummary,
	EXP_Metadata.o_ClassGroup AS ClassGroup,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),'NEW',
	-- lkp_RetroactiveDate != RetroactiveDate
	-- -----------------------Add for EDWP-3959-------------------
	-- OR lkp_LiabilityFormCode != LiabilityFormCode
	-- ------------------------------------------------------------------------
	-- OR lkp_ClassSummary != ClassSummary
	-- OR lkp_ClassGroupCode != ClassGroup
	-- ,'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'NEW',
		lkp_RetroactiveDate != RetroactiveDate 
		OR lkp_LiabilityFormCode != LiabilityFormCode 
		OR lkp_ClassSummary != ClassSummary 
		OR lkp_ClassGroupCode != ClassGroup, 'UPDATE',
		'NOCHANGE'
	) AS o_ChangeFlag
	FROM EXP_Metadata
	LEFT JOIN LKP_CoverageDetailGeneralLiability
	ON LKP_CoverageDetailGeneralLiability.PremiumTransactionID = EXP_Metadata.o_PremiumTransactionID
),
FIL_Records AS (
	SELECT
	PremiumTransactionID, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	CoverageGUID, 
	RetroactiveDate, 
	LiabilityFormCode, 
	ClassSummary, 
	ClassGroup, 
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChanges
	WHERE ChangeFlag='NEW'
),
CoverageDetailGeneralLiability_INSERT AS (
	INSERT INTO CoverageDetailGeneralLiability
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, RetroactiveDate, LiabilityFormCode, ISOGeneralLiabilityClassSummary, ISOGeneralLiabilityClassGroupCode)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CoverageGUID AS COVERAGEGUID, 
	RETROACTIVEDATE, 
	LIABILITYFORMCODE, 
	ClassSummary AS ISOGENERALLIABILITYCLASSSUMMARY, 
	ClassGroup AS ISOGENERALLIABILITYCLASSGROUPCODE
	FROM FIL_Records
),
SQ_CoverageDetailGeneralLiability AS (
	SELECT 
	CDGLPrevious.RetroactiveDate,
	CDGLPrevious.LiabilityFormCode,
	CDGLPrevious.ISOGeneralLiabilityClassSummary,
	CDGLPrevious.ISOGeneralLiabilityClassGroupCode,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailGeneralLiability CDGLPrevious
	on ( CDGLPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailGeneralLiability CDGLToUpdate
	on ( CDGLToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
		INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  COALESCE(CDGLPrevious.ISOGeneralLiabilityClassGroupCode, '*^') <> COALESCE(CDGLToUpdate.ISOGeneralLiabilityClassGroupCode, '*^')
	  OR COALESCE(CDGLPrevious.ISOGeneralLiabilityClassSummary, '*^') <> COALESCE(CDGLToUpdate.ISOGeneralLiabilityClassSummary, '*^')
	  OR COALESCE(CDGLPrevious.LiabilityFormCode, '*^') <> COALESCE(CDGLToUpdate.LiabilityFormCode, '*^')
	  OR CDGLPrevious.RetroactiveDate <> CDGLToUpdate.RetroactiveDate
	  )
),
Exp_CoverageDetailGeneralLiability AS (
	SELECT
	RetroactiveDate,
	LiabilityFormCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailGeneralLiability
),
UPD_CoverageDetailGeneralLiability AS (
	SELECT
	RetroactiveDate, 
	LiabilityFormCode, 
	ISOGeneralLiabilityClassSummary, 
	ISOGeneralLiabilityClassGroupCode, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailGeneralLiability
),
TGT_CoverageDetailGeneralLiability_Upd_Ofsets AS (
	MERGE INTO CoverageDetailGeneralLiability AS T
	USING UPD_CoverageDetailGeneralLiability AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.RetroactiveDate = S.RetroactiveDate, T.LiabilityFormCode = S.LiabilityFormCode, T.ISOGeneralLiabilityClassSummary = S.ISOGeneralLiabilityClassSummary, T.ISOGeneralLiabilityClassGroupCode = S.ISOGeneralLiabilityClassGroupCode
),
SQ_CoverageDetailGeneralLiability_Deprecated AS (
	SELECT 
	CDGLPrevious.RetroactiveDate,
	CDGLPrevious.LiabilityFormCode,
	CDGLPrevious.ISOGeneralLiabilityClassSummary,
	CDGLPrevious.ISOGeneralLiabilityClassGroupCode,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailGeneralLiability CDGLPrevious
	on ( CDGLPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailGeneralLiability CDGLToUpdate
	on ( CDGLToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
		INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  COALESCE(CDGLPrevious.ISOGeneralLiabilityClassGroupCode, '*^') <> COALESCE(CDGLToUpdate.ISOGeneralLiabilityClassGroupCode, '*^')
	  OR COALESCE(CDGLPrevious.ISOGeneralLiabilityClassSummary, '*^') <> COALESCE(CDGLToUpdate.ISOGeneralLiabilityClassSummary, '*^')
	  OR COALESCE(CDGLPrevious.LiabilityFormCode, '*^') <> COALESCE(CDGLToUpdate.LiabilityFormCode, '*^')
	  OR CDGLPrevious.RetroactiveDate <> CDGLToUpdate.RetroactiveDate
	  )
),
Exp_CoverageDetailGeneralLiability_Deprecated AS (
	SELECT
	RetroactiveDate,
	LiabilityFormCode,
	ISOGeneralLiabilityClassSummary,
	ISOGeneralLiabilityClassGroupCode,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailGeneralLiability_Deprecated
),
UPD_CoverageDetailGeneralLiability_Deprecated AS (
	SELECT
	RetroactiveDate, 
	LiabilityFormCode, 
	ISOGeneralLiabilityClassSummary, 
	ISOGeneralLiabilityClassGroupCode, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailGeneralLiability_Deprecated
),
TGT_CoverageDetailGeneralLiability_Upd_Deprecated AS (
	MERGE INTO CoverageDetailGeneralLiability AS T
	USING UPD_CoverageDetailGeneralLiability_Deprecated AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.RetroactiveDate = S.RetroactiveDate, T.LiabilityFormCode = S.LiabilityFormCode, T.ISOGeneralLiabilityClassSummary = S.ISOGeneralLiabilityClassSummary, T.ISOGeneralLiabilityClassGroupCode = S.ISOGeneralLiabilityClassGroupCode
),