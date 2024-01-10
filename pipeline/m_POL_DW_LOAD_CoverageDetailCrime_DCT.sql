WITH
LKP_SupClassificationCrime AS (
	SELECT
	IndustryGroup,
	ClassCode,
	RatingStateCode
	FROM (
		SELECT 
			IndustryGroup,
			ClassCode,
			RatingStateCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationCrime
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,RatingStateCode ORDER BY IndustryGroup) = 1
),
SQ_PremiumTransaction AS (
	SELECT DISTINCT PT.Premiumtransactionid, 
	                PT.Currentsnapshotflag, 
	                RC.Coverageguid, 
	                RC.Classcode, 
	                PT.Premiumtransactioneffectivedate, 
	                RL.Stateprovincecode AS StateCode 
	FROM   dbo.Premiumtransaction PT INNER JOIN Ratingcoverage RC ON PT.Ratingcoverageakid = RC.Ratingcoverageakid 
	                  AND PT.Effectivedate = RC.Effectivedate 
	       INNER JOIN dbo.Policycoverage PC ON RC.Policycoverageakid = PC.Policycoverageakid AND PC.Currentsnapshotflag = 1 
	       INNER JOIN dbo.Risklocation RL ON RL.Risklocationakid = PC.Risklocationakid AND RL.Currentsnapshotflag = 1 
	       INNER JOIN dbo.Workpremiumtransaction WPT ON WPT.Premiumtransactionakid = PT.Premiumtransactionakid 
	WHERE  PT.Sourcesystemid = 'DCT' AND WPT.Sourcesystemid = 'DCT'
	       AND PC.Insuranceline = 'Crime' 
	       AND PT.Createddate >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_Default AS (
	SELECT
	PremiumTransactionID,
	CurrentSnapshotFlag,
	CoverageGUID,
	ClassCode,
	PremiumTransactionEffectiveDate,
	StateCode
	FROM SQ_PremiumTransaction
),
LKP_CDC AS (
	SELECT
	PremiumTransactionID,
	IndustryGroup,
	i_PremiumTransactionID
	FROM (
		SELECT 
			PremiumTransactionID,
			IndustryGroup,
			i_PremiumTransactionID
		FROM CoverageDetailCrime
		WHERE SourceSystemID='DCT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CDC.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CDC.IndustryGroup AS lkp_IndustryGroup,
	EXP_Default.PremiumTransactionID AS i_PremiumTransactionID,
	EXP_Default.CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	EXP_Default.CoverageGUID AS i_CoverageGUID,
	EXP_Default.ClassCode AS i_ClassCode,
	EXP_Default.PremiumTransactionEffectiveDate AS i_PTEffDate,
	EXP_Default.StateCode AS i_StateCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	i_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800/01/01 00:00:00','YYYY/MM/DD HH24:MI:SS')
	TO_DATE('1800/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100/12/31 23:59:59','YYYY/MM/DD HH24:MI:SS')
	TO_DATE('2100/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS'
	) AS o_ExpirationDate,
	'DCT' AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGUID AS o_CoverageGUID,
	-- *INF*: IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,i_StateCode) ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, i_StateCode) , 'N/A')
	IFF(LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.IndustryGroup IS NOT NULL,
		LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.IndustryGroup,
		'N/A'
	) AS v_lkp_result,
	-- *INF*: IIF( v_lkp_result ='N/A', 
	-- IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,'99') ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, '99') , 'N/A')
	--   ,v_lkp_result )
	-- --IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,'99') ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, '99') , 'N/A'), 
	IFF(v_lkp_result = 'N/A',
		IFF(LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.IndustryGroup IS NOT NULL,
			LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.IndustryGroup,
			'N/A'
		),
		v_lkp_result
	) AS v_lkp_result_99,
	-- *INF*: LTRIM(RTRIM(v_lkp_result_99))
	LTRIM(RTRIM(v_lkp_result_99
		)
	) AS o_IndustryGroup,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID), 'INSERT',
	-- lkp_IndustryGroup<>v_lkp_result_99, 'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'INSERT',
		lkp_IndustryGroup <> v_lkp_result_99, 'UPDATE',
		'NOCHANGE'
	) AS o_changeflag
	FROM EXP_Default
	LEFT JOIN LKP_CDC
	ON LKP_CDC.PremiumTransactionID = EXP_Default.PremiumTransactionID
	LEFT JOIN LKP_SUPCLASSIFICATIONCRIME LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode
	ON LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.RatingStateCode = i_StateCode

	LEFT JOIN LKP_SUPCLASSIFICATIONCRIME LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99
	ON LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.RatingStateCode = '99'

),
FIL_Records AS (
	SELECT
	o_PremiumTransactionID, 
	o_CurrentSnapshotFlag, 
	o_AuditID, 
	o_EffectiveDate, 
	o_ExpirationDate, 
	o_SourceSystemID, 
	o_CreatedDate, 
	o_ModifiedDate, 
	o_CoverageGUID, 
	o_IndustryGroup, 
	o_changeflag
	FROM EXP_DetectChanges
	WHERE o_changeflag='INSERT'
),
CoverageDetailCrime_INSERT AS (
	INSERT INTO CoverageDetailCrime
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, IndustryGroup)
	SELECT 
	o_PremiumTransactionID AS PREMIUMTRANSACTIONID, 
	o_CurrentSnapshotFlag AS CURRENTSNAPSHOTFLAG, 
	o_AuditID AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_CoverageGUID AS COVERAGEGUID, 
	o_IndustryGroup AS INDUSTRYGROUP
	FROM FIL_Records
),
SQ_CoverageDetailCrime AS (
	SELECT 
	CDCRPrevious.IndustryGroup,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailCrime CDCRPrevious
	on ( CDCRPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailCrime CDCRToUpdate
	on ( CDCRToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
	WPTOL.premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (CDCRPrevious.IndustryGroup <> CDCRToUpdate.IndustryGroup)
),
Exp_CoverageDetailCrime AS (
	SELECT
	IndustryGroup,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailCrime
),
UPD_CoverageDetailCrime AS (
	SELECT
	IndustryGroup, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailCrime
),
TGT_CoverageDetailCrime_Upd_Offsets AS (
	MERGE INTO CoverageDetailCrime AS T
	USING UPD_CoverageDetailCrime AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.IndustryGroup = S.IndustryGroup
),
SQ_CoverageDetailCrime_Deprecated AS (
	SELECT 
	CDCRPrevious.IndustryGroup,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailCrime CDCRPrevious
	on ( CDCRPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailCrime CDCRToUpdate
	on ( CDCRToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionid)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
	WPTOL.premiumtransactionID=pt.premiumtransactionID and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (CDCRPrevious.IndustryGroup <> CDCRToUpdate.IndustryGroup)
),
Exp_CoverageDetailCrime_Deprecated AS (
	SELECT
	IndustryGroup,
	Wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailCrime_Deprecated
),
UPD_CoverageDetailCrime_Deprecated AS (
	SELECT
	IndustryGroup, 
	Wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoverageDetailCrime_Deprecated
),
TGT_CoverageDetailCrime_Upd_Deprecated AS (
	MERGE INTO CoverageDetailCrime AS T
	USING UPD_CoverageDetailCrime_Deprecated AS S
	ON T.PremiumTransactionID = S.Wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.IndustryGroup = S.IndustryGroup
),