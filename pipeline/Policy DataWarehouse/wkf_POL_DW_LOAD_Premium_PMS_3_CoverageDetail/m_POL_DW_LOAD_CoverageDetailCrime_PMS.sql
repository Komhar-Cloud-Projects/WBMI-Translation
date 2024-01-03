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
	select distinct pt.PremiumTransactionID,
	pt.CurrentSnapshotFlag,
	sc.StatisticalCoverageHashKey,
	sc.ClassCode,
	pt.PremiumTransactionEffectiveDate,
	rl.StateProvinceCode as StateCode
	from dbo.PremiumTransaction pt
	inner join dbo.StatisticalCoverage sc
	on pt.StatisticalCoverageAKID=sc.StatisticalCoverageAKID
	and sc.CurrentSnapshotFlag=1
	inner join PolicyCoverage PC 
	on PC.PolicyCoverageAKID = SC.PolicyCoverageAKID 
	and pc.CurrentSnapshotFlag=1
	inner join RiskLocation RL 
	on RL.RiskLocationAKID = PC.RiskLocationAKID
	and rl.CurrentSnapshotFlag=1
	where pc.InsuranceLine='CR'
	and pt.SourceSystemId='PMS'
	and pt.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
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
		WHERE SourceSystemID ='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_CDC.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CDC.IndustryGroup AS lkp_IndustryGroup,
	SQ_PremiumTransaction.PremiumTransactionID AS i_PremiumTransactionID,
	SQ_PremiumTransaction.CurrentSnapshotFlag AS i_CurrentSnapshotFlag,
	SQ_PremiumTransaction.StatisticalCoverageHashKey AS i_CoverageGUID,
	SQ_PremiumTransaction.ClassCode AS i_ClassCode,
	SQ_PremiumTransaction.PremiumTransactionEffectiveDate AS i_PTEffDate,
	SQ_PremiumTransaction.StateCode AS i_StateCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	i_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('1800/01/01 00:00:00','YYYY/MM/DD HH24:MI:SS')
	TO_DATE('1800/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS') AS o_EffectiveDate,
	-- *INF*: TO_DATE('2100/12/31 23:59:59','YYYY/MM/DD HH24:MI:SS')
	TO_DATE('2100/12/31 23:59:59', 'YYYY/MM/DD HH24:MI:SS') AS o_ExpirationDate,
	'PMS' AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGUID AS o_CoverageGUID,
	-- *INF*: IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,i_StateCode) ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, i_StateCode) , 'N/A')
	IFF(NOT LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.IndustryGroup IS NULL, LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.IndustryGroup, 'N/A') AS v_lkp_result,
	-- *INF*: IIF( v_lkp_result ='N/A', 
	-- IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,'99') ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, '99') , 'N/A')
	--   ,v_lkp_result )
	-- --IIF( NOT ISNULL(:LKP.LKP_SupClassificationCrime(i_ClassCode,'99') ) , :LKP.LKP_SupClassificationCrime(i_ClassCode, '99') , 'N/A'), 
	IFF(v_lkp_result = 'N/A', IFF(NOT LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.IndustryGroup IS NULL, LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.IndustryGroup, 'N/A'), v_lkp_result) AS v_lkp_result_99,
	-- *INF*: Ltrim(Rtrim(v_lkp_result_99))
	Ltrim(Rtrim(v_lkp_result_99)) AS o_IndustryGroup,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID),
	-- 'INSERT',
	-- lkp_IndustryGroup<>v_lkp_result_99,
	-- 'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
	lkp_PremiumTransactionID IS NULL, 'INSERT',
	lkp_IndustryGroup <> v_lkp_result_99, 'UPDATE',
	'NOCHANGE') AS o_changeflag
	FROM SQ_PremiumTransaction
	LEFT JOIN LKP_CDC
	ON LKP_CDC.PremiumTransactionID = SQ_PremiumTransaction.PremiumTransactionID
	LEFT JOIN LKP_SUPCLASSIFICATIONCRIME LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode
	ON LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_i_StateCode.RatingStateCode = i_StateCode

	LEFT JOIN LKP_SUPCLASSIFICATIONCRIME LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99
	ON LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONCRIME_i_ClassCode_99.RatingStateCode = '99'

),
RTRTRANS AS (
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
),
RTRTRANS_INSERT AS (SELECT * FROM RTRTRANS WHERE o_changeflag='INSERT'),
RTRTRANS_UPDATE AS (SELECT * FROM RTRTRANS WHERE o_changeflag='UPDATE'),
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
	FROM RTRTRANS_INSERT
),
UPD_CDC AS (
	SELECT
	o_PremiumTransactionID AS o_PremiumTransactionID3, 
	o_ModifiedDate AS o_ModifiedDate3, 
	o_CoverageGUID AS o_CoverageGUID3, 
	o_IndustryGroup AS o_IndustryGroup3
	FROM RTRTRANS_UPDATE
),
CoverageDetailCrime_UPDATE AS (
	MERGE INTO CoverageDetailCrime AS T
	USING UPD_CDC AS S
	ON T.PremiumTransactionID = S.o_PremiumTransactionID3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.o_ModifiedDate3, T.CoverageGuid = S.o_CoverageGUID3, T.IndustryGroup = S.o_IndustryGroup3
),