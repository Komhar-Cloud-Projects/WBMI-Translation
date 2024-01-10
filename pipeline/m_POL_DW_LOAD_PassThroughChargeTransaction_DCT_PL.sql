WITH
LKP_IncludePassThroughOnly AS (
	SELECT
	RatedCoverageCode
	FROM (
		select cc.RatedCoverageCode as RatedCoverageCode 
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageSummary CS
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageGroup CG
		on CS.CoverageSummaryId=CG.CoverageSummaryId
		inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.ConformedCoverage CC
		on CG.CoverageGroupId=CC.CoverageGroupId
		where CS.CoverageSummaryCode='PASSTHRU'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatedCoverageCode ORDER BY RatedCoverageCode) = 1
),
SQ_WorkDCTPLCoverage AS (
	select  
	A.PolicySymbol,
	A.PolicyNumber,
	A.PolicyVersion,
	A.TransactionCreatedDate,
	A.TransactionTypeCode,
	A.PolicyEffectiveDate,
	A.MeasureName,
	A.MeasureDetailCode,
	SUM(A.TransactionAmount) AS TransactionAmount,
	SUM(A.FullTermPremium) AS FullTermPremium,
	A.TransactionEffectiveDate,
	A.TransactionIssueDate,
	A.TransactionReasonCode,
	A.CoverageCodeKey,
	A.CoverageSubCd,
	A.ExtractDate
	,MAX(A.PolicyExpirationDate) as TransactionExpirationDate
	from 
	
	(
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	P.TransactionCreatedDate,
	P.TransactionTypeCode,
	P.PolicyEffectiveDate,
	C.MeasureName,
	C.MeasureDetailCode,
	C.TransactionAmount as TransactionAmount,
	ISNULL(C.FullTermPremium,C.TransactionAmount) as FullTermPremium,
	C.CoverageKey,
	C.TransactionEffectiveDate,
	C.TransactionIssueDate,
	ISNULL(C.TransactionReasonCode,'N/A') TransactionReasonCode,
	C.CoverageCodeKey,
	ISNULL(C.CoverageSubCd,'') CoverageSubCd
	,P.PolicyExpirationDate
	,P.ExtractDate
	from DBO.WorkDCTPLPolicy P
	inner join DBO.WorkDCTPLCoverage C
	on P.PolicyKey=C.PolicyKey
	and P.StartDate=C.StartDate
	where C.MeasureName in ('Tax')
	and not exists(select 1 from DBO.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE_CLAUSE}
	--
	) A
	GROUP BY
	A.PolicySymbol,
	A.PolicyNumber,
	A.PolicyVersion,
	A.TransactionCreatedDate,
	A.TransactionTypeCode,
	A.PolicyEffectiveDate,
	A.MeasureName,
	A.MeasureDetailCode,
	A.TransactionEffectiveDate,
	A.TransactionIssueDate,
	A.TransactionReasonCode,
	A.CoverageCodeKey,
	A.CoverageSubCd,
	A.ExtractDate
	having SUM(A.TransactionAmount)<>0
	order by A.PolicySymbol,A.PolicyNumber,A.PolicyVersion,A.TransactionCreatedDate
),
EXP_EvaluatePassThrough AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	TransactionCreatedDate,
	TransactionTypeCode,
	PolicyEffectiveDate,
	MeasureName,
	MeasureDetailCode,
	TransactionAmount,
	FullTermPremium,
	TransactionEffectiveDate,
	TransactionIssueDate,
	TransactionReasonCode,
	CoverageCodeKey,
	CoverageSubCd,
	TransactionExpirationDate,
	-- *INF*: :LKP.LKP_INCLUDEPASSTHROUGHONLY(CoverageSubCd)
	LKP_INCLUDEPASSTHROUGHONLY_CoverageSubCd.RatedCoverageCode AS v_lkp_RatedCoverageCode,
	-- *INF*: IIF(ISNULL(v_lkp_RatedCoverageCode),'0','1')
	IFF(v_lkp_RatedCoverageCode IS NULL, '0', '1') AS o_OnluPassThroughFlag,
	ExtractDate
	FROM SQ_WorkDCTPLCoverage
	LEFT JOIN LKP_INCLUDEPASSTHROUGHONLY LKP_INCLUDEPASSTHROUGHONLY_CoverageSubCd
	ON LKP_INCLUDEPASSTHROUGHONLY_CoverageSubCd.RatedCoverageCode = CoverageSubCd

),
FIL_LetOnlyPassThrough AS (
	SELECT
	PolicySymbol, 
	PolicyNumber, 
	PolicyVersion, 
	TransactionCreatedDate, 
	TransactionTypeCode, 
	PolicyEffectiveDate, 
	MeasureName, 
	MeasureDetailCode, 
	TransactionAmount, 
	FullTermPremium, 
	TransactionEffectiveDate, 
	TransactionIssueDate, 
	TransactionReasonCode, 
	CoverageCodeKey, 
	CoverageSubCd, 
	TransactionExpirationDate, 
	o_OnluPassThroughFlag, 
	ExtractDate
	FROM EXP_EvaluatePassThrough
	WHERE o_OnluPassThroughFlag='1'
),
EXP_SourceData AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	TransactionCreatedDate,
	TransactionTypeCode,
	-- *INF*: Decode(TRUE,
	-- TransactionTypeCode='New','10',
	-- TransactionTypeCode='Renew','11',
	-- TransactionTypeCode='Endorse','12',
	-- TransactionTypeCode='Reinstate','15',
	-- TransactionTypeCode='Cancel','20',
	-- TransactionTypeCode='Reissue','30',
	-- TransactionTypeCode='Rewrite','31',
	-- '-1'
	-- )
	Decode(TRUE,
		TransactionTypeCode = 'New', '10',
		TransactionTypeCode = 'Renew', '11',
		TransactionTypeCode = 'Endorse', '12',
		TransactionTypeCode = 'Reinstate', '15',
		TransactionTypeCode = 'Cancel', '20',
		TransactionTypeCode = 'Reissue', '30',
		TransactionTypeCode = 'Rewrite', '31',
		'-1') AS o_standardTransactiontypeCode,
	PolicyEffectiveDate,
	MeasureName,
	MeasureDetailCode,
	TransactionAmount,
	FullTermPremium,
	TransactionEffectiveDate,
	TransactionIssueDate,
	TransactionReasonCode,
	CoverageCodeKey,
	CoverageSubCd,
	TransactionExpirationDate,
	-- *INF*: IIF(ISNULL(ltrim(rtrim(PolicyVersion))) or Length(ltrim(rtrim(PolicyVersion)))=0 or IS_SPACES(PolicyVersion),'00',PolicyVersion)
	IFF(ltrim(rtrim(PolicyVersion)) IS NULL OR Length(ltrim(rtrim(PolicyVersion))) = 0 OR IS_SPACES(PolicyVersion), '00', PolicyVersion) AS v_PolicyVersion,
	PolicyNumber || v_PolicyVersion AS v_PolicyKey,
	v_PolicyKey AS o_PolicyKey,
	-- *INF*: MD5(v_PolicyKey||TO_CHAR(TransactionCreatedDate)||CoverageSubCd)
	MD5(v_PolicyKey || TO_CHAR(TransactionCreatedDate) || CoverageSubCd) AS o_PassThroughChargeTransactionHashKey,
	-- *INF*: DECODE(TRUE,
	-- TO_CHAR(ExtractDate, 'DD' ) ='02' and TO_CHAR( ExtractDate, 'DAY' )='Tuesday',1,
	-- TO_CHAR(ExtractDate, 'DD' ) ='01',1,
	-- 0
	-- )
	DECODE(TRUE,
		TO_CHAR(ExtractDate, 'DD') = '02' AND TO_CHAR(ExtractDate, 'DAY') = 'Tuesday', 1,
		TO_CHAR(ExtractDate, 'DD') = '01', 1,
		0) AS v_AdjustForMonthEnd,
	-- *INF*: DECODE(TRUE,
	-- v_AdjustForMonthEnd = 1, ADD_TO_DATE(ExtractDate,'MM',-1),
	-- ExtractDate)
	DECODE(TRUE,
		v_AdjustForMonthEnd = 1, ADD_TO_DATE(ExtractDate, 'MM', - 1),
		ExtractDate) AS v_ExtractDate,
	-- *INF*: ADD_TO_DATE(ADD_TO_DATE(TRUNC(GREATEST(GREATEST(TransactionCreatedDate,TransactionEffectiveDate),v_ExtractDate), 'MM'),'MM',1),'SS',-1)
	ADD_TO_DATE(ADD_TO_DATE(TRUNC(GREATEST(GREATEST(TransactionCreatedDate, TransactionEffectiveDate), v_ExtractDate), 'MM'), 'MM', 1), 'SS', - 1) AS o_PremiumTransactionBookedDate,
	ExtractDate
	FROM FIL_LetOnlyPassThrough
),
LKP_GetPolicyAkId AS (
	SELECT
	PolicyKey,
	PolicyAkID
	FROM (
		select Distinct P.pol_key as PolicyKey
		,P.pol_ak_id as PolicyAkID
		from V2.policy P with (nolock)
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy W with (nolock) on W.PolicyNumber=P.pol_num and P.pol_mod=RIGHT('00'+W.PolicyVersion,2)
		where P.crrnt_snpsht_flag=1
		and P.source_sys_id='DCT'
		order by P.pol_key--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY PolicyKey) = 1
),
LKP_PassThroughChargeTransaction AS (
	SELECT
	PassThroughChargeTransactionHashKey,
	PassThroughChargeTransactionAKID,
	in_PassThroughChargeTransactionHashKey
	FROM (
		select PT.PassThroughChargeTransactionHashKey as PassThroughChargeTransactionHashKey
		,PT.PassThroughChargeTransactionAKID as PassThroughChargeTransactionAKID
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction PT with (nolock)
		where PT.SourceSystemID='DCT'
		and PT.CurrentSnapshotFlag=1
		order by PT.PassThroughChargeTransactionHashKey--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionHashKey ORDER BY PassThroughChargeTransactionHashKey) = 1
),
LKP_SupPassThroughChargeType AS (
	SELECT
	SupPassThroughChargeTypeID,
	DCTTaxCode,
	in_CoverageSubCd
	FROM (
		select 
		S.SupPassThroughChargeTypeID as SupPassThroughChargeTypeID 
		, S.DCTTaxCode as DCTTaxCode
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.SupPassThroughChargeType S
		where S.CurrentSnapshotFlag=1
		and S.SourceSystemID='DCT'
		order by S.DCTTaxCode--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DCTTaxCode ORDER BY SupPassThroughChargeTypeID) = 1
),
LKP_sup_passthrough_transaction_code AS (
	SELECT
	sup_prem_trans_code_id,
	prem_trans_code,
	StandardPremiumTransactionCode
	FROM (
		SELECT 
			sup_prem_trans_code_id,
			prem_trans_code,
			StandardPremiumTransactionCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_premium_transaction_code
		WHERE crrnt_snpsht_flag='1' AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prem_trans_code,StandardPremiumTransactionCode ORDER BY sup_prem_trans_code_id) = 1
),
SEQ_PassThroughChargeTransactionAKID AS (
	CREATE SEQUENCE SEQ_PassThroughChargeTransactionAKID
	START = 0
	INCREMENT = 1;
),
EXP_PassThroughChargeTransaction AS (
	SELECT
	SEQ_PassThroughChargeTransactionAKID.NEXTVAL AS in_NEXTVAL,
	EXP_SourceData.PolicySymbol,
	EXP_SourceData.PolicyNumber,
	EXP_SourceData.PolicyVersion,
	EXP_SourceData.TransactionCreatedDate,
	EXP_SourceData.TransactionTypeCode,
	EXP_SourceData.PolicyEffectiveDate,
	EXP_SourceData.MeasureName,
	EXP_SourceData.MeasureDetailCode,
	EXP_SourceData.TransactionAmount,
	EXP_SourceData.FullTermPremium AS FullTermPremium1,
	EXP_SourceData.TransactionEffectiveDate,
	EXP_SourceData.TransactionIssueDate,
	EXP_SourceData.TransactionReasonCode,
	EXP_SourceData.CoverageCodeKey,
	EXP_SourceData.CoverageSubCd,
	EXP_SourceData.TransactionExpirationDate,
	EXP_SourceData.o_PolicyKey AS PolicyKey,
	EXP_SourceData.o_PassThroughChargeTransactionHashKey AS in_PassThroughChargeTransactionHashKey,
	EXP_SourceData.o_PremiumTransactionBookedDate AS in_PremiumTransactionBookedDate,
	LKP_GetPolicyAkId.PolicyAkID AS lkp_PolicyAkID,
	LKP_PassThroughChargeTransaction.PassThroughChargeTransactionAKID AS lkp_PassThroughChargeTransactionAKID,
	LKP_SupPassThroughChargeType.SupPassThroughChargeTypeID AS lkp_SupPassThroughChargeTypeID,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	TransactionCreatedDate AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	0 AS LogicalIndicator,
	'1' AS LogicalDeleteFlag,
	1 AS DuplicateSequence,
	in_PassThroughChargeTransactionHashKey AS PassThroughChargeTransactionHashKey,
	-- *INF*: IIF(ISNULL(lkp_PassThroughChargeTransactionAKID),in_NEXTVAL,lkp_PassThroughChargeTransactionAKID )
	IFF(lkp_PassThroughChargeTransactionAKID IS NULL, in_NEXTVAL, lkp_PassThroughChargeTransactionAKID) AS PassThroughChargeTransactionAKID,
	-1 AS StatisticalCoverageAKID,
	-- *INF*: IIF(ISNULL(TransactionTypeCode),'N/A',TransactionTypeCode)
	IFF(TransactionTypeCode IS NULL, 'N/A', TransactionTypeCode) AS PassThroughChargeTransactionCode,
	TransactionCreatedDate AS PassThroughChargeTransactionEnteredDate,
	TransactionEffectiveDate AS PassThroughChargeTransactionEffectiveDate,
	TransactionExpirationDate AS PassThroughChargeTransactionExpirationDate,
	in_PremiumTransactionBookedDate AS PassThroughChargeTransactionBookedDate,
	TransactionAmount AS PassThroughChargeTransactionAmount,
	0 AS FullTermPremium,
	FullTermPremium1 AS FullTaxAmount,
	0 AS TaxPercentageRate,
	TransactionReasonCode AS ReasonAmendedCode,
	LKP_sup_passthrough_transaction_code.sup_prem_trans_code_id AS PassThroughChargeTransactionCodeId,
	-- *INF*: IIF(ISNULL(PassThroughChargeTransactionCodeId),-1,PassThroughChargeTransactionCodeId)
	IFF(PassThroughChargeTransactionCodeId IS NULL, - 1, PassThroughChargeTransactionCodeId) AS o_PassThroughTransactionCodeId,
	-1 AS RiskLocationAKID,
	-- *INF*: IIF(ISNULL(lkp_PolicyAkID),-1,lkp_PolicyAkID)
	IFF(lkp_PolicyAkID IS NULL, - 1, lkp_PolicyAkID) AS PolicyAKID,
	-1 AS SupLGTLineOfInsuranceID,
	-1 AS PolicyCoverageAKID,
	-1 AS RatingCoverageAKID,
	-1 AS SupSurchargeExemptID,
	-- *INF*: IIF(ISNULL(lkp_SupPassThroughChargeTypeID),-1,lkp_SupPassThroughChargeTypeID)
	IFF(lkp_SupPassThroughChargeTypeID IS NULL, - 1, lkp_SupPassThroughChargeTypeID) AS SupPassThroughChargeTypeID,
	0 AS TotalAnnualPremiumSubjectToTax,
	-- *INF*: IIF(ISNULL(CoverageSubCd),'N/A',CoverageSubCd)
	IFF(CoverageSubCd IS NULL, 'N/A', CoverageSubCd) AS DCTTaxCode,
	'Onset' AS OffsetOnsetCode,
	1 AS LoadSequence,
	'N/A' AS NegateRestateCode,
	-- *INF*: IIF(ISNULL(lkp_PassThroughChargeTransactionAKID),1,0)
	-- -- 1 Do Not Filer
	-- -- 0 Filter
	IFF(lkp_PassThroughChargeTransactionAKID IS NULL, 1, 0) AS FilterFlag
	FROM EXP_SourceData
	LEFT JOIN LKP_GetPolicyAkId
	ON LKP_GetPolicyAkId.PolicyKey = EXP_SourceData.o_PolicyKey
	LEFT JOIN LKP_PassThroughChargeTransaction
	ON LKP_PassThroughChargeTransaction.PassThroughChargeTransactionHashKey = EXP_SourceData.o_PassThroughChargeTransactionHashKey
	LEFT JOIN LKP_SupPassThroughChargeType
	ON LKP_SupPassThroughChargeType.DCTTaxCode = EXP_SourceData.CoverageSubCd
	LEFT JOIN LKP_sup_passthrough_transaction_code
	ON LKP_sup_passthrough_transaction_code.prem_trans_code = EXP_SourceData.TransactionTypeCode AND LKP_sup_passthrough_transaction_code.StandardPremiumTransactionCode = EXP_SourceData.o_standardTransactiontypeCode
),
FIL_Insert_PassThroughCharge AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	LogicalIndicator, 
	LogicalDeleteFlag, 
	DuplicateSequence, 
	PassThroughChargeTransactionHashKey, 
	PassThroughChargeTransactionAKID, 
	StatisticalCoverageAKID, 
	PassThroughChargeTransactionCode, 
	PassThroughChargeTransactionEnteredDate, 
	PassThroughChargeTransactionEffectiveDate, 
	PassThroughChargeTransactionExpirationDate, 
	PassThroughChargeTransactionBookedDate, 
	PassThroughChargeTransactionAmount, 
	FullTermPremium, 
	FullTaxAmount, 
	TaxPercentageRate, 
	ReasonAmendedCode, 
	o_PassThroughTransactionCodeId AS PassThroughChargeTransactionCodeId, 
	RiskLocationAKID, 
	PolicyAKID, 
	SupLGTLineOfInsuranceID, 
	PolicyCoverageAKID, 
	SupSurchargeExemptID, 
	SupPassThroughChargeTypeID, 
	TotalAnnualPremiumSubjectToTax, 
	DCTTaxCode, 
	OffsetOnsetCode, 
	LoadSequence, 
	NegateRestateCode, 
	FilterFlag, 
	RatingCoverageAKID
	FROM EXP_PassThroughChargeTransaction
	WHERE IIF(FilterFlag='1',TRUE,FALSE)
),
PassThroughChargeTransaction AS (
	INSERT INTO PassThroughChargeTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, DuplicateSequence, PassThroughChargeTransactionHashKey, PassThroughChargeTransactionAKID, StatisticalCoverageAKID, PassThroughChargeTransactionCode, PassThroughChargeTransactionEnteredDate, PassThroughChargeTransactionEffectiveDate, PassThroughChargeTransactionExpirationDate, PassThroughChargeTransactionBookedDate, PassThroughChargeTransactionAmount, FullTermPremium, FullTaxAmount, TaxPercentageRate, ReasonAmendedCode, PassThroughChargeTransactionCodeId, RiskLocationAKID, PolicyAKID, SupLGTLineOfInsuranceID, PolicyCoverageAKID, SupSurchargeExemptID, SupPassThroughChargeTypeID, TotalAnnualPremiumSubjectToTax, DCTTaxCode, OffsetOnsetCode, LoadSequence, NegateRestateCode, RatingCoverageAKID)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	DUPLICATESEQUENCE, 
	PASSTHROUGHCHARGETRANSACTIONHASHKEY, 
	PASSTHROUGHCHARGETRANSACTIONAKID, 
	STATISTICALCOVERAGEAKID, 
	PASSTHROUGHCHARGETRANSACTIONCODE, 
	PASSTHROUGHCHARGETRANSACTIONENTEREDDATE, 
	PASSTHROUGHCHARGETRANSACTIONEFFECTIVEDATE, 
	PASSTHROUGHCHARGETRANSACTIONEXPIRATIONDATE, 
	PASSTHROUGHCHARGETRANSACTIONBOOKEDDATE, 
	PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	FULLTAXAMOUNT, 
	TAXPERCENTAGERATE, 
	REASONAMENDEDCODE, 
	PASSTHROUGHCHARGETRANSACTIONCODEID, 
	RISKLOCATIONAKID, 
	POLICYAKID, 
	SUPLGTLINEOFINSURANCEID, 
	POLICYCOVERAGEAKID, 
	SUPSURCHARGEEXEMPTID, 
	SUPPASSTHROUGHCHARGETYPEID, 
	TOTALANNUALPREMIUMSUBJECTTOTAX, 
	DCTTAXCODE, 
	OFFSETONSETCODE, 
	LOADSEQUENCE, 
	NEGATERESTATECODE, 
	RATINGCOVERAGEAKID
	FROM FIL_Insert_PassThroughCharge
),