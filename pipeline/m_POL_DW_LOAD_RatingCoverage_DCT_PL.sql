WITH
LKP_RiskLocation AS (
	SELECT
	RiskLocationAKID,
	RiskLocationKey
	FROM (
		SELECT 
			RiskLocationAKID,
			RiskLocationKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
		WHERE CurrentSnapshotFlag='1' and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and
		PolicyAKId in (
		select pol_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
		where exists (
		select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT
		where WCT.PolicyNumber=pol.pol_num
		and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod)
		and pol.crrnt_snpsht_flag=1)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey ORDER BY RiskLocationAKID) = 1
),
LKP_Pol_AK_Id AS (
	SELECT
	pol_ak_id,
	Pol_Key
	FROM (
		SELECT 
			pol_ak_id,
			Pol_Key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag='1' and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		and exists ( select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT where WCT.PolicyNumber=pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol_mod)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_Key ORDER BY pol_ak_id) = 1
),
LKP_PolicyCoverageAKID AS (
	SELECT
	PolicyCoverageAKID,
	PolicyCoverageHashKey
	FROM (
		SELECT a.PolicyCoverageAKID as PolicyCoverageAKID, a.PolicyCoverageHashKey as PolicyCoverageHashKey FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage a
		inner hash join
		@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy b
		on b.pol_ak_id=a.PolicyAKId
		and b.crrnt_snpsht_flag=1
		inner hash join
		(select distinct WCT.PolicyNumber,ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00') as PolicyVersionFormatted from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy WCT) WCT
		on WCT.PolicyNumber=b.pol_num
		and PolicyVersionFormatted=b.pol_mod
		where a.CurrentSnapshotFlag=1 and a.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		order by a.PolicyCoverageHashKey
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID) = 1
),
LKP_Product AS (
	SELECT
	ProductAKId,
	ProductCode
	FROM (
		SELECT 
			ProductAKId,
			ProductCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.Product
		WHERE CurrentSnapshotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductCode ORDER BY ProductAKId) = 1
),
LKP_InsuranceReferenceLineOfBusiness AS (
	SELECT
	InsuranceReferenceLineOfBusinessAKId,
	InsuranceReferenceLineOfBusinessCode
	FROM (
		SELECT 
			InsuranceReferenceLineOfBusinessAKId,
			InsuranceReferenceLineOfBusinessCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness
		WHERE CurrentSnapshotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceReferenceLineOfBusinessCode ORDER BY InsuranceReferenceLineOfBusinessAKId) = 1
),
LKP_ExcludePassThrough AS (
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
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	P.TransactionCreatedDate,
	substring(REPLACE(Addresskey,P.Policykey+'||',''),1,charindex('|',REPLACE(Addresskey,P.Policykey+'||','') ,1)-1) Locationid,
	'0000' LocationNumber,
	C.LineOfInsuranceCode,
	P.PolicyEffectiveDate,
	C.CoverageEffectiveDate,
	C.CoverageExpirationDate,
	C.TransactionAmount,
	ISNULL(C.ExposureAmount,0) ExposureAmount,
	ISNULL(C.ExposureClassCode,'N/A') ExposureClassCode,
	ISNULL(C.SublineCode,'N/A') SublineCode,
	ISNULL(C.PerilType,'N/A') PerilType,
	C.PerilCode,
	C.AnnualStatementLineNumber,
	C.AnnualStatementLineCode,
	C.SubAnnualStatementLineCode,
	C.SubNonAnnualStatementLineCode,
	C.CoverageKey,
	C.CoverageCodeKey,
	C.CoverageCodeDesc,
	ISNULL(C.CoverageSubCd,'') CoverageSubCd,
	C.CoverageVersion,
	C.TransactionEffectiveDate,
	C.ProductCode,
	C.ProductDesc,
	ISNULL(C.TerminationDate,'2100-12-31 23:59:59') TerminationDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLLocation L
	on P.PolicyKey=L.PolicyKey
	and P.StartDate=L.StartDate
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
	on P.PolicyKey=C.PolicyKey
	and P.StartDate=C.StartDate
	and L.AddressKey=C.RiskAddressKey
	and C.MeasureName='WrittenPremium'
	where not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION
	--Coverage that are not linked to any location
	select distinct P.PolicySymbol,
	P.PolicyNumber,
	P.PolicyVersion,
	P.TransactionCreatedDate,
	'' Locationid,
	'0000' LocationNumber,
	C.LineOfInsuranceCode,
	P.PolicyEffectiveDate,
	C.CoverageEffectiveDate,
	C.CoverageExpirationDate,
	C.TransactionAmount,
	ISNULL(C.ExposureAmount,0) ExposureAmount,
	ISNULL(C.ExposureClassCode,'N/A') ExposureClassCode,
	ISNULL(C.SublineCode,'N/A') SublineCode,
	ISNULL(C.PerilType,'N/A') PerilType,
	C.PerilCode,
	C.AnnualStatementLineNumber,
	C.AnnualStatementLineCode,
	C.SubAnnualStatementLineCode,
	C.SubNonAnnualStatementLineCode,
	C.CoverageKey,
	C.CoverageCodeKey,
	C.CoverageCodeDesc,
	ISNULL(C.CoverageSubCd,'') CoverageSubCd,
	C.CoverageVersion,
	C.TransactionEffectiveDate,
	C.ProductCode,
	C.ProductDesc,
	ISNULL(C.TerminationDate,'2100-12-31 23:59:59') TerminationDate
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
	on P.PolicyKey=C.PolicyKey
	and P.StartDate=C.StartDate
	and C.MeasureName='WrittenPremium'
	where C.RiskAddressKey is null
	and not exists(select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLPolicy P2 where P2.LineageId=P.LineageId and P2.PolicyStatusKey='ClaimFreeAward')
	@{pipeline().parameters.WHERE_CLAUSE}
	
	order by P.PolicySymbol,P.PolicyNumber,P.PolicyVersion,Locationid,CoverageKey,P.TransactionCreatedDate
),
EXP_SRCDataCollect AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	TransactionCreatedDate,
	LocationId,
	LocationNumber,
	LineOfInsuranceCode,
	PolicyEffectiveDate,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	TransactionAmount,
	ExposureAmount,
	ExposureClassCode,
	SublineCode,
	PerilType,
	PerilCode,
	AnnualStatementLineNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	CoverageKey,
	CoverageCodeKey,
	CoverageCodeDesc,
	CoverageSubCd,
	CoverageVersion,
	TransactionEffectiveDate,
	ProductCode,
	ProductDesc,
	TerminationDate,
	-- *INF*: :LKP.LKP_EXCLUDEPASSTHROUGH(CoverageSubCd)
	LKP_EXCLUDEPASSTHROUGH_CoverageSubCd.RatedCoverageCode AS v_LKP_PassThroughExclusion,
	-- *INF*: IIF(ISNULL(v_LKP_PassThroughExclusion),'1','0')
	IFF(v_LKP_PassThroughExclusion IS NULL, '1', '0') AS o_PassThroughExlcusionFlag
	FROM SQ_WorkDCTPLCoverage
	LEFT JOIN LKP_EXCLUDEPASSTHROUGH LKP_EXCLUDEPASSTHROUGH_CoverageSubCd
	ON LKP_EXCLUDEPASSTHROUGH_CoverageSubCd.RatedCoverageCode = CoverageSubCd

),
FIL_ExcludePassThroughCoverage AS (
	SELECT
	PolicySymbol, 
	PolicyNumber, 
	PolicyVersion, 
	TransactionCreatedDate, 
	LocationId, 
	LocationNumber, 
	LineOfInsuranceCode, 
	PolicyEffectiveDate, 
	CoverageEffectiveDate, 
	CoverageExpirationDate, 
	TransactionAmount, 
	ExposureAmount, 
	ExposureClassCode, 
	SublineCode, 
	PerilType, 
	PerilCode, 
	AnnualStatementLineNumber, 
	AnnualStatementLineCode, 
	SubAnnualStatementLineCode, 
	SubNonAnnualStatementLineCode, 
	CoverageKey, 
	CoverageCodeKey, 
	CoverageCodeDesc, 
	CoverageSubCd, 
	CoverageVersion, 
	TransactionEffectiveDate, 
	ProductCode, 
	ProductDesc, 
	TerminationDate, 
	o_PassThroughExlcusionFlag AS PassThroughExlcusionFlag
	FROM EXP_SRCDataCollect
	WHERE PassThroughExlcusionFlag='1'
),
AGG_Remove_Duplicates AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	TransactionCreatedDate,
	LocationId,
	LocationNumber,
	LineOfInsuranceCode,
	PolicyEffectiveDate,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	TransactionAmount,
	ExposureAmount,
	ExposureClassCode,
	SublineCode,
	PerilType,
	PerilCode,
	AnnualStatementLineNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	CoverageKey,
	CoverageCodeKey,
	CoverageCodeDesc,
	CoverageSubCd,
	CoverageVersion,
	TransactionEffectiveDate,
	ProductCode,
	ProductDesc,
	TerminationDate
	FROM FIL_ExcludePassThroughCoverage
	QUALIFY ROW_NUMBER() OVER (PARTITION BY TransactionCreatedDate, CoverageKey ORDER BY NULL) = 1
),
EXP_Key_BuiltUp AS (
	SELECT
	PolicySymbol,
	PolicyNumber,
	PolicyVersion,
	-- *INF*: PolicyNumber || IIF(ISNULL(ltrim(rtrim(PolicyVersion))) or Length(ltrim(rtrim(PolicyVersion)))=0 or IS_SPACES(PolicyVersion),'00',PolicyVersion)
	PolicyNumber || IFF(
	    ltrim(rtrim(PolicyVersion)) IS NULL
	    or Length(ltrim(rtrim(PolicyVersion))) = 0
	    or LENGTH(PolicyVersion)>0
	    and TRIM(PolicyVersion)='',
	    '00',
	    PolicyVersion
	) AS v_PolicyKey,
	-- *INF*: IIF(ISNULL(:LKP.LKP_POL_AK_ID(v_PolicyKey)),-1 , :LKP.LKP_POL_AK_ID(v_PolicyKey) )
	IFF(LKP_POL_AK_ID_v_PolicyKey.pol_ak_id IS NULL, - 1, LKP_POL_AK_ID_v_PolicyKey.pol_ak_id) AS v_Policyakid,
	v_LineOfInsuranceDesc AS o_LineOfInsuranceDesc,
	'N/A' AS v_LineOfInsuranceDesc,
	v_Policyakid AS o_PolicyAkid,
	TransactionCreatedDate,
	LocationId,
	LocationNumber,
	v_PolicyKey || '|' || LocationId || '|' ||LocationNumber AS v_RiskLocationKey,
	-- *INF*: IIF(ISNULL(:LKP.LKP_RISKLOCATION(v_RiskLocationKey)), -1, :LKP.LKP_RISKLOCATION(v_RiskLocationKey) )
	IFF(
	    LKP_RISKLOCATION_v_RiskLocationKey.RiskLocationAKID IS NULL, - 1,
	    LKP_RISKLOCATION_v_RiskLocationKey.RiskLocationAKID
	) AS v_RiskLocationakid,
	v_RiskLocationakid AS o_RiskLocationAkid,
	-- *INF*: MD5(TO_CHAR(v_Policyakid)||TO_CHAR(v_RiskLocationakid)||v_LineOfInsuranceDesc||TO_CHAR(PolicyEffectiveDate))
	MD5(TO_CHAR(v_Policyakid) || TO_CHAR(v_RiskLocationakid) || v_LineOfInsuranceDesc || TO_CHAR(PolicyEffectiveDate)) AS v_PolicyCoverageHashKey,
	-- *INF*: IIF(ISNULL(:LKP.LKP_POLICYCOVERAGEAKID(v_PolicyCoverageHashKey)), -1, :LKP.LKP_POLICYCOVERAGEAKID(v_PolicyCoverageHashKey))
	IFF(
	    LKP_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageAKID IS NULL, - 1,
	    LKP_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageAKID
	) AS v_PolicyCoverageakid,
	v_PolicyCoverageakid AS o_PolicyCoverageakid,
	LineOfInsuranceCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_INSURANCEREFERENCELINEOFBUSINESS(LineOfInsuranceCode)),33,:LKP.LKP_INSURANCEREFERENCELINEOFBUSINESS(LineOfInsuranceCode))
	IFF(
	    LKP_INSURANCEREFERENCELINEOFBUSINESS_LineOfInsuranceCode.InsuranceReferenceLineOfBusinessAKId IS NULL,
	    33,
	    LKP_INSURANCEREFERENCELINEOFBUSINESS_LineOfInsuranceCode.InsuranceReferenceLineOfBusinessAKId
	) AS v_InsuranceReferenceLineOfBusinessAKId,
	v_InsuranceReferenceLineOfBusinessAKId AS o_InsuranceReferenceLineOfBusinessAKId,
	PolicyEffectiveDate,
	CoverageEffectiveDate,
	CoverageExpirationDate,
	TransactionAmount,
	-- *INF*: IIF(TransactionAmount<>0,1,0)
	IFF(TransactionAmount <> 0, 1, 0) AS o_PremiumBearingIndicator,
	ExposureAmount,
	ExposureClassCode,
	SublineCode,
	PerilType,
	PerilCode,
	AnnualStatementLineNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	CoverageKey,
	CoverageCodeKey,
	CoverageCodeDesc,
	CoverageSubCd,
	CoverageVersion,
	-- *INF*: IIF(ISNULL(CoverageVersion) or IS_SPACES(CoverageVersion)  or LENGTH(CoverageVersion)=0,'N/A',CoverageVersion)
	IFF(
	    CoverageVersion IS NULL
	    or LENGTH(CoverageVersion)>0
	    and TRIM(CoverageVersion)=''
	    or LENGTH(CoverageVersion) = 0,
	    'N/A',
	    CoverageVersion
	) AS o_CoverageVersion,
	TransactionEffectiveDate,
	-1 AS o_StatisticalCoverageAKID,
	ProductCode,
	ProductDesc,
	-- *INF*: IIF(ISNULL(:LKP.LKP_PRODUCT(ProductCode)),34,:LKP.LKP_PRODUCT(ProductCode))
	-- --34 is Unassigned
	IFF(LKP_PRODUCT_ProductCode.ProductAKId IS NULL, 34, LKP_PRODUCT_ProductCode.ProductAKId) AS v_ProductAkid,
	v_ProductAkid AS o_ProductAkid,
	'000' AS SubLocationUnitNumber,
	'N/A' AS SpecialClassGroupCode,
	'ISO' AS ClassCodeOrganizationCode,
	-1 AS AnnualStatementLineId,
	TerminationDate,
	'N/A' AS SchedulePNumber,
	'N/A' AS SubAnnualStatementLineNumber,
	'N/A' AS OccupancyClassDescription,
	'0' AS ActiveBuildingFlag,
	'N/A' AS RiskType,
	'N/A' AS InsuranceLine,
	'N/A' AS o_CoverageForm,
	'N/A' AS PerilGroup
	FROM AGG_Remove_Duplicates
	LEFT JOIN LKP_POL_AK_ID LKP_POL_AK_ID_v_PolicyKey
	ON LKP_POL_AK_ID_v_PolicyKey.Pol_Key = v_PolicyKey

	LEFT JOIN LKP_RISKLOCATION LKP_RISKLOCATION_v_RiskLocationKey
	ON LKP_RISKLOCATION_v_RiskLocationKey.RiskLocationKey = v_RiskLocationKey

	LEFT JOIN LKP_POLICYCOVERAGEAKID LKP_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey
	ON LKP_POLICYCOVERAGEAKID_v_PolicyCoverageHashKey.PolicyCoverageHashKey = v_PolicyCoverageHashKey

	LEFT JOIN LKP_INSURANCEREFERENCELINEOFBUSINESS LKP_INSURANCEREFERENCELINEOFBUSINESS_LineOfInsuranceCode
	ON LKP_INSURANCEREFERENCELINEOFBUSINESS_LineOfInsuranceCode.InsuranceReferenceLineOfBusinessCode = LineOfInsuranceCode

	LEFT JOIN LKP_PRODUCT LKP_PRODUCT_ProductCode
	ON LKP_PRODUCT_ProductCode.ProductCode = ProductCode

),
LKP_ASL AS (
	SELECT
	AnnualStatementLineId,
	SchedulePNumber,
	AnnualStatementLineNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	InsuranceLineCode,
	DctRiskTypeCode,
	DctCoverageTypeCode,
	DctPerilGroup,
	DctSubCoverageTypeCode,
	DctCoverageVersion
	FROM (
		SELECT DISTINCT ASLRule.AnnualStatementLineId as AnnualStatementLineId, 
		ASL.SchedulePNumber as SchedulePNumber, 
		ASL.AnnualStatementLineNumber as AnnualStatementLineNumber, 
		ASL.AnnualStatementLineCode as AnnualStatementLineCode, 
		ASL.SubAnnualStatementLineNumber as SubAnnualStatementLineNumber, 
		ASL.SubAnnualStatementLineCode as SubAnnualStatementLineCode, 
		ASL.SubNonAnnualStatementLineCode as SubNonAnnualStatementLineCode, 
		SC.InsuranceLineCode as InsuranceLineCode, 
		SC.DctRiskTypeCode as DctRiskTypeCode, 
		SC.DctCoverageTypeCode as DctCoverageTypeCode, 
		SC.DctPerilGroup as DctPerilGroup, 
		SC.DctSubCoverageTypeCode as DctSubCoverageTypeCode, 
		SC.DctCoverageVersion as DctCoverageVersion 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule ASLRule
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine ASL
		on ASLRule.AnnualStatementLineId=ASL.AnnualStatementLineId
		join @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage SC
		on ASLRule.SystemCoverageId=SC.SystemCoverageId
		WHERE SC.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLineCode,DctRiskTypeCode,DctCoverageTypeCode,DctPerilGroup,DctSubCoverageTypeCode,DctCoverageVersion ORDER BY AnnualStatementLineId) = 1
),
LKP_RatingCoverage AS (
	SELECT
	RatingCoverageAKID,
	RatingCoverageId,
	RatingCoverageHashKey,
	RatingCoverageCancellationDate,
	CoverageGUID,
	CoverageType,
	SubCoverageTypeCode
	FROM (
		SELECT RC.RatingCoverageId as RatingCoverageId,
		RC.RatingCoverageAKID as RatingCoverageAKID, 
		RC.RatingCoverageHashKey as RatingCoverageHashKey, 
		RC.RatingCoverageCancellationDate as RatingCoverageCancellationDate, 
		RC.CoverageGUID as CoverageGUID,
		RC.CoverageType as CoverageType,
		RC.SubCoverageTypeCode as SubCoverageTypeCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
		on RC.CoverageGUID=C.Coveragekey
		and RC.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		order by RC.CoverageGUID,EffectiveDate desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID ORDER BY RatingCoverageAKID) = 1
),
LKP_RatingCoverage_WithDate AS (
	SELECT
	RatingCoverageAKID,
	RatingCoverageId,
	RatingCoverageHashKey,
	RatingCoverageCancellationDate,
	CoverageGUID,
	EffectiveDate,
	CoverageType,
	SubCoverageTypeCode
	FROM (
		SELECT RC.RatingCoverageId as RatingCoverageId,
		RC.RatingCoverageAKID as RatingCoverageAKID, 
		RC.RatingCoverageHashKey as RatingCoverageHashKey, 
		RC.RatingCoverageCancellationDate as RatingCoverageCancellationDate, 
		RC.CoverageGUID as CoverageGUID,
		RC.EffectiveDate as EffectiveDate, 
		RC.CoverageType as CoverageType,
		RC.SubCoverageTypeCode as SubCoverageTypeCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
		inner join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPLCoverage C
		on RC.CoverageGUID=C.Coveragekey
		and RC.SourceSystemId='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		order by RC.CoverageGUID,EffectiveDate desc,RC.CoverageType,RC.SubCoverageTypeCode
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageGUID,EffectiveDate ORDER BY RatingCoverageAKID) = 1
),
SEQ_RatingCoverageAKID AS (
	CREATE SEQUENCE SEQ_RatingCoverageAKID
	START = 1
	INCREMENT = 1;
),
EXP_CalValues AS (
	SELECT
	LKP_RatingCoverage.RatingCoverageId AS lkp_RatingCoverageId,
	LKP_RatingCoverage.RatingCoverageAKID AS lkp_RatingCoverageAKID,
	LKP_RatingCoverage.RatingCoverageHashKey AS lkp_RatingCoverageHashKey,
	LKP_RatingCoverage_WithDate.RatingCoverageId AS lkp_Dt_RatingCoverageId,
	LKP_RatingCoverage_WithDate.RatingCoverageAKID AS lkp_Dt_RatingCoverageAKID,
	LKP_RatingCoverage_WithDate.RatingCoverageHashKey AS lkp_Dt_RatingCoverageHashKey,
	SEQ_RatingCoverageAKID.NEXTVAL AS i_NEXTVAL,
	-- *INF*: IIF(ISNULL(lkp_Dt_RatingCoverageId),lkp_RatingCoverageId,lkp_Dt_RatingCoverageId)
	IFF(lkp_Dt_RatingCoverageId IS NULL, lkp_RatingCoverageId, lkp_Dt_RatingCoverageId) AS v_lkp_RatingCoverageid,
	v_lkp_RatingCoverageid AS o_lkp_RatingCoverageId,
	-- *INF*: IIF(ISNULL(lkp_Dt_RatingCoverageAKID), lkp_RatingCoverageAKID, lkp_Dt_RatingCoverageAKID)
	IFF(lkp_Dt_RatingCoverageAKID IS NULL, lkp_RatingCoverageAKID, lkp_Dt_RatingCoverageAKID) AS v_lkp_RatingCoverageAKID,
	-- *INF*: IIF(ISNULL(lkp_Dt_RatingCoverageHashKey), lkp_RatingCoverageHashKey, lkp_Dt_RatingCoverageHashKey)
	IFF(
	    lkp_Dt_RatingCoverageHashKey IS NULL, lkp_RatingCoverageHashKey,
	    lkp_Dt_RatingCoverageHashKey
	) AS v_lkp_RatingCoverageHashKey,
	EXP_Key_BuiltUp.CoverageKey AS i_CoverageGUID,
	EXP_Key_BuiltUp.TransactionEffectiveDate AS i_TEffectiveDate,
	-- *INF*: PolicyAKID||'~'||RiskLocationAkid||'~'||i_PolicyCoverageAKID||'~'||TO_CHAR(i_TCreatedDate)||'~'||i_CoverageGUID
	PolicyAKID || '~' || RiskLocationAkid || '~' || i_PolicyCoverageAKID || '~' || TO_CHAR(i_TCreatedDate) || '~' || i_CoverageGUID AS v_RatingCoverageKey,
	EXP_Key_BuiltUp.o_CoverageForm AS i_CoverageForm,
	EXP_Key_BuiltUp.RiskType AS i_RiskType,
	EXP_Key_BuiltUp.ExposureClassCode AS i_ClassCode,
	EXP_Key_BuiltUp.ExposureAmount AS i_Exposure,
	EXP_Key_BuiltUp.TransactionCreatedDate AS i_TCreatedDate,
	EXP_Key_BuiltUp.CoverageExpirationDate AS i_TExpirationDate,
	EXP_Key_BuiltUp.o_CoverageVersion AS i_CoverageVersion,
	EXP_Key_BuiltUp.SublineCode AS i_SubLineCode,
	LKP_ASL.AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	EXP_Key_BuiltUp.o_ProductAkid AS i_ProductAKId,
	EXP_Key_BuiltUp.o_InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId,
	EXP_Key_BuiltUp.SubLocationUnitNumber AS i_SubLocationUnitNumber,
	EXP_Key_BuiltUp.SpecialClassGroupCode AS i_SpecialClassGroupCode,
	EXP_Key_BuiltUp.ClassCodeOrganizationCode AS i_ClassCodeOrganizationCode,
	EXP_Key_BuiltUp.o_PolicyCoverageakid AS i_PolicyCoverageAKID,
	EXP_Key_BuiltUp.o_StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	LKP_ASL.AnnualStatementLineId AS i_AnnualStatementLineId,
	EXP_Key_BuiltUp.PerilGroup AS i_PerilGroup,
	EXP_Key_BuiltUp.o_PolicyAkid AS PolicyAKID,
	-- *INF*: MD5(i_ClassCode
	-- ||i_RiskType
	-- ||TO_CHAR(i_Exposure)
	-- ||TO_CHAR(RatingCoverageCancellationDate)
	-- ||i_SubLineCode
	-- ||i_AnnualStatementLineNumber
	-- ||TO_CHAR(i_PremiumBearingIndicator)
	-- --||TO_CHAR(i_AnnualStatementLineId)
	-- ||i_SubLocationUnitNumber
	-- ||i_SpecialClassGroupCode
	-- ||i_ClassCodeOrganizationCode
	-- ||i_PerilGroup
	-- ||OccupancyClassDescription
	-- ||ActiveBuildingFlag)
	MD5(i_ClassCode || i_RiskType || TO_CHAR(i_Exposure) || TO_CHAR(RatingCoverageCancellationDate) || i_SubLineCode || i_AnnualStatementLineNumber || TO_CHAR(i_PremiumBearingIndicator) || i_SubLocationUnitNumber || i_SpecialClassGroupCode || i_ClassCodeOrganizationCode || i_PerilGroup || OccupancyClassDescription || ActiveBuildingFlag) AS v_RatingCoverageHashKey,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(v_lkp_RatingCoverageid) ,'New', 
	-- ISNULL(lkp_Dt_RatingCoverageAKID),'New',
	-- v_lkp_RatingCoverageHashKey!=v_RatingCoverageHashKey,'Change',
	-- 'NoChange')
	DECODE(
	    TRUE,
	    v_lkp_RatingCoverageid IS NULL, 'New',
	    lkp_Dt_RatingCoverageAKID IS NULL, 'New',
	    v_lkp_RatingCoverageHashKey != v_RatingCoverageHashKey, 'Change',
	    'NoChange'
	) AS v_ChangeFlag,
	-- *INF*: IIF(PolicyAKID=v_prev_PolicyAKID and 
	-- i_CoverageGUID=v_prev_CoverageGUID and 
	-- ParentCoverageType=v_prev_coverageType and 
	-- SubCoverageTypeCode=v_prev_subCoverageType, v_prev_NEXTVAL, i_NEXTVAL)
	IFF(
	    PolicyAKID = v_prev_PolicyAKID
	    and i_CoverageGUID = v_prev_CoverageGUID
	    and ParentCoverageType = v_prev_coverageType
	    and SubCoverageTypeCode = v_prev_subCoverageType,
	    v_prev_NEXTVAL,
	    i_NEXTVAL
	) AS v_NEXTVAL,
	-- *INF*: IIF(PolicyAKID=v_prev_PolicyAKID and 
	-- i_CoverageGUID=v_prev_CoverageGUID and 
	-- ParentCoverageType=v_prev_coverageType and 
	-- SubCoverageTypeCode=v_prev_subCoverageType, v_Seq+1, 1)
	IFF(
	    PolicyAKID = v_prev_PolicyAKID
	    and i_CoverageGUID = v_prev_CoverageGUID
	    and ParentCoverageType = v_prev_coverageType
	    and SubCoverageTypeCode = v_prev_subCoverageType,
	    v_Seq + 1,
	    1
	) AS v_Seq,
	v_NEXTVAL AS v_prev_NEXTVAL,
	i_CoverageGUID AS v_prev_CoverageGUID,
	i_PolicyCoverageAKID AS v_prev_PolicyCoverageAKID,
	PolicyAKID AS v_prev_PolicyAKID,
	ParentCoverageType AS v_prev_coverageType,
	SubCoverageTypeCode AS v_prev_subCoverageType,
	v_ChangeFlag AS o_ChangeFlag,
	'1' AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	i_TCreatedDate AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	0 AS o_LogicalIndicator,
	v_RatingCoverageHashKey AS o_RatingCoverageHashKey,
	-- *INF*: IIF(v_ChangeFlag='New' AND ISNULL(v_lkp_RatingCoverageAKID),v_NEXTVAL,v_lkp_RatingCoverageAKID)
	IFF(
	    v_ChangeFlag = 'New' AND v_lkp_RatingCoverageAKID IS NULL, v_NEXTVAL,
	    v_lkp_RatingCoverageAKID
	) AS o_RatingCoverageAKID,
	i_StatisticalCoverageAKID AS o_StatisticalCoverageAKID,
	i_PolicyCoverageAKID AS o_PolicyCoverageAKID,
	v_RatingCoverageKey AS o_RatingCoverageKey,
	i_CoverageForm AS o_CoverageForm,
	i_ClassCode AS o_ClassCode,
	i_RiskType AS o_RiskType,
	EXP_Key_BuiltUp.CoverageCodeKey AS ParentCoverageType,
	i_Exposure AS o_Exposure,
	i_CoverageVersion AS o_CoverageVersion,
	i_CoverageGUID AS o_CoverageGUID,
	EXP_Key_BuiltUp.TerminationDate AS RatingCoverageCancellationDate,
	i_SubLineCode AS o_SubLineCode,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	i_PremiumBearingIndicator AS o_PremiumBearingIndicator,
	i_ProductAKId AS o_ProductAKId,
	i_InsuranceReferenceLineOfBusinessAKId AS o_InsuranceReferenceLineOfBusinessAKId,
	i_SubLocationUnitNumber AS o_SubLocationUnitNumber,
	i_SpecialClassGroupCode AS o_SpecialClassGroupCode,
	i_AnnualStatementLineId AS o_AnnualStatementLineId,
	i_ClassCodeOrganizationCode AS o_ClassCodeOrganizationCode,
	i_PerilGroup AS o_PerilGroup,
	LKP_ASL.SchedulePNumber,
	LKP_ASL.AnnualStatementLineCode,
	LKP_ASL.SubAnnualStatementLineNumber,
	LKP_ASL.SubAnnualStatementLineCode,
	LKP_ASL.SubNonAnnualStatementLineCode,
	EXP_Key_BuiltUp.CoverageSubCd AS SubCoverageTypeCode,
	EXP_Key_BuiltUp.OccupancyClassDescription,
	EXP_Key_BuiltUp.ActiveBuildingFlag,
	EXP_Key_BuiltUp.CoverageEffectiveDate AS RatingCoverageEffectiveDate,
	EXP_Key_BuiltUp.CoverageExpirationDate AS RatingCoverageExpirationDate,
	EXP_Key_BuiltUp.o_RiskLocationAkid AS RiskLocationAkid
	FROM EXP_Key_BuiltUp
	LEFT JOIN LKP_ASL
	ON LKP_ASL.InsuranceLineCode = EXP_Key_BuiltUp.InsuranceLine AND LKP_ASL.DctRiskTypeCode = EXP_Key_BuiltUp.RiskType AND LKP_ASL.DctCoverageTypeCode = EXP_Key_BuiltUp.CoverageCodeKey AND LKP_ASL.DctPerilGroup = EXP_Key_BuiltUp.PerilGroup AND LKP_ASL.DctSubCoverageTypeCode = EXP_Key_BuiltUp.CoverageSubCd AND LKP_ASL.DctCoverageVersion = EXP_Key_BuiltUp.o_CoverageVersion
	LEFT JOIN LKP_RatingCoverage
	ON LKP_RatingCoverage.CoverageGUID = EXP_Key_BuiltUp.CoverageKey
	LEFT JOIN LKP_RatingCoverage_WithDate
	ON LKP_RatingCoverage_WithDate.CoverageGUID = EXP_Key_BuiltUp.CoverageKey AND LKP_RatingCoverage_WithDate.EffectiveDate = EXP_Key_BuiltUp.TransactionCreatedDate
),
RTR_New_Upd AS (
	SELECT
	o_lkp_RatingCoverageId AS lkp_RatingCoverageId,
	o_ChangeFlag AS i_ChangeFlag,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LogicalIndicator AS LogicalIndicator,
	o_RatingCoverageHashKey AS RatingCoverageHashKey,
	o_RatingCoverageAKID AS RatingCoverageAKID,
	o_StatisticalCoverageAKID AS StatisticalCoverageAKID,
	o_PolicyCoverageAKID AS PolicyCoverageAKID,
	o_RatingCoverageKey AS RatingCoverageKey,
	o_CoverageForm AS CoverageForm,
	o_ClassCode AS ClassCode,
	o_RiskType AS RiskType,
	ParentCoverageType AS CoverageType,
	o_Exposure AS Exposure,
	RatingCoverageEffectiveDate,
	RatingCoverageExpirationDate,
	o_CoverageVersion AS CoverageVersion,
	o_CoverageGUID AS CoverageGUID,
	RatingCoverageCancellationDate,
	o_SubLineCode AS SubLineCode,
	o_AnnualStatementLineNumber AS ASLNum,
	o_PremiumBearingIndicator AS PremiumBearingIndicator,
	o_ProductAKId AS ProductAKId,
	o_InsuranceReferenceLineOfBusinessAKId AS InsuranceReferenceLineOfBusinessAKId,
	o_SubLocationUnitNumber AS SubLocationUnitNumber,
	o_SpecialClassGroupCode AS SpecialClassGroupCode,
	o_AnnualStatementLineId AS AnnualStatementLineId,
	o_ClassCodeOrganizationCode AS ClassCodeOrganizationCode,
	o_PerilGroup AS PerilGroup,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	SubCoverageTypeCode,
	OccupancyClassDescription,
	ActiveBuildingFlag
	FROM EXP_CalValues
),
RTR_New_Upd_INSERT AS (SELECT * FROM RTR_New_Upd WHERE i_ChangeFlag='New'),
RTR_New_Upd_UPDATE AS (SELECT * FROM RTR_New_Upd WHERE i_ChangeFlag='Change'),
UPD_DueToCodeChange AS (
	SELECT
	lkp_RatingCoverageId, 
	ModifiedDate, 
	LogicalIndicator, 
	RatingCoverageHashKey, 
	CoverageForm, 
	ClassCode, 
	RiskType, 
	CoverageType, 
	Exposure, 
	CoverageVersion, 
	RatingCoverageCancellationDate AS RatingCoverageCancellationDate3, 
	SubLineCode, 
	ASLNum, 
	PremiumBearingIndicator, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	SubLocationUnitNumber, 
	SpecialClassGroupCode, 
	AnnualStatementLineId, 
	ClassCodeOrganizationCode, 
	PerilGroup, 
	SchedulePNumber, 
	AnnualStatementLineCode, 
	SubAnnualStatementLineNumber, 
	SubAnnualStatementLineCode, 
	SubNonAnnualStatementLineCode, 
	SubCoverageTypeCode, 
	OccupancyClassDescription, 
	ActiveBuildingFlag
	FROM RTR_New_Upd_UPDATE
),
TGT_RatingCoverage_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage AS T
	USING UPD_DueToCodeChange AS S
	ON T.RatingCoverageId = S.lkp_RatingCoverageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.LogicalIndicator = S.LogicalIndicator, T.RatingCoverageHashKey = S.RatingCoverageHashKey, T.CoverageForm = S.CoverageForm, T.ClassCode = S.ClassCode, T.RiskType = S.RiskType, T.CoverageType = S.CoverageType, T.Exposure = S.Exposure, T.CoverageVersion = S.CoverageVersion, T.SublineCode = S.SubLineCode, T.AnnualStatementLineNumber = S.ASLNum, T.PremiumBearingIndicator = S.PremiumBearingIndicator, T.ProductAKId = S.ProductAKId, T.InsuranceReferenceLineOfBusinessAKId = S.InsuranceReferenceLineOfBusinessAKId, T.SubLocationUnitNumber = S.SubLocationUnitNumber, T.SpecialClassGroupCode = S.SpecialClassGroupCode, T.AnnualStatementLineId = S.AnnualStatementLineId, T.ClassCodeOrganizationCode = S.ClassCodeOrganizationCode, T.PerilGroup = S.PerilGroup, T.SchedulePNumber = S.SchedulePNumber, T.AnnualStatementLineCode = S.AnnualStatementLineCode, T.SubAnnualStatementLineNumber = S.SubAnnualStatementLineNumber, T.SubAnnualStatementLineCode = S.SubAnnualStatementLineCode, T.SubNonAnnualStatementLineCode = S.SubNonAnnualStatementLineCode, T.SubCoverageTypeCode = S.SubCoverageTypeCode, T.OccupancyClassDescription = S.OccupancyClassDescription, T.ActiveBuildingFlag = S.ActiveBuildingFlag
),
SRT_SetRecordOrderForAkid AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	CoverageGUID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	LogicalIndicator, 
	RatingCoverageHashKey, 
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
	CoverageVersion, 
	RatingCoverageCancellationDate, 
	SubLineCode, 
	ASLNum, 
	PremiumBearingIndicator, 
	ProductAKId, 
	InsuranceReferenceLineOfBusinessAKId, 
	SubLocationUnitNumber, 
	SpecialClassGroupCode, 
	AnnualStatementLineId, 
	ClassCodeOrganizationCode, 
	PerilGroup, 
	SchedulePNumber, 
	AnnualStatementLineCode, 
	SubAnnualStatementLineNumber, 
	SubAnnualStatementLineCode, 
	SubNonAnnualStatementLineCode, 
	SubCoverageTypeCode, 
	OccupancyClassDescription, 
	ActiveBuildingFlag
	FROM RTR_New_Upd_INSERT
	ORDER BY CoverageGUID ASC, EffectiveDate ASC
),
EXP_SetRatingCoverageAkid AS (
	SELECT
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	LogicalIndicator,
	RatingCoverageHashKey,
	CoverageGUID,
	RatingCoverageAKID,
	-- *INF*: IIF(CoverageGUID=v_PrevTransactionCoverageGUID,v_PrevTransactionRatingCoverageAKID,RatingCoverageAKID)
	IFF(
	    CoverageGUID = v_PrevTransactionCoverageGUID, v_PrevTransactionRatingCoverageAKID,
	    RatingCoverageAKID
	) AS v_RatingCoverageAKID,
	v_RatingCoverageAKID AS o_RatingCoverageAKID,
	v_RatingCoverageAKID AS v_PrevTransactionRatingCoverageAKID,
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
	CoverageVersion,
	CoverageGUID AS v_PrevTransactionCoverageGUID,
	RatingCoverageCancellationDate,
	SubLineCode,
	ASLNum,
	PremiumBearingIndicator,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	SubLocationUnitNumber,
	SpecialClassGroupCode,
	AnnualStatementLineId,
	ClassCodeOrganizationCode,
	PerilGroup,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	SubCoverageTypeCode,
	OccupancyClassDescription,
	ActiveBuildingFlag
	FROM SRT_SetRecordOrderForAkid
),
TGT_RatingCoverage_Insert AS (

	------------ PRE SQL ----------
	update RC
	set RC.AnnualStatementLineId=ASL.AnnualStatementLineId,
	RC.AnnualStatementLineNumber=ASL.AnnualStatementLineNumber,
	RC.AnnualStatementLineCode=ASL.AnnualStatementLineCode,
	RC.SubAnnualStatementLineCode=ASL.SubAnnualStatementLineCode,
	RC.SubAnnualStatementLineNumber=ASL.SubAnnualStatementLineNumber,
	RC.SubNonAnnualStatementLineCode=ASL.SubNonAnnualStatementLineCode,
	RC.SchedulePNumber=ASL.SchedulePNumber,
	RC.RatingCoverageHashKey=convert(varchar(max),hashbytes('MD5',convert(varchar(max),RC.ClassCode+RC.RiskType+convert(varchar(max),RC.Exposure)+
	Convert(CHAR(10),RC.RatingCoverageCancellationDate,101) + ' ' + Convert(CHAR(8),RC.RatingCoverageCancellationDate,108)
	+RC.SubLineCode+ASL.AnnualStatementLineNumber+CONVERT(varchar(max),RC.PremiumBearingIndicator)+RC.SubLocationUnitNumber
	+RC.SpecialClassGroupCode+RC.ClassCodeOrganizationCode+RC.PerilGroup+RC.OccupancyClassDescription+convert(varchar(1),RC.ActiveBuildingFlag))),2)
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
	on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	left join @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line SIL
	on PC.InsuranceLine=SIL.ins_line_code and SIL.crrnt_snpsht_flag=1
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.SystemCoverage SC
	on RC.CoverageType=SC.DctCoverageTypeCode
	and RC.RiskType=SC.DctRiskTypeCode
	and RC.CoverageVersion=SC.DctCoverageVersion
	and RC.PerilGroup=SC.DctPerilGroup
	and RC.SubCoverageTypeCode=SC.DctSubCoverageTypeCode
	and isnull(SIL.StandardInsuranceLineCode, 'N/A')=SC.InsuranceLineCode
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.SupDCTAnnualStatementLineRule R
	on SC.SystemCoverageId=R.SystemCoverageId
	join @{pipeline().parameters.TARGET_TABLE_OWNER}.AnnualStatementLine ASL
	on R.AnnualStatementLineId=ASL.AnnualStatementLineId
	where RC.AnnualStatementLineId=-1 and ASL.AnnualStatementLineId is not null
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, RatingCoverageHashKey, RatingCoverageAKID, StatisticalCoverageAKID, PolicyCoverageAKID, RatingCoverageKey, CoverageForm, ClassCode, RiskType, CoverageType, Exposure, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, CoverageVersion, CoverageGUID, RatingCoverageCancellationDate, SublineCode, AnnualStatementLineNumber, PremiumBearingIndicator, ProductAKId, InsuranceReferenceLineOfBusinessAKId, SubLocationUnitNumber, SpecialClassGroupCode, AnnualStatementLineId, ClassCodeOrganizationCode, PerilGroup, SchedulePNumber, AnnualStatementLineCode, SubAnnualStatementLineNumber, SubAnnualStatementLineCode, SubNonAnnualStatementLineCode, SubCoverageTypeCode, OccupancyClassDescription, ActiveBuildingFlag)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	RATINGCOVERAGEHASHKEY, 
	o_RatingCoverageAKID AS RATINGCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	POLICYCOVERAGEAKID, 
	RATINGCOVERAGEKEY, 
	COVERAGEFORM, 
	CLASSCODE, 
	RISKTYPE, 
	COVERAGETYPE, 
	EXPOSURE, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	COVERAGEVERSION, 
	COVERAGEGUID, 
	RATINGCOVERAGECANCELLATIONDATE, 
	SubLineCode AS SUBLINECODE, 
	ASLNum AS ANNUALSTATEMENTLINENUMBER, 
	PREMIUMBEARINGINDICATOR, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	SUBLOCATIONUNITNUMBER, 
	SPECIALCLASSGROUPCODE, 
	ANNUALSTATEMENTLINEID, 
	CLASSCODEORGANIZATIONCODE, 
	PERILGROUP, 
	SCHEDULEPNUMBER, 
	ANNUALSTATEMENTLINECODE, 
	SUBANNUALSTATEMENTLINENUMBER, 
	SUBANNUALSTATEMENTLINECODE, 
	SUBNONANNUALSTATEMENTLINECODE, 
	SUBCOVERAGETYPECODE, 
	OCCUPANCYCLASSDESCRIPTION, 
	ACTIVEBUILDINGFLAG
	FROM EXP_SetRatingCoverageAkid
),
SQ_RatingCoverage_UPDATE AS (
	SELECT RC.RatingCoverageId, RC.EffectiveDate, RC.ExpirationDate, RC.RatingCoverageAKID,PC.Policyakid 
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC
	 inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
	on PC.PolicyCoverageAKID=RC.PolicyCoverageAKID
	and PC.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and PC.CurrentSnapshotFlag=1
	WHERE EXISTS (SELECT RC1.RatingCoverageAKID
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC1 
	inner join @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC1
	on PC1.PolicyCoverageAKID=RC1.PolicyCoverageAKID
	and PC1.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and PC1.CurrentSnapshotFlag=1
	WHERE RC1.CurrentSnapshotFlag = 1 AND RC1.SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND RC1.RatingCoverageAKID=RC.RatingCoverageAKID and PC1.PolicyAKID=PC.PolicyAKID
	GROUP BY PC1.PolicyAKID,RC1.RatingCoverageAKID HAVING COUNT(*)>1)
	ORDER BY PC.PolicyAKID,RatingCoverageAKID, EffectiveDate DESC
),
EXP_GetDates AS (
	SELECT
	RatingCoverageId AS i_RatingCoverageId,
	EffectiveDate AS i_EffectiveDate,
	ExpirationDate AS i_ExpirationDate,
	RatingCoverageAKID AS i_RatingCoverageAKID,
	PolicyAKID AS i_PolicyAKID,
	-- *INF*: IIF(i_PolicyAKID=v_Prev_PolicyAKID and i_RatingCoverageAKID = v_Prev_RatingCoverageAKID,ADD_TO_DATE(v_Prev_EffectiveDate,'SS',-1),i_ExpirationDate)
	IFF(
	    i_PolicyAKID = v_Prev_PolicyAKID and i_RatingCoverageAKID = v_Prev_RatingCoverageAKID,
	    DATEADD(SECOND,- 1,v_Prev_EffectiveDate),
	    i_ExpirationDate
	) AS v_ExpirationDate,
	i_PolicyAKID AS v_Prev_PolicyAKID,
	i_RatingCoverageAKID AS v_Prev_RatingCoverageAKID,
	i_EffectiveDate AS v_Prev_EffectiveDate,
	i_ExpirationDate AS o_Orig_ExpirationDate,
	i_RatingCoverageId AS o_RatingCoverageId,
	'0' AS o_CurrentSnapshotFlag,
	v_ExpirationDate AS o_ExpirationDate,
	SYSDATE AS o_ModifiedDate
	FROM SQ_RatingCoverage_UPDATE
),
FIL_FirstRowInAKIDGroup AS (
	SELECT
	o_Orig_ExpirationDate AS i_Orig_ExpirationDate, 
	o_RatingCoverageId AS RatingCoverageId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_ExpirationDate AS ExpirationDate, 
	o_ModifiedDate AS ModifiedDate
	FROM EXP_GetDates
	WHERE i_Orig_ExpirationDate  !=  ExpirationDate
),
UPD_ExpiratedRecords AS (
	SELECT
	RatingCoverageId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKIDGroup
),
TGT_RatingCoverage_Expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage AS T
	USING UPD_ExpiratedRecords AS S
	ON T.RatingCoverageId = S.RatingCoverageId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),
SQ_WorkDCTPLPolicy_CFA AS (
	SELECT DISTINCT
	P.PolicyNumber + P.PolicyVersion as PolicyKey,
	P.TransactionCreatedDate,
	P.LineageId,
	ISNULL(P.PolicyState,'N/A') as PolicyState
	FROM 
	WorkDCTPLPolicy P
	WHERE 
	P.PolicyStatusKey='ClaimFreeAward'
	@{pipeline().parameters.WHERE_CLAUSE_STG_CFA}
	Order by 1,2
),
EXP_Input_WorkDCTPLPolicy_CFA AS (
	SELECT
	PolicyKey,
	TransactionCreatedDate,
	LineageId,
	PolicyState
	FROM SQ_WorkDCTPLPolicy_CFA
),
LKP_WorkCFAPolicyList_Exists AS (
	SELECT
	WorkCFAPolicyListId,
	PolicyKey,
	TransactionCreatedDate,
	CoverageKey,
	Status,
	AuditId,
	SourceSysId,
	CreatedDate,
	ModifiedDate,
	in_PolicyKey,
	in_TransactionCreatedDate,
	in_LineageId,
	in_PolicyState
	FROM (
		SELECT 
			WorkCFAPolicyListId,
			PolicyKey,
			TransactionCreatedDate,
			CoverageKey,
			Status,
			AuditId,
			SourceSysId,
			CreatedDate,
			ModifiedDate,
			in_PolicyKey,
			in_TransactionCreatedDate,
			in_LineageId,
			in_PolicyState
		FROM WorkCFAPolicyList
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY WorkCFAPolicyListId) = 1
),
EXP_WorkCFAPolicyList_lookup_output AS (
	SELECT
	WorkCFAPolicyListId,
	in_PolicyKey AS PolicyKey,
	in_TransactionCreatedDate AS TransactionCreatedDate,
	in_LineageId AS LineageId,
	in_PolicyState AS PolicyState
	FROM LKP_WorkCFAPolicyList_Exists
),
FIL_WorkCFAPolicyList_New AS (
	SELECT
	WorkCFAPolicyListId, 
	PolicyKey, 
	TransactionCreatedDate, 
	LineageId, 
	PolicyState
	FROM EXP_WorkCFAPolicyList_lookup_output
	WHERE ISNULL(WorkCFAPolicyListId)
),
EXP_WorkCFAPolicyList_Output AS (
	SELECT
	PolicyKey,
	TransactionCreatedDate,
	LineageId,
	PolicyState,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_DefaultDate
	FROM FIL_WorkCFAPolicyList_New
),
TGT_WorkCFAPolicyList_Insert AS (
	INSERT INTO WorkCFAPolicyList
	(PolicyKey, TransactionCreatedDate, LineageId, AuditId, SourceSysId, CreatedDate, ModifiedDate, PolicyStateCode)
	SELECT 
	POLICYKEY, 
	TRANSACTIONCREATEDDATE, 
	LINEAGEID, 
	o_AuditId AS AUDITID, 
	o_SourceSystemID AS SOURCESYSID, 
	o_DefaultDate AS CREATEDDATE, 
	o_DefaultDate AS MODIFIEDDATE, 
	PolicyState AS POLICYSTATECODE
	FROM EXP_WorkCFAPolicyList_Output
),
SQ_RatingCoverage_CFA AS (
	SELECT
	P.pol_key AS PolicyKey,
	RC.RatingCoverageId, 
	RC.CurrentSnapshotFlag, 
	RC.AuditID, 
	RC.EffectiveDate, 
	RC.ExpirationDate, 
	RC.SourceSystemID, 
	RC.CreatedDate, 
	RC.ModifiedDate, 
	RC.LogicalIndicator, 
	RC.RatingCoverageHashKey, 
	RC.RatingCoverageAKID, 
	RC.StatisticalCoverageAKID, 
	RC.PolicyCoverageAKID, 
	RC.RatingCoverageKey, 
	RC.CoverageForm, 
	RC.ClassCode, 
	RC.RiskType, 
	RC.CoverageType, 
	RC.Exposure, 
	RC.RatingCoverageEffectiveDate, 
	RC.RatingCoverageExpirationDate, 
	RC.CoverageVersion, 
	RC.CoverageGUID, 
	RC.RatingCoverageCancellationDate, 
	RC.SublineCode, 
	RC.AnnualStatementLineNumber, 
	RC.PremiumBearingIndicator, 
	RC.ProductAKId, 
	RC.InsuranceReferenceLineOfBusinessAKId, 
	RC.SubLocationUnitNumber, 
	RC.SpecialClassGroupCode, 
	RC.AnnualStatementLineId, 
	RC.ClassCodeOrganizationCode, 
	RC.PerilGroup, 
	RC.SchedulePNumber, 
	RC.AnnualStatementLineCode, 
	RC.SubAnnualStatementLineNumber, 
	RC.SubAnnualStatementLineCode, 
	RC.SubNonAnnualStatementLineCode, 
	RC.SubCoverageTypeCode, 
	RC.OccupancyClassDescription, 
	RC.ActiveBuildingFlag ,
	PC.PolicyAKId,
	CFA.WorkCFAPolicyListId,
	CFA.TransactionCreatedDate as CFATransactionCreatedDate,
	CFA.LineageId 
	FROM 
	RatingCoverage RC
	INNER JOIN PolicyCoverage PC 
	ON RC.PolicyCoverageAKID=PC.PolicyCoverageAKID AND PC.CurrentSnapshotFlag=1
	INNER JOIN v2.Policy P 
	ON P.pol_ak_id =PC.policyakid AND P.crrnt_snpsht_flag=1
	INNER JOIN WorkCFAPolicyList CFA
	ON P.Pol_key=CFA.PolicyKey AND RC.EffectiveDate < CFA.TransactionCreatedDate AND CFA.Status IS NULL
	WHERE
	NOT RC.CoverageGuid Like '%CFA' AND
	NOT EXISTS 
	(Select 1 FROM
	RatingCoverage RC2 with (nolock)
	INNER JOIN PolicyCoverage PC2 with (nolock)
	ON RC2.PolicyCoverageAKID=PC2.PolicyCoverageAKID AND PC2.CurrentSnapshotFlag=1
	INNER JOIN v2.Policy P2 with (nolock)
	ON P2.pol_ak_id =PC2.policyakid AND P2.crrnt_snpsht_flag=1
	INNER JOIN WorkCFAPolicyList CFA2 with (nolock)
	ON P2.Pol_key=CFA2.PolicyKey and CFA.Status IS NULL where RC2.CoverageGuid = RC.CoverageGuid+'CFA'
	)
	@{pipeline().parameters.WHERE_CLAUSE_RC_CFA}
	order by P.Pol_Key, RC.RatingCoverageAKID, eff_from_date
),
EXP_Input_RatingCoverage_CFA AS (
	SELECT
	PolicyKey,
	RatingCoverageId,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	LogicalIndicator,
	RatingCoverageHashKey,
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
	CoverageVersion,
	CoverageGUID,
	RatingCoverageCancellationDate,
	SublineCode,
	AnnualStatementLineNumber,
	PremiumBearingIndicator,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	SubLocationUnitNumber,
	SpecialClassGroupCode,
	AnnualStatementLineId,
	ClassCodeOrganizationCode,
	PerilGroup,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	SubCoverageTypeCode,
	OccupancyClassDescription,
	ActiveBuildingFlag,
	PolicyAKId,
	WorkCFAPolicyListId,
	CFATransactionCreatedDate,
	LineageId
	FROM SQ_RatingCoverage_CFA
),
EXP_Output_CFA AS (
	SELECT
	RatingCoverageAKID,
	RatingCoverageId,
	PolicyKey AS RCPolicyKey,
	CurrentSnapshotFlag,
	AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreatedDate,
	ModifiedDate,
	LogicalIndicator,
	RatingCoverageHashKey,
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
	CoverageVersion,
	CoverageGUID,
	RatingCoverageCancellationDate,
	SublineCode,
	AnnualStatementLineNumber,
	PremiumBearingIndicator,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	SubLocationUnitNumber,
	SpecialClassGroupCode,
	AnnualStatementLineId,
	ClassCodeOrganizationCode,
	PerilGroup,
	SchedulePNumber,
	AnnualStatementLineCode,
	SubAnnualStatementLineNumber,
	SubAnnualStatementLineCode,
	SubNonAnnualStatementLineCode,
	SubCoverageTypeCode,
	OccupancyClassDescription,
	ActiveBuildingFlag,
	PolicyAKId,
	WorkCFAPolicyListId,
	CFATransactionCreatedDate,
	LineageId,
	SEQ_RatingCoverageAKID.NEXTVAL AS i_NEXTVAL,
	-- *INF*: IIF(RatingCoverageAKID=v_prev_RatingCoverageAKID, v_prev_NEXTVAL,i_NEXTVAL)
	-- 
	-- 
	-- //IIF(PolicyAKId=v_prev_PolicyAKID and 
	-- //v_CoverageGUID=v_prev_CoverageGUID and 
	-- //CoverageType=v_prev_CoverageType and 
	-- //v_SubCoverageType=v_prev_SubCoverageType, v_prev_NEXTVAL, i_NEXTVAL)
	IFF(RatingCoverageAKID = v_prev_RatingCoverageAKID, v_prev_NEXTVAL, i_NEXTVAL) AS v_NEXTVAL,
	CoverageGUID || 'CFA' AS v_CoverageGUID,
	SubCoverageTypeCode || 'CFA' AS v_SubCoverageType,
	RatingCoverageKey||'CFA' AS v_RatingCoverageKey,
	v_NEXTVAL AS v_prev_NEXTVAL,
	v_CoverageGUID AS v_prev_CoverageGUID,
	PolicyCoverageAKID AS v_prev_PolicyCoverageAKID,
	PolicyAKId AS v_prev_PolicyAKID,
	CoverageType AS v_prev_CoverageType,
	v_SubCoverageType AS v_prev_SubCoverageType,
	RatingCoverageAKID AS v_prev_RatingCoverageAKID,
	v_NEXTVAL AS o_RatingCoverageAKID,
	v_CoverageGUID AS o_CoverageGuid,
	v_RatingCoverageKey AS o_RatingCoverageKey,
	v_SubCoverageType AS o_SubCoverageTypeCode,
	SYSDATE AS o_ModifiedDate,
	'Processed' AS o_Status,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Input_RatingCoverage_CFA
),
TGT_WorkCFARatingCoverageXRef_Insert AS (
	INSERT INTO WorkCFARatingCoverageXRef
	(WorkCFAPolicyListId, PolicyKey, TransactionCreatedDate, LineageId, OriginalRatingCoverageAKID, OriginalCoverageGuid, OriginalEffectiveDate, CFARatingCoverageAKID, CFACoverageGuid, AuditId, SourceSysId, CreatedDate, ModifiedDate)
	SELECT 
	WORKCFAPOLICYLISTID, 
	RCPolicyKey AS POLICYKEY, 
	CFATransactionCreatedDate AS TRANSACTIONCREATEDDATE, 
	LINEAGEID, 
	RatingCoverageAKID AS ORIGINALRATINGCOVERAGEAKID, 
	CoverageGUID AS ORIGINALCOVERAGEGUID, 
	EffectiveDate AS ORIGINALEFFECTIVEDATE, 
	o_RatingCoverageAKID AS CFARATINGCOVERAGEAKID, 
	o_CoverageGuid AS CFACOVERAGEGUID, 
	o_AuditId AS AUDITID, 
	SourceSystemID AS SOURCESYSID, 
	o_ModifiedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE
	FROM EXP_Output_CFA
),
AGG_WorkCFAPolicyList AS (
	SELECT
	WorkCFAPolicyListId,
	o_Status,
	o_ModifiedDate
	FROM EXP_Output_CFA
	QUALIFY ROW_NUMBER() OVER (PARTITION BY WorkCFAPolicyListId, o_ModifiedDate ORDER BY NULL) = 1
),
UPD_WorkCFAPolicyList AS (
	SELECT
	WorkCFAPolicyListId, 
	o_Status, 
	o_ModifiedDate
	FROM AGG_WorkCFAPolicyList
),
TGT_WorkCFAPolicyList_Update AS (
	MERGE INTO WorkCFAPolicyList AS T
	USING UPD_WorkCFAPolicyList AS S
	ON T.WorkCFAPolicyListId = S.WorkCFAPolicyListId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.Status = S.o_Status, T.ModifiedDate = S.o_ModifiedDate

	------------ POST SQL ----------
	UPDATE WorkCFAPolicyList Set Status='Ignored' WHERE Status is NULL
	-------------------------------


),
TGT_RatingCoverage_CFA AS (
	INSERT INTO RatingCoverage
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, RatingCoverageHashKey, RatingCoverageAKID, StatisticalCoverageAKID, PolicyCoverageAKID, RatingCoverageKey, CoverageForm, ClassCode, RiskType, CoverageType, Exposure, RatingCoverageEffectiveDate, RatingCoverageExpirationDate, CoverageVersion, CoverageGUID, RatingCoverageCancellationDate, SublineCode, AnnualStatementLineNumber, PremiumBearingIndicator, ProductAKId, InsuranceReferenceLineOfBusinessAKId, SubLocationUnitNumber, SpecialClassGroupCode, AnnualStatementLineId, ClassCodeOrganizationCode, PerilGroup, SchedulePNumber, AnnualStatementLineCode, SubAnnualStatementLineNumber, SubAnnualStatementLineCode, SubNonAnnualStatementLineCode, SubCoverageTypeCode, OccupancyClassDescription, ActiveBuildingFlag)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	o_AuditId AS AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	o_ModifiedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	LOGICALINDICATOR, 
	RATINGCOVERAGEHASHKEY, 
	o_RatingCoverageAKID AS RATINGCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	POLICYCOVERAGEAKID, 
	o_RatingCoverageKey AS RATINGCOVERAGEKEY, 
	COVERAGEFORM, 
	CLASSCODE, 
	RISKTYPE, 
	COVERAGETYPE, 
	EXPOSURE, 
	RATINGCOVERAGEEFFECTIVEDATE, 
	RATINGCOVERAGEEXPIRATIONDATE, 
	COVERAGEVERSION, 
	o_CoverageGuid AS COVERAGEGUID, 
	RATINGCOVERAGECANCELLATIONDATE, 
	SUBLINECODE, 
	ANNUALSTATEMENTLINENUMBER, 
	PREMIUMBEARINGINDICATOR, 
	PRODUCTAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	SUBLOCATIONUNITNUMBER, 
	SPECIALCLASSGROUPCODE, 
	ANNUALSTATEMENTLINEID, 
	CLASSCODEORGANIZATIONCODE, 
	PERILGROUP, 
	SCHEDULEPNUMBER, 
	ANNUALSTATEMENTLINECODE, 
	SUBANNUALSTATEMENTLINENUMBER, 
	SUBANNUALSTATEMENTLINECODE, 
	SUBNONANNUALSTATEMENTLINECODE, 
	o_SubCoverageTypeCode AS SUBCOVERAGETYPECODE, 
	OCCUPANCYCLASSDESCRIPTION, 
	ACTIVEBUILDINGFLAG
	FROM EXP_Output_CFA
),