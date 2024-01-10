WITH
LKP_Supclassification_Lob AS (
	SELECT
	Result,
	LineOfBusinessAbbreviation,
	ClassCode,
	RatingStateCode
	FROM (
		select ClassCode as ClassCode 
		,RatingStateCode as RatingStateCode 
		,LineOfBusinessAbbreviation as LineOfBusinessAbbreviation
		, Result as Result  from 
		(
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'CF' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationCommercialProperty
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'CA' AS LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationCommercialAuto
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'CR' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationCrime
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'WC' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationWorkersCompensation
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'BND' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationBonds
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'IM' as LineOfBusinessAbbreviation , case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end  As Result
		from dbo.SupClassificationInlandMarine
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'UMB' as LineOfBusinessAbbreviation , case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationUmbrella
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'DNO' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationDirectorsOfficers
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'ENO' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationErrorsOmissions
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'EPLI' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationEPLI
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'EL' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result 
		from dbo.SupClassificationExcessLiability
		where CurrentSnapshotFlag=1
		Union 
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'GA' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationGarage
		where CurrentSnapshotFlag=1
		Union
		select ClassCode as ClassCode ,RatingStateCode as RatingStateCode,'GL' as LineOfBusinessAbbreviation, case when ClassDescription IS NULL then 'N/A'  Else ClassDescription end + '#'+case when OriginatingOrganizationCode IS NULL then 'N/A'  Else OriginatingOrganizationCode end As Result
		from dbo.SupClassificationGeneralLiability
		where CurrentSnapshotFlag=1
		) a
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusinessAbbreviation,ClassCode,RatingStateCode ORDER BY Result) = 1
),
SQ_CoverageDetailDim AS (
	SELECT CoverageDetailDimId
		,ClassCode
		,RatingStateProvinceCode
		,InsuranceReferenceLineOfBusinessAbbreviation
		,ProductAbbreviation
	FROM (
		SELECT CDD.CoverageDetailDimId
			,Cdd.ClassCode
			,Cdd.RatingStateProvinceCode
			,IR.InsuranceReferenceLineOfBusinessAbbreviation
			,P.ProductAbbreviation
			,ISC.InsuranceSegmentCode
			,CDD.ClassDescription
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT ON CDD.EDWPremiumTransactionPKId = PT.PremiumTransactionID
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC ON PT.RatingCoverageAKId = RC.RatingCoverageAKID
			AND PT.EffectiveDate = RC.EffectiveDate
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.v2.Policy Pol ON RL.PolicyAKID = Pol.pol_ak_id
			AND Pol.crrnt_snpsht_flag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IR ON RC.InsuranceReferenceLineOfBusinessAKId = IR.InsuranceReferenceLineOfBusinessAKId
			AND IR.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product P ON RC.ProductAKId = P.ProductAKId
			AND p.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment ISC ON Pol.InsuranceSegmentAKId = ISC.InsuranceSegmentAKId
			AND ISC.CurrentSnapshotFlag = 1
		
		UNION ALL
		
		SELECT CDD.CoverageDetailDimId
			,Cdd.ClassCode
			,Cdd.RatingStateProvinceCode
			,IR.InsuranceReferenceLineOfBusinessAbbreviation
			,P.ProductAbbreviation
			,ISC.InsuranceSegmentCode
			,CDD.ClassDescription
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.CoverageDetailDim CDD
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT ON CDD.EDWPremiumTransactionPKId = PT.PremiumTransactionID
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID
			AND SC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID
			AND PC.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL ON PC.RiskLocationAKID = RL.RiskLocationAKID
			AND RL.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.v2.Policy Pol ON RL.PolicyAKID = Pol.pol_ak_id
			AND Pol.crrnt_snpsht_flag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceLineOfBusiness IR ON SC.InsuranceReferenceLineOfBusinessAKId = IR.InsuranceReferenceLineOfBusinessAKId
			AND IR.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.Product P ON SC.ProductAKId = P.ProductAKId
			AND p.CurrentSnapshotFlag = 1
		INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment ISC ON Pol.InsuranceSegmentAKId = ISC.InsuranceSegmentAKId
			AND ISC.CurrentSnapshotFlag = 1
		) CoverageDetail
	WHERE InsuranceSegmentCode != '1' and ClassDescription='N/A'
),
EXP_ClassDesc AS (
	SELECT
	CoverageDetailDimId,
	ClassCode AS i_ClassCode,
	RatingStateProvinceCode AS i_RatingState,
	InsuranceReferenceLineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	ProductAbbreviation AS i_ProductAbbreviation,
	-- *INF*: DECODE(TRUE,IN (i_LineOfBusinessAbbreviation,'Bonds - Fidelity', 'Bonds - Surety') , 'BND' ,
	-- IN (i_LineOfBusinessAbbreviation,'NFP D&O', 'D&O') , 'DNO' ,
	-- IN (i_LineOfBusinessAbbreviation,'CL B&M', 'CL Mine Sub','CL Prop','Cyber Security','Data Compromise') , 'CF' ,
	-- IN (i_LineOfBusinessAbbreviation,'CL IM') , 'IM',
	-- IN (i_LineOfBusinessAbbreviation,'CL Umb') , 'UMB',
	-- IN (i_LineOfBusinessAbbreviation,'CL Auto') , 'CA',
	-- IN (i_LineOfBusinessAbbreviation,'Crime') , 'CR',
	-- IN (i_LineOfBusinessAbbreviation,'E&O') , 'ENO' ,
	-- IN (i_LineOfBusinessAbbreviation,'EPLI') , 'EPLI' ,
	-- IN (i_LineOfBusinessAbbreviation,'Garage') , 'GA' ,
	-- IN (i_LineOfBusinessAbbreviation,'Excess Liab') , 'EL' ,
	-- IN (i_LineOfBusinessAbbreviation,'GL') , 'GL' ,
	-- IN (i_LineOfBusinessAbbreviation,'WC') , 'WC' ,
	-- 'N/A')
	DECODE(TRUE,
		IN(i_LineOfBusinessAbbreviation, 'Bonds - Fidelity', 'Bonds - Surety'), 'BND',
		IN(i_LineOfBusinessAbbreviation, 'NFP D&O', 'D&O'), 'DNO',
		IN(i_LineOfBusinessAbbreviation, 'CL B&M', 'CL Mine Sub', 'CL Prop', 'Cyber Security', 'Data Compromise'), 'CF',
		IN(i_LineOfBusinessAbbreviation, 'CL IM'), 'IM',
		IN(i_LineOfBusinessAbbreviation, 'CL Umb'), 'UMB',
		IN(i_LineOfBusinessAbbreviation, 'CL Auto'), 'CA',
		IN(i_LineOfBusinessAbbreviation, 'Crime'), 'CR',
		IN(i_LineOfBusinessAbbreviation, 'E&O'), 'ENO',
		IN(i_LineOfBusinessAbbreviation, 'EPLI'), 'EPLI',
		IN(i_LineOfBusinessAbbreviation, 'Garage'), 'GA',
		IN(i_LineOfBusinessAbbreviation, 'Excess Liab'), 'EL',
		IN(i_LineOfBusinessAbbreviation, 'GL'), 'GL',
		IN(i_LineOfBusinessAbbreviation, 'WC'), 'WC',
		'N/A') AS v_LineOfBusinessAbbreviation,
	-- *INF*: DECODE(TRUE, v_LineOfBusinessAbbreviation='WC',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,substr(i_ClassCode,1,4),i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,substr(i_ClassCode,1,4),i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,substr(i_ClassCode,1,4),'99')) 
	-- ,v_LineOfBusinessAbbreviation='CA',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)), :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) ,
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99'),
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB('GA',i_ClassCode,i_RatingState)), :LKP.LKP_SupClassification_LOB('GA',i_ClassCode,i_RatingState), 
	--  :LKP.LKP_SupClassification_LOB('GA',i_ClassCode,'99'))))
	-- ,v_LineOfBusinessAbbreviation='CF',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99'))
	-- ,v_LineOfBusinessAbbreviation='BND',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='UMB',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='IM',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='CR',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='DNO',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='ENO',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='EPLI',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) 
	-- ,v_LineOfBusinessAbbreviation='EL',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99'))
	-- ,v_LineOfBusinessAbbreviation='GA',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)), :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) ,
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99')) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99'),
	-- IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB('CA',i_ClassCode,i_RatingState)), :LKP.LKP_SupClassification_LOB('CA',i_ClassCode,i_RatingState), 
	--  :LKP.LKP_SupClassification_LOB('CA',i_ClassCode,'99'))))
	-- ,v_LineOfBusinessAbbreviation='GL',  IIF( NOT ISNULL( :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState)),
	-- :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,i_RatingState) , :LKP.LKP_SupClassification_LOB(v_LineOfBusinessAbbreviation,i_ClassCode,'99'))
	-- )
	DECODE(TRUE,
		v_LineOfBusinessAbbreviation = 'WC', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_99.Result),
		v_LineOfBusinessAbbreviation = 'CA', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result, IFF(NOT LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_99.Result))),
		v_LineOfBusinessAbbreviation = 'CF', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'BND', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'UMB', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'IM', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'CR', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'DNO', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'ENO', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'EPLI', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'EL', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result),
		v_LineOfBusinessAbbreviation = 'GA', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result, IFF(NOT LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_99.Result))),
		v_LineOfBusinessAbbreviation = 'GL', IFF(NOT LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result IS NULL, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.Result, LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.Result)) AS v_Result,
	-- *INF*: DECODE(TRUE, IN( i_ProductAbbreviation,'SMART','BOP') OR IN(i_LineOfBusinessAbbreviation,'SMP','BOP') , 'Not Assigned',
	--  ISNULL(v_Result), 'N/A',
	-- SUBSTR(v_Result,1,INSTR(v_Result,'#')-1))
	DECODE(TRUE,
		IN(i_ProductAbbreviation, 'SMART', 'BOP') OR IN(i_LineOfBusinessAbbreviation, 'SMP', 'BOP'), 'Not Assigned',
		v_Result IS NULL, 'N/A',
		SUBSTR(v_Result, 1, INSTR(v_Result, '#') - 1)) AS ClassDescription,
	-- *INF*: DECODE(TRUE, IN( i_ProductAbbreviation,'SMART','BOP') OR IN(i_LineOfBusinessAbbreviation,'SMP','BOP'), 'N/A',
	-- ISNULL(v_Result), 'N/A',
	-- SUBSTR(v_Result,INSTR(v_Result,'#')+1,length(v_Result)))
	-- 
	-- 
	-- 
	DECODE(TRUE,
		IN(i_ProductAbbreviation, 'SMART', 'BOP') OR IN(i_LineOfBusinessAbbreviation, 'SMP', 'BOP'), 'N/A',
		v_Result IS NULL, 'N/A',
		SUBSTR(v_Result, INSTR(v_Result, '#') + 1, length(v_Result))) AS ClassCodeOrganizationCode
	FROM SQ_CoverageDetailDim
	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_i_RatingState
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_i_RatingState.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_i_RatingState.ClassCode = substr(i_ClassCode, 1, 4)
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_i_RatingState.RatingStateCode = i_RatingState

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_99
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_99.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_99.ClassCode = substr(i_ClassCode, 1, 4)
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_substr_i_ClassCode_1_4_99.RatingStateCode = '99'

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_i_RatingState.RatingStateCode = i_RatingState

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99
	ON LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.LineOfBusinessAbbreviation = v_LineOfBusinessAbbreviation
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATION_LOB_v_LineOfBusinessAbbreviation_i_ClassCode_99.RatingStateCode = '99'

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_i_RatingState
	ON LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_i_RatingState.LineOfBusinessAbbreviation = 'GA'
	AND LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_i_RatingState.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_i_RatingState.RatingStateCode = i_RatingState

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_99
	ON LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_99.LineOfBusinessAbbreviation = 'GA'
	AND LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__GA_i_ClassCode_99.RatingStateCode = '99'

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_i_RatingState
	ON LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_i_RatingState.LineOfBusinessAbbreviation = 'CA'
	AND LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_i_RatingState.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_i_RatingState.RatingStateCode = i_RatingState

	LEFT JOIN LKP_SUPCLASSIFICATION_LOB LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_99
	ON LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_99.LineOfBusinessAbbreviation = 'CA'
	AND LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATION_LOB__CA_i_ClassCode_99.RatingStateCode = '99'

),
UPD_CoverageDetailDim AS (
	SELECT
	CoverageDetailDimId AS o_CoverageDetailDimId, 
	ClassDescription AS o_ClassDescription, 
	ClassCodeOrganizationCode AS o_OriginatingOrganizationCode, 
	ClassDescription AS o_ISOClassDescription
	FROM EXP_ClassDesc
),
CoverageDetailDim AS (
	MERGE INTO CoverageDetailDim AS T
	USING UPD_CoverageDetailDim AS S
	ON T.CoverageDetailDimId = S.o_CoverageDetailDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ISOClassDescription = S.o_ISOClassDescription, T.ClassDescription = S.o_ClassDescription, T.ClassCodeOrganizationCode = S.o_OriginatingOrganizationCode
),