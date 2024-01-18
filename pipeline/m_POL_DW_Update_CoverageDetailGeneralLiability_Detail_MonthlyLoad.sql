WITH
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
		  FROM SupClassificationGeneralLiability
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClassCode,SublineCode,RatingStateCode ORDER BY lkp_result) = 1
),
SQ_CoverageDetailGeneralLiability_DCT AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select distinct t.PremiumTransactionID AS PremiumTransactionID
	,RC.ClassCode AS ClassCode
	,RC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT' 
	INNER JOIN RatingCoverage RC ON PT.RatingCoverageAKId = RC.RatingCoverageAKId and PT.EffectiveDate=RC.EffectiveDate
	INNER JOIN PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT'  AND PC.InsuranceLine in ('GeneralLiability','SBOPGeneralLiability')
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='DCT'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select distinct t.PremiumTransactionID AS PremiumTransactionID
	,RC.ClassCode AS ClassCode
	,RC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT' 
	INNER JOIN RatingCoverage RC ON PT.RatingCoverageAKId = RC.RatingCoverageAKId and PT.EffectiveDate=RC.EffectiveDate
	INNER JOIN PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT'  AND PC.InsuranceLine in ('GeneralLiability','SBOPGeneralLiability')
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='DCT'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=1  
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select distinct t.PremiumTransactionID AS PremiumTransactionID
	,RC.ClassCode AS ClassCode
	,RC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT' 
	INNER JOIN RatingCoverage RC ON PT.RatingCoverageAKId = RC.RatingCoverageAKId and PT.EffectiveDate=RC.EffectiveDate
	INNER JOIN PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT'  AND PC.InsuranceLine in ('GeneralLiability','SBOPGeneralLiability')
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='DCT'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=2 
	@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select distinct t.PremiumTransactionID AS PremiumTransactionID
	,RC.ClassCode AS ClassCode
	,RC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'DCT' and T.SourceSystemID='DCT' 
	INNER JOIN RatingCoverage RC ON PT.RatingCoverageAKId = RC.RatingCoverageAKId and PT.EffectiveDate=RC.EffectiveDate
	INNER JOIN PolicyCoverage PC ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT'  AND PC.InsuranceLine in ('GeneralLiability','SBOPGeneralLiability')
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='DCT'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=3 
	@{pipeline().parameters.WHERE_CLAUSE}
),
SQ_CoverageDetailGeneralLiability_PMS AS (
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select t.PremiumTransactionID AS PremiumTransactionID
	,SC.ClassCode AS ClassCode
	,SC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,SC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	JOIN StatisticalCoverage SC ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID  AND SC.SourceSystemID = 'PMS' 
	JOIN PolicyCoverage PC ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'PMS' 
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=0
	@{pipeline().parameters.WHERE_CLAUSE}
	
	--union all
	--select t.PremiumTransactionID AS PremiumTransactionID
	--,RC.ClassCode AS ClassCode
	--,RC.SublineCode AS SublineCode
	--,rl.StateProvinceCode as StateCode
	--,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	--from PremiumTransaction PT
	--inner join CoverageDetailGeneralLiability t
	--on t.PremiumTransactionID=PT.PremiumTransactionID
	--JOIN RatingCoverage RC
	--ON PT.RatingCoverageAKId = RC.RatingCoverageAKId AND PT.SourceSystemID = 'DCT' AND RC.SourceSystemID = 'DCT'
	--and PT.EffectiveDate=RC.EffectiveDate
	--JOIN PolicyCoverage PC
	--ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT' 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select t.PremiumTransactionID AS PremiumTransactionID
	,SC.ClassCode AS ClassCode
	,SC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,SC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	JOIN StatisticalCoverage SC ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID  AND SC.SourceSystemID = 'PMS' 
	JOIN PolicyCoverage PC ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'PMS' 
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=1
	@{pipeline().parameters.WHERE_CLAUSE}
	
	--union all
	--select t.PremiumTransactionID AS PremiumTransactionID
	--,RC.ClassCode AS ClassCode
	--,RC.SublineCode AS SublineCode
	--,rl.StateProvinceCode as StateCode
	--,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	--from PremiumTransaction PT
	--inner join CoverageDetailGeneralLiability t
	--on t.PremiumTransactionID=PT.PremiumTransactionID
	--JOIN RatingCoverage RC
	--ON PT.RatingCoverageAKId = RC.RatingCoverageAKId AND PT.SourceSystemID = 'DCT' AND RC.SourceSystemID = 'DCT'
	--and PT.EffectiveDate=RC.EffectiveDate
	--JOIN PolicyCoverage PC
	--ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT' 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select t.PremiumTransactionID AS PremiumTransactionID
	,SC.ClassCode AS ClassCode
	,SC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,SC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	JOIN StatisticalCoverage SC ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID  AND SC.SourceSystemID = 'PMS' 
	JOIN PolicyCoverage PC ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'PMS' 
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=2
	@{pipeline().parameters.WHERE_CLAUSE}
	
	--union all
	--select t.PremiumTransactionID AS PremiumTransactionID
	--,RC.ClassCode AS ClassCode
	--,RC.SublineCode AS SublineCode
	--,rl.StateProvinceCode as StateCode
	--,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	--from PremiumTransaction PT
	--inner join CoverageDetailGeneralLiability t
	--on t.PremiumTransactionID=PT.PremiumTransactionID
	--JOIN RatingCoverage RC
	--ON PT.RatingCoverageAKId = RC.RatingCoverageAKId AND PT.SourceSystemID = 'DCT' AND RC.SourceSystemID = 'DCT'
	--and PT.EffectiveDate=RC.EffectiveDate
	--JOIN PolicyCoverage PC
	--ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT' 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--@{pipeline().parameters.WHERE_CLAUSE}
	
	UNION ALL
	DECLARE @Date1 DATE = DATEADD(D, -1, DATEADD(M,DATEDIFF(M,0,GETDATE())@{pipeline().parameters.NO_MONTHS},0)) 
	
	select t.PremiumTransactionID AS PremiumTransactionID
	,SC.ClassCode AS ClassCode
	,SC.SublineCode AS SublineCode
	,rl.StateProvinceCode as StateCode
	,SC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	from PremiumTransaction PT
	inner join CoverageDetailGeneralLiability t on t.PremiumTransactionID=PT.PremiumTransactionID AND PT.SourceSystemID = 'PMS' and T.SourceSystemID='PMS'
	JOIN StatisticalCoverage SC ON PT.StatisticalCoverageAKID = SC.StatisticalCoverageAKID  AND SC.SourceSystemID = 'PMS' 
	JOIN PolicyCoverage PC ON SC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'PMS' 
	inner join RiskLocation RL on RL.RiskLocationAKID = PC.RiskLocationAKID and RL.SourceSystemID='PMS'
	where @{pipeline().parameters.PCOLUMN}%@{pipeline().parameters.NUM_OF_PARTITIONS}=3
	@{pipeline().parameters.WHERE_CLAUSE}
	
	--union all
	--select t.PremiumTransactionID AS PremiumTransactionID
	--,RC.ClassCode AS ClassCode
	--,RC.SublineCode AS SublineCode
	--,rl.StateProvinceCode as StateCode
	--,RC.ClassCodeOrganizationCode AS ClassCodeOrganizationCode
	--from PremiumTransaction PT
	--inner join CoverageDetailGeneralLiability t
	--on t.PremiumTransactionID=PT.PremiumTransactionID
	--JOIN RatingCoverage RC
	--ON PT.RatingCoverageAKId = RC.RatingCoverageAKId AND PT.SourceSystemID = 'DCT' AND RC.SourceSystemID = 'DCT'
	--and PT.EffectiveDate=RC.EffectiveDate
	--JOIN PolicyCoverage PC
	--ON RC.PolicyCoverageAKID = PC.PolicyCoverageAKID AND PC.SourceSystemID = 'DCT' 
	--inner join RiskLocation RL 
	--on RL.RiskLocationAKID = PC.RiskLocationAKID
	--@{pipeline().parameters.WHERE_CLAUSE}
),
Union_DCT_PMS AS (
	SELECT PremiumTransactionID, ClassCode, SublineCode, StateCode, ClassCodeOrganizationCode
	FROM SQ_CoverageDetailGeneralLiability_DCT
	UNION
	SELECT PremiumTransactionID, ClassCode, SublineCode, StateCode, ClassCodeOrganizationCode
	FROM SQ_CoverageDetailGeneralLiability_PMS
),
EXP_MetaData AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	ClassCode AS i_ClassCode,
	SublineCode AS i_SublineCode,
	StateCode AS i_RatingStateCode,
	ClassCodeOrganizationCode AS i_OriginatingOrganizationCode,
	-- *INF*: DECODE(true,
	-- NOT ISNULL(:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,i_RatingStateCode)),:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,i_RatingStateCode),
	-- NOT ISNULL(:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,'99')),:LKP.LKP_SupClassificationGeneralLiability(i_ClassCode,i_SublineCode,'99'),
	-- 'N/A')
	DECODE(
	    true,
	    LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.lkp_result IS NOT NULL, LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.lkp_result,
	    LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.lkp_result IS NOT NULL, LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.lkp_result,
	    'N/A'
	) AS v_lkp_result,
	-- *INF*: SUBSTR(v_lkp_result,1,instr(v_lkp_result,'@1')-1)
	SUBSTR(v_lkp_result, 1, REGEXP_INSTR(v_lkp_result, '@1') - 1) AS v_ClassSummary,
	-- *INF*: SUBSTR(v_lkp_result,instr(v_lkp_result,'@1')+2,instr(v_lkp_result,'@2')-instr(v_lkp_result,'@1')-2)
	SUBSTR(v_lkp_result, REGEXP_INSTR(v_lkp_result, '@1') + 2, REGEXP_INSTR(v_lkp_result, '@2') - REGEXP_INSTR(v_lkp_result, '@1') - 2) AS v_ClassGroupCode,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	-- *INF*: IIF(length(v_ClassSummary)=0,'N/A',v_ClassSummary)
	IFF(length(v_ClassSummary) = 0, 'N/A', v_ClassSummary) AS o_ISOGeneralLiabilityClassSummary,
	-- *INF*: IIF(length(v_ClassGroupCode)=0,'N/A',v_ClassGroupCode)
	IFF(length(v_ClassGroupCode) = 0, 'N/A', v_ClassGroupCode) AS o_ISOGeneralLiabilityClassGroupCode
	FROM Union_DCT_PMS
	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.SublineCode = i_SublineCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_i_RatingStateCode.RatingStateCode = i_RatingStateCode

	LEFT JOIN LKP_SUPCLASSIFICATIONGENERALLIABILITY LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99
	ON LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.SublineCode = i_SublineCode
	AND LKP_SUPCLASSIFICATIONGENERALLIABILITY_i_ClassCode_i_SublineCode_99.RatingStateCode = '99'

),
UPD_PMS AS (
	SELECT
	o_PremiumTransactionID, 
	o_ISOGeneralLiabilityClassSummary, 
	o_ISOGeneralLiabilityClassGroupCode
	FROM EXP_MetaData
),
CoverageDetailGeneralLiability1 AS (
	MERGE INTO CoverageDetailGeneralLiability AS T
	USING UPD_PMS AS S
	ON T.PremiumTransactionID = S.o_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ISOGeneralLiabilityClassSummary = S.o_ISOGeneralLiabilityClassSummary, T.ISOGeneralLiabilityClassGroupCode = S.o_ISOGeneralLiabilityClassGroupCode
),