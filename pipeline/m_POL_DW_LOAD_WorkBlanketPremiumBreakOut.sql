WITH
SQ_WorkBlanketPremiumBreakOut AS (
	with t_CF as (
	select p.SessionId,
	rsk.CF_RiskId,
	p.CF_PropertyId,
	c.CoverageId BlanketCoverageId,
	pt.PremiumTransactionAKID BlanketPremiumTransactionAKId,
	pm.PremiumTransactionAKId,
	pm.EffectiveDate,
	pm.RatingCoverageAKId,
	cov.CoverageId,
	rsk.RiskType SourceRiskType,
	cov.type SourceCoverageType,
	convert(varchar(255),frm.Value) BlanketType,
	case when ce.Prem is not null then ce.Prem when cov.Type='BG1' then p.BG1PremRatingGroup
	when cov.Type='BG2' then p.BG2PremRatingGroup when cov.Type='OtherPerils' then p.OtherPremRatingGroup end BreakOutNumerator,
	pt.PremiumTransactionAmount as TotalBlanketWrittenPremium
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFPropertyStaging p
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFRiskStaging rsk
	on rsk.CF_RiskId = p.CF_RiskId
	and not rsk.RiskType='EETOOLS'
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging cov
	on cov.ObjectId=rsk.CF_RiskId
	and cov.ObjectName='DC_CF_Risk'
	and cov.Type in ('BG1','BG2','OtherPerils','EarthQuakeRisk')
	and (not exists (
	select 1 from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging c
	where c.ObjectId=cov.Objectid
	and c.ObjectName='DC_CF_Risk'
	and c.Type in ('BG1','BG2','OtherPerils')
	and c.Premium<>0)
	or cov.Type='EarthQuakeRisk')
	left hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFCoverageEarthquakeRiskStage ce
	on ce.CoverageId=cov.CoverageId
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCFRatingGroupStaging rg
	on rg.CF_RiskId = rsk.CF_RiskId
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCFormStaging frm
	on frm.ObjectId = rg.CF_RatingGroupId
	and frm.ObjectName = 'DC_CF_RatingGroup'
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging c on p.SessionId = c.SessionId and c.ObjectName = 'DC_Line' and c.Type = 'RatingGroup'
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction w
	on w.PremiumTransactionStageId=c.CoverageId
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt
	on pt.PremiumTransactionAKID=w.PremiumTransactionAKId
	and pt.SourceSystemID='DCT'
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction wp
	on wp.PremiumTransactionStageId=cov.CoverageId
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pm
	on pm.PremiumTransactionAKID=wp.PremiumTransactionAKId
	and pm.SourceSystemID='DCT'
	and pm.OffsetOnsetCode<>'Offset'),
	
	t_BO as (
	select rsk.SessionId,
	rsk.BPRiskId,
	cov.CoverageId,
	pt.PremiumTransactionAKID BlanketPremiumTransactionAKID,
	pm.PremiumTransactionAKID,
	pm.EffectiveDate,
	pm.RatingCoverageAKId,
	cov.type SourceCoverageType,
	pt.PremiumTransactionAmount TotalBlanketWrittenPremium,
	COALESCE(cb.BlanketPremium,cp.BlanketPremium,ISNULL(ce.BlanketBuildingPremium,0)+ISNULL(ce.BlanketPersonalPropertyPremium,0)) BreakOutNumerator
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPRiskStage rsk
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging c on c.SessionId = rsk.SessionId and c.Type = 'Blanket' and c.ObjectName = 'DC_Line'
	and exists (
	select 1 from DCBPBlanketGroupStage bg
	where bg.BP_RiskId=rsk.BPRiskId)
	inner hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCoverageStaging cov
	on rsk.BPRiskId=cov.ObjectId
	and cov.ObjectName='DC_BP_Risk' 
	and cov.Type in ('Building','PersonalProperty','EarthQuake') 
	and (ISNULL(cov.Premium,0)=0
	or cov.Type='EarthQuake')
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction w
	on w.PremiumTransactionStageId=c.CoverageId
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pt
	on pt.PremiumTransactionAKID=w.PremiumTransactionAKId
	and pt.SourceSystemID='DCT'
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction wp
	on wp.PremiumTransactionStageId=cov.CoverageId
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction pm
	on pm.PremiumTransactionAKID=wp.PremiumTransactionAKId
	and pm.SourceSystemID='DCT'
	and pm.OffsetOnsetCode<>'Offset'
	left hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPCoverageBuildingStage cb
	on cb.CoverageId=cov.CoverageId
	left hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPCoveragePersonalPropertyStage cp
	on cp.CoverageId=cov.CoverageId
	left hash join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPCoverageEarthQuakeStage ce
	on ce.CoverageId=cov.CoverageId
	where COALESCE(cb.BlanketPremium,cp.BlanketPremium,ISNULL(ce.BlanketPersonalPropertyPremium,0)+ISNULL(ce.BlanketBuildingPremium,0),0)<>0)
	
	select t_CF.BlanketPremiumTransactionAKId, t_CF.PremiumTransactionAKId, t_cf.SourceCoverageType, t_cf.BreakOutNumerator, t_cf.TotalBlanketWrittenPremium,a.BreakOutDenominator,
	RC.AnnualStatementLineId
	 from t_CF
	inner hash join
	(select BlanketPremiumTransactionAKId,sum(BreakOutNumerator) BreakOutDenominator from 
	(select distinct BlanketPremiumTransactionAKId,CoverageId,BreakOutNumerator from t_CF) a
	group by BlanketPremiumTransactionAKId
	having sum(BreakOutNumerator)<>0) a
	on t_CF.BlanketPremiumTransactionAKId=a.BlanketPremiumTransactionAKId
	and t_CF.BreakOutNumerator is not null
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC 
	on t_CF.RatingCoverageAKId = RC.RatingCoverageAKID and t_CF.EffectiveDate =RC.EffectiveDate
	UNION ALL
	
	select t_BO.BlanketPremiumTransactionAKID, t_BO.PremiumTransactionAKID, t_BO.SourceCoverageType, t_BO.BreakOutNumerator, T_BO.TotalBlanketWrittenPremium,a.BreakOutDenominator,
	RC.AnnualStatementLineId
	from t_BO
	inner hash join 
	(select BlanketPremiumTransactionAKId,sum(BreakOutNumerator) BreakOutDenominator  from 
	(select distinct BlanketPremiumTransactionAKId,CoverageId,BreakOutNumerator from t_BO) a
	group by BlanketPremiumTransactionAKId
	having sum(BreakOutNumerator)<>0) a
	on t_BO.BlanketPremiumTransactionAKID=a.BlanketPremiumTransactionAKID
	inner hash join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC 
	on t_BO.RatingCoverageAKId = RC.RatingCoverageAKID and t_BO.EffectiveDate =RC.EffectiveDate
),
EXP_Values AS (
	SELECT
	BlanketPremiumTransactionAKId,
	PremiumTransactionAKId,
	SourceCoverageType,
	BreakOutNumerator,
	TotalBlanketWrittenPremium,
	BreakOutDenominator,
	AnnualStatementLineId,
	-- *INF*: ROUND(TotalBlanketWrittenPremium*BreakOutNumerator/BreakOutDenominator,4)
	ROUND(TotalBlanketWrittenPremium * BreakOutNumerator / BreakOutDenominator, 4
	) AS o_BreakOutPremium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	SYSDATE AS o_CreatedDate
	FROM SQ_WorkBlanketPremiumBreakOut
),
LKP_Exist AS (
	SELECT
	WorkBlanketPremiumBreakOutId,
	BlanketPremiumTransactionAKID,
	PremiumTransactionAKId
	FROM (
		SELECT 
			WorkBlanketPremiumBreakOutId,
			BlanketPremiumTransactionAKID,
			PremiumTransactionAKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkBlanketPremiumBreakOut
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY BlanketPremiumTransactionAKID,PremiumTransactionAKId ORDER BY WorkBlanketPremiumBreakOutId) = 1
),
RTR_InsertUpdate AS (
	SELECT
	LKP_Exist.WorkBlanketPremiumBreakOutId,
	EXP_Values.BlanketPremiumTransactionAKId,
	EXP_Values.PremiumTransactionAKId,
	EXP_Values.SourceCoverageType,
	EXP_Values.BreakOutNumerator,
	EXP_Values.TotalBlanketWrittenPremium,
	EXP_Values.BreakOutDenominator,
	EXP_Values.AnnualStatementLineId,
	EXP_Values.o_BreakOutPremium AS BreakOutPremium,
	EXP_Values.o_AuditID AS AuditID,
	EXP_Values.o_CreatedDate AS CreatedDate
	FROM EXP_Values
	LEFT JOIN LKP_Exist
	ON LKP_Exist.BlanketPremiumTransactionAKID = EXP_Values.BlanketPremiumTransactionAKId AND LKP_Exist.PremiumTransactionAKId = EXP_Values.PremiumTransactionAKId
),
RTR_InsertUpdate_INSERT AS (SELECT * FROM RTR_InsertUpdate WHERE ISNULL(WorkBlanketPremiumBreakOutId)),
WorkBlanketPremiumBreakOut_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkBlanketPremiumBreakOut
	(AuditId, CreatedDate, BlanketPremiumTransactionAKID, PremiumTransactionAKId, AnnualStatementLineId, SourceCoverageType, TotalBlanketPremium, BreakOutNumerator, BreakOutDenominator, BreakOutPremium)
	SELECT 
	AuditID AS AUDITID, 
	CREATEDDATE, 
	BlanketPremiumTransactionAKId AS BLANKETPREMIUMTRANSACTIONAKID, 
	PREMIUMTRANSACTIONAKID, 
	ANNUALSTATEMENTLINEID, 
	SOURCECOVERAGETYPE, 
	TotalBlanketWrittenPremium AS TOTALBLANKETPREMIUM, 
	BREAKOUTNUMERATOR, 
	BREAKOUTDENOMINATOR, 
	BREAKOUTPREMIUM
	FROM RTR_InsertUpdate_INSERT
),