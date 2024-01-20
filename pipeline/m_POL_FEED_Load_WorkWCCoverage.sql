WITH
SQ_DC_Coverage AS (
	Select DT.HistoryID,
	DS.Purpose,
	DS.SessionId,
	PC.ObjectId ParentObjectId,
	PC.ObjectName ParentObjectName,
	PC.CoverageId ParentCoverageId,
	PC.Type ParentCoverageType,
	CASE WHEN DT.Type='New' and PC.Deleted <> 1 THEN '0' 
	WHEN PC.Deleted = 1 THEN '1' 
	WHEN WC.Indicator = 1 THEN '0' 
	WHEN WC.IndicatorbValue = 1 THEN '0' 
	ELSE '1' END AS ParentCoverageDeleteFlag,
	case when TAX.Type in ('SecondInjuryFund','SafetyEducationAndTrainingFund','AdministrationFund',
	'UninsuredEmployersFund','OtherTaxesAndAssessments1','StateAssessment','SecurityFundCharge','EmployerAssessment') 
	then ISNULL(Tax.Amount,0) 
	when PC.Premium=0 or PC.Premium is null then ISNULL(CC.Premium,0) else PC.Premium end ParentPremium, 
	case when TAX.Type in ('SecondInjuryFund','SafetyEducationAndTrainingFund','AdministrationFund',
	'UninsuredEmployersFund','OtherTaxesAndAssessments1','StateAssessment','SecurityFundCharge','EmployerAssessment') 
	then ISNULL(Tax.Change,0) 
	when PC.Change=0 or PC.Change is null then ISNULL(CC.Change,0) else PC.Change end ParentChange, 
	Case when TAX.Type in ('SecondInjuryFund','SafetyEducationAndTrainingFund','AdministrationFund',
	'UninsuredEmployersFund','OtherTaxesAndAssessments1','StateAssessment','SecurityFundCharge','EmployerAssessment') 
	then ISNULL(Tax.Written,0) 
	when PC.Written=0 or PC.Written is null then ISNULL(CC.Written,0) else PC.Written end ParentWritten, 
	CC.ObjectId ChildObjectId,
	CC.ObjectName ChildObjectName,
	CC.CoverageId ChildCoverageId,
	CC.Type ChildCoverageType,
	CASE WHEN CC.Deleted = 1 THEN '1' WHEN WCC.Indicator = 1 THEN '0' WHEN WCC.IndicatorbValue = 1 THEN '0' ELSE '1' END AS ChildCoverageDeleteFlag,
	WWC.TermType,
	WWC.TermRateEffectiveDate,
	WWC.PeriodStartDate,
	WWC.PeriodEndDate,
	case when TAX.Type in ('SecondInjuryFund','SafetyEducationAndTrainingFund','AdministrationFund',
	'UninsuredEmployersFund','OtherTaxesAndAssessments1','StateAssessment','SecurityFundCharge','EmployerAssessment') 
	then ISNULL(Tax.Rate,0) 
	when CC.Type in ('USL&H','USLandH','USLANDH') then ISNULL(CC.BaseRate,0) else ISNULL(PC.BaseRate,0) END BaseRate,
	WP.PolicyNumber+WP.PolicyVersionFormatted PolKey,
	PC.ID as CoverageGUID,
	ISNULL(CC.ID,'N/A') as ChildCoverageGUID,
	DT.Type as TransactionType
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage PC with(nolock)
	on DP.SessionId=PC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP with(nolock)
	on WP.SessionId=DS.SessionId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage CC with(nolock)
	on PC.SessionId=CC.SessionId
	and PC.CoverageId=CC.ObjectId
	and CC.ObjectName='DC_Coverage'
	left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Coverage WCC with(nolock)
	on CC.SessionId=WCC.SessionId
	and CC.CoverageId=WCC.CoverageId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Coverage WC with(Nolock)
	on PC.SessionId=WC.SessionId
	and PC.CoverageId=WC.CoverageId
	Left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_CoverageTerm WWC with(nolock)
	on WC.Sessionid=WWC.Sessionid
	and WC.WB_Coverageid=WWC.WB_Coverageid
	left join (select tx.Type, tx.Written, tx.Amount, tx.Change, tx.Rate, tx.SessionId, st.WC_StateTermId, ws.State TaxState
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_StateTerm st with (nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_state ws with (nolock)
	on ws.wc_stateid = st.wc_stateid
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_TaxSurcharge tx with (nolock)
	on tx.ObjectId = ws.WC_StateId
	and tx.ObjectName = 'DC_WC_State'
	where tx.type in ('SecondInjuryFund','SafetyEducationAndTrainingFund','AdministrationFund',
	'UninsuredEmployersFund','OtherTaxesAndAssessments1','StateAssessment','SecurityFundCharge','EmployerAssessment')) TAX
	on TAX.SessionId=DP.SessionId
	and PC.ObjectId=Tax.WC_StateTermId
	and PC.ObjectName='DC_WC_StateTerm'
	and Tax.Type=PC.Type
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and PC.ObjectName<>'DC_Coverage'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	Purpose,
	SessionId,
	ParentObjectId,
	ParentObjectName,
	ParentCoverageId,
	ParentCoverageType,
	ParentPremium,
	ParentChange,
	ParentWritten,
	ChildObjectId,
	ChildObjectName,
	ChildCoverageId,
	ChildCoverageType,
	TermType,
	TermRateEffectiveDate,
	PeriodStartDate,
	PeriodEndDate,
	BaseRate,
	ParentCoverageDeleteFlag,
	ChildCoverageDeleteFlag,
	PolKey,
	CoverageGUID,
	ChildCoverageGUID,
	TransactionType
	FROM SQ_DC_Coverage
),
LKP_DC_ClassCode AS (
	SELECT
	Value,
	ObjectId,
	SessionId
	FROM (
		select 
		DCC.ObjectId as ObjectId,
		DCC.SessionId as SessionId,
		Value as Value 
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_ClassCode DCC
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
		on DCC.SessionId=DL.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
		on DCC.SessionId=DS.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
		on DS.SessionId=DT.SessionId
		where DCC.ObjectName='DC_WC_Risk'
		and DL.Type='WorkersCompensation'
		and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
		and DT.State='Committed'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY Value) = 1
),
LKP_DC_StatCode AS (
	SELECT
	ClassCode,
	ObjectId,
	SessionId
	FROM (
		select 
		case when DSC.Type not in ('ExperienceModification','ExpenseConstant','PremiumDiscount') then DSC.Value else '' end as ClassCode,
		DSC.ObjectId as ObjectId,
		DSC.SessionId as SessionId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_StatCode DSC
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
		on DSC.SessionId=DL.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
		on DSC.SessionId=DS.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
		on DS.SessionId=DT.SessionId
		where DSC.ObjectName='DC_Coverage'
		and DL.Type='WorkersCompensation'
		and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
		and DT.State='Committed'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,SessionId ORDER BY ClassCode) = 1
),
LKP_IfAny_Limit AS (
	SELECT
	CoverageId,
	In_Coverageid
	FROM (
		select C.CoverageId AS CoverageId
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage C
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Limit Li
		on C.CoverageId=Li.ObjectId
		and Li.ObjectName='DC_Coverage'
		and C.SessionId=Li.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line L
		on C.SessionId=L.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction T
		on C.SessionId=T.SessionId
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session S
		on C.SessionId=S.SessionId
		where L.Type='WorkersCompensation'
		and C.Type='ManualPremium'
		and Li.Value='If Any'
		and C.Deleted<>1
		and T.State='Committed'
		and S.Purpose='Onset'
		and S.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageId ORDER BY CoverageId) = 1
),
Exp_ClassCodeDerivation AS (
	SELECT
	EXP_SRCDataCollect.HistoryID,
	EXP_SRCDataCollect.Purpose,
	EXP_SRCDataCollect.SessionId,
	EXP_SRCDataCollect.ParentObjectId,
	EXP_SRCDataCollect.ParentObjectName,
	EXP_SRCDataCollect.ParentCoverageId,
	EXP_SRCDataCollect.ParentCoverageType,
	EXP_SRCDataCollect.ParentPremium,
	EXP_SRCDataCollect.ParentChange,
	EXP_SRCDataCollect.ParentWritten,
	EXP_SRCDataCollect.ChildObjectId,
	EXP_SRCDataCollect.ChildObjectName,
	EXP_SRCDataCollect.ChildCoverageId,
	EXP_SRCDataCollect.ChildCoverageType,
	EXP_SRCDataCollect.TermType,
	EXP_SRCDataCollect.TermRateEffectiveDate,
	EXP_SRCDataCollect.PeriodStartDate,
	EXP_SRCDataCollect.PeriodEndDate,
	LKP_DC_ClassCode.Value,
	LKP_DC_StatCode.ClassCode AS Class1,
	-- *INF*: DECODE(TRUE,
	-- ParentObjectName='DC_WC_Risk',Value,
	-- NOT ISNULL(Class1),Class1,
	-- Class2)
	DECODE(
	    TRUE,
	    ParentObjectName = 'DC_WC_Risk', Value,
	    Class1 IS NOT NULL, Class1,
	    Class2
	) AS ClassCode,
	EXP_SRCDataCollect.BaseRate,
	EXP_SRCDataCollect.ParentCoverageDeleteFlag,
	-- *INF*: IIF(ISNULL(lkp_IfAny_ObjectId),ParentCoverageDeleteFlag,'0')
	IFF(lkp_IfAny_ObjectId IS NULL, ParentCoverageDeleteFlag, '0') AS o_ParentCoverageDeleteFlag,
	EXP_SRCDataCollect.ChildCoverageDeleteFlag,
	LKP_IfAny_Limit.CoverageId AS lkp_IfAny_ObjectId,
	EXP_SRCDataCollect.PolKey,
	EXP_SRCDataCollect.CoverageGUID,
	EXP_SRCDataCollect.ChildCoverageGUID,
	EXP_SRCDataCollect.TransactionType
	FROM EXP_SRCDataCollect
	LEFT JOIN LKP_DC_ClassCode
	ON LKP_DC_ClassCode.ObjectId = EXP_SRCDataCollect.ParentObjectId AND LKP_DC_ClassCode.SessionId = EXP_SRCDataCollect.SessionId
	LEFT JOIN LKP_DC_StatCode
	ON LKP_DC_StatCode.ObjectId = EXP_SRCDataCollect.ParentCoverageId AND LKP_DC_StatCode.SessionId = EXP_SRCDataCollect.SessionId
	LEFT JOIN LKP_IfAny_Limit
	ON LKP_IfAny_Limit.CoverageId = EXP_SRCDataCollect.ParentCoverageId
),
LKP_LatestSession AS (
	SELECT
	SessionId,
	Purpose,
	HistoryID
	FROM (
		Select distinct DT.HistoryID AS HistoryID,
		DS.Purpose AS Purpose,
		Max(DS.Sessionid) AS Sessionid
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
		on DT.Sessionid=DS.Sessionid
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
		on DT.Sessionid=DL.Sessionid
		where DL.Type='WorkersCompensation'
		and DS.Purpose='Onset'
		and DT.State='Committed'
		and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
		group by DT.HistoryID,DS.Purpose
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,Purpose,HistoryID ORDER BY SessionId) = 1
),
LKP_WorkWCTrackHistory AS (
	SELECT
	WCTrackHistoryID,
	Auditid,
	HistoryID,
	Purpose
	FROM (
		SELECT 
		WorkWCTrackHistory.WCTrackHistoryID as WCTrackHistoryID, 
		WorkWCTrackHistory.Auditid as Auditid, 
		WorkWCTrackHistory.HistoryID as HistoryID, 
		WorkWCTrackHistory.Purpose as Purpose 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkWCTrackHistory
		order by WorkWCTrackHistory.HistoryID,WorkWCTrackHistory.Purpose,WorkWCTrackHistory.Auditid ASC
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,Purpose ORDER BY WCTrackHistoryID) = 1
),
EXP_RecordFlagging AS (
	SELECT
	LKP_WorkWCTrackHistory.WCTrackHistoryID AS lkp_WCTrackHistoryID,
	LKP_WorkWCTrackHistory.Auditid AS lkp_Auditid,
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (NOT ISNULL(lkp_SessionId)),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AND (lkp_SessionId IS NOT NULL), '1', '0') AS FilterFlag,
	Exp_ClassCodeDerivation.ParentObjectId,
	Exp_ClassCodeDerivation.ParentObjectName,
	Exp_ClassCodeDerivation.ParentCoverageId,
	Exp_ClassCodeDerivation.ParentCoverageType,
	Exp_ClassCodeDerivation.ParentPremium,
	Exp_ClassCodeDerivation.ParentChange,
	Exp_ClassCodeDerivation.ParentWritten,
	Exp_ClassCodeDerivation.ChildObjectId,
	Exp_ClassCodeDerivation.ChildObjectName,
	Exp_ClassCodeDerivation.ChildCoverageId,
	Exp_ClassCodeDerivation.ChildCoverageType,
	Exp_ClassCodeDerivation.TermType,
	Exp_ClassCodeDerivation.TermRateEffectiveDate,
	Exp_ClassCodeDerivation.PeriodStartDate,
	Exp_ClassCodeDerivation.PeriodEndDate,
	Exp_ClassCodeDerivation.ClassCode,
	Exp_ClassCodeDerivation.BaseRate,
	LKP_LatestSession.SessionId AS lkp_SessionId,
	Exp_ClassCodeDerivation.o_ParentCoverageDeleteFlag AS ParentCoverageDeleteFlag,
	Exp_ClassCodeDerivation.ChildCoverageDeleteFlag,
	-- *INF*: IIF(ParentCoverageDeleteFlag='1','1',ChildCoverageDeleteFlag)
	IFF(ParentCoverageDeleteFlag = '1', '1', ChildCoverageDeleteFlag) AS o_ChildCoverageDeleteFlag,
	Exp_ClassCodeDerivation.PolKey,
	Exp_ClassCodeDerivation.CoverageGUID,
	Exp_ClassCodeDerivation.ChildCoverageGUID,
	Exp_ClassCodeDerivation.TransactionType
	FROM Exp_ClassCodeDerivation
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = Exp_ClassCodeDerivation.SessionId AND LKP_LatestSession.Purpose = Exp_ClassCodeDerivation.Purpose AND LKP_LatestSession.HistoryID = Exp_ClassCodeDerivation.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = Exp_ClassCodeDerivation.HistoryID AND LKP_WorkWCTrackHistory.Purpose = Exp_ClassCodeDerivation.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	ExtractDate, 
	Auditid, 
	FilterFlag, 
	ParentObjectId, 
	ParentObjectName, 
	ParentCoverageId, 
	ParentCoverageType, 
	ParentPremium, 
	ParentChange, 
	ParentWritten, 
	ChildObjectId, 
	ChildObjectName, 
	ChildCoverageId, 
	ChildCoverageType, 
	TermType, 
	TermRateEffectiveDate, 
	PeriodStartDate, 
	PeriodEndDate, 
	ClassCode, 
	BaseRate, 
	ParentCoverageDeleteFlag, 
	o_ChildCoverageDeleteFlag AS ChildCoverageDeleteFlag, 
	PolKey, 
	CoverageGUID, 
	ChildCoverageGUID, 
	TransactionType
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
SQ_DC_Coverage_Deleted AS (
	WITH HistoryID_CTE
	AS
	(
	select distinct WP.PolicyNumber,WP.PolicyVersionFormatted,pc.Id,MAX(DT.SessionId) OVER(PARTITION BY WP.PolicyNumber,WP.PolicyVersionFormatted,DT.HistoryID) MaxSessionID 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage PC with(nolock)
	on DP.SessionId=PC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP with (NOLOCK)
	on WP.SessionId=DL.SessionId
	inner join @{pipeline().parameters.DATABASE_EXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.History H with (NOLOCK)
	on H.HistoryID=DT.HistoryID
	and H.DeprecatedBy IS NULL
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage CC with(nolock)
	on PC.SessionId=CC.SessionId
	and PC.CoverageId=CC.ObjectId
	and CC.ObjectName='DC_Coverage'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Coverage WC with(Nolock)
	on PC.SessionId=WC.SessionId
	and PC.CoverageId=WC.CoverageId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and H.DeprecatedBy IS NULL
	and PC.ObjectName<>'DC_Coverage'
	and DT.State='Committed'
	and CASE WHEN DT.Type='New' and PC.Deleted <> 1 THEN '0' 
	WHEN PC.Deleted = 1 THEN '1' 
	WHEN WC.Indicator = 1 THEN '0' 
	WHEN WC.IndicatorbValue = 1 THEN '0' 
	ELSE '1' END=1
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	@{pipeline().parameters.WHERE_CLAUSE_DELETED}
	)
	
	select distinct DT.HistoryID,
	WP.PolicyNumber+WP.PolicyVersionFormatted PolicyKey,
	PC.Type ParentCoverageType,
	PC.ID as ParentCoverageGUID,
	CASE WHEN DT.Type='New' and PC.Deleted <> 1 THEN '0' 
	WHEN PC.Deleted = 1 THEN '1' 
	WHEN WC.Indicator = 1 THEN '0' 
	WHEN WC.IndicatorbValue = 1 THEN '0' 
	ELSE '1' END AS ParentCoverageDeleteFlag,
	ISNULL(CC.ID,'N/A') as ChildCoverageGUID
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage PC with(nolock)
	on DP.SessionId=PC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP with (NOLOCK)
	on WP.SessionId=DL.SessionId
	inner join HistoryID_CTE CTE
	on WP.PolicyNumber=CTE.PolicyNumber
	and WP.PolicyVersionFormatted=CTE.PolicyVersionFormatted
	and PC.ID=CTE.ID
	inner join @{pipeline().parameters.DATABASE_EXAMPLEDATA}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.History H with (NOLOCK)
	on H.HistoryID=DT.HistoryID
	and H.DeprecatedBy IS NULL
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage CC with(nolock)
	on PC.SessionId=CC.SessionId
	and PC.CoverageId=CC.ObjectId
	and CC.ObjectName='DC_Coverage'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Coverage WC with(Nolock)
	on PC.SessionId=WC.SessionId
	and PC.CoverageId=WC.CoverageId
	where DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and H.DeprecatedBy IS NULL
	and PC.ObjectName<>'DC_Coverage'
	and DT.State='Committed'
),
EXP_Source AS (
	SELECT
	HistoryID,
	PolicyKey,
	ParentCoverageType,
	ParentCoverageGUID,
	ParentCoverageDeleteFlag,
	ChildCoverageGUID
	FROM SQ_DC_Coverage_Deleted
),
LKP_TrackHistory AS (
	SELECT
	HistoryID,
	PolicyKey
	FROM (
		Select distinct HistoryID as HistoryID, PolicyKey as PolicyKey  from WorkWCTrackHistory
		where AuditID<>@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		order by 2,1--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,PolicyKey ORDER BY HistoryID) = 1
),
EXP_Filter AS (
	SELECT
	EXP_Source.HistoryID,
	EXP_Source.PolicyKey,
	EXP_Source.ParentCoverageType,
	EXP_Source.ParentCoverageGUID,
	EXP_Source.ParentCoverageDeleteFlag,
	EXP_Source.ChildCoverageGUID,
	LKP_TrackHistory.HistoryID AS HistoryID_LKP,
	LKP_TrackHistory.PolicyKey AS PolicyKey_LKP,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(HistoryID_LKP))),'NEW','EXISTS')
	IFF(LTRIM(RTRIM(HistoryID_LKP)) IS NULL, 'NEW', 'EXISTS') AS FilterFlag
	FROM EXP_Source
	LEFT JOIN LKP_TrackHistory
	ON LKP_TrackHistory.HistoryID = EXP_Source.HistoryID AND LKP_TrackHistory.PolicyKey = EXP_Source.PolicyKey
),
FIL_NewTxns AS (
	SELECT
	HistoryID, 
	PolicyKey, 
	ParentCoverageType, 
	ParentCoverageGUID, 
	ParentCoverageDeleteFlag, 
	ChildCoverageGUID, 
	FilterFlag
	FROM EXP_Filter
	WHERE LTRIM(RTRIM(FilterFlag))='EXISTS'
),
SRT_MaxHistID AS (
	SELECT
	PolicyKey, 
	HistoryID, 
	ParentCoverageType, 
	ParentCoverageGUID, 
	ParentCoverageDeleteFlag, 
	ChildCoverageGUID, 
	FilterFlag
	FROM FIL_NewTxns
	ORDER BY PolicyKey ASC, HistoryID DESC
),
EXP_ExistingTxns AS (
	SELECT
	PolicyKey,
	HistoryID,
	ParentCoverageType,
	-- *INF*: UPPER(ParentCoverageType)
	UPPER(ParentCoverageType) AS o_ParentCoverageType,
	ParentCoverageGUID,
	-- *INF*: UPPER(ParentCoverageGUID)
	UPPER(ParentCoverageGUID) AS o_ParentCoverageGUID,
	ParentCoverageDeleteFlag,
	ChildCoverageGUID,
	-- *INF*: UPPER(ChildCoverageGUID)
	UPPER(ChildCoverageGUID) AS o_ChildCoverageGUID,
	FilterFlag,
	-- *INF*: DECODE(TRUE,
	-- PolicyKey<>v_PriorPolicyKey,HistoryID,
	-- PolicyKey=v_PriorPolicyKey and HistoryID=v_MaxHistID,v_MaxHistID,
	-- 0)
	DECODE(
	    TRUE,
	    PolicyKey <> v_PriorPolicyKey, HistoryID,
	    PolicyKey = v_PriorPolicyKey and HistoryID = v_MaxHistID, v_MaxHistID,
	    0
	) AS v_MaxHistID,
	PolicyKey AS v_PriorPolicyKey,
	HistoryID AS v_PriorHistoryID,
	-- *INF*: IIF(HistoryID=v_MaxHistID,'1','0')
	IFF(HistoryID = v_MaxHistID, '1', '0') AS v_MaxHistIDFilterFlag,
	v_MaxHistIDFilterFlag AS MaxHistIDFilterFlag
	FROM SRT_MaxHistID
),
FIL_MaxHistID AS (
	SELECT
	PolicyKey, 
	HistoryID, 
	ParentCoverageType, 
	ParentCoverageGUID, 
	ParentCoverageDeleteFlag, 
	ChildCoverageGUID, 
	MaxHistIDFilterFlag
	FROM EXP_ExistingTxns
	WHERE MaxHistIDFilterFlag='1'
),
JNR_DeletedTxn AS (SELECT
	FIL_MaxHistID.PolicyKey, 
	FIL_MaxHistID.HistoryID, 
	FIL_MaxHistID.ParentCoverageType AS ParentCoverageType_Deleted, 
	FIL_MaxHistID.ParentCoverageGUID AS ParentCoverageGUID_Deleted, 
	FIL_MaxHistID.ParentCoverageDeleteFlag AS ParentCoverageDeleteFlag_Deleted, 
	FIL_MaxHistID.ChildCoverageGUID AS ChildCoverageGUID_Deleted, 
	FIL_MaxHistID.MaxHistIDFilterFlag, 
	FIL_ExcludeSubmittedRecords.WCTrackHistoryID, 
	FIL_ExcludeSubmittedRecords.ExtractDate, 
	FIL_ExcludeSubmittedRecords.Auditid, 
	FIL_ExcludeSubmittedRecords.FilterFlag, 
	FIL_ExcludeSubmittedRecords.ParentObjectId, 
	FIL_ExcludeSubmittedRecords.ParentObjectName, 
	FIL_ExcludeSubmittedRecords.ParentCoverageId, 
	FIL_ExcludeSubmittedRecords.ParentCoverageType, 
	FIL_ExcludeSubmittedRecords.ParentPremium, 
	FIL_ExcludeSubmittedRecords.ParentChange, 
	FIL_ExcludeSubmittedRecords.ParentWritten, 
	FIL_ExcludeSubmittedRecords.ChildObjectId, 
	FIL_ExcludeSubmittedRecords.ChildObjectName, 
	FIL_ExcludeSubmittedRecords.ChildCoverageId, 
	FIL_ExcludeSubmittedRecords.ChildCoverageType, 
	FIL_ExcludeSubmittedRecords.TermType, 
	FIL_ExcludeSubmittedRecords.TermRateEffectiveDate, 
	FIL_ExcludeSubmittedRecords.PeriodStartDate, 
	FIL_ExcludeSubmittedRecords.PeriodEndDate, 
	FIL_ExcludeSubmittedRecords.ClassCode, 
	FIL_ExcludeSubmittedRecords.StatCode, 
	FIL_ExcludeSubmittedRecords.BaseRate, 
	FIL_ExcludeSubmittedRecords.ParentCoverageDeleteFlag, 
	FIL_ExcludeSubmittedRecords.ChildCoverageDeleteFlag, 
	FIL_ExcludeSubmittedRecords.PolKey, 
	FIL_ExcludeSubmittedRecords.CoverageGUID, 
	FIL_ExcludeSubmittedRecords.ChildCoverageGUID, 
	FIL_ExcludeSubmittedRecords.TransactionType
	FROM FIL_ExcludeSubmittedRecords
	LEFT OUTER JOIN FIL_MaxHistID
	ON FIL_MaxHistID.PolicyKey = FIL_ExcludeSubmittedRecords.PolKey AND FIL_MaxHistID.ParentCoverageType = FIL_ExcludeSubmittedRecords.ParentCoverageType AND FIL_MaxHistID.ParentCoverageGUID = FIL_ExcludeSubmittedRecords.CoverageGUID AND FIL_MaxHistID.ChildCoverageGUID = FIL_ExcludeSubmittedRecords.ChildCoverageGUID
),
EXP_TARGET AS (
	SELECT
	PolicyKey,
	HistoryID,
	ParentCoverageType_Deleted,
	ParentCoverageGUID_Deleted,
	ParentCoverageDeleteFlag_Deleted,
	ChildCoverageGUID_Deleted,
	MaxHistIDFilterFlag,
	WCTrackHistoryID,
	ExtractDate,
	Auditid,
	FilterFlag,
	ParentObjectId,
	ParentObjectName,
	ParentCoverageId,
	ParentCoverageType,
	ParentPremium,
	ParentChange,
	ParentWritten,
	ChildObjectId,
	ChildObjectName,
	ChildCoverageId,
	ChildCoverageType,
	TermType,
	TermRateEffectiveDate,
	PeriodStartDate,
	PeriodEndDate,
	ClassCode,
	StatCode,
	BaseRate,
	ParentCoverageDeleteFlag,
	ChildCoverageDeleteFlag,
	PolKey,
	CoverageGUID,
	ChildCoverageGUID,
	TransactionType,
	-- *INF*: DECODE(TRUE,
	-- ParentCoverageDeleteFlag_Deleted='1','1',
	-- IN(TransactionType,'New','Reissue','Renew','Rewrite') AND ParentCoverageDeleteFlag='1','1',
	-- '0')
	DECODE(
	    TRUE,
	    ParentCoverageDeleteFlag_Deleted = '1', '1',
	    TransactionType IN ('New','Reissue','Renew','Rewrite') AND ParentCoverageDeleteFlag = '1', '1',
	    '0'
	) AS Filter_Flag
	FROM JNR_DeletedTxn
),
FIL_Output AS (
	SELECT
	PolicyKey, 
	HistoryID, 
	ParentCoverageType_Deleted, 
	ParentCoverageGUID_Deleted, 
	ParentCoverageDeleteFlag_Deleted, 
	ChildCoverageGUID_Deleted, 
	MaxHistIDFilterFlag, 
	WCTrackHistoryID, 
	ExtractDate, 
	Auditid, 
	FilterFlag, 
	ParentObjectId, 
	ParentObjectName, 
	ParentCoverageId, 
	ParentCoverageType, 
	ParentPremium, 
	ParentChange, 
	ParentWritten, 
	ChildObjectId, 
	ChildObjectName, 
	ChildCoverageId, 
	ChildCoverageType, 
	TermType, 
	TermRateEffectiveDate, 
	PeriodStartDate, 
	PeriodEndDate, 
	ClassCode, 
	StatCode, 
	BaseRate, 
	ParentCoverageDeleteFlag, 
	ChildCoverageDeleteFlag, 
	PolKey, 
	CoverageGUID, 
	ChildCoverageGUID, 
	Filter_Flag
	FROM EXP_TARGET
	WHERE Filter_Flag='0'
),
WorkWCCoverage AS (
	TRUNCATE TABLE WorkWCCoverage;
	INSERT INTO WorkWCCoverage
	(Auditid, ExtractDate, WCTrackHistoryID, ParentObjectId, ParentObjectName, ParentCoverageId, ParentCoverageType, ParentCoverageDeleteFlag, Premium, Change, Written, ChildObjectName, ChildCoverageId, ChildCoverageType, ChildCoverageDeleteFlag, CoverageTermType, CoverageTermRateEffectiveDate, PeriodStartDate, PeriodEndDate, ClassCode, StatCode, BaseRate)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	PARENTOBJECTID, 
	PARENTOBJECTNAME, 
	PARENTCOVERAGEID, 
	PARENTCOVERAGETYPE, 
	PARENTCOVERAGEDELETEFLAG, 
	ParentPremium AS PREMIUM, 
	ParentChange AS CHANGE, 
	ParentWritten AS WRITTEN, 
	CHILDOBJECTNAME, 
	CHILDCOVERAGEID, 
	CHILDCOVERAGETYPE, 
	CHILDCOVERAGEDELETEFLAG, 
	TermType AS COVERAGETERMTYPE, 
	TermRateEffectiveDate AS COVERAGETERMRATEEFFECTIVEDATE, 
	PERIODSTARTDATE, 
	PERIODENDDATE, 
	CLASSCODE, 
	STATCODE, 
	BASERATE
	FROM FIL_Output
),