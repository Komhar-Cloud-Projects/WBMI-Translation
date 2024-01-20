WITH
SQ_DC_WC_StateTerm AS (
	SELECT
	 
	DT.HistoryID,
	DT.Type TransactionType,
	DS.Purpose,
	DS.SessionId,
	DWS.WC_StateId,
	DWST.WC_StateTermId,
	DWSTC.WC_StateTermCoverageId,
	DWS.State,WWS.EmployeeLeasing,
	WWS.EmployeeLeasingRatingOption,
	WWS.PreviousRateEffectiveDate,
	WWS.RateEffectiveDate,
	DWST.PeriodEndDate,
	DWST.PeriodStartDate,
	DWST.PeriodTerm,
	DWST.TermType,
	WWST.RiskID IntrastateRiskid,
	DWSTC.PendingRateChangeEffectiveDate,
	DWST.TotalStandardPremium,
	WWS.UnemploymentIDNumber,
	DM.Value ExperienceModificationFactorMeritRatingFactor,
	DWST.ExperienceModType,
	DM.Value ModifierValue,
	DWST.ExperienceModEffectiveDate,
	WWS.WCStateAddedThisTransaction,
	WWS.WCStateAddedThisTransactionState,
	JA.USLAndHFormsPercentage,
	WWS.EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	WWS.PremiumDiscountLevel1Factor,
	WWS.PremiumDiscountLevel2Factor,
	WWS.PremiumDiscountLevel3Factor,
	WWS.PremiumDiscountLevel4Factor,
	WWSD.PremiumDiscountAveragePercentageDiscount,
	WWS.BasisOfAuditNonComplianceCharge,
	WWS.AuditNoncomplianceChargeMultiplier,
	CASE WHEN DWS.Deleted=1 then '1' else '0' END DeletedStateFlag
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP with(nolock)
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line DL with(nolock)
	on DP.SessionId=DL.SessionId
	and DP.PolicyId=DL.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS with(nolock)
	on DP.SessionId=DS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT with(nolock)
	on DS.SessionId=DT.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_State DWS with(nolock)
	on DL.LineId=DWS.LineId
	and DL.SessionId=DWS.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_State WWS with(nolock)
	on DWS.SessionId=WWS.SessionId
	and DWS.WC_StateId=WWS.WC_StateId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_StateDefault DWSD with(nolock)
	on DWS.WC_StateId=DWSD.WC_StateId
	and DWS.SessionId=DWSD.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_StateDefault WWSD with(Nolock)
	on DWSD.WC_StateDefaultId=WWSD.WC_StateDefaultId
	and DWSD.SessionId=WWSD.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_StateTerm DWST with(nolock)
	on DWS.WC_StateId=DWST.WC_StateId
	and DWS.SessionId=DWST.SessionId
	Left Join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Modifier DM with(nolock)
	on DWST.Sessionid=DM.Sessionid
	and  DWST.WC_StateTermId=DM.Objectid
	and DM.ObjectName='DC_WC_StateTerm'
	and DM.Scope='LineStateTerm'
	and DM.Type = 'ExperienceMod'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_StateTerm WWST with(nolock)
	on DWST.WC_StateTermId=WWST.WC_StateTermId
	and DWST.SessionId=WWST.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_StateTermCoverage DWSTC with(nolock)
	on DWST.WC_StateTermId=DWSTC.WC_StateTermId
	and DWST.SessionId=DWSTC.SessionId
	
	OUTER APPLY
	(
	select 
	distinct 
	WP.PolicyNumber,
	WP.PolicyVersionFormatted,
	P.Type ParentCoverageType,
	P.Coverageid ParentCoverageid,
	C.Type ChildCoverageType,
	C.Coverageid ChildCoverageid,
	R.Description,
	DL.StateProv,
	isnull(cast (WWS.USLAndHFormsPercentage as varchar(4)),'') as USLAndHFormsPercentage
	FROM 
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage P
	on WP.SessionId=P.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage C
	on P.SessionId=C.SessionId
	and P.CoverageId=C.ObjectId
	and C.ObjectName='DC_Coverage'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_Risk R
	on P.SessionId=R.SessionId
	and P.ObjectId=R.WC_RiskId
	and P.ObjectName='DC_WC_Risk'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLA
	on R.SessionId=DLA.SessionId
	and R.WC_LocationId=DLA.ObjectId
	and DLA.ObjectName='DC_WC_Location'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Location DL
	on DLA.SessionId=DL.SessionId
	and DLA.LocationId=DL.LocationId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_WC_State DWS
	on Dl.SessionId=DWS.SessionId
	and DL.StateProv=DWS.State
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_WC_State WWS
	on DWS.SessionId=WWS.SessionId
	and DWS.WC_StateId=WWS.WC_StateId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Location DLC
	on WP.SessionId=DLC.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLCA
	on DLC.SessionId=DLCA.SessionId
	and DLC.LocationId=DLCA.LocationId
	and DLCA.LocationAssociationType='Location'
	WHERE
	C.type='USLandH'
	and WP.SessionId = DP.SessionId and DWS.WC_StateId=DWST.WC_StateId
	) JA
	
	WHERE
	DL.Type='WorkersCompensation'
	and DS.Purpose='Onset'
	and DS.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and DT.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_SRCDataCollect AS (
	SELECT
	HistoryID,
	TransactionType,
	Purpose,
	SessionId,
	WC_StateId,
	WC_StateTermId,
	WC_StateTermCoverageId,
	State,
	EmployeeLeasing,
	EmployeeLeasingRatingOption,
	PreviousRateEffectiveDate,
	RateEffectiveDate,
	PeriodEndDate,
	PeriodStartDate,
	PeriodTerm,
	TermType,
	IntrastateRiskID,
	PendingRateChangeEffectiveDate,
	TotalStandardPremium,
	UnemploymentIDNumber,
	ExperienceModificationFactorMeritRatingFactor,
	ExperienceModType,
	ModifierValue,
	ExperienceModEffectiveDate,
	WCStateAddedThisTransaction,
	WCStateAddedThisTransactionState,
	USLAndHFormsPercentage,
	EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	PremiumDiscountLevel1Factor,
	PremiumDiscountLevel2Factor,
	PremiumDiscountLevel3Factor,
	PremiumDiscountLevel4Factor,
	PremiumDiscountAveragePercentageDiscount,
	BasisOfAuditNonComplianceCharge,
	AuditNoncomplianceChargeMultiplier,
	DeletedStateFlag
	FROM SQ_DC_WC_StateTerm
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
	PolicyKey,
	HistoryID,
	Purpose
	FROM (
		SELECT 
		WorkWCTrackHistory.WCTrackHistoryID as WCTrackHistoryID, 
		WorkWCTrackHistory.Auditid as Auditid, 
		WorkWCTrackHistory.HistoryID as HistoryID, 
		WorkWCTrackHistory.Purpose as Purpose ,
		WorkWCTrackHistory.PolicyKey as PolicyKey
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
	LKP_WorkWCTrackHistory.PolicyKey AS lkp_PolicyKey,
	CURRENT_TIMESTAMP AS ExtractDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	-- *INF*: IIF(lkp_Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and (NOT ISNULL(LKP_SessionId)),'1','0')
	IFF(lkp_Auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and (LKP_SessionId IS NOT NULL), '1', '0') AS FilterFlag,
	EXP_SRCDataCollect.TransactionType,
	EXP_SRCDataCollect.WC_StateId,
	EXP_SRCDataCollect.WC_StateTermId,
	EXP_SRCDataCollect.WC_StateTermCoverageId,
	EXP_SRCDataCollect.State,
	EXP_SRCDataCollect.EmployeeLeasing,
	EXP_SRCDataCollect.EmployeeLeasingRatingOption,
	EXP_SRCDataCollect.PreviousRateEffectiveDate,
	EXP_SRCDataCollect.RateEffectiveDate,
	EXP_SRCDataCollect.PeriodEndDate,
	EXP_SRCDataCollect.PeriodStartDate,
	EXP_SRCDataCollect.PeriodTerm,
	EXP_SRCDataCollect.TermType,
	EXP_SRCDataCollect.IntrastateRiskID,
	EXP_SRCDataCollect.PendingRateChangeEffectiveDate,
	EXP_SRCDataCollect.TotalStandardPremium,
	EXP_SRCDataCollect.UnemploymentIDNumber,
	EXP_SRCDataCollect.ExperienceModificationFactorMeritRatingFactor,
	EXP_SRCDataCollect.ExperienceModType,
	EXP_SRCDataCollect.ModifierValue,
	EXP_SRCDataCollect.ExperienceModEffectiveDate,
	EXP_SRCDataCollect.WCStateAddedThisTransaction,
	EXP_SRCDataCollect.WCStateAddedThisTransactionState,
	LKP_LatestSession.SessionId AS LKP_SessionId,
	EXP_SRCDataCollect.USLAndHFormsPercentage,
	EXP_SRCDataCollect.EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	EXP_SRCDataCollect.PremiumDiscountLevel1Factor,
	EXP_SRCDataCollect.PremiumDiscountLevel2Factor,
	EXP_SRCDataCollect.PremiumDiscountLevel3Factor,
	EXP_SRCDataCollect.PremiumDiscountLevel4Factor,
	EXP_SRCDataCollect.PremiumDiscountAveragePercentageDiscount,
	EXP_SRCDataCollect.BasisOfAuditNonComplianceCharge,
	EXP_SRCDataCollect.AuditNoncomplianceChargeMultiplier,
	EXP_SRCDataCollect.DeletedStateFlag
	FROM EXP_SRCDataCollect
	LEFT JOIN LKP_LatestSession
	ON LKP_LatestSession.SessionId = EXP_SRCDataCollect.SessionId AND LKP_LatestSession.Purpose = EXP_SRCDataCollect.Purpose AND LKP_LatestSession.HistoryID = EXP_SRCDataCollect.HistoryID
	LEFT JOIN LKP_WorkWCTrackHistory
	ON LKP_WorkWCTrackHistory.HistoryID = EXP_SRCDataCollect.HistoryID AND LKP_WorkWCTrackHistory.Purpose = EXP_SRCDataCollect.Purpose
),
FIL_ExcludeSubmittedRecords AS (
	SELECT
	lkp_WCTrackHistoryID AS WCTrackHistoryID, 
	lkp_PolicyKey AS PolicyKey, 
	ExtractDate, 
	Auditid, 
	FilterFlag, 
	TransactionType, 
	WC_StateId, 
	WC_StateTermId, 
	WC_StateTermCoverageId, 
	State, 
	EmployeeLeasing, 
	EmployeeLeasingRatingOption, 
	PreviousRateEffectiveDate, 
	RateEffectiveDate, 
	PeriodEndDate, 
	PeriodStartDate, 
	PeriodTerm, 
	TermType, 
	IntrastateRiskID, 
	PendingRateChangeEffectiveDate, 
	TotalStandardPremium, 
	UnemploymentIDNumber, 
	ExperienceModificationFactorMeritRatingFactor, 
	ExperienceModType, 
	ModifierValue, 
	ExperienceModEffectiveDate, 
	WCStateAddedThisTransaction, 
	WCStateAddedThisTransactionState, 
	USLAndHFormsPercentage, 
	EmployersLiabilityCoverageEndorsementStateListExcludingOH, 
	PremiumDiscountLevel1Factor, 
	PremiumDiscountLevel2Factor, 
	PremiumDiscountLevel3Factor, 
	PremiumDiscountLevel4Factor, 
	PremiumDiscountAveragePercentageDiscount, 
	BasisOfAuditNonComplianceCharge, 
	AuditNoncomplianceChargeMultiplier, 
	DeletedStateFlag
	FROM EXP_RecordFlagging
	WHERE FilterFlag='1'
),
EXP_NewTxns AS (
	SELECT
	WCTrackHistoryID,
	PolicyKey,
	ExtractDate,
	Auditid,
	FilterFlag,
	TransactionType,
	WC_StateId,
	WC_StateTermId,
	WC_StateTermCoverageId,
	State,
	EmployeeLeasing,
	EmployeeLeasingRatingOption,
	PreviousRateEffectiveDate,
	RateEffectiveDate,
	PeriodEndDate,
	PeriodStartDate,
	PeriodTerm,
	TermType,
	IntrastateRiskID,
	PendingRateChangeEffectiveDate,
	TotalStandardPremium,
	UnemploymentIDNumber,
	ExperienceModificationFactorMeritRatingFactor,
	ExperienceModType,
	ModifierValue,
	ExperienceModEffectiveDate,
	WCStateAddedThisTransaction,
	WCStateAddedThisTransactionState,
	USLAndHFormsPercentage,
	EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	PremiumDiscountLevel1Factor,
	PremiumDiscountLevel2Factor,
	PremiumDiscountLevel3Factor,
	PremiumDiscountLevel4Factor,
	PremiumDiscountAveragePercentageDiscount,
	BasisOfAuditNonComplianceCharge,
	AuditNoncomplianceChargeMultiplier,
	DeletedStateFlag
	FROM FIL_ExcludeSubmittedRecords
),
SQ_DC_WC_State_StateFlag AS (
	Select distinct D.PolKey,DT.HistoryID,St.State ListedState,
	case when St.Deleted=1 then '1' Else '0' End as DeletedStateFlag
	from DC_WC_State ST	
	inner  join DC_Transaction DT
	on ST.SessionId=DT.SessionId
	inner join WB_Policy P
	on P.SessionId=DT.SessionId
	inner JOIN
	(Select distinct WP.PolicyNumber+WP.PolicyVersionFormatted PolKey from WB_Policy WP
	inner join DC_Transaction T with(nolock)
	on WP.SessionId=T.SessionId
	inner join DC_Line DL with(nolock)
	on T.Sessionid=DL.Sessionid
	inner join DC_Session S
	on WP.SessionID=S.SessionID
	and S.CreateDateTime between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}'
	and T.State='Committed'
	and DL.Type='WorkersCompensation'
	and S.Purpose='Onset'
	and T.State='Committed'
	@{pipeline().parameters.WHERE_CLAUSE_STATE}
	) D
	on D.PolKey=(P.PolicyNumber+P.PolicyVersionFormatted)
	where DT.State='Committed'
),
EXP_StateFlag AS (
	SELECT
	PolKey AS PolKey_StateFlag,
	HistoryID AS HistoryID_StateFlag,
	ListedState AS State_StateFlag,
	-- *INF*: LTRIM(RTRIM(State_StateFlag))
	LTRIM(RTRIM(State_StateFlag)) AS o_State_StateFlag,
	DeletedStateFlag AS Deleted_StateFlag
	FROM SQ_DC_WC_State_StateFlag
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
	QUALIFY ROW_NUMBER() OVER (PARTITION BY HistoryID,PolicyKey ORDER BY HistoryID DESC) = 1
),
EXP_Filter AS (
	SELECT
	EXP_StateFlag.PolKey_StateFlag,
	EXP_StateFlag.HistoryID_StateFlag,
	EXP_StateFlag.o_State_StateFlag AS State_StateFlag,
	EXP_StateFlag.Deleted_StateFlag,
	LKP_TrackHistory.HistoryID AS HistoryID_LKP,
	LKP_TrackHistory.PolicyKey AS PolicyKey_LKP,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(HistoryID_LKP))),'NEW','EXISTS')
	IFF(LTRIM(RTRIM(HistoryID_LKP)) IS NULL, 'NEW', 'EXISTS') AS FilterFlag
	FROM EXP_StateFlag
	LEFT JOIN LKP_TrackHistory
	ON LKP_TrackHistory.HistoryID = EXP_StateFlag.HistoryID_StateFlag AND LKP_TrackHistory.PolicyKey = EXP_StateFlag.PolKey_StateFlag
),
FIL_NewTxns AS (
	SELECT
	PolKey_StateFlag, 
	HistoryID_StateFlag, 
	State_StateFlag, 
	Deleted_StateFlag, 
	FilterFlag
	FROM EXP_Filter
	WHERE LTRIM(RTRIM(FilterFlag))='EXISTS'
),
SRT_MaxHistID AS (
	SELECT
	PolKey_StateFlag, 
	HistoryID_StateFlag, 
	State_StateFlag, 
	Deleted_StateFlag, 
	FilterFlag
	FROM FIL_NewTxns
	ORDER BY PolKey_StateFlag ASC, HistoryID_StateFlag DESC
),
EXP_ExistingTxns AS (
	SELECT
	PolKey_StateFlag,
	HistoryID_StateFlag,
	State_StateFlag,
	Deleted_StateFlag,
	-- *INF*: DECODE(TRUE,
	-- PolKey_StateFlag<>v_PriorPolicyKey,HistoryID_StateFlag,
	-- PolKey_StateFlag=v_PriorPolicyKey and HistoryID_StateFlag=v_MaxHistID,v_MaxHistID,
	-- 0)
	DECODE(
	    TRUE,
	    PolKey_StateFlag <> v_PriorPolicyKey, HistoryID_StateFlag,
	    PolKey_StateFlag = v_PriorPolicyKey and HistoryID_StateFlag = v_MaxHistID, v_MaxHistID,
	    0
	) AS v_MaxHistID,
	PolKey_StateFlag AS v_PriorPolicyKey,
	HistoryID_StateFlag AS v_PriorHistoryID,
	-- *INF*: IIF(HistoryID_StateFlag=v_MaxHistID,'1','0')
	IFF(HistoryID_StateFlag = v_MaxHistID, '1', '0') AS v_MaxHistIDFilterFlag,
	v_MaxHistIDFilterFlag AS MaxHistIDFilterFlag
	FROM SRT_MaxHistID
),
FIL_MaxHistID AS (
	SELECT
	PolKey_StateFlag, 
	HistoryID_StateFlag, 
	State_StateFlag, 
	Deleted_StateFlag, 
	MaxHistIDFilterFlag
	FROM EXP_ExistingTxns
	WHERE MaxHistIDFilterFlag='1'
),
JNR_DeletedFlag AS (SELECT
	EXP_NewTxns.WCTrackHistoryID, 
	EXP_NewTxns.PolicyKey, 
	EXP_NewTxns.ExtractDate, 
	EXP_NewTxns.Auditid, 
	EXP_NewTxns.FilterFlag, 
	EXP_NewTxns.TransactionType, 
	EXP_NewTxns.WC_StateId, 
	EXP_NewTxns.WC_StateTermId, 
	EXP_NewTxns.WC_StateTermCoverageId, 
	EXP_NewTxns.State, 
	EXP_NewTxns.EmployeeLeasing, 
	EXP_NewTxns.EmployeeLeasingRatingOption, 
	EXP_NewTxns.PreviousRateEffectiveDate, 
	EXP_NewTxns.RateEffectiveDate, 
	EXP_NewTxns.PeriodEndDate, 
	EXP_NewTxns.PeriodStartDate, 
	EXP_NewTxns.PeriodTerm, 
	EXP_NewTxns.TermType, 
	EXP_NewTxns.IntrastateRiskID, 
	EXP_NewTxns.PendingRateChangeEffectiveDate, 
	EXP_NewTxns.TotalStandardPremium, 
	EXP_NewTxns.UnemploymentIDNumber, 
	EXP_NewTxns.ExperienceModificationFactorMeritRatingFactor, 
	EXP_NewTxns.ExperienceModType, 
	EXP_NewTxns.ModifierValue, 
	EXP_NewTxns.ExperienceModEffectiveDate, 
	EXP_NewTxns.WCStateAddedThisTransaction, 
	EXP_NewTxns.WCStateAddedThisTransactionState, 
	EXP_NewTxns.USLAndHFormsPercentage, 
	EXP_NewTxns.EmployersLiabilityCoverageEndorsementStateListExcludingOH, 
	EXP_NewTxns.PremiumDiscountLevel1Factor, 
	EXP_NewTxns.PremiumDiscountLevel2Factor, 
	EXP_NewTxns.PremiumDiscountLevel3Factor, 
	EXP_NewTxns.PremiumDiscountLevel4Factor, 
	EXP_NewTxns.PremiumDiscountAveragePercentageDiscount, 
	EXP_NewTxns.BasisOfAuditNonComplianceCharge, 
	EXP_NewTxns.AuditNoncomplianceChargeMultiplier, 
	EXP_NewTxns.DeletedStateFlag AS StateDeletedFlag, 
	FIL_MaxHistID.PolKey_StateFlag, 
	FIL_MaxHistID.HistoryID_StateFlag, 
	FIL_MaxHistID.State_StateFlag, 
	FIL_MaxHistID.Deleted_StateFlag
	FROM EXP_NewTxns
	LEFT OUTER JOIN FIL_MaxHistID
	ON FIL_MaxHistID.PolKey_StateFlag = EXP_NewTxns.PolicyKey AND FIL_MaxHistID.State_StateFlag = EXP_NewTxns.State
),
EXP_Output AS (
	SELECT
	PolicyKey,
	Auditid,
	ExtractDate,
	WCTrackHistoryID,
	TransactionType,
	WC_StateId,
	State,
	EmployeeLeasing,
	EmployeeLeasingRatingOption,
	PreviousRateEffectiveDate,
	RateEffectiveDate,
	WC_StateTermId,
	PeriodStartDate,
	PeriodEndDate,
	PeriodTerm,
	TermType,
	IntrastateRiskID,
	WC_StateTermCoverageId,
	PendingRateChangeEffectiveDate,
	TotalStandardPremium,
	UnemploymentIDNumber,
	ExperienceModificationFactorMeritRatingFactor,
	ExperienceModType,
	ModifierValue,
	ExperienceModEffectiveDate,
	WCStateAddedThisTransaction,
	WCStateAddedThisTransactionState,
	USLAndHFormsPercentage,
	EmployersLiabilityCoverageEndorsementStateListExcludingOH,
	PremiumDiscountLevel1Factor,
	PremiumDiscountLevel2Factor,
	PremiumDiscountLevel3Factor,
	PremiumDiscountLevel4Factor,
	PremiumDiscountAveragePercentageDiscount,
	BasisOfAuditNonComplianceCharge,
	AuditNoncomplianceChargeMultiplier,
	StateDeletedFlag,
	-- *INF*: DECODE(TRUE,
	-- IN(LTRIM(RTRIM(TransactionType)),'Renew','New','Reissue','Rewrite') AND StateDeletedFlag='1', '0',
	-- NOT ISNULL(State_StateFlag) AND Deleted_StateFlag='0' AND StateDeletedFlag='1','1',
	-- NOT IN(LTRIM(RTRIM(TransactionType)),'Renew','New','Reissue','Rewrite') AND ISNULL(State_StateFlag) AND StateDeletedFlag='1','1',
	-- '0')
	DECODE(
	    TRUE,
	    LTRIM(RTRIM(TransactionType)) IN ('Renew','New','Reissue','Rewrite') AND StateDeletedFlag = '1', '0',
	    State_StateFlag IS NULL AND Deleted_StateFlag = '0' AND StateDeletedFlag =NOT  '1', '1',
	    NOT LTRIM(RTRIM(TransactionType)) IN ('Renew','New','Reissue','Rewrite') AND State_StateFlag IS NULL AND StateDeletedFlag = '1', '1',
	    '0'
	) AS v_StateDeletedFlag,
	v_StateDeletedFlag AS o_StateDeletedFlag,
	PolKey_StateFlag,
	HistoryID_StateFlag,
	State_StateFlag,
	Deleted_StateFlag
	FROM JNR_DeletedFlag
),
WorkWCStateTerm AS (
	TRUNCATE TABLE WorkWCStateTerm;
	INSERT INTO WorkWCStateTerm
	(Auditid, ExtractDate, WCTrackHistoryID, WC_StateId, State, EmployeeLeasing, EmployeeLeasingRatingOption, PreviousRateEffectiveDate, RateEffectiveDate, WC_StateTermId, PeriodStartDate, PeriodEndDate, PeriodTerm, TermType, IntrastateRiskid, WC_StateTermCoverageId, PendingRateChangeEffectiveDate, TotalStandardPremium, UnemploymentIDNumber, ExperienceModificationFactorMeritRatingFactor, ExperienceModType, ModifierValue, ExperienceModEffectiveDate, WCStateAddedThisTransaction, WCStateAddedThisTransactionState, USLAndHFormsPercentage, EmployersLiabilityCoverageEndorsementStateListExcludingOH, PremiumDiscountLevel1Factor, PremiumDiscountLevel2Factor, PremiumDiscountLevel3Factor, PremiumDiscountLevel4Factor, PremiumDiscountAveragePercentageDiscount, BasisOfAuditNonComplianceCharge, AuditNoncomplianceChargeMultiplier, StateDeletedFlag)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	WC_STATEID, 
	STATE, 
	EMPLOYEELEASING, 
	EMPLOYEELEASINGRATINGOPTION, 
	PREVIOUSRATEEFFECTIVEDATE, 
	RATEEFFECTIVEDATE, 
	WC_STATETERMID, 
	PERIODSTARTDATE, 
	PERIODENDDATE, 
	PERIODTERM, 
	TERMTYPE, 
	IntrastateRiskID AS INTRASTATERISKID, 
	WC_STATETERMCOVERAGEID, 
	PENDINGRATECHANGEEFFECTIVEDATE, 
	TOTALSTANDARDPREMIUM, 
	UNEMPLOYMENTIDNUMBER, 
	EXPERIENCEMODIFICATIONFACTORMERITRATINGFACTOR, 
	EXPERIENCEMODTYPE, 
	MODIFIERVALUE, 
	EXPERIENCEMODEFFECTIVEDATE, 
	WCSTATEADDEDTHISTRANSACTION, 
	WCSTATEADDEDTHISTRANSACTIONSTATE, 
	USLANDHFORMSPERCENTAGE, 
	EMPLOYERSLIABILITYCOVERAGEENDORSEMENTSTATELISTEXCLUDINGOH, 
	PREMIUMDISCOUNTLEVEL1FACTOR, 
	PREMIUMDISCOUNTLEVEL2FACTOR, 
	PREMIUMDISCOUNTLEVEL3FACTOR, 
	PREMIUMDISCOUNTLEVEL4FACTOR, 
	PREMIUMDISCOUNTAVERAGEPERCENTAGEDISCOUNT, 
	BASISOFAUDITNONCOMPLIANCECHARGE, 
	AUDITNONCOMPLIANCECHARGEMULTIPLIER, 
	o_StateDeletedFlag AS STATEDELETEDFLAG
	FROM EXP_Output
),
SRT_RemoveDuplicates AS (
	SELECT
	Auditid, 
	WCTrackHistoryID, 
	State, 
	o_StateDeletedFlag AS StateDeletedFlag
	FROM EXP_Output
	ORDER BY Auditid ASC, WCTrackHistoryID ASC, State ASC, StateDeletedFlag ASC
),
EXP_StateOutput AS (
	SELECT
	Auditid,
	CURRENT_TIMESTAMP AS ExtractDate,
	WCTrackHistoryID,
	State,
	StateDeletedFlag
	FROM SRT_RemoveDuplicates
),
WorkWCTrackHistoryState AS (
	INSERT INTO WorkWCTrackHistoryState
	(Auditid, ExtractDate, WCTrackHistoryID, State, StateDeletedFlag)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	WCTRACKHISTORYID, 
	STATE, 
	STATEDELETEDFLAG
	FROM EXP_StateOutput
),