WITH
SQ_WB_CL_Policy AS (
	WITH cte_WBCLPolicy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_PolicyId, 
	X.WB_CL_PolicyId, 
	X.SessionId, 
	X.ApplicantBuildingsMoreThan2Apartments, 
	X.OperationType, 
	X.OperationTypeNext30Days, 
	X.DescriptionOfChildCarePremises, 
	X.ChildCarePremisesOther, 
	X.OperationTypePersonalAppearance, 
	X.OperationTypeNext30DaysPersonalAppearance, 
	X.DoNotAutomaticallyRenewThisPolicy, 
	X.AlteredTermsRequired, 
	X.SMARTIndicator, 
	X.AuditType, 
	X.AuditTypePolicyPeriodOverride, 
	X.AuditTypePermanentOverride, 
	X.AuditTypeOverrideReason, 
	X.AssignedAuditor, 
	X.AssignedAuditorPolicyPeriodOverride, 
	X.AssignedAuditorPermanentOverride, 
	X.AssignedAuditorOverrideReason, 
	X.CloseAudit, 
	X.CloseAuditReason, 
	X.AuditPriority, 
	X.HasCorrespondingFrontingPolicy, 
	X.FrontingPolicyNumber, 
	X.FrontingPolicyPremium, 
	X.AssignedAuditorUserId, 
	X.IsApplicant, 
	X.TermReason, 
	X.LPService, 
	X.IncludePolicy, 
	X.MailPolicyToInsured, 
	X.TaskFlagNewPolicyWCPoolWI, 
	X.TaskFlagPolicyBound, 
	X.TaskFlagPolicyIssued, 
	X.TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold, 
	X.TaskFlagPolicyCLTotalAboveThreshold, 
	X.TaskFlagPolicyNSITotalAboveThreshold, 
	X.TaskFlagFormSelectedWB2035, 
	X.TaskFlagFormSelectedWB2044C, 
	X.TaskFlagFormSelectedWB1514, 
	X.TaskFlagFormSelectedWB1078, 
	X.TaskFlagAuditPolicyCancelled, 
	X.TaskFlagAuditPolicyCancelledWCPoolArgent, 
	X.TaskFlagAuditPolicyReinstated, 
	X.TaskFlagAuditRequiredMidTerm, 
	X.TaskFlagAuditRequiredPremliminary, 
	X.TaskFlagCancelNonPayReinsurance, 
	X.TaskFlagCancelReinsurance, 
	X.TaskFlagFormSelectedNW0034, 
	X.TaskFlagFormSelectedNW0044, 
	X.TaskFlagFormSelectedWB1558, 
	X.TaskFlagFormSelectedWB2115, 
	X.TaskFlagFormSelectedWB2138, 
	X.TaskFlagFormSelectedWB2185, 
	X.TaskFlagFormSelectedWB345, 
	X.TaskFlagFormSelectedWB610, 
	X.TaskFlagNewPolicy, 
	X.TaskFlagPolicyCanceledWithFiling, 
	X.TaskFlagPolicyCanceledWithNonPayFiling, 
	X.TaskFlagPolicyCoverageRemovedWithFiling, 
	X.TaskFlagPolicyEndorsement, 
	X.TaskFlagPolicyFinalAudit, 
	X.TaskFlagPolicyFinalAuditWithCustomerCare, 
	X.TaskFlagPolicyFinalAuditWithReinsurer, 
	X.TaskFlagPolicyReissue, 
	X.TaskFlagPolicyRenewal, 
	X.TaskFlagPolicyRewrite, 
	X.TaskFlagPolicySignatureRequiredIteration, 
	X.TaskFlagPolicyWithRiskGradeDNW, 
	X.TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold, 
	X.TaskFlagPolicyWithReinsuranceAndCertificateReceived, 
	X.TaskFlagFormSelectedWB1214, 
	X.TaskFlagFormSelectedWB1214IA, 
	X.TaskFlagFormSelectedWB1930, 
	X.TaskFlagFormSelectedWB2580, 
	X.NoteFlagLPServicesOrderedOnIssue, 
	X.InitialPendingOnTransaction, 
	X.StoreInfoAnswersOnApprove, 
	X.StoreReferralAnswers, 
	X.ReinsuranceApplied, 
	X.ReinsuranceRemoved, 
	X.AuditablePremium, 
	X.SBOPIndicator, 
	X.CrimeIndicator, 
	X.ReleasedDate, 
	X.ReleasedDateFirst, 
	X.NoticeOfCancellationDays, 
	X.IsNewQuoteWCPool, 
	X.NAICSHasBeenVisited, 
	X.PolicyAuditablePremium, 
	X.PremiumDuplicatePath, 
	X.LPServiceFlag, 
	X.IncludePolicyFlag, 
	X.FinalAuditDueDate, 
	X.ApplyPropertyTransition, 
	X.RenewalCounter, 
	X.ASignedOhioFraudStatement, 
	X.ExpiredReason, 
	X.ExpiredReasonDetails, 
	X.RejectedReason, 
	X.RejectedReasonDetails, 
	X.Indicator, 
	X.DandOPremium, 
	X.LiquorLiabilityPremium, 
	X.HideDandO, 
	X.HideLiquorLiability, 
	X.ShowBlackBoxRating, 
	X.TotalPolicyQuotedPremium, 
	X.TotalLineQuotedPremium, 
	X.TaskFlagLPServiceRequestArgent, 
	X.TaskFlagLPServiceRequestNSI, 
	X.TaskFlagLPServiceScheduledPolicyCancel, 
	X.TaskFlagLPServiceScheduledPolicyCancelNonpayment, 
	X.SAFER, 
	X.USDOTNumber, 
	X.FilingName, 
	X.FederalOperatingAuthority, 
	X.FEIN, 
	X.FilingType, 
	X.FederalStateLimit, 
	X.OtherLimit, 
	X.OtherLimitComments, 
	X.MC90Only, 
	X.CommoditiesDescription, 
	X.CommoditiesLimit, 
	X.InterstateBMC91X, 
	X.FormType, 
	X.IntrastateFormEEX, 
	X.IntrastateFormEEXHaulingStates, 
	X.WIHumanServices, 
	X.WISchoolBuss, 
	X.OHHaulingPermit, 
	X.OHLimit, 
	X.OHOtherLimit, 
	X.OHOtherLimitComments, 
	X.InterstateBMC34, 
	X.IntrastateFormH, 
	X.IntrastateFormHHaulingStates, 
	X.Effective, 
	X.FilingNameMessage, 
	X.MC90FSLimitLessThanCommLimitMessage, 
	X.Status, 
	X.NameCheckMessage, 
	X.Deleted, 
	X.IsAtCollections, 
	X.TaskFlagCancelPending, 
	X.TaskFlagCancelPendingArgent, 
	X.PreviousEffectiveDate, 
	X.TaskFlagConversionProcessRenewal, 
	X.TaskFlagPolicySMARTTotalAboveThreshold, 
	X.CATotalLimitForReinsuranceTask, 
	X.SMARTTotalLimitForReinsuranceTask, 
	X.EstimatedQuotePremium,
	X.NoncomplianceofWCPoolAudit 
	FROM
	WB_CL_Policy X
	inner join
	cte_WBCLPolicy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_PolicyId,
	WB_CL_PolicyId,
	SessionId,
	ApplicantBuildingsMoreThan2Apartments AS i_ApplicantBuildingsMoreThan2Apartments,
	-- *INF*: DECODE(i_ApplicantBuildingsMoreThan2Apartments, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ApplicantBuildingsMoreThan2Apartments,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ApplicantBuildingsMoreThan2Apartments,
	OperationType,
	OperationTypeNext30Days,
	DescriptionOfChildCarePremises,
	ChildCarePremisesOther,
	OperationTypePersonalAppearance AS i_OperationTypePersonalAppearance,
	-- *INF*: DECODE(i_OperationTypePersonalAppearance, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_OperationTypePersonalAppearance,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_OperationTypePersonalAppearance,
	OperationTypeNext30DaysPersonalAppearance AS i_OperationTypeNext30DaysPersonalAppearance,
	-- *INF*: DECODE(i_OperationTypeNext30DaysPersonalAppearance, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_OperationTypeNext30DaysPersonalAppearance,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_OperationTypeNext30DaysPersonalAppearance,
	DoNotAutomaticallyRenewThisPolicy AS i_DoNotAutomaticallyRenewThisPolicy,
	-- *INF*: DECODE(i_DoNotAutomaticallyRenewThisPolicy, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_DoNotAutomaticallyRenewThisPolicy,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_DoNotAutomaticallyRenewThisPolicy,
	AlteredTermsRequired AS i_AlteredTermsRequired,
	-- *INF*: DECODE(i_AlteredTermsRequired, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AlteredTermsRequired,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AlteredTermsRequired,
	SMARTIndicator AS i_SMARTIndicator,
	-- *INF*: DECODE(i_SMARTIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_SMARTIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_SMARTIndicator,
	AuditType,
	AuditTypePolicyPeriodOverride AS i_AuditTypePolicyPeriodOverride,
	-- *INF*: DECODE(i_AuditTypePolicyPeriodOverride, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AuditTypePolicyPeriodOverride,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AuditTypePolicyPeriodOverride,
	AuditTypePermanentOverride AS i_AuditTypePermanentOverride,
	-- *INF*: DECODE(i_AuditTypePermanentOverride, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AuditTypePermanentOverride,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AuditTypePermanentOverride,
	AuditTypeOverrideReason,
	AssignedAuditor,
	AssignedAuditorPolicyPeriodOverride AS i_AssignedAuditorPolicyPeriodOverride,
	-- *INF*: DECODE(i_AssignedAuditorPolicyPeriodOverride, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AssignedAuditorPolicyPeriodOverride,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AssignedAuditorPolicyPeriodOverride,
	AssignedAuditorPermanentOverride AS i_AssignedAuditorPermanentOverride,
	-- *INF*: DECODE(i_AssignedAuditorPermanentOverride, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_AssignedAuditorPermanentOverride,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AssignedAuditorPermanentOverride,
	AssignedAuditorOverrideReason,
	CloseAudit AS i_CloseAudit,
	-- *INF*: DECODE(i_CloseAudit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_CloseAudit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_CloseAudit,
	CloseAuditReason,
	AuditPriority,
	HasCorrespondingFrontingPolicy AS i_HasCorrespondingFrontingPolicy,
	-- *INF*: DECODE(i_HasCorrespondingFrontingPolicy, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_HasCorrespondingFrontingPolicy,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_HasCorrespondingFrontingPolicy,
	FrontingPolicyNumber,
	FrontingPolicyPremium,
	AssignedAuditorUserId,
	IsApplicant,
	TermReason,
	LPService,
	IncludePolicy AS i_IncludePolicy,
	-- *INF*: DECODE(i_IncludePolicy, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IncludePolicy,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncludePolicy,
	MailPolicyToInsured AS i_MailPolicyToInsured,
	-- *INF*: DECODE(i_MailPolicyToInsured, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_MailPolicyToInsured,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MailPolicyToInsured,
	TaskFlagNewPolicyWCPoolWI AS i_TaskFlagNewPolicyWCPoolWI,
	-- *INF*: DECODE(i_TaskFlagNewPolicyWCPoolWI, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagNewPolicyWCPoolWI,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagNewPolicyWCPoolWI,
	TaskFlagPolicyBound AS i_TaskFlagPolicyBound,
	-- *INF*: DECODE(i_TaskFlagPolicyBound, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyBound,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyBound,
	TaskFlagPolicyIssued AS i_TaskFlagPolicyIssued,
	-- *INF*: DECODE(i_TaskFlagPolicyIssued, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyIssued,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyIssued,
	-- *INF*: DECODE(i_TaskFlagPolicySignatureRequired, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicySignatureRequired,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicySignatureRequired,
	TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold AS i_TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold,
	-- *INF*: DECODE(i_TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold,
	TaskFlagPolicyCLTotalAboveThreshold AS i_TaskFlagPolicyCLTotalAboveThreshold,
	-- *INF*: DECODE(i_TaskFlagPolicyCLTotalAboveThreshold, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyCLTotalAboveThreshold,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyCLTotalAboveThreshold,
	TaskFlagPolicyNSITotalAboveThreshold AS i_TaskFlagPolicyNSITotalAboveThreshold,
	-- *INF*: DECODE(i_TaskFlagPolicyNSITotalAboveThreshold, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyNSITotalAboveThreshold,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyNSITotalAboveThreshold,
	TaskFlagFormSelectedWB2035 AS i_TaskFlagFormSelectedWB2035,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB2035, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB2035,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB2035,
	TaskFlagFormSelectedWB2044C AS i_TaskFlagFormSelectedWB2044C,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB2044C, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB2044C,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB2044C,
	TaskFlagFormSelectedWB1514 AS i_TaskFlagFormSelectedWB1514,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB1514, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB1514,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB1514,
	TaskFlagFormSelectedWB1078 AS i_TaskFlagFormSelectedWB1078,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB1078, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB1078,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB1078,
	TaskFlagAuditPolicyCancelled AS i_TaskFlagAuditPolicyCancelled,
	-- *INF*: DECODE(i_TaskFlagAuditPolicyCancelled, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagAuditPolicyCancelled,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagAuditPolicyCancelled,
	TaskFlagAuditPolicyCancelledWCPoolArgent AS i_TaskFlagAuditPolicyCancelledWCPoolArgent,
	-- *INF*: DECODE(i_TaskFlagAuditPolicyCancelledWCPoolArgent, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagAuditPolicyCancelledWCPoolArgent,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagAuditPolicyCancelledWCPoolArgent,
	TaskFlagAuditPolicyReinstated AS i_TaskFlagAuditPolicyReinstated,
	-- *INF*: DECODE(i_TaskFlagAuditPolicyReinstated, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagAuditPolicyReinstated,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagAuditPolicyReinstated,
	TaskFlagAuditRequiredMidTerm AS i_TaskFlagAuditRequiredMidTerm,
	-- *INF*: DECODE(i_TaskFlagAuditRequiredMidTerm, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagAuditRequiredMidTerm,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagAuditRequiredMidTerm,
	TaskFlagAuditRequiredPremliminary AS i_TaskFlagAuditRequiredPremliminary,
	-- *INF*: DECODE(i_TaskFlagAuditRequiredPremliminary, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagAuditRequiredPremliminary,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagAuditRequiredPremliminary,
	TaskFlagCancelNonPayReinsurance AS i_TaskFlagCancelNonPayReinsurance,
	-- *INF*: DECODE(i_TaskFlagCancelNonPayReinsurance, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagCancelNonPayReinsurance,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagCancelNonPayReinsurance,
	TaskFlagCancelReinsurance AS i_TaskFlagCancelReinsurance,
	-- *INF*: DECODE(i_TaskFlagCancelReinsurance, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagCancelReinsurance,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagCancelReinsurance,
	TaskFlagFormSelectedNW0034 AS i_TaskFlagFormSelectedNW0034,
	-- *INF*: DECODE(i_TaskFlagFormSelectedNW0034, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedNW0034,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedNW0034,
	TaskFlagFormSelectedNW0044 AS i_TaskFlagFormSelectedNW0044,
	-- *INF*: DECODE(i_TaskFlagFormSelectedNW0044, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedNW0044,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedNW0044,
	TaskFlagFormSelectedWB1558 AS i_TaskFlagFormSelectedWB1558,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB1558, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB1558,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB1558,
	TaskFlagFormSelectedWB2115 AS i_TaskFlagFormSelectedWB2115,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB2115, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB2115,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB2115,
	TaskFlagFormSelectedWB2138 AS i_TaskFlagFormSelectedWB2138,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB2138, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB2138,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB2138,
	TaskFlagFormSelectedWB2185 AS i_TaskFlagFormSelectedWB2185,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB2185, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB2185,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB2185,
	TaskFlagFormSelectedWB345 AS i_TaskFlagFormSelectedWB345,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB345, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB345,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB345,
	TaskFlagFormSelectedWB610 AS i_TaskFlagFormSelectedWB610,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB610, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB610,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB610,
	TaskFlagNewPolicy AS i_TaskFlagNewPolicy,
	-- *INF*: DECODE(i_TaskFlagNewPolicy, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagNewPolicy,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagNewPolicy,
	-- *INF*: DECODE(i_TaskFlagNewPolicyRequestLPService, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagNewPolicyRequestLPService,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagNewPolicyRequestLPService,
	TaskFlagPolicyCanceledWithFiling AS i_TaskFlagPolicyCanceledWithFiling,
	-- *INF*: DECODE(i_TaskFlagPolicyCanceledWithFiling, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyCanceledWithFiling,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyCanceledWithFiling,
	TaskFlagPolicyCanceledWithNonPayFiling AS i_TaskFlagPolicyCanceledWithNonPayFiling,
	-- *INF*: DECODE(i_TaskFlagPolicyCanceledWithNonPayFiling, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyCanceledWithNonPayFiling,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyCanceledWithNonPayFiling,
	TaskFlagPolicyCoverageRemovedWithFiling AS i_TaskFlagPolicyCoverageRemovedWithFiling,
	-- *INF*: DECODE(i_TaskFlagPolicyCoverageRemovedWithFiling, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyCoverageRemovedWithFiling,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyCoverageRemovedWithFiling,
	TaskFlagPolicyEndorsement AS i_TaskFlagPolicyEndorsement,
	-- *INF*: DECODE(i_TaskFlagPolicyEndorsement, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyEndorsement,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyEndorsement,
	TaskFlagPolicyFinalAudit AS i_TaskFlagPolicyFinalAudit,
	-- *INF*: DECODE(i_TaskFlagPolicyFinalAudit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyFinalAudit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyFinalAudit,
	TaskFlagPolicyFinalAuditWithCustomerCare AS i_TaskFlagPolicyFinalAuditWithCustomerCare,
	-- *INF*: DECODE(i_TaskFlagPolicyFinalAuditWithCustomerCare, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyFinalAuditWithCustomerCare,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyFinalAuditWithCustomerCare,
	TaskFlagPolicyFinalAuditWithReinsurer AS i_TaskFlagPolicyFinalAuditWithReinsurer,
	-- *INF*: DECODE(i_TaskFlagPolicyFinalAuditWithReinsurer, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyFinalAuditWithReinsurer,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyFinalAuditWithReinsurer,
	TaskFlagPolicyReissue AS i_TaskFlagPolicyReissue,
	-- *INF*: DECODE(i_TaskFlagPolicyReissue, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyReissue,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyReissue,
	TaskFlagPolicyRenewal AS i_TaskFlagPolicyRenewal,
	-- *INF*: DECODE(i_TaskFlagPolicyRenewal, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyRenewal,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyRenewal,
	-- *INF*: DECODE(i_TaskFlagPolicyRequestLPService, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyRequestLPService,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyRequestLPService,
	TaskFlagPolicyRewrite AS i_TaskFlagPolicyRewrite,
	-- *INF*: DECODE(i_TaskFlagPolicyRewrite, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyRewrite,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyRewrite,
	-- *INF*: DECODE(i_TaskFlagPolicyScheduledLPService, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyScheduledLPService,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyScheduledLPService,
	TaskFlagPolicySignatureRequiredIteration AS i_TaskFlagPolicySignatureRequiredIteration,
	-- *INF*: DECODE(i_TaskFlagPolicySignatureRequiredIteration, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicySignatureRequiredIteration,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicySignatureRequiredIteration,
	TaskFlagPolicyWithRiskGradeDNW AS i_TaskFlagPolicyWithRiskGradeDNW,
	-- *INF*: DECODE(i_TaskFlagPolicyWithRiskGradeDNW, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyWithRiskGradeDNW,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyWithRiskGradeDNW,
	TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold AS i_TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold,
	-- *INF*: DECODE(i_TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold,
	-- *INF*: DECODE(i_TaskFlagPolicyScheduledLPServiceCommited, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyScheduledLPServiceCommited,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyScheduledLPServiceCommited,
	TaskFlagPolicyWithReinsuranceAndCertificateReceived AS i_TaskFlagPolicyWithReinsuranceAndCertificateReceived,
	-- *INF*: DECODE(i_TaskFlagPolicyWithReinsuranceAndCertificateReceived, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicyWithReinsuranceAndCertificateReceived,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicyWithReinsuranceAndCertificateReceived,
	TaskFlagFormSelectedWB1214 AS i_TaskFlagFormSelectedWB1214,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB1214, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB1214,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB1214,
	TaskFlagFormSelectedWB1214IA AS i_TaskFlagFormSelectedWB1214IA,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB1214IA, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB1214IA,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB1214IA,
	TaskFlagFormSelectedWB1930 AS i_TaskFlagFormSelectedWB1930,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB1930, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB1930,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB1930,
	TaskFlagFormSelectedWB2580 AS i_TaskFlagFormSelectedWB2580,
	-- *INF*: DECODE(i_TaskFlagFormSelectedWB2580, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagFormSelectedWB2580,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagFormSelectedWB2580,
	-- *INF*: DECODE(i_IsBCCCodeHotelsAndMotels, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IsBCCCodeHotelsAndMotels,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsBCCCodeHotelsAndMotels,
	NoteFlagLPServicesOrderedOnIssue AS i_NoteFlagLPServicesOrderedOnIssue,
	-- *INF*: DECODE(i_NoteFlagLPServicesOrderedOnIssue, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_NoteFlagLPServicesOrderedOnIssue,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_NoteFlagLPServicesOrderedOnIssue,
	InitialPendingOnTransaction AS i_InitialPendingOnTransaction,
	-- *INF*: DECODE(i_InitialPendingOnTransaction, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_InitialPendingOnTransaction,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_InitialPendingOnTransaction,
	StoreInfoAnswersOnApprove AS i_StoreInfoAnswersOnApprove,
	-- *INF*: DECODE(i_StoreInfoAnswersOnApprove, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_StoreInfoAnswersOnApprove,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_StoreInfoAnswersOnApprove,
	StoreReferralAnswers AS i_StoreReferralAnswers,
	-- *INF*: DECODE(i_StoreReferralAnswers, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_StoreReferralAnswers,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_StoreReferralAnswers,
	ReinsuranceApplied AS i_ReinsuranceApplied,
	-- *INF*: DECODE(i_ReinsuranceApplied, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReinsuranceApplied,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReinsuranceApplied,
	ReinsuranceRemoved AS i_ReinsuranceRemoved,
	-- *INF*: DECODE(i_ReinsuranceRemoved, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ReinsuranceRemoved,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ReinsuranceRemoved,
	AuditablePremium,
	SBOPIndicator AS i_SBOPIndicator,
	-- *INF*: DECODE(i_SBOPIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_SBOPIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_SBOPIndicator,
	CrimeIndicator AS i_CrimeIndicator,
	-- *INF*: DECODE(i_CrimeIndicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_CrimeIndicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_CrimeIndicator,
	ReleasedDate,
	ReleasedDateFirst,
	NoticeOfCancellationDays,
	IsNewQuoteWCPool AS i_IsNewQuoteWCPool,
	-- *INF*: DECODE(i_IsNewQuoteWCPool, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IsNewQuoteWCPool,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsNewQuoteWCPool,
	NAICSHasBeenVisited AS i_NAICSHasBeenVisited,
	-- *INF*: DECODE(i_NAICSHasBeenVisited, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_NAICSHasBeenVisited,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_NAICSHasBeenVisited,
	PolicyAuditablePremium,
	PremiumDuplicatePath,
	LPServiceFlag,
	IncludePolicyFlag AS i_IncludePolicyFlag,
	-- *INF*: DECODE(i_IncludePolicyFlag, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IncludePolicyFlag,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncludePolicyFlag,
	FinalAuditDueDate,
	ApplyPropertyTransition AS i_ApplyPropertyTransition,
	-- *INF*: DECODE(i_ApplyPropertyTransition, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ApplyPropertyTransition,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ApplyPropertyTransition,
	RenewalCounter,
	ASignedOhioFraudStatement,
	ExpiredReason,
	ExpiredReasonDetails,
	RejectedReason,
	RejectedReasonDetails,
	Indicator AS i_Indicator,
	-- *INF*: DECODE(i_Indicator, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Indicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Indicator,
	DandOPremium,
	LiquorLiabilityPremium,
	HideDandO AS i_HideDandO,
	-- *INF*: DECODE(i_HideDandO, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_HideDandO,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_HideDandO,
	HideLiquorLiability AS i_HideLiquorLiability,
	-- *INF*: DECODE(i_HideLiquorLiability, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_HideLiquorLiability,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_HideLiquorLiability,
	ShowBlackBoxRating AS i_ShowBlackBoxRating,
	-- *INF*: DECODE(i_ShowBlackBoxRating, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_ShowBlackBoxRating,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_ShowBlackBoxRating,
	TotalPolicyQuotedPremium,
	TotalLineQuotedPremium,
	TaskFlagLPServiceRequestArgent AS i_TaskFlagLPServiceRequestArgent,
	-- *INF*: DECODE(i_TaskFlagLPServiceRequestArgent, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagLPServiceRequestArgent,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagLPServiceRequestArgent,
	TaskFlagLPServiceRequestNSI AS i_TaskFlagLPServiceRequestNSI,
	-- *INF*: DECODE(i_TaskFlagLPServiceRequestNSI, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagLPServiceRequestNSI,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagLPServiceRequestNSI,
	TaskFlagLPServiceScheduledPolicyCancel AS i_TaskFlagLPServiceScheduledPolicyCancel,
	-- *INF*: DECODE(i_TaskFlagLPServiceScheduledPolicyCancel, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagLPServiceScheduledPolicyCancel,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagLPServiceScheduledPolicyCancel,
	TaskFlagLPServiceScheduledPolicyCancelNonpayment AS i_TaskFlagLPServiceScheduledPolicyCancelNonpayment,
	-- *INF*: DECODE(i_TaskFlagLPServiceScheduledPolicyCancelNonpayment, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagLPServiceScheduledPolicyCancelNonpayment,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagLPServiceScheduledPolicyCancelNonpayment,
	SAFER,
	USDOTNumber,
	FilingName,
	FederalOperatingAuthority AS i_FederalOperatingAuthority,
	-- *INF*: DECODE(i_FederalOperatingAuthority, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_FederalOperatingAuthority,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_FederalOperatingAuthority,
	FEIN,
	FilingType,
	FederalStateLimit,
	OtherLimit,
	OtherLimitComments,
	MC90Only AS i_MC90Only,
	-- *INF*: DECODE(i_MC90Only, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_MC90Only,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_MC90Only,
	CommoditiesDescription,
	CommoditiesLimit,
	InterstateBMC91X AS i_InterstateBMC91X,
	-- *INF*: DECODE(i_InterstateBMC91X, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_InterstateBMC91X,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_InterstateBMC91X,
	FormType,
	IntrastateFormEEX AS i_IntrastateFormEEX,
	-- *INF*: DECODE(i_IntrastateFormEEX, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IntrastateFormEEX,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IntrastateFormEEX,
	IntrastateFormEEXHaulingStates AS i_IntrastateFormEEXHaulingStates,
	-- *INF*: DECODE(i_IntrastateFormEEXHaulingStates, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IntrastateFormEEXHaulingStates,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IntrastateFormEEXHaulingStates,
	WIHumanServices AS i_WIHumanServices,
	-- *INF*: DECODE(i_WIHumanServices, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_WIHumanServices,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_WIHumanServices,
	WISchoolBuss AS i_WISchoolBuss,
	-- *INF*: DECODE(i_WISchoolBuss, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_WISchoolBuss,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_WISchoolBuss,
	OHHaulingPermit AS i_OHHaulingPermit,
	-- *INF*: DECODE(i_OHHaulingPermit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_OHHaulingPermit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_OHHaulingPermit,
	OHLimit,
	OHOtherLimit,
	OHOtherLimitComments,
	InterstateBMC34 AS i_InterstateBMC34,
	-- *INF*: DECODE(i_InterstateBMC34, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_InterstateBMC34,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_InterstateBMC34,
	IntrastateFormH AS i_IntrastateFormH,
	-- *INF*: DECODE(i_IntrastateFormH, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IntrastateFormH,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IntrastateFormH,
	IntrastateFormHHaulingStates,
	Effective,
	FilingNameMessage,
	MC90FSLimitLessThanCommLimitMessage,
	Status,
	NameCheckMessage,
	Deleted AS i_Deleted,
	-- *INF*: DECODE(i_Deleted, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_Deleted,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Deleted,
	IsAtCollections AS i_IsAtCollections,
	-- *INF*: DECODE(i_IsAtCollections, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_IsAtCollections,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IsAtCollections,
	TaskFlagCancelPending AS i_TaskFlagCancelPending,
	-- *INF*: DECODE(i_TaskFlagCancelPending, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagCancelPending,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagCancelPending,
	TaskFlagCancelPendingArgent AS i_TaskFlagCancelPendingArgent,
	-- *INF*: DECODE(i_TaskFlagCancelPendingArgent, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagCancelPendingArgent,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagCancelPendingArgent,
	PreviousEffectiveDate,
	TaskFlagConversionProcessRenewal AS i_TaskFlagConversionProcessRenewal,
	-- *INF*: DECODE(i_TaskFlagConversionProcessRenewal, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagConversionProcessRenewal,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagConversionProcessRenewal,
	TaskFlagPolicySMARTTotalAboveThreshold AS i_TaskFlagPolicySMARTTotalAboveThreshold,
	-- *INF*: DECODE(i_TaskFlagPolicySMARTTotalAboveThreshold, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_TaskFlagPolicySMARTTotalAboveThreshold,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaskFlagPolicySMARTTotalAboveThreshold,
	CATotalLimitForReinsuranceTask,
	SMARTTotalLimitForReinsuranceTask,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	EstimatedQuotePremium,
	NoncomplianceofWCPoolAudit AS i_NoncomplianceofWCPoolAudit,
	-- *INF*: DECODE(i_NoncomplianceofWCPoolAudit, 'T', '1', 'F', '0', NULL)
	DECODE(
	    i_NoncomplianceofWCPoolAudit,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_NoncomplianceofWCPoolAudit
	FROM SQ_WB_CL_Policy
),
WbClPolicyStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WbClPolicyStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WbClPolicyStage
	(ExtractDate, SourceSystemId, WB_PolicyId, WB_CL_PolicyId, SessionId, ApplicantBuildingsMoreThan2Apartments, OperationType, OperationTypeNext30Days, DescriptionOfChildCarePremises, ChildCarePremisesOther, OperationTypePersonalAppearance, OperationTypeNext30DaysPersonalAppearance, DoNotAutomaticallyRenewThisPolicy, AlteredTermsRequired, SMARTIndicator, AuditType, AuditTypePolicyPeriodOverride, AuditTypePermanentOverride, AuditTypeOverrideReason, AssignedAuditor, AssignedAuditorPolicyPeriodOverride, AssignedAuditorPermanentOverride, AssignedAuditorOverrideReason, CloseAudit, CloseAuditReason, AuditPriority, HasCorrespondingFrontingPolicy, FrontingPolicyNumber, FrontingPolicyPremium, AssignedAuditorUserId, IsApplicant, TermReason, LPService, IncludePolicy, MailPolicyToInsured, TaskFlagNewPolicyWCPoolWI, TaskFlagPolicyBound, TaskFlagPolicyIssued, TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold, TaskFlagPolicyCLTotalAboveThreshold, TaskFlagPolicyNSITotalAboveThreshold, TaskFlagFormSelectedWB2035, TaskFlagFormSelectedWB2044C, TaskFlagFormSelectedWB1514, TaskFlagFormSelectedWB1078, TaskFlagAuditPolicyCancelled, TaskFlagAuditPolicyCancelledWCPoolArgent, TaskFlagAuditPolicyReinstated, TaskFlagAuditRequiredMidTerm, TaskFlagAuditRequiredPremliminary, TaskFlagCancelNonPayReinsurance, TaskFlagCancelReinsurance, TaskFlagFormSelectedNW0034, TaskFlagFormSelectedNW0044, TaskFlagFormSelectedWB1558, TaskFlagFormSelectedWB2115, TaskFlagFormSelectedWB2138, TaskFlagFormSelectedWB2185, TaskFlagFormSelectedWB345, TaskFlagFormSelectedWB610, TaskFlagNewPolicy, TaskFlagLPServiceRequestArgent, TaskFlagPolicyCanceledWithFiling, TaskFlagPolicyCanceledWithNonPayFiling, TaskFlagPolicyCoverageRemovedWithFiling, TaskFlagPolicyEndorsement, TaskFlagPolicyFinalAudit, TaskFlagPolicyFinalAuditWithCustomerCare, TaskFlagPolicyFinalAuditWithReinsurer, TaskFlagPolicyReissue, TaskFlagPolicyRenewal, TaskFlagLPServiceRequestNSI, TaskFlagPolicyRewrite, TaskFlagLPServiceScheduledPolicyCancel, TaskFlagPolicySignatureRequiredIteration, TaskFlagPolicyWithRiskGradeDNW, TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold, TaskFlagLPServiceScheduledPolicyCancelNonpayment, TaskFlagPolicyWithReinsuranceAndCertificateReceived, TaskFlagFormSelectedWB1214, TaskFlagFormSelectedWB1214IA, TaskFlagFormSelectedWB1930, TaskFlagFormSelectedWB2580, TaskFlagCancelPending, TaskFlagCancelPendingArgent, TaskFlagConversionProcessRenewal, TaskFlagPolicySMARTTotalAboveThreshold, IsBCCCodeHotelsAndMotels, NoteFlagLPServicesOrderedOnIssue, PreviousEffectiveDate, InitialPendingOnTransaction, StoreInfoAnswersOnApprove, StoreReferralAnswers, ReinsuranceApplied, ReinsuranceRemoved, AuditablePremium, SBOPIndicator, CrimeIndicator, ReleasedDate, ReleasedDateFirst, NoticeOfCancellationDays, IsNewQuoteWCPool, NAICSHasBeenVisited, PolicyAuditablePremium, PremiumDuplicatePath, LPServiceFlag, IncludePolicyFlag, FinalAuditDueDate, ApplyPropertyTransition, RenewalCounter, ASignedOhioFraudStatement, ExpiredReason, ExpiredReasonDetails, RejectedReason, RejectedReasonDetails, IsAtCollections, CATotalLimitForReinsuranceTask, SMARTTotalLimitForReinsuranceTask, Indicator, DandOPremium, LiquorLiabilityPremium, HideDandO, HideLiquorLiability, ShowBlackBoxRating, TotalPolicyQuotedPremium, TotalLineQuotedPremium, SAFER, USDOTNumber, FilingName, FederalOperatingAuthority, FEIN, FilingType, FederalStateLimit, OtherLimit, OtherLimitComments, MC90Only, CommoditiesDescription, CommoditiesLimit, InterstateBMC91X, FormType, IntrastateFormEEX, IntrastateFormEEXHaulingStates, WIHumanServices, WISchoolBuss, OHHaulingPermit, OHLimit, OHOtherLimit, OHOtherLimitComments, InterstateBMC34, IntrastateFormH, IntrastateFormHHaulingStates, Effective, FilingNameMessage, MC90FSLimitLessThanCommLimitMessage, Status, NameCheckMessage, Deleted, EstimatedQuotePremium, NoncomplianceofWCPoolAudit)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_POLICYID, 
	WB_CL_POLICYID, 
	SESSIONID, 
	o_ApplicantBuildingsMoreThan2Apartments AS APPLICANTBUILDINGSMORETHAN2APARTMENTS, 
	OPERATIONTYPE, 
	OPERATIONTYPENEXT30DAYS, 
	DESCRIPTIONOFCHILDCAREPREMISES, 
	CHILDCAREPREMISESOTHER, 
	o_OperationTypePersonalAppearance AS OPERATIONTYPEPERSONALAPPEARANCE, 
	o_OperationTypeNext30DaysPersonalAppearance AS OPERATIONTYPENEXT30DAYSPERSONALAPPEARANCE, 
	o_DoNotAutomaticallyRenewThisPolicy AS DONOTAUTOMATICALLYRENEWTHISPOLICY, 
	o_AlteredTermsRequired AS ALTEREDTERMSREQUIRED, 
	o_SMARTIndicator AS SMARTINDICATOR, 
	AUDITTYPE, 
	o_AuditTypePolicyPeriodOverride AS AUDITTYPEPOLICYPERIODOVERRIDE, 
	o_AuditTypePermanentOverride AS AUDITTYPEPERMANENTOVERRIDE, 
	AUDITTYPEOVERRIDEREASON, 
	ASSIGNEDAUDITOR, 
	o_AssignedAuditorPolicyPeriodOverride AS ASSIGNEDAUDITORPOLICYPERIODOVERRIDE, 
	o_AssignedAuditorPermanentOverride AS ASSIGNEDAUDITORPERMANENTOVERRIDE, 
	ASSIGNEDAUDITOROVERRIDEREASON, 
	o_CloseAudit AS CLOSEAUDIT, 
	CLOSEAUDITREASON, 
	AUDITPRIORITY, 
	o_HasCorrespondingFrontingPolicy AS HASCORRESPONDINGFRONTINGPOLICY, 
	FRONTINGPOLICYNUMBER, 
	FRONTINGPOLICYPREMIUM, 
	ASSIGNEDAUDITORUSERID, 
	ISAPPLICANT, 
	TERMREASON, 
	LPSERVICE, 
	o_IncludePolicy AS INCLUDEPOLICY, 
	o_MailPolicyToInsured AS MAILPOLICYTOINSURED, 
	o_TaskFlagNewPolicyWCPoolWI AS TASKFLAGNEWPOLICYWCPOOLWI, 
	o_TaskFlagPolicyBound AS TASKFLAGPOLICYBOUND, 
	o_TaskFlagPolicyIssued AS TASKFLAGPOLICYISSUED, 
	o_TaskFlagWIPolicyTotalWrittenPremiumChangeAboveThreshold AS TASKFLAGWIPOLICYTOTALWRITTENPREMIUMCHANGEABOVETHRESHOLD, 
	o_TaskFlagPolicyCLTotalAboveThreshold AS TASKFLAGPOLICYCLTOTALABOVETHRESHOLD, 
	o_TaskFlagPolicyNSITotalAboveThreshold AS TASKFLAGPOLICYNSITOTALABOVETHRESHOLD, 
	o_TaskFlagFormSelectedWB2035 AS TASKFLAGFORMSELECTEDWB2035, 
	o_TaskFlagFormSelectedWB2044C AS TASKFLAGFORMSELECTEDWB2044C, 
	o_TaskFlagFormSelectedWB1514 AS TASKFLAGFORMSELECTEDWB1514, 
	o_TaskFlagFormSelectedWB1078 AS TASKFLAGFORMSELECTEDWB1078, 
	o_TaskFlagAuditPolicyCancelled AS TASKFLAGAUDITPOLICYCANCELLED, 
	o_TaskFlagAuditPolicyCancelledWCPoolArgent AS TASKFLAGAUDITPOLICYCANCELLEDWCPOOLARGENT, 
	o_TaskFlagAuditPolicyReinstated AS TASKFLAGAUDITPOLICYREINSTATED, 
	o_TaskFlagAuditRequiredMidTerm AS TASKFLAGAUDITREQUIREDMIDTERM, 
	o_TaskFlagAuditRequiredPremliminary AS TASKFLAGAUDITREQUIREDPREMLIMINARY, 
	o_TaskFlagCancelNonPayReinsurance AS TASKFLAGCANCELNONPAYREINSURANCE, 
	o_TaskFlagCancelReinsurance AS TASKFLAGCANCELREINSURANCE, 
	o_TaskFlagFormSelectedNW0034 AS TASKFLAGFORMSELECTEDNW0034, 
	o_TaskFlagFormSelectedNW0044 AS TASKFLAGFORMSELECTEDNW0044, 
	o_TaskFlagFormSelectedWB1558 AS TASKFLAGFORMSELECTEDWB1558, 
	o_TaskFlagFormSelectedWB2115 AS TASKFLAGFORMSELECTEDWB2115, 
	o_TaskFlagFormSelectedWB2138 AS TASKFLAGFORMSELECTEDWB2138, 
	o_TaskFlagFormSelectedWB2185 AS TASKFLAGFORMSELECTEDWB2185, 
	o_TaskFlagFormSelectedWB345 AS TASKFLAGFORMSELECTEDWB345, 
	o_TaskFlagFormSelectedWB610 AS TASKFLAGFORMSELECTEDWB610, 
	o_TaskFlagNewPolicy AS TASKFLAGNEWPOLICY, 
	o_TaskFlagLPServiceRequestArgent AS TASKFLAGLPSERVICEREQUESTARGENT, 
	o_TaskFlagPolicyCanceledWithFiling AS TASKFLAGPOLICYCANCELEDWITHFILING, 
	o_TaskFlagPolicyCanceledWithNonPayFiling AS TASKFLAGPOLICYCANCELEDWITHNONPAYFILING, 
	o_TaskFlagPolicyCoverageRemovedWithFiling AS TASKFLAGPOLICYCOVERAGEREMOVEDWITHFILING, 
	o_TaskFlagPolicyEndorsement AS TASKFLAGPOLICYENDORSEMENT, 
	o_TaskFlagPolicyFinalAudit AS TASKFLAGPOLICYFINALAUDIT, 
	o_TaskFlagPolicyFinalAuditWithCustomerCare AS TASKFLAGPOLICYFINALAUDITWITHCUSTOMERCARE, 
	o_TaskFlagPolicyFinalAuditWithReinsurer AS TASKFLAGPOLICYFINALAUDITWITHREINSURER, 
	o_TaskFlagPolicyReissue AS TASKFLAGPOLICYREISSUE, 
	o_TaskFlagPolicyRenewal AS TASKFLAGPOLICYRENEWAL, 
	o_TaskFlagLPServiceRequestNSI AS TASKFLAGLPSERVICEREQUESTNSI, 
	o_TaskFlagPolicyRewrite AS TASKFLAGPOLICYREWRITE, 
	o_TaskFlagLPServiceScheduledPolicyCancel AS TASKFLAGLPSERVICESCHEDULEDPOLICYCANCEL, 
	o_TaskFlagPolicySignatureRequiredIteration AS TASKFLAGPOLICYSIGNATUREREQUIREDITERATION, 
	o_TaskFlagPolicyWithRiskGradeDNW AS TASKFLAGPOLICYWITHRISKGRADEDNW, 
	o_TaskFlagWIPolicyTotalWrittenPremiumAboveThreshold AS TASKFLAGWIPOLICYTOTALWRITTENPREMIUMABOVETHRESHOLD, 
	o_TaskFlagLPServiceScheduledPolicyCancelNonpayment AS TASKFLAGLPSERVICESCHEDULEDPOLICYCANCELNONPAYMENT, 
	o_TaskFlagPolicyWithReinsuranceAndCertificateReceived AS TASKFLAGPOLICYWITHREINSURANCEANDCERTIFICATERECEIVED, 
	o_TaskFlagFormSelectedWB1214 AS TASKFLAGFORMSELECTEDWB1214, 
	o_TaskFlagFormSelectedWB1214IA AS TASKFLAGFORMSELECTEDWB1214IA, 
	o_TaskFlagFormSelectedWB1930 AS TASKFLAGFORMSELECTEDWB1930, 
	o_TaskFlagFormSelectedWB2580 AS TASKFLAGFORMSELECTEDWB2580, 
	o_TaskFlagCancelPending AS TASKFLAGCANCELPENDING, 
	o_TaskFlagCancelPendingArgent AS TASKFLAGCANCELPENDINGARGENT, 
	o_TaskFlagConversionProcessRenewal AS TASKFLAGCONVERSIONPROCESSRENEWAL, 
	o_TaskFlagPolicySMARTTotalAboveThreshold AS TASKFLAGPOLICYSMARTTOTALABOVETHRESHOLD, 
	o_IsBCCCodeHotelsAndMotels AS ISBCCCODEHOTELSANDMOTELS, 
	o_NoteFlagLPServicesOrderedOnIssue AS NOTEFLAGLPSERVICESORDEREDONISSUE, 
	PREVIOUSEFFECTIVEDATE, 
	o_InitialPendingOnTransaction AS INITIALPENDINGONTRANSACTION, 
	o_StoreInfoAnswersOnApprove AS STOREINFOANSWERSONAPPROVE, 
	o_StoreReferralAnswers AS STOREREFERRALANSWERS, 
	o_ReinsuranceApplied AS REINSURANCEAPPLIED, 
	o_ReinsuranceRemoved AS REINSURANCEREMOVED, 
	AUDITABLEPREMIUM, 
	o_SBOPIndicator AS SBOPINDICATOR, 
	o_CrimeIndicator AS CRIMEINDICATOR, 
	RELEASEDDATE, 
	RELEASEDDATEFIRST, 
	NOTICEOFCANCELLATIONDAYS, 
	o_IsNewQuoteWCPool AS ISNEWQUOTEWCPOOL, 
	o_NAICSHasBeenVisited AS NAICSHASBEENVISITED, 
	POLICYAUDITABLEPREMIUM, 
	PREMIUMDUPLICATEPATH, 
	LPSERVICEFLAG, 
	o_IncludePolicyFlag AS INCLUDEPOLICYFLAG, 
	FINALAUDITDUEDATE, 
	o_ApplyPropertyTransition AS APPLYPROPERTYTRANSITION, 
	RENEWALCOUNTER, 
	ASIGNEDOHIOFRAUDSTATEMENT, 
	EXPIREDREASON, 
	EXPIREDREASONDETAILS, 
	REJECTEDREASON, 
	REJECTEDREASONDETAILS, 
	o_IsAtCollections AS ISATCOLLECTIONS, 
	CATOTALLIMITFORREINSURANCETASK, 
	SMARTTOTALLIMITFORREINSURANCETASK, 
	o_Indicator AS INDICATOR, 
	DANDOPREMIUM, 
	LIQUORLIABILITYPREMIUM, 
	o_HideDandO AS HIDEDANDO, 
	o_HideLiquorLiability AS HIDELIQUORLIABILITY, 
	o_ShowBlackBoxRating AS SHOWBLACKBOXRATING, 
	TOTALPOLICYQUOTEDPREMIUM, 
	TOTALLINEQUOTEDPREMIUM, 
	SAFER, 
	USDOTNUMBER, 
	FILINGNAME, 
	o_FederalOperatingAuthority AS FEDERALOPERATINGAUTHORITY, 
	FEIN, 
	FILINGTYPE, 
	FEDERALSTATELIMIT, 
	OTHERLIMIT, 
	OTHERLIMITCOMMENTS, 
	o_MC90Only AS MC90ONLY, 
	COMMODITIESDESCRIPTION, 
	COMMODITIESLIMIT, 
	o_InterstateBMC91X AS INTERSTATEBMC91X, 
	FORMTYPE, 
	o_IntrastateFormEEX AS INTRASTATEFORMEEX, 
	o_IntrastateFormEEXHaulingStates AS INTRASTATEFORMEEXHAULINGSTATES, 
	o_WIHumanServices AS WIHUMANSERVICES, 
	o_WISchoolBuss AS WISCHOOLBUSS, 
	o_OHHaulingPermit AS OHHAULINGPERMIT, 
	OHLIMIT, 
	OHOTHERLIMIT, 
	OHOTHERLIMITCOMMENTS, 
	o_InterstateBMC34 AS INTERSTATEBMC34, 
	o_IntrastateFormH AS INTRASTATEFORMH, 
	INTRASTATEFORMHHAULINGSTATES, 
	EFFECTIVE, 
	FILINGNAMEMESSAGE, 
	MC90FSLIMITLESSTHANCOMMLIMITMESSAGE, 
	STATUS, 
	NAMECHECKMESSAGE, 
	o_Deleted AS DELETED, 
	ESTIMATEDQUOTEPREMIUM, 
	o_NoncomplianceofWCPoolAudit AS NONCOMPLIANCEOFWCPOOLAUDIT
	FROM EXP_Metadata
),