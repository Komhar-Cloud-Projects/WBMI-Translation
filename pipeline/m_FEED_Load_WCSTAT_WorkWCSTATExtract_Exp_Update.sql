WITH
SQ_WorkWCSTATExtract AS (
	with BeforeChange9898 as 
	(
	select PolicyKey,
	StateProvinceCode,
	PolicyEffectiveDate,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract
	where PremiumMasterClassCode='9898'
	and auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and ExperienceModificationEffectiveDate=PolicyEffectiveDate
	),
	AfterChange9898 as 
	(
	select PolicyKey,
	StateProvinceCode,
	PolicyEffectiveDate,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract
	where PremiumMasterClassCode='9898'
	and auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	and ExperienceModificationEffectiveDate<>PolicyEffectiveDate
	)
	
	select A.ExperienceModificationFactor as ChangedExpModFactor,
	A.ExperienceModificationEffectiveDate as ChangedExpModEffDate,
	W.*
	from @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkWCSTATExtract W
	join BeforeChange9898 B
	on W.PolicyKey=B.PolicyKey and W.StateProvinceCode=B.StateProvinceCode 
	and W.PolicyEffectiveDate=B.PolicyEffectiveDate and W.ExperienceModificationEffectiveDate=B.ExperienceModificationEffectiveDate
	and W.ExperienceModificationFactor=B.ExperienceModificationFactor
	join AfterChange9898 A
	on W.PolicyKey=A.PolicyKey and W.StateProvinceCode=A.StateProvinceCode 
	and W.PolicyEffectiveDate=A.PolicyEffectiveDate and W.ExperienceModificationEffectiveDate<>A.ExperienceModificationEffectiveDate
	and W.ExperienceModificationFactor<>A.ExperienceModificationFactor
	where W.PremiumMasterClassCode<>'9898'
	and W.EDWLossMasterCalculationPKId=-1
	and W.Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE}
),
LKP_Worktable AS (
	SELECT
	WorkWCSTATExtractId,
	PolicyKey,
	StateProvinceCode,
	PremiumMasterClassCode,
	ExperienceModificationFactor,
	ExperienceModificationEffectiveDate
	FROM (
		SELECT 
			WorkWCSTATExtractId,
			PolicyKey,
			StateProvinceCode,
			PremiumMasterClassCode,
			ExperienceModificationFactor,
			ExperienceModificationEffectiveDate
		FROM WorkWCSTATExtract
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,StateProvinceCode,PremiumMasterClassCode,ExperienceModificationFactor,ExperienceModificationEffectiveDate ORDER BY WorkWCSTATExtractId) = 1
),
FIL_Existing AS (
	SELECT
	LKP_Worktable.WorkWCSTATExtractId AS LKP_WorkWCSTATExtractId, 
	SQ_WorkWCSTATExtract.ChangedExpModFactor, 
	SQ_WorkWCSTATExtract.ChangedExpModEffDate, 
	SQ_WorkWCSTATExtract.WorkWCSTATExtractId, 
	SQ_WorkWCSTATExtract.AuditId, 
	SQ_WorkWCSTATExtract.CreatedDate, 
	SQ_WorkWCSTATExtract.EDWPremiumMasterCalculationPKId, 
	SQ_WorkWCSTATExtract.EDWLossMasterCalculationPKId, 
	SQ_WorkWCSTATExtract.TypeBureauCode, 
	SQ_WorkWCSTATExtract.PremiumMasterRunDate, 
	SQ_WorkWCSTATExtract.LossMasterRunDate, 
	SQ_WorkWCSTATExtract.BureauCompanyCode, 
	SQ_WorkWCSTATExtract.PolicyKey, 
	SQ_WorkWCSTATExtract.StateProvinceCode, 
	SQ_WorkWCSTATExtract.PolicyEffectiveDate, 
	SQ_WorkWCSTATExtract.PolicyEndDate, 
	SQ_WorkWCSTATExtract.InterstateRiskId, 
	SQ_WorkWCSTATExtract.EmployeeLeasingCode, 
	SQ_WorkWCSTATExtract.StateRatingEffectiveDate, 
	SQ_WorkWCSTATExtract.FederalTaxId, 
	SQ_WorkWCSTATExtract.ThreeYearFixedRatePolicyIndicator, 
	SQ_WorkWCSTATExtract.MultistatePolicyIndicator, 
	SQ_WorkWCSTATExtract.InterstateRatedPolicyIndicator, 
	SQ_WorkWCSTATExtract.EstimatedAuditCode, 
	SQ_WorkWCSTATExtract.RetrospectiveratedPolicyIndicator, 
	SQ_WorkWCSTATExtract.CancelledMidTermPolicyIndicator, 
	SQ_WorkWCSTATExtract.ManagedCareOrganizationPolicyIndicator, 
	SQ_WorkWCSTATExtract.TypeOfCoverageIdCode, 
	SQ_WorkWCSTATExtract.TypeOfPlan, 
	SQ_WorkWCSTATExtract.LossSubjectToDeductibleCode, 
	SQ_WorkWCSTATExtract.BasisOfDeductibleCalculationCode, 
	SQ_WorkWCSTATExtract.DeductibleAmountPerClaimAccident, 
	SQ_WorkWCSTATExtract.InsuredName, 
	SQ_WorkWCSTATExtract.WCSTATAddress, 
	SQ_WorkWCSTATExtract.PremiumMasterClassCode, 
	SQ_WorkWCSTATExtract.ExperienceModificationFactor, 
	SQ_WorkWCSTATExtract.ExperienceModificationEffectiveDate, 
	SQ_WorkWCSTATExtract.Exposure, 
	SQ_WorkWCSTATExtract.PremiumMasterDirectWrittenPremiumAmount, 
	SQ_WorkWCSTATExtract.ManualChargedRate, 
	SQ_WorkWCSTATExtract.LossMasterClassCode, 
	SQ_WorkWCSTATExtract.ClaimLossDate, 
	SQ_WorkWCSTATExtract.ClaimNumber, 
	SQ_WorkWCSTATExtract.ClaimOccurrenceStatusCode, 
	SQ_WorkWCSTATExtract.InjuryTypeCode, 
	SQ_WorkWCSTATExtract.CatastropheCode, 
	SQ_WorkWCSTATExtract.IncurredIndemnityAmount, 
	SQ_WorkWCSTATExtract.IncurredMedicalAmount, 
	SQ_WorkWCSTATExtract.CauseOfLoss, 
	SQ_WorkWCSTATExtract.TypeOfRecoveryCode, 
	SQ_WorkWCSTATExtract.JurisdictionStateCode, 
	SQ_WorkWCSTATExtract.BodyPartCode, 
	SQ_WorkWCSTATExtract.NatureOfInjuryCode, 
	SQ_WorkWCSTATExtract.CauseOfInjuryCode, 
	SQ_WorkWCSTATExtract.PaidIndemnityAmount, 
	SQ_WorkWCSTATExtract.PaidMedicalAmount, 
	SQ_WorkWCSTATExtract.DeductibleReimbursementAmount, 
	SQ_WorkWCSTATExtract.PaidAllocatedLossAdjustmentExpenseAmount, 
	SQ_WorkWCSTATExtract.IncurredAllocatedLossAdjustmentExpenseAmount, 
	SQ_WorkWCSTATExtract.SubjectPremiumTotal, 
	SQ_WorkWCSTATExtract.StandarPremiumTotal, 
	SQ_WorkWCSTATExtract.CorrectionSeqNumber, 
	SQ_WorkWCSTATExtract.ReplacementReportCode, 
	SQ_WorkWCSTATExtract.CorrectionTypeCode, 
	SQ_WorkWCSTATExtract.TypeOfNonStandardIdCode, 
	SQ_WorkWCSTATExtract.PreviousReportLevelCodeReportNumber, 
	SQ_WorkWCSTATExtract.UpdateTypeCode, 
	SQ_WorkWCSTATExtract.TypeOfLoss, 
	SQ_WorkWCSTATExtract.TypeOfClaim, 
	SQ_WorkWCSTATExtract.TypeOfSettlement, 
	SQ_WorkWCSTATExtract.ManagedCareOrganizationType, 
	SQ_WorkWCSTATExtract.VocationalRehabIndicator, 
	SQ_WorkWCSTATExtract.LumpSumIndicator, 
	SQ_WorkWCSTATExtract.FraudulentClaimCode, 
	SQ_WorkWCSTATExtract.ClaimantNumber, 
	SQ_WorkWCSTATExtract.RateEffectiveDate, 
	SQ_WorkWCSTATExtract.SplitPeriodCode, 
	SQ_WorkWCSTATExtract.OccupationDescription, 
	SQ_WorkWCSTATExtract.WeeklyWageAmount, 
	SQ_WorkWCSTATExtract.EmployerAttorneyFees
	FROM SQ_WorkWCSTATExtract
	LEFT JOIN LKP_Worktable
	ON LKP_Worktable.PolicyKey = SQ_WorkWCSTATExtract.PolicyKey AND LKP_Worktable.StateProvinceCode = SQ_WorkWCSTATExtract.StateProvinceCode AND LKP_Worktable.PremiumMasterClassCode = SQ_WorkWCSTATExtract.PremiumMasterClassCode AND LKP_Worktable.ExperienceModificationFactor = SQ_WorkWCSTATExtract.ChangedExpModFactor AND LKP_Worktable.ExperienceModificationEffectiveDate = SQ_WorkWCSTATExtract.ChangedExpModEffDate
	WHERE ISNULL(LKP_WorkWCSTATExtractId)
),
EXP_Split AS (
	SELECT
	ChangedExpModFactor AS i_ChangedExpModFactor,
	ChangedExpModEffDate AS i_ChangedExpModEffDate,
	WorkWCSTATExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	BureauCompanyCode,
	PolicyKey,
	StateProvinceCode,
	PolicyEffectiveDate,
	PolicyEndDate,
	InterstateRiskId,
	EmployeeLeasingCode,
	StateRatingEffectiveDate,
	FederalTaxId,
	ThreeYearFixedRatePolicyIndicator AS i_ThreeYearFixedRatePolicyIndicator,
	-- *INF*: IIF(i_ThreeYearFixedRatePolicyIndicator='T','1','0')
	IFF(i_ThreeYearFixedRatePolicyIndicator = 'T', '1', '0') AS o_ThreeYearFixedRatePolicyIndicator,
	MultistatePolicyIndicator AS i_MultistatePolicyIndicator,
	-- *INF*: IIF(i_MultistatePolicyIndicator='T','1','0')
	IFF(i_MultistatePolicyIndicator = 'T', '1', '0') AS o_MultistatePolicyIndicator,
	InterstateRatedPolicyIndicator AS i_InterstateRatedPolicyIndicator,
	-- *INF*: IIF(i_InterstateRatedPolicyIndicator='T','1','0')
	IFF(i_InterstateRatedPolicyIndicator = 'T', '1', '0') AS o_InterstateRatedPolicyIndicator,
	EstimatedAuditCode,
	RetrospectiveratedPolicyIndicator AS i_RetrospectiveratedPolicyIndicator,
	-- *INF*: IIF(i_RetrospectiveratedPolicyIndicator='T','1','0')
	IFF(i_RetrospectiveratedPolicyIndicator = 'T', '1', '0') AS o_RetrospectiveratedPolicyIndicator,
	CancelledMidTermPolicyIndicator AS i_CancelledMidTermPolicyIndicator,
	-- *INF*: IIF(i_CancelledMidTermPolicyIndicator='T','1','0')
	IFF(i_CancelledMidTermPolicyIndicator = 'T', '1', '0') AS o_CancelledMidTermPolicyIndicator,
	ManagedCareOrganizationPolicyIndicator AS i_ManagedCareOrganizationPolicyIndicator,
	-- *INF*: IIF(i_ManagedCareOrganizationPolicyIndicator='T','1','0')
	IFF(i_ManagedCareOrganizationPolicyIndicator = 'T', '1', '0') AS o_ManagedCareOrganizationPolicyIndicator,
	TypeOfCoverageIdCode,
	TypeOfPlan,
	LossSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode,
	DeductibleAmountPerClaimAccident,
	InsuredName,
	WCSTATAddress,
	PremiumMasterClassCode,
	ExperienceModificationFactor AS i_ExperienceModificationFactor,
	ExperienceModificationEffectiveDate AS i_ExperienceModificationEffectiveDate,
	i_ChangedExpModFactor AS o_ChangedExperienceModificationFactor,
	i_ChangedExpModEffDate AS o_ChangedExperienceModificationEffectiveDate,
	Exposure AS i_Exposure,
	PremiumMasterDirectWrittenPremiumAmount AS i_PremiumMasterDirectWrittenPremiumAmount,
	-- *INF*: DATE_DIFF(PolicyEndDate,PolicyEffectiveDate,'DD')
	DATEDIFF(DAY,PolicyEndDate,PolicyEffectiveDate) AS v_PolicyTerm,
	-- *INF*: DATE_DIFF(i_ChangedExpModEffDate, PolicyEffectiveDate, 'DD') / v_PolicyTerm
	DATEDIFF(DAY,i_ChangedExpModEffDate,PolicyEffectiveDate) / v_PolicyTerm AS v_PercentageBefore,
	-- *INF*: DATE_DIFF(PolicyEndDate, i_ChangedExpModEffDate, 'DD') / v_PolicyTerm
	DATEDIFF(DAY,PolicyEndDate,i_ChangedExpModEffDate) / v_PolicyTerm AS v_PercentageAfter,
	-- *INF*: ROUND(i_Exposure * v_PercentageBefore)
	ROUND(i_Exposure * v_PercentageBefore) AS o_Exposure_SplitBefore,
	-- *INF*: ROUND(i_Exposure * v_PercentageAfter)
	ROUND(i_Exposure * v_PercentageAfter) AS o_Exposure_SplitAfter,
	-- *INF*: ROUND(i_PremiumMasterDirectWrittenPremiumAmount * v_PercentageBefore)
	ROUND(i_PremiumMasterDirectWrittenPremiumAmount * v_PercentageBefore) AS o_PremiumMasterDirectWrittenPremiumAmount_SplitBefore,
	-- *INF*: ROUND(i_PremiumMasterDirectWrittenPremiumAmount * v_PercentageAfter)
	ROUND(i_PremiumMasterDirectWrittenPremiumAmount * v_PercentageAfter) AS o_PremiumMasterDirectWrittenPremiumAmount_SplitAfter,
	ManualChargedRate,
	LossMasterClassCode,
	ClaimLossDate,
	ClaimNumber,
	ClaimOccurrenceStatusCode,
	InjuryTypeCode,
	CatastropheCode,
	IncurredIndemnityAmount,
	IncurredMedicalAmount,
	CauseOfLoss,
	TypeOfRecoveryCode,
	JurisdictionStateCode,
	BodyPartCode,
	NatureOfInjuryCode,
	CauseOfInjuryCode,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	DeductibleReimbursementAmount,
	PaidAllocatedLossAdjustmentExpenseAmount,
	IncurredAllocatedLossAdjustmentExpenseAmount,
	SubjectPremiumTotal,
	StandarPremiumTotal,
	CorrectionSeqNumber,
	ReplacementReportCode,
	CorrectionTypeCode,
	TypeOfNonStandardIdCode,
	PreviousReportLevelCodeReportNumber,
	UpdateTypeCode,
	TypeOfLoss,
	TypeOfClaim,
	TypeOfSettlement,
	ManagedCareOrganizationType,
	VocationalRehabIndicator,
	LumpSumIndicator,
	FraudulentClaimCode,
	ClaimantNumber,
	RateEffectiveDate,
	-- *INF*: DECODE(True,ISNULL(RateEffectiveDate),PolicyEffectiveDate,RateEffectiveDate)
	DECODE(
	    True,
	    RateEffectiveDate IS NULL, PolicyEffectiveDate,
	    RateEffectiveDate
	) AS o_RateEffectiveDate,
	SplitPeriodCode,
	OccupationDescription AS in_OccupationDescription,
	-- *INF*: iif(isnull(in_OccupationDescription),'N/A',in_OccupationDescription)
	IFF(in_OccupationDescription IS NULL, 'N/A', in_OccupationDescription) AS OccupationDescription,
	WeeklyWageAmount AS in_WeeklyWageAmount,
	-- *INF*: iif(isnull(in_WeeklyWageAmount),0,in_WeeklyWageAmount)
	IFF(in_WeeklyWageAmount IS NULL, 0, in_WeeklyWageAmount) AS WeeklyWageAmount,
	EmployerAttorneyFees AS in_EmployerAttorneyFees,
	-- *INF*: iif(isnull(in_EmployerAttorneyFees),0,in_EmployerAttorneyFees)
	IFF(in_EmployerAttorneyFees IS NULL, 0, in_EmployerAttorneyFees) AS EmployerAttorneyFees
	FROM FIL_Existing
),
RTR_Split AS (
	SELECT
	WorkWCSTATExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	BureauCompanyCode,
	PolicyKey,
	StateProvinceCode,
	PolicyEffectiveDate,
	PolicyEndDate,
	InterstateRiskId,
	EmployeeLeasingCode,
	StateRatingEffectiveDate,
	FederalTaxId,
	o_ThreeYearFixedRatePolicyIndicator AS ThreeYearFixedRatePolicyIndicator,
	o_MultistatePolicyIndicator AS MultistatePolicyIndicator,
	o_InterstateRatedPolicyIndicator AS InterstateRatedPolicyIndicator,
	EstimatedAuditCode,
	o_RetrospectiveratedPolicyIndicator AS RetrospectiveratedPolicyIndicator,
	o_CancelledMidTermPolicyIndicator AS CancelledMidTermPolicyIndicator,
	o_ManagedCareOrganizationPolicyIndicator AS ManagedCareOrganizationPolicyIndicator,
	TypeOfCoverageIdCode,
	TypeOfPlan,
	LossSubjectToDeductibleCode,
	BasisOfDeductibleCalculationCode,
	DeductibleAmountPerClaimAccident,
	InsuredName,
	WCSTATAddress,
	PremiumMasterClassCode,
	o_ChangedExperienceModificationFactor AS ChangedExperienceModificationFactor,
	o_ChangedExperienceModificationEffectiveDate AS ChangedExperienceModificationEffectiveDate,
	o_Exposure_SplitBefore AS Exposure_SplitBefore,
	o_Exposure_SplitAfter AS Exposure_SplitAfter,
	o_PremiumMasterDirectWrittenPremiumAmount_SplitBefore AS PremiumMasterDirectWrittenPremiumAmount_SplitBefore,
	o_PremiumMasterDirectWrittenPremiumAmount_SplitAfter AS PremiumMasterDirectWrittenPremiumAmount_SplitAfter,
	ManualChargedRate,
	LossMasterClassCode,
	ClaimLossDate,
	ClaimNumber,
	ClaimOccurrenceStatusCode,
	InjuryTypeCode,
	CatastropheCode,
	IncurredIndemnityAmount,
	IncurredMedicalAmount,
	CauseOfLoss,
	TypeOfRecoveryCode,
	JurisdictionStateCode,
	BodyPartCode,
	NatureOfInjuryCode,
	CauseOfInjuryCode,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	DeductibleReimbursementAmount,
	PaidAllocatedLossAdjustmentExpenseAmount,
	IncurredAllocatedLossAdjustmentExpenseAmount,
	SubjectPremiumTotal,
	StandarPremiumTotal,
	CorrectionSeqNumber,
	ReplacementReportCode,
	CorrectionTypeCode,
	TypeOfNonStandardIdCode,
	PreviousReportLevelCodeReportNumber,
	UpdateTypeCode,
	TypeOfLoss,
	TypeOfClaim,
	TypeOfSettlement,
	ManagedCareOrganizationType,
	VocationalRehabIndicator,
	LumpSumIndicator,
	FraudulentClaimCode,
	ClaimantNumber,
	o_RateEffectiveDate AS RateEffectiveDate,
	OccupationDescription,
	WeeklyWageAmount,
	EmployerAttorneyFees
	FROM EXP_Split
),
RTR_Split_Update AS (SELECT * FROM RTR_Split WHERE TRUE),
RTR_Split_Insert AS (SELECT * FROM RTR_Split WHERE TRUE),
UPD_SplitBefore AS (
	SELECT
	WorkWCSTATExtractId, 
	Exposure_SplitBefore, 
	PremiumMasterDirectWrittenPremiumAmount_SplitBefore
	FROM RTR_Split_Update
),
TGT_WorkWCSTATExtract_ExpMod_Update AS (
	MERGE INTO WorkWCSTATExtract AS T
	USING UPD_SplitBefore AS S
	ON T.WorkWCSTATExtractId = S.WorkWCSTATExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.Exposure = S.Exposure_SplitBefore, T.PremiumMasterDirectWrittenPremiumAmount = S.PremiumMasterDirectWrittenPremiumAmount_SplitBefore
),
TGT_WorkWCSTATExtract_ExpMod_Insert AS (
	INSERT INTO WorkWCSTATExtract
	(AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, PremiumMasterRunDate, LossMasterRunDate, BureauCompanyCode, PolicyKey, StateProvinceCode, PolicyEffectiveDate, PolicyEndDate, InterstateRiskId, EmployeeLeasingCode, StateRatingEffectiveDate, FederalTaxId, ThreeYearFixedRatePolicyIndicator, MultistatePolicyIndicator, InterstateRatedPolicyIndicator, EstimatedAuditCode, RetrospectiveratedPolicyIndicator, CancelledMidTermPolicyIndicator, ManagedCareOrganizationPolicyIndicator, TypeOfCoverageIdCode, TypeOfPlan, LossSubjectToDeductibleCode, BasisOfDeductibleCalculationCode, DeductibleAmountPerClaimAccident, InsuredName, WCSTATAddress, PremiumMasterClassCode, ExperienceModificationFactor, ExperienceModificationEffectiveDate, Exposure, PremiumMasterDirectWrittenPremiumAmount, ManualChargedRate, LossMasterClassCode, ClaimLossDate, ClaimNumber, ClaimOccurrenceStatusCode, InjuryTypeCode, CatastropheCode, IncurredIndemnityAmount, IncurredMedicalAmount, CauseOfLoss, TypeOfRecoveryCode, JurisdictionStateCode, BodyPartCode, NatureOfInjuryCode, CauseOfInjuryCode, PaidIndemnityAmount, PaidMedicalAmount, DeductibleReimbursementAmount, PaidAllocatedLossAdjustmentExpenseAmount, IncurredAllocatedLossAdjustmentExpenseAmount, SubjectPremiumTotal, StandarPremiumTotal, CorrectionSeqNumber, ReplacementReportCode, CorrectionTypeCode, TypeOfNonStandardIdCode, PreviousReportLevelCodeReportNumber, UpdateTypeCode, TypeOfLoss, TypeOfClaim, TypeOfSettlement, ManagedCareOrganizationType, VocationalRehabIndicator, LumpSumIndicator, FraudulentClaimCode, ClaimantNumber, RateEffectiveDate, OccupationDescription, WeeklyWageAmount, EmployerAttorneyFees)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	BUREAUCOMPANYCODE, 
	POLICYKEY, 
	STATEPROVINCECODE, 
	POLICYEFFECTIVEDATE, 
	POLICYENDDATE, 
	INTERSTATERISKID, 
	EMPLOYEELEASINGCODE, 
	STATERATINGEFFECTIVEDATE, 
	FEDERALTAXID, 
	THREEYEARFIXEDRATEPOLICYINDICATOR, 
	MULTISTATEPOLICYINDICATOR, 
	INTERSTATERATEDPOLICYINDICATOR, 
	ESTIMATEDAUDITCODE, 
	RETROSPECTIVERATEDPOLICYINDICATOR, 
	CANCELLEDMIDTERMPOLICYINDICATOR, 
	MANAGEDCAREORGANIZATIONPOLICYINDICATOR, 
	TYPEOFCOVERAGEIDCODE, 
	TYPEOFPLAN, 
	LOSSSUBJECTTODEDUCTIBLECODE, 
	BASISOFDEDUCTIBLECALCULATIONCODE, 
	DEDUCTIBLEAMOUNTPERCLAIMACCIDENT, 
	INSUREDNAME, 
	WCSTATADDRESS, 
	PREMIUMMASTERCLASSCODE, 
	ChangedExperienceModificationFactor AS EXPERIENCEMODIFICATIONFACTOR, 
	ChangedExperienceModificationEffectiveDate AS EXPERIENCEMODIFICATIONEFFECTIVEDATE, 
	Exposure_SplitAfter AS EXPOSURE, 
	PremiumMasterDirectWrittenPremiumAmount_SplitAfter AS PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	MANUALCHARGEDRATE, 
	LOSSMASTERCLASSCODE, 
	CLAIMLOSSDATE, 
	CLAIMNUMBER, 
	CLAIMOCCURRENCESTATUSCODE, 
	INJURYTYPECODE, 
	CATASTROPHECODE, 
	INCURREDINDEMNITYAMOUNT, 
	INCURREDMEDICALAMOUNT, 
	CAUSEOFLOSS, 
	TYPEOFRECOVERYCODE, 
	JURISDICTIONSTATECODE, 
	BODYPARTCODE, 
	NATUREOFINJURYCODE, 
	CAUSEOFINJURYCODE, 
	PAIDINDEMNITYAMOUNT, 
	PAIDMEDICALAMOUNT, 
	DEDUCTIBLEREIMBURSEMENTAMOUNT, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	INCURREDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	SUBJECTPREMIUMTOTAL, 
	STANDARPREMIUMTOTAL, 
	CORRECTIONSEQNUMBER, 
	REPLACEMENTREPORTCODE, 
	CORRECTIONTYPECODE, 
	TYPEOFNONSTANDARDIDCODE, 
	PREVIOUSREPORTLEVELCODEREPORTNUMBER, 
	UPDATETYPECODE, 
	TYPEOFLOSS, 
	TYPEOFCLAIM, 
	TYPEOFSETTLEMENT, 
	MANAGEDCAREORGANIZATIONTYPE, 
	VOCATIONALREHABINDICATOR, 
	LUMPSUMINDICATOR, 
	FRAUDULENTCLAIMCODE, 
	CLAIMANTNUMBER, 
	RATEEFFECTIVEDATE, 
	OCCUPATIONDESCRIPTION, 
	WEEKLYWAGEAMOUNT, 
	EMPLOYERATTORNEYFEES
	FROM RTR_Split_Insert
),