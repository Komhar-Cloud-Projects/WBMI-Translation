WITH
SQ_Pif08Stage AS (
	SELECT
		Pif08StageId,
		ExtractDate,
		SourceSystemid AS SourceSystemId,
		PifSymbol,
		PifPolicyNumber,
		PifModule,
		WBMSBNDRecordId,
		WBMSBNDPartCode,
		WBMSBNDChangeDate,
		WBMSBNDEstimatedCompletionDate,
		WBMSBNDFormOfRenewal,
		WBMSBNDTypeBond,
		WBMSBNDClassCode,
		WBMSBNDRatePlan,
		WBMSBNDRateClass,
		WBMSBNDMonthSurchargePercent,
		WBMSBNDNumberOfMonthsSurchargeApplied,
		WBMSBNDBondPenalty,
		WBMSBNDPaymentBondPenalty,
		WBMSBNDContractPrice,
		WBMSBNDFinalContractPrice,
		WBMSBNDContractNumber,
		WBMSBNDApprovedBy,
		WBMSBNDMultiPrinciples,
		WBMSBNDMultiObligees,
		WBMSBNDLetterOfCredit,
		WBMSBNDSetAsideLetter,
		WBMSBNDIndemnity,
		WBMSBNDCollateral,
		WBMSBNDCancellable,
		WBMSBNDJointVenture,
		WBMSBNDPeriodOfJointVenture,
		WBMSBNDConsentToRatePremium,
		WBMSBNDIndividualRiskModificationFactor,
		WBMSBNDCommission,
		WBMSBNDDescription1,
		WBMSBNDDescription2,
		WBMSBNDDescription3,
		WBMSBNDExecutiveOrdersAmount,
		WBMSBNDFutureUse
	FROM Pif08Stage
),
EXP_MetaData AS (
	SELECT
	Pif08StageId,
	ExtractDate,
	SourceSystemId,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	WBMSBNDRecordId,
	WBMSBNDPartCode,
	WBMSBNDChangeDate,
	WBMSBNDEstimatedCompletionDate,
	WBMSBNDFormOfRenewal,
	WBMSBNDTypeBond,
	WBMSBNDClassCode,
	WBMSBNDRatePlan,
	WBMSBNDRateClass,
	WBMSBNDMonthSurchargePercent,
	WBMSBNDNumberOfMonthsSurchargeApplied,
	WBMSBNDBondPenalty,
	WBMSBNDPaymentBondPenalty,
	WBMSBNDContractPrice,
	WBMSBNDFinalContractPrice,
	WBMSBNDContractNumber,
	WBMSBNDApprovedBy,
	WBMSBNDMultiPrinciples,
	WBMSBNDMultiObligees,
	WBMSBNDLetterOfCredit,
	WBMSBNDSetAsideLetter,
	WBMSBNDIndemnity,
	WBMSBNDCollateral,
	WBMSBNDCancellable,
	WBMSBNDJointVenture,
	WBMSBNDPeriodOfJointVenture,
	WBMSBNDConsentToRatePremium,
	WBMSBNDIndividualRiskModificationFactor,
	WBMSBNDCommission,
	WBMSBNDDescription1,
	WBMSBNDDescription2,
	WBMSBNDDescription3,
	WBMSBNDExecutiveOrdersAmount,
	WBMSBNDFutureUse,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_Pif08Stage
),
ArchPif08Stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchPif08Stage
	(ExtractDate, SourceSystemId, AuditId, Pif08StageId, PifSymbol, PifPolicyNumber, PifModule, WBMSBNDRecordId, WBMSBNDPartCode, WBMSBNDChangeDate, WBMSBNDEstimatedCompletionDate, WBMSBNDFormOfRenewal, WBMSBNDTypeBond, WBMSBNDClassCode, WBMSBNDRatePlan, WBMSBNDRateClass, WBMSBNDMonthSurchargePercent, WBMSBNDNumberOfMonthsSurchargeApplied, WBMSBNDBondPenalty, WBMSBNDPaymentBondPenalty, WBMSBNDContractPrice, WBMSBNDFinalContractPrice, WBMSBNDContractNumber, WBMSBNDApprovedBy, WBMSBNDMultiPrinciples, WBMSBNDMultiObligees, WBMSBNDLetterOfCredit, WBMSBNDSetAsideLetter, WBMSBNDIndemnity, WBMSBNDCollateral, WBMSBNDCancellable, WBMSBNDJointVenture, WBMSBNDPeriodOfJointVenture, WBMSBNDConsentToRatePremium, WBMSBNDIndividualRiskModificationFactor, WBMSBNDCommission, WBMSBNDDescription1, WBMSBNDDescription2, WBMSBNDDescription3, WBMSBNDExecutiveOrdersAmount, WBMSBNDFutureUse)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	PIF08STAGEID, 
	PIFSYMBOL, 
	PIFPOLICYNUMBER, 
	PIFMODULE, 
	WBMSBNDRECORDID, 
	WBMSBNDPARTCODE, 
	WBMSBNDCHANGEDATE, 
	WBMSBNDESTIMATEDCOMPLETIONDATE, 
	WBMSBNDFORMOFRENEWAL, 
	WBMSBNDTYPEBOND, 
	WBMSBNDCLASSCODE, 
	WBMSBNDRATEPLAN, 
	WBMSBNDRATECLASS, 
	WBMSBNDMONTHSURCHARGEPERCENT, 
	WBMSBNDNUMBEROFMONTHSSURCHARGEAPPLIED, 
	WBMSBNDBONDPENALTY, 
	WBMSBNDPAYMENTBONDPENALTY, 
	WBMSBNDCONTRACTPRICE, 
	WBMSBNDFINALCONTRACTPRICE, 
	WBMSBNDCONTRACTNUMBER, 
	WBMSBNDAPPROVEDBY, 
	WBMSBNDMULTIPRINCIPLES, 
	WBMSBNDMULTIOBLIGEES, 
	WBMSBNDLETTEROFCREDIT, 
	WBMSBNDSETASIDELETTER, 
	WBMSBNDINDEMNITY, 
	WBMSBNDCOLLATERAL, 
	WBMSBNDCANCELLABLE, 
	WBMSBNDJOINTVENTURE, 
	WBMSBNDPERIODOFJOINTVENTURE, 
	WBMSBNDCONSENTTORATEPREMIUM, 
	WBMSBNDINDIVIDUALRISKMODIFICATIONFACTOR, 
	WBMSBNDCOMMISSION, 
	WBMSBNDDESCRIPTION1, 
	WBMSBNDDESCRIPTION2, 
	WBMSBNDDESCRIPTION3, 
	WBMSBNDEXECUTIVEORDERSAMOUNT, 
	WBMSBNDFUTUREUSE
	FROM EXP_MetaData
),