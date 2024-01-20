WITH
LKP_cause_of_injury_sup AS (
	SELECT
	cause_of_inj_code,
	cause_of_inj_support_id
	FROM (
		SELECT 
			cause_of_inj_code,
			cause_of_inj_support_id
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.cause_of_injury_sup
		WHERE crrnt_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cause_of_inj_support_id ORDER BY cause_of_inj_code) = 1
),
SQ_WcrbWorkTable_Type3 AS (
	SELECT 
	       PolKey
	      ,PolicyEffectiveDae
	      ,NameOfInsured
	      ,StateCode
	      ,AccidentYear
	      ,ClaimNum
	      ,ClaimClosedDate
	      ,ClaimOccurrenceStatusCode
	      ,PaidLossAmt
	      ,TypeLossCode
	 	,Premiummasterrundate
	      ,sum(IndemnityPaymentAmount) as IndemnityPaymentAmount
	      ,sum(MedicalPaymentAmount) as MedicalPaymentAmount
	      ,RecoveryAmount
	     ,IndemnityReserveAmount
		 ,MedicalReserveAmount
	      ,TotalMedicalAmount
	      ,TotalIndemnityAmount
	      ,CauseOfInjSupportId
	      ,InjrDescPartOfBody
	      ,InjrDescNatureOfInjr
	      ,ClassCode
	      ,OutstandingAmount
	      ,ExtractDate
	      ,SourceSystemId
	      ,AuditID
	  FROM WcrbWorkTable
	Where PremiumMasterCalculationID = -1 AND @{pipeline().parameters.EXTRACTDATE}
	--AND ClaimNum like  ('%AF83770%')
	  group by 
	   PolKey
	      ,PolicyEffectiveDae
	      ,NameOfInsured
	      ,StateCode
	      ,AccidentYear
	      ,ClaimNum
	      ,ClaimClosedDate
	      ,ClaimOccurrenceStatusCode
	      ,PaidLossAmt
	      ,TypeLossCode
		 ,Premiummasterrundate
	    --  ,IndemnityPaymentAmount
	      ,MedicalPaymentAmount
	      ,RecoveryAmount
	      ,IndemnityReserveAmount
	      ,MedicalReserveAmount
	      ,TotalMedicalAmount
	      ,TotalIndemnityAmount
	      ,CauseOfInjSupportId
	      ,InjrDescPartOfBody
	      ,InjrDescNatureOfInjr
	      ,ClassCode
	      ,OutstandingAmount
	      ,ExtractDate
	      ,SourceSystemId
	      ,AuditID
),
AGG_RecordType3 AS (
	SELECT
	PolKey,
	PolicyEffectiveDae,
	NameOfInsured,
	StateCode,
	AccidentYear,
	ClaimNum,
	ClaimClosedDate,
	ClaimOccurrenceStatusCode,
	PaidLossAmt AS i_PaidLossAmt,
	TypeLossCode AS i_TypeLossCode,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	IndemnityPaymentAmount AS i_IndemnityPaymentAmount,
	MedicalPaymentAmount AS i_MedicalPaymentAmount,
	RecoveryAmount AS i_RecoveryAmount,
	IndemnityReserveAmount AS i_IndemnityReserveAmount,
	MedicalReserveAmount AS i_MedicalReserveAmount,
	TotalMedicalAmount AS i_TotalMedicalAmount,
	TotalIndemnityAmount AS i_TotalIndemnityAmount,
	CauseOfInjSupportId AS i_CauseOfInjSupportId,
	InjrDescPartOfBody AS i_InjrDescPartOfBody,
	InjrDescNatureOfInjr AS i_InjrDescNatureOfInjr,
	ClassCode AS i_ClassCode,
	OutstandingAmt AS i_OutstandingAmt,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	AuditID AS i_AuditID,
	i_TypeLossCode AS o_TypeLossCode,
	-- *INF*: SUM(i_IndemnityPaymentAmount)
	SUM(i_IndemnityPaymentAmount) AS o_IndemnityPaymentAmount,
	-- *INF*: SUM(i_MedicalPaymentAmount)
	SUM(i_MedicalPaymentAmount) AS o_MedicalPaymentAmount,
	-- *INF*: SUM(i_RecoveryAmount)
	SUM(i_RecoveryAmount) AS o_RecoveryAmount,
	i_IndemnityReserveAmount AS o_IndemnityReserveAmount,
	i_MedicalReserveAmount AS o_MedicalReserveAmount,
	i_TotalMedicalAmount AS o_TotalMedicalAmount,
	i_TotalIndemnityAmount AS o_TotalIndemnityAmount,
	i_CauseOfInjSupportId AS o_CauseOfInjSupportId,
	i_InjrDescPartOfBody AS o_InjrDescPartOfBody,
	i_InjrDescNatureOfInjr AS o_InjrDescNatureOfInjr,
	i_ClassCode AS o_ClassCode,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_AuditID AS o_AuditID,
	-- *INF*: IIF(SUM(i_PaidLossAmt)+SUM(i_OutstandingAmt)>1000000,1,0)
	IFF(SUM(i_PaidLossAmt) + SUM(i_OutstandingAmt) > 1000000, 1, 0) AS o_Flag
	FROM SQ_WcrbWorkTable_Type3
	GROUP BY PolKey, PolicyEffectiveDae, NameOfInsured, StateCode, AccidentYear, ClaimNum, ClaimClosedDate, ClaimOccurrenceStatusCode, i_TypeLossCode, i_CauseOfInjSupportId, i_InjrDescPartOfBody, i_InjrDescNatureOfInjr, i_ClassCode
),
EXP_Value_Type3 AS (
	SELECT
	PolKey AS i_PolKey,
	PolicyEffectiveDae AS i_PolicyEffectiveDae,
	NameOfInsured AS i_NameOfInsured,
	StateCode AS i_StateCode,
	AccidentYear AS i_AccidentYear,
	ClaimNum AS i_ClaimNum,
	ClaimClosedDate AS i_ClaimClosedDate,
	ClaimOccurrenceStatusCode AS i_ClaimOccurrenceStatusCode,
	o_TypeLossCode AS i_TypeLossCode,
	o_IndemnityPaymentAmount AS i_IndemnityPaymentAmount,
	o_MedicalPaymentAmount AS i_MedicalPaymentAmount,
	o_RecoveryAmount AS i_RecoveryAmount,
	o_IndemnityReserveAmount AS i_IndemnityReserveAmount,
	o_MedicalReserveAmount AS i_MedicalReserveAmount,
	o_TotalMedicalAmount AS i_TotalMedicalAmount,
	o_TotalIndemnityAmount AS i_TotalIndemnityAmount,
	o_CauseOfInjSupportId AS i_CauseOfInjSupportId,
	o_InjrDescPartOfBody AS i_InjrDescPartOfBody,
	o_InjrDescNatureOfInjr AS i_InjrDescNatureOfInjr,
	o_ClassCode AS i_ClassCode,
	o_ExtractDate AS i_ExtractDate,
	o_SourceSystemId AS i_SourceSystemId,
	o_AuditID AS i_AuditID,
	o_Flag AS i_Flag,
	'03' AS o_RecordType,
	i_PolKey AS o_PolicyNumber,
	i_PolicyEffectiveDae AS o_PolicyEffectiveDate,
	i_NameOfInsured AS o_NameOfInsured,
	i_StateCode AS o_StateCode,
	i_AccidentYear AS o_AccidentYear,
	-- *INF*: SUBSTR(i_ClaimNum,1,18)
	SUBSTR(i_ClaimNum, 1, 18) AS o_ClaimNumber,
	i_ClaimClosedDate AS o_ClaimClosedDate,
	i_ClaimOccurrenceStatusCode AS o_ClaimStatus,
	i_ClassCode AS o_ClassCode,
	i_IndemnityPaymentAmount AS o_IndemnityPaymentAmount,
	i_MedicalPaymentAmount AS o_MedicalPaymentAmount,
	i_RecoveryAmount AS o_RecoveryAmount,
	i_IndemnityReserveAmount AS o_IndemnityReserveAmount,
	i_MedicalReserveAmount AS o_MedicalReserveAmount,
	i_MedicalPaymentAmount + i_MedicalReserveAmount AS o_TotalMedicalAmount,
	i_IndemnityPaymentAmount + i_IndemnityReserveAmount AS o_TotalIndemnityAmount,
	-- *INF*: IIF(i_Flag=1,IIF(LTRIM(i_TypeLossCode)='','00',i_TypeLossCode),'00')
	IFF(i_Flag = 1, IFF(
	        LTRIM(i_TypeLossCode) = '', '00', i_TypeLossCode
	    ), '00') AS o_InjuryDescriptionCodeForLossesIncurredGt1m,
	-- *INF*: IIF(i_Flag=1,IIF(LTRIM(i_InjrDescPartOfBody)='','00',i_InjrDescPartOfBody),'00')
	IFF(
	    i_Flag = 1,
	    IFF(
	        LTRIM(i_InjrDescPartOfBody) = '', '00', i_InjrDescPartOfBody
	    ),
	    '00'
	) AS o_InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m,
	-- *INF*: IIF(i_Flag=1,IIF(LTRIM(i_InjrDescNatureOfInjr)='','00',i_InjrDescNatureOfInjr),'00')
	IFF(
	    i_Flag = 1,
	    IFF(
	        LTRIM(i_InjrDescNatureOfInjr) = '', '00', i_InjrDescNatureOfInjr
	    ),
	    '00'
	) AS o_InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m,
	-- *INF*: RTRIM(LTRIM(:LKP.LKP_CAUSE_OF_INJURY_SUP(i_CauseOfInjSupportId)))
	RTRIM(LTRIM(LKP_CAUSE_OF_INJURY_SUP_i_CauseOfInjSupportId.cause_of_inj_code)) AS v_CauseOfInj,
	-- *INF*: IIF(i_Flag=1,IIF(v_CauseOfInj='N/A','00',v_CauseOfInj),'00')
	IFF(i_Flag = 1, IFF(
	        v_CauseOfInj = 'N/A', '00', v_CauseOfInj
	    ), '00') AS o_InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_AuditID AS o_AuditID
	FROM AGG_RecordType3
	LEFT JOIN LKP_CAUSE_OF_INJURY_SUP LKP_CAUSE_OF_INJURY_SUP_i_CauseOfInjSupportId
	ON LKP_CAUSE_OF_INJURY_SUP_i_CauseOfInjSupportId.cause_of_inj_support_id = i_CauseOfInjSupportId

),
TGT_WcrbCarrierPoolWIRecordType3_Insert AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbCarrierPoolWIRecordType3;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbCarrierPoolWIRecordType3
	(RecordType, PolicyNumber, PolicyEffectiveDate, NameOfInsured, StateCode, AccidentYear, ClaimNumber, ClaimClosedDate, ClaimStatus, ClassCode, IndemnityPaymentAmount, MedicalPaymentAmount, RecoveryAmount, IndemnityReserveAmount, MedicalReserveAmount, TotalMedicalAmount, TotalIndemnityAmount, InjuryDescriptionCodeForLossesIncurredGt1m, InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m, InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m, InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m, ExtractDate, SourceSystemId, AuditID)
	SELECT 
	o_RecordType AS RECORDTYPE, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyEffectiveDate AS POLICYEFFECTIVEDATE, 
	o_NameOfInsured AS NAMEOFINSURED, 
	o_StateCode AS STATECODE, 
	o_AccidentYear AS ACCIDENTYEAR, 
	o_ClaimNumber AS CLAIMNUMBER, 
	o_ClaimClosedDate AS CLAIMCLOSEDDATE, 
	o_ClaimStatus AS CLAIMSTATUS, 
	o_ClassCode AS CLASSCODE, 
	o_IndemnityPaymentAmount AS INDEMNITYPAYMENTAMOUNT, 
	o_MedicalPaymentAmount AS MEDICALPAYMENTAMOUNT, 
	o_RecoveryAmount AS RECOVERYAMOUNT, 
	o_IndemnityReserveAmount AS INDEMNITYRESERVEAMOUNT, 
	o_MedicalReserveAmount AS MEDICALRESERVEAMOUNT, 
	o_TotalMedicalAmount AS TOTALMEDICALAMOUNT, 
	o_TotalIndemnityAmount AS TOTALINDEMNITYAMOUNT, 
	o_InjuryDescriptionCodeForLossesIncurredGt1m AS INJURYDESCRIPTIONCODEFORLOSSESINCURREDGT1M, 
	o_InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m AS INJURYDESCRIPTIONCODEPARTOFBODYFORLOSSESINCURREDGT1M, 
	o_InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m AS INJURYDESCRIPTIONCODENATUREOFINJURYFORLOSSESINCURREDGT1M, 
	o_InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m AS INJURYDESCRIPTIONCODECAUSEOFINJURYFORLOSSESINCURREDGT1M, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditID AS AUDITID
	FROM EXP_Value_Type3
),
SQ_WcrbWorkTable_Type2 AS (
	SELECT
		WCClaimantDetId,
		ClaimantId,
		ClaimOccurrenceId,
		ClaimPartyOccurrenceId,
		ClaimantCovDetId,
		LossMasterCalculationId,
		PolId,
		RiskLocationID,
		PolicyCoverageID,
		StatisticalCoverageID,
		PremiumTransactionID,
		BureauStatisticalCodeID,
		PremiumMasterCalculationID,
		PremiumMasterRunDate,
		PolKey,
		PolicyEffectiveDae,
		NameOfInsured,
		StateCode,
		WrittenPremium,
		EarnedPremium,
		UneranedPremium,
		DeferredPremium,
		AccidentYear,
		ClaimNum,
		ClaimClosedDate,
		ClaimOccurrenceStatusCode,
		PaidLossAmt,
		TypeLossCode,
		IndemnityPaymentAmount,
		MedicalPaymentAmount,
		RecoveryAmount,
		IndemnityReserveAmount,
		MedicalReserveAmount,
		TotalMedicalAmount,
		TotalIndemnityAmount,
		CauseOfInjSupportId,
		InjrDescLoss,
		BodyPartSupportId,
		InjrDescPartOfBody,
		NatureOfInjSupportId,
		InjrDescNatureOfInjr,
		TypeDisabilitySupportId,
		InjrDescCauseOfLoss,
		PremiumMasterAgencyCommissionRate,
		CommissionPaid,
		ClassCode,
		OutstandingAmount AS OutstandingAmt,
		ExtractDate,
		SourceSystemId,
		AuditID,
		AuditNonComplianceChargePremium
	FROM WcrbWorkTable
	WHERE @{pipeline().parameters.EXTRACTDATE} and LossMasterCalculationId=-1
),
AGG_RecordType2 AS (
	SELECT
	PolKey,
	PolicyEffectiveDae,
	NameOfInsured,
	StateCode,
	WrittenPremium AS i_WrittenPremium,
	EarnedPremium AS i_EarnedPremium,
	UneranedPremium AS i_UneranedPremium,
	DeferredPremium AS i_DeferredPremium,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	AuditID AS i_AuditID,
	AuditNonComplianceChargePremium AS i_AuditNonComplianceChargePremium,
	-- *INF*: SUM(i_WrittenPremium)
	SUM(i_WrittenPremium) AS o_AmountOfPremiumWritten,
	-- *INF*: SUM(i_EarnedPremium)
	SUM(i_EarnedPremium) AS o_AmountOfEarnedPremium,
	-- *INF*: SUM(i_UneranedPremium)
	SUM(i_UneranedPremium) AS o_AmountOfUnearnedPremium,
	i_DeferredPremium AS o_AmountOfPremiumDeferred,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_AuditID AS o_AuditID,
	-- *INF*: SUM(i_AuditNonComplianceChargePremium)
	SUM(i_AuditNonComplianceChargePremium) AS o_AuditNonComplianceChargePremium
	FROM SQ_WcrbWorkTable_Type2
	GROUP BY PolKey, PolicyEffectiveDae, NameOfInsured, StateCode
),
EXP_Value_Type2 AS (
	SELECT
	PolKey AS i_PolKey,
	PolicyEffectiveDae AS i_PolicyEffectiveDae,
	NameOfInsured AS i_NameOfInsured,
	StateCode AS i_StateCode,
	o_AmountOfPremiumWritten AS i_AmountOfPremiumWritten,
	o_AmountOfEarnedPremium AS i_AmountOfEarnedPremium,
	o_AmountOfUnearnedPremium AS i_AmountOfUnearnedPremium,
	o_AmountOfPremiumDeferred AS i_AmountOfPremiumDeferred,
	o_ExtractDate AS i_ExtractDate,
	o_SourceSystemId AS i_SourceSystemId,
	o_AuditID AS i_AuditID,
	o_AuditNonComplianceChargePremium AS i_AuditNonComplianceChargePremium,
	'02' AS o_RecordType,
	i_PolKey AS o_PolicyNumber,
	i_PolicyEffectiveDae AS o_PolicyEffectiveDate,
	i_NameOfInsured AS o_NameOfInsured,
	i_StateCode AS o_StateCode,
	-- *INF*: i_AmountOfPremiumWritten - i_AuditNonComplianceChargePremium
	-- --deduct ANC amounts computed for quarter from Written premium totals
	i_AmountOfPremiumWritten - i_AuditNonComplianceChargePremium AS o_AmountOfPremiumWrittenlessANC,
	i_AmountOfEarnedPremium AS o_AmountOfEarnedPremium,
	i_AmountOfUnearnedPremium AS o_AmountOfUnearnedPremium,
	i_AmountOfPremiumDeferred AS o_AmountOfPremiumDeferred,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_AuditID AS o_AuditID,
	i_AuditNonComplianceChargePremium AS o_AuditNonComplianceChargePremium
	FROM AGG_RecordType2
),
TGT_WcrbCarrierPoolWIRecordType2_Insert AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbCarrierPoolWIRecordType2;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbCarrierPoolWIRecordType2
	(RecordType, PolicyNumber, PolicyEffectiveDate, NameOfInsured, StateCode, AmountOfPremiumWritten, AmountOfEarnedPremium, AmountOfUnearnedPremium, AmountOfPremiumDeferred, ExtractDate, SourceSystemId, AuditID, AuditNonComplianceChargePremium)
	SELECT 
	o_RecordType AS RECORDTYPE, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyEffectiveDate AS POLICYEFFECTIVEDATE, 
	o_NameOfInsured AS NAMEOFINSURED, 
	o_StateCode AS STATECODE, 
	o_AmountOfPremiumWrittenlessANC AS AMOUNTOFPREMIUMWRITTEN, 
	o_AmountOfEarnedPremium AS AMOUNTOFEARNEDPREMIUM, 
	o_AmountOfUnearnedPremium AS AMOUNTOFUNEARNEDPREMIUM, 
	o_AmountOfPremiumDeferred AS AMOUNTOFPREMIUMDEFERRED, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_AuditID AS AUDITID, 
	o_AuditNonComplianceChargePremium AS AUDITNONCOMPLIANCECHARGEPREMIUM
	FROM EXP_Value_Type2
),
SQ_WcrbWorkTable_Type6 AS (
	SELECT
		WCClaimantDetId,
		ClaimantId,
		ClaimOccurrenceId,
		ClaimPartyOccurrenceId,
		ClaimantCovDetId,
		LossMasterCalculationId,
		PolId,
		RiskLocationID,
		PolicyCoverageID,
		StatisticalCoverageID,
		PremiumTransactionID,
		BureauStatisticalCodeID,
		PremiumMasterCalculationID,
		PremiumMasterRunDate,
		PolKey,
		PolicyEffectiveDae,
		NameOfInsured,
		StateCode,
		WrittenPremium,
		EarnedPremium,
		UneranedPremium,
		DeferredPremium,
		AccidentYear,
		ClaimNum,
		ClaimClosedDate,
		ClaimOccurrenceStatusCode,
		PaidLossAmt,
		TypeLossCode,
		IndemnityPaymentAmount,
		MedicalPaymentAmount,
		RecoveryAmount,
		IndemnityReserveAmount,
		MedicalReserveAmount,
		TotalMedicalAmount,
		TotalIndemnityAmount,
		CauseOfInjSupportId,
		InjrDescLoss,
		BodyPartSupportId,
		InjrDescPartOfBody,
		NatureOfInjSupportId,
		InjrDescNatureOfInjr,
		TypeDisabilitySupportId,
		InjrDescCauseOfLoss,
		PremiumMasterAgencyCommissionRate,
		CommissionPaid,
		ClassCode,
		OutstandingAmount AS OutstandingAmt,
		ExtractDate,
		SourceSystemId,
		AuditID
	FROM WcrbWorkTable
	WHERE @{pipeline().parameters.EXTRACTDATE} and LossMasterCalculationId=-999 and PremiumMasterCalculationID=-999
),
EXP_Value_Type6 AS (
	SELECT
	PolKey AS i_PolKey,
	PolicyEffectiveDae AS i_PolicyEffectiveDae,
	NameOfInsured AS i_NameOfInsured,
	StateCode AS i_StateCode,
	ExtractDate AS i_ExtractDate,
	SourceSystemId AS i_SourceSystemId,
	AuditID AS i_AuditID,
	'06' AS o_RecordType,
	i_PolKey AS o_PolicyNumber,
	i_PolicyEffectiveDae AS o_PolicyEffectiveDate,
	i_NameOfInsured AS o_NameOfInsured,
	i_StateCode AS o_StateCode,
	i_ExtractDate AS o_ExtractDate,
	i_SourceSystemId AS o_SourceSystemId,
	i_AuditID AS o_AuditID,
	CommissionPaid
	FROM SQ_WcrbWorkTable_Type6
),
EXP_Value_Commission AS (
	SELECT
	CommissionPaid AS i_CommissionPaid,
	o_RecordType AS RecordType,
	o_PolicyNumber AS PolicyNumber,
	o_PolicyEffectiveDate AS PolicyEffectiveDate,
	o_NameOfInsured AS NameOfInsured,
	o_StateCode AS StateCode,
	-- *INF*: IIF(ISNULL(i_CommissionPaid),0,i_CommissionPaid)
	IFF(i_CommissionPaid IS NULL, 0, i_CommissionPaid) AS o_CommissionPaid,
	o_ExtractDate AS ExtractDate,
	o_SourceSystemId AS SourceSystemId,
	o_AuditID AS AuditID
	FROM EXP_Value_Type6
),
TGT_WcrbCarrierPoolWIRecordType6_Insert AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbCarrierPoolWIRecordType6;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WcrbCarrierPoolWIRecordType6
	(RecordType, PolicyNumber, PolicyEffectiveDate, NameOfInsured, StateCode, CommissionPaid, ExtractDate, SourceSystemId, AuditID)
	SELECT 
	RECORDTYPE, 
	POLICYNUMBER, 
	POLICYEFFECTIVEDATE, 
	NAMEOFINSURED, 
	STATECODE, 
	o_CommissionPaid AS COMMISSIONPAID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXP_Value_Commission
),