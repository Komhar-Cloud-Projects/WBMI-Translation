WITH
SQ_WcrbCarrierPoolWIRecordType2 AS (
	SELECT
		WcrbCarrierPoolWIRecordType2Id,
		RecordType,
		PolicyNumber,
		PolicyEffectiveDate,
		NameOfInsured,
		StateCode,
		AmountOfPremiumWritten,
		AmountOfEarnedPremium,
		AmountOfUnearnedPremium,
		AuditNonComplianceChargePremium,
		AmountOfPremiumDeferred
	FROM WcrbCarrierPoolWIRecordType2
),
EXP_Pad_Type2 AS (
	SELECT
	RecordType AS i_RecordType,
	PolicyNumber AS i_PolicyNumber,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	NameOfInsured AS i_NameOfInsured,
	StateCode AS i_StateCode,
	AmountOfPremiumWritten AS i_AmountOfPremiumWritten,
	AmountOfEarnedPremium AS i_AmountOfEarnedPremium,
	AmountOfUnearnedPremium AS i_AmountOfUnearnedPremium,
	AuditNonComplianceChargePremium AS i_AuditNonComplianceChargePremium,
	AmountOfPremiumDeferred AS i_AmountOfPremiumDeferred,
	i_RecordType AS v_RecordType_1_2,
	-- *INF*: RPAD(i_PolicyNumber,18,' ')
	RPAD(i_PolicyNumber, 18, ' ') AS v_PolicyNumber_3_20,
	i_PolicyEffectiveDate AS v_PolicyEffectiveDate_21_28,
	-- *INF*: RPAD(i_NameOfInsured,90,' ')
	RPAD(i_NameOfInsured, 90, ' ') AS v_NameofInsured_29_118,
	i_StateCode AS v_StateCode_119_120,
	-- *INF*: IIF(i_AmountOfPremiumWritten<0,'-'||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumWritten,2)*100)),11,'0'),' '||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumWritten,2)*100)),11,'0'))
	IFF(
	    i_AmountOfPremiumWritten < 0,
	    '-' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumWritten, 2) * 100)), 11, '0'),
	    ' ' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumWritten, 2) * 100)), 11, '0')
	) AS v_AmountOfPremiumWritten_121_132,
	-- *INF*: IIF(i_AmountOfEarnedPremium<0,'-'||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfEarnedPremium,2)*100)),11,'0'),' '||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfEarnedPremium,2)*100)),11,'0'))
	IFF(
	    i_AmountOfEarnedPremium < 0,
	    '-' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfEarnedPremium, 2) * 100)), 11, '0'),
	    ' ' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfEarnedPremium, 2) * 100)), 11, '0')
	) AS v_AmountOfEarnedPremium_133_144,
	-- *INF*: IIF(i_AmountOfUnearnedPremium<0,'-'||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfUnearnedPremium,2)*100)),11,'0'),' '||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfUnearnedPremium,2)*100)),11,'0'))
	IFF(
	    i_AmountOfUnearnedPremium < 0,
	    '-' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfUnearnedPremium, 2) * 100)), 11, '0'),
	    ' ' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfUnearnedPremium, 2) * 100)), 11, '0')
	) AS v_AmountOfUnearnedPremium_145_156,
	-- *INF*: IIF(i_AuditNonComplianceChargePremium<0,'-'||LPAD(TO_CHAR(ABS(ROUND(i_AuditNonComplianceChargePremium,2)*100)),11,'0'),' '||LPAD(TO_CHAR(ABS(ROUND(i_AuditNonComplianceChargePremium,2)*100)),11,'0'))
	IFF(
	    i_AuditNonComplianceChargePremium < 0,
	    '-' || LPAD(TO_CHAR(ABS(ROUND(i_AuditNonComplianceChargePremium, 2) * 100)), 11, '0'),
	    ' ' || LPAD(TO_CHAR(ABS(ROUND(i_AuditNonComplianceChargePremium, 2) * 100)), 11, '0')
	) AS v_AuditNonComplianceChargePremium_157_168,
	-- *INF*: IIF(i_AmountOfPremiumDeferred<0,'-'||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumDeferred,2)*100)),11,'0'),' '||LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumDeferred,2)*100)),11,'0'))
	IFF(
	    i_AmountOfPremiumDeferred < 0,
	    '-' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumDeferred, 2) * 100)), 11, '0'),
	    ' ' || LPAD(TO_CHAR(ABS(ROUND(i_AmountOfPremiumDeferred, 2) * 100)), 11, '0')
	) AS v_AmountOfPremiumDeferred_169_180,
	-- *INF*: RPAD('',116,' ')
	RPAD('', 116, ' ') AS v_Filler_181_296,
	v_RecordType_1_2
 || '~'
||v_PolicyNumber_3_20
 || '~'
||v_PolicyEffectiveDate_21_28
 || '~'
||v_NameofInsured_29_118
 || '~'
||v_StateCode_119_120
 || '~'
||v_AmountOfPremiumWritten_121_132
 || '~'
||v_AmountOfEarnedPremium_133_144
 || '~'
||v_AmountOfUnearnedPremium_145_156
 || '~'
||v_AuditNonComplianceChargePremium_157_168
 || '~'
||v_AmountOfPremiumDeferred_169_180
 || '~'
||v_Filler_181_296 AS o_Records
	FROM SQ_WcrbCarrierPoolWIRecordType2
),
TGT_WCRB_DataFeed_Type2 AS (
	INSERT INTO DataFeed_FlatFile
	(Record)
	SELECT 
	o_Records AS RECORD
	FROM EXP_Pad_Type2
),
SQ_WcrbCarrierPoolWIRecordType3 AS (
	SELECT
		WcrbCarrierPoolWIRecordType3Id,
		RecordType,
		PolicyNumber,
		PolicyEffectiveDate,
		NameOfInsured,
		StateCode,
		AccidentYear,
		ClaimNumber,
		ClaimClosedDate,
		ClaimStatus,
		ClassCode,
		IndemnityPaymentAmount,
		MedicalPaymentAmount,
		RecoveryAmount,
		IndemnityReserveAmount,
		MedicalReserveAmount,
		TotalMedicalAmount,
		TotalIndemnityAmount,
		InjuryDescriptionCodeForLossesIncurredGt1m,
		InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m,
		InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m,
		InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m,
		ExtractDate,
		SourceSystemId,
		AuditID
	FROM WcrbCarrierPoolWIRecordType3
),
AGGTRANS AS (
	SELECT
	RecordType,
	PolicyNumber,
	PolicyEffectiveDate,
	NameOfInsured,
	StateCode,
	AccidentYear,
	ClaimNumber,
	ClaimClosedDate,
	ClaimStatus,
	ClassCode,
	IndemnityPaymentAmount AS i_IndemnityPaymentAmount,
	MedicalPaymentAmount AS i_MedicalPaymentAmount,
	RecoveryAmount AS i_RecoveryAmount,
	IndemnityReserveAmount AS i_IndemnityReserveAmount,
	MedicalReserveAmount AS i_MedicalReserveAmount,
	TotalMedicalAmount AS i_TotalMedicalAmount,
	TotalIndemnityAmount AS i_TotalIndemnityAmount,
	-- *INF*: SUM(i_IndemnityPaymentAmount)
	SUM(i_IndemnityPaymentAmount) AS o_IndemnityPaymentAmount,
	-- *INF*: SUM(i_MedicalPaymentAmount)
	SUM(i_MedicalPaymentAmount) AS o_MedicalPaymentAmount,
	-- *INF*: SUM(i_RecoveryAmount)
	SUM(i_RecoveryAmount) AS o_RecoveryAmount,
	-- *INF*: SUM(i_IndemnityReserveAmount)
	SUM(i_IndemnityReserveAmount) AS o_IndemnityReserveAmount,
	-- *INF*: SUM(i_MedicalReserveAmount)
	SUM(i_MedicalReserveAmount) AS o_MedicalReserveAmount,
	-- *INF*: SUM(i_TotalMedicalAmount)
	SUM(i_TotalMedicalAmount) AS o_TotalMedicalAmount,
	-- *INF*: SUM(i_TotalIndemnityAmount)
	SUM(i_TotalIndemnityAmount) AS o_TotalIndemnityAmount,
	InjuryDescriptionCodeForLossesIncurredGt1m,
	InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m,
	InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m,
	InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m
	FROM SQ_WcrbCarrierPoolWIRecordType3
	GROUP BY RecordType, PolicyNumber, PolicyEffectiveDate, NameOfInsured, StateCode, AccidentYear, ClaimNumber, ClaimClosedDate, ClaimStatus, ClassCode
),
EXP_Pad_Type3 AS (
	SELECT
	RecordType AS i_RecordType,
	PolicyNumber AS i_PolicyNumber,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	NameOfInsured AS i_NameOfInsured,
	StateCode AS i_StateCode,
	AccidentYear AS i_AccidentYear,
	ClaimNumber AS i_ClaimNumber,
	ClaimClosedDate AS i_ClaimClosedDate,
	ClaimStatus AS i_ClaimStatus,
	ClassCode AS i_ClassCode,
	o_IndemnityPaymentAmount AS i_IndemnityPaymentAmount,
	o_MedicalPaymentAmount AS i_MedicalPaymentAmount,
	o_RecoveryAmount AS i_RecoveryAmount,
	o_IndemnityReserveAmount AS i_IndemnityReserveAmount,
	o_MedicalReserveAmount AS i_MedicalReserveAmount,
	o_TotalMedicalAmount AS i_TotalMedicalAmount,
	o_TotalIndemnityAmount AS i_TotalIndemnityAmount,
	InjuryDescriptionCodeForLossesIncurredGt1m AS i_InjuryDescriptionCodeForLossesIncurredGt1m,
	InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m AS i_InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m,
	InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m AS i_InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m,
	InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m AS i_InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m,
	i_RecordType AS v_RecordType_1_2,
	-- *INF*: RPAD(i_PolicyNumber,18,' ')
	RPAD(i_PolicyNumber, 18, ' ') AS v_PolicyNumber_3_20,
	i_PolicyEffectiveDate AS v_PolicyEffectiveDate_21_28,
	-- *INF*: RPAD(i_NameOfInsured,90,' ')
	RPAD(i_NameOfInsured, 90, ' ') AS v_NameOfInsured_29_118,
	i_StateCode AS v_StateCode_119_120,
	-- *INF*: RPAD(i_AccidentYear,4,' ')
	RPAD(i_AccidentYear, 4, ' ') AS v_AccidentYear_121_124,
	-- *INF*: RPAD(i_ClaimNumber,18,' ')
	RPAD(i_ClaimNumber, 18, ' ') AS v_ClaimNumber_125_142,
	i_ClaimClosedDate AS v_ClaimClosedDate_143_150,
	i_ClaimStatus AS v_ClaimStatus_151,
	-- *INF*: RPAD(i_ClassCode,4,' ')
	RPAD(i_ClassCode, 4, ' ') AS v_ClassCode_152_155,
	-- *INF*: LPAD(TO_CHAR(ABS(ROUND(i_IndemnityPaymentAmount,2)*100)),11,'0')
	LPAD(TO_CHAR(ABS(ROUND(i_IndemnityPaymentAmount, 2) * 100)), 11, '0') AS v_IndemnityPaymentAmount,
	-- *INF*: IIF(i_IndemnityPaymentAmount<0,'-'||v_IndemnityPaymentAmount,' '||v_IndemnityPaymentAmount)
	IFF(
	    i_IndemnityPaymentAmount < 0, '-' || v_IndemnityPaymentAmount,
	    ' ' || v_IndemnityPaymentAmount
	) AS v_IndemnityPaymentAmount_156_167,
	-- *INF*: LPAD(TO_CHAR(ABS(ROUND(i_MedicalPaymentAmount,2)*100)),11,'0')
	LPAD(TO_CHAR(ABS(ROUND(i_MedicalPaymentAmount, 2) * 100)), 11, '0') AS v_MedicalPaymentAmount,
	-- *INF*: IIF(i_MedicalPaymentAmount<0,'-'||v_MedicalPaymentAmount,' '||v_MedicalPaymentAmount)
	IFF(
	    i_MedicalPaymentAmount < 0, '-' || v_MedicalPaymentAmount, ' ' || v_MedicalPaymentAmount
	) AS v_MedicalPaymentAmount_168_179,
	-- *INF*: LPAD(TO_CHAR(ABS(ROUND(i_RecoveryAmount,2)*100)),11,'0')
	LPAD(TO_CHAR(ABS(ROUND(i_RecoveryAmount, 2) * 100)), 11, '0') AS v_RecoveryAmount,
	-- *INF*: --updated per PROD-17809
	-- ' '||v_RecoveryAmount
	-- 
	-- --IIF(i_RecoveryAmount<0,'-'||v_RecoveryAmount,' '||v_RecoveryAmount)
	' ' || v_RecoveryAmount AS v_RecoveryAmount_180_191,
	i_IndemnityReserveAmount AS v_IndemnityReserve,
	-- *INF*: DECODE(TRUE,ISNULL(v_IndemnityReserve),'00000000000',LPAD(TO_CHAR(ABS(ROUND(v_IndemnityReserve,2)*100)),11,'0'))
	DECODE(
	    TRUE,
	    v_IndemnityReserve IS NULL, '00000000000',
	    LPAD(TO_CHAR(ABS(ROUND(v_IndemnityReserve, 2) * 100)), 11, '0')
	) AS v_IndemnityReserveAmount,
	-- *INF*: LPAD(TO_CHAR(ABS(ROUND(i_IndemnityReserveAmount,2)*100)),11,'0')
	LPAD(TO_CHAR(ABS(ROUND(i_IndemnityReserveAmount, 2) * 100)), 11, '0') AS v_IndemnityReserveAmount_new,
	-- *INF*: IIF(v_IndemnityReserve<0,'-'||v_IndemnityReserveAmount,' '||v_IndemnityReserveAmount)
	-- --IIF(v_IndemnityReserve<0,'-'||v_IndemnityReserveAmount,' '||v_IndemnityReserveAmount)
	-- 
	-- 
	-- 
	IFF(
	    v_IndemnityReserve < 0, '-' || v_IndemnityReserveAmount, ' ' || v_IndemnityReserveAmount
	) AS v_IndemnityReserveAmount_192_203,
	i_MedicalReserveAmount AS v_MedicalReserve,
	-- *INF*: DECODE(TRUE,ISNULL(v_MedicalReserve),'00000000000',LPAD(TO_CHAR(ABS(ROUND(v_MedicalReserve,2)*100)),11,'0'))
	-- 
	DECODE(
	    TRUE,
	    v_MedicalReserve IS NULL, '00000000000',
	    LPAD(TO_CHAR(ABS(ROUND(v_MedicalReserve, 2) * 100)), 11, '0')
	) AS v_MedicalReserveAmount,
	-- *INF*: IIF(v_MedicalReserve<0,'-'||v_MedicalReserveAmount,' '||v_MedicalReserveAmount)
	IFF(v_MedicalReserve < 0, '-' || v_MedicalReserveAmount, ' ' || v_MedicalReserveAmount) AS v_MedicalReserveAmount_204_215,
	-- *INF*: DECODE(TRUE,ISNULL(v_MedicalReserve),i_MedicalPaymentAmount,(i_MedicalPaymentAmount + v_MedicalReserve))
	DECODE(
	    TRUE,
	    v_MedicalReserve IS NULL, i_MedicalPaymentAmount,
	    (i_MedicalPaymentAmount + v_MedicalReserve)
	) AS v_TotalMedicalAmount_calc,
	-- *INF*: LPAD(TO_CHAR(ABS(ROUND(v_TotalMedicalAmount_calc,2)*100)),11,'0')
	LPAD(TO_CHAR(ABS(ROUND(v_TotalMedicalAmount_calc, 2) * 100)), 11, '0') AS v_TotalMedicalAmount,
	-- *INF*: IIF(v_TotalMedicalAmount_calc<0,'-'||v_TotalMedicalAmount,' '||v_TotalMedicalAmount)
	IFF(v_TotalMedicalAmount_calc < 0, '-' || v_TotalMedicalAmount, ' ' || v_TotalMedicalAmount) AS v_TotalMedicalAmount_216_227,
	-- *INF*: DECODE(TRUE,ISNULL(v_IndemnityReserve),i_IndemnityPaymentAmount,(i_IndemnityPaymentAmount + v_IndemnityReserve))
	DECODE(
	    TRUE,
	    v_IndemnityReserve IS NULL, i_IndemnityPaymentAmount,
	    (i_IndemnityPaymentAmount + v_IndemnityReserve)
	) AS v_TotalIndemnityAmount_calc,
	-- *INF*: LPAD(TO_CHAR(ABS(ROUND(v_TotalIndemnityAmount_calc,2)*100)),11,'0')
	LPAD(TO_CHAR(ABS(ROUND(v_TotalIndemnityAmount_calc, 2) * 100)), 11, '0') AS v_TotalIndemnityAmount,
	-- *INF*: IIF(v_TotalIndemnityAmount_calc<0,'-'||v_TotalIndemnityAmount,' '||v_TotalIndemnityAmount)
	IFF(
	    v_TotalIndemnityAmount_calc < 0, '-' || v_TotalIndemnityAmount,
	    ' ' || v_TotalIndemnityAmount
	) AS v_TotalIndemnityAmount_228_239,
	i_InjuryDescriptionCodeForLossesIncurredGt1m AS v_InjuryDescriptionCodeForLossesIncurredGt1m_240_241,
	i_InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m AS v_InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m_242_243,
	i_InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m AS v_InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m_244_245,
	i_InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m AS v_InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m_246_247,
	-- *INF*: RPAD('',49,' ')
	RPAD('', 49, ' ') AS v_Filler_248_296,
	v_RecordType_1_2
 || '~'
 || v_PolicyNumber_3_20
 || '~'
 || v_PolicyEffectiveDate_21_28
 || '~'
 || v_NameOfInsured_29_118
 || '~'
 || v_StateCode_119_120
 || '~'
 || v_AccidentYear_121_124
 || '~'
 || v_ClaimNumber_125_142
 || '~'
 || v_ClaimClosedDate_143_150
 || '~'
 || v_ClaimStatus_151
 || '~'
 || v_ClassCode_152_155
 || '~'
 || v_IndemnityPaymentAmount_156_167
 || '~'
 || v_MedicalPaymentAmount_168_179
 || '~'
 || v_RecoveryAmount_180_191
 || '~'
 || v_IndemnityReserveAmount_192_203
 || '~'
 || v_MedicalReserveAmount_204_215
 || '~'
 || v_TotalMedicalAmount_216_227
 || '~'
 || v_TotalIndemnityAmount_228_239
 || '~'
 || v_InjuryDescriptionCodeForLossesIncurredGt1m_240_241
 || '~'
 || v_InjuryDescriptionCodePartOfBodyForLossesIncurredGt1m_242_243
 || '~'
 || v_InjuryDescriptionCodeNatureOfInjuryForLossesIncurredGt1m_244_245
 || '~'
 || v_InjuryDescriptionCodeCauseOfInjuryForLossesIncurredGt1m_246_247
 || '~'
 || v_Filler_248_296 AS o_Records
	FROM AGGTRANS
),
TGT_WCRB_DataFeed_Type3 AS (
	INSERT INTO DataFeed_FlatFile
	(Record)
	SELECT 
	o_Records AS RECORD
	FROM EXP_Pad_Type3
),
SQ_WcrbCarrierPoolWIRecordType6 AS (
	SELECT
		WcrbCarrierPoolWIRecordType6Id,
		RecordType,
		PolicyNumber,
		PolicyEffectiveDate,
		NameOfInsured,
		StateCode,
		CommissionPaid,
		ExtractDate,
		SourceSystemId,
		AuditID
	FROM WcrbCarrierPoolWIRecordType6
),
EXP_Pad_Type6 AS (
	SELECT
	RecordType AS i_RecordType,
	PolicyNumber AS i_PolicyNumber,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	NameOfInsured AS i_NameOfInsured,
	StateCode AS i_StateCode,
	CommissionPaid AS i_CommissionPaid,
	i_RecordType AS v_RecordType_1_2,
	-- *INF*: RPAD(i_PolicyNumber,18,' ')
	RPAD(i_PolicyNumber, 18, ' ') AS v_PolicyNumber_3_20,
	i_PolicyEffectiveDate AS v_PolicyEffectiveDate_21_28,
	-- *INF*: RPAD(i_NameOfInsured,90,' ')
	RPAD(i_NameOfInsured, 90, ' ') AS v_NameOfInsured_29_118,
	i_StateCode AS v_StateCode_119_120,
	-- *INF*: IIF(i_CommissionPaid<0,'-'||LPAD(TO_CHAR(ABS(ROUND(i_CommissionPaid,2)*100)),11,'0'),' '||LPAD(TO_CHAR(ABS(ROUND(i_CommissionPaid,2)*100)),11,'0'))
	IFF(
	    i_CommissionPaid < 0, '-' || LPAD(TO_CHAR(ABS(ROUND(i_CommissionPaid, 2) * 100)), 11, '0'),
	    ' ' || LPAD(TO_CHAR(ABS(ROUND(i_CommissionPaid, 2) * 100)), 11, '0')
	) AS v_CommissionPaid_121_132,
	-- *INF*: RPAD('',164,' ')
	RPAD('', 164, ' ') AS v_Filler_133_296,
	v_RecordType_1_2
 || '~'
 || v_PolicyNumber_3_20
 || '~'
 || v_PolicyEffectiveDate_21_28
 || '~'
 || v_NameOfInsured_29_118
 || '~'
 || v_StateCode_119_120
 || '~'
 || v_CommissionPaid_121_132
 || '~'
 || v_Filler_133_296 AS o_Records
	FROM SQ_WcrbCarrierPoolWIRecordType6
),
TGT_WCRB_DataFeed_Type6 AS (
	INSERT INTO DataFeed_FlatFile
	(Record)
	SELECT 
	o_Records AS RECORD
	FROM EXP_Pad_Type6
),