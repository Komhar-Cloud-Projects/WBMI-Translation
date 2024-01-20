WITH
SQ_ISSGL_Loss AS (
	--loss
	SELECT DISTINCT ISSGeneralLiabilityExtract.issgeneralliabilityextractid, 
	                ISSGeneralLiabilityExtract.auditid, 
	                ISSGeneralLiabilityExtract.createddate, 
	                ISSGeneralLiabilityExtract.edwpremiummastercalculationpkid, 
	                ISSGeneralLiabilityExtract.edwlossmastercalculationpkid, 
	                ISSGeneralLiabilityExtract.typebureaucode, 
	                ISSGeneralLiabilityExtract.bureaulineofinsurance, 
	                ISSGeneralLiabilityExtract.bureaucompanynumber, 
	                ISSGeneralLiabilityExtract.stateprovincecode, 
	                ISSGeneralLiabilityExtract.premiummasterrundate, 
	                ISSGeneralLiabilityExtract.lossmasterrundate, 
	                ISSGeneralLiabilityExtract.policykey, 
	                ISSGeneralLiabilityExtract.premiummasterclasscode, 
	                ISSGeneralLiabilityExtract.lossmasterclasscode, 
	                ISSGeneralLiabilityExtract.claimnumber, 
	                ISSGeneralLiabilityExtract.claimantnumber, 
	                ISSGeneralLiabilityExtract.riskterritorycode, 
	                ISSGeneralLiabilityExtract.policyeffectivedate, 
	                ISSGeneralLiabilityExtract.causeofloss, 
	                ISSGeneralLiabilityExtract.sublinecode, 
	                ISSGeneralLiabilityExtract.packagemodificationadjustmentgroupdescription, 
	                ISSGeneralLiabilityExtract.premiummasterdirectwrittenpremiumamount, 
	                ISSGeneralLiabilityExtract.paidlossamount, 
	                ISSGeneralLiabilityExtract.outstandinglossamount, 
	                ISSGeneralLiabilityExtract.policyexpirationdate, 
	                ISSGeneralLiabilityExtract.inceptiontodatepaidlossamount, 
	                ISSGeneralLiabilityExtract.claimantcoveragedetailid, 
	                ISSGeneralLiabilityExtract.annualstatementlinenumber, 
	                ISSGeneralLiabilityExtract.writtenexposure, 
	                ISSGeneralLiabilityExtract.outstandingallocatedlossadjustmentexpenseamount, 
	                ISSGeneralLiabilityExtract.claimlossdate, 
	                ISSGeneralLiabilityExtract.typeofpolicycontract, 
	                ISSGeneralLiabilityExtract.claimsentryyear, 
	                ISSGeneralLiabilityExtract.paidallocatedlossadjustmentexpenseamount, 
	                ISSGeneralLiabilityExtract.zippostalcode, 
	                ISSGeneralLiabilityExtract.transactioneffectivedate , 
	                ISSGeneralLiabilityExtract.locationnumber 
	FROM            ISSGeneralLiabilityExtract 
	WHERE           edwlossmastercalculationpkid<>-1 
	@{pipeline().parameters.WHERE_CLAUSE_GL_LOSS}
),
EXP_GL_LOSS AS (
	SELECT
	ISSGeneralLiabilityExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate,
	TypeofPolicycontract,
	ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode,
	TransactionEffectiveDate,
	LocationNumber
	FROM SQ_ISSGL_Loss
),
SQ_ISSGL_Premium_RemoveDuplicate AS (
	WITH ROLLUP_TABLE_TEMP
	AS
	(SELECT
			SUM(PremiumMasterDirectWrittenPremiumAmount) ROLL_UP_DWP_AMT
			,MAX(ISSGeneralLiabilityExtractId) MAX_ISS_KEY
			,COUNT(1) AS count
		FROM ISSGeneralLiabilityExtract a
		WHERE a.edwpremiummastercalculationpkid <> -1
		@{pipeline().parameters.WHERE_CLAUSE_GL_PREMIUM}
		GROUP BY CONCAT(typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription, policyexpirationdate, claimantcoveragedetailid, annualstatementlinenumber, abs(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber)
		HAVING COUNT(1) > 1)
	
	
	SELECT
		ISSGeneralLiabilityExtract.issgeneralliabilityextractid
		,ISSGeneralLiabilityExtract.auditid
		,ISSGeneralLiabilityExtract.createddate
		,ISSGeneralLiabilityExtract.edwpremiummastercalculationpkid
		,ISSGeneralLiabilityExtract.edwlossmastercalculationpkid
		,ISSGeneralLiabilityExtract.typebureaucode
		,ISSGeneralLiabilityExtract.bureaulineofinsurance
		,ISSGeneralLiabilityExtract.bureaucompanynumber
		,ISSGeneralLiabilityExtract.stateprovincecode
		,ISSGeneralLiabilityExtract.premiummasterrundate
		,ISSGeneralLiabilityExtract.lossmasterrundate
		,ISSGeneralLiabilityExtract.policykey
		,ISSGeneralLiabilityExtract.premiummasterclasscode
		,ISSGeneralLiabilityExtract.lossmasterclasscode
		,ISSGeneralLiabilityExtract.claimnumber
		,ISSGeneralLiabilityExtract.claimantnumber
		,ISSGeneralLiabilityExtract.riskterritorycode
		,ISSGeneralLiabilityExtract.policyeffectivedate
		,ISSGeneralLiabilityExtract.causeofloss
		,ISSGeneralLiabilityExtract.sublinecode
		,ISSGeneralLiabilityExtract.packagemodificationadjustmentgroupdescription
		,ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT AS PremiumMasterDirectWrittenPremiumAmount
		,ISSGeneralLiabilityExtract.paidlossamount
		,ISSGeneralLiabilityExtract.outstandinglossamount
		,ISSGeneralLiabilityExtract.policyexpirationdate
		,ISSGeneralLiabilityExtract.inceptiontodatepaidlossamount
		,ISSGeneralLiabilityExtract.claimantcoveragedetailid
		,ISSGeneralLiabilityExtract.annualstatementlinenumber
		,CASE
			WHEN ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT > 0 THEN ABS(ISSGeneralLiabilityExtract.WrittenExposure)
			ELSE (-1 * ABS(ISSGeneralLiabilityExtract.WrittenExposure))
		END AS WrittenExposure
		,ISSGeneralLiabilityExtract.outstandingallocatedlossadjustmentexpenseamount
		,ISSGeneralLiabilityExtract.claimlossdate
		,ISSGeneralLiabilityExtract.typeofpolicycontract
		,ISSGeneralLiabilityExtract.claimsentryyear
		,ISSGeneralLiabilityExtract.paidallocatedlossadjustmentexpenseamount
		,ISSGeneralLiabilityExtract.zippostalcode
		,ISSGeneralLiabilityExtract.transactioneffectivedate
		,ISSGeneralLiabilityExtract.locationnumber
	FROM ROLLUP_TABLE_TEMP
	INNER JOIN ISSGeneralLiabilityExtract
		ON ISSGeneralLiabilityExtract.ISSGeneralLiabilityExtractId = ROLLUP_TABLE_TEMP.MAX_ISS_KEY
	WHERE ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT <> 0
),
EXP_Premium_Duplicate AS (
	SELECT
	ISSGeneralLiabilityExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate,
	TypeofPolicycontract,
	ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode,
	TransactionEffectiveDate,
	LocationNumber
	FROM SQ_ISSGL_Premium_RemoveDuplicate
),
SQ_ISSGL_Premium_Unique AS (
	with prem_unique AS 
	( 
	         SELECT   concat( typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription,policyexpirationdate, claimantcoveragedetailid,annualstatementlinenumber, abs(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber) concct, 
	                  count(*)                                                                                                                                                                                                        count 
	         FROM     ISSGeneralLiabilityExtract  a
			 WHERE    a.edwpremiummastercalculationpkid<>-1 
			 @{pipeline().parameters.WHERE_CLAUSE_GL_PREMIUM}
	         GROUP BY concat( typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription,policyexpirationdate, claimantcoveragedetailid,annualstatementlinenumber, abs(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber) 
	         HAVING   count(*) =1 
			) 
			--premiumunique 
	SELECT     ISSGeneralLiabilityExtract.issgeneralliabilityextractid, 
	           ISSGeneralLiabilityExtract.auditid, 
	           ISSGeneralLiabilityExtract.createddate, 
	           ISSGeneralLiabilityExtract.edwpremiummastercalculationpkid, 
	           ISSGeneralLiabilityExtract.edwlossmastercalculationpkid, 
	           ISSGeneralLiabilityExtract.typebureaucode, 
	           ISSGeneralLiabilityExtract.bureaulineofinsurance, 
	           ISSGeneralLiabilityExtract.bureaucompanynumber, 
	           ISSGeneralLiabilityExtract.stateprovincecode, 
	           ISSGeneralLiabilityExtract.premiummasterrundate, 
	           ISSGeneralLiabilityExtract.lossmasterrundate, 
	           ISSGeneralLiabilityExtract.policykey, 
	           ISSGeneralLiabilityExtract.premiummasterclasscode, 
	           ISSGeneralLiabilityExtract.lossmasterclasscode, 
	           ISSGeneralLiabilityExtract.claimnumber, 
	           ISSGeneralLiabilityExtract.claimantnumber, 
	           ISSGeneralLiabilityExtract.riskterritorycode, 
	           ISSGeneralLiabilityExtract.policyeffectivedate, 
	           ISSGeneralLiabilityExtract.causeofloss, 
	           ISSGeneralLiabilityExtract.sublinecode, 
	           ISSGeneralLiabilityExtract.packagemodificationadjustmentgroupdescription, 
	           ISSGeneralLiabilityExtract.premiummasterdirectwrittenpremiumamount, 
	           ISSGeneralLiabilityExtract.paidlossamount, 
	           ISSGeneralLiabilityExtract.outstandinglossamount, 
	           ISSGeneralLiabilityExtract.policyexpirationdate, 
	           ISSGeneralLiabilityExtract.inceptiontodatepaidlossamount, 
	           ISSGeneralLiabilityExtract.claimantcoveragedetailid, 
	           ISSGeneralLiabilityExtract.annualstatementlinenumber, 
	           ISSGeneralLiabilityExtract.writtenexposure, 
	           ISSGeneralLiabilityExtract.outstandingallocatedlossadjustmentexpenseamount, 
	           ISSGeneralLiabilityExtract.claimlossdate, 
	           ISSGeneralLiabilityExtract.typeofpolicycontract, 
	           ISSGeneralLiabilityExtract.claimsentryyear, 
	           ISSGeneralLiabilityExtract.paidallocatedlossadjustmentexpenseamount, 
	           ISSGeneralLiabilityExtract.zippostalcode, 
	           ISSGeneralLiabilityExtract.transactioneffectivedate , 
	           ISSGeneralLiabilityExtract.locationnumber 
	FROM       ISSGeneralLiabilityExtract 
	INNER JOIN prem_unique b 
	ON         ( 
	                                 Concat( typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription, policyexpirationdate, claimantcoveragedetailid,annualstatementlinenumber, abs(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber) =b.concct) 
	WHERE      ISSGeneralLiabilityExtract.edwpremiummastercalculationpkid<>-1 
	@{pipeline().parameters.WHERE_CLAUSE_GL_PREMIUM}
),
EXP_Premium_Unique AS (
	SELECT
	ISSGeneralLiabilityExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate,
	TypeofPolicycontract,
	ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode,
	TransactionEffectiveDate,
	LocationNumber
	FROM SQ_ISSGL_Premium_Unique
),
Union_GL_Data AS (
	SELECT ISSGeneralLiabilityExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber
	FROM EXP_GL_LOSS
	UNION
	SELECT ISSGeneralLiabilityExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber
	FROM EXP_Premium_Unique
	UNION
	SELECT ISSGeneralLiabilityExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber
	FROM EXP_Premium_Duplicate
),
EXP_CleanData_GL AS (
	SELECT
	ISSGeneralLiabilityExtractId AS i_WorkISSExtractId,
	AuditId AS i_AuditId,
	CreatedDate AS i_CreatedDate,
	EDWPremiumMasterCalculationPKId AS i_EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId AS i_EDWLossMasterCalculationPKId,
	TypeBureauCode AS i_TypeBureauCode,
	BureauLineOfInsurance AS i_BureauLineOfInsurance,
	BureauCompanyNumber AS i_BureauCompanyNumber,
	StateProvinceCode AS i_StateProvinceCode,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	LossMasterRunDate AS i_LossMasterRunDate,
	PolicyKey AS i_PolicyKey,
	PremiumMasterClassCode AS i_PremiumMasterClassCode,
	LossMasterClassCode AS i_LossMasterClassCode,
	ClaimNumber AS i_ClaimNumber,
	ClaimantNumber AS i_ClaimantNumber,
	RiskTerritoryCode AS i_RiskTerritoryCode,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	CauseOfLoss AS i_CauseOfLoss,
	SublineCode AS i_SublineCode,
	PackageModificationAdjustmentGroupDescription AS i_PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount AS i_PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount AS i_PaidLossAmount,
	OutstandingLossAmount AS i_OutstandingLossAmount,
	PolicyExpirationDate AS i_PolicyExpirationDate,
	InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId AS i_ClaimantCoverageDetailId,
	AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	WrittenExposure AS i_WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount AS i_OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate AS i_Claimlossdate,
	TypeofPolicycontract AS i_TypeofPolicycontract,
	ClaimsEntryYear AS i_ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount AS i_PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode AS i_ZipPostalCode,
	TransactionEffectiveDate AS i_TransactionEffectiveDate,
	LocationNumber AS i_LocationNumber,
	i_WorkISSExtractId AS o_WorkISSExtractId,
	i_AuditId AS o_AuditId,
	i_CreatedDate AS o_CreatedDate,
	i_EDWPremiumMasterCalculationPKId AS o_EDWPremiumMasterCalculationPKId,
	i_EDWLossMasterCalculationPKId AS o_EDWLossMasterCalculationPKId,
	-- *INF*: LTRIM(RTRIM(i_TypeBureauCode))
	LTRIM(RTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: LTRIM(RTRIM(i_BureauLineOfInsurance))
	LTRIM(RTRIM(i_BureauLineOfInsurance)) AS o_BureauLineOfInsurance,
	-- *INF*: LTRIM(RTRIM(i_BureauCompanyNumber))
	LTRIM(RTRIM(i_BureauCompanyNumber)) AS o_BureauCompanyNumber,
	-- *INF*: LTRIM(RTRIM(i_StateProvinceCode))
	LTRIM(RTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	-- *INF*: LTRIM(RTRIM(i_PolicyKey))
	LTRIM(RTRIM(i_PolicyKey)) AS o_PolicyKey,
	-- *INF*: LTRIM(RTRIM(i_PremiumMasterClassCode))
	LTRIM(RTRIM(i_PremiumMasterClassCode)) AS o_PremiumMasterClassCode,
	-- *INF*: LTRIM(RTRIM(i_LossMasterClassCode))
	LTRIM(RTRIM(i_LossMasterClassCode)) AS o_LossMasterClassCode,
	-- *INF*: LTRIM(RTRIM(i_ClaimNumber))
	LTRIM(RTRIM(i_ClaimNumber)) AS o_ClaimNumber,
	-- *INF*: LTRIM(RTRIM(i_ClaimantNumber))
	LTRIM(RTRIM(i_ClaimantNumber)) AS o_ClaimantNumber,
	-- *INF*: LTRIM(RTRIM(i_RiskTerritoryCode))
	LTRIM(RTRIM(i_RiskTerritoryCode)) AS o_RiskTerritoryCode,
	i_PolicyEffectiveDate AS o_PolicyEffectiveDate,
	-- *INF*: LTRIM(RTRIM(i_CauseOfLoss))
	LTRIM(RTRIM(i_CauseOfLoss)) AS o_CauseOfLoss,
	-- *INF*: LTRIM(RTRIM(i_DeductibleAmount))
	LTRIM(RTRIM(i_DeductibleAmount)) AS o_DeductibleAmount,
	-- *INF*: LTRIM(RTRIM(i_CoverageCode))
	LTRIM(RTRIM(i_CoverageCode)) AS o_CoverageCode,
	-- *INF*: LTRIM(RTRIM(i_ConstructionCode))
	LTRIM(RTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: LTRIM(RTRIM(i_ISOFireProtectionCode))
	LTRIM(RTRIM(i_ISOFireProtectionCode)) AS o_ISOFireProtectionCode,
	-- *INF*: LTRIM(RTRIM(i_SublineCode))
	LTRIM(RTRIM(i_SublineCode)) AS o_SublineCode,
	-- *INF*: LTRIM(RTRIM(i_PackageModificationAdjustmentGroupDescription))
	LTRIM(RTRIM(i_PackageModificationAdjustmentGroupDescription)) AS o_PackageModificationAdjustmentGroupDescription,
	-- *INF*: LTRIM(RTRIM(i_PolicyForm))
	LTRIM(RTRIM(i_PolicyForm)) AS o_PolicyForm,
	i_PremiumMasterDirectWrittenPremiumAmount AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmount AS o_PaidLossAmount,
	i_OutstandingLossAmount AS o_OutstandingLossAmount,
	i_PolicyExpirationDate AS o_PolicyExpirationDate,
	i_InceptionToDatePaidLossAmount AS o_InceptionToDatePaidLossAmount,
	i_ClaimantCoverageDetailId AS o_ClaimantCoverageDetailID,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	i_WrittenExposure AS o_WrittenExposure,
	i_OutstandingAllocatedLossAdjustmentExpenseAmount AS o_OutstandingAllocatedLossAdjustmentExpenseAmount,
	i_Claimlossdate AS o_Claimlossdate,
	-- *INF*: LTRIM(RTRIM(i_TypeofPolicycontract))
	LTRIM(RTRIM(i_TypeofPolicycontract)) AS o_TypeofPolicycontract,
	i_ClaimsEntryYear AS o_ClaimsEntryYear,
	i_PaidAllocatedlossAdjustmentExpenseAmount AS o_PaidAllocatedlossAdjustmentExpenseAmount,
	i_ZipPostalCode AS o_ZipPostalCode,
	i_TransactionEffectiveDate AS o_TransactionEffectiveDate,
	i_LocationNumber AS o_LocationNumber
	FROM Union_GL_Data
),
EXP_Target_GL AS (
	SELECT
	-- *INF*: TRUNC(sysdate,'DD')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	-- *INF*: 'ISS_GL_GL'||TO_CHAR(v_RunDate,'YYYYMMDD')||'.CSV'
	'ISS_GL_GL' || TO_CHAR(v_RunDate, 'YYYYMMDD') || '.CSV' AS FileName,
	o_WorkISSExtractId AS WorkISSExtractId,
	o_AuditId AS AuditId,
	o_CreatedDate AS CreatedDate,
	o_EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationPKId,
	o_EDWLossMasterCalculationPKId AS EDWLossMasterCalculationPKId,
	o_TypeBureauCode AS TypeBureauCode,
	o_BureauLineOfInsurance AS BureauLineOfInsurance,
	o_BureauCompanyNumber AS BureauCompanyNumber,
	o_StateProvinceCode AS StateProvinceCode,
	o_PremiumMasterRunDate AS PremiumMasterRunDate,
	o_LossMasterRunDate AS LossMasterRunDate,
	o_PolicyKey AS PolicyKey,
	o_PremiumMasterClassCode AS PremiumMasterClassCode,
	o_LossMasterClassCode AS LossMasterClassCode,
	o_ClaimNumber AS ClaimNumber,
	o_ClaimantNumber AS ClaimantNumber,
	o_RiskTerritoryCode AS RiskTerritoryCode,
	o_PolicyEffectiveDate AS PolicyEffectiveDate,
	o_CauseOfLoss AS CauseOfLoss,
	o_DeductibleAmount AS DeductibleAmount,
	o_CoverageCode AS CoverageCode,
	o_ConstructionCode AS ConstructionCode,
	o_ISOFireProtectionCode AS ISOFireProtectionCode,
	o_SublineCode AS SublineCode,
	o_PackageModificationAdjustmentGroupDescription AS PackageModificationAdjustmentGroupDescription,
	o_PolicyForm AS PolicyForm,
	o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount,
	o_PaidLossAmount AS PaidLossAmount,
	o_OutstandingLossAmount AS OutstandingLossAmount,
	o_PolicyExpirationDate AS PolicyExpirationDate,
	o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount,
	o_ClaimantCoverageDetailID AS ClaimantCoverageDetailID,
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	o_WrittenExposure AS WrittenExposure,
	o_OutstandingAllocatedLossAdjustmentExpenseAmount AS OutstandingAllocatedLossAdjustmentExpenseAmount,
	o_Claimlossdate AS Claimlossdate,
	o_TypeofPolicycontract AS TypeofPolicycontract,
	o_ClaimsEntryYear AS ClaimsEntryYear,
	o_PaidAllocatedlossAdjustmentExpenseAmount AS PaidAllocatedlossAdjustmentExpenseAmount,
	o_ZipPostalCode AS ZipPostalCode,
	o_TransactionEffectiveDate AS TransactionEffectiveDate,
	o_LocationNumber AS LocationNumber
	FROM EXP_CleanData_GL
),
SRT_FlatFile_GL AS (
	SELECT
	FileName, 
	WorkISSExtractId, 
	AuditId, 
	CreatedDate, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	TypeBureauCode, 
	BureauLineOfInsurance, 
	BureauCompanyNumber, 
	StateProvinceCode, 
	PremiumMasterRunDate, 
	LossMasterRunDate, 
	PolicyKey, 
	PremiumMasterClassCode, 
	LossMasterClassCode, 
	ClaimNumber, 
	ClaimantNumber, 
	RiskTerritoryCode, 
	PolicyEffectiveDate, 
	CauseOfLoss, 
	SublineCode, 
	PackageModificationAdjustmentGroupDescription, 
	PremiumMasterDirectWrittenPremiumAmount, 
	PaidLossAmount, 
	OutstandingLossAmount, 
	PolicyExpirationDate, 
	InceptionToDatePaidLossAmount, 
	ClaimantCoverageDetailID, 
	AnnualStatementLineNumber, 
	Claimlossdate, 
	TypeofPolicycontract, 
	ClaimsEntryYear, 
	WrittenExposure, 
	PaidAllocatedlossAdjustmentExpenseAmount, 
	OutstandingAllocatedLossAdjustmentExpenseAmount, 
	ZipPostalCode, 
	TransactionEffectiveDate, 
	LocationNumber
	FROM EXP_Target_GL
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, AnnualStatementLineNumber ASC, ZipPostalCode ASC
),
ISSFlatFile_GL AS (
	INSERT INTO ISSFlatFile_GL
	(FileName, WorkISSExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailID, AnnualStatementLineNumber, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, WrittenExposure, PaidAllocatedlossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber)
	SELECT 
	FILENAME, 
	WORKISSEXTRACTID, 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	CLAIMNUMBER, 
	CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	CLAIMLOSSDATE, 
	TYPEOFPOLICYCONTRACT, 
	CLAIMSENTRYYEAR, 
	WRITTENEXPOSURE, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	ZIPPOSTALCODE, 
	TRANSACTIONEFFECTIVEDATE, 
	LOCATIONNUMBER
	FROM SRT_FlatFile_GL
),
SQ_ISSPL_Loss AS (
	--loss
	SELECT DISTINCT ISSProfessionalLiabilityExtract.ISSProfessionalLiabilityExtractid, 
	                ISSProfessionalLiabilityExtract.auditid, 
	                ISSProfessionalLiabilityExtract.createddate, 
	                ISSProfessionalLiabilityExtract.edwpremiummastercalculationpkid, 
	                ISSProfessionalLiabilityExtract.edwlossmastercalculationpkid, 
	                ISSProfessionalLiabilityExtract.typebureaucode, 
	                ISSProfessionalLiabilityExtract.bureaulineofinsurance, 
	                ISSProfessionalLiabilityExtract.bureaucompanynumber, 
	                ISSProfessionalLiabilityExtract.stateprovincecode, 
	                ISSProfessionalLiabilityExtract.premiummasterrundate, 
	                ISSProfessionalLiabilityExtract.lossmasterrundate, 
	                ISSProfessionalLiabilityExtract.policykey, 
	                ISSProfessionalLiabilityExtract.premiummasterclasscode, 
	                ISSProfessionalLiabilityExtract.lossmasterclasscode, 
	                ISSProfessionalLiabilityExtract.claimnumber, 
	                ISSProfessionalLiabilityExtract.claimantnumber, 
	                ISSProfessionalLiabilityExtract.riskterritorycode, 
	                ISSProfessionalLiabilityExtract.policyeffectivedate, 
	                ISSProfessionalLiabilityExtract.causeofloss, 
	                ISSProfessionalLiabilityExtract.sublinecode, 
	                ISSProfessionalLiabilityExtract.packagemodificationadjustmentgroupdescription, 
	                ISSProfessionalLiabilityExtract.premiummasterdirectwrittenpremiumamount, 
	                ISSProfessionalLiabilityExtract.paidlossamount, 
	                ISSProfessionalLiabilityExtract.outstandinglossamount, 
	                ISSProfessionalLiabilityExtract.policyexpirationdate, 
	                ISSProfessionalLiabilityExtract.inceptiontodatepaidlossamount, 
	                ISSProfessionalLiabilityExtract.claimantcoveragedetailid, 
	                ISSProfessionalLiabilityExtract.annualstatementlinenumber, 
	                ISSProfessionalLiabilityExtract.writtenexposure, 
	                ISSProfessionalLiabilityExtract.outstandingallocatedlossadjustmentexpenseamount, 
	                ISSProfessionalLiabilityExtract.claimlossdate, 
	                ISSProfessionalLiabilityExtract.typeofpolicycontract, 
	                ISSProfessionalLiabilityExtract.claimsentryyear, 
	                ISSProfessionalLiabilityExtract.paidallocatedlossadjustmentexpenseamount, 
	                ISSProfessionalLiabilityExtract.zippostalcode, 
	                ISSProfessionalLiabilityExtract.transactioneffectivedate , 
	                ISSProfessionalLiabilityExtract.locationnumber 
	FROM            ISSProfessionalLiabilityExtract 
	WHERE           edwlossmastercalculationpkid<>-1 
	@{pipeline().parameters.WHERE_CLAUSE_PL_LOSS}
),
EXP_PL_LOSS AS (
	SELECT
	ISSProfessionalLiabilityExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate,
	TypeofPolicycontract,
	ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode,
	TransactionEffectiveDate,
	LocationNumber
	FROM SQ_ISSPL_Loss
),
SQ_ISSPL_Premium_RemoveDuplicate AS (
	WITH ROLLUP_TABLE_TEMP
	AS
	(SELECT
			SUM(PremiumMasterDirectWrittenPremiumAmount) ROLL_UP_DWP_AMT
			,MAX(ISSProfessionalLiabilityExtractid) MAX_ISS_KEY
		FROM ISSProfessionalLiabilityExtract
		WHERE EDWPremiumMasterCalculationPKId <> -1
		@{pipeline().parameters.WHERE_CLAUSE_PL_PREMIUM}
		GROUP BY CONCAT(typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription, ABS(premiummasterdirectwrittenpremiumamount), policyexpirationdate, claimantcoveragedetailid, annualstatementlinenumber, ABS(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber)
		HAVING COUNT(1) > 1)
	
	
	SELECT
		ISSProfessionalLiabilityExtract.ISSProfessionalLiabilityExtractid
		,ISSProfessionalLiabilityExtract.auditid
		,ISSProfessionalLiabilityExtract.createddate
		,ISSProfessionalLiabilityExtract.edwpremiummastercalculationpkid
		,ISSProfessionalLiabilityExtract.edwlossmastercalculationpkid
		,ISSProfessionalLiabilityExtract.typebureaucode
		,ISSProfessionalLiabilityExtract.bureaulineofinsurance
		,ISSProfessionalLiabilityExtract.bureaucompanynumber
		,ISSProfessionalLiabilityExtract.stateprovincecode
		,ISSProfessionalLiabilityExtract.premiummasterrundate
		,ISSProfessionalLiabilityExtract.lossmasterrundate
		,ISSProfessionalLiabilityExtract.policykey
		,ISSProfessionalLiabilityExtract.premiummasterclasscode
		,ISSProfessionalLiabilityExtract.lossmasterclasscode
		,ISSProfessionalLiabilityExtract.claimnumber
		,ISSProfessionalLiabilityExtract.claimantnumber
		,ISSProfessionalLiabilityExtract.riskterritorycode
		,ISSProfessionalLiabilityExtract.policyeffectivedate
		,ISSProfessionalLiabilityExtract.causeofloss
		,ISSProfessionalLiabilityExtract.sublinecode
		,ISSProfessionalLiabilityExtract.packagemodificationadjustmentgroupdescription
		,ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT AS PremiumMasterDirectWrittenPremiumAmount
		,ISSProfessionalLiabilityExtract.paidlossamount
		,ISSProfessionalLiabilityExtract.outstandinglossamount
		,ISSProfessionalLiabilityExtract.policyexpirationdate
		,ISSProfessionalLiabilityExtract.inceptiontodatepaidlossamount
		,ISSProfessionalLiabilityExtract.claimantcoveragedetailid
		,ISSProfessionalLiabilityExtract.annualstatementlinenumber
		,CASE
			WHEN ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT > 0 THEN ABS(ISSProfessionalLiabilityExtract.WrittenExposure)
			ELSE (-1 * ABS(ISSProfessionalLiabilityExtract.WrittenExposure))
		END AS WrittenExposure
		,ISSProfessionalLiabilityExtract.outstandingallocatedlossadjustmentexpenseamount
		,ISSProfessionalLiabilityExtract.claimlossdate
		,ISSProfessionalLiabilityExtract.typeofpolicycontract
		,ISSProfessionalLiabilityExtract.claimsentryyear
		,ISSProfessionalLiabilityExtract.paidallocatedlossadjustmentexpenseamount
		,ISSProfessionalLiabilityExtract.zippostalcode
		,ISSProfessionalLiabilityExtract.transactioneffectivedate
		,ISSProfessionalLiabilityExtract.locationnumber
	FROM ROLLUP_TABLE_TEMP
	INNER JOIN ISSProfessionalLiabilityExtract
		ON ISSProfessionalLiabilityExtract.ISSProfessionalLiabilityExtractid = ROLLUP_TABLE_TEMP.MAX_ISS_KEY
	WHERE ROLLUP_TABLE_TEMP.ROLL_UP_DWP_AMT <> 0
),
EXP_PL_Premium_RemoveDuplicate AS (
	SELECT
	ISSProfessionalLiabilityExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate,
	TypeofPolicycontract,
	ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode,
	TransactionEffectiveDate,
	LocationNumber
	FROM SQ_ISSPL_Premium_RemoveDuplicate
),
SQ_ISSPL_PremiumUnique AS (
	with prem_unique AS 
	( 
	         SELECT   CONCAT(typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription, ABS(premiummasterdirectwrittenpremiumamount), policyexpirationdate, claimantcoveragedetailid, annualstatementlinenumber, ABS(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber) concct, 
	                  count(*)                                                                                                                                                                                                        count 
	         FROM     ISSProfessionalLiabilityExtract  a
			 WHERE    a.edwpremiummastercalculationpkid<>-1 
			 @{pipeline().parameters.WHERE_CLAUSE_PL_PREMIUM}
	         GROUP BY CONCAT(typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription, ABS(premiummasterdirectwrittenpremiumamount), policyexpirationdate, claimantcoveragedetailid, annualstatementlinenumber, ABS(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber) 
	         HAVING   count(*) =1 
			) 
			--premiumunique 
	SELECT     ISSProfessionalLiabilityExtract.ISSProfessionalLiabilityExtractid, 
	           ISSProfessionalLiabilityExtract.auditid, 
	           ISSProfessionalLiabilityExtract.createddate, 
	           ISSProfessionalLiabilityExtract.edwpremiummastercalculationpkid, 
	           ISSProfessionalLiabilityExtract.edwlossmastercalculationpkid, 
	           ISSProfessionalLiabilityExtract.typebureaucode, 
	           ISSProfessionalLiabilityExtract.bureaulineofinsurance, 
	           ISSProfessionalLiabilityExtract.bureaucompanynumber, 
	           ISSProfessionalLiabilityExtract.stateprovincecode, 
	           ISSProfessionalLiabilityExtract.premiummasterrundate, 
	           ISSProfessionalLiabilityExtract.lossmasterrundate, 
	           ISSProfessionalLiabilityExtract.policykey, 
	           ISSProfessionalLiabilityExtract.premiummasterclasscode, 
	           ISSProfessionalLiabilityExtract.lossmasterclasscode, 
	           ISSProfessionalLiabilityExtract.claimnumber, 
	           ISSProfessionalLiabilityExtract.claimantnumber, 
	           ISSProfessionalLiabilityExtract.riskterritorycode, 
	           ISSProfessionalLiabilityExtract.policyeffectivedate, 
	           ISSProfessionalLiabilityExtract.causeofloss, 
	           ISSProfessionalLiabilityExtract.sublinecode, 
	           ISSProfessionalLiabilityExtract.packagemodificationadjustmentgroupdescription, 
	           ISSProfessionalLiabilityExtract.premiummasterdirectwrittenpremiumamount, 
	           ISSProfessionalLiabilityExtract.paidlossamount, 
	           ISSProfessionalLiabilityExtract.outstandinglossamount, 
	           ISSProfessionalLiabilityExtract.policyexpirationdate, 
	           ISSProfessionalLiabilityExtract.inceptiontodatepaidlossamount, 
	           ISSProfessionalLiabilityExtract.claimantcoveragedetailid, 
	           ISSProfessionalLiabilityExtract.annualstatementlinenumber, 
	           ISSProfessionalLiabilityExtract.writtenexposure, 
	           ISSProfessionalLiabilityExtract.outstandingallocatedlossadjustmentexpenseamount, 
	           ISSProfessionalLiabilityExtract.claimlossdate, 
	           ISSProfessionalLiabilityExtract.typeofpolicycontract, 
	           ISSProfessionalLiabilityExtract.claimsentryyear, 
	           ISSProfessionalLiabilityExtract.paidallocatedlossadjustmentexpenseamount, 
	           ISSProfessionalLiabilityExtract.zippostalcode, 
	           ISSProfessionalLiabilityExtract.transactioneffectivedate , 
	           ISSProfessionalLiabilityExtract.locationnumber 
	FROM       ISSProfessionalLiabilityExtract 
	INNER JOIN prem_unique b 
	ON         ( 
	                                 CONCAT(typebureaucode, bureaulineofinsurance, bureaucompanynumber, stateprovincecode, policykey, premiummasterclasscode, lossmasterclasscode, claimnumber, claimantnumber, riskterritorycode, policyeffectivedate, causeofloss, sublinecode, packagemodificationadjustmentgroupdescription, ABS(premiummasterdirectwrittenpremiumamount), policyexpirationdate, claimantcoveragedetailid, annualstatementlinenumber, ABS(WrittenExposure), claimlossdate, typeofpolicycontract, claimsentryyear, zippostalcode, transactioneffectivedate, locationnumber) =b.concct) 
	WHERE      ISSProfessionalLiabilityExtract.edwpremiummastercalculationpkid<>-1 
	@{pipeline().parameters.WHERE_CLAUSE_PL_PREMIUM}
),
EXP_PL_Premium_Unique AS (
	SELECT
	ISSProfessionalLiabilityExtractId,
	AuditId,
	CreatedDate,
	EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId,
	TypeBureauCode,
	BureauLineOfInsurance,
	BureauCompanyNumber,
	StateProvinceCode,
	PremiumMasterRunDate,
	LossMasterRunDate,
	PolicyKey,
	PremiumMasterClassCode,
	LossMasterClassCode,
	ClaimNumber,
	ClaimantNumber,
	RiskTerritoryCode,
	PolicyEffectiveDate,
	CauseOfLoss,
	SublineCode,
	PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount,
	OutstandingLossAmount,
	PolicyExpirationDate,
	InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId,
	AnnualStatementLineNumber,
	WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate,
	TypeofPolicycontract,
	ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode,
	TransactionEffectiveDate,
	LocationNumber
	FROM SQ_ISSPL_PremiumUnique
),
Union_PL_Data AS (
	SELECT ISSProfessionalLiabilityExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber
	FROM EXP_PL_LOSS
	UNION
	SELECT ISSProfessionalLiabilityExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber
	FROM EXP_PL_Premium_Unique
	UNION
	SELECT ISSProfessionalLiabilityExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailId, AnnualStatementLineNumber, WrittenExposure, OutstandingAllocatedLossAdjustmentExpenseAmount, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, PaidAllocatedlossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber
	FROM EXP_PL_Premium_RemoveDuplicate
),
EXP_CleanData_PL AS (
	SELECT
	ISSProfessionalLiabilityExtractId AS i_WorkISSExtractId,
	AuditId AS i_AuditId,
	CreatedDate AS i_CreatedDate,
	EDWPremiumMasterCalculationPKId AS i_EDWPremiumMasterCalculationPKId,
	EDWLossMasterCalculationPKId AS i_EDWLossMasterCalculationPKId,
	TypeBureauCode AS i_TypeBureauCode,
	BureauLineOfInsurance AS i_BureauLineOfInsurance,
	BureauCompanyNumber AS i_BureauCompanyNumber,
	StateProvinceCode AS i_StateProvinceCode,
	PremiumMasterRunDate AS i_PremiumMasterRunDate,
	LossMasterRunDate AS i_LossMasterRunDate,
	PolicyKey AS i_PolicyKey,
	PremiumMasterClassCode AS i_PremiumMasterClassCode,
	LossMasterClassCode AS i_LossMasterClassCode,
	ClaimNumber AS i_ClaimNumber,
	ClaimantNumber AS i_ClaimantNumber,
	RiskTerritoryCode AS i_RiskTerritoryCode,
	PolicyEffectiveDate AS i_PolicyEffectiveDate,
	CauseOfLoss AS i_CauseOfLoss,
	SublineCode AS i_SublineCode,
	PackageModificationAdjustmentGroupDescription AS i_PackageModificationAdjustmentGroupDescription,
	PremiumMasterDirectWrittenPremiumAmount AS i_PremiumMasterDirectWrittenPremiumAmount,
	PaidLossAmount AS i_PaidLossAmount,
	OutstandingLossAmount AS i_OutstandingLossAmount,
	PolicyExpirationDate AS i_PolicyExpirationDate,
	InceptionToDatePaidLossAmount AS i_InceptionToDatePaidLossAmount,
	ClaimantCoverageDetailId AS i_ClaimantCoverageDetailId,
	AnnualStatementLineNumber AS i_AnnualStatementLineNumber,
	WrittenExposure AS i_WrittenExposure,
	OutstandingAllocatedLossAdjustmentExpenseAmount AS i_OutstandingAllocatedLossAdjustmentExpenseAmount,
	Claimlossdate AS i_Claimlossdate,
	TypeofPolicycontract AS i_TypeofPolicycontract,
	ClaimsEntryYear AS i_ClaimsEntryYear,
	PaidAllocatedlossAdjustmentExpenseAmount AS i_PaidAllocatedlossAdjustmentExpenseAmount,
	ZipPostalCode AS i_ZipPostalCode,
	TransactionEffectiveDate AS i_TransactionEffectiveDate,
	LocationNumber AS i_LocationNumber,
	i_WorkISSExtractId AS o_WorkISSExtractId,
	i_AuditId AS o_AuditId,
	i_CreatedDate AS o_CreatedDate,
	i_EDWPremiumMasterCalculationPKId AS o_EDWPremiumMasterCalculationPKId,
	i_EDWLossMasterCalculationPKId AS o_EDWLossMasterCalculationPKId,
	-- *INF*: LTRIM(RTRIM(i_TypeBureauCode))
	LTRIM(RTRIM(i_TypeBureauCode)) AS o_TypeBureauCode,
	-- *INF*: LTRIM(RTRIM(i_BureauLineOfInsurance))
	LTRIM(RTRIM(i_BureauLineOfInsurance)) AS o_BureauLineOfInsurance,
	-- *INF*: LTRIM(RTRIM(i_BureauCompanyNumber))
	LTRIM(RTRIM(i_BureauCompanyNumber)) AS o_BureauCompanyNumber,
	-- *INF*: LTRIM(RTRIM(i_StateProvinceCode))
	LTRIM(RTRIM(i_StateProvinceCode)) AS o_StateProvinceCode,
	i_PremiumMasterRunDate AS o_PremiumMasterRunDate,
	i_LossMasterRunDate AS o_LossMasterRunDate,
	-- *INF*: LTRIM(RTRIM(i_PolicyKey))
	LTRIM(RTRIM(i_PolicyKey)) AS o_PolicyKey,
	-- *INF*: LTRIM(RTRIM(i_PremiumMasterClassCode))
	LTRIM(RTRIM(i_PremiumMasterClassCode)) AS o_PremiumMasterClassCode,
	-- *INF*: LTRIM(RTRIM(i_LossMasterClassCode))
	LTRIM(RTRIM(i_LossMasterClassCode)) AS o_LossMasterClassCode,
	-- *INF*: LTRIM(RTRIM(i_ClaimNumber))
	LTRIM(RTRIM(i_ClaimNumber)) AS o_ClaimNumber,
	-- *INF*: LTRIM(RTRIM(i_ClaimantNumber))
	LTRIM(RTRIM(i_ClaimantNumber)) AS o_ClaimantNumber,
	-- *INF*: LTRIM(RTRIM(i_RiskTerritoryCode))
	LTRIM(RTRIM(i_RiskTerritoryCode)) AS o_RiskTerritoryCode,
	i_PolicyEffectiveDate AS o_PolicyEffectiveDate,
	-- *INF*: LTRIM(RTRIM(i_CauseOfLoss))
	LTRIM(RTRIM(i_CauseOfLoss)) AS o_CauseOfLoss,
	-- *INF*: LTRIM(RTRIM(i_DeductibleAmount))
	LTRIM(RTRIM(i_DeductibleAmount)) AS o_DeductibleAmount,
	-- *INF*: LTRIM(RTRIM(i_CoverageCode))
	LTRIM(RTRIM(i_CoverageCode)) AS o_CoverageCode,
	-- *INF*: LTRIM(RTRIM(i_ConstructionCode))
	LTRIM(RTRIM(i_ConstructionCode)) AS o_ConstructionCode,
	-- *INF*: LTRIM(RTRIM(i_ISOFireProtectionCode))
	LTRIM(RTRIM(i_ISOFireProtectionCode)) AS o_ISOFireProtectionCode,
	-- *INF*: LTRIM(RTRIM(i_SublineCode))
	LTRIM(RTRIM(i_SublineCode)) AS o_SublineCode,
	-- *INF*: LTRIM(RTRIM(i_PackageModificationAdjustmentGroupDescription))
	LTRIM(RTRIM(i_PackageModificationAdjustmentGroupDescription)) AS o_PackageModificationAdjustmentGroupDescription,
	-- *INF*: LTRIM(RTRIM(i_PolicyForm))
	LTRIM(RTRIM(i_PolicyForm)) AS o_PolicyForm,
	i_PremiumMasterDirectWrittenPremiumAmount AS o_PremiumMasterDirectWrittenPremiumAmount,
	i_PaidLossAmount AS o_PaidLossAmount,
	i_OutstandingLossAmount AS o_OutstandingLossAmount,
	i_PolicyExpirationDate AS o_PolicyExpirationDate,
	i_InceptionToDatePaidLossAmount AS o_InceptionToDatePaidLossAmount,
	i_ClaimantCoverageDetailId AS o_ClaimantCoverageDetailID,
	i_AnnualStatementLineNumber AS o_AnnualStatementLineNumber,
	i_WrittenExposure AS o_WrittenExposure,
	i_OutstandingAllocatedLossAdjustmentExpenseAmount AS o_OutstandingAllocatedLossAdjustmentExpenseAmount,
	i_Claimlossdate AS o_Claimlossdate,
	-- *INF*: LTRIM(RTRIM(i_TypeofPolicycontract))
	LTRIM(RTRIM(i_TypeofPolicycontract)) AS o_TypeofPolicycontract,
	i_ClaimsEntryYear AS o_ClaimsEntryYear,
	i_PaidAllocatedlossAdjustmentExpenseAmount AS o_PaidAllocatedlossAdjustmentExpenseAmount,
	i_ZipPostalCode AS o_ZipPostalCode,
	i_TransactionEffectiveDate AS o_TransactionEffectiveDate,
	i_LocationNumber AS o_LocationNumber
	FROM Union_PL_Data
),
EXP_Target_PL AS (
	SELECT
	-- *INF*: TRUNC(sysdate,'DD')
	CAST(TRUNC(CURRENT_TIMESTAMP, 'DAY') AS TIMESTAMP_NTZ(0)) AS v_RunDate,
	-- *INF*: 'ISS_GL_PL_'||TO_CHAR(v_RunDate,'YYYYMMDD')||'.CSV'
	'ISS_GL_PL_' || TO_CHAR(v_RunDate, 'YYYYMMDD') || '.CSV' AS FileName,
	o_WorkISSExtractId AS WorkISSExtractId,
	o_AuditId AS AuditId,
	o_CreatedDate AS CreatedDate,
	o_EDWPremiumMasterCalculationPKId AS EDWPremiumMasterCalculationPKId,
	o_EDWLossMasterCalculationPKId AS EDWLossMasterCalculationPKId,
	o_TypeBureauCode AS TypeBureauCode,
	o_BureauLineOfInsurance AS BureauLineOfInsurance,
	o_BureauCompanyNumber AS BureauCompanyNumber,
	o_StateProvinceCode AS StateProvinceCode,
	o_PremiumMasterRunDate AS PremiumMasterRunDate,
	o_LossMasterRunDate AS LossMasterRunDate,
	o_PolicyKey AS PolicyKey,
	o_PremiumMasterClassCode AS PremiumMasterClassCode,
	o_LossMasterClassCode AS LossMasterClassCode,
	o_ClaimNumber AS ClaimNumber,
	o_ClaimantNumber AS ClaimantNumber,
	o_RiskTerritoryCode AS RiskTerritoryCode,
	o_PolicyEffectiveDate AS PolicyEffectiveDate,
	o_CauseOfLoss AS CauseOfLoss,
	o_DeductibleAmount AS DeductibleAmount,
	o_CoverageCode AS CoverageCode,
	o_ConstructionCode AS ConstructionCode,
	o_ISOFireProtectionCode AS ISOFireProtectionCode,
	o_SublineCode AS SublineCode,
	o_PackageModificationAdjustmentGroupDescription AS PackageModificationAdjustmentGroupDescription,
	o_PolicyForm AS PolicyForm,
	o_PremiumMasterDirectWrittenPremiumAmount AS PremiumMasterDirectWrittenPremiumAmount,
	o_PaidLossAmount AS PaidLossAmount,
	o_OutstandingLossAmount AS OutstandingLossAmount,
	o_PolicyExpirationDate AS PolicyExpirationDate,
	o_InceptionToDatePaidLossAmount AS InceptionToDatePaidLossAmount,
	o_ClaimantCoverageDetailID AS ClaimantCoverageDetailID,
	o_AnnualStatementLineNumber AS AnnualStatementLineNumber,
	o_WrittenExposure AS WrittenExposure,
	o_OutstandingAllocatedLossAdjustmentExpenseAmount AS OutstandingAllocatedLossAdjustmentExpenseAmount,
	o_Claimlossdate AS Claimlossdate,
	o_TypeofPolicycontract AS TypeofPolicycontract,
	o_ClaimsEntryYear AS ClaimsEntryYear,
	o_PaidAllocatedlossAdjustmentExpenseAmount AS PaidAllocatedlossAdjustmentExpenseAmount,
	o_ZipPostalCode AS ZipPostalCode,
	o_TransactionEffectiveDate AS TransactionEffectiveDate,
	o_LocationNumber AS LocationNumber
	FROM EXP_CleanData_PL
),
SRT_FlatFile_PL AS (
	SELECT
	FileName, 
	WorkISSExtractId, 
	AuditId, 
	CreatedDate, 
	EDWPremiumMasterCalculationPKId, 
	EDWLossMasterCalculationPKId, 
	TypeBureauCode, 
	BureauLineOfInsurance, 
	BureauCompanyNumber, 
	StateProvinceCode, 
	PremiumMasterRunDate, 
	LossMasterRunDate, 
	PolicyKey, 
	PremiumMasterClassCode, 
	LossMasterClassCode, 
	ClaimNumber, 
	ClaimantNumber, 
	RiskTerritoryCode, 
	PolicyEffectiveDate, 
	CauseOfLoss, 
	SublineCode, 
	PackageModificationAdjustmentGroupDescription, 
	PremiumMasterDirectWrittenPremiumAmount, 
	PaidLossAmount, 
	OutstandingLossAmount, 
	PolicyExpirationDate, 
	InceptionToDatePaidLossAmount, 
	ClaimantCoverageDetailID, 
	AnnualStatementLineNumber, 
	Claimlossdate, 
	TypeofPolicycontract, 
	ClaimsEntryYear, 
	WrittenExposure, 
	PaidAllocatedlossAdjustmentExpenseAmount, 
	OutstandingAllocatedLossAdjustmentExpenseAmount, 
	ZipPostalCode, 
	TransactionEffectiveDate, 
	LocationNumber
	FROM EXP_Target_PL
	ORDER BY PolicyKey ASC, PremiumMasterClassCode ASC, AnnualStatementLineNumber ASC, ZipPostalCode ASC
),
ISSFlatFile_PL AS (
	INSERT INTO ISSFlatFile_GL
	(FileName, WorkISSExtractId, AuditId, CreatedDate, EDWPremiumMasterCalculationPKId, EDWLossMasterCalculationPKId, TypeBureauCode, BureauLineOfInsurance, BureauCompanyNumber, StateProvinceCode, PremiumMasterRunDate, LossMasterRunDate, PolicyKey, PremiumMasterClassCode, LossMasterClassCode, ClaimNumber, ClaimantNumber, RiskTerritoryCode, PolicyEffectiveDate, CauseOfLoss, SublineCode, PackageModificationAdjustmentGroupDescription, PremiumMasterDirectWrittenPremiumAmount, PaidLossAmount, OutstandingLossAmount, PolicyExpirationDate, InceptionToDatePaidLossAmount, ClaimantCoverageDetailID, AnnualStatementLineNumber, Claimlossdate, TypeofPolicycontract, ClaimsEntryYear, WrittenExposure, PaidAllocatedlossAdjustmentExpenseAmount, OutstandingAllocatedLossAdjustmentExpenseAmount, ZipPostalCode, TransactionEffectiveDate, LocationNumber)
	SELECT 
	FILENAME, 
	WORKISSEXTRACTID, 
	AUDITID, 
	CREATEDDATE, 
	EDWPREMIUMMASTERCALCULATIONPKID, 
	EDWLOSSMASTERCALCULATIONPKID, 
	TYPEBUREAUCODE, 
	BUREAULINEOFINSURANCE, 
	BUREAUCOMPANYNUMBER, 
	STATEPROVINCECODE, 
	PREMIUMMASTERRUNDATE, 
	LOSSMASTERRUNDATE, 
	POLICYKEY, 
	PREMIUMMASTERCLASSCODE, 
	LOSSMASTERCLASSCODE, 
	CLAIMNUMBER, 
	CLAIMANTNUMBER, 
	RISKTERRITORYCODE, 
	POLICYEFFECTIVEDATE, 
	CAUSEOFLOSS, 
	SUBLINECODE, 
	PACKAGEMODIFICATIONADJUSTMENTGROUPDESCRIPTION, 
	PREMIUMMASTERDIRECTWRITTENPREMIUMAMOUNT, 
	PAIDLOSSAMOUNT, 
	OUTSTANDINGLOSSAMOUNT, 
	POLICYEXPIRATIONDATE, 
	INCEPTIONTODATEPAIDLOSSAMOUNT, 
	CLAIMANTCOVERAGEDETAILID, 
	ANNUALSTATEMENTLINENUMBER, 
	CLAIMLOSSDATE, 
	TYPEOFPOLICYCONTRACT, 
	CLAIMSENTRYYEAR, 
	WRITTENEXPOSURE, 
	PAIDALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	OUTSTANDINGALLOCATEDLOSSADJUSTMENTEXPENSEAMOUNT, 
	ZIPPOSTALCODE, 
	TRANSACTIONEFFECTIVEDATE, 
	LOCATIONNUMBER
	FROM SRT_FlatFile_PL
),