WITH
SEQ_PassThroughChargeTransactionAKID AS (
	CREATE SEQUENCE SEQ_PassThroughChargeTransactionAKID
	START = 0
	INCREMENT = 1;
),
SQ_PassThroughChargeTransaction_SRC AS (
	WITH Common AS
	(
	SELECT distinct
	DCTrans.SessionId SessionId,
	DCTrans.Type TType,
	DCTrans.EffectiveDate TEffectiveDate,
	DCTrans.CreatedDate TCreatedDate,
	DCTrans.ExpirationDate TExpirationDate,
	DCTrans.TransactionDate TransactionDate,
	DCPolicy.PolicyId PolicyId,
	DCPolicy.Id Id,
	DCPolicy.PolicyNumber PolicyNumber,
	DCPolicy.EffectiveDate PEffectiveDate,
	DCPolicy.LineOfBusiness LineOfBusiness,
	WBPolicy.PolicyVersion PolicyVersion,
	WBParty.CustomerNum CustomerNum,
	WBR.Code Code,
	(SELECT COUNT(DISTINCT Type) FROM DCLineStaging 
	WHERE PolicyId=DCPolicy.PolicyId
	AND SessionId=DCPolicy.SessionId) NumOfLine,
	DCSession.Purpose
	FROM DCTransactionStaging DCTrans
	INNER HASH JOIN DCPolicyStaging DCPolicy
	ON DCPolicy.SessionId=DCTrans.SessionId
	AND LEN(DCPolicy.PolicyNumber)=7
	AND DCPolicy.Status<>'Quote'
	AND DCTrans.State='committed'
	AND DCTrans.HistoryID =(SELECT MAX(HistoryID) FROM DCTransactionStaging DCT 
	WHERE DCT.SessionId=DCTrans.SessionId)
	AND DCTrans.Type @{pipeline().parameters.EXCLUDE_TTYPE}
	INNER HASH JOIN WBPolicyStaging WBPolicy
	ON WBPolicy.SessionId=DCPolicy.SessionId 
	AND WBPolicy.PolicyId=DCPolicy.PolicyId
	INNER HASH JOIN DCPartyAssociationStaging DCPA
	ON DCPA.SessionId=DCTrans.SessionId 
	AND DCPA.PartyAssociationType='Account'
	INNER HASH JOIN WBPartyStaging WBParty
	ON WBParty.SessionId=DCTrans.SessionId 
	AND WBParty.PartyId=DCPA.PartyId
	LEFT HASH JOIN WBReasonStaging WBR
	ON WBR.SessionId=DCTrans.SessionId 
	AND WBR.TransactionId=DCTrans.TransactionId
	INNER HASH JOIN DCSessionStaging DCSession
	ON DCSession.SessionId=DCTrans.SessionId
	INNER HASH JOIN WBTransactionStage WBT
	ON DCTrans.TransactionId=WBT.TransactionId
	AND DCTrans.SessionId=WBT.SessionId
	AND WBT.DataFix='Y'
	AND WBT.DataFixType @{pipeline().parameters.DATAFIX_TYPE}
	AND DCSession.Purpose = 'Onset'
	)
	
	SELECT Common.SessionId SessionId,
	DCLine.Type LineType,
	Common.Id Id,
	Common.PEffectiveDate PEffectiveDate,
	Common.LineOfBusiness LineOfBusiness,
	Common.TType TType,
	Common.TEffectiveDate TEffectiveDate,
	Common.TCreatedDate TTransactionDate,
	Common.TExpirationDate TExpirationDate,
	WBLocation.LocationNumber LocationNumber,
	Common.CustomerNum CustomerNum,
	Common.PolicyVersion PolicyVersion,
	DCTS.ObjectName as TaxSurchargeObjectName,
	DCTS.Id as TaxSurchargeId,
	DCTS.Type DCTSType,
	DCTS.Written Written,
	DCTS.Amount Amount,
	DCTS.Change Change,
	Common.Code Code,
	WBTS.ChangeAttr ChangeAttr,
	WBTS.WrittenAttr WrittenAttr,
	WBTS.EntityType EntityType,
	WBLA.GeoTaxCityTaxPercent GeoTaxCityTaxPercent,
	WBLA.GeoTaxCountyTaxPercent GeoTaxCountyTaxPercent,
	'N/A' as State,
	DCPST.Written as DCPSTWritten,
	DCLocation.LocationXMLId as LocationXMLId,
	NULL as ObjectId,
	NULL as ObjectName,
	Common.PolicyNumber as PolicyNumber,
	Common.NumOfLine,
	DCLocation.LocationId,
	Common.TransactionDate  as DCTranTransactionDate,
	Common.Purpose as Purpose,
	'N/A' as CoverageGuid
	,'Query1' as Query
	FROM Common
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTaxSurchargeStaging DCTS
	ON Common.SessionId=DCTS.SessionId
	AND Common.PolicyId=DCTS.ObjectId
	AND DCTS.ObjectName='DC_Policy'
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBTaxSurchargeStage WBTS
	ON WBTS.TaxSurchargeId=DCTS.TaxSurchargeId
	AND WBTS.SessionId=DCTS.SessionId
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPremiumSubtotalStaging DCPST
	ON DCPST.SessionId=Common.SessionId
	AND DCPST.ObjectId=Common.PolicyId
	AND DCPST.ObjectName='DC_Policy'
	AND DCPST.Type='PurePremiumValues'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLocation
	ON DCLocation.SessionId=Common.SessionId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging WBLocation
	ON WBLocation.SessionId=DCLocation.SessionId
	AND WBLocation.LocationId=DCLocation.LocationId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA
	ON DCLocation.SessionId=DCLA.SessionId
	AND DCLocation.LocationId=DCLA.LocationId
	AND DCLA.LocationAssociationType='Location'
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage WBLA
	ON WBLA.SessionId=DCLocation.SessionId 
	AND WBLA.LocationId=DCLocation.LocationId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging DCLine
	ON DCLine.PolicyId=Common.PolicyId
	AND DCLine.SessionId=Common.SessionId
	AND NOT EXISTS (
	SELECT 1 FROM DCLineStaging DCL
	WHERE DCL.SessionId=DCLine.SessionId
	AND DCL.LineId<DCLine.LineId)
	
	UNION ALL
	
	SELECT Common.SessionId SessionId,
	DCLine.Type LineType,
	Common.Id Id,
	Common.PEffectiveDate PEffectiveDate,
	Common.LineOfBusiness LineOfBusiness,
	Common.TType TType,
	Common.TEffectiveDate TEffectiveDate,
	Common.TCreatedDate TTransactionDate,
	Common.TExpirationDate TExpirationDate,
	WBLocation.LocationNumber LocationNumber,
	Common.CustomerNum CustomerNum,
	Common.PolicyVersion PolicyVersion,
	DCTS.ObjectName as TaxSurchargeObjectName,
	DCTS.Id as TaxSurchargeId,
	DCTS.Type DCTSType,
	DCTS.Written Written,
	DCTS.Amount Amount,
	DCTS.Change Change,
	Common.Code Code,
	WBTS.ChangeAttr ChangeAttr,
	WBTS.WrittenAttr WrittenAttr,
	WBTS.EntityType EntityType,
	WBLA.GeoTaxCityTaxPercent GeoTaxCityTaxPercent,
	WBLA.GeoTaxCountyTaxPercent GeoTaxCountyTaxPercent,
	'N/A' as State,
	DCPST.Written as DCPSTWritten,
	DCLocation.LocationXMLId as LocationXMLId,
	NULL as ObjectId,
	NULL as ObjectName,
	Common.PolicyNumber as PolicyNumber,
	Common.NumOfLine,
	DCLocation.LocationId,
	Common.TransactionDate  as DCTranTransactionDate,
	Common.Purpose as Purpose,
	'N/A' as CoverageGuid
	,'Query2' as Query
	FROM Common
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging DCLine
	ON DCLine.PolicyId=Common.PolicyId
	AND DCLine.SessionId=Common.SessionId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTaxSurchargeStaging DCTS
	ON DCLine.SessionId=DCTS.SessionId
	AND DCLine.LineId=DCTS.ObjectId
	AND DCTS.ObjectName='DC_Line'
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBTaxSurchargeStage WBTS
	ON WBTS.TaxSurchargeId=DCTS.TaxSurchargeId
	AND WBTS.SessionId=DCTS.SessionId
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPremiumSubtotalStaging DCPST
	ON DCPST.SessionId=DCLine.SessionId
	AND DCPST.ObjectId=DCLine.LineId
	AND DCPST.ObjectName='DC_Line'
	AND DCPST.Type='PurePremiumValues'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLocation
	ON DCLocation.SessionId=Common.SessionId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging WBLocation
	ON WBLocation.SessionId=DCLocation.SessionId
	AND WBLocation.LocationId=DCLocation.LocationId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA
	ON DCLocation.SessionId=DCLA.SessionId
	AND DCLocation.LocationId=DCLA.LocationId
	AND DCLA.LocationAssociationType='Location'
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage WBLA
	ON WBLA.SessionId=DCLocation.SessionId 
	AND WBLA.LocationId=DCLocation.LocationId
	
	UNION ALL
	
	------ Below SQL is used to pull Taxes and Surcharges for WC Policies.
	
	SELECT Common.SessionId SessionId,
	DCLine.Type LineType,
	Common.Id Id,
	Common.PEffectiveDate PEffectiveDate,
	Common.LineOfBusiness LineOfBusiness,
	Common.TType TType,
	Common.TEffectiveDate TEffectiveDate,
	Common.TCreatedDate TTransactionDate,
	Common.TExpirationDate TExpirationDate,
	WBLocation.LocationNumber LocationNumber,
	Common.CustomerNum CustomerNum,
	Common.PolicyVersion PolicyVersion,
	DCTS.ObjectName as TaxSurchargeObjectName,
	DCTS.Id as TaxSurchargeId,
	DCTS.Type DCTSType,
	DCTS.Written Written,
	DCTS.Amount Amount,
	DCTS.Change Change,
	Common.Code Code,
	WBTS.ChangeAttr ChangeAttr,
	WBTS.WrittenAttr WrittenAttr,
	WBTS.EntityType EntityType,
	WBLA.GeoTaxCityTaxPercent GeoTaxCityTaxPercent,
	WBLA.GeoTaxCountyTaxPercent GeoTaxCountyTaxPercent,
	DCWCState.State as State,
	DCPST.Written as DCPSTWritten,
	DCLocation.LocationXMLId as LocationXMLId,
	NULL as ObjectId,
	NULL as ObjectName,
	Common.PolicyNumber as PolicyNumber,
	Common.NumOfLine,
	DCLocation.LocationId,
	Common.TransactionDate  as DCTranTransactionDate,
	Common.Purpose as Purpose,
	DCCov.Id as CoverageGuid
	,'Query3' as Query
	FROM Common
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging DCLine
	ON DCLine.PolicyId=Common.PolicyId
	AND DCLine.SessionId=Common.SessionId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCWCStateStaging DCWCState ON DCLine.LineId=DCWCState.LineId AND DCLine.SessionId=DCWCState.SessionId
	INNER HASH JOIN dbo.DCWCStateTermStaging DCWCStateTerm ON DCWCStateTerm.SessionId = DCWCState.SessionId AND DCWCState.WC_StateId = DCWCStateTerm.WC_StateId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTaxSurchargeStaging DCTS
	ON DCWCState.SessionId=DCTS.SessionId AND DCWCState.WC_StateId=DCTS.ObjectId AND DCTS.ObjectName='DC_WC_State'
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBTaxSurchargeStage WBTS ON WBTS.TaxSurchargeId=DCTS.TaxSurchargeId
	AND WBTS.SessionId=DCTS.SessionId
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPremiumSubtotalStaging DCPST ON DCPST.SessionId=DCWCState.SessionId
	AND DCPST.ObjectId=DCWCState.WC_StateId AND DCPST.ObjectName='DC_WC_State' AND DCPST.Type='PurePremiumValues'
	LEFT HASH JOIN dbo.DCCoverageStaging DCCov on DCCov.sessionid = DCTS.SessionId AND DCCov.ObjectId=DCWCStateTerm.WC_StateTermId 
	and DCCov.ObjectName = 'DC_WC_StateTerm' and DCCov.Type = DCTS.Type
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLocation ON DCLocation.SessionId=Common.SessionId AND DCLocation.StateProv=DCWCState.State
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA ON DCLocation.SessionId=DCLA.SessionId AND DCLocation.LocationId=DCLA.LocationId
	AND DCLA.LocationAssociationType='Location'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging WBLocation ON WBLocation.SessionId=DCLocation.SessionId AND WBLocation.LocationId=DCLocation.LocationId
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage WBLA ON WBLA.SessionId=DCLocation.SessionId AND WBLA.LocationId=DCLocation.LocationId
	
	UNION ALL
	
	SELECT Common.SessionId SessionId,
	DCLine.Type LineType,
	Common.Id Id,
	Common.PEffectiveDate PEffectiveDate,
	Common.LineOfBusiness LineOfBusiness,
	Common.TType TType,
	Common.TEffectiveDate TEffectiveDate,
	Common.TCreatedDate TTransactionDate,
	Common.TExpirationDate TExpirationDate,
	WBLocation.LocationNumber LocationNumber,
	Common.CustomerNum CustomerNum,
	Common.PolicyVersion PolicyVersion,
	DCTS.ObjectName as TaxSurchargeObjectName,
	DCTS.Id as TaxSurchargeId,
	DCTS.Type DCTSType,
	DCTS.Written Written,
	DCTS.Amount Amount,
	DCTS.Change Change,
	Common.Code Code,
	WBTS.ChangeAttr ChangeAttr,
	WBTS.WrittenAttr WrittenAttr,
	WBTS.EntityType EntityType,
	WBLA.GeoTaxCityTaxPercent GeoTaxCityTaxPercent,
	WBLA.GeoTaxCountyTaxPercent GeoTaxCountyTaxPercent,
	'N/A' as State,
	DCPST.Written as DCPSTWritten,
	DCLocation.LocationXMLId as LocationXMLId,
	DCLA.ObjectId as ObjectId,
	DCLA.ObjectName as ObjectName,
	Common.PolicyNumber as PolicyNumber,
	Common.NumOfLine,
	DCLocation.LocationId,
	Common.TransactionDate  as DCTranTransactionDate,
	Common.Purpose as Purpose,
	'N/A' as CoverageGuid
	,'Query4' as Query
	FROM Common
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLineStaging DCLine
	ON DCLine.PolicyId=Common.PolicyId
	AND DCLine.SessionId=Common.SessionId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging DCLA
	ON DCLA.SessionId=DCLine.SessionId
	AND DCLA.ObjectName=CASE DCLine.Type WHEN 'BusinessOwners' THEN 'DC_BP_Location'
	WHEN 'Crime' THEN 'DC_CR_Location'
	WHEN 'InlandMarine' THEN 'DC_IM_Location'
	WHEN 'WorkersCompensation' THEN 'DC_WC_Location'
	WHEN 'Property' THEN 'DC_CF_Location'
	WHEN 'SBOPProperty' THEN 'DC_CF_Location'
	WHEN 'CommercialAuto' THEN 'DC_CA_Location'
	WHEN 'GeneralLiability' THEN 'DC_GL_Location'
	WHEN 'SBOPGeneralLiability' THEN 'DC_GL_Location'
	ELSE 'DC_Session' END
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTaxSurchargeStaging DCTS ON DCLA.SessionId=DCTS.SessionId AND DCLA.LocationId=DCTS.ObjectId
	AND DCTS.ObjectName='DC_Location'
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBTaxSurchargeStage WBTS ON WBTS.TaxSurchargeId=DCTS.TaxSurchargeId AND WBTS.SessionId=DCTS.SessionId
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCPremiumSubtotalStaging DCPST ON DCPST.SessionId=DCLA.SessionId AND DCPST.ObjectId=DCLA.LocationId
	AND DCPST.ObjectName='DC_Location' AND DCPST.Type='PurePremiumValues'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging DCLocation ON DCLocation.SessionId=Common.SessionId AND DCLocation.LocationId=DCLA.LocationId
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging WBLocation ON WBLocation.SessionId=DCLocation.SessionId AND WBLocation.LocationId=DCLocation.LocationId
	LEFT HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationAccountStage WBLA ON WBLA.SessionId=DCLocation.SessionId AND WBLA.LocationId=DCLocation.LocationId
	WHERE DCTS.Type <> 'MNFirefighterReliefFundSurcharge'
	
	UNION ALL
	
	----- Below query we are pulling the data for MNFirefighterReliefFundSurcharge at location level and it is only need from these lines ('BusinessOwners', 'Property','SBOPProperty' )
	----- otherwise it is causing issue for data in the Aggregator as it is tying to other different lines like GL, IM, CU etc.
	
	SELECT Common.SessionId SessionId,
	DCLine.Type LineType,
	Common.Id Id,
	Common.PEffectiveDate PEffectiveDate,
	Common.LineOfBusiness LineOfBusiness,
	Common.TType TType,
	Common.TEffectiveDate TEffectiveDate,
	Common.TCreatedDate TTransactionDate,
	Common.TExpirationDate TExpirationDate,
	WBLocation.LocationNumber LocationNumber,
	Common.CustomerNum CustomerNum,
	Common.PolicyVersion PolicyVersion,
	DCTS.ObjectName as TaxSurchargeObjectName,
	DCTS.Id as TaxSurchargeId,
	DCTS.Type DCTSType,
	DCTS.Written Written,
	DCTS.Amount Amount,
	DCTS.Change Change,
	Common.Code Code,
	WBTS.ChangeAttr ChangeAttr,
	WBTS.WrittenAttr WrittenAttr,
	WBTS.EntityType EntityType,
	WBLA.GeoTaxCityTaxPercent GeoTaxCityTaxPercent,
	WBLA.GeoTaxCountyTaxPercent GeoTaxCountyTaxPercent,
	'N/A' as State,
	DCPST.Written as DCPSTWritten,
	DCLocation.LocationXMLId as LocationXMLId,
	DCLA.ObjectId as ObjectId,
	DCLA.ObjectName as ObjectName,
	Common.PolicyNumber as PolicyNumber,
	Common.NumOfLine,
	DCLocation.LocationId,
	Common.TransactionDate  as DCTranTransactionDate,
	Common.Purpose as Purpose,
	'N/A' as CoverageGuid
	,'Query5' as Query
	FROM Common
	INNER HASH JOIN DBO.DCLineStaging DCLine ON DCLine.PolicyId=Common.PolicyId AND DCLine.SessionId=Common.SessionId and DCLine.Type in  ('BusinessOwners', 'Property','SBOPProperty')
	INNER HASH JOIN DBO.DCLocationAssociationStaging DCLA
	ON DCLA.SessionId=DCLine.SessionId
	AND DCLA.ObjectName=CASE DCLine.Type WHEN 'BusinessOwners' THEN 'DC_BP_Location'
	WHEN 'Crime' THEN 'DC_CR_Location'
	WHEN 'InlandMarine' THEN 'DC_IM_Location'
	WHEN 'WorkersCompensation' THEN 'DC_WC_Location'
	WHEN 'Property' THEN 'DC_CF_Location'
	WHEN 'SBOPProperty' THEN 'DC_CF_Location'
	WHEN 'CommercialAuto' THEN 'DC_CA_Location'
	WHEN 'GeneralLiability' THEN 'DC_GL_Location'
	WHEN 'SBOPGeneralLiability' THEN 'DC_GL_Location'
	ELSE 'DC_Session' END
	INNER HASH JOIN DBO.DCTaxSurchargeStaging DCTS ON DCLA.SessionId=DCTS.SessionId AND DCLA.LocationId=DCTS.ObjectId
	AND DCTS.ObjectName='DC_Location'
	LEFT HASH JOIN DBO.WBTaxSurchargeStage WBTS ON WBTS.TaxSurchargeId=DCTS.TaxSurchargeId AND WBTS.SessionId=DCTS.SessionId
	LEFT HASH JOIN DBO.DCPremiumSubtotalStaging DCPST ON DCPST.SessionId=DCLA.SessionId AND DCPST.ObjectId=DCLA.LocationId
	AND DCPST.ObjectName='DC_Location' AND DCPST.Type='PurePremiumValues'
	INNER HASH JOIN DBO.DCLocationStaging DCLocation ON DCLocation.SessionId=Common.SessionId AND DCLocation.LocationId=DCLA.LocationId
	INNER HASH JOIN DBO.WBLocationStaging WBLocation ON WBLocation.SessionId=DCLocation.SessionId AND WBLocation.LocationId=DCLocation.LocationId
	LEFT HASH JOIN DBO.WBLocationAccountStage WBLA ON WBLA.SessionId=DCLocation.SessionId AND WBLA.LocationId=DCLocation.LocationId
	WHERE DCTS.Type = 'MNFirefighterReliefFundSurcharge'
	
	ORDER BY DCTS.Id,Common.TCreatedDate,DCLocation.LocationId desc
),
LKP_Territory AS (
	SELECT
	Territory,
	ObjectId,
	ObjectName
	FROM (
		SELECT Territory AS Territory, ObjectId AS ObjectId, ObjectName AS ObjectName
		FROM (
		SELECT Territory, GL_LocationId AS ObjectId, 'DC_GL_Location' as ObjectName
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCGLLocationStaging
		UNION ALL
		SELECT convert(varchar(128),Territory) as Territory, CA_LocationId AS ObjectId, 'DC_CA_Location' as ObjectName
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCALocationStaging) A
		ORDER BY ObjectId, ObjectName
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ObjectId,ObjectName ORDER BY Territory) = 1
),
LKP_WorkDCTInBalancePolicy AS (
	SELECT
	AccountingDate,
	ManualOverrideAccountingDate,
	SessionID,
	PolicyNumber
	FROM (
		SELECT 
			AccountingDate,
			ManualOverrideAccountingDate,
			SessionID,
			PolicyNumber
		FROM WorkDCTInBalancePolicy
		WHERE HistoryId=(select max(HistoryId) from WorkDCTInBalancePolicy B where B.SessionId=WorkDCTInBalancePolicy.SessionId)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionID,PolicyNumber ORDER BY AccountingDate) = 1
),
EXP_PassThrough AS (
	SELECT
	SQ_PassThroughChargeTransaction_SRC.SessionId,
	SQ_PassThroughChargeTransaction_SRC.LineType,
	SQ_PassThroughChargeTransaction_SRC.Id,
	SQ_PassThroughChargeTransaction_SRC.PolicyEffectiveDate,
	SQ_PassThroughChargeTransaction_SRC.LineOfBusiness,
	SQ_PassThroughChargeTransaction_SRC.TType,
	SQ_PassThroughChargeTransaction_SRC.TEffectiveDate,
	SQ_PassThroughChargeTransaction_SRC.TCreatedDate,
	SQ_PassThroughChargeTransaction_SRC.TExpirationDate,
	SQ_PassThroughChargeTransaction_SRC.LocationNumber,
	SQ_PassThroughChargeTransaction_SRC.CustomerNum,
	SQ_PassThroughChargeTransaction_SRC.PolicyVersion,
	SQ_PassThroughChargeTransaction_SRC.TaxSurchargeObjectName,
	SQ_PassThroughChargeTransaction_SRC.TaxSurchargeId,
	SQ_PassThroughChargeTransaction_SRC.DCTSType,
	SQ_PassThroughChargeTransaction_SRC.Written,
	SQ_PassThroughChargeTransaction_SRC.Amount,
	SQ_PassThroughChargeTransaction_SRC.Change,
	SQ_PassThroughChargeTransaction_SRC.Code,
	SQ_PassThroughChargeTransaction_SRC.ChangeAttr,
	SQ_PassThroughChargeTransaction_SRC.WrittenAttr,
	SQ_PassThroughChargeTransaction_SRC.EntityType,
	SQ_PassThroughChargeTransaction_SRC.GeoTaxCityTaxPercent,
	SQ_PassThroughChargeTransaction_SRC.GeoTaxCountyTaxPercent,
	SQ_PassThroughChargeTransaction_SRC.State,
	SQ_PassThroughChargeTransaction_SRC.DCPSTWritten,
	LKP_Territory.Territory,
	SQ_PassThroughChargeTransaction_SRC.LocationXmlId,
	SQ_PassThroughChargeTransaction_SRC.PolicyNumber,
	SQ_PassThroughChargeTransaction_SRC.NumOfLine,
	SQ_PassThroughChargeTransaction_SRC.LocationId,
	SQ_PassThroughChargeTransaction_SRC.DCTranTransactionDate,
	SQ_PassThroughChargeTransaction_SRC.Purpose,
	LKP_WorkDCTInBalancePolicy.AccountingDate,
	1 AS IterationId,
	'PremiumChange' AS RestateRepair,
	SQ_PassThroughChargeTransaction_SRC.CoverageGuid AS CoverageGUID,
	SQ_PassThroughChargeTransaction_SRC.Query
	FROM SQ_PassThroughChargeTransaction_SRC
	LEFT JOIN LKP_Territory
	ON LKP_Territory.ObjectId = SQ_PassThroughChargeTransaction_SRC.ObjectId AND LKP_Territory.ObjectName = SQ_PassThroughChargeTransaction_SRC.ObjectName
	LEFT JOIN LKP_WorkDCTInBalancePolicy
	ON LKP_WorkDCTInBalancePolicy.SessionID = SQ_PassThroughChargeTransaction_SRC.SessionId AND LKP_WorkDCTInBalancePolicy.PolicyNumber = SQ_PassThroughChargeTransaction_SRC.PolicyNumber
),
mplt_PassThroughCharge AS (WITH
	LKP_SupPassthroughMap_PreFilter_ByTypeAndLineType AS (
		SELECT
		RuleResult,
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		in_DCTSType,
		in_LineType
		FROM (
			SELECT 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.ElementValue as ElementValue, 
			MAP.AttributeValue as AttributeValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='PreFilter' AND
			MAP.ElementKey ='DCTSType' AND
			MAP.AttributeKey='LineType' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue ORDER BY RuleResult) = 1
	),
	LKP_SupPassthroughMap_PreFilter_ByTypeAndObject AS (
		SELECT
		RuleResult,
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		in_DCTSType,
		in_TaxSurchargeObjectName
		FROM (
			SELECT 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.ElementValue as ElementValue, 
			MAP.AttributeValue as AttributeValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='PreFilter' AND
			MAP.ElementKey ='DCTSType' AND
			MAP.AttributeKey='TaxSurchargeObjectName' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue ORDER BY RuleResult) = 1
	),
	LKP_RiskLocation_RiskLocationKey_LocNum_Territory AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber,
		RiskTerritory
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber,
				RiskTerritory
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber,RiskTerritory ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey ORDER BY RiskLocationAKID) = 1
	),
	LKP_RiskLocation_RiskLocationKey_LocNum AS (
		SELECT
		RiskLocationAKID,
		RiskLocationKey,
		LocationUnitNumber
		FROM (
			SELECT 
				RiskLocationAKID,
				RiskLocationKey,
				LocationUnitNumber
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation
			WHERE CurrentSnapshotFlag = 1 and SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and 
			exists
			 ( select 1 from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol inner join  @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT 
			 on WCT.PolicyNumber=pol.pol_num and ISNULL(RIGHT('00'+convert(varchar(3),WCT.PolicyVersion),2),'00')=pol.pol_mod and pol.crrnt_snpsht_flag=1 and PolicyAKID=pol.Pol_AK_ID ) Order By EffectiveDate DESC,CreatedDate DESC,RiskLocationAKID desc--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationKey,LocationUnitNumber ORDER BY RiskLocationAKID) = 1
	),
	LKP_SupPassthroughMap_PreFilter_ByType AS (
		SELECT
		RuleResult,
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		in_DCTSType
		FROM (
			SELECT 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.ElementValue as ElementValue, 
			MAP.AttributeValue as AttributeValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='PreFilter' AND
			MAP.ElementKey ='DCTSType' AND
			MAP.AttributeKey='N/A' AND
			MAP.AttributeValue ='N/A'  AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue ORDER BY RuleResult) = 1
	),
	LKP_archDCTaxSurchargeStaging AS (
		SELECT
		SessionId
		FROM (
			SELECT  distinct a.SessionId AS SessionId 
			from DCTaxSurchargeStaging a
			where a.ObjectName='DC_Location'
			and a.Type = 'MNFirefighterReliefFundSurcharge'
			and a.SessionId in (SELECT distinct  ab.SessionId AS SessionId from DCTaxSurchargeStaging ab where ab.Type = 'MNFirefighterReliefFundSurcharge' and ab.ObjectName='DC_Line')
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY SessionId) = 1
	),
	IN_PassThroughChargeMapplet AS (
		
	),
	EXP_PassThrough AS (
		SELECT
		SessionId,
		LineType,
		Id,
		PolicyEffectiveDate,
		LineOfBusiness,
		TType,
		TEffectiveDate,
		TCreatedDate,
		TExpirationDate,
		LocationNumber,
		CustomerNum,
		PolicyVersion,
		TaxSurchargeObjectName,
		TaxSurchargeId,
		DCTSType,
		Written,
		Amount,
		Change,
		Code,
		ChangeAttr,
		WrittenAttr,
		EntityType,
		GeoTaxCityTaxPercent,
		GeoTaxCountyTaxPercent,
		State,
		DCPSTWritten,
		Territory,
		LocationXmlId,
		PolicyNumber,
		NumOfLine,
		LocationId,
		-- *INF*: DECODE(TRUE,
		-- LineType='CommercialUmbrella','Umbrella',
		-- LineType='SBOPGeneralLiability','GeneralLiabilitySBOP',
		-- LineType='SBOPProperty','PropertySBOP',
		-- LineType='DirectorsAndOfficersNFP','DandONFP',
		-- LineType='EmploymentPracticesLiab','EPLI',
		-- LineType='DirectorsAndOffsCondos','DandOCondo',
		-- INSTR(DCTSType,'SMART')>0,'SMART',
		-- LineType
		-- )
		DECODE(
		    TRUE,
		    LineType = 'CommercialUmbrella', 'Umbrella',
		    LineType = 'SBOPGeneralLiability', 'GeneralLiabilitySBOP',
		    LineType = 'SBOPProperty', 'PropertySBOP',
		    LineType = 'DirectorsAndOfficersNFP', 'DandONFP',
		    LineType = 'EmploymentPracticesLiab', 'EPLI',
		    LineType = 'DirectorsAndOffsCondos', 'DandOCondo',
		    REGEXP_INSTR(DCTSType, 'SMART') > 0, 'SMART',
		    LineType
		) AS v_LineTypeFormatted,
		-- *INF*: LTRIM(RTRIM(v_LineTypeFormatted))
		LTRIM(RTRIM(v_LineTypeFormatted)) AS o_LineTypeFormatted,
		-- *INF*: DECODE(TRUE,
		-- EntityType='taxAppliedTo',0,
		-- --filter out fee and surcharge in the location level excluding MNFirefighterReliefFundSurcharge
		-- TaxSurchargeObjectName='DC_Location' AND (INSTR(DCTSType,'CollectionFee')>0 OR INSTR(DCTSType,'Surcharge')>0) AND DCTSType <> 'MNFirefighterReliefFundSurcharge' ,0,
		-- --filter out tax with invalid type
		-- TaxSurchargeObjectName='DC_Location' AND (INSTR(DCTSType,'Total')>0),0,
		-- 
		-- TaxSurchargeObjectName='DC_Line' AND DCTSType = 'MNFirefighterReliefFundSurcharge' AND NOT ISNULL(:LKP.LKP_archDCTaxSurchargeStaging(SessionId)),0,
		-- -- Note if MNFirefighterReliefFundSurcharge is DC_Line and DC_line we only want the DC_line data to pass through
		-- 
		-- NOT ISNULL(:LKP.LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPE(DCTSType)),TO_INTEGER(:LKP.LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPE(DCTSType)),
		-- 
		-- NOT ISNULL(:LKP.LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE(DCTSType,LineType)),TO_INTEGER(:LKP.LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE(DCTSType,LineType)),
		-- 
		-- NOT ISNULL(:LKP.LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT(DCTSType,TaxSurchargeObjectName)),TO_INTEGER(:LKP.LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT(DCTSType,TaxSurchargeObjectName)),
		-- 
		-- TaxSurchargeObjectName='DC_WC_State',1,
		-- TaxSurchargeObjectName='DC_Location' AND NumOfLine>=1 AND INSTR(DCTSType,v_LineTypeFormatted)>0,1,
		-- 0
		-- )
		DECODE(
		    TRUE,
		    EntityType = 'taxAppliedTo', 0,
		    TaxSurchargeObjectName = 'DC_Location' AND (REGEXP_INSTR(DCTSType, 'CollectionFee') > 0 OR REGEXP_INSTR(DCTSType, 'Surcharge') > 0) AND DCTSType <> 'MNFirefighterReliefFundSurcharge', 0,
		    TaxSurchargeObjectName = 'DC_Location' AND (REGEXP_INSTR(DCTSType, 'Total') > 0), 0,
		    TaxSurchargeObjectName = 'DC_Line' AND DCTSType = 'MNFirefighterReliefFundSurcharge' AND LKP_ARCHDCTAXSURCHARGESTAGING_SessionId.SessionId IS NOT NULL, 0,
		    LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPE_DCTSType.RuleResult IS NOT NULL, CAST(LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPE_DCTSType.RuleResult AS INTEGER),
		    LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE_DCTSType_LineType.RuleResult IS NOT NULL, CAST(LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE_DCTSType_LineType.RuleResult AS INTEGER),
		    LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT_DCTSType_TaxSurchargeObjectName.RuleResult IS NOT NULL, CAST(LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT_DCTSType_TaxSurchargeObjectName.RuleResult AS INTEGER),
		    TaxSurchargeObjectName = 'DC_WC_State', 1,
		    TaxSurchargeObjectName = 'DC_Location' AND NumOfLine >= 1 AND REGEXP_INSTR(DCTSType, v_LineTypeFormatted) > 0, 1,
		    0
		) AS o_FilterFlag,
		DCTranTransactionDate,
		Purpose AS i_Purpose,
		-- *INF*: IIF(ISNULL(i_Purpose) or IS_SPACES(i_Purpose) or LENGTH(i_Purpose)=0,'N/A', LTRIM(RTRIM (i_Purpose))) 
		IFF(
		    i_Purpose IS NULL or LENGTH(i_Purpose)>0 AND TRIM(i_Purpose)='' or LENGTH(i_Purpose) = 0,
		    'N/A',
		    LTRIM(RTRIM(i_Purpose))
		) AS v_Purpose,
		-- *INF*: LTRIM(RTRIM(v_Purpose))
		LTRIM(RTRIM(v_Purpose)) AS o_Purpose,
		AccountingDate AS CreatedDate,
		IterationId,
		-- *INF*: TRUNC(CreatedDate,'MM')
		-- 
		-- --- Using the CreatedDate from the WorkDCTDataRepairPolicy table we are determing the AccountingDate
		CAST(TRUNC(CreatedDate, 'MONTH') AS TIMESTAMP_NTZ(0)) AS AccountingDate,
		RestateRepair,
		CoverageGUID
		FROM IN_PassThroughChargeMapplet
		LEFT JOIN LKP_ARCHDCTAXSURCHARGESTAGING LKP_ARCHDCTAXSURCHARGESTAGING_SessionId
		ON LKP_ARCHDCTAXSURCHARGESTAGING_SessionId.SessionId = SessionId
	
		LEFT JOIN LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPE LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPE_DCTSType
		ON LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPE_DCTSType.ElementValue = DCTSType
	
		LEFT JOIN LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE_DCTSType_LineType
		ON LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE_DCTSType_LineType.ElementValue = DCTSType
		AND LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDLINETYPE_DCTSType_LineType.AttributeValue = LineType
	
		LEFT JOIN LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT_DCTSType_TaxSurchargeObjectName
		ON LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT_DCTSType_TaxSurchargeObjectName.ElementValue = DCTSType
		AND LKP_SUPPASSTHROUGHMAP_PREFILTER_BYTYPEANDOBJECT_DCTSType_TaxSurchargeObjectName.AttributeValue = TaxSurchargeObjectName
	
	),
	FIL_Passthrough AS (
		SELECT
		SessionId, 
		LineType, 
		Id, 
		PolicyEffectiveDate, 
		LineOfBusiness, 
		TType, 
		TEffectiveDate, 
		TCreatedDate, 
		TExpirationDate, 
		LocationNumber, 
		CustomerNum, 
		PolicyVersion, 
		TaxSurchargeObjectName, 
		TaxSurchargeId, 
		DCTSType, 
		Written, 
		Amount, 
		Change, 
		Code, 
		ChangeAttr, 
		WrittenAttr, 
		EntityType, 
		GeoTaxCityTaxPercent, 
		GeoTaxCountyTaxPercent, 
		State, 
		DCPSTWritten, 
		Territory, 
		LocationXmlId, 
		PolicyNumber, 
		LocationId, 
		o_LineTypeFormatted AS LineTypeFormatted, 
		o_FilterFlag AS FilterFlag, 
		DCTranTransactionDate, 
		o_Purpose AS Purpose, 
		IterationId, 
		AccountingDate, 
		RestateRepair, 
		CoverageGUID
		FROM EXP_PassThrough
		WHERE FilterFlag=1 
	---and NOT ISNULL(AccountingDate)
	),
	EXP_CleanInput AS (
		SELECT
		SessionId,
		LineType AS in_DCLineType,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_DCLineType)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_DCLineType) AS o_DCLineType,
		Id AS in_Id,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Id)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_Id) AS o_Id,
		PolicyEffectiveDate AS in_PolicyEffectiveDate,
		-- *INF*: :UDF.DEFAULT_DATE_TO_21001231(in_PolicyEffectiveDate)
		UDF_DEFAULT_DATE_TO_21001231(in_PolicyEffectiveDate) AS o_PolicyEffectiveDate,
		TType AS in_Type,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Type)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_Type) AS o_Type,
		TEffectiveDate AS in_EffectiveDate,
		-- *INF*: :UDF.DEFAULT_DATE_TO_21001231(in_EffectiveDate)
		UDF_DEFAULT_DATE_TO_21001231(in_EffectiveDate) AS o_EffectiveDate,
		TCreatedDate AS in_CreatedDate,
		-- *INF*: IIF(ISNULL(in_CreatedDate),TO_DATE('2100-12-31 23:59:59.000','YYYY-MM-DD HH24:MI:SS.MS'),in_CreatedDate)
		IFF(
		    in_CreatedDate IS NULL, TO_TIMESTAMP('2100-12-31 23:59:59.000', 'YYYY-MM-DD HH24:MI:SS.MS'),
		    in_CreatedDate
		) AS v_CreatedDate,
		v_CreatedDate AS o_CreatedDate,
		TExpirationDate AS in_ExpirationDate,
		-- *INF*: :UDF.DEFAULT_DATE_TO_21001231(in_ExpirationDate)
		UDF_DEFAULT_DATE_TO_21001231(in_ExpirationDate) AS o_ExpirationDate,
		LocationNumber AS in_LocationNumber,
		-- *INF*: IIF(ISNULL(in_LocationNumber) or IS_SPACES(in_LocationNumber) or LENGTH(in_LocationNumber)=0,'0000', LPAD(LTRIM(RTRIM (in_LocationNumber)), 4, '0')) 
		IFF(
		    in_LocationNumber IS NULL
		    or LENGTH(in_LocationNumber)>0
		    and TRIM(in_LocationNumber)=''
		    or LENGTH(in_LocationNumber) = 0,
		    '0000',
		    LPAD(LTRIM(RTRIM(in_LocationNumber)), 4, '0')
		) AS o_LocationNumber,
		PolicyVersion AS in_PolicyVersion,
		-- *INF*: IIF(ISNULL(in_PolicyVersion),'00',LPAD(TO_CHAR(in_PolicyVersion),2,'0'))
		IFF(in_PolicyVersion IS NULL, '00', LPAD(TO_CHAR(in_PolicyVersion), 2, '0')) AS o_PolicyVersion,
		TaxSurchargeObjectName AS in_TaxSurchargeObjectName,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_TaxSurchargeObjectName)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_TaxSurchargeObjectName) AS o_TaxSurchargeObjectName,
		TaxSurchargeId AS in_TaxSurchargeId,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_TaxSurchargeId)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_TaxSurchargeId) AS o_TaxSurchargeId,
		DCTSType AS in_DCTSType,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_DCTSType)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_DCTSType) AS o_DCTSType,
		Amount AS in_Amount,
		-- *INF*: IIF(ISNULL(in_Amount), 0, in_Amount)
		IFF(in_Amount IS NULL, 0, in_Amount) AS o_Amount,
		Change AS in_Change,
		-- *INF*: IIF(ISNULL(in_Change), 0, in_Change)
		IFF(in_Change IS NULL, 0, in_Change) AS o_Change,
		Code AS in_Code,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Code)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_Code) AS o_Code,
		ChangeAttr AS in_ChangeAttr,
		-- *INF*: IIF(ISNULL(in_ChangeAttr), 0, in_ChangeAttr)
		IFF(in_ChangeAttr IS NULL, 0, in_ChangeAttr) AS o_ChangeAttr,
		WrittenAttr AS in_WrittenAttr,
		-- *INF*: IIF(ISNULL(in_WrittenAttr), 0, in_WrittenAttr)
		IFF(in_WrittenAttr IS NULL, 0, in_WrittenAttr) AS o_WrittenAttr,
		EntityType AS in_EntityType,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_EntityType)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_EntityType) AS o_EntityType,
		GeoTaxCityTaxPercent AS in_GeoTaxCityTaxPercent,
		-- *INF*: IIF(ISNULL(in_GeoTaxCityTaxPercent), 0, TO_DECIMAL(in_GeoTaxCityTaxPercent))
		IFF(in_GeoTaxCityTaxPercent IS NULL, 0, CAST(in_GeoTaxCityTaxPercent AS FLOAT)) AS o_GeoTaxCityTaxPercent,
		GeoTaxCountyTaxPercent AS in_GeoTaxCountyTaxPercent,
		-- *INF*: IIF(ISNULL(in_GeoTaxCountyTaxPercent), 0, TO_DECIMAL(in_GeoTaxCountyTaxPercent))
		IFF(in_GeoTaxCountyTaxPercent IS NULL, 0, CAST(in_GeoTaxCountyTaxPercent AS FLOAT)) AS o_GeoTaxCountyTaxPercent,
		State AS in_State,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_State)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_State) AS o_State,
		DCPSTWritten AS in_DCPSTWritten,
		-- *INF*: IIF(ISNULL(in_DCPSTWritten), 0, in_DCPSTWritten)
		IFF(in_DCPSTWritten IS NULL, 0, in_DCPSTWritten) AS o_DCPSTWritten,
		Territory AS in_Territory,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_Territory)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_Territory) AS o_Territory,
		LocationXmlId AS in_LocationXmlId,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_LocationXmlId)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_LocationXmlId) AS o_LocationXmlId,
		PolicyNumber AS in_PolicyNumber,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(in_PolicyNumber)
		UDF_DEFAULT_VALUE_FOR_STRINGS(in_PolicyNumber) AS o_PolicyNumber,
		LocationId,
		LineTypeFormatted,
		DCTranTransactionDate AS in_DCTranTransactionDate,
		-- *INF*: IIF(ISNULL(in_DCTranTransactionDate),v_CreatedDate,in_DCTranTransactionDate)
		IFF(in_DCTranTransactionDate IS NULL, v_CreatedDate, in_DCTranTransactionDate) AS o_TransactionDate,
		Purpose AS in_Purpose,
		-- *INF*: IIF(in_Purpose!='Offset',ltrim(rtrim(in_Purpose)),'Deprecated')
		IFF(in_Purpose != 'Offset', ltrim(rtrim(in_Purpose)), 'Deprecated') AS o_Purpose,
		AccountingDate,
		IterationId,
		RestateRepair,
		CoverageGUID
		FROM FIL_Passthrough
	),
	LKP_SupPassThroughChargeMap_TransAmount_NoState_Entity AS (
		SELECT
		RuleResult,
		in_EntityType,
		ElementValue,
		AttributeValue
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='TransAmount' AND
			MAP.ElementKey='DCTSType' AND
			MAP.AttributeKey='EntityType' AND
			MAP.StateAbbrev='N/A' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue ORDER BY RuleResult) = 1
	),
	LKP_SupPassThroughChargeMap_DCTTaxCode AS (
		SELECT
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		RuleResult
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='DCTTaxCode' AND
			MAP.ElementKey='DCTSType' AND
			MAP.AttributeKey='Combine' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbrev,ElementValue ORDER BY StateAbbrev) = 1
	),
	LKP_SupPassThroughChargeMap_TaxPercentRate AS (
		SELECT
		AttributeValue,
		RuleResult,
		ElementValue
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='TaxPercentRate' AND
			MAP.ElementKey='DCTSType' AND
			MAP.AttributeKey='TaxPercentRate' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue ORDER BY AttributeValue) = 1
	),
	LKP_SupPassThroughChargeMap_TransAmount AS (
		SELECT
		RuleResult,
		StateAbbrev,
		ElementValue,
		AttributeValue
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='TransAmount' AND
			MAP.ElementKey='DCTSType' AND
			MAP.AttributeKey='Objectname' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY StateAbbrev,ElementValue,AttributeValue ORDER BY RuleResult) = 1
	),
	LKP_SupPassThroughChargeMap_TransAmount_NoState_Object AS (
		SELECT
		RuleResult,
		ElementValue,
		AttributeValue
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='TransAmount' AND
			MAP.ElementKey='DCTSType' AND
			MAP.AttributeKey ='Objectname' AND
			MAP.StateAbbrev='N/A' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue ORDER BY RuleResult) = 1
	),
	LKP_SupPassThroughChargeMap_FullTaxAmount AS (
		SELECT
		RuleResult,
		ElementValue,
		AttributeValue
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='FullTaxAmount' AND
			MAP.ElementKey='DCTSType' AND
			MAP.AttributeKey='EntityType' AND
			MAP.RuleCondition=0
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue ORDER BY RuleResult) = 1
	),
	EXP_Check_LkpSupPassThroughMap_Outputs AS (
		SELECT
		LKP_SupPassThroughChargeMap_FullTaxAmount.RuleResult AS FullTaxAmount_Result,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(FullTaxAmount_Result)
		UDF_DEFAULT_VALUE_FOR_STRINGS(FullTaxAmount_Result) AS o_FullTaxAmount_Result,
		LKP_SupPassThroughChargeMap_DCTTaxCode.RuleResult AS DCTTaxCode_Result,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(DCTTaxCode_Result)
		UDF_DEFAULT_VALUE_FOR_STRINGS(DCTTaxCode_Result) AS o_DCTTaxCode_Result,
		LKP_SupPassThroughChargeMap_TransAmount.RuleResult AS TransAmount_Result,
		LKP_SupPassThroughChargeMap_TransAmount_NoState_Object.RuleResult AS TransAmount_NoState_Object_Result,
		LKP_SupPassThroughChargeMap_TransAmount_NoState_Entity.RuleResult AS TransAmount_NoState_Entity_Result,
		-- *INF*: Decode(True,
		-- NOT ISNULL(TransAmount_Result), :UDF.DEFAULT_VALUE_FOR_STRINGS(TransAmount_Result),
		-- NOT ISNULL(TransAmount_NoState_Entity_Result), :UDF.DEFAULT_VALUE_FOR_STRINGS(TransAmount_NoState_Entity_Result),
		-- NOT ISNULL(TransAmount_NoState_Object_Result), :UDF.DEFAULT_VALUE_FOR_STRINGS(TransAmount_NoState_Object_Result),
		-- 'N/A')
		Decode(
		    True,
		    TransAmount_Result IS NOT NULL, UDF_DEFAULT_VALUE_FOR_STRINGS(TransAmount_Result),
		    TransAmount_NoState_Entity_Result IS NOT NULL, UDF_DEFAULT_VALUE_FOR_STRINGS(TransAmount_NoState_Entity_Result),
		    TransAmount_NoState_Object_Result IS NOT NULL, UDF_DEFAULT_VALUE_FOR_STRINGS(TransAmount_NoState_Object_Result),
		    'N/A'
		) AS o_TransAmount_Result,
		LKP_SupPassThroughChargeMap_TaxPercentRate.AttributeValue AS TaxPercentRate_Type,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TaxPercentRate_Type)
		UDF_DEFAULT_VALUE_FOR_STRINGS(TaxPercentRate_Type) AS o_TaxPercentRate_Type,
		LKP_SupPassThroughChargeMap_TaxPercentRate.RuleResult AS TaxPercentRate_Result,
		-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(TaxPercentRate_Result)
		UDF_DEFAULT_VALUE_FOR_STRINGS(TaxPercentRate_Result) AS o_TaxPercentRate_Result
		FROM 
		LEFT JOIN LKP_SupPassThroughChargeMap_DCTTaxCode
		ON LKP_SupPassThroughChargeMap_DCTTaxCode.StateAbbrev = EXP_CleanInput.o_State AND LKP_SupPassThroughChargeMap_DCTTaxCode.ElementValue = EXP_CleanInput.o_DCTSType
		LEFT JOIN LKP_SupPassThroughChargeMap_FullTaxAmount
		ON LKP_SupPassThroughChargeMap_FullTaxAmount.ElementValue = EXP_CleanInput.o_DCTSType AND LKP_SupPassThroughChargeMap_FullTaxAmount.AttributeValue != EXP_CleanInput.o_EntityType
		LEFT JOIN LKP_SupPassThroughChargeMap_TaxPercentRate
		ON LKP_SupPassThroughChargeMap_TaxPercentRate.ElementValue = EXP_CleanInput.o_DCTSType
		LEFT JOIN LKP_SupPassThroughChargeMap_TransAmount
		ON LKP_SupPassThroughChargeMap_TransAmount.StateAbbrev = EXP_CleanInput.o_State AND LKP_SupPassThroughChargeMap_TransAmount.ElementValue = EXP_CleanInput.o_DCTSType AND LKP_SupPassThroughChargeMap_TransAmount.AttributeValue = EXP_CleanInput.o_TaxSurchargeObjectName
		LEFT JOIN LKP_SupPassThroughChargeMap_TransAmount_NoState_Entity
		ON LKP_SupPassThroughChargeMap_TransAmount_NoState_Entity.ElementValue = EXP_CleanInput.o_DCTSType AND LKP_SupPassThroughChargeMap_TransAmount_NoState_Entity.AttributeValue = EXP_CleanInput.o_EntityType
		LEFT JOIN LKP_SupPassThroughChargeMap_TransAmount_NoState_Object
		ON LKP_SupPassThroughChargeMap_TransAmount_NoState_Object.ElementValue = EXP_CleanInput.o_DCTSType AND LKP_SupPassThroughChargeMap_TransAmount_NoState_Object.AttributeValue = EXP_CleanInput.o_TaxSurchargeObjectName
	),
	AGG_RemoveDuplicates AS (
		SELECT
		EXP_CleanInput.SessionId,
		EXP_CleanInput.o_Id AS Id,
		EXP_CleanInput.o_DCLineType AS DCLineType,
		EXP_CleanInput.o_PolicyEffectiveDate AS PolicyEffectiveDate,
		EXP_CleanInput.o_Type AS Type,
		EXP_CleanInput.o_EffectiveDate AS EffectiveDate,
		EXP_CleanInput.o_CreatedDate AS CreatedDate,
		EXP_CleanInput.o_ExpirationDate AS ExpirationDate,
		EXP_CleanInput.o_LocationNumber AS LocationNumber,
		EXP_CleanInput.o_PolicyVersion AS PolicyVersion,
		EXP_CleanInput.o_TaxSurchargeObjectName AS TaxSurchargeObjectName,
		EXP_CleanInput.o_TaxSurchargeId AS TaxSurchargeId,
		EXP_CleanInput.o_DCTSType AS DCTSType,
		EXP_CleanInput.o_Amount AS Amount,
		EXP_CleanInput.o_Change AS in_Change,
		EXP_CleanInput.o_Code AS Code,
		EXP_CleanInput.o_ChangeAttr AS ChangeAttr,
		EXP_CleanInput.o_WrittenAttr AS WrittenAttr,
		EXP_CleanInput.o_EntityType AS EntityType,
		EXP_CleanInput.o_GeoTaxCityTaxPercent AS in_GeoTaxCityTaxPercent,
		EXP_CleanInput.o_GeoTaxCountyTaxPercent AS in_GeoTaxCountyTaxPercent,
		EXP_CleanInput.o_State AS in_State,
		EXP_CleanInput.o_DCPSTWritten AS DCPSTWritten,
		EXP_CleanInput.o_Territory AS Territory,
		EXP_CleanInput.o_LocationXmlId AS LocationXmlId,
		EXP_CleanInput.o_PolicyNumber AS PolicyNumber,
		EXP_CleanInput.LocationId,
		EXP_CleanInput.LineTypeFormatted,
		EXP_CleanInput.o_TransactionDate AS TranTransactionDate,
		EXP_CleanInput.o_Purpose AS Purpose,
		EXP_CleanInput.AccountingDate AS in_AccountingDate,
		EXP_Check_LkpSupPassThroughMap_Outputs.o_FullTaxAmount_Result AS FullTaxAmount_Result,
		EXP_Check_LkpSupPassThroughMap_Outputs.o_DCTTaxCode_Result AS DCTTaxCode_Result,
		EXP_Check_LkpSupPassThroughMap_Outputs.o_TransAmount_Result AS TransAmount_Result,
		EXP_Check_LkpSupPassThroughMap_Outputs.o_TaxPercentRate_Type AS TaxPercentRate_Type,
		EXP_Check_LkpSupPassThroughMap_Outputs.o_TaxPercentRate_Result AS TaxPercentRate_Result,
		-- *INF*: LAST(LAST_DAY(SET_DATE_PART(
		--     SET_DATE_PART(
		--          SET_DATE_PART(
		--                      SET_DATE_PART(
		--                             in_AccountingDate
		--                    , 'HH', 23) 
		--            ,'MI',59)
		--     ,'SS',59)
		-- ,'MS', 000))
		-- )
		LAST(LAST_DAY(DATEADD(,000-DATE_PART(,DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate)),DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate)),DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate)))),DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate)),DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate)),DATEADD(HOUR,23-DATE_PART(HOUR,in_AccountingDate),in_AccountingDate)))))) AS out_PassThroughChargeTransactionBookedDate,
		-- *INF*: DECODE(TRUE,
		-- TransAmount_Result !='N/A' AND TransAmount_Result='ChangeAttr', ChangeAttr,
		-- TransAmount_Result !='N/A' AND TransAmount_Result='Change', in_Change,
		-- ChangeAttr
		-- )
		-- 
		DECODE(
		    TRUE,
		    TransAmount_Result != 'N/A' AND TransAmount_Result = 'ChangeAttr', ChangeAttr,
		    TransAmount_Result != 'N/A' AND TransAmount_Result = 'Change', in_Change,
		    ChangeAttr
		) AS out_PassThroughChargeTransactionAmount,
		-- *INF*: DECODE(TRUE,
		-- FullTaxAmount_Result = 'City_X_WrittenAttr', in_GeoTaxCityTaxPercent * WrittenAttr,
		-- FullTaxAmount_Result = 'County_X_WrittenAttr', in_GeoTaxCountyTaxPercent * WrittenAttr,
		-- 0)
		-- 
		-- 
		DECODE(
		    TRUE,
		    FullTaxAmount_Result = 'City_X_WrittenAttr', in_GeoTaxCityTaxPercent * WrittenAttr,
		    FullTaxAmount_Result = 'County_X_WrittenAttr', in_GeoTaxCountyTaxPercent * WrittenAttr,
		    0
		) AS out_FullTaxAmount,
		-- *INF*: DECODE(TRUE,
		-- TaxPercentRate_Type='Rate', TO_DECIMAL(TaxPercentRate_Result,3),
		-- TaxPercentRate_Type='City',in_GeoTaxCityTaxPercent,
		-- TaxPercentRate_Type='County',in_GeoTaxCountyTaxPercent,
		-- 0.000)
		-- 
		DECODE(
		    TRUE,
		    TaxPercentRate_Type = 'Rate', CAST(TaxPercentRate_Result AS FLOAT),
		    TaxPercentRate_Type = 'City', in_GeoTaxCityTaxPercent,
		    TaxPercentRate_Type = 'County', in_GeoTaxCountyTaxPercent,
		    0.000
		) AS out_TaxPercentageRate,
		-- *INF*: DECODE(TRUE,
		-- DCLineType='WorkersCompensation' AND TaxSurchargeObjectName= 'DC_WC_State' AND DCTTaxCode_Result != 'N/A', DCTTaxCode_Result,
		-- DCTSType)
		DECODE(
		    TRUE,
		    DCLineType = 'WorkersCompensation' AND TaxSurchargeObjectName = 'DC_WC_State' AND DCTTaxCode_Result != 'N/A', DCTTaxCode_Result,
		    DCTSType
		) AS out_DCTTaxCode,
		-- *INF*: LTRIM(RTRIM(REPLACESTR(0,DCTSType,'TaxCity','TaxCounty','')))
		LTRIM(RTRIM(REGEXP_REPLACE(DCTSType,'TaxCity','TaxCounty','','i'))) AS out_DCTaxSurchargeType,
		EXP_CleanInput.IterationId,
		EXP_CleanInput.RestateRepair,
		EXP_CleanInput.CoverageGUID
		FROM EXP_Check_LkpSupPassThroughMap_Outputs
		 -- Manually join with EXP_CleanInput
		GROUP BY CreatedDate, TaxSurchargeId, Purpose
	),
	LKP_TotalAnnualPremiumSubjectToTax AS (
		SELECT
		ChangeAttr,
		SessionId,
		ObjectId,
		ObjectName,
		Type,
		LineType
		FROM (
			select a.SessionId as SessionId,
			a.ObjectId as ObjectId,
			a.ObjectName as ObjectName,
			a.type as Type,
			LTRIM(RTRIM(REPLACE(a.Type,'KYMunicipal',''))) as LineType,
			b.ChangeAttr as ChangeAttr
			from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTaxSurchargeStaging a
			join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBTaxSurchargeStage b
			on a.SessionId=b.SessionId
			and a.TaxSurchargeId=b.TaxSurchargeId
			and b.EntityType='taxAppliedTo'
			and a.ObjectName='DC_Location'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY SessionId,ObjectId,ObjectName,Type,LineType ORDER BY ChangeAttr) = 1
	),
	EXP_Values AS (
		SELECT
		AGG_RemoveDuplicates.Id AS in_Id,
		AGG_RemoveDuplicates.DCLineType AS in_DCLineType,
		AGG_RemoveDuplicates.PolicyEffectiveDate AS in_PolicyEffectiveDate,
		AGG_RemoveDuplicates.Type AS in_Type,
		AGG_RemoveDuplicates.EffectiveDate AS in_EffectiveDate,
		AGG_RemoveDuplicates.CreatedDate AS in_TransactionDate,
		AGG_RemoveDuplicates.ExpirationDate AS in_ExpirationDate,
		AGG_RemoveDuplicates.LocationNumber,
		AGG_RemoveDuplicates.PolicyVersion AS in_PolicyVersion,
		AGG_RemoveDuplicates.TaxSurchargeObjectName,
		AGG_RemoveDuplicates.TaxSurchargeId,
		AGG_RemoveDuplicates.DCTSType AS in_DCTSType,
		AGG_RemoveDuplicates.Amount AS in_FullTermPremium,
		AGG_RemoveDuplicates.Code AS in_ReasonAmendedCode,
		AGG_RemoveDuplicates.ChangeAttr AS in_ChangeAttr,
		AGG_RemoveDuplicates.WrittenAttr AS in_WrittenAttr,
		AGG_RemoveDuplicates.EntityType AS in_EntityType,
		AGG_RemoveDuplicates.DCPSTWritten AS in_DCPSTWritten,
		AGG_RemoveDuplicates.Territory,
		AGG_RemoveDuplicates.LocationXmlId,
		AGG_RemoveDuplicates.PolicyNumber AS in_PolicyNumber,
		AGG_RemoveDuplicates.TranTransactionDate AS in_EnteredDate,
		AGG_RemoveDuplicates.Purpose AS in_Purpose,
		AGG_RemoveDuplicates.out_PassThroughChargeTransactionBookedDate AS in_PassThroughChargeTransactionBookedDate,
		AGG_RemoveDuplicates.out_PassThroughChargeTransactionAmount AS in_PassThroughChargeTransactionAmount,
		AGG_RemoveDuplicates.out_FullTaxAmount AS in_FullTaxAmount,
		AGG_RemoveDuplicates.out_TaxPercentageRate AS in_TaxPercentageRate,
		AGG_RemoveDuplicates.out_DCTTaxCode AS DCTTaxCode,
		LKP_TotalAnnualPremiumSubjectToTax.ChangeAttr AS lkp_ChangeAttr,
		-- *INF*: in_Id||in_PolicyVersion
		-- 
		-- 
		-- 
		-- --in_CustomerNumber||in_PolicyNumber||in_PolicyVersion
		in_Id || in_PolicyVersion AS v_PolicyKey_old,
		in_PolicyNumber||in_PolicyVersion AS v_PolicyKey_new,
		-- *INF*: --New RisklocationKey based on UID project
		-- --in_LocationNumber ||'~'|| in_Territory||'~'|| in_LocationXmlId
		-- --v_PolicyKey_old||in_LocationNumber || in_Territory || in_LocationXmlId
		'' AS v_RiskLocationKey,
		-- *INF*: --in_ClassCode   ||','|| 
		-- --in_SublineCode   ||','|| 
		-- --in_LineOfBusiness   ||','|| 
		-- --TO_CHAR(in_EffectiveDate)  ||','|| 
		-- --TO_CHAR(in_ExpirationDate)   ||','|| 
		-- --TO_CHAR(in_CommissionPercentage)   ||','||   
		-- --TO_CHAR(in_Exposure)   ||','|| 
		-- --in_CoverageForm   ||','|| 
		-- --v_RiskLocationKey ||','|| 
		-- --in_DCLineType   ||','|| 
		-- --TO_CHAR(in_PolicyEffectiveDate)  ||','||
		-- --in_RiskType
		'' AS v_tmp,
		-- *INF*: IIF(in_Purpose!='Deprecated',in_PassThroughChargeTransactionAmount,-1*in_PassThroughChargeTransactionAmount)
		IFF(
		    in_Purpose != 'Deprecated', in_PassThroughChargeTransactionAmount,
		    - 1 * in_PassThroughChargeTransactionAmount
		) AS v_PassThroughChargeTransactionAmount,
		-- *INF*: IIF(in_Purpose!='Deprecated',in_FullTermPremium,-1*in_FullTermPremium)
		IFF(in_Purpose != 'Deprecated', in_FullTermPremium, - 1 * in_FullTermPremium) AS v_FullTermPremium,
		v_PolicyKey_new AS out_PolicyKey,
		v_RiskLocationKey AS out_RiskLocationKey,
		in_PolicyEffectiveDate AS out_PolicyEffectiveDate,
		in_Type AS out_Type,
		in_TransactionDate AS out_TransactionDate,
		in_EnteredDate AS out_EnteredDate,
		in_EffectiveDate AS out_EffectiveDate,
		in_ExpirationDate AS out_ExpirationDate,
		-- *INF*: IIF(ISNULL(in_PassThroughChargeTransactionBookedDate), TO_DATE('1800-01-01', 'YYYY-MM-DD'), in_PassThroughChargeTransactionBookedDate)
		-- 
		-- --There is a possibility that no match is found in the lookup LKP_WorkDCTInBalancePolicy. So we have to do regular null check and assign a default date '1800-01-01' to pass the job first and then investigate why and correct the data.
		IFF(
		    in_PassThroughChargeTransactionBookedDate IS NULL, TO_TIMESTAMP('1800-01-01', 'YYYY-MM-DD'),
		    in_PassThroughChargeTransactionBookedDate
		) AS out_PassThroughChargeTransactionBookedDate,
		v_PassThroughChargeTransactionAmount AS out_PassThroughChargeTransactionAmount,
		v_FullTermPremium AS out_FullTermPremium,
		in_FullTaxAmount AS out_FullTaxAmount,
		in_TaxPercentageRate AS out_TaxPercentageRate,
		in_ReasonAmendedCode AS out_ReasonAmendedCode,
		in_DCLineType AS out_InsuranceLine,
		-- *INF*: DECODE(TRUE,
		-- in_DCTSType='KYPremiumSurcharge', in_DCPSTWritten,
		-- in_DCTSType='KYCollectionFee', 0,  --Waiting for feedback from Steven Davis, hard code as 0 for now
		-- IIF(ISNULL(lkp_ChangeAttr),0,lkp_ChangeAttr))
		DECODE(
		    TRUE,
		    in_DCTSType = 'KYPremiumSurcharge', in_DCPSTWritten,
		    in_DCTSType = 'KYCollectionFee', 0,
		    IFF(
		        lkp_ChangeAttr IS NULL, 0, lkp_ChangeAttr
		    )
		) AS out_TotalAnnualPremiumSubjectToTax,
		in_DCTSType AS out_DCTSType,
		in_EntityType AS out_EntityType,
		in_Purpose AS out_Purpose,
		AGG_RemoveDuplicates.IterationId,
		AGG_RemoveDuplicates.RestateRepair,
		AGG_RemoveDuplicates.CoverageGUID
		FROM AGG_RemoveDuplicates
		LEFT JOIN LKP_TotalAnnualPremiumSubjectToTax
		ON LKP_TotalAnnualPremiumSubjectToTax.SessionId = AGG_RemoveDuplicates.SessionId AND LKP_TotalAnnualPremiumSubjectToTax.ObjectId = AGG_RemoveDuplicates.LocationId AND LKP_TotalAnnualPremiumSubjectToTax.ObjectName = AGG_RemoveDuplicates.TaxSurchargeObjectName AND LKP_TotalAnnualPremiumSubjectToTax.Type = AGG_RemoveDuplicates.out_DCTaxSurchargeType AND LKP_TotalAnnualPremiumSubjectToTax.LineType = AGG_RemoveDuplicates.LineTypeFormatted
	),
	LKP_policy AS (
		SELECT
		pol_ak_id,
		SupSurchargeExemptID,
		in_PolicyKey,
		pol_key
		FROM (
			SELECT 
				pol_ak_id,
				SupSurchargeExemptID,
				in_PolicyKey,
				pol_key
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
			WHERE crrnt_snpsht_flag = 1 AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
	),
	EXP_RiskLocationKey AS (
		SELECT
		EXP_Values.LocationNumber,
		EXP_Values.LocationXmlId AS in_LocationXmlId,
		LKP_policy.pol_ak_id AS i_pol_ak_id,
		-- *INF*: IIF(ISNULL(i_pol_ak_id), -1, i_pol_ak_id)
		IFF(i_pol_ak_id IS NULL, - 1, i_pol_ak_id) AS v_pol_ak_id,
		v_pol_ak_id||'~'||in_LocationXmlId AS o_RisklocationKey,
		EXP_Values.Territory
		FROM EXP_Values
		LEFT JOIN LKP_policy
		ON LKP_policy.pol_key = EXP_Values.out_PolicyKey
	),
	Exp_RiskLocationAKID_population AS (
		SELECT
		o_RisklocationKey AS i_RiskLocationKey,
		LocationNumber AS i_LocationNumber,
		Territory AS i_Territory,
		-- *INF*: IIF(ISNULL(i_RatingCoverage_RiskLocationAKID),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY(i_RiskLocationKey,i_LocationNumber,i_Territory),i_RatingCoverage_RiskLocationAKID)
		IFF(
		    i_RatingCoverage_RiskLocationAKID IS NULL,
		    LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskLocationAKID,
		    i_RatingCoverage_RiskLocationAKID
		) AS v_RiskLocationAKID_RiskKey_Location_Territory,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location_Territory),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM(i_RiskLocationKey,i_LocationNumber),v_RiskLocationAKID_RiskKey_Location_Territory)
		IFF(
		    v_RiskLocationAKID_RiskKey_Location_Territory IS NULL,
		    LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.RiskLocationAKID,
		    v_RiskLocationAKID_RiskKey_Location_Territory
		) AS v_RiskLocationAKID_RiskKey_Location,
		-- *INF*: IIF(ISNULL(v_RiskLocationAKID_RiskKey_Location),:LKP.LKP_RISKLOCATION_RISKLOCATIONKEY(i_RiskLocationKey),v_RiskLocationAKID_RiskKey_Location)
		IFF(
		    v_RiskLocationAKID_RiskKey_Location IS NULL,
		    LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey.RiskLocationAKID,
		    v_RiskLocationAKID_RiskKey_Location
		) AS v_RiskLocationAKID_RiskKey,
		-- *INF*: iif(isnull(v_RiskLocationAKID_RiskKey),-1,v_RiskLocationAKID_RiskKey)
		IFF(v_RiskLocationAKID_RiskKey IS NULL, - 1, v_RiskLocationAKID_RiskKey) AS o_RiskLocationAKID
		FROM EXP_RiskLocationKey
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskLocationKey = i_RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.LocationUnitNumber = i_LocationNumber
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_TERRITORY_i_RiskLocationKey_i_LocationNumber_i_Territory.RiskTerritory = i_Territory
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.RiskLocationKey = i_RiskLocationKey
		AND LKP_RISKLOCATION_RISKLOCATIONKEY_LOCNUM_i_RiskLocationKey_i_LocationNumber.LocationUnitNumber = i_LocationNumber
	
		LEFT JOIN LKP_RISKLOCATION_RISKLOCATIONKEY LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey
		ON LKP_RISKLOCATION_RISKLOCATIONKEY_i_RiskLocationKey.RiskLocationKey = i_RiskLocationKey
	
	),
	LKP_RiskLocation AS (
		SELECT
		RiskLocationAKID,
		StateProvinceCode
		FROM (
			SELECT RL.RiskLocationAKID as RiskLocationAKID, RL.StateProvinceCode as StateProvinceCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL 
			WHERE RL.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND RL.CurrentSnapshotFlag=1 AND
			RL.PolicyAKId in (
			select pol_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy pol
			where exists (
			select 1 from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy DCP
			where DCP.PolicyNumber=pol.pol_num
			and ISNULL(RIGHT('00'+convert(varchar(3),DCP.PolicyVersion),2),'00')=pol.pol_mod)
			and pol.crrnt_snpsht_flag=1)
			---
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationAKID ORDER BY RiskLocationAKID) = 1
	),
	EXP_MD5 AS (
		SELECT
		LKP_policy.pol_ak_id AS in_pol_ak_id,
		LKP_policy.SupSurchargeExemptID AS in_SupSurchargeExemptID,
		LKP_RiskLocation.RiskLocationAKID AS in_RiskLocationAKID,
		EXP_Values.DCTTaxCode AS in_DCTTaxCode,
		EXP_Values.TaxSurchargeId AS in_TaxSurchargeId,
		EXP_Values.out_PolicyEffectiveDate AS in_PolicyEffectiveDate,
		EXP_Values.out_InsuranceLine AS in_InsuranceLine,
		EXP_Values.TaxSurchargeObjectName AS in_TaxSurchargeObjectName,
		EXP_Values.out_DCTSType AS in_DCTSType,
		-- *INF*: substr(in_DCTTaxCode,1,2)
		-- 
		-- -- change this to DCTTaxCode because we want both the passed in DCTSType values that get assigned to DCTTaxCode and the cases where DCTTaxCode has appended States which should all be in DCTTaxCode
		substr(in_DCTTaxCode, 1, 2) AS o_DCTSTypeStateAbbrev,
		EXP_Values.out_Type AS Type,
		EXP_Values.out_TransactionDate AS TransactionDate,
		EXP_Values.out_EnteredDate AS EnteredDate,
		EXP_Values.out_EffectiveDate AS EffectiveDate,
		EXP_Values.out_ExpirationDate AS ExpirationDate,
		EXP_Values.out_PassThroughChargeTransactionBookedDate AS PassThroughChargeTransactionBookedDate,
		EXP_Values.out_PassThroughChargeTransactionAmount AS PassThroughChargeTransactionAmount,
		EXP_Values.out_FullTermPremium AS FullTermPremium,
		EXP_Values.out_FullTaxAmount AS FullTaxAmount,
		EXP_Values.out_TaxPercentageRate AS TaxPercentageRate,
		EXP_Values.out_ReasonAmendedCode AS ReasonAmendedCode,
		EXP_Values.out_TotalAnnualPremiumSubjectToTax AS TotalAnnualPremiumSubjectToTax,
		-- *INF*: IIF(ISNULL(in_pol_ak_id),-1,in_pol_ak_id)
		IFF(in_pol_ak_id IS NULL, - 1, in_pol_ak_id) AS v_pol_ak_id,
		-- *INF*: IIF(ISNULL(in_SupSurchargeExemptID),-1,in_SupSurchargeExemptID)
		IFF(in_SupSurchargeExemptID IS NULL, - 1, in_SupSurchargeExemptID) AS v_SupSurchargeExemptID,
		-- *INF*: IIF(ISNULL(in_RiskLocationAKID) ,
		-- -1,in_RiskLocationAKID)
		IFF(in_RiskLocationAKID IS NULL, - 1, in_RiskLocationAKID) AS v_RiskLocationAKID,
		-- *INF*: MD5(in_TaxSurchargeId||
		-- TO_CHAR(TransactionDate))
		MD5(in_TaxSurchargeId || TO_CHAR(TransactionDate)) AS v_PassThroughChargeTransactionHashKey,
		-- *INF*: DECODE(TRUE,
		-- PassThroughChargeTransactionAmount<0 AND in_PolicyEffectiveDate=EffectiveDate AND Type='Cancel', 'ReturnFull', 
		-- PassThroughChargeTransactionAmount<0, 'Return',
		-- 'Add')
		DECODE(
		    TRUE,
		    PassThroughChargeTransactionAmount < 0 AND in_PolicyEffectiveDate = EffectiveDate AND Type = 'Cancel', 'ReturnFull',
		    PassThroughChargeTransactionAmount < 0, 'Return',
		    'Add'
		) AS v_PremPlusMinusDescription,
		LKP_RiskLocation.StateProvinceCode,
		1 AS out_DuplicateSequence,
		v_PassThroughChargeTransactionHashKey AS out_PassThroughChargeTransactionHashKey,
		v_RiskLocationAKID AS out_RiskLocationAKID,
		v_pol_ak_id AS out_PolicyAKID,
		-- *INF*: MD5(TO_CHAR(v_pol_ak_id) || 
		-- TO_CHAR(v_RiskLocationAKID) || 
		-- in_InsuranceLine||
		-- TO_CHAR(in_PolicyEffectiveDate))
		MD5(TO_CHAR(v_pol_ak_id) || TO_CHAR(v_RiskLocationAKID) || in_InsuranceLine || TO_CHAR(in_PolicyEffectiveDate)) AS out_PolicyCoverageHashKey,
		v_SupSurchargeExemptID AS out_SupSurchargeExemptID,
		Type AS out_prem_trans_code,
		v_PremPlusMinusDescription AS out_PremPlusMinusDescription,
		in_DCTTaxCode AS out_DCTTaxCode,
		EXP_Values.out_EntityType AS EntityType,
		EXP_Values.out_Purpose AS Purpose,
		EXP_Values.IterationId,
		-- *INF*: IIF(RestateRepair='Restate',IterationId + 1,IterationId)
		IFF(RestateRepair = 'Restate', IterationId + 1, IterationId) AS LoadSequence,
		EXP_Values.RestateRepair,
		'N/A' AS DefaultString,
		EXP_Values.CoverageGUID
		FROM EXP_Values
		LEFT JOIN LKP_RiskLocation
		ON LKP_RiskLocation.RiskLocationAKID = Exp_RiskLocationAKID_population.o_RiskLocationAKID
		LEFT JOIN LKP_policy
		ON LKP_policy.pol_key = EXP_Values.out_PolicyKey
	),
	LKP_PassThroughChargeTransaction_Restate AS (
		SELECT
		PassThroughChargeTransactionAKID,
		PassThroughChargeTransactionHashKey,
		DuplicateSequence,
		OffsetOnsetCode,
		LoadSequence,
		NegateRestateCode
		FROM (
			SELECT 
				PassThroughChargeTransactionAKID,
				PassThroughChargeTransactionHashKey,
				DuplicateSequence,
				OffsetOnsetCode,
				LoadSequence,
				NegateRestateCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction
			WHERE CurrentSnapshotFlag = 1 AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionHashKey,DuplicateSequence,OffsetOnsetCode,LoadSequence,NegateRestateCode ORDER BY PassThroughChargeTransactionAKID DESC) = 1
	),
	LKP_PassThroughChargeTransaction AS (
		SELECT
		PassThroughChargeTransactionAKID,
		PassThroughChargeTransactionHashKey,
		DuplicateSequence,
		OffsetOnsetCode
		FROM (
			SELECT 
				PassThroughChargeTransactionAKID,
				PassThroughChargeTransactionHashKey,
				DuplicateSequence,
				OffsetOnsetCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction
			WHERE CurrentSnapshotFlag = 1 AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
			AND NegateRestateCode not in ('PremiumChange') -- To make sure that we load True Offset records created in IDO due to deprecation (PROD-14950), added condition on NegateRestateCode
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionHashKey,DuplicateSequence,OffsetOnsetCode ORDER BY PassThroughChargeTransactionAKID) = 1
	),
	LKP_PolicyCoverageAKID AS (
		SELECT
		PolicyCoverageAKID,
		PolicyCoverageHashKey
		FROM (
			SELECT 
				PolicyCoverageAKID,
				PolicyCoverageHashKey
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage
			WHERE CurrentSnapshotFlag=1 and SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageHashKey ORDER BY PolicyCoverageAKID) = 1
	),
	LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode AS (
		SELECT
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		RuleResult,
		in_NA,
		StateProvinceCode
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='FilterRule' AND
			MAP.ElementKey='StateProvinceCode' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue,AttributeKey ORDER BY StateAbbrev) = 1
	),
	LKP_PassThroughChargeTransaction_DataRepair AS (
		SELECT
		PassThroughChargeTransactionAKID,
		PassThroughChargeTransactionAmount,
		PassThroughChargeTransactionHashKey,
		OffsetOnsetCode
		FROM (
			SELECT 
				PassThroughChargeTransactionAKID,
				PassThroughChargeTransactionAmount,
				PassThroughChargeTransactionHashKey,
				OffsetOnsetCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransaction
			WHERE CurrentSnapshotFlag = 1 AND SourceSystemID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND AuditID<0
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionHashKey,OffsetOnsetCode ORDER BY PassThroughChargeTransactionAKID) = 1
	),
	LKP_SupPassThroughChargeType AS (
		SELECT
		SupPassThroughChargeTypeID,
		DCTTaxCode
		FROM (
			SELECT 
				SupPassThroughChargeTypeID,
				DCTTaxCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupPassThroughChargeType
			WHERE CurrentSnapshotFlag='1' AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY DCTTaxCode ORDER BY SupPassThroughChargeTypeID) = 1
	),
	LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev AS (
		SELECT
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		RuleResult,
		in_NA,
		in_DCTSTypeStateAbbrev
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='FilterRule' AND
			MAP.ElementKey='DCTSTypeStateAbbrev' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue,AttributeKey ORDER BY StateAbbrev) = 1
	),
	LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode_InsuranceLine AS (
		SELECT
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		RuleResult,
		in_InsuranceLine,
		in_StateProvinceCode
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='FilterRule' AND
			MAP.ElementKey='StateProvinceCode' AND
			MAP.AttributeKey='InsuranceLine' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue ORDER BY StateAbbrev) = 1
	),
	LKP_RatingCoverage AS (
		SELECT
		RatingCoverageAKID,
		PolicyAKID,
		CoverageGUID,
		EffectiveDate
		FROM (
			SELECT RC.RatingCoverageAKID as RatingCoverageAKID, 
			PC.PolicyAKID as PolicyAKID, 
			RC.CoverageGUID as CoverageGUID,
			RC.EffectiveDate as EffectiveDate
			FROM RatingCoverage RC 
			INNER JOIN PolicyCoverage PC on PC.PolicyCoverageAKID = RC.PolicyCoverageAKID and PC.CurrentSnapshotFlag = 1
			INNER JOIN V2.policy P on P.pol_ak_id = PC.PolicyAKID and P.crrnt_snpsht_flag =1
			INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy WCT ON WCT.PolicyNumber = P.pol_num AND ISNULL(RIGHT('00' + convert(VARCHAR(3), WCT.PolicyVersion), 2), '00') = P.pol_mod
			WHERE PC.InsuranceLine = 'WorkersCompensation'
			ORDER BY RC.CoverageGUID,RC.EffectiveDate
			--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,CoverageGUID,EffectiveDate ORDER BY RatingCoverageAKID) = 1
	),
	LKP_sup_premium_transaction_code AS (
		SELECT
		sup_prem_trans_code_id,
		prem_trans_code,
		PremiumPlusMinusDescription
		FROM (
			SELECT 
				sup_prem_trans_code_id,
				prem_trans_code,
				PremiumPlusMinusDescription
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_premium_transaction_code
			WHERE crrnt_snpsht_flag='1' AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY prem_trans_code,PremiumPlusMinusDescription ORDER BY sup_prem_trans_code_id) = 1
	),
	LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev_InsuranceLine AS (
		SELECT
		StateAbbrev,
		RecordType,
		ElementKey,
		ElementValue,
		AttributeKey,
		AttributeValue,
		RuleCondition,
		RuleResult,
		in_InsuranceLine,
		in_DCTSTypeStateAbbrev
		FROM (
			SELECT 
			MAP.RecordType as RecordType, 
			MAP.ElementKey as ElementKey, 
			MAP.AttributeKey as AttributeKey, 
			MAP.AttributeValue as AttributeValue, 
			MAP.RuleCondition as RuleCondition, 
			MAP.RuleResult as RuleResult, 
			MAP.StateAbbrev as StateAbbrev, 
			MAP.ElementValue as ElementValue 
			FROM 
			SupPassThroughChargeMap MAP
			WHERE
			MAP.RecordType='FilterRule' AND
			MAP.ElementKey='DCTSTypeStateAbbrev' AND
			MAP.AttributeKey='InsuranceLine' AND
			MAP.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY ElementValue,AttributeValue ORDER BY StateAbbrev) = 1
	),
	EXP_ApplyFilterRule AS (
		SELECT
		LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev_InsuranceLine.RuleResult AS lkp_FilterRule_StateAbbrev_InsuranceLine,
		LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev.RuleResult AS lkp_FilterRule_StateAbbrev,
		LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode.RuleResult AS lkp_FilterRule_StateProvenceCode,
		LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode_InsuranceLine.RuleResult AS lkp_FilterRule_StateProvenceCode_InsuranceLine,
		-- *INF*: DECODE(TRUE,
		-- NOT ISNULL(lkp_FilterRule_StateProvenceCode_InsuranceLine), TO_INTEGER(lkp_FilterRule_StateProvenceCode_InsuranceLine),
		-- NOT ISNULL(lkp_FilterRule_StateProvenceCode), TO_INTEGER(lkp_FilterRule_StateProvenceCode),
		-- NOT ISNULL(lkp_FilterRule_StateAbbrev_InsuranceLine),TO_INTEGER(lkp_FilterRule_StateAbbrev_InsuranceLine),
		-- NOT ISNULL(lkp_FilterRule_StateAbbrev), TO_INTEGER(lkp_FilterRule_StateAbbrev),
		-- 0)
		-- 
		-- -- hierarchy of rules -- StateProvenceCode (numeric) values first, then parsed StateAbbrev (text) 
		DECODE(
		    TRUE,
		    lkp_FilterRule_StateProvenceCode_InsuranceLine IS NOT NULL, CAST(lkp_FilterRule_StateProvenceCode_InsuranceLine AS INTEGER),
		    lkp_FilterRule_StateProvenceCode IS NOT NULL, CAST(lkp_FilterRule_StateProvenceCode AS INTEGER),
		    lkp_FilterRule_StateAbbrev_InsuranceLine IS NOT NULL, CAST(lkp_FilterRule_StateAbbrev_InsuranceLine AS INTEGER),
		    lkp_FilterRule_StateAbbrev IS NOT NULL, CAST(lkp_FilterRule_StateAbbrev AS INTEGER),
		    0
		) AS v_FilterRule,
		v_FilterRule AS FilterRule
		FROM 
		LEFT JOIN LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev
		ON LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev.ElementValue = EXP_MD5.o_DCTSTypeStateAbbrev AND LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev.AttributeValue = EXP_MD5.DefaultString AND LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev.AttributeKey = EXP_MD5.DefaultString
		LEFT JOIN LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev_InsuranceLine
		ON LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev_InsuranceLine.ElementValue = EXP_MD5.o_DCTSTypeStateAbbrev AND LKP_SupPassThroughChargeMap_FilterRule_StateAbbrev_InsuranceLine.AttributeValue = EXP_MD5.in_InsuranceLine
		LEFT JOIN LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode
		ON LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode.ElementValue = EXP_MD5.StateProvinceCode AND LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode.AttributeValue = EXP_MD5.DefaultString AND LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode.AttributeKey = EXP_MD5.DefaultString
		LEFT JOIN LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode_InsuranceLine
		ON LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode_InsuranceLine.ElementValue = EXP_MD5.StateProvinceCode AND LKP_SupPassThroughChargeMap_FilterRule_StateProvenceCode_InsuranceLine.AttributeValue = EXP_MD5.in_InsuranceLine
	),
	LKP_RatingCoverage_SupPassThroughMap AS (
		SELECT
		ModInsuranceLine,
		in_InsuranceLine,
		PolicyCoverageAKID,
		InsuranceLine
		FROM (
			SELECT Distinct
			RC.PolicyCoverageAKID as PolicyCoverageAKID,
			Map.ElementValue as InsuranceLine,
			Map.RuleResult as ModInsuranceLine
			FROM 
			RatingCoverage RC
			inner join SupPassThroughChargeMap Map on RC.CoverageForm=Map.AttributeValue
			Where
			Map.RecordType='LgtLobOverride' and Map.ElementKey='InsuranceLine' and Map.AttributeKey='CoverageForm'
			and Map.RuleCondition=1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyCoverageAKID,InsuranceLine ORDER BY ModInsuranceLine) = 1
	),
	EXP_EvaluatePassThroughLookupResponse AS (
		SELECT
		LKP_PassThroughChargeTransaction.PassThroughChargeTransactionAKID AS i_PassThroughChargeTransactionAKID,
		LKP_PassThroughChargeTransaction_Restate.PassThroughChargeTransactionAKID AS i_PassThroughChargeTransactionAKID_Restate,
		LKP_PassThroughChargeTransaction_DataRepair.PassThroughChargeTransactionAKID AS i_PassThroughChargeTransactionAKID_DataRepair,
		LKP_PassThroughChargeTransaction_DataRepair.PassThroughChargeTransactionAmount AS i_PassThroughChargeTransactionAmount_DataRepair,
		EXP_MD5.PassThroughChargeTransactionAmount AS i_PassThroughChargeTransactionAmount,
		EXP_MD5.RestateRepair,
		EXP_MD5.Purpose,
		-- *INF*: Decode(True,
		-- RestateRepair='Restate',i_PassThroughChargeTransactionAKID_Restate,
		-- RestateRepair='PremiumChange' and Purpose !='Onset', i_PassThroughChargeTransactionAKID_DataRepair,
		-- i_PassThroughChargeTransactionAKID
		-- )
		-- 
		-- -- PremiumChange check needed  because onsets will not return values
		Decode(
		    True,
		    RestateRepair = 'Restate', i_PassThroughChargeTransactionAKID_Restate,
		    RestateRepair = 'PremiumChange' and Purpose != 'Onset', i_PassThroughChargeTransactionAKID_DataRepair,
		    i_PassThroughChargeTransactionAKID
		) AS v_PassThroughChargeTransactionAKID,
		-- *INF*: Decode(True,
		-- RestateRepair='Restate',i_PassThroughChargeTransactionAmount,
		-- RestateRepair='PremiumChange' and Purpose !='Onset', i_PassThroughChargeTransactionAmount_DataRepair,
		-- i_PassThroughChargeTransactionAmount
		-- )
		-- 
		-- -- PremiumChange check needed  because onsets will not return values
		Decode(
		    True,
		    RestateRepair = 'Restate', i_PassThroughChargeTransactionAmount,
		    RestateRepair = 'PremiumChange' and Purpose != 'Onset', i_PassThroughChargeTransactionAmount_DataRepair,
		    i_PassThroughChargeTransactionAmount
		) AS v_PassThroughChargeTransactionAmount,
		-- *INF*: DECODE(TRUE,
		-- RestateRepair='PremiumChange' AND ISNULL(i_PassThroughChargeTransactionAKID_DataRepair),'Y',
		-- RestateRepair='PremiumChange' AND i_PassThroughChargeTransactionAmount_DataRepair<>i_PassThroughChargeTransactionAmount,'Y',
		-- 'N'
		-- )
		DECODE(
		    TRUE,
		    RestateRepair = 'PremiumChange' AND i_PassThroughChargeTransactionAKID_DataRepair IS NULL, 'Y',
		    RestateRepair = 'PremiumChange' AND i_PassThroughChargeTransactionAmount_DataRepair <> i_PassThroughChargeTransactionAmount, 'Y',
		    'N'
		) AS v_RepairChangeFlag,
		v_PassThroughChargeTransactionAKID AS o_PassThroughChargeTransactionAKID,
		v_PassThroughChargeTransactionAmount AS o_PassThroughChargeTransactionAmount,
		v_RepairChangeFlag AS o_RepairChangeFlag
		FROM EXP_MD5
		LEFT JOIN LKP_PassThroughChargeTransaction
		ON LKP_PassThroughChargeTransaction.PassThroughChargeTransactionHashKey = EXP_MD5.out_PassThroughChargeTransactionHashKey AND LKP_PassThroughChargeTransaction.DuplicateSequence = EXP_MD5.out_DuplicateSequence AND LKP_PassThroughChargeTransaction.OffsetOnsetCode = EXP_MD5.Purpose
		LEFT JOIN LKP_PassThroughChargeTransaction_DataRepair
		ON LKP_PassThroughChargeTransaction_DataRepair.PassThroughChargeTransactionHashKey = EXP_MD5.out_PassThroughChargeTransactionHashKey AND LKP_PassThroughChargeTransaction_DataRepair.OffsetOnsetCode = EXP_MD5.Purpose
		LEFT JOIN LKP_PassThroughChargeTransaction_Restate
		ON LKP_PassThroughChargeTransaction_Restate.PassThroughChargeTransactionHashKey = EXP_MD5.out_PassThroughChargeTransactionHashKey AND LKP_PassThroughChargeTransaction_Restate.DuplicateSequence = EXP_MD5.out_DuplicateSequence AND LKP_PassThroughChargeTransaction_Restate.OffsetOnsetCode = EXP_MD5.Purpose AND LKP_PassThroughChargeTransaction_Restate.LoadSequence = EXP_MD5.LoadSequence AND LKP_PassThroughChargeTransaction_Restate.NegateRestateCode = EXP_MD5.RestateRepair
	),
	EXP_Insuranceline_CoverageForm_Check AS (
		SELECT
		ModInsuranceLine,
		in_InsuranceLine AS InsuranceLine,
		-- *INF*: IIF(NOT ISNULL(ModInsuranceLine),ModInsuranceLine,InsuranceLine)
		IFF(ModInsuranceLine IS NOT NULL, ModInsuranceLine, InsuranceLine) AS o_InsurancelIne
		FROM LKP_RatingCoverage_SupPassThroughMap
	),
	LKP_SupLGTLineOfInsurance AS (
		SELECT
		SupLGTLineOfInsuranceId,
		LGTLineOfInsuranceCode
		FROM (
			SELECT 
				SupLGTLineOfInsuranceId,
				LGTLineOfInsuranceCode
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupLGTLineOfInsurance
			WHERE CurrentSnapshotFlag = 1 AND SourceSystemId = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY LGTLineOfInsuranceCode ORDER BY SupLGTLineOfInsuranceId) = 1
	),
	EXP_Detect_Changes AS (
		SELECT
		EXP_MD5.out_DuplicateSequence AS in_DuplicateSequence,
		EXP_MD5.out_PassThroughChargeTransactionHashKey AS in_PassThroughChargeTransactionHashKey,
		EXP_MD5.in_TaxSurchargeObjectName,
		EXP_MD5.in_DCTSType,
		EXP_MD5.Type AS in_Type,
		EXP_MD5.EnteredDate AS in_PassThroughChargeTransactionEnteredDate,
		EXP_MD5.EffectiveDate AS in_PassThroughChargeTransactionEffectiveDate,
		EXP_MD5.ExpirationDate AS in_PassThroughChargeTransactionExpirationDate,
		EXP_MD5.PassThroughChargeTransactionBookedDate AS in_PassThroughChargeTransactionBookedDate,
		EXP_EvaluatePassThroughLookupResponse.o_PassThroughChargeTransactionAmount AS in_PassThroughChargeTransactionAmount,
		EXP_MD5.FullTermPremium AS in_FullTermPremium,
		EXP_MD5.FullTaxAmount AS in_FullTaxAmount,
		EXP_MD5.TaxPercentageRate AS in_TaxPercentageRate,
		EXP_MD5.ReasonAmendedCode AS in_ReasonAmendedCode,
		EXP_MD5.TotalAnnualPremiumSubjectToTax AS in_TotalAnnualPremiumSubjectToTax,
		EXP_MD5.out_RiskLocationAKID AS in_RiskLocationAKID,
		EXP_MD5.out_PolicyAKID AS in_PolicyAKID,
		LKP_SupLGTLineOfInsurance.SupLGTLineOfInsuranceId AS in_SupLGTLineOfInsuranceId,
		LKP_PolicyCoverageAKID.PolicyCoverageAKID AS in_PolicyCoverageAKID,
		EXP_MD5.out_SupSurchargeExemptID AS in_SupSurchargeExemptID,
		LKP_sup_premium_transaction_code.sup_prem_trans_code_id AS in_sup_prem_trans_code_id,
		EXP_MD5.out_DCTTaxCode AS in_DCTTaxCode,
		LKP_SupPassThroughChargeType.SupPassThroughChargeTypeID AS in_SupPassThroughChargeTypeID,
		EXP_MD5.StateProvinceCode AS in_StateProvinceCode,
		EXP_MD5.in_InsuranceLine,
		EXP_MD5.EntityType AS in_EntityType,
		EXP_MD5.Purpose,
		EXP_EvaluatePassThroughLookupResponse.o_PassThroughChargeTransactionAKID AS PassThroughChargeTransactionAKID,
		-- *INF*: IIF(ISNULL(in_sup_prem_trans_code_id), -1, in_sup_prem_trans_code_id)
		IFF(in_sup_prem_trans_code_id IS NULL, - 1, in_sup_prem_trans_code_id) AS v_sup_prem_trans_code_id,
		-- *INF*: IIF(ISNULL(in_SupPassThroughChargeTypeID), -1, in_SupPassThroughChargeTypeID)
		IFF(in_SupPassThroughChargeTypeID IS NULL, - 1, in_SupPassThroughChargeTypeID) AS v_SupPassThroughChargeTypeID,
		-- *INF*: IIF(ISNULL(in_SupLGTLineOfInsuranceId),-1,in_SupLGTLineOfInsuranceId)
		IFF(in_SupLGTLineOfInsuranceId IS NULL, - 1, in_SupLGTLineOfInsuranceId) AS v_SupLGTLineOfInsuranceId,
		EXP_EvaluatePassThroughLookupResponse.o_RepairChangeFlag AS ChangeFlag,
		EXP_ApplyFilterRule.FilterRule AS AdditionalFilter,
		'1' AS out_CurrentSnapshotFlag,
		-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
		TO_TIMESTAMP('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') AS out_EffectiveDate,
		-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
		TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS out_ExpirationDate,
		@{pipeline().parameters.SOURCE_SYSTEM_ID} AS out_SourceSystemID,
		SYSDATE AS out_CreateDate,
		SYSDATE AS out_ModifiedDate,
		0 AS out_logicalIndicator,
		'1' AS out_LogicalDeleteFlag,
		in_DuplicateSequence AS out_DuplicateSequence,
		in_PassThroughChargeTransactionHashKey AS out_PassThroughChargeTransactionHashKey,
		in_Type AS out_Type,
		in_PassThroughChargeTransactionEnteredDate AS out_PassThroughChargeTransactionEnteredDate,
		in_PassThroughChargeTransactionEffectiveDate AS out_PassThroughChargeTransactionEffectiveDate,
		in_PassThroughChargeTransactionExpirationDate AS out_PassThroughChargeTransactionExpirationDate,
		in_PassThroughChargeTransactionBookedDate AS out_PassThroughChargeTransactionBookedDate,
		in_PassThroughChargeTransactionAmount AS out_PassThroughChargeTransactionAmount,
		in_FullTermPremium AS out_FullTermPremium,
		in_FullTaxAmount AS out_FullTaxAmount,
		in_TaxPercentageRate AS out_TaxPercentageRate,
		-- *INF*: IIF(ISNULL(in_ReasonAmendedCode),'N/A',in_ReasonAmendedCode)
		IFF(in_ReasonAmendedCode IS NULL, 'N/A', in_ReasonAmendedCode) AS out_ReasonAmendedCode,
		v_sup_prem_trans_code_id AS out_sup_prem_trans_code_id,
		-- *INF*: IIF( ISNULL(in_RiskLocationAKID) or (IN(in_DCTSType,'KYPremiumSurcharge','KYCollectionFee') AND IN(in_TaxSurchargeObjectName,'DC_Policy')),-1, in_RiskLocationAKID)
		IFF(
		    in_RiskLocationAKID IS NULL
		    or (in_DCTSType IN ('KYPremiumSurcharge','KYCollectionFee')
		    and in_TaxSurchargeObjectName IN ('DC_Policy')),
		    - 1,
		    in_RiskLocationAKID
		) AS out_RiskLocationAKID,
		in_PolicyAKID AS out_PolicyAKID,
		v_SupLGTLineOfInsuranceId AS out_SupLGTLineOfInsuranceId,
		-- *INF*: IIF( ISNULL(in_PolicyCoverageAKID) or (IN(in_DCTSType,'KYPremiumSurcharge','KYCollectionFee') AND IN(in_TaxSurchargeObjectName,'DC_Policy')),-1, in_PolicyCoverageAKID)
		IFF(
		    in_PolicyCoverageAKID IS NULL
		    or (in_DCTSType IN ('KYPremiumSurcharge','KYCollectionFee')
		    and in_TaxSurchargeObjectName IN ('DC_Policy')),
		    - 1,
		    in_PolicyCoverageAKID
		) AS out_PolicyCoverageAKID,
		in_SupSurchargeExemptID AS out_SupSurchargeExemptID,
		v_SupPassThroughChargeTypeID AS out_SupPassThroughChargeTypeID,
		in_TotalAnnualPremiumSubjectToTax AS out_TotalAnnualPremiumSubjectToTax,
		in_TaxSurchargeObjectName AS out_TaxSurchargeObjectName,
		-- *INF*: LTRIM(RTRIM(in_DCTTaxCode))
		LTRIM(RTRIM(in_DCTTaxCode)) AS out_DCTTaxCode,
		Purpose AS out_Purpose,
		EXP_MD5.LoadSequence,
		EXP_MD5.RestateRepair,
		LKP_RatingCoverage.RatingCoverageAKID,
		-- *INF*: IIF(ISNULL(RatingCoverageAKID),-1,RatingCoverageAKID)
		IFF(RatingCoverageAKID IS NULL, - 1, RatingCoverageAKID) AS O_RatingCoverageAKID
		FROM EXP_ApplyFilterRule
		 -- Manually join with EXP_EvaluatePassThroughLookupResponse
		 -- Manually join with EXP_MD5
		LEFT JOIN LKP_PolicyCoverageAKID
		ON LKP_PolicyCoverageAKID.PolicyCoverageHashKey = EXP_MD5.out_PolicyCoverageHashKey
		LEFT JOIN LKP_RatingCoverage
		ON LKP_RatingCoverage.PolicyAKID = EXP_MD5.out_PolicyAKID AND LKP_RatingCoverage.CoverageGUID = EXP_MD5.CoverageGUID AND LKP_RatingCoverage.EffectiveDate = EXP_MD5.EnteredDate
		LEFT JOIN LKP_SupLGTLineOfInsurance
		ON LKP_SupLGTLineOfInsurance.LGTLineOfInsuranceCode = EXP_Insuranceline_CoverageForm_Check.o_InsurancelIne
		LEFT JOIN LKP_SupPassThroughChargeType
		ON LKP_SupPassThroughChargeType.DCTTaxCode = EXP_MD5.out_DCTTaxCode
		LEFT JOIN LKP_sup_premium_transaction_code
		ON LKP_sup_premium_transaction_code.prem_trans_code = EXP_MD5.out_prem_trans_code AND LKP_sup_premium_transaction_code.PremiumPlusMinusDescription = EXP_MD5.out_PremPlusMinusDescription
	),
	FIL_Insert_rows AS (
		SELECT
		PassThroughChargeTransactionAKID, 
		AdditionalFilter, 
		out_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
		out_EffectiveDate AS EffectiveDate, 
		out_ExpirationDate AS ExpirationDate, 
		out_SourceSystemID AS SourceSystemID, 
		out_CreateDate AS CreateDate, 
		out_ModifiedDate AS ModifiedDate, 
		out_logicalIndicator AS logicalIndicator, 
		out_LogicalDeleteFlag AS LogicalDeleteFlag, 
		out_DuplicateSequence AS DuplicateSequence, 
		out_PassThroughChargeTransactionHashKey AS PassThroughChargeTransactionHashKey, 
		out_Type AS Type, 
		out_PassThroughChargeTransactionEnteredDate AS PassThroughChargeTransactionEnteredDate, 
		out_PassThroughChargeTransactionEffectiveDate AS PassThroughChargeTransactionEffectiveDate, 
		out_PassThroughChargeTransactionExpirationDate AS PassThroughChargeTransactionExpirationDate, 
		out_PassThroughChargeTransactionBookedDate AS PassThroughChargeTransactionBookedDate, 
		out_PassThroughChargeTransactionAmount AS PassThroughChargeTransactionAmount, 
		out_FullTermPremium AS FullTermPremium, 
		out_FullTaxAmount AS FullTaxAmount, 
		out_TaxPercentageRate AS TaxPercentageRate, 
		out_ReasonAmendedCode AS ReasonAmendedCode, 
		out_sup_prem_trans_code_id AS sup_prem_trans_code_id, 
		out_RiskLocationAKID AS RiskLocationAKID, 
		out_PolicyAKID AS PolicyAKID, 
		out_SupLGTLineOfInsuranceId AS SupLGTLineOfInsuranceId, 
		out_PolicyCoverageAKID AS PolicyCoverageAKID, 
		out_SupSurchargeExemptID AS SupSurchargeExemptID, 
		out_SupPassThroughChargeTypeID AS SupPassThroughChargeTypeID, 
		out_TotalAnnualPremiumSubjectToTax AS TotalAnnualPremiumSubjectToTax, 
		out_TaxSurchargeObjectName AS TaxSurchargeObjectName, 
		out_DCTTaxCode AS DCTTaxCode, 
		out_Purpose AS Purpose, 
		LoadSequence, 
		RestateRepair, 
		ChangeFlag, 
		O_RatingCoverageAKID AS RatingCoverageAKID
		FROM EXP_Detect_Changes
		WHERE DECODE(True,
	RestateRepair='PremiumChange' AND ChangeFlag='Y' AND AdditionalFilter=1, TRUE,
	RestateRepair !='PremiumChange' AND ISNULL(PassThroughChargeTransactionAKID) AND AdditionalFilter=1, TRUE,
	FALSE)
	),
	OUT_PassThroughChargeMapplet AS (
		SELECT
		PassThroughChargeTransactionAKID, 
		CurrentSnapshotFlag, 
		EffectiveDate, 
		ExpirationDate, 
		SourceSystemID, 
		CreateDate, 
		ModifiedDate, 
		logicalIndicator, 
		LogicalDeleteFlag, 
		DuplicateSequence, 
		PassThroughChargeTransactionHashKey, 
		Type, 
		PassThroughChargeTransactionEnteredDate, 
		PassThroughChargeTransactionEffectiveDate, 
		PassThroughChargeTransactionExpirationDate, 
		PassThroughChargeTransactionBookedDate, 
		PassThroughChargeTransactionAmount, 
		FullTermPremium, 
		FullTaxAmount, 
		TaxPercentageRate, 
		ReasonAmendedCode, 
		sup_prem_trans_code_id, 
		RiskLocationAKID, 
		PolicyAKID, 
		SupLGTLineOfInsuranceId, 
		PolicyCoverageAKID, 
		SupSurchargeExemptID, 
		SupPassThroughChargeTypeID, 
		TotalAnnualPremiumSubjectToTax, 
		TaxSurchargeObjectName, 
		DCTTaxCode, 
		Purpose, 
		LoadSequence, 
		RestateRepair, 
		RatingCoverageAKID
		FROM FIL_Insert_rows
	),
),
EXP_Detemine_AK_ID AS (
	SELECT
	CurrentSnapshotFlag,
	-1*@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	EffectiveDate,
	ExpirationDate,
	SourceSystemID,
	CreateDate,
	ModifiedDate,
	logicalIndicator,
	LogicalDeleteFlag,
	DuplicateSequence,
	PassThroughChargeTransactionHashKey AS PassTroughChargeTransactionHashKey,
	-1 AS StatisticalCoverageAKID,
	Type,
	PassThroughChargeTransactionEnteredDate,
	PassThroughChargeTransactionEffectiveDate,
	PassThroughChargeTransactionExpirationDate,
	PassThroughChargeTransactionBookedDate,
	PassThroughChargeTransactionAmount,
	FullTermPremium,
	FullTaxAmount,
	TaxPercentageRate,
	ReasonAmendedCode,
	sup_prem_trans_code_id,
	RiskLocationAKID,
	PolicyAKID,
	SupLGTLineOfInsuranceId,
	PolicyCoverageAKID,
	SupSurchargeExemptID,
	SupPassThroughChargeTypeID,
	TotalAnnualPremiumSubjectToTax,
	SEQ_PassThroughChargeTransactionAKID.NEXTVAL AS PassThroughChargeTransactionAKID,
	DCTTaxCode,
	Purpose1 AS Purpose,
	LoadSequence,
	RestateRepair1 AS NegateRestateCode,
	RatingCoverageAKID
	FROM mplt_PassThroughCharge
),
LKP_PassThroughChargeTransaction_Offset AS (
	SELECT
	EffectiveDate,
	ExpirationDate,
	LogicalIndicator,
	LogicalDeleteFlag,
	DuplicateSequence,
	PassThroughChargeTransactionHashKey,
	PassThroughChargeTransactionAKID,
	StatisticalCoverageAKID,
	PassThroughChargeTransactionCode,
	PassThroughChargeTransactionEnteredDate,
	PassThroughChargeTransactionEffectiveDate,
	PassThroughChargeTransactionExpirationDate,
	PassThroughChargeTransactionBookedDate,
	PassThroughChargeTransactionAmount,
	FullTermPremium,
	FullTaxAmount,
	TaxPercentageRate,
	ReasonAmendedCode,
	PassThroughChargeTransactionCodeId,
	RisklocationAKID,
	PolicyAKID,
	SupLGTLineOfInsuranceID,
	SupSurchargeExemptID,
	SupPassThroughChargeTypeID,
	TotalAnnualPremiumSubjectToTax,
	PolicyCoverageAKId,
	DCTTaxCode
	FROM (
		SELECT EffectiveDate as EffectiveDate, 
		ExpirationDate as ExpirationDate, 
		LogicalIndicator as LogicalIndicator, 
		LogicalDeleteFlag as LogicalDeleteFlag, 
		DuplicateSequence as DuplicateSequence, 
		PassThroughChargeTransactionAKID as PassThroughChargeTransactionAKID, 
		StatisticalCoverageAKID as StatisticalCoverageAKID, 
		PassThroughChargeTransactionCode as PassThroughChargeTransactionCode, 
		PassThroughChargeTransactionEnteredDate as PassThroughChargeTransactionEnteredDate, 
		PassThroughChargeTransactionEffectiveDate as PassThroughChargeTransactionEffectiveDate, 
		PassThroughChargeTransactionExpirationDate as PassThroughChargeTransactionExpirationDate, 
		PassThroughChargeTransactionBookedDate as PassThroughChargeTransactionBookedDate, 
		PassThroughChargeTransactionAmount as PassThroughChargeTransactionAmount, 
		FullTermPremium as FullTermPremium, 
		FullTaxAmount as FullTaxAmount, 
		TaxPercentageRate as TaxPercentageRate, 
		ReasonAmendedCode as ReasonAmendedCode, 
		PassThroughChargeTransactionCodeId as PassThroughChargeTransactionCodeId, 
		RisklocationAKID as RisklocationAKID, 
		PolicyAKID as PolicyAKID, 
		SupLGTLineOfInsuranceID as SupLGTLineOfInsuranceID, 
		SupSurchargeExemptID as SupSurchargeExemptID, 
		SupPassThroughChargeTypeID as SupPassThroughChargeTypeID, 
		TotalAnnualPremiumSubjectToTax as TotalAnnualPremiumSubjectToTax, 
		PolicyCoverageAKId as PolicyCoverageAKId, 
		DCTTaxCode as DCTTaxCode, 
		PassThroughChargeTransactionHashKey as PassThroughChargeTransactionHashKey 
		FROM PassThroughChargeTransaction
		WHERE OffsetOnsetCode<>'Deprecated' and SourceSystemID='DCT'
		ORDER BY PassThroughChargeTransactionHashKey,CreatedDate desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionHashKey ORDER BY EffectiveDate) = 1
),
FIL_PassThroughOnset AS (
	SELECT
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionAmount AS lkp_PassThroughChargeTransactionAmount, 
	EXP_Detemine_AK_ID.CurrentSnapshotFlag, 
	EXP_Detemine_AK_ID.o_AuditID AS AuditID, 
	EXP_Detemine_AK_ID.EffectiveDate, 
	EXP_Detemine_AK_ID.ExpirationDate, 
	EXP_Detemine_AK_ID.SourceSystemID, 
	EXP_Detemine_AK_ID.CreateDate AS CreatedDate, 
	EXP_Detemine_AK_ID.ModifiedDate, 
	EXP_Detemine_AK_ID.logicalIndicator AS LogicalIndicator, 
	EXP_Detemine_AK_ID.LogicalDeleteFlag, 
	EXP_Detemine_AK_ID.DuplicateSequence, 
	EXP_Detemine_AK_ID.PassTroughChargeTransactionHashKey AS PassThroughChargeTransactionHashKey, 
	EXP_Detemine_AK_ID.PassThroughChargeTransactionAKID, 
	EXP_Detemine_AK_ID.StatisticalCoverageAKID, 
	EXP_Detemine_AK_ID.Type AS PassThroughChargeTransactionCode, 
	EXP_Detemine_AK_ID.PassThroughChargeTransactionEnteredDate, 
	EXP_Detemine_AK_ID.PassThroughChargeTransactionEffectiveDate, 
	EXP_Detemine_AK_ID.PassThroughChargeTransactionExpirationDate, 
	EXP_Detemine_AK_ID.PassThroughChargeTransactionBookedDate, 
	EXP_Detemine_AK_ID.PassThroughChargeTransactionAmount, 
	EXP_Detemine_AK_ID.FullTermPremium, 
	EXP_Detemine_AK_ID.FullTaxAmount, 
	EXP_Detemine_AK_ID.TaxPercentageRate, 
	EXP_Detemine_AK_ID.ReasonAmendedCode, 
	EXP_Detemine_AK_ID.sup_prem_trans_code_id AS PassThroughChargeTransactionCodeId, 
	EXP_Detemine_AK_ID.RiskLocationAKID AS RisklocationAKID, 
	EXP_Detemine_AK_ID.PolicyAKID, 
	EXP_Detemine_AK_ID.SupLGTLineOfInsuranceId AS SupLGTLineOfInsuranceID, 
	EXP_Detemine_AK_ID.PolicyCoverageAKID AS PolicyCoverageAKId, 
	EXP_Detemine_AK_ID.SupSurchargeExemptID, 
	EXP_Detemine_AK_ID.SupPassThroughChargeTypeID, 
	EXP_Detemine_AK_ID.TotalAnnualPremiumSubjectToTax, 
	EXP_Detemine_AK_ID.DCTTaxCode, 
	EXP_Detemine_AK_ID.Purpose AS OffsetOnsetCode, 
	EXP_Detemine_AK_ID.LoadSequence, 
	EXP_Detemine_AK_ID.NegateRestateCode, 
	EXP_Detemine_AK_ID.RatingCoverageAKID
	FROM EXP_Detemine_AK_ID
	LEFT JOIN LKP_PassThroughChargeTransaction_Offset
	ON LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionHashKey = EXP_Detemine_AK_ID.PassTroughChargeTransactionHashKey
	WHERE NOT ISNULL(lkp_PassThroughChargeTransactionAmount)
),
TGT_PassThroughChargeTransaction_ONSET_INSERT AS (
	INSERT INTO PassThroughChargeTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, DuplicateSequence, PassThroughChargeTransactionHashKey, PassThroughChargeTransactionAKID, StatisticalCoverageAKID, PassThroughChargeTransactionCode, PassThroughChargeTransactionEnteredDate, PassThroughChargeTransactionEffectiveDate, PassThroughChargeTransactionExpirationDate, PassThroughChargeTransactionBookedDate, PassThroughChargeTransactionAmount, FullTermPremium, FullTaxAmount, TaxPercentageRate, ReasonAmendedCode, PassThroughChargeTransactionCodeId, RiskLocationAKID, PolicyAKID, SupLGTLineOfInsuranceID, PolicyCoverageAKID, SupSurchargeExemptID, SupPassThroughChargeTypeID, TotalAnnualPremiumSubjectToTax, DCTTaxCode, OffsetOnsetCode, LoadSequence, NegateRestateCode, RatingCoverageAKID)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	DUPLICATESEQUENCE, 
	PASSTHROUGHCHARGETRANSACTIONHASHKEY, 
	PASSTHROUGHCHARGETRANSACTIONAKID, 
	STATISTICALCOVERAGEAKID, 
	PASSTHROUGHCHARGETRANSACTIONCODE, 
	PASSTHROUGHCHARGETRANSACTIONENTEREDDATE, 
	PASSTHROUGHCHARGETRANSACTIONEFFECTIVEDATE, 
	PASSTHROUGHCHARGETRANSACTIONEXPIRATIONDATE, 
	PASSTHROUGHCHARGETRANSACTIONBOOKEDDATE, 
	PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	FULLTAXAMOUNT, 
	TAXPERCENTAGERATE, 
	REASONAMENDEDCODE, 
	PASSTHROUGHCHARGETRANSACTIONCODEID, 
	RisklocationAKID AS RISKLOCATIONAKID, 
	POLICYAKID, 
	SUPLGTLINEOFINSURANCEID, 
	PolicyCoverageAKId AS POLICYCOVERAGEAKID, 
	SUPSURCHARGEEXEMPTID, 
	SUPPASSTHROUGHCHARGETYPEID, 
	TOTALANNUALPREMIUMSUBJECTTOTAX, 
	DCTTAXCODE, 
	OFFSETONSETCODE, 
	LOADSEQUENCE, 
	NEGATERESTATECODE, 
	RATINGCOVERAGEAKID
	FROM FIL_PassThroughOnset
),
EXP_PassThrough_Offset AS (
	SELECT
	'1' AS CurrentSnapshotFlag,
	EXP_Detemine_AK_ID.o_AuditID AS AuditID,
	LKP_PassThroughChargeTransaction_Offset.EffectiveDate,
	LKP_PassThroughChargeTransaction_Offset.ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemID,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	LKP_PassThroughChargeTransaction_Offset.LogicalIndicator,
	'1' AS o_LogicalDeleteFlag,
	LKP_PassThroughChargeTransaction_Offset.DuplicateSequence,
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionHashKey,
	SEQ_PassThroughChargeTransactionAKID.NEXTVAL AS PassThroughChargeTransactionAKID,
	LKP_PassThroughChargeTransaction_Offset.StatisticalCoverageAKID,
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionCode,
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionEnteredDate,
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionEffectiveDate,
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionExpirationDate,
	EXP_Detemine_AK_ID.PassThroughChargeTransactionBookedDate,
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionAmount AS i_PassThroughChargeTransactionAmount,
	-1*i_PassThroughChargeTransactionAmount AS o_PassThroughChargeTransactionAmount,
	LKP_PassThroughChargeTransaction_Offset.FullTermPremium AS i_FullTermPremium,
	-1*i_FullTermPremium AS o_FullTermPremium,
	LKP_PassThroughChargeTransaction_Offset.FullTaxAmount AS i_FullTaxAmount,
	-1*i_FullTaxAmount AS o_FullTaxAmount,
	LKP_PassThroughChargeTransaction_Offset.TaxPercentageRate,
	LKP_PassThroughChargeTransaction_Offset.ReasonAmendedCode,
	LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionCodeId,
	LKP_PassThroughChargeTransaction_Offset.RisklocationAKID,
	LKP_PassThroughChargeTransaction_Offset.PolicyAKID,
	LKP_PassThroughChargeTransaction_Offset.SupLGTLineOfInsuranceID,
	LKP_PassThroughChargeTransaction_Offset.SupSurchargeExemptID,
	LKP_PassThroughChargeTransaction_Offset.SupPassThroughChargeTypeID,
	LKP_PassThroughChargeTransaction_Offset.TotalAnnualPremiumSubjectToTax AS i_TotalAnnualPremiumSubjectToTax,
	-1*i_TotalAnnualPremiumSubjectToTax AS o_TotalAnnualPremiumSubjectToTax,
	LKP_PassThroughChargeTransaction_Offset.PolicyCoverageAKId,
	LKP_PassThroughChargeTransaction_Offset.DCTTaxCode,
	-- *INF*: 'Deprecated'
	-- 
	-- --IIF(in_Purpose!='Offset',in_Purpose,'Deprecated')
	'Deprecated' AS OffsetOnsetCode,
	EXP_Detemine_AK_ID.LoadSequence,
	EXP_Detemine_AK_ID.NegateRestateCode,
	EXP_Detemine_AK_ID.RatingCoverageAKID
	FROM EXP_Detemine_AK_ID
	LEFT JOIN LKP_PassThroughChargeTransaction_Offset
	ON LKP_PassThroughChargeTransaction_Offset.PassThroughChargeTransactionHashKey = EXP_Detemine_AK_ID.PassTroughChargeTransactionHashKey
),
FIL_PassThroughOffset AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	LogicalIndicator, 
	o_LogicalDeleteFlag AS LogicalDeleteFlag, 
	DuplicateSequence, 
	PassThroughChargeTransactionHashKey, 
	PassThroughChargeTransactionAKID, 
	StatisticalCoverageAKID, 
	PassThroughChargeTransactionCode, 
	PassThroughChargeTransactionEnteredDate, 
	PassThroughChargeTransactionEffectiveDate, 
	PassThroughChargeTransactionExpirationDate, 
	PassThroughChargeTransactionBookedDate, 
	o_PassThroughChargeTransactionAmount AS PassThroughChargeTransactionAmount, 
	o_FullTermPremium AS FullTermPremium, 
	o_FullTaxAmount AS FullTaxAmount, 
	TaxPercentageRate, 
	ReasonAmendedCode, 
	PassThroughChargeTransactionCodeId, 
	RisklocationAKID, 
	PolicyAKID, 
	SupLGTLineOfInsuranceID, 
	PolicyCoverageAKId, 
	SupSurchargeExemptID, 
	SupPassThroughChargeTypeID, 
	o_TotalAnnualPremiumSubjectToTax AS TotalAnnualPremiumSubjectToTax, 
	DCTTaxCode, 
	OffsetOnsetCode, 
	LoadSequence, 
	NegateRestateCode, 
	RatingCoverageAKID
	FROM EXP_PassThrough_Offset
	WHERE NOT ISNULL(PassThroughChargeTransactionAmount)
),
TGT_PassThroughChargeTransaction_OFFSET_INSERT AS (
	INSERT INTO PassThroughChargeTransaction
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, LogicalIndicator, LogicalDeleteFlag, DuplicateSequence, PassThroughChargeTransactionHashKey, PassThroughChargeTransactionAKID, StatisticalCoverageAKID, PassThroughChargeTransactionCode, PassThroughChargeTransactionEnteredDate, PassThroughChargeTransactionEffectiveDate, PassThroughChargeTransactionExpirationDate, PassThroughChargeTransactionBookedDate, PassThroughChargeTransactionAmount, FullTermPremium, FullTaxAmount, TaxPercentageRate, ReasonAmendedCode, PassThroughChargeTransactionCodeId, RiskLocationAKID, PolicyAKID, SupLGTLineOfInsuranceID, PolicyCoverageAKID, SupSurchargeExemptID, SupPassThroughChargeTypeID, TotalAnnualPremiumSubjectToTax, DCTTaxCode, OffsetOnsetCode, LoadSequence, NegateRestateCode, RatingCoverageAKID)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LOGICALINDICATOR, 
	LOGICALDELETEFLAG, 
	DUPLICATESEQUENCE, 
	PASSTHROUGHCHARGETRANSACTIONHASHKEY, 
	PASSTHROUGHCHARGETRANSACTIONAKID, 
	STATISTICALCOVERAGEAKID, 
	PASSTHROUGHCHARGETRANSACTIONCODE, 
	PASSTHROUGHCHARGETRANSACTIONENTEREDDATE, 
	PASSTHROUGHCHARGETRANSACTIONEFFECTIVEDATE, 
	PASSTHROUGHCHARGETRANSACTIONEXPIRATIONDATE, 
	PASSTHROUGHCHARGETRANSACTIONBOOKEDDATE, 
	PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	FULLTERMPREMIUM, 
	FULLTAXAMOUNT, 
	TAXPERCENTAGERATE, 
	REASONAMENDEDCODE, 
	PASSTHROUGHCHARGETRANSACTIONCODEID, 
	RisklocationAKID AS RISKLOCATIONAKID, 
	POLICYAKID, 
	SUPLGTLINEOFINSURANCEID, 
	PolicyCoverageAKId AS POLICYCOVERAGEAKID, 
	SUPSURCHARGEEXEMPTID, 
	SUPPASSTHROUGHCHARGETYPEID, 
	TOTALANNUALPREMIUMSUBJECTTOTAX, 
	DCTTAXCODE, 
	OFFSETONSETCODE, 
	LOADSEQUENCE, 
	NEGATERESTATECODE, 
	RATINGCOVERAGEAKID
	FROM FIL_PassThroughOffset
),