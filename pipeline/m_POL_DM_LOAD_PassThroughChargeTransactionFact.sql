WITH
LKP_Calender_Dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id DESC) = 1
),
LKP_InsuranceReferenceDim AS (
	SELECT
	InsuranceReferenceDimId,
	EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode,
	InsuranceSegmentCode,
	PolicyOfferingCode,
	ProductCode,
	InsuranceReferenceLineOfBusinessCode,
	RatingPlanCode
	FROM (
		SELECT 
			InsuranceReferenceDimId,
			EnterpriseGroupCode,
			InsuranceReferenceLegalEntityCode,
			StrategicProfitCenterCode,
			InsuranceSegmentCode,
			PolicyOfferingCode,
			ProductCode,
			InsuranceReferenceLineOfBusinessCode,
			RatingPlanCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceReferenceDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EnterpriseGroupCode,InsuranceReferenceLegalEntityCode,StrategicProfitCenterCode,InsuranceSegmentCode,PolicyOfferingCode,ProductCode,InsuranceReferenceLineOfBusinessCode,RatingPlanCode ORDER BY InsuranceReferenceDimId) = 1
),
SQ_PassThroughChargeTransaction_AndFact_Physical_Deletes AS (
	SELECT 
	PTCTF.PassThroughChargeTransactionFactID
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PassThroughChargeTransaction PTCT
	inner join 
	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME} PTCTF
	on PTCT.PassThroughChargeTransactionID=PTCTF.EDWPassThroughChargeTransactionPkId
	WHERE
	PTCT.PassThroughChargeTransactionEnteredDate>='01-01-1998'
	AND PTCT.ModifiedDate>='@{pipeline().parameters.SELECTION_START_TS}' 
	AND PTCT.CurrentSnapshotFlag='0'
),
EXP_pass_through AS (
	SELECT
	PassThroughChargeTransactionFactID
	FROM SQ_PassThroughChargeTransaction_AndFact_Physical_Deletes
),
UPD_Delete_expired_records AS (
	SELECT
	PassThroughChargeTransactionFactID
	FROM EXP_pass_through
),
TGT_PassThroughChargeTransactionFact_DEL AS (
	DELETE FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
	WHERE (PassThroughChargeTransactionFactID) IN (SELECT  PASSTHROUGHCHARGETRANSACTIONFACTID FROM UPD_Delete_expired_records)
),
SQ_PassThroughChargeTransaction AS (
	SELECT distinct 
	PTCT.PassThroughChargeTransactionID , 
	PTCT.StatisticalCoverageAKID , 
	PTCT.PassThroughChargeTransactionEnteredDate ,
	PTCT.PassThroughChargeTransactionEffectiveDate , 
	PTCT.PassThroughChargeTransactionExpirationDate ,
	PTCT.PassThroughChargeTransactionBookedDate , 
	PTCT.PassThroughChargeTransactionAmount , 
	PTCT.FullTaxAmount , 
	PTCT.TaxPercentageRate ,
	PTCT.ReasonAmendedCode , 
	PTCT.PassThroughChargeTransactionCodeId ,
	SC.MajorPerilCode ,
	POL.contract_cust_ak_id ,
	POL.AgencyAKId , 
	PTCT.PolicyAKID ,
	PC.InsuranceLine , 
	PTCT.SupPassThroughChargeTypeID , 
	PTCT.TotalAnnualPremiumSubjectToTax , 
	RL.RiskLocationHashKey ,
	Product.ProductCode , 
	PolicyOffering.PolicyOfferingCode ,
	InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessCode , 
	EnterpriseGroup.EnterpriseGroupCode , 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode ,
	StrategicProfitCenter.StrategicProfitCenterCode , 
	InsuranceSegment.InsuranceSegmentCode,
	RPDT.RatingPlanCode,
	PTCT.SupLGTLineOfInsuranceID
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PassThroughChargeTransaction PTCT
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StatisticalCoverage SC
	ON PTCT.StatisticalCoverageAKID=SC.StatisticalCoverageAKID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON PC.PolicyCoverageAKID=SC.PolicyCoverageAKID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	ON PC.RiskLocationAKID=RL.RiskLocationAKID 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	ON RL.PolicyAKID=POL.pol_ak_id 
	and POL.crrnt_snpsht_flag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Product 
	ON Product.ProductAKId=SC.ProductAKId 
	AND Product.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering 
	ON PolicyOffering.PolicyOfferingAKId=POL.PolicyOfferingAKId 
	AND PolicyOffering.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness 
	ON InsuranceReferenceLineOfBusiness.InsuranceReferenceLineOfBusinessAKId=SC.InsuranceReferenceLineOfBusinessAKId 
	AND InsuranceReferenceLineOfBusiness.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter 
	ON POL.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId 
	AND StrategicProfitCenter.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment 
	ON InsuranceSegment.InsuranceSegmentAKId=POL.InsuranceSegmentAKId 
	AND InsuranceSegment.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup 
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity 
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	on PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE
	POL.source_sys_id='PMS'
	AND PTCT.PassThroughChargeTransactionEnteredDate>='01-01-1998' 
	--AND PTCT.CREATEDDATE>='@{pipeline().parameters.SELECTION_START_TS}' 
	AND PTCT.CurrentSnapshotFlag='1' AND NOT EXISTS (
	SELECT 1 FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransactionFact PTCTF
	WHERE PTCT.PassThroughChargeTransactionID=PTCTF.EDWPassThroughChargeTransactionPkId)
	
	UNION ALL 
	
	SELECT distinct 
	PTCT.PassThroughChargeTransactionID , 
	PTCT.StatisticalCoverageAKID , 
	PTCT.PassThroughChargeTransactionEnteredDate ,
	PTCT.PassThroughChargeTransactionEffectiveDate , 
	PTCT.PassThroughChargeTransactionExpirationDate ,
	PTCT.PassThroughChargeTransactionBookedDate , 
	PTCT.PassThroughChargeTransactionAmount , 
	PTCT.FullTaxAmount , 
	PTCT.TaxPercentageRate ,
	PTCT.ReasonAmendedCode , 
	PTCT.PassThroughChargeTransactionCodeId ,
	'N/A' AS MajorPerilCode ,
	POL.contract_cust_ak_id ,
	POL.AgencyAKId , 
	PTCT.PolicyAKID ,
	PC.InsuranceLine , 
	PTCT.SupPassThroughChargeTypeID , 
	PTCT.TotalAnnualPremiumSubjectToTax , 
	RL.RiskLocationHashKey ,
	'N/A' AS ProductCode , 
	PolicyOffering.PolicyOfferingCode ,
	'N/A' AS InsuranceReferenceLineOfBusinessCode , 
	EnterpriseGroup.EnterpriseGroupCode , 
	InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityCode ,
	StrategicProfitCenter.StrategicProfitCenterCode , 
	InsuranceSegment.InsuranceSegmentCode,
	RPDT.RatingPlanCode,
	PTCT.SupLGTLineOfInsuranceID
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.PassThroughChargeTransaction PTCT
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage PC
	ON PC.PolicyCoverageAKID=PTCT.PolicyCoverageAKID 
	AND PC.CurrentSnapshotFlag=1
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RiskLocation RL
	ON PC.RiskLocationAKID=RL.RiskLocationAKID 
	and RL.CurrentSnapshotFlag=1 
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy POL
	ON PTCT.PolicyAKID=POL.pol_ak_id 
	and POL.crrnt_snpsht_flag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyOffering 
	ON PolicyOffering.PolicyOfferingAKId=POL.PolicyOfferingAKId 
	AND PolicyOffering.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.StrategicProfitCenter 
	ON POL.StrategicProfitCenterAKId=StrategicProfitCenter.StrategicProfitCenterAKId 
	AND StrategicProfitCenter.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceSegment 
	ON InsuranceSegment.InsuranceSegmentAKId=POL.InsuranceSegmentAKId 
	AND InsuranceSegment.CurrentSnapshotFlag=1 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.EnterpriseGroup 
	ON EnterpriseGroup.EnterpriseGroupId=StrategicProfitCenter.EnterpriseGroupId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLegalEntity 
	ON InsuranceReferenceLegalEntity.InsuranceReferenceLegalEntityId=StrategicProfitCenter.InsuranceReferenceLegalEntityId 
	LEFT OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingPlan RPDT
	on PC.RatingPlanAKId=RPDT.RatingPlanAKId and RPDT.CurrentSnapshotFlag=1
	WHERE
	POL.source_sys_id='DCT'
	--AND PTCT.CREATEDDATE>='@{pipeline().parameters.SELECTION_START_TS}' 
	AND PTCT.CurrentSnapshotFlag='1' AND NOT EXISTS (
	SELECT 1 FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransactionFact PTCTF
	WHERE PTCT.PassThroughChargeTransactionID=PTCTF.EDWPassThroughChargeTransactionPkId)
),
Exp_get_data AS (
	SELECT
	PassThroughChargeTransactionID,
	StatisticalCoverageAKID,
	PassThroughChargeTransactionEnteredDate,
	PassThroughChargeTransactionEffectiveDate,
	PassThroughChargeTransactionExpirationDate,
	PassThroughChargeTransactionBookedDate,
	PassThroughChargeTransactionAmount,
	FullTaxAmount,
	TaxPercentageRate,
	ReasonAmendedCode,
	PassThroughChargeTransactionCodeId,
	MajorPerilCode,
	contract_cust_ak_id,
	AgencyAkId,
	PolicyAKID,
	InsuranceLine,
	SupPassThroughChargeTypeID,
	TotalAnnualPremiumSubjectToTax,
	RiskLocationHashKey,
	ProductCode AS i_ProductCode,
	PolicyOfferingCode AS i_PolicyOfferingCode,
	InsuranceReferenceLineOfBusinessCode AS i_InsuranceReferenceLineOfBusinessCode,
	EnterpriseGroupCode AS i_EnterpriseGroupCode,
	InsuranceReferenceLegalEntityCode AS i_InsuranceReferenceLegalEntityCode,
	StrategicProfitCenterCode AS i_StrategicProfitCenterCode,
	InsuranceSegmentCode AS i_InsuranceSegmentCode,
	RatingPlanCode AS i_RatingPlanCode,
	SupLGTLineOfInsuranceID,
	-- *INF*: IIF(NOT ISNULL(i_ProductCode), i_ProductCode, '000')
	IFF(i_ProductCode IS NOT NULL, i_ProductCode, '000') AS ProductCode,
	-- *INF*: IIF(NOT ISNULL(i_PolicyOfferingCode), i_PolicyOfferingCode, '000')
	IFF(i_PolicyOfferingCode IS NOT NULL, i_PolicyOfferingCode, '000') AS PolicyOfferingCode,
	-- *INF*: IIF(NOT ISNULL(i_InsuranceReferenceLineOfBusinessCode), i_InsuranceReferenceLineOfBusinessCode, '000')
	IFF(
	    i_InsuranceReferenceLineOfBusinessCode IS NOT NULL, i_InsuranceReferenceLineOfBusinessCode,
	    '000'
	) AS InsuranceReferenceLineOfBusinessCode,
	-- *INF*: IIF(NOT ISNULL(i_EnterpriseGroupCode), i_EnterpriseGroupCode, '1')
	IFF(i_EnterpriseGroupCode IS NOT NULL, i_EnterpriseGroupCode, '1') AS EnterpriseGroupCode,
	-- *INF*: IIF(NOT ISNULL(i_InsuranceReferenceLegalEntityCode), i_InsuranceReferenceLegalEntityCode, '1')
	IFF(
	    i_InsuranceReferenceLegalEntityCode IS NOT NULL, i_InsuranceReferenceLegalEntityCode, '1'
	) AS InsuranceReferenceLegalEntityCode,
	-- *INF*: IIF(NOT ISNULL(i_StrategicProfitCenterCode), i_StrategicProfitCenterCode, '6')
	IFF(i_StrategicProfitCenterCode IS NOT NULL, i_StrategicProfitCenterCode, '6') AS StrategicProfitCenterCode,
	-- *INF*: IIF(NOT ISNULL(i_InsuranceSegmentCode), i_InsuranceSegmentCode, 'N/A')
	IFF(i_InsuranceSegmentCode IS NOT NULL, i_InsuranceSegmentCode, 'N/A') AS InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_RatingPlanCode), '1', i_RatingPlanCode)
	IFF(i_RatingPlanCode IS NULL, '1', i_RatingPlanCode) AS RatingPlanCode
	FROM SQ_PassThroughChargeTransaction
),
LKP_SupLGTLineOfInsurance AS (
	SELECT
	StandardLGTLineOfInsuranceCode,
	SupLGTLineOfInsuranceId,
	In_SupLGTLineOfInsuranceID
	FROM (
		SELECT 
			StandardLGTLineOfInsuranceCode,
			SupLGTLineOfInsuranceId,
			In_SupLGTLineOfInsuranceID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupLGTLineOfInsurance
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupLGTLineOfInsuranceId ORDER BY StandardLGTLineOfInsuranceCode) = 1
),
LKP_LGTLineOfInsuranceDim AS (
	SELECT
	LGTLineOfInsuranceDimId,
	LGTLineOfInsuranceCode
	FROM (
		SELECT 
			LGTLineOfInsuranceDimId,
			LGTLineOfInsuranceCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.LGTLineOfInsuranceDim
		WHERE CurrentSnapshotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LGTLineOfInsuranceCode ORDER BY LGTLineOfInsuranceDimId) = 1
),
LKP_SupPassThroughChargeType AS (
	SELECT
	PassThroughChargeType,
	IN_SupPassThroughChargeTypeID1,
	SupPassThroughChargeTypeID
	FROM (
		SELECT 
			PassThroughChargeType,
			IN_SupPassThroughChargeTypeID1,
			SupPassThroughChargeTypeID
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupPassThroughChargeType
		WHERE CurrentSnapShotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupPassThroughChargeTypeID ORDER BY PassThroughChargeType) = 1
),
LKP_Sup_Reason_Amended_Code AS (
	SELECT
	StandardReasonAmendedCode,
	rsn_amended_code
	FROM (
		SELECT 
			StandardReasonAmendedCode,
			rsn_amended_code
		FROM Sup_Reason_Amended_Code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY rsn_amended_code ORDER BY StandardReasonAmendedCode) = 1
),
lkp_sup_premim_transaction_code AS (
	SELECT
	StandardPremiumTransactionCode,
	sup_prem_trans_code_id
	FROM (
		SELECT
		sup_premium_transaction_code.sup_prem_trans_code_id as sup_prem_trans_code_id,
		sup_premium_transaction_code.StandardPremiumTransactionCode as StandardPremiumTransactionCode
		FROM sup_premium_transaction_code
		where sup_premium_transaction_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sup_prem_trans_code_id ORDER BY StandardPremiumTransactionCode DESC) = 1
),
LKP_PassThroughChargeTransactionTypeDim AS (
	SELECT
	PassThroughChargeTransactionTypeDimID,
	PassThroughChargeTransactionCode,
	ReasonAmendedCode,
	PassThroughChargeType
	FROM (
		SELECT 
			PassThroughChargeTransactionTypeDimID,
			PassThroughChargeTransactionCode,
			ReasonAmendedCode,
			PassThroughChargeType
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PassThroughChargeTransactionTypeDim
		WHERE CurrentSnapshotFlag='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PassThroughChargeTransactionCode,ReasonAmendedCode,PassThroughChargeType ORDER BY PassThroughChargeTransactionTypeDimID) = 1
),
LKP_RiskLocationDim AS (
	SELECT
	RiskLocationDimID,
	RiskLocationHashKey
	FROM (
		SELECT 
			RiskLocationDimID,
			RiskLocationHashKey
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocationDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RiskLocationHashKey ORDER BY RiskLocationDimID) = 1
),
LKP_V3_AgencyDim AS (
	SELECT
	AgencyDimID,
	EDWAgencyAKID
	FROM (
		SELECT 
			AgencyDimID,
			EDWAgencyAKID
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V3}.AgencyDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWAgencyAKID ORDER BY AgencyDimID) = 1
),
LKP_contract_customer_dim AS (
	SELECT
	contract_cust_dim_id,
	edw_contract_cust_ak_id
	FROM (
		SELECT 
			contract_cust_dim_id,
			edw_contract_cust_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_dim
		WHERE crrnt_snpsht_flag ='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_contract_cust_ak_id ORDER BY contract_cust_dim_id) = 1
),
LKP_policy_dim AS (
	SELECT
	pol_dim_id,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_sym,
	edw_pol_ak_id
	FROM (
		SELECT 
			pol_dim_id,
			pol_eff_date,
			pol_exp_date,
			pol_cancellation_date,
			pol_sym,
			edw_pol_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
		WHERE crrnt_snpsht_flag ='1'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id ORDER BY pol_dim_id) = 1
),
EXP_FinalizeIDs AS (
	SELECT
	LKP_RiskLocationDim.RiskLocationDimID AS in_RiskLocationDimID,
	LKP_V3_AgencyDim.AgencyDimID AS in_agency_dim_id,
	LKP_contract_customer_dim.contract_cust_dim_id AS in_contract_cust_dim_id,
	LKP_policy_dim.pol_dim_id AS in_pol_dim_id,
	LKP_policy_dim.pol_eff_date AS in_pol_eff_date,
	LKP_policy_dim.pol_exp_date AS in_pol_exp_date,
	LKP_policy_dim.pol_cancellation_date AS in_pol_cancellation_date,
	LKP_policy_dim.pol_sym AS in_pol_sym,
	LKP_PassThroughChargeTransactionTypeDim.PassThroughChargeTransactionTypeDimID AS in_PassThroughChargeTransactionTypeDimID,
	LKP_LGTLineOfInsuranceDim.LGTLineOfInsuranceDimId AS in_LGTLineOfInsuranceDimId,
	Exp_get_data.PassThroughChargeTransactionID AS in_PassThroughChargeTransactionID,
	Exp_get_data.PassThroughChargeTransactionEnteredDate AS in_PassThroughChargeTransactionEnteredDate,
	Exp_get_data.PassThroughChargeTransactionEffectiveDate AS in_PassThroughChargeTransactionEffectiveDate,
	Exp_get_data.PassThroughChargeTransactionExpirationDate AS in_PassThroughChargeTransactionExpirationDate,
	Exp_get_data.PassThroughChargeTransactionBookedDate AS in_PassThroughChargeTransactionBookedDate,
	Exp_get_data.PassThroughChargeTransactionAmount AS in_PassThroughChargeTransactionAmount,
	Exp_get_data.FullTaxAmount AS in_FullTaxAmount,
	Exp_get_data.TaxPercentageRate AS in_TaxPercentageRate,
	Exp_get_data.MajorPerilCode AS in_MajorPerilCode,
	Exp_get_data.TotalAnnualPremiumSubjectToTax AS in_TotalAnnualPremiumSubjectToTax,
	Exp_get_data.ProductCode AS in_ProductCode,
	Exp_get_data.PolicyOfferingCode AS in_PolicyOfferingCode,
	Exp_get_data.InsuranceReferenceLineOfBusinessCode AS in_InsuranceReferenceLineOfBusinessCode,
	Exp_get_data.EnterpriseGroupCode AS in_EnterpriseGroupCode,
	Exp_get_data.InsuranceReferenceLegalEntityCode AS in_InsuranceReferenceLegalEntityCode,
	Exp_get_data.StrategicProfitCenterCode AS in_StrategicProfitCenterCode,
	Exp_get_data.InsuranceSegmentCode AS in_InsuranceSegmentCode,
	Exp_get_data.RatingPlanCode AS in_RatingPlanCode,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCEDIM(in_ProductCode,in_PolicyOfferingCode,in_InsuranceReferenceLineOfBusinessCode,in_EnterpriseGroupCode,in_InsuranceReferenceLegalEntityCode,in_StrategicProfitCenterCode,in_InsuranceSegmentCode,in_RatingPlanCode)
	LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceReferenceDimId AS v_InsuranceReferenceDimId,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(in_pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_eff_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(in_pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_exp_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(in_pol_cancellation_date, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_cancellation_date_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_pol_cancellation_date,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(in_PassThroughChargeTransactionEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PassThroughChargeTransactionEffectiveDateID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(in_PassThroughChargeTransactionExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PassThroughChargeTransactionExpirationDateID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(in_PassThroughChargeTransactionBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PassThroughChargeTransactionBookedDateID,
	-- *INF*: :LKP.LKP_CALENDER_DIM(TO_DATE(TO_CHAR(in_PassThroughChargeTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY'))
	LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_id AS v_PassThroughChargeTransactionEnteredDateID,
	-- *INF*: :LKP.LKP_INSURANCEREFERENCEDIM(in_ProductCode,in_PolicyOfferingCode,'300',in_EnterpriseGroupCode,in_InsuranceReferenceLegalEntityCode,in_StrategicProfitCenterCode,in_InsuranceSegmentCode, in_RatingPlanCode)
	LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceReferenceDimId AS v_InsuranceReferenceDimId_300,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS out_AuditId,
	in_PassThroughChargeTransactionID AS out_PassThroughChargeTransactionID,
	-- *INF*: IIF(ISNULL(in_agency_dim_id),-1,in_agency_dim_id)
	IFF(in_agency_dim_id IS NULL, - 1, in_agency_dim_id) AS out_agency_dim_id,
	-- *INF*: IIF(ISNULL(in_pol_dim_id),-1,in_pol_dim_id)
	IFF(in_pol_dim_id IS NULL, - 1, in_pol_dim_id) AS out_pol_dim_id,
	-- *INF*: IIF(ISNULL(in_contract_cust_dim_id),-1,in_contract_cust_dim_id)
	IFF(in_contract_cust_dim_id IS NULL, - 1, in_contract_cust_dim_id) AS out_contract_cust_dim_id,
	-- *INF*: IIF(ISNULL(in_RiskLocationDimID),-1,in_RiskLocationDimID)
	IFF(in_RiskLocationDimID IS NULL, - 1, in_RiskLocationDimID) AS out_RiskLocationDimID,
	-- *INF*: -1
	-- --wreq-9642 removed lookup on statisticalcoveragedim table
	- 1 AS out_StatisticalCoverageDimID,
	-- *INF*: IIF(ISNULL(in_PassThroughChargeTransactionTypeDimID),-1,in_PassThroughChargeTransactionTypeDimID)
	IFF(
	    in_PassThroughChargeTransactionTypeDimID IS NULL, - 1,
	    in_PassThroughChargeTransactionTypeDimID
	) AS out_PassThroughChargeTransactionTypeDimID,
	-- *INF*: IIF(ISNULL(v_pol_eff_date),-1,v_pol_eff_date)
	IFF(v_pol_eff_date IS NULL, - 1, v_pol_eff_date) AS out_PolicyEffectiveDateID,
	-- *INF*: IIF(ISNULL(v_pol_exp_date),-1,v_pol_exp_date)
	IFF(v_pol_exp_date IS NULL, - 1, v_pol_exp_date) AS out_PolicyExpirationDateID,
	-- *INF*: IIF(ISNULL(v_PassThroughChargeTransactionEffectiveDateID),-1,v_PassThroughChargeTransactionEffectiveDateID)
	IFF(
	    v_PassThroughChargeTransactionEffectiveDateID IS NULL, - 1,
	    v_PassThroughChargeTransactionEffectiveDateID
	) AS out_PassThroughChargeTransactionEffectiveDateID,
	-- *INF*: IIF(ISNULL(v_PassThroughChargeTransactionExpirationDateID),-1,v_PassThroughChargeTransactionExpirationDateID)
	IFF(
	    v_PassThroughChargeTransactionExpirationDateID IS NULL, - 1,
	    v_PassThroughChargeTransactionExpirationDateID
	) AS out_PassThroughChargeTransactionExpirationDateID,
	-- *INF*: IIF(ISNULL(v_PassThroughChargeTransactionBookedDateID),-1,v_PassThroughChargeTransactionBookedDateID)
	IFF(
	    v_PassThroughChargeTransactionBookedDateID IS NULL, - 1,
	    v_PassThroughChargeTransactionBookedDateID
	) AS out_PassThroughChargeTransactionBookedDateID,
	-- *INF*: IIF(ISNULL(v_PassThroughChargeTransactionEnteredDateID),-1,v_PassThroughChargeTransactionEnteredDateID)
	IFF(
	    v_PassThroughChargeTransactionEnteredDateID IS NULL, - 1,
	    v_PassThroughChargeTransactionEnteredDateID
	) AS out_PassThroughChargeTransactionEnteredDateID,
	in_FullTaxAmount AS out_FullTaxAmount,
	in_TaxPercentageRate AS out_TaxPercentageRate,
	in_PassThroughChargeTransactionAmount AS out_PassThroughChargeTransactionAmount,
	-- *INF*: IIF(ISNULL(v_pol_cancellation_date),-1,v_pol_cancellation_date)
	IFF(v_pol_cancellation_date IS NULL, - 1, v_pol_cancellation_date) AS out_PolicyCancellationDateID,
	-1 AS out_asl_dim_id,
	-- *INF*: IIF(ISNULL(in_LGTLineOfInsuranceDimId),-1,in_LGTLineOfInsuranceDimId)
	IFF(in_LGTLineOfInsuranceDimId IS NULL, - 1, in_LGTLineOfInsuranceDimId) AS out_LGTLineOfInsuranceDimId,
	in_TotalAnnualPremiumSubjectToTax AS out_TotalAnnualPremiumSubjectToTax,
	-- *INF*: IIF(ISNULL(v_InsuranceReferenceDimId),-1,v_InsuranceReferenceDimId)
	IFF(v_InsuranceReferenceDimId IS NULL, - 1, v_InsuranceReferenceDimId) AS out_InsuranceReferenceDimId
	FROM Exp_get_data
	LEFT JOIN LKP_LGTLineOfInsuranceDim
	ON LKP_LGTLineOfInsuranceDim.LGTLineOfInsuranceCode = LKP_SupLGTLineOfInsurance.StandardLGTLineOfInsuranceCode
	LEFT JOIN LKP_PassThroughChargeTransactionTypeDim
	ON LKP_PassThroughChargeTransactionTypeDim.PassThroughChargeTransactionCode = lkp_sup_premim_transaction_code.StandardPremiumTransactionCode AND LKP_PassThroughChargeTransactionTypeDim.ReasonAmendedCode = LKP_Sup_Reason_Amended_Code.StandardReasonAmendedCode AND LKP_PassThroughChargeTransactionTypeDim.PassThroughChargeType = LKP_SupPassThroughChargeType.PassThroughChargeType
	LEFT JOIN LKP_RiskLocationDim
	ON LKP_RiskLocationDim.RiskLocationHashKey = Exp_get_data.RiskLocationHashKey
	LEFT JOIN LKP_V3_AgencyDim
	ON LKP_V3_AgencyDim.EDWAgencyAKID = Exp_get_data.AgencyAkId
	LEFT JOIN LKP_contract_customer_dim
	ON LKP_contract_customer_dim.edw_contract_cust_ak_id = Exp_get_data.contract_cust_ak_id
	LEFT JOIN LKP_policy_dim
	ON LKP_policy_dim.edw_pol_ak_id = Exp_get_data.PolicyAKID
	LEFT JOIN LKP_INSURANCEREFERENCEDIM LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode
	ON LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.EnterpriseGroupCode = in_ProductCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceReferenceLegalEntityCode = in_PolicyOfferingCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.StrategicProfitCenterCode = in_InsuranceReferenceLineOfBusinessCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceSegmentCode = in_EnterpriseGroupCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.PolicyOfferingCode = in_InsuranceReferenceLegalEntityCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.ProductCode = in_StrategicProfitCenterCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceReferenceLineOfBusinessCode = in_InsuranceSegmentCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_in_InsuranceReferenceLineOfBusinessCode_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.RatingPlanCode = in_RatingPlanCode

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_eff_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(in_pol_eff_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_exp_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(in_pol_exp_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_cancellation_date_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_pol_cancellation_date_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(in_pol_cancellation_date, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionEffectiveDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(in_PassThroughChargeTransactionEffectiveDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionExpirationDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(in_PassThroughChargeTransactionExpirationDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionBookedDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(in_PassThroughChargeTransactionBookedDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_CALENDER_DIM LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY
	ON LKP_CALENDER_DIM_TO_DATE_TO_CHAR_in_PassThroughChargeTransactionEnteredDate_MM_DD_YYYY_MM_DD_YYYY.clndr_date = TO_TIMESTAMP(TO_CHAR(in_PassThroughChargeTransactionEnteredDate, 'MM/DD/YYYY'), 'MM/DD/YYYY')

	LEFT JOIN LKP_INSURANCEREFERENCEDIM LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode
	ON LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.EnterpriseGroupCode = in_ProductCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceReferenceLegalEntityCode = in_PolicyOfferingCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.StrategicProfitCenterCode = '300'
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceSegmentCode = in_EnterpriseGroupCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.PolicyOfferingCode = in_InsuranceReferenceLegalEntityCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.ProductCode = in_StrategicProfitCenterCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.InsuranceReferenceLineOfBusinessCode = in_InsuranceSegmentCode
	AND LKP_INSURANCEREFERENCEDIM_in_ProductCode_in_PolicyOfferingCode_300_in_EnterpriseGroupCode_in_InsuranceReferenceLegalEntityCode_in_StrategicProfitCenterCode_in_InsuranceSegmentCode_in_RatingPlanCode.RatingPlanCode = in_RatingPlanCode

),
TGT_PassThroughChargeTransactionFact_INS AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME};
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.@{pipeline().parameters.TARGET_TABLE_NAME}
	(AuditId, EDWPassThroughChargeTransactionPkId, AgencyDimID, PolicyDimID, ContractCustomerDimID, RiskLocationDimID, StatisticalCoverageDimID, PassThroughChargeTransactionTypeDimID, PolicyEffectiveDateID, PolicyExpirationDateID, PassThroughChargeTransactionEffectiveDateId, PassThroughChargeTransactionExpirationDateId, PassThroughChargeTransactionBookedDateId, PassThroughChargeTransactionEnteredDateId, FullTaxAmount, TaxPercentageRate, PassThroughChargeTransactionAmount, PolicyCancellationDateID, asl_dim_id, LGTLineOfInsuranceDimId, TotalAnnualPremiumSubjectToTax, InsuranceReferenceDimId)
	SELECT 
	out_AuditId AS AUDITID, 
	out_PassThroughChargeTransactionID AS EDWPASSTHROUGHCHARGETRANSACTIONPKID, 
	out_agency_dim_id AS AGENCYDIMID, 
	out_pol_dim_id AS POLICYDIMID, 
	out_contract_cust_dim_id AS CONTRACTCUSTOMERDIMID, 
	out_RiskLocationDimID AS RISKLOCATIONDIMID, 
	out_StatisticalCoverageDimID AS STATISTICALCOVERAGEDIMID, 
	out_PassThroughChargeTransactionTypeDimID AS PASSTHROUGHCHARGETRANSACTIONTYPEDIMID, 
	out_PolicyEffectiveDateID AS POLICYEFFECTIVEDATEID, 
	out_PolicyExpirationDateID AS POLICYEXPIRATIONDATEID, 
	out_PassThroughChargeTransactionEffectiveDateID AS PASSTHROUGHCHARGETRANSACTIONEFFECTIVEDATEID, 
	out_PassThroughChargeTransactionExpirationDateID AS PASSTHROUGHCHARGETRANSACTIONEXPIRATIONDATEID, 
	out_PassThroughChargeTransactionBookedDateID AS PASSTHROUGHCHARGETRANSACTIONBOOKEDDATEID, 
	out_PassThroughChargeTransactionEnteredDateID AS PASSTHROUGHCHARGETRANSACTIONENTEREDDATEID, 
	out_FullTaxAmount AS FULLTAXAMOUNT, 
	out_TaxPercentageRate AS TAXPERCENTAGERATE, 
	out_PassThroughChargeTransactionAmount AS PASSTHROUGHCHARGETRANSACTIONAMOUNT, 
	out_PolicyCancellationDateID AS POLICYCANCELLATIONDATEID, 
	out_asl_dim_id AS ASL_DIM_ID, 
	out_LGTLineOfInsuranceDimId AS LGTLINEOFINSURANCEDIMID, 
	out_TotalAnnualPremiumSubjectToTax AS TOTALANNUALPREMIUMSUBJECTTOTAX, 
	out_InsuranceReferenceDimId AS INSURANCEREFERENCEDIMID
	FROM EXP_FinalizeIDs
),