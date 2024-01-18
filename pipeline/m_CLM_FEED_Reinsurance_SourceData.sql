WITH
LKP_Get_Max_Sapiens_SourceSequenceNumber AS (
	SELECT
	Source_Seq_Num,
	ID
	FROM (
		SELECT MAX(A.SourceSequenceNumber) AS Source_Seq_Num,
			1 AS ID
		FROM (
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaimRestate
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaimRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicyRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicyRestate	) A
			--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Source_Seq_Num DESC) = 1
),
SQ_claim_loss_transaction_fact AS (
	declare @startDate date
	set @startDate = CASE WHEN '@{pipeline().parameters.PMSESSIONNAME}' LIKE '%Restate%' THEN '1800-01-01 00:00:00' ELSE CAST('@{pipeline().parameters.SELECTION_START_TS}' as date) END
	
	select * 
	from (SELECT 
		CASE WHEN cod.claim_num = 'N/A' THEN SUBSTRING(cod.claim_occurrence_key, 1, 20) ELSE RTRIM(cod.claim_num) END as ClaimNumber,
		P.pol_key as PolicyKey,
	      P.pol_eff_date as pol_eff_date,
		IRD.ProductCode AS ProductCode,
		IRD.AccountingProductCode AS AccountingProductCode,
		IRD.StrategicProfitCenterAbbreviation,
		asld.asl_code AS ASLCode,
		CASE 
			WHEN asl_code = '220' then '220'
			ELSE CASE WHEN sub_asl_code = 'N/A' THEN '' ELSE sub_asl_code END
		END AS SubASLCode,
		IRD.InsuranceReferenceLineOfBusinessCode,
		P.state_of_domicile_code AS RiskStateCode,
		cov.edw_claimant_cov_det_ak_id as SubClaim,
		cfint.financial_type_code as FinancialTypeCode,
		cfint.financial_type_code_descript as FinancialTypeCodeDescription,
		cov.cause_of_loss as CauseOfLoss,
		clmt.claimant_num as ClaimantNumber,
		clmt.claimant_full_name as ClaimantFullName,
		cod.claim_loss_date as ClaimLossDate,
		cod.claim_rpted_date as ClaimReportedDate,
		cod.claim_cat_code as ClaimCatastropheCode,
		cod.claim_cat_start_date as ClaimCatastropheStartDate,
		cod.claim_cat_end_date as ClaimCatastropheEndDate,
		transdt.CalendarDate as ClaimTransactionDate,
		ctyp.trans_code as TransactionCode,
		ctyp.trans_code_descript as TransactionCodeDescription,
		cltf.trans_amt as TransactionAmount,
		cltf.trans_hist_amt as TransactionHistoryAmount,
		CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss = '06'	-- WC Medical Loss Paid (WCM)
				THEN cltf.direct_loss_paid_including_recoveries
			ELSE 0.0 
		END as WorkersCompensationMedicalLossPaid,
		CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss = '06'	-- WC Medical Expense Paid (WME)
				THEN cltf.direct_alae_paid_including_recoveries
			ELSE 0.0 
		END as WorkersCompensationMedicalExpensePaid,
		CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss <> '06'	-- WC Indemnity Expense Paid (WCE)
				THEN cltf.direct_alae_paid_including_recoveries
			ELSE 0.0 
		END as WorkersCompensationIndemnityExpensePaid,
		CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss <> '06'	-- WC Indemnity Loss Paid (WCI)
				THEN cltf.direct_loss_paid_including_recoveries
			ELSE 0.0 
		END as WorkersCompensationIndemnityLossPaid,
		CASE
			WHEN IRD.ProductCode <> '100'								-- PC Expense Paid (EXP)
				THEN cltf.direct_alae_paid_including_recoveries
			ELSE 0.0 
		END as PropertyCasualtyExpensePaid,
		CASE
			WHEN IRD.ProductCode <> '100'								-- PC Loss Paid (IDM)
				THEN cltf.direct_loss_paid_including_recoveries
			ELSE 0.0 
		END as PropertyCasualtyLossPaid,
	
		SUM(CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss = '06'	-- WC Medical Loss Outstanding (OWM)
				THEN cltf.direct_loss_outstanding_excluding_recoveries
			ELSE 0.0 
			END) OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code
				ORDER BY cltf.claim_trans_date_id, cltf.edw_claim_trans_pk_id
			) as WorkersCompensationMedicalLossOutstanding,
		SUM(CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss = '06'	-- WC Medical Expense Outstanding (OWE)
				THEN cltf.direct_alae_outstanding_excluding_recoveries
			ELSE 0.0 
			END) OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code
				ORDER BY cltf.claim_trans_date_id, cltf.edw_claim_trans_pk_id
			) as WorkersCompensationMedicalExpenseOutstanding,
		SUM(CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss <> '06'	-- WC Indemnity Expense Outstanding (OWC)
				THEN cltf.direct_alae_outstanding_excluding_recoveries
			ELSE 0.0 
			END) OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code
				ORDER BY cltf.claim_trans_date_id, cltf.edw_claim_trans_pk_id
			) as WorkersCompensationIndemnityExpenseOutstanding,
		SUM(CASE
			WHEN IRD.ProductCode = '100' AND cov.cause_of_loss <> '06'	-- WC Indemnity Loss Outstanding (OWI)
				THEN cltf.direct_loss_outstanding_excluding_recoveries
			ELSE 0.0 
			END) OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code
				ORDER BY cltf.claim_trans_date_id, cltf.edw_claim_trans_pk_id
			) as WorkersCompensationIndemnityLossOutstanding,
		SUM(CASE
			WHEN IRD.ProductCode <> '100'								-- PC Expense Outstanding (OXP)
				THEN cltf.direct_alae_outstanding_excluding_recoveries
			ELSE 0.0 
			END) OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code
				ORDER BY cltf.claim_trans_date_id, cltf.edw_claim_trans_pk_id
			) as PropertyCasualtyExpenseOutstanding,
	
		SUM(CASE
			WHEN IRD.ProductCode <> '100'								-- PC Loss Outstanding (ODM)
				THEN cltf.direct_loss_outstanding_excluding_recoveries
			ELSE 0.0 
		END) OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code  
				ORDER BY cltf.claim_trans_date_id, cltf.edw_claim_trans_pk_id
		) as PropertyCasualtyLossOutstanding,
		cltf.edw_claim_trans_pk_id,
		CASE row_number() OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code
				ORDER BY cltf.claim_trans_date_id desc, cltf.edw_claim_trans_pk_id desc) 
			WHEN 1 THEN 1
			ELSE 0
		END as ContainsOutstandingReserveAmountFlag,
		cov.unit_descript,
		cov.LocationUnitNumber,
		(case when ISNUMERIC(cod.ClaimRelationshipKey)=1 then cod.ClaimRelationshipKey else NULL end) as ClaimRelationshipId,
		ctyp.trans_ctgry_code as ClaimTransactionCategory,
		CASE LEN(RTRIM(cod.claim_occurrence_key)) 
			WHEN 20 THEN 'EXCEED'
			WHEN 23 THEN 'PMS'
			ELSE ''
		END AS SourceSystemID
	
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact cltf with (nolock) 
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P with (nolock) ON cltf.pol_dim_id = P.pol_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction_type_dim ctyp with (nolock) ON cltf.claim_trans_type_dim_id = ctyp.claim_trans_type_dim_id 
			AND ctyp.trans_kind_code = 'D'
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_financial_type_dim cfint with (nolock) ON cfint.claim_financial_type_dim_id = cltf.claim_financial_type_dim_id
			AND cfint.financial_type_code in ('D','E','S','R','B')
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim cod with (nolock) ON cltf.claim_occurrence_dim_id = cod.claim_occurrence_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_dim clmt with (nolock) ON cltf.claimant_dim_id = clmt.claimant_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim cov with (nolock) ON cltf.claimant_cov_dim_id = cov.claimant_cov_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD with (nolock) ON cltf.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim asld with (nolock) ON cltf.asl_dim_id = asld.asl_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim transdt with (nolock) ON transdt.clndr_id = cltf.claim_trans_date_id
		@{pipeline().parameters.WHERE}
	) A 
	where A.ClaimTransactionDate >= @startDate
	@{pipeline().parameters.WHERE_ERROR}
),
EXP_Umbrella_layer AS (
	SELECT
	ClaimNumber,
	PolicyKey,
	pol_eff_date,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	SubASLCode,
	-- *INF*: DECODE (true,
	-- ASLCode = '220',SubASLCode,
	-- ASLCode = '440',SubASLCode,
	-- ASLCode = '500',SubASLCode,
	-- pol_eff_date >= TO_DATE('2020-01-01','YYYY-MM-DD')  AND ClaimLossDate >= TO_DATE('2020-01-01','YYYY-MM-DD'),SubASLCode,
	-- PolicyKey = 'A08302003',SubASLCode,
	-- PolicyKey = 'NSP134447200',SubASLCode,
	-- PolicyKey = 'NAQ196857501',SubASLCode,
	-- PolicyKey = 'A24779000',SubASLCode,
	-- '')
	-- --all policies with claims whose losses are newer than 1/1/2020, asl codes of 220, 440, 500 and the above select policies
	DECODE(
	    true,
	    ASLCode = '220', SubASLCode,
	    ASLCode = '440', SubASLCode,
	    ASLCode = '500', SubASLCode,
	    pol_eff_date >= TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD') AND ClaimLossDate >= TO_TIMESTAMP('2020-01-01', 'YYYY-MM-DD'), SubASLCode,
	    PolicyKey = 'A08302003', SubASLCode,
	    PolicyKey = 'NSP134447200', SubASLCode,
	    PolicyKey = 'NAQ196857501', SubASLCode,
	    PolicyKey = 'A24779000', SubASLCode,
	    ''
	) AS o_SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	cause_of_loss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimCatastropheEndDate,
	ClaimTransactionDate,
	TransactionCode,
	TransactionCodeDescription,
	TransactionAmount,
	TransactionHistoryAmount,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	edw_claim_trans_pk_id AS ClaimTransactionPKId,
	ContainsOutstandingReserveAmountFlag,
	unit_descript AS i_unit_descript,
	LocationUnitNumber AS i_LocationUnitNumber,
	-- *INF*: DECODE(ProductCode,
	-- '900', i_LocationUnitNumber,
	-- '890', IIF(INSTR(i_unit_descript,'UMBRELLA')>0,	
	--         LTRIM(RTRIM(SUBSTR(i_unit_descript,9)))
	-- 		)
	-- 	)
	DECODE(
	    ProductCode,
	    '900', i_LocationUnitNumber,
	    '890', IFF(
	        REGEXP_INSTR(i_unit_descript, 'UMBRELLA') > 0,
	        LTRIM(RTRIM(SUBSTR(i_unit_descript, 9)))
	    )
	) AS v_LayerForUmbrellaPolicies,
	-- *INF*: IIF(IN(ProductCode,'890','900')=1,
	--     IIF(LTRIM(RTRIM(v_LayerForUmbrellaPolicies))='',
	--         '1',
	--         LTRIM(RTRIM(v_LayerForUmbrellaPolicies))),
	--     NULL)
	IFF(
	    ProductCode IN ('890','900') = 1,
	    IFF(
	        LTRIM(RTRIM(v_LayerForUmbrellaPolicies)) = '', '1',
	        LTRIM(RTRIM(v_LayerForUmbrellaPolicies))
	    ),
	    NULL
	) AS v_ReinsuranceUmbrellaLayer,
	-- *INF*: IIF(Is_Number(v_ReinsuranceUmbrellaLayer),
	-- TO_INTEGER(RTRIM(v_ReinsuranceUmbrellaLayer)),
	-- NULL)
	IFF(
	    REGEXP_LIKE(v_ReinsuranceUmbrellaLayer, '^[0-9]+$'),
	    CAST(RTRIM(v_ReinsuranceUmbrellaLayer) AS INTEGER),
	    NULL
	) AS o_ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	ClaimTransactionCategory,
	SourceSystemID
	FROM SQ_claim_loss_transaction_fact
),
RTR_Reserves_Payments AS (
	SELECT
	ClaimNumber,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	o_SubASLCode AS SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	cause_of_loss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimCatastropheEndDate,
	ClaimTransactionDate,
	TransactionCode,
	TransactionCodeDescription,
	TransactionAmount,
	TransactionHistoryAmount,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ClaimTransactionPKId,
	ContainsOutstandingReserveAmountFlag,
	o_ReinsuranceUmbrellaLayer AS ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	ClaimTransactionCategory,
	SourceSystemID
	FROM EXP_Umbrella_layer
),
RTR_Reserves_Payments_Reserves AS (SELECT * FROM RTR_Reserves_Payments WHERE (INDEXOF(TransactionCode,'40','41','42','65','66','90','91','92','95','97','98','99') > 0 
or ContainsOutstandingReserveAmountFlag='1') 
AND 
INDEXOF(FinancialTypeCode,'D','E') > 0),
RTR_Reserves_Payments_Payments AS (SELECT * FROM RTR_Reserves_Payments WHERE INDEXOF(TransactionCode,'40','41','42','65','66','90','91','92','95','97','98','99')=0),
EXP_Payments AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime,
	ClaimNumber,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	cause_of_loss AS CauseOfLoss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimCatastropheEndDate,
	ClaimTransactionDate,
	TransactionCode,
	TransactionCodeDescription,
	'Payment' AS o_TransactionType,
	TransactionAmount,
	TransactionHistoryAmount,
	PaymentSourceSequenceNumber AS SourceSequenceNumber,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ClaimTransactionPKId,
	ContainsOutstandingReserveAmountFlag AS ContainsOutstandingReserveAmountFlag2,
	ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	ClaimTransactionCategory AS ClaimTransactionCategory3,
	SourceSystemID AS SourceSystemID3
	FROM RTR_Reserves_Payments_Payments
),
EXP_Reserves AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDateTime,
	ClaimNumber,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	cause_of_loss AS CauseOfLoss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimCatastropheEndDate,
	ClaimTransactionDate,
	TransactionCode,
	TransactionCodeDescription,
	'Reserve' AS o_TransactionType,
	TransactionAmount,
	TransactionHistoryAmount,
	ReserveSourceSequenceNumber AS SourceSequenceNumber,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ClaimTransactionPKId,
	ContainsOutstandingReserveAmountFlag AS ContainsOutstandingReserveAmountFlag1,
	ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	ClaimTransactionCategory AS ClaimTransactionCategory1,
	SourceSystemID AS SourceSystemID1
	FROM RTR_Reserves_Payments_Reserves
),
union_payment_reserves AS (
	SELECT AuditId, CurrentDateTime, ClaimNumber, PolicyKey, ProductCode, AccountingProductCode, StrategicProfitCenterAbbreviation, ASLCode, SubASLCode, InsuranceReferenceLineOfBusinessCode, RiskStateCode, SubClaim, FinancialTypeCode, FinancialTypeCodeDescription, CauseOfLoss, ClaimantNumber, ClaimantFullName, ClaimLossDate, ClaimReportedDate, ClaimCatastropheCode, ClaimCatastropheStartDate, ClaimCatastropheEndDate, ClaimTransactionDate, TransactionCode, TransactionCodeDescription, o_TransactionType, TransactionAmount, TransactionHistoryAmount, SourceSequenceNumber, WorkersCompensationMedicalLossPaid, WorkersCompensationMedicalExpensePaid, WorkersCompensationIndemnityExpensePaid, WorkersCompensationIndemnityLossPaid, PropertyCasualtyExpensePaid, PropertyCasualtyLossPaid, WorkersCompensationMedicalLossOutstanding, WorkersCompensationMedicalExpenseOutstanding, WorkersCompensationIndemnityExpenseOutstanding, WorkersCompensationIndemnityLossOutstanding, PropertyCasualtyExpenseOutstanding, PropertyCasualtyLossOutstanding, ClaimTransactionPKId, ContainsOutstandingReserveAmountFlag2, ReinsuranceUmbrellaLayer, ClaimRelationshipId, ClaimTransactionCategory3, SourceSystemID3
	FROM EXP_Payments
	UNION
	SELECT AuditId, CurrentDateTime, ClaimNumber, PolicyKey, ProductCode, AccountingProductCode, StrategicProfitCenterAbbreviation, ASLCode, SubASLCode, InsuranceReferenceLineOfBusinessCode, RiskStateCode, SubClaim, FinancialTypeCode, FinancialTypeCodeDescription, CauseOfLoss, ClaimantNumber, ClaimantFullName, ClaimLossDate, ClaimReportedDate, ClaimCatastropheCode, ClaimCatastropheStartDate, ClaimCatastropheEndDate, ClaimTransactionDate, TransactionCode, TransactionCodeDescription, o_TransactionType, TransactionAmount, TransactionHistoryAmount, SourceSequenceNumber, WorkersCompensationMedicalLossPaid, WorkersCompensationMedicalExpensePaid, WorkersCompensationIndemnityExpensePaid, WorkersCompensationIndemnityLossPaid, PropertyCasualtyExpensePaid, PropertyCasualtyLossPaid, WorkersCompensationMedicalLossOutstanding, WorkersCompensationMedicalExpenseOutstanding, WorkersCompensationIndemnityExpenseOutstanding, WorkersCompensationIndemnityLossOutstanding, PropertyCasualtyExpenseOutstanding, PropertyCasualtyLossOutstanding, ClaimTransactionPKId, ContainsOutstandingReserveAmountFlag1 AS ContainsOutstandingReserveAmountFlag2, ReinsuranceUmbrellaLayer, ClaimRelationshipId, ClaimTransactionCategory1 AS ClaimTransactionCategory3, SourceSystemID1 AS SourceSystemID3
	FROM EXP_Reserves
),
sort_claim_subclaim AS (
	SELECT
	AuditId, 
	CurrentDateTime, 
	ClaimNumber, 
	PolicyKey, 
	ProductCode, 
	AccountingProductCode, 
	StrategicProfitCenterAbbreviation, 
	ASLCode, 
	SubASLCode, 
	InsuranceReferenceLineOfBusinessCode, 
	RiskStateCode, 
	SubClaim, 
	FinancialTypeCode, 
	FinancialTypeCodeDescription, 
	CauseOfLoss, 
	ClaimantNumber, 
	ClaimantFullName, 
	ClaimLossDate, 
	ClaimReportedDate, 
	ClaimCatastropheCode, 
	ClaimCatastropheStartDate, 
	ClaimCatastropheEndDate, 
	ClaimTransactionDate AS Payment_TransactionDate, 
	TransactionCode, 
	TransactionCodeDescription, 
	o_TransactionType, 
	TransactionAmount, 
	TransactionHistoryAmount, 
	SourceSequenceNumber, 
	WorkersCompensationMedicalLossPaid, 
	WorkersCompensationMedicalExpensePaid, 
	WorkersCompensationIndemnityExpensePaid, 
	WorkersCompensationIndemnityLossPaid, 
	PropertyCasualtyExpensePaid, 
	PropertyCasualtyLossPaid, 
	WorkersCompensationMedicalLossOutstanding, 
	WorkersCompensationMedicalExpenseOutstanding, 
	WorkersCompensationIndemnityExpenseOutstanding, 
	WorkersCompensationIndemnityLossOutstanding, 
	PropertyCasualtyExpenseOutstanding, 
	PropertyCasualtyLossOutstanding, 
	ClaimTransactionPKId, 
	ContainsOutstandingReserveAmountFlag2, 
	ReinsuranceUmbrellaLayer, 
	ClaimRelationshipId, 
	ClaimTransactionCategory3, 
	SourceSystemID3
	FROM union_payment_reserves
	ORDER BY ClaimNumber ASC, SubClaim ASC
),
exp_passthrough AS (
	SELECT
	AuditId,
	CurrentDateTime,
	ClaimNumber,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	CauseOfLoss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimCatastropheEndDate,
	Payment_TransactionDate,
	TransactionCode,
	TransactionCodeDescription,
	o_TransactionType,
	TransactionAmount,
	TransactionHistoryAmount,
	SourceSequenceNumber,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ClaimTransactionPKId,
	ContainsOutstandingReserveAmountFlag2,
	ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	ClaimTransactionCategory3,
	SourceSystemID3
	FROM sort_claim_subclaim
),
agg_SubClaim_Transactiondate AS (
	SELECT
	ClaimNumber,
	SubClaim,
	ClaimTransactionDate,
	-- *INF*: max(ClaimTransactionDate)
	max(ClaimTransactionDate) AS Reserve_TransactionDate
	FROM union_payment_reserves
	GROUP BY SubClaim
),
sort_claim_subclaim2 AS (
	SELECT
	ClaimNumber, 
	SubClaim, 
	Reserve_TransactionDate
	FROM agg_SubClaim_Transactiondate
	ORDER BY ClaimNumber ASC, SubClaim ASC
),
jnr_get_reserve_transaction_date AS (SELECT
	sort_claim_subclaim2.ClaimNumber, 
	sort_claim_subclaim2.SubClaim, 
	sort_claim_subclaim2.Reserve_TransactionDate, 
	exp_passthrough.AuditId, 
	exp_passthrough.CurrentDateTime, 
	exp_passthrough.ClaimNumber AS ClaimNumber1, 
	exp_passthrough.PolicyKey, 
	exp_passthrough.ProductCode, 
	exp_passthrough.AccountingProductCode, 
	exp_passthrough.StrategicProfitCenterAbbreviation, 
	exp_passthrough.ASLCode, 
	exp_passthrough.SubASLCode, 
	exp_passthrough.InsuranceReferenceLineOfBusinessCode, 
	exp_passthrough.RiskStateCode, 
	exp_passthrough.SubClaim AS SubClaim1, 
	exp_passthrough.FinancialTypeCode, 
	exp_passthrough.FinancialTypeCodeDescription, 
	exp_passthrough.CauseOfLoss, 
	exp_passthrough.ClaimantNumber, 
	exp_passthrough.ClaimantFullName, 
	exp_passthrough.ClaimLossDate, 
	exp_passthrough.ClaimReportedDate, 
	exp_passthrough.ClaimCatastropheCode, 
	exp_passthrough.ClaimCatastropheStartDate, 
	exp_passthrough.ClaimCatastropheEndDate, 
	exp_passthrough.Payment_TransactionDate, 
	exp_passthrough.TransactionCode, 
	exp_passthrough.TransactionCodeDescription, 
	exp_passthrough.o_TransactionType, 
	exp_passthrough.TransactionAmount, 
	exp_passthrough.TransactionHistoryAmount, 
	exp_passthrough.SourceSequenceNumber, 
	exp_passthrough.WorkersCompensationMedicalLossPaid, 
	exp_passthrough.WorkersCompensationMedicalExpensePaid, 
	exp_passthrough.WorkersCompensationIndemnityExpensePaid, 
	exp_passthrough.WorkersCompensationIndemnityLossPaid, 
	exp_passthrough.PropertyCasualtyExpensePaid, 
	exp_passthrough.PropertyCasualtyLossPaid, 
	exp_passthrough.WorkersCompensationMedicalLossOutstanding, 
	exp_passthrough.WorkersCompensationMedicalExpenseOutstanding, 
	exp_passthrough.WorkersCompensationIndemnityExpenseOutstanding, 
	exp_passthrough.WorkersCompensationIndemnityLossOutstanding, 
	exp_passthrough.PropertyCasualtyExpenseOutstanding, 
	exp_passthrough.PropertyCasualtyLossOutstanding, 
	exp_passthrough.ClaimTransactionPKId, 
	exp_passthrough.ContainsOutstandingReserveAmountFlag2, 
	exp_passthrough.ReinsuranceUmbrellaLayer, 
	exp_passthrough.ClaimRelationshipId, 
	exp_passthrough.ClaimTransactionCategory3, 
	exp_passthrough.SourceSystemID3
	FROM exp_passthrough
	INNER JOIN sort_claim_subclaim2
	ON sort_claim_subclaim2.ClaimNumber = exp_passthrough.ClaimNumber AND sort_claim_subclaim2.SubClaim = exp_passthrough.SubClaim
),
exp_transacion_date_calculation AS (
	SELECT
	AuditId,
	CurrentDateTime,
	ClaimNumber1,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim1,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	CauseOfLoss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimCatastropheEndDate,
	TransactionCode,
	TransactionCodeDescription,
	o_TransactionType,
	TransactionAmount,
	TransactionHistoryAmount,
	SourceSequenceNumber,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ClaimTransactionPKId,
	ContainsOutstandingReserveAmountFlag2,
	ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	ClaimTransactionCategory3,
	SourceSystemID3,
	Reserve_TransactionDate,
	Payment_TransactionDate,
	-- *INF*: IIF(o_TransactionType='Reserve',Reserve_TransactionDate,Payment_TransactionDate)
	IFF(o_TransactionType = 'Reserve', Reserve_TransactionDate, Payment_TransactionDate) AS v_Claim_TransactionDate,
	v_Claim_TransactionDate AS Claim_TransactionDate
	FROM jnr_get_reserve_transaction_date
),
sort_transactiondate_pkid AS (
	SELECT
	AuditId, 
	CurrentDateTime, 
	ClaimNumber1 AS ClaimNumber, 
	PolicyKey, 
	ProductCode, 
	AccountingProductCode, 
	StrategicProfitCenterAbbreviation, 
	ASLCode, 
	SubASLCode, 
	InsuranceReferenceLineOfBusinessCode, 
	RiskStateCode, 
	SubClaim1 AS SubClaim, 
	FinancialTypeCode, 
	FinancialTypeCodeDescription, 
	CauseOfLoss, 
	ClaimantNumber, 
	ClaimantFullName, 
	ClaimLossDate, 
	ClaimReportedDate, 
	ClaimCatastropheCode, 
	ClaimCatastropheStartDate, 
	ClaimCatastropheEndDate, 
	Claim_TransactionDate, 
	TransactionCode, 
	TransactionCodeDescription, 
	o_TransactionType, 
	TransactionAmount, 
	TransactionHistoryAmount, 
	SourceSequenceNumber, 
	WorkersCompensationMedicalLossPaid, 
	WorkersCompensationMedicalExpensePaid, 
	WorkersCompensationIndemnityExpensePaid, 
	WorkersCompensationIndemnityLossPaid, 
	PropertyCasualtyExpensePaid, 
	PropertyCasualtyLossPaid, 
	WorkersCompensationMedicalLossOutstanding, 
	WorkersCompensationMedicalExpenseOutstanding, 
	WorkersCompensationIndemnityExpenseOutstanding, 
	WorkersCompensationIndemnityLossOutstanding, 
	PropertyCasualtyExpenseOutstanding, 
	PropertyCasualtyLossOutstanding, 
	ClaimTransactionPKId, 
	ContainsOutstandingReserveAmountFlag2, 
	ReinsuranceUmbrellaLayer, 
	ClaimRelationshipId, 
	ClaimTransactionCategory3, 
	SourceSystemID3
	FROM exp_transacion_date_calculation
	ORDER BY Claim_TransactionDate ASC, ClaimTransactionPKId ASC
),
exp_calculate_SSN AS (
	SELECT
	AuditId,
	CurrentDateTime,
	ClaimNumber,
	PolicyKey,
	ProductCode,
	AccountingProductCode,
	StrategicProfitCenterAbbreviation,
	ASLCode,
	SubASLCode,
	InsuranceReferenceLineOfBusinessCode,
	RiskStateCode,
	SubClaim,
	FinancialTypeCode,
	FinancialTypeCodeDescription,
	CauseOfLoss,
	ClaimantNumber,
	ClaimantFullName,
	ClaimLossDate,
	ClaimReportedDate,
	ClaimCatastropheCode,
	ClaimCatastropheStartDate,
	ClaimCatastropheEndDate,
	Claim_TransactionDate AS Reserve_TransactionDate,
	TransactionCode,
	-- *INF*: IIF(ISNULL(:LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1)),
	-- 	0,
	-- 	:LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1))
	IFF(
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num IS NULL, 0,
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num
	) AS v_lkp_Source_Seq_Num,
	v_count+1 AS v_count,
	-- *INF*: iif(o_TransactionType='Reserve' OR  
	-- o_TransactionType='Payment',
	-- 	v_lkp_Source_Seq_Num + v_count,-1)
	IFF(
	    o_TransactionType = 'Reserve' OR o_TransactionType = 'Payment',
	    v_lkp_Source_Seq_Num + v_count,
	    - 1
	) AS o_SourceSequenceNumber,
	TransactionCodeDescription,
	o_TransactionType,
	TransactionAmount,
	TransactionHistoryAmount,
	SourceSequenceNumber,
	WorkersCompensationMedicalLossPaid,
	WorkersCompensationMedicalExpensePaid,
	WorkersCompensationIndemnityExpensePaid,
	WorkersCompensationIndemnityLossPaid,
	PropertyCasualtyExpensePaid,
	PropertyCasualtyLossPaid,
	WorkersCompensationMedicalLossOutstanding,
	WorkersCompensationMedicalExpenseOutstanding,
	WorkersCompensationIndemnityExpenseOutstanding,
	WorkersCompensationIndemnityLossOutstanding,
	PropertyCasualtyExpenseOutstanding,
	PropertyCasualtyLossOutstanding,
	ClaimTransactionPKId,
	ContainsOutstandingReserveAmountFlag2,
	ReinsuranceUmbrellaLayer,
	ClaimRelationshipId,
	ClaimTransactionCategory3,
	SourceSystemID3
	FROM sort_transactiondate_pkid
	LEFT JOIN LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1
	ON LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.ID = 1

),
SapiensReinsuranceClaim AS (
	INSERT INTO SapiensReinsuranceClaim
	(AuditId, CreatedDate, ClaimNumber, PolicyKey, ProductCode, AccountingProductCode, StrategicProfitCenterAbbreviation, ASLCode, SubASLCode, InsuranceReferenceLineOfBusinessCode, RiskStateCode, SubClaim, FinancialTypeCode, FinancialTypeCodeDescription, CauseOfLoss, ClaimantNumber, ClaimantFullName, ClaimLossDate, ClaimReportedDate, ClaimCatastropheCode, ClaimCatastropheStartDate, ClaimCatastropheEndDate, ClaimTransactionDate, TransactionCode, TransactionCodeDescription, TransactionType, TransactionAmount, TransactionHistoryAmount, SourceSequenceNumber, TransactionNumber, WorkersCompensationMedicalLossPaid, WorkersCompensationMedicalExpensePaid, WorkersCompensationIndemnityExpensePaid, WorkersCompensationIndemnityLossPaid, PropertyCasualtyExpensePaid, PropertyCasualtyLossPaid, WorkersCompensationMedicalLossOutstanding, WorkersCompensationMedicalExpenseOutstanding, WorkersCompensationIndemnityExpenseOutstanding, WorkersCompensationIndemnityLossOutstanding, PropertyCasualtyExpenseOutstanding, PropertyCasualtyLossOutstanding, ClaimTransactionPKId, ContainsOutstandingReserveAmountFlag, ReinsuranceUmbrellaLayer, ClaimRelationshipId, ClaimTransactionCategory, SourceSystemID)
	SELECT 
	AUDITID, 
	CurrentDateTime AS CREATEDDATE, 
	CLAIMNUMBER, 
	POLICYKEY, 
	PRODUCTCODE, 
	ACCOUNTINGPRODUCTCODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	ASLCODE, 
	SUBASLCODE, 
	INSURANCEREFERENCELINEOFBUSINESSCODE, 
	RISKSTATECODE, 
	SUBCLAIM, 
	FINANCIALTYPECODE, 
	FINANCIALTYPECODEDESCRIPTION, 
	CAUSEOFLOSS, 
	CLAIMANTNUMBER, 
	CLAIMANTFULLNAME, 
	CLAIMLOSSDATE, 
	CLAIMREPORTEDDATE, 
	CLAIMCATASTROPHECODE, 
	CLAIMCATASTROPHESTARTDATE, 
	CLAIMCATASTROPHEENDDATE, 
	Reserve_TransactionDate AS CLAIMTRANSACTIONDATE, 
	TRANSACTIONCODE, 
	TRANSACTIONCODEDESCRIPTION, 
	o_TransactionType AS TRANSACTIONTYPE, 
	TRANSACTIONAMOUNT, 
	TRANSACTIONHISTORYAMOUNT, 
	o_SourceSequenceNumber AS SOURCESEQUENCENUMBER, 
	o_SourceSequenceNumber AS TRANSACTIONNUMBER, 
	WORKERSCOMPENSATIONMEDICALLOSSPAID, 
	WORKERSCOMPENSATIONMEDICALEXPENSEPAID, 
	WORKERSCOMPENSATIONINDEMNITYEXPENSEPAID, 
	WORKERSCOMPENSATIONINDEMNITYLOSSPAID, 
	PROPERTYCASUALTYEXPENSEPAID, 
	PROPERTYCASUALTYLOSSPAID, 
	WORKERSCOMPENSATIONMEDICALLOSSOUTSTANDING, 
	WORKERSCOMPENSATIONMEDICALEXPENSEOUTSTANDING, 
	WORKERSCOMPENSATIONINDEMNITYEXPENSEOUTSTANDING, 
	WORKERSCOMPENSATIONINDEMNITYLOSSOUTSTANDING, 
	PROPERTYCASUALTYEXPENSEOUTSTANDING, 
	PROPERTYCASUALTYLOSSOUTSTANDING, 
	CLAIMTRANSACTIONPKID, 
	ContainsOutstandingReserveAmountFlag2 AS CONTAINSOUTSTANDINGRESERVEAMOUNTFLAG, 
	REINSURANCEUMBRELLALAYER, 
	CLAIMRELATIONSHIPID, 
	ClaimTransactionCategory3 AS CLAIMTRANSACTIONCATEGORY, 
	SourceSystemID3 AS SOURCESYSTEMID
	FROM exp_calculate_SSN
),