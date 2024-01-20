WITH
SQ_1099_Reporting_EXD AS (
	SELECT clmdtl.search_tax_id,
		sum(clmdtl.ctx_trs_amt) AS Paid_Amt,
		clmdtl.ctx_claim_nbr,
		clmdtl.ctx_draft_nbr,
		clmdtl.ctx_trs_dt
	FROM (
		SELECT CML.search_tax_id,
			CT.ctx_trs_amt AS ctx_trs_amt,
			CT.ctx_claim_nbr,
			CT.ctx_draft_nbr,
			CT.ctx_trs_dt
		FROM claim_transaction_full_extract_stage CT WITH (NOLOCK)
		INNER JOIN ClaimDraftMonthlyStage CDM WITH (NOLOCK) ON CT.ctx_draft_nbr = CDM.dft_draft_nbr
			AND CT.ctx_claim_nbr = CDM.dft_claim_nbr
		INNER JOIN Master1099ListMonthlyStage CML WITH (NOLOCK) ON CDM.dft_tax_id_nbr = CML.search_tax_id
			AND CDM.dft_tax_id_type_cd = CML.tax_id_type
		WHERE
			-- CT.ctx_trs_dt between '2013-01-01' and '2013-05-31' -- make year begin, year end (current or prior?)
	         --  use (-1) for @{pipeline().parameters.NO_OF_MONTHS} to include current month, otherwise 0 will provide previous month
			(CT.ctx_trs_dt >= (SELECT DATEADD(YEAR, DATEDIFF(YEAR, 0, DATEADD(m, -1, getdate())), 0) 'First Day of Year using previous month date')
				AND CT.ctx_trs_dt <= (SELECT DATEADD(s, -1, DATEADD(mm, DATEDIFF(m, 0, GETDATE()) -@{pipeline().parameters.NO_OF_MONTHS}, 0)) LastDay_PreviousMonth))
	 		AND CT.source_system_id = 'EXCEED'
			AND CML.reportable_ind = 'Y'
			AND CML.is_valid = 'Y'
			AND CDM.dft_dbs_status_cd IN ('P', 'D', 'U')
			AND NOT EXISTS (SELECT 1
				FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.dbo.claim_payment cpa
				JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME2}.dbo.sup_payment_method pm on pm.sup_payment_method_id = cpa.sup_payment_method_id
	                   and pm.payment_method IN ('Virtual Payment','Debit Card','Digital Prepaid','CAT Card','PayPal','Venmo','Electronic to Lienholder')
				WHERE cpa.claim_pay_num = CT.ctx_draft_nbr)	
		) clmdtl
	 @{pipeline().parameters.WHERE_CLAUSE}
	GROUP BY clmdtl.search_tax_id,
		clmdtl.ctx_claim_nbr,
		clmdtl.ctx_draft_nbr,
		clmdtl.ctx_trs_dt
),
EXP_cleanse_input AS (
	SELECT
	search_tax_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(search_tax_id)
	UDF_DEFAULT_VALUE_FOR_STRINGS(search_tax_id) AS search_tax_id_out,
	ctx_trs_amt,
	-- *INF*: IIF(ISNULL(ctx_trs_amt),0,ctx_trs_amt)
	IFF(ctx_trs_amt IS NULL, 0, ctx_trs_amt) AS ctx_trs_amt_out,
	ctx_claim_nbr AS ClaimNumber_out,
	ctx_draft_nbr AS CheckNumber_out,
	ctx_trs_dt AS PaymentIssueDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM SQ_1099_Reporting_EXD
),
LKP_Work1099Reporting AS (
	SELECT
	Work1099ReportingId,
	SearchTaxId,
	SourceSystemID
	FROM (
		SELECT Work1099ReportingId as Work1099ReportingId, SearchTaxId as SearchTaxId, Work1099Reporting.SourceSystemID as SourceSystemID FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.Work1099Reporting
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SearchTaxId,SourceSystemID ORDER BY Work1099ReportingId DESC) = 1
),
LKP_check_num AS (
	SELECT
	micro_ecd_draft_num,
	pay_issued_date,
	claim_pay_num
	FROM (
		SELECT 
			micro_ecd_draft_num,
			pay_issued_date,
			claim_pay_num
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_payment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_num ORDER BY micro_ecd_draft_num DESC) = 1
),
LKP_claim_num_loss_desc AS (
	SELECT
	claim_loss_date,
	s3p_claim_num,
	claim_loss_descript,
	claim_occurrence_key
	FROM (
		SELECT 
			claim_loss_date,
			s3p_claim_num,
			claim_loss_descript,
			claim_occurrence_key
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_loss_date DESC) = 1
),
EXP_output AS (
	SELECT
	1 AS default_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS default_date,
	LKP_Work1099Reporting.Work1099ReportingId,
	EXP_cleanse_input.ctx_trs_amt_out AS ctx_trs_amt,
	LKP_claim_num_loss_desc.s3p_claim_num AS ClaimNumber,
	LKP_check_num.micro_ecd_draft_num AS CheckNumber,
	EXP_cleanse_input.PaymentIssueDate,
	LKP_claim_num_loss_desc.claim_loss_date AS LossDate,
	LKP_claim_num_loss_desc.claim_loss_descript AS LossDescription
	FROM EXP_cleanse_input
	LEFT JOIN LKP_Work1099Reporting
	ON LKP_Work1099Reporting.SearchTaxId = EXP_cleanse_input.search_tax_id_out AND LKP_Work1099Reporting.SourceSystemID = EXP_cleanse_input.SourceSystemId
	LEFT JOIN LKP_check_num
	ON LKP_check_num.claim_pay_num = EXP_cleanse_input.CheckNumber_out
	LEFT JOIN LKP_claim_num_loss_desc
	ON LKP_claim_num_loss_desc.claim_occurrence_key = EXP_cleanse_input.ClaimNumber_out
),
Work1099ReportingDetail AS (
	INSERT INTO Work1099ReportingDetail
	(Work1099ReportingId, PaidAmount, ClaimNumber, CheckNumber, PaymentIssueDate, LossDate, LossDescription)
	SELECT 
	WORK1099REPORTINGID, 
	ctx_trs_amt AS PAIDAMOUNT, 
	CLAIMNUMBER, 
	CHECKNUMBER, 
	PAYMENTISSUEDATE, 
	LOSSDATE, 
	LOSSDESCRIPTION
	FROM EXP_output
),