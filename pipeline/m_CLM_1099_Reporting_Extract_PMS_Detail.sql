WITH
SQ_1099_Reporting_PMS AS (
	SELECT
	C.SEARCH_TAX_ID,
	A.DraftAmt AS PAID_AMT,
	A.TransDate AS TransDate,
	A.DraftNum as DraftNum,
	A.Sym as Sym,
	A.PolicyNum as PolicyNum,
	A.Module as Module,
	A.LossDate as LossDate
	
	FROM
	Pif4578RecStage A with (nolock) 
	
	INNER JOIN 
	pms_adjuster_master_stage B with (nolock) 
	ON A.AdjustorNo = B.adnm_adjustor_nbr 
	
	INNER JOIN 
	Master1099ListMonthlyStage C with (nolock) 
	ON B.adnm_taxid_ssn = C.TAX_ID
	
	WHERE
	A.TransDate >= (SELECT      DATEADD(YEAR, DATEDIFF(YEAR, 0,DATEADD(m,-1,getdate())),0)
	            'First Day of Year using previous month date') 
	AND
	A.TransDate <= (SELECT DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTHS},0))
	LastDay_PreviousMonth) AND
	C.REPORTABLE_IND = 'Y' AND
	A.PAIDRESERVEAMT >0 AND
	C.Is_Valid='Y' AND 
	B.adnm_taxid_ssn <> ''
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_cleanse_input AS (
	SELECT
	search_tax_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(search_tax_id)
	UDF_DEFAULT_VALUE_FOR_STRINGS(search_tax_id) AS search_tax_id_out,
	DraftAmt AS ctx_trs_amt,
	-- *INF*: IIF(ISNULL(ctx_trs_amt),0,ctx_trs_amt)
	IFF(ctx_trs_amt IS NULL, 0, ctx_trs_amt) AS ctx_trs_amt_out,
	DraftNum AS CheckNumber,
	TransDate AS PaymentIssueDate,
	Sym,
	PolicyNum,
	Module,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(Sym || PolicyNum || Module),'N/A',
	-- Sym || PolicyNum || Module)
	DECODE(
	    TRUE,
	    Sym || PolicyNum || Module IS NULL, 'N/A',
	    Sym || PolicyNum || Module
	) AS v_PolicyKey,
	v_PolicyKey AS PolicyKey,
	LossDate,
	-- *INF*: TO_CHAR(GET_DATE_PART(LossDate,'YYYY')) ||
	-- LPAD(TO_CHAR(GET_DATE_PART(LossDate,'MM')),2,'0') || LPAD(TO_CHAR(GET_DATE_PART(LossDate, 'DD')),2,'0')
	TO_CHAR(DATE_PART(LossDate, 'YYYY')) || LPAD(TO_CHAR(DATE_PART(LossDate, 'MM')), 2, '0') || LPAD(TO_CHAR(DATE_PART(LossDate, 'DD')), 2, '0') AS v_LossDate,
	v_PolicyKey || v_LossDate AS ClaimNumber,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM SQ_1099_Reporting_PMS
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
LKP_claim_occ_key_Loss_desc AS (
	SELECT
	claim_loss_descript,
	pol_key,
	claim_loss_date
	FROM (
		SELECT distinct
		claim_occurrence.claim_occurrence_key as claim_occurrence_key, 
		claim_occurrence.claim_loss_descript as claim_loss_descript, claim_occurrence.pol_key as pol_key, claim_occurrence.claim_loss_date as claim_loss_date FROM @{pipeline().parameters.SOURCE_DATABASE_NAME2}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,claim_loss_date ORDER BY claim_loss_descript DESC) = 1
),
EXP_output AS (
	SELECT
	1 AS default_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS default_date,
	'PMS' AS source_sys_id,
	LKP_Work1099Reporting.Work1099ReportingId,
	EXP_cleanse_input.search_tax_id_out AS search_tax_id,
	EXP_cleanse_input.ctx_trs_amt_out AS ctx_trs_amt,
	EXP_cleanse_input.PolicyKey,
	EXP_cleanse_input.LossDate,
	EXP_cleanse_input.ClaimNumber,
	EXP_cleanse_input.CheckNumber,
	EXP_cleanse_input.PaymentIssueDate,
	LKP_claim_occ_key_Loss_desc.claim_loss_descript AS LossDescription,
	-- *INF*: IIF(ISNULL(LossDescription),'N/A',
	-- LossDescription)
	IFF(LossDescription IS NULL, 'N/A', LossDescription) AS o_LossDescription
	FROM EXP_cleanse_input
	LEFT JOIN LKP_Work1099Reporting
	ON LKP_Work1099Reporting.SearchTaxId = EXP_cleanse_input.search_tax_id_out AND LKP_Work1099Reporting.SourceSystemID = EXP_cleanse_input.source_sys_id
	LEFT JOIN LKP_claim_occ_key_Loss_desc
	ON LKP_claim_occ_key_Loss_desc.pol_key = EXP_cleanse_input.PolicyKey AND LKP_claim_occ_key_Loss_desc.claim_loss_date = EXP_cleanse_input.LossDate
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
	o_LossDescription AS LOSSDESCRIPTION
	FROM EXP_output
),