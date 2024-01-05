WITH
SQ_SupPaymentWorkflowStage AS (
	SELECT
		SupPaymentWorkflowId,
		PaymentWorkflow
	FROM SupPaymentWorkflowStage
),
EXP_Source AS (
	SELECT
	SupPaymentWorkflowId,
	PaymentWorkflow
	FROM SQ_SupPaymentWorkflowStage
),
LKP_sup_claim_payment_workflow AS (
	SELECT
	sup_claim_payment_workflow_id,
	payment_workflow,
	source_payment_workflow_id
	FROM (
		SELECT sup_claim_payment_workflow_id as sup_claim_payment_workflow_id,
			source_payment_workflow_id as source_payment_workflow_id,
			payment_workflow as payment_workflow
		FROM dbo.sup_claim_payment_workflow 
		WHERE crrnt_snpsht_flag=1 
		ORDER BY source_payment_workflow_id, eff_from_date desc
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY source_payment_workflow_id ORDER BY sup_claim_payment_workflow_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Source.SupPaymentWorkflowId,
	EXP_Source.PaymentWorkflow,
	LKP_sup_claim_payment_workflow.sup_claim_payment_workflow_id AS lkp_sup_claim_payment_workflow_id,
	LKP_sup_claim_payment_workflow.payment_workflow AS lkp_payment_workflow,
	-- *INF*: IIF(ISNULL(lkp_sup_claim_payment_workflow_id),
	-- 	'NEW',
	-- 	IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(PaymentWorkflow) != lkp_payment_workflow,
	-- 		'UPDATE',
	-- 	'NOCHANGE'))
	IFF(lkp_sup_claim_payment_workflow_id IS NULL, 'NEW', IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(PaymentWorkflow) != lkp_payment_workflow, 'UPDATE', 'NOCHANGE')) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	-- *INF*: IIF(v_ChangeFlag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	--     SYSDATE)
	IFF(v_ChangeFlag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SYSDATE AS CurrentDate
	FROM EXP_Source
	LEFT JOIN LKP_sup_claim_payment_workflow
	ON LKP_sup_claim_payment_workflow.source_payment_workflow_id = EXP_Source.SupPaymentWorkflowId
),
FIL_NewOrChanged AS (
	SELECT
	SupPaymentWorkflowId, 
	PaymentWorkflow, 
	o_ChangeFlag AS ChangeFlag, 
	eff_from_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	audit_id, 
	CurrentDate
	FROM EXP_Detect_Changes
	WHERE ChangeFlag = 'NEW' or ChangeFlag = 'UPDATE'
),
sup_claim_payment_workflow_Insert AS (
	INSERT INTO sup_claim_payment_workflow
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, source_payment_workflow_id, payment_workflow)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CurrentDate AS CREATED_DATE, 
	CurrentDate AS MODIFIED_DATE, 
	SupPaymentWorkflowId AS SOURCE_PAYMENT_WORKFLOW_ID, 
	PaymentWorkflow AS PAYMENT_WORKFLOW
	FROM FIL_NewOrChanged
),
SQ_sup_claim_payment_workflow_Type2 AS (
	SELECT a.sup_claim_payment_workflow_id,
		a.source_payment_workflow_id,
		a.eff_from_date,
		a.eff_to_date
	FROM dbo.sup_claim_payment_workflow a
	WHERE EXISTS (
			SELECT 1
			FROM dbo.sup_claim_payment_workflow b
			WHERE b.crrnt_snpsht_flag = 1
				AND a.source_payment_workflow_id = b.source_payment_workflow_id
			GROUP BY b.source_payment_workflow_id
			HAVING COUNT(1) > 1
			) 
	ORDER BY a.source_payment_workflow_id,
		a.eff_from_date DESC
),
EXP_eff_to_date AS (
	SELECT
	sup_claim_payment_workflow_id,
	source_payment_workflow_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	source_payment_workflow_id = v_prev_row_source_payment_workflow_id, 
	-- 		ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1), 
	-- 	orig_eff_to_date)
	DECODE(TRUE,
	source_payment_workflow_id = v_prev_row_source_payment_workflow_id, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	source_payment_workflow_id AS v_prev_row_source_payment_workflow_id,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS CurrentDate
	FROM SQ_sup_claim_payment_workflow_Type2
),
FIL_First_Row_in_AK_Group AS (
	SELECT
	sup_claim_payment_workflow_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	CurrentDate
	FROM EXP_eff_to_date
	WHERE orig_eff_to_date != eff_to_date
),
EXP_RowsToExpire AS (
	SELECT
	sup_claim_payment_workflow_id,
	eff_to_date,
	crrnt_snpsht_flag,
	CurrentDate
	FROM FIL_First_Row_in_AK_Group
),
UPD_eff_to_date AS (
	SELECT
	sup_claim_payment_workflow_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	CurrentDate
	FROM EXP_RowsToExpire
),
sup_claim_payment_workflow_Update AS (
	MERGE INTO sup_claim_payment_workflow AS T
	USING UPD_eff_to_date AS S
	ON T.sup_claim_payment_workflow_id = S.sup_claim_payment_workflow_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.CurrentDate
),