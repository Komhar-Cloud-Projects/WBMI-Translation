WITH
SQ_claim_total_loss AS (
	SELECT TL.claim_total_loss_id, 
	TL.claim_total_loss_ak_id, 
	TL.claimant_cov_det_ak_id, 
	TL.claim_total_loss_seq_num, 
	TL.vin_num, 
	TL.veh_yr, 
	TL.veh_make, 
	TL.veh_model, 
	TL.total_loss_date, 
	TL.loss_owner, 
	TL.new_owner, 
	TL.owner_retained_ind, 
	TL.pay_retained_amt, 
	TL.loss_acv, 
	TL.sale_tax, 
	TL.title_fee, 
	TL.registration_fee, 
	TL.salvage_ded, 
	TL.salvage_amt, 
	SH.claim_total_loss_submit_hist_id, 
	SH.claim_total_loss_submit_hist_ak_id, 
	SH.submit_to_vendor_date, 
	SH.submit_to_vendor_action,
	distinct_eff_From_Dates.eff_from_date 
	FROM
	(
	SELECT claim_total_loss_ak_id,eff_from_date FROM dbo.claim_total_loss
	WHERE created_Date>= '@{pipeline().parameters.SELECTION_START_TS}'
	UNION
	SELECT claim_total_loss_ak_id,eff_from_date FROM dbo.claim_total_loss_submit_history
	WHERE created_Date>= '@{pipeline().parameters.SELECTION_START_TS}'
	) AS distinct_eff_From_Dates
	
	left outer join claim_total_loss TL on
	distinct_eff_From_Dates.claim_total_loss_ak_id = TL.claim_total_loss_ak_id
	AND distinct_eff_From_Dates.eff_from_date between TL.eff_from_date AND TL.eff_to_date
	
	left outer join claim_total_loss_submit_history SH on
	distinct_eff_From_Dates.claim_total_loss_ak_id = SH.claim_total_loss_ak_id
	AND distinct_eff_From_Dates.eff_from_date between SH.eff_from_date AND SH.eff_to_date
),
LKP_claimant_coverage_dim AS (
	SELECT
	claimant_cov_dim_id,
	edw_claimant_cov_det_ak_id
	FROM (
		SELECT claimant_coverage_dim.claimant_cov_dim_id as claimant_cov_dim_id, claimant_coverage_dim.edw_claimant_cov_det_ak_id as edw_claimant_cov_det_ak_id FROM claimant_coverage_dim 
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claimant_cov_det_ak_id ORDER BY claimant_cov_dim_id) = 1
),
EXPTRANS AS (
	SELECT
	SQ_claim_total_loss.claim_total_loss_id,
	SQ_claim_total_loss.claim_total_loss_ak_id,
	SQ_claim_total_loss.claimant_cov_det_ak_id,
	SQ_claim_total_loss.claim_total_loss_seq_num,
	SQ_claim_total_loss.vin_num,
	SQ_claim_total_loss.veh_yr,
	SQ_claim_total_loss.veh_make,
	SQ_claim_total_loss.veh_model,
	SQ_claim_total_loss.total_loss_date,
	SQ_claim_total_loss.loss_owner,
	SQ_claim_total_loss.new_owner,
	SQ_claim_total_loss.owner_retained_ind,
	SQ_claim_total_loss.pay_retained_amt,
	SQ_claim_total_loss.loss_acv,
	SQ_claim_total_loss.sale_tax,
	SQ_claim_total_loss.title_fee,
	SQ_claim_total_loss.registration_fee,
	SQ_claim_total_loss.salvage_ded,
	SQ_claim_total_loss.salvage_amt,
	SQ_claim_total_loss.claim_total_loss_submit_hist_id,
	-- *INF*: IIF(ISNULL(claim_total_loss_submit_hist_id), -1, claim_total_loss_submit_hist_id)
	IFF(claim_total_loss_submit_hist_id IS NULL,
		- 1,
		claim_total_loss_submit_hist_id
	) AS claim_total_loss_submit_hist_id_out,
	SQ_claim_total_loss.claim_total_loss_submit_hist_ak_id,
	-- *INF*: IIF(ISNULL(claim_total_loss_submit_hist_ak_id), -1, claim_total_loss_submit_hist_ak_id)
	IFF(claim_total_loss_submit_hist_ak_id IS NULL,
		- 1,
		claim_total_loss_submit_hist_ak_id
	) AS claim_total_loss_submit_hist_ak_id_out,
	SQ_claim_total_loss.submit_to_vendor_date,
	-- *INF*: IIF(ISNULL(submit_to_vendor_date), TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), submit_to_vendor_date)
	IFF(submit_to_vendor_date IS NULL,
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		submit_to_vendor_date
	) AS submit_to_vendor_date_out,
	SQ_claim_total_loss.submit_to_vendor_action,
	-- *INF*: IIF(ISNULL(submit_to_vendor_action), 'N/A', submit_to_vendor_action)
	IFF(submit_to_vendor_action IS NULL,
		'N/A',
		submit_to_vendor_action
	) AS submit_to_vendor_action_out,
	LKP_claimant_coverage_dim.claimant_cov_dim_id,
	SQ_claim_total_loss.eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	sysdate AS created_modified_Date,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_claim_total_loss
	LEFT JOIN LKP_claimant_coverage_dim
	ON LKP_claimant_coverage_dim.edw_claimant_cov_det_ak_id = SQ_claim_total_loss.claimant_cov_det_ak_id
),
LKP_claim_total_loss_dim AS (
	SELECT
	claim_total_loss_dim_id,
	edw_claim_total_loss_pk_id,
	edw_claim_total_loss_submit_hist_pk_id
	FROM (
		SELECT 
			claim_total_loss_dim_id,
			edw_claim_total_loss_pk_id,
			edw_claim_total_loss_submit_hist_pk_id
		FROM claim_total_loss_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_total_loss_pk_id,edw_claim_total_loss_submit_hist_pk_id ORDER BY claim_total_loss_dim_id) = 1
),
RTRTRANS AS (
	SELECT
	LKP_claim_total_loss_dim.claim_total_loss_dim_id,
	EXPTRANS.claim_total_loss_id,
	EXPTRANS.claim_total_loss_ak_id,
	EXPTRANS.claimant_cov_det_ak_id,
	EXPTRANS.claim_total_loss_seq_num,
	EXPTRANS.vin_num,
	EXPTRANS.veh_yr,
	EXPTRANS.veh_make,
	EXPTRANS.veh_model,
	EXPTRANS.total_loss_date,
	EXPTRANS.loss_owner,
	EXPTRANS.new_owner,
	EXPTRANS.owner_retained_ind,
	EXPTRANS.pay_retained_amt,
	EXPTRANS.loss_acv,
	EXPTRANS.sale_tax,
	EXPTRANS.title_fee,
	EXPTRANS.registration_fee,
	EXPTRANS.salvage_ded,
	EXPTRANS.salvage_amt,
	EXPTRANS.claim_total_loss_submit_hist_id_out AS claim_total_loss_submit_hist_id,
	EXPTRANS.claim_total_loss_submit_hist_ak_id_out AS claim_total_loss_submit_hist_ak_id,
	EXPTRANS.submit_to_vendor_date_out AS submit_to_vendor_date,
	EXPTRANS.submit_to_vendor_action_out AS submit_to_vendor_action,
	EXPTRANS.claimant_cov_dim_id,
	EXPTRANS.eff_from_date,
	EXPTRANS.eff_to_date,
	EXPTRANS.created_modified_Date,
	EXPTRANS.crrnt_snpsht_flag,
	EXPTRANS.audit_id
	FROM EXPTRANS
	LEFT JOIN LKP_claim_total_loss_dim
	ON LKP_claim_total_loss_dim.edw_claim_total_loss_pk_id = EXPTRANS.claim_total_loss_id AND LKP_claim_total_loss_dim.edw_claim_total_loss_submit_hist_pk_id = EXPTRANS.claim_total_loss_submit_hist_id_out
),
RTRTRANS_Insert AS (SELECT * FROM RTRTRANS WHERE ISNULL(claim_total_loss_dim_id)),
RTRTRANS_DEFAULT1 AS (SELECT * FROM RTRTRANS WHERE NOT ( (ISNULL(claim_total_loss_dim_id)) )),
UPD_existing_records AS (
	SELECT
	claim_total_loss_dim_id, 
	claim_total_loss_id AS claim_total_loss_id2, 
	claim_total_loss_ak_id AS claim_total_loss_ak_id2, 
	claimant_cov_det_ak_id AS claimant_cov_det_ak_id2, 
	claim_total_loss_seq_num AS claim_total_loss_seq_num2, 
	vin_num AS vin_num2, 
	veh_yr AS veh_yr2, 
	veh_make AS veh_make2, 
	veh_model AS veh_model2, 
	total_loss_date AS total_loss_date2, 
	loss_owner AS loss_owner2, 
	new_owner AS new_owner2, 
	owner_retained_ind AS owner_retained_ind2, 
	pay_retained_amt AS pay_retained_amt2, 
	loss_acv AS loss_acv2, 
	sale_tax AS sale_tax2, 
	title_fee AS title_fee2, 
	registration_fee AS registration_fee2, 
	salvage_ded AS salvage_ded2, 
	salvage_amt AS salvage_amt2, 
	claim_total_loss_submit_hist_id AS claim_total_loss_submit_hist_id2, 
	claim_total_loss_submit_hist_ak_id AS claim_total_loss_submit_hist_ak_id2, 
	submit_to_vendor_date AS submit_to_vendor_date2, 
	submit_to_vendor_action AS submit_to_vendor_action2, 
	claimant_cov_dim_id AS claimant_cov_dim_id2, 
	eff_from_date AS eff_from_date2, 
	eff_to_date AS eff_to_date2, 
	created_modified_Date AS created_modified_Date2, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag2, 
	audit_id AS audit_id2
	FROM RTRTRANS_DEFAULT1
),
claim_total_loss_dim_update AS (
	MERGE INTO claim_total_loss_dim AS T
	USING UPD_existing_records AS S
	ON T.claim_total_loss_dim_id = S.claim_total_loss_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag2, T.audit_id = S.audit_id2, T.eff_from_date = S.eff_from_date2, T.eff_to_date = S.eff_to_date2, T.modified_date = S.created_modified_Date2, T.edw_claim_total_loss_pk_id = S.claim_total_loss_id2, T.edw_claim_total_loss_submit_hist_pk_id = S.claim_total_loss_submit_hist_id2, T.edw_claim_total_loss_ak_id = S.claim_total_loss_ak_id2, T.edw_claim_total_loss_submit_hist_ak_id = S.claim_total_loss_submit_hist_ak_id2, T.claimant_cov_dim_id = S.claimant_cov_dim_id2, T.claim_total_loss_seq_num = S.claim_total_loss_seq_num2, T.submit_to_vendor_date = S.submit_to_vendor_date2, T.submit_to_vendor_action = S.submit_to_vendor_action2, T.vin_num = S.vin_num2, T.veh_yr = S.veh_yr2, T.veh_make = S.veh_make2, T.veh_model = S.veh_model2, T.total_loss_date = S.total_loss_date2, T.loss_owner = S.loss_owner2, T.new_owner = S.new_owner2, T.owner_retained_ind = S.owner_retained_ind2, T.pay_retained_amt = S.pay_retained_amt2, T.loss_acv = S.loss_acv2, T.sale_tax = S.sale_tax2, T.title_fee = S.title_fee2, T.registration_fee = S.registration_fee2, T.salvage_ded = S.salvage_ded2, T.salvage_amt = S.salvage_amt2
),
claim_total_loss_dim AS (
	INSERT INTO claim_total_loss_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_claim_total_loss_pk_id, edw_claim_total_loss_submit_hist_pk_id, edw_claim_total_loss_ak_id, edw_claim_total_loss_submit_hist_ak_id, claimant_cov_dim_id, claim_total_loss_seq_num, submit_to_vendor_date, submit_to_vendor_action, vin_num, veh_yr, veh_make, veh_model, total_loss_date, loss_owner, new_owner, owner_retained_ind, pay_retained_amt, loss_acv, sale_tax, title_fee, registration_fee, salvage_ded, salvage_amt)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	created_modified_Date AS CREATED_DATE, 
	created_modified_Date AS MODIFIED_DATE, 
	claim_total_loss_id AS EDW_CLAIM_TOTAL_LOSS_PK_ID, 
	claim_total_loss_submit_hist_id AS EDW_CLAIM_TOTAL_LOSS_SUBMIT_HIST_PK_ID, 
	claim_total_loss_ak_id AS EDW_CLAIM_TOTAL_LOSS_AK_ID, 
	claim_total_loss_submit_hist_ak_id AS EDW_CLAIM_TOTAL_LOSS_SUBMIT_HIST_AK_ID, 
	CLAIMANT_COV_DIM_ID, 
	CLAIM_TOTAL_LOSS_SEQ_NUM, 
	SUBMIT_TO_VENDOR_DATE, 
	SUBMIT_TO_VENDOR_ACTION, 
	VIN_NUM, 
	VEH_YR, 
	VEH_MAKE, 
	VEH_MODEL, 
	TOTAL_LOSS_DATE, 
	LOSS_OWNER, 
	NEW_OWNER, 
	OWNER_RETAINED_IND, 
	PAY_RETAINED_AMT, 
	LOSS_ACV, 
	SALE_TAX, 
	TITLE_FEE, 
	REGISTRATION_FEE, 
	SALVAGE_DED, 
	SALVAGE_AMT
	FROM RTRTRANS_Insert
),
SQ_claim_total_loss_dim AS (
	SELECT a.claim_total_loss_dim_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.edw_claim_total_loss_ak_id, 
	a.edw_claim_total_loss_submit_hist_ak_id 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_total_loss_dim a
	WHERE EXISTS
	(SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_total_loss_dim b
	WHERE b.crrnt_snpsht_flag = 1 AND
	a.edw_claim_total_loss_ak_id = b.edw_claim_total_loss_ak_id
	AND a.edw_claim_total_loss_submit_hist_ak_id = b.edw_claim_total_loss_submit_hist_ak_id 
	GROUP BY b.edw_claim_total_loss_ak_id, b.edw_claim_total_loss_submit_hist_ak_id  HAVING count(*) > 1)
	ORDER BY a.edw_claim_total_loss_ak_id, a.edw_claim_total_loss_submit_hist_ak_id, a.eff_from_date DESC
),
EXP_Source AS (
	SELECT
	claim_total_loss_dim_id,
	edw_claim_total_loss_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	edw_claim_total_loss_submit_hist_ak_id,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_total_loss_ak_id=v_PREV_ROW_edw_claim_total_loss_ak_id , 	ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	--        ,orig_eff_to_date)
	DECODE(TRUE,
		edw_claim_total_loss_ak_id = v_PREV_ROW_edw_claim_total_loss_ak_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS o_eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_total_loss_ak_id AS v_PREV_ROW_edw_claim_total_loss_ak_id,
	sysdate AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_total_loss_dim
),
FLT_Source_Rows AS (
	SELECT
	claim_total_loss_dim_id, 
	orig_eff_to_date, 
	o_eff_to_date AS eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Source
	WHERE orig_eff_to_date <> eff_to_date
),
Upd_Update_Eff_Dates AS (
	SELECT
	claim_total_loss_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FLT_Source_Rows
),
claim_total_loss_dim_expire_rows AS (
	MERGE INTO claim_total_loss_dim AS T
	USING Upd_Update_Eff_Dates AS S
	ON T.claim_total_loss_dim_id = S.claim_total_loss_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),