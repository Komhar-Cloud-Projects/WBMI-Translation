WITH
LKP_CLAIM_CASE_AK_ID AS (
	SELECT
	claim_case_ak_id,
	claim_case_key
	FROM (
		SELECT 
		claim_case.claim_case_ak_id as claim_case_ak_id, 
		claim_case.claim_case_key as claim_case_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case 
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_key ORDER BY claim_case_ak_id) = 1
),
SQ_CLM_DEMAND_OFFER_STAGE AS (
	SELECT 
	cdo.tch_claim_nbr, cdo.tch_client_id, cdo.create_ts, cdo.demand_offer_dt, cdo.demand_amt, cdo.offer_amt, cdo.damage_desc 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_demand_offer_stage cdo
),
EXP_VALIDATE AS (
	SELECT
	tch_claim_nbr2 AS tch_claim_nbr_cdo,
	tch_client_id2 AS tch_client_id_cdo,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claim_nbr_cdo))) OR IS_SPACES(LTRIM(RTRIM(tch_claim_nbr_cdo))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_cdo)))=0,'N/A',LTRIM(RTRIM(tch_claim_nbr_cdo)))
	--                                                                                            	
	IFF(LTRIM(RTRIM(tch_claim_nbr_cdo
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_cdo
			)
		))>0 AND TRIM(LTRIM(RTRIM(tch_claim_nbr_cdo
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_cdo
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(tch_claim_nbr_cdo
			)
		)
	) AS v_tch_claim_nbr,
	v_tch_claim_nbr AS tch_claim_nbr,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_client_id_cdo))) OR IS_SPACES(LTRIM(RTRIM(tch_client_id_cdo))) OR LENGTH(LTRIM(RTRIM(tch_client_id_cdo)))=0,'N/A',LTRIM(RTRIM(tch_client_id_cdo)))
	--                                                                                          
	IFF(LTRIM(RTRIM(tch_client_id_cdo
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(tch_client_id_cdo
			)
		))>0 AND TRIM(LTRIM(RTRIM(tch_client_id_cdo
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(tch_client_id_cdo
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(tch_client_id_cdo
			)
		)
	) AS v_tch_client_id,
	v_tch_client_id AS tch_client_id,
	-- *INF*: v_tch_claim_nbr || '//'||v_tch_client_id
	v_tch_claim_nbr || '//' || v_tch_client_id AS v_Claim_Case_Key,
	v_Claim_Case_Key AS Out_Claim_Case_key,
	-- *INF*: :LKP.LKP_CLAIM_CASE_AK_ID(v_Claim_Case_Key)
	LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_ak_id AS v_claim_case_ak_id,
	-- *INF*: IIF(ISNULL(v_claim_case_ak_id),-1,v_claim_case_ak_id)
	-- 
	-- ---v_claim_case_ak_id
	IFF(v_claim_case_ak_id IS NULL,
		- 1,
		v_claim_case_ak_id
	) AS claim_case_ak_id,
	claim_case_ak_id AS out_claim_case_ak_id,
	demand_offer_dt AS IN_demand_offer_dt,
	-- *INF*: IIF(v_prev_row_claim_case_ak_id = v_claim_case_ak_id,
	--       IIF(ISNULL(IN_demand_amt),v_prev_row_demand_date,IN_demand_offer_dt),
	-- IIF(ISNULL(IN_demand_amt),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IN_demand_offer_dt))
	-- 
	-- 
	IFF(v_prev_row_claim_case_ak_id = v_claim_case_ak_id,
		IFF(IN_demand_amt IS NULL,
			v_prev_row_demand_date,
			IN_demand_offer_dt
		),
		IFF(IN_demand_amt IS NULL,
			TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
			),
			IN_demand_offer_dt
		)
	) AS v_demand_dt,
	v_demand_dt AS out_demand_date,
	-- *INF*: IIF(ISNULL(IN_offer_amt),TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IN_demand_offer_dt)
	IFF(IN_offer_amt IS NULL,
		TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		IN_demand_offer_dt
	) AS v_demand_offer_dt,
	v_demand_offer_dt AS out_demand_offer_date,
	create_ts2,
	demand_amt AS IN_demand_amt,
	-- *INF*: IIF(ISNULL(IN_demand_amt) ,0 ,IN_demand_amt)
	IFF(IN_demand_amt IS NULL,
		0,
		IN_demand_amt
	) AS demand_amt,
	offer_amt AS IN_offer_amt,
	-- *INF*: IIF(ISNULL(IN_offer_amt) ,0 ,IN_offer_amt)
	IFF(IN_offer_amt IS NULL,
		0,
		IN_offer_amt
	) AS offer_amt,
	damage_desc1 AS IN_damage_desc_do,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_damage_desc_do))) OR IS_SPACES(LTRIM(RTRIM(IN_damage_desc_do))) OR LENGTH(LTRIM(RTRIM(IN_damage_desc_do)))=0,'N/A' ,LTRIM(RTRIM(IN_damage_desc_do)))
	IFF(LTRIM(RTRIM(IN_damage_desc_do
			)
		) IS NULL 
		OR LENGTH(LTRIM(RTRIM(IN_damage_desc_do
			)
		))>0 AND TRIM(LTRIM(RTRIM(IN_damage_desc_do
			)
		))='' 
		OR LENGTH(LTRIM(RTRIM(IN_damage_desc_do
				)
			)
		) = 0,
		'N/A',
		LTRIM(RTRIM(IN_damage_desc_do
			)
		)
	) AS damage_desc_do,
	claim_case_ak_id AS v_prev_row_claim_case_ak_id,
	v_demand_dt AS v_prev_row_demand_date,
	v_demand_offer_dt AS v_prev_row_offer_date
	FROM SQ_CLM_DEMAND_OFFER_STAGE
	LEFT JOIN LKP_CLAIM_CASE_AK_ID LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key
	ON LKP_CLAIM_CASE_AK_ID_v_Claim_Case_Key.claim_case_key = v_Claim_Case_Key

),
LKP_CLAIM_CASE_DEMAND_DETAIL AS (
	SELECT
	claim_case_demand_det_id,
	claim_case_demand_det_ak_id,
	demand_date,
	demand_offer_date,
	demand_create_date,
	demand_amt,
	offer_amt,
	demand_comment,
	claim_case_ak_id
	FROM (
		SELECT
		A.claim_case_demand_det_id as claim_case_demand_det_id, 
		A.claim_case_demand_det_ak_id as claim_case_demand_det_ak_id, 
		A.demand_comment as demand_comment, 
		A.claim_case_ak_id as claim_case_ak_id, 
		A.demand_date as demand_date, 
		A.demand_offer_date as demand_offer_date, 
		A.demand_create_date as demand_create_date, 
		A.demand_amt as demand_amt, 
		A.offer_amt as offer_amt 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_demand_detail A
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id,demand_date,demand_offer_date,demand_create_date,demand_amt,offer_amt ORDER BY claim_case_demand_det_id) = 1
),
EXP_DETECT_CHANGES AS (
	SELECT
	EXP_VALIDATE.out_claim_case_ak_id AS claim_case_ak_id,
	EXP_VALIDATE.Out_Claim_Case_key,
	EXP_VALIDATE.out_demand_date AS demand_dt,
	EXP_VALIDATE.out_demand_offer_date AS demand_offer_dt,
	EXP_VALIDATE.create_ts2 AS demand_create_dt,
	EXP_VALIDATE.demand_amt,
	EXP_VALIDATE.offer_amt,
	EXP_VALIDATE.damage_desc_do,
	LKP_CLAIM_CASE_DEMAND_DETAIL.claim_case_demand_det_id AS old_claim_case_demand_det_id,
	LKP_CLAIM_CASE_DEMAND_DETAIL.claim_case_demand_det_ak_id AS old_claim_case_demand_det_ak_id,
	LKP_CLAIM_CASE_DEMAND_DETAIL.demand_date AS old_demand_date,
	LKP_CLAIM_CASE_DEMAND_DETAIL.demand_offer_date AS old_demand_offer_date,
	LKP_CLAIM_CASE_DEMAND_DETAIL.demand_create_date AS old_demand_create_date,
	LKP_CLAIM_CASE_DEMAND_DETAIL.demand_amt AS old_demand_amt,
	LKP_CLAIM_CASE_DEMAND_DETAIL.offer_amt AS old_offer_amt,
	LKP_CLAIM_CASE_DEMAND_DETAIL.demand_comment AS old_demand_comment,
	-- *INF*: IIF(ISNULL(old_claim_case_demand_det_id),'NEW',
	--      IIF(
	-- 
	-- LTRIM(RTRIM(damage_desc_do)) <> LTRIM(RTRIM(old_demand_comment)),'UPDATE','NOCHANGE'))
	-- 
	IFF(old_claim_case_demand_det_id IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(damage_desc_do
				)
			) <> LTRIM(RTRIM(old_demand_comment
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: demand_create_dt
	-- 
	-- --IIF(v_changed_flag='NEW',TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),demand_dt)
	demand_create_dt AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS modified_date,
	SYSDATE AS created_date
	FROM EXP_VALIDATE
	LEFT JOIN LKP_CLAIM_CASE_DEMAND_DETAIL
	ON LKP_CLAIM_CASE_DEMAND_DETAIL.claim_case_ak_id = EXP_VALIDATE.out_claim_case_ak_id AND LKP_CLAIM_CASE_DEMAND_DETAIL.demand_date = EXP_VALIDATE.out_demand_date AND LKP_CLAIM_CASE_DEMAND_DETAIL.demand_offer_date = EXP_VALIDATE.out_demand_offer_date AND LKP_CLAIM_CASE_DEMAND_DETAIL.demand_create_date = EXP_VALIDATE.create_ts2 AND LKP_CLAIM_CASE_DEMAND_DETAIL.demand_amt = EXP_VALIDATE.demand_amt AND LKP_CLAIM_CASE_DEMAND_DETAIL.offer_amt = EXP_VALIDATE.offer_amt
),
FIL_INSERT AS (
	SELECT
	old_claim_case_demand_det_ak_id, 
	claim_case_ak_id, 
	Out_Claim_Case_key, 
	demand_dt, 
	demand_offer_dt, 
	demand_create_dt, 
	demand_amt, 
	offer_amt, 
	damage_desc_do, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_DETECT_CHANGES
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_Claim_Case_Demand_Det_ak_id AS (
	CREATE SEQUENCE SEQ_Claim_Case_Demand_Det_ak_id
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	old_claim_case_demand_det_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,old_claim_case_demand_det_ak_id)
	IFF(changed_flag = 'NEW',
		NEXTVAL,
		old_claim_case_demand_det_ak_id
	) AS claim_case_demand_det_ak_id,
	claim_case_ak_id,
	Out_Claim_Case_key,
	demand_dt,
	demand_offer_dt,
	demand_create_dt,
	demand_amt,
	offer_amt,
	damage_desc_do,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	SEQ_Claim_Case_Demand_Det_ak_id.NEXTVAL
	FROM FIL_INSERT
),
claim_case_demand_detail_insert AS (
	INSERT INTO claim_case_demand_detail
	(claim_case_demand_det_ak_id, claim_case_ak_id, claim_case_key, demand_date, demand_offer_date, demand_create_date, demand_amt, offer_amt, demand_comment, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	CLAIM_CASE_DEMAND_DET_AK_ID, 
	CLAIM_CASE_AK_ID, 
	Out_Claim_Case_key AS CLAIM_CASE_KEY, 
	demand_dt AS DEMAND_DATE, 
	demand_offer_dt AS DEMAND_OFFER_DATE, 
	demand_create_dt AS DEMAND_CREATE_DATE, 
	DEMAND_AMT, 
	OFFER_AMT, 
	damage_desc_do AS DEMAND_COMMENT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_Determine_AK
),
SQ_claim_case_demand_detail AS (
	SELECT 
	a.claim_case_demand_det_id, 
	a.claim_case_ak_id, 
	a.demand_date, 
	a.demand_offer_date, 
	a.demand_create_date, 
	a.demand_amt, 
	a.offer_amt, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_demand_detail a
	WHERE a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case_demand_detail b
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	                 AND a.claim_case_ak_id = b.claim_case_ak_id
	                 GROUP BY b.claim_case_ak_id 
	                HAVING COUNT(*) >1) 
	ORDER BY a.claim_case_ak_id,  a.claim_case_demand_det_ak_id DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_case_demand_det_id AS claim_case_id,
	claim_case_ak_id,
	demand_date,
	demand_offer_date,
	demand_create_date,
	demand_amt,
	offer_amt,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- claim_case_ak_id=v_prev_row_claim_case_ak_id 
	-- ,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		claim_case_ak_id = v_prev_row_claim_case_ak_id, DATEADD(SECOND,- 1,v_prev_row_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_case_ak_id AS v_prev_row_claim_case_ak_id,
	demand_date AS v_prev_row_demand_date,
	demand_offer_date AS v_prev_row_demand_offer_date,
	demand_create_date AS v_prev_row_demand_create_date,
	demand_amt AS v_prev_row_demand_amt,
	offer_amt AS v_prev_row_offer_amt,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_claim_case_demand_detail
),
FIL_FirstRow_In_AKGroup AS (
	SELECT
	claim_case_id AS claim_case_demand_det_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_Claim_Case_Demand_Detail AS (
	SELECT
	claim_case_demand_det_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRow_In_AKGroup
),
claim_case_demand_detail_update AS (
	MERGE INTO claim_case_demand_detail AS T
	USING UPD_Claim_Case_Demand_Detail AS S
	ON T.claim_case_demand_det_id = S.claim_case_demand_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),