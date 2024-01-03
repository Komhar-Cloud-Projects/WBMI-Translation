WITH
SQ_clmnt_impairment_stage AS (
	SELECT
		clmnt_impairment_stage_id,
		claim_nbr,
		client_id,
		seq_nbr,
		body_part_code,
		impair_percentage,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clmnt_impairment_stage
),
EXP_Workers_Comp_Claimant_Impairment AS (
	SELECT
	claim_nbr,
	client_id,
	seq_nbr,
	body_part_code,
	impair_percentage,
	source_system_id
	FROM SQ_clmnt_impairment_stage
),
EXP_LKP_Value_Workers_comp_claimant_detila AS (
	SELECT
	claim_nbr AS IN_claim_nbr,
	client_id AS IN_client_id,
	seq_nbr AS IN_seq_nbr,
	body_part_code AS IN_body_part_code,
	impair_percentage AS IN_impair_percentage,
	source_system_id AS IN_source_system_id,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_claim_nbr))),'N/A',IIF(IS_SPACES(IN_claim_nbr),'N/A',LTRIM(RTRIM(IN_claim_nbr))))
	IFF(LTRIM(RTRIM(IN_claim_nbr)) IS NULL, 'N/A', IFF(IS_SPACES(IN_claim_nbr), 'N/A', LTRIM(RTRIM(IN_claim_nbr)))) AS CLAIM_NBR,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_client_id))),'N/A',IIF(IS_SPACES(IN_client_id),'N/A',LTRIM(RTRIM(IN_client_id))))
	IFF(LTRIM(RTRIM(IN_client_id)) IS NULL, 'N/A', IFF(IS_SPACES(IN_client_id), 'N/A', LTRIM(RTRIM(IN_client_id)))) AS CLIENT_ID,
	-- *INF*: IIF(ISNULL(IN_seq_nbr),0,IN_seq_nbr)
	IFF(IN_seq_nbr IS NULL, 0, IN_seq_nbr) AS SEQ_NBR,
	-- *INF*: IIF(ISNULL(IN_impair_percentage),0,IN_impair_percentage)
	IFF(IN_impair_percentage IS NULL, 0, IN_impair_percentage) AS IMPAIRMENT_PERCENTAGE,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_source_system_id))),'N/A',IIF(IS_SPACES(IN_source_system_id),'N/A',LTRIM(RTRIM(IN_source_system_id))))
	IFF(LTRIM(RTRIM(IN_source_system_id)) IS NULL, 'N/A', IFF(IS_SPACES(IN_source_system_id), 'N/A', LTRIM(RTRIM(IN_source_system_id)))) AS SOURCE_SYSTEM_ID,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_body_part_code))),'N/A',IIF(IS_SPACES(IN_body_part_code),'N/A',LTRIM(RTRIM(IN_body_part_code))))
	IFF(LTRIM(RTRIM(IN_body_part_code)) IS NULL, 'N/A', IFF(IS_SPACES(IN_body_part_code), 'N/A', LTRIM(RTRIM(IN_body_part_code)))) AS BODY_PART_CODE
	FROM EXP_Workers_Comp_Claimant_Impairment
),
LKP_Claim_Party_Occurrence_AK_ID1 AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CLMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id DESC) = 1
),
LKP_Workers_Comp_Claimant_Detail1 AS (
	SELECT
	wc_claimant_det_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT 
		A.wc_claimant_det_ak_id as wc_claimant_det_ak_id,  
		A.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail A
		WHERE (source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}') AND (CRRNT_SNPSHT_FLAG='1')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY wc_claimant_det_ak_id DESC) = 1
),
EXP_Set_LKP_Val AS (
	SELECT
	EXP_LKP_Value_Workers_comp_claimant_detila.BODY_PART_CODE,
	EXP_LKP_Value_Workers_comp_claimant_detila.IMPAIRMENT_PERCENTAGE,
	EXP_LKP_Value_Workers_comp_claimant_detila.SEQ_NBR AS SEQ_NUM,
	EXP_LKP_Value_Workers_comp_claimant_detila.SOURCE_SYSTEM_ID,
	LKP_Workers_Comp_Claimant_Detail1.wc_claimant_det_ak_id AS LKP_wc_claimant_det_ak_id,
	LKP_wc_claimant_det_ak_id AS wc_claimant_det_ak_id
	FROM EXP_LKP_Value_Workers_comp_claimant_detila
	LEFT JOIN LKP_Workers_Comp_Claimant_Detail1
	ON LKP_Workers_Comp_Claimant_Detail1.claim_party_occurrence_ak_id = LKP_Claim_Party_Occurrence_AK_ID1.claim_party_occurrence_ak_id
),
LKP_IMPAIRMENT AS (
	SELECT
	wc_claimant_impairment_ak_id,
	wc_claimant_det_ak_id,
	lkp_body_part_code,
	lkp_impairment_percentage,
	IN_wc_claimant_det_ak_id,
	IN_seq_num,
	seq_num,
	wc_claimant_impairment_id
	FROM (
		SELECT CPO.wc_claimant_impairment_ak_id as wc_claimant_impairment_ak_id, 
		CPO.body_part_code as lkp_body_part_code ,
		CPO.impairment_percentage  as lkp_impairment_percentage,
		CPO.seq_num as seq_num , CPO.wc_claimant_det_ak_id as wc_claimant_det_ak_id,
		CPO.wc_claimant_impairment_id as wc_claimant_impairment_id
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_impairment CPO  
		WHERE    CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_claimant_det_ak_id,seq_num ORDER BY wc_claimant_impairment_ak_id DESC) = 1
),
EXP_DETECT_CHANGES_workers_comp_claimant_detail11 AS (
	SELECT
	EXP_Set_LKP_Val.BODY_PART_CODE,
	EXP_Set_LKP_Val.IMPAIRMENT_PERCENTAGE,
	EXP_Set_LKP_Val.SEQ_NUM,
	-- *INF*: IIF(ISNULL(wc_claimant_impairment_ak_id),'NEW',
	-- IIF(LKP_IMPAIRMENT_PERCENTAGE <> IMPAIRMENT_PERCENTAGE OR ltrim(rtrim(LKP_BODY_PART_CODE)) <> ltrim(rtrim(BODY_PART_CODE))  ,'UPDATE',
	-- 'UPDATE') )
	-- 
	-- --- All records will be either inserted if new or updated even if the data has not changed as shown above.  This is needed for the last mapping that runs in this WF to set the curr_snpsht_flag = 0 for records that have a different audit id than that of what is currently in this mapping but have the same key (detail_ak_id).  This is how we identify deleted records.
	-- 
	-- 
	-- ---'NOCHANGE') )
	IFF(wc_claimant_impairment_ak_id IS NULL, 'NEW', IFF(LKP_IMPAIRMENT_PERCENTAGE <> IMPAIRMENT_PERCENTAGE OR ltrim(rtrim(LKP_BODY_PART_CODE)) <> ltrim(rtrim(BODY_PART_CODE)), 'UPDATE', 'UPDATE')) AS V_CHANGE_FLAG,
	V_CHANGE_FLAG AS CHANGE_FLAG_OP,
	'1' AS CRRNT_SNPSHT_FLAG,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	-- *INF*: IIF(V_CHANGE_FLAG='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	TO_DATE(TO_CHAR(SYSDATE,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS'))
	IFF(V_CHANGE_FLAG = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), TO_DATE(TO_CHAR(SYSDATE, 'MM/DD/YYYY HH24:MI:SS'), 'MM/DD/YYYY HH24:MI:SS')) AS EFF_FROM_DATE,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS EFF_TO_DATE,
	EXP_Set_LKP_Val.SOURCE_SYSTEM_ID,
	SYSDATE AS CREATED_DATE,
	SYSDATE AS MODIFIED_DATE,
	LKP_IMPAIRMENT.wc_claimant_det_ak_id AS LKP_wc_claimant_det_ak_id,
	v_counter + 1 AS v_counter,
	LKP_IMPAIRMENT.lkp_body_part_code AS LKP_BODY_PART_CODE,
	LKP_IMPAIRMENT.lkp_impairment_percentage AS LKP_IMPAIRMENT_PERCENTAGE,
	LKP_IMPAIRMENT.wc_claimant_impairment_ak_id,
	EXP_Set_LKP_Val.wc_claimant_det_ak_id,
	LKP_IMPAIRMENT.wc_claimant_impairment_id
	FROM EXP_Set_LKP_Val
	LEFT JOIN LKP_IMPAIRMENT
	ON LKP_IMPAIRMENT.wc_claimant_det_ak_id = EXP_Set_LKP_Val.wc_claimant_det_ak_id AND LKP_IMPAIRMENT.seq_num = EXP_Set_LKP_Val.SEQ_NUM
),
FIL_INSERT_workers_comp_claimant_impairment AS (
	SELECT
	CHANGE_FLAG_OP, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	wc_claimant_det_ak_id AS LKP_wc_claimant_det_ak_id, 
	wc_claimant_impairment_ak_id, 
	BODY_PART_CODE, 
	IMPAIRMENT_PERCENTAGE, 
	SEQ_NUM
	FROM EXP_DETECT_CHANGES_workers_comp_claimant_detail11
	WHERE CHANGE_FLAG_OP<>'NOCHANGE'
),
SEQ_Workers_Comp_Claiment_Impairment AS (
	CREATE SEQUENCE SEQ_Workers_Comp_Claiment_Impairment
	START = 0
	INCREMENT = 1;
),
EXP_INSERT1 AS (
	SELECT
	SEQ_Workers_Comp_Claiment_Impairment.NEXTVAL,
	CHANGE_FLAG_OP,
	CRRNT_SNPSHT_FLAG,
	AUDIT_ID,
	EFF_FROM_DATE,
	EFF_TO_DATE,
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID,
	CREATED_DATE,
	MODIFIED_DATE,
	wc_claimant_impairment_ak_id AS IN_wc_claimant_impairment_ak_id,
	-- *INF*: IIF(CHANGE_FLAG_OP='NEW', NEXTVAL, IN_wc_claimant_impairment_ak_id)
	IFF(CHANGE_FLAG_OP = 'NEW', NEXTVAL, IN_wc_claimant_impairment_ak_id) AS WC_CLAIMANT_IMPAIRMENT_AK_ID,
	LKP_wc_claimant_det_ak_id,
	BODY_PART_CODE,
	IMPAIRMENT_PERCENTAGE,
	SEQ_NUM
	FROM FIL_INSERT_workers_comp_claimant_impairment
),
workers_comp_claimant_impairment AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_impairment
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, wc_claimant_impairment_ak_id, wc_claimant_det_ak_id, body_part_code, impairment_percentage, seq_num)
	SELECT 
	CRRNT_SNPSHT_FLAG AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE AS EFF_FROM_DATE, 
	EFF_TO_DATE AS EFF_TO_DATE, 
	SOURCE_SYS_ID AS SOURCE_SYS_ID, 
	CREATED_DATE AS CREATED_DATE, 
	MODIFIED_DATE AS MODIFIED_DATE, 
	WC_CLAIMANT_IMPAIRMENT_AK_ID AS WC_CLAIMANT_IMPAIRMENT_AK_ID, 
	LKP_wc_claimant_det_ak_id AS WC_CLAIMANT_DET_AK_ID, 
	BODY_PART_CODE AS BODY_PART_CODE, 
	IMPAIRMENT_PERCENTAGE AS IMPAIRMENT_PERCENTAGE, 
	SEQ_NUM AS SEQ_NUM
	FROM EXP_INSERT1
),
SQ_workers_comp_claimant_impairment AS (
	SELECT a.wc_claimant_impairment_id as wc_claimant_impairment_id  , 
	a.wc_claimant_impairment_ak_id  as wc_claimant_impairment_ak_id,
	a.eff_from_date  as eff_from_date , 
	a.eff_to_date as eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_impairment a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_impairment b
			WHERE b.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND b.crrnt_snpsht_flag = 1
			AND a.wc_claimant_impairment_ak_id = b.wc_claimant_impairment_ak_id
			GROUP BY wc_claimant_impairment_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY wc_claimant_impairment_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date11 AS (
	SELECT
	wc_claimant_impairment_id,
	wc_claimant_impairment_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: IIF(wc_claimant_impairment_ak_id = v_PREV_ROW_wc_claimant_det_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),orig_eff_to_date)
	IFF(wc_claimant_impairment_ak_id = v_PREV_ROW_wc_claimant_det_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1), orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	wc_claimant_impairment_ak_id AS v_PREV_ROW_wc_claimant_det_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_workers_comp_claimant_impairment
),
FIL_FirstRowInAKGroup1 AS (
	SELECT
	wc_claimant_impairment_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date11
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_workers_comp_claimant_impairment1 AS (
	SELECT
	wc_claimant_impairment_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup1
),
workers_comp_claimant_impairment2 AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_impairment AS T
	USING UPD_workers_comp_claimant_impairment1 AS S
	ON T.wc_claimant_impairment_id = S.wc_claimant_impairment_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
SQ_workers_comp_claimant_impairment2 AS (
	SELECT  
	w.wc_claimant_impairment_id 
	FROM
	  workers_comp_claimant_impairment w
	WHERE
	 w.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND w.crrnt_snpsht_flag = 1
	 AND w.audit_id  <>   '@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}' 
	and w.seq_num >
	 (select ISNULL(MAX(aa.seq_num),999)
	 FROM  workers_comp_claimant_impairment aa
	 WHERE  aa.wc_claimant_det_ak_id = w.wc_claimant_det_ak_id
	 AND  aa.source_sys_id = w.source_sys_id
	 AND  aa.crrnt_snpsht_flag = 1
	 AND aa.audit_id =  '@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}')
	
	--and  exists( select 'x' from 
	 --workers_comp_claimant_impairment ww
	 --Where ww.wc_claimant_det_ak_id = w.wc_claimant_det_ak_id
	 --AND ww.crrnt_snpsht_flag = 1
	 --AND ww.source_sys_id = w.source_sys_id
	 --AND ww.seq_num <> w.seq_num
	 --AND ww.audit_id =   '@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}' )
	----
),
EXP_Set_Defaults AS (
	SELECT
	wc_claimant_impairment_id,
	0 AS crrnt_snpsht_flg,
	sysdate AS modified_date
	FROM SQ_workers_comp_claimant_impairment2
),
UPD_Workers_Comp_Claimant_Impairment_Deleted_rec AS (
	SELECT
	wc_claimant_impairment_id, 
	crrnt_snpsht_flg, 
	modified_date
	FROM EXP_Set_Defaults
),
workers_comp_claimant_impairment6 AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_impairment AS T
	USING UPD_Workers_Comp_Claimant_Impairment_Deleted_rec AS S
	ON T.wc_claimant_impairment_id = S.wc_claimant_impairment_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flg, T.eff_from_date = S.modified_date, T.eff_to_date = S.modified_date
),