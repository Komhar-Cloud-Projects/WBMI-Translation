WITH
LKP_CLM_COMMENTS_STAGE AS (
	SELECT
	tcc_comment_txt,
	folder_key,
	comment_item_nbr
	FROM (
		SELECT A.tcc_comment_txt as tcc_comment_txt, 
		A.folder_key as folder_key, A.comment_item_nbr as comment_item_nbr 
		FROM clm_comments_stage A INNER JOIN clm_case_manage_stage B ON 
		B.tch_claim_nbr= A.folder_key
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY folder_key,comment_item_nbr ORDER BY tcc_comment_txt DESC) = 1
),
SQ_CLAIM_CASE_MANAGE_STAGE AS (
	SELECT 
	ccm.tch_claim_nbr, ccm.tch_client_id, ccm.case_name, ccm.case_number, 
	ccm.suit_venue, ccm.suit_state, ccm.trial_date, ccm.policy_limit_id, ccm.case_desc_id, ccm.inj_dam_desc_id, ccm.pripst_inj_desc_id, ccm.subro_cont_id, ccm.first_not_law_suit, ccm.declaratory_act, ccm.suit_status, ccm.denial_date, ccm.prim_lit_handler, ccm.litigation_date, ccm.litigation_closed, ccm.liab_cmt_id, ccm.pros_cmt_id, ccm.cons_cmt_id, ccm.com_umb_res, ccm.how_clm_closed, ccm.payment_amt, ccm.reins_reported, ccm.demand_at_init_lit ,ccm.settlement_type_cd 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_case_manage_stage ccm
),
EXP_VALIDATE AS (
	SELECT
	tch_claim_nbr AS tch_claim_nbr_ccm,
	tch_client_id AS tch_client_id_ccm,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_claim_nbr_ccm))) OR IS_SPACES(LTRIM(RTRIM(tch_claim_nbr_ccm))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_ccm)))=0,'N/A',LTRIM(RTRIM(tch_claim_nbr_ccm)))
	IFF(LTRIM(RTRIM(tch_claim_nbr_ccm)) IS NULL OR IS_SPACES(LTRIM(RTRIM(tch_claim_nbr_ccm))) OR LENGTH(LTRIM(RTRIM(tch_claim_nbr_ccm))) = 0, 'N/A', LTRIM(RTRIM(tch_claim_nbr_ccm))) AS v_tch_claim_nbr,
	v_tch_claim_nbr AS tch_claim_nbr,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(tch_client_id_ccm))) OR IS_SPACES(LTRIM(RTRIM(tch_client_id_ccm))) OR LENGTH(LTRIM(RTRIM(tch_client_id_ccm)))=0,'N/A',LTRIM(RTRIM(tch_client_id_ccm)))
	IFF(LTRIM(RTRIM(tch_client_id_ccm)) IS NULL OR IS_SPACES(LTRIM(RTRIM(tch_client_id_ccm))) OR LENGTH(LTRIM(RTRIM(tch_client_id_ccm))) = 0, 'N/A', LTRIM(RTRIM(tch_client_id_ccm))) AS v_tch_client_id,
	v_tch_client_id AS tch_client_id,
	-- *INF*: v_tch_claim_nbr || '//'||v_tch_client_id
	v_tch_claim_nbr || '//' || v_tch_client_id AS CLAIM_CASE_KEY,
	case_name AS IN_case_name,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_case_name))) OR IS_SPACES(LTRIM(RTRIM(IN_case_name))) OR LENGTH(LTRIM(RTRIM(IN_case_name)))=0,'N/A' ,LTRIM(RTRIM(IN_case_name)))
	IFF(LTRIM(RTRIM(IN_case_name)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_case_name))) OR LENGTH(LTRIM(RTRIM(IN_case_name))) = 0, 'N/A', LTRIM(RTRIM(IN_case_name))) AS case_name,
	case_number AS IN_case_number,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_case_number))) OR IS_SPACES(LTRIM(RTRIM(IN_case_number))) OR LENGTH(LTRIM(RTRIM(IN_case_number)))=0,'N/A' ,LTRIM(RTRIM(IN_case_number)))
	IFF(LTRIM(RTRIM(IN_case_number)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_case_number))) OR LENGTH(LTRIM(RTRIM(IN_case_number))) = 0, 'N/A', LTRIM(RTRIM(IN_case_number))) AS case_number,
	suit_venue AS IN_suit_venue,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_suit_venue))) OR IS_SPACES(LTRIM(RTRIM(IN_suit_venue))) OR LENGTH(LTRIM(RTRIM(IN_suit_venue)))=0,'N/A' ,LTRIM(RTRIM(IN_suit_venue)))
	IFF(LTRIM(RTRIM(IN_suit_venue)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_suit_venue))) OR LENGTH(LTRIM(RTRIM(IN_suit_venue))) = 0, 'N/A', LTRIM(RTRIM(IN_suit_venue))) AS suit_venue,
	suit_state AS IN_suit_state,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_suit_state))) OR IS_SPACES(LTRIM(RTRIM(IN_suit_state))) OR LENGTH(LTRIM(RTRIM(IN_suit_state)))=0,'N/A' ,LTRIM(RTRIM(IN_suit_state)))
	IFF(LTRIM(RTRIM(IN_suit_state)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_suit_state))) OR LENGTH(LTRIM(RTRIM(IN_suit_state))) = 0, 'N/A', LTRIM(RTRIM(IN_suit_state))) AS suit_state,
	trial_date AS IN_trial_date,
	-- *INF*: IIF(ISNULL(IN_trial_date) ,TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS'),IN_trial_date)
	IFF(IN_trial_date IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IN_trial_date) AS trial_date,
	policy_limit_id AS IN_policy_limit_id,
	case_desc_id AS IN_case_desc_id,
	inj_dam_desc_id AS IN_inj_dam_desc_id,
	subro_cont_id AS IN_subro_cont_id,
	first_not_law_suit AS IN_first_not_law_suit,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_first_not_law_suit))) OR IS_SPACES(LTRIM(RTRIM(IN_first_not_law_suit))) OR LENGTH(LTRIM(RTRIM(IN_first_not_law_suit)))=0,'N/A' ,LTRIM(RTRIM(IN_first_not_law_suit)))
	IFF(LTRIM(RTRIM(IN_first_not_law_suit)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_first_not_law_suit))) OR LENGTH(LTRIM(RTRIM(IN_first_not_law_suit))) = 0, 'N/A', LTRIM(RTRIM(IN_first_not_law_suit))) AS first_not_law_suit,
	declaratory_act AS IN_declaratory_act,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_declaratory_act))) OR IS_SPACES(LTRIM(RTRIM(IN_declaratory_act))) OR LENGTH(LTRIM(RTRIM(IN_declaratory_act)))=0,'N/A' ,LTRIM(RTRIM(IN_declaratory_act)))
	IFF(LTRIM(RTRIM(IN_declaratory_act)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_declaratory_act))) OR LENGTH(LTRIM(RTRIM(IN_declaratory_act))) = 0, 'N/A', LTRIM(RTRIM(IN_declaratory_act))) AS declaratory_act,
	suit_status AS IN_suit_status,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_suit_status))) OR IS_SPACES(LTRIM(RTRIM(IN_suit_status))) OR LENGTH(LTRIM(RTRIM(IN_suit_status)))=0,'N/A' ,LTRIM(RTRIM(IN_suit_status)))
	IFF(LTRIM(RTRIM(IN_suit_status)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_suit_status))) OR LENGTH(LTRIM(RTRIM(IN_suit_status))) = 0, 'N/A', LTRIM(RTRIM(IN_suit_status))) AS suit_status,
	denial_date AS IN_denial_date,
	-- *INF*: IIF(ISNULL(IN_denial_date) ,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ,IN_denial_date)
	IFF(IN_denial_date IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IN_denial_date) AS denial_date,
	litigation_date AS IN_litigation_date,
	-- *INF*: IIF(ISNULL(IN_litigation_date),TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ,IN_litigation_date)
	IFF(IN_litigation_date IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IN_litigation_date) AS litigation_date,
	prim_lit_handler AS IN_prim_lit_handler,
	litigation_closed AS IN_litigation_closed,
	-- *INF*: IIF(ISNULL(IN_litigation_closed) ,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS') ,IN_litigation_closed)
	IFF(IN_litigation_closed IS NULL, TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), IN_litigation_closed) AS litigation_closed,
	how_clm_closed AS IN_how_clm_closed,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_how_clm_closed))) OR IS_SPACES(LTRIM(RTRIM(IN_how_clm_closed))) OR LENGTH(LTRIM(RTRIM(IN_how_clm_closed)))=0,'N/A' ,LTRIM(RTRIM(IN_how_clm_closed)))
	IFF(LTRIM(RTRIM(IN_how_clm_closed)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_how_clm_closed))) OR LENGTH(LTRIM(RTRIM(IN_how_clm_closed))) = 0, 'N/A', LTRIM(RTRIM(IN_how_clm_closed))) AS how_clm_closed,
	reins_reported AS IN_reins_reported,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(IN_reins_reported))) OR IS_SPACES(LTRIM(RTRIM(IN_reins_reported))) OR LENGTH(LTRIM(RTRIM(IN_reins_reported)))=0,'N/A' ,LTRIM(RTRIM(IN_reins_reported)))
	IFF(LTRIM(RTRIM(IN_reins_reported)) IS NULL OR IS_SPACES(LTRIM(RTRIM(IN_reins_reported))) OR LENGTH(LTRIM(RTRIM(IN_reins_reported))) = 0, 'N/A', LTRIM(RTRIM(IN_reins_reported))) AS reins_reported,
	pros_cmt_id AS IN_pros_cmt_id,
	cons_cmt_id AS IN_cons_cmt_id,
	pripst_inj_desc_id AS IN_pripst_inj_desc_id,
	liab_cmt_id AS IN_liab_cmt_id,
	com_umb_res AS IN_com_umb_res,
	-- *INF*: IIF(ISNULL(IN_com_umb_res) ,0 ,IN_com_umb_res)
	IFF(IN_com_umb_res IS NULL, 0, IN_com_umb_res) AS com_umb_res,
	payment_amt AS IN_payment_amt,
	-- *INF*: IIF(ISNULL(IN_payment_amt) ,0 ,IN_payment_amt)
	IFF(IN_payment_amt IS NULL, 0, IN_payment_amt) AS payment_amt,
	demand_at_init_lit,
	-- *INF*: IIF(ISNULL(demand_at_init_lit) ,0 ,demand_at_init_lit)
	IFF(demand_at_init_lit IS NULL, 0, demand_at_init_lit) AS demand_at_initial_litigation,
	settlement_type_cd
	FROM SQ_CLAIM_CASE_MANAGE_STAGE
),
LKP_ADJUSTOR_TAB_STAGE AS (
	SELECT
	caj_emp_client_id,
	caj_adjuster_class,
	IN_prim_lit_handler
	FROM (
		SELECT 
			caj_emp_client_id,
			caj_adjuster_class,
			IN_prim_lit_handler
		FROM adjuster_tab_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY caj_emp_client_id ORDER BY caj_emp_client_id) = 1
),
LKP_CLM_CLT_RELATION_STAGE AS (
	SELECT
	cre_client_id,
	cre_claim_nbr
	FROM (
		SELECT A.cre_claim_nbr as cre_claim_nbr, A.cre_client_id as cre_client_id 
		FROM clm_clt_relation_stage A
		WHERE A.source_system_id ='EXCEED' and A.cre_client_role_cd ='WLIA'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cre_claim_nbr,cre_client_id ORDER BY cre_client_id) = 1
),
LKP_Prim_Lit_handler_ak_id AS (
	SELECT
	claim_party_ak_id,
	claim_party_key
	FROM (
		SELECT 
		A.claim_party_ak_id as claim_party_ak_id, 
		A.claim_party_key as claim_party_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party  A INNER JOIN  
		@{pipeline().parameters.STAGING_DATABASE}.@{pipeline().parameters.TARGET_TABLE_OWNER}.clm_case_manage_stage B
		ON RTRIM(A.claim_party_key) = RTRIM(B.prim_lit_handler)
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key ORDER BY claim_party_ak_id DESC) = 1
),
EXP_LKP_VALUES AS (
	SELECT
	EXP_VALIDATE.tch_claim_nbr,
	EXP_VALIDATE.tch_client_id,
	EXP_VALIDATE.CLAIM_CASE_KEY,
	EXP_VALIDATE.case_name,
	EXP_VALIDATE.case_number,
	EXP_VALIDATE.suit_venue,
	EXP_VALIDATE.suit_state,
	EXP_VALIDATE.trial_date,
	EXP_VALIDATE.IN_policy_limit_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_policy_limit_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_policy_limit_id.tcc_comment_txt) AS v_pol_lim_comment,
	EXP_VALIDATE.IN_case_desc_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_case_desc_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_case_desc_id.tcc_comment_txt) AS v_claim_case_comment,
	EXP_VALIDATE.IN_inj_dam_desc_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_inj_dam_desc_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_inj_dam_desc_id.tcc_comment_txt) AS v_injury_dam_comment,
	EXP_VALIDATE.IN_subro_cont_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_subro_cont_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_subro_cont_id.tcc_comment_txt) AS v_subrogation_contri_comment,
	EXP_VALIDATE.first_not_law_suit,
	EXP_VALIDATE.declaratory_act,
	EXP_VALIDATE.suit_status,
	EXP_VALIDATE.denial_date,
	EXP_VALIDATE.IN_prim_lit_handler,
	LKP_Prim_Lit_handler_ak_id.claim_party_ak_id AS IN_prim_lit_handler_ak_id,
	-- *INF*: IIF(ISNULL(IN_prim_lit_handler_ak_id),-1,IN_prim_lit_handler_ak_id)
	IFF(IN_prim_lit_handler_ak_id IS NULL, - 1, IN_prim_lit_handler_ak_id) AS Out_prim_lit_handler_ak_id,
	LKP_ADJUSTOR_TAB_STAGE.caj_adjuster_class,
	LKP_ADJUSTOR_TAB_STAGE.caj_emp_client_id,
	LKP_CLM_CLT_RELATION_STAGE.cre_client_id,
	-- *INF*: IIF(ISNULL(IN_prim_lit_handler) OR IS_SPACES(IN_prim_lit_handler) OR LENGTH(IN_prim_lit_handler) =0,'N/A',
	-- 	IIF(NOT ISNULL(caj_emp_client_id),
	-- 		IIF(caj_adjuster_class = 'L','STF','ADJ'),IIF(ISNULL(cre_client_id),'REG','ARB')))
	IFF(IN_prim_lit_handler IS NULL OR IS_SPACES(IN_prim_lit_handler) OR LENGTH(IN_prim_lit_handler) = 0, 'N/A', IFF(NOT caj_emp_client_id IS NULL, IFF(caj_adjuster_class = 'L', 'STF', 'ADJ'), IFF(cre_client_id IS NULL, 'REG', 'ARB'))) AS v_prim_lit_handler_role_code,
	v_prim_lit_handler_role_code AS prim_lit_handler_role_code,
	EXP_VALIDATE.litigation_date,
	EXP_VALIDATE.litigation_closed,
	EXP_VALIDATE.how_clm_closed,
	EXP_VALIDATE.reins_reported,
	EXP_VALIDATE.IN_pros_cmt_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_pros_cmt_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pros_cmt_id.tcc_comment_txt) AS v_pros_comment,
	EXP_VALIDATE.IN_cons_cmt_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_cons_cmt_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_cons_cmt_id.tcc_comment_txt) AS v_cons_comment,
	EXP_VALIDATE.IN_pripst_inj_desc_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_pripst_inj_desc_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pripst_inj_desc_id.tcc_comment_txt) AS v_pripst_injury_comment,
	EXP_VALIDATE.IN_liab_cmt_id,
	-- *INF*: rtrim(:LKP.LKP_CLM_COMMENTS_STAGE(tch_claim_nbr,IN_liab_cmt_id))
	rtrim(LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_liab_cmt_id.tcc_comment_txt) AS v_liability_comment,
	EXP_VALIDATE.com_umb_res,
	EXP_VALIDATE.payment_amt,
	EXP_VALIDATE.demand_at_initial_litigation,
	-- *INF*: 1+ length(
	-- v_pol_lim_comment || 
	-- v_claim_case_comment || 
	-- v_injury_dam_comment || 
	-- v_subrogation_contri_comment ||  
	-- v_pros_comment || 
	-- v_cons_comment || 
	-- v_pripst_injury_comment ||  
	-- v_liability_comment
	-- )
	-- 
	-- //getting the total length of all the comments
	-- // adding 1 so that the len is not equal to 0 if all comments are null
	-- // This helps to avoid zero divisor error in the ratio variables
	1 + length(v_pol_lim_comment || v_claim_case_comment || v_injury_dam_comment || v_subrogation_contri_comment || v_pros_comment || v_cons_comment || v_pripst_injury_comment || v_liability_comment) AS v_total_comments_length,
	-- *INF*: v_total_comments_length - 7000
	-- 
	-- 
	-- // the total length of comments should only be 7000. If it is more than that then the difference is calculated
	v_total_comments_length - 7000 AS v_extra_comments_length,
	-- *INF*: length(v_pol_lim_comment)/v_total_comments_length
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_pol_lim_comment) / v_total_comments_length AS pol_lmt_comment_ratio,
	-- *INF*: length(v_claim_case_comment)/v_total_comments_length
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_claim_case_comment) / v_total_comments_length AS claim_case_comment_ratio,
	-- *INF*: length(v_injury_dam_comment)/v_total_comments_length
	-- 
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_injury_dam_comment) / v_total_comments_length AS inj_dam_comment_ratio,
	-- *INF*: length(v_subrogation_contri_comment)/v_total_comments_length
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_subrogation_contri_comment) / v_total_comments_length AS sub_cont_comment_ratio,
	-- *INF*: length(v_pros_comment)/v_total_comments_length
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_pros_comment) / v_total_comments_length AS pros_comment_ratio,
	-- *INF*: length(v_cons_comment)/v_total_comments_length
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_cons_comment) / v_total_comments_length AS cons_comment_ratio,
	-- *INF*: length(v_pripst_injury_comment)/v_total_comments_length
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_pripst_injury_comment) / v_total_comments_length AS prior_post_inj_comment_ratio,
	-- *INF*: length(v_liability_comment)/v_total_comments_length
	-- 
	-- //finding out what ratio of the total length, this particular comment field is taking
	length(v_liability_comment) / v_total_comments_length AS liab_comment_ratio,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_pol_lim_comment) OR IS_SPACES(v_pol_lim_comment) OR LENGTH(v_pol_lim_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_pol_lim_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_pol_lim_comment, 
	-- 1, 
	-- length(v_pol_lim_comment) - ( FLOOR(v_extra_comments_length * pol_lmt_comment_ratio ))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_pol_lim_comment) OR IS_SPACES(v_pol_lim_comment) OR LENGTH(v_pol_lim_comment) =0, 'N/A', LTRIM(RTRIM(v_pol_lim_comment)))
	DECODE(TRUE,
		v_pol_lim_comment IS NULL OR IS_SPACES(v_pol_lim_comment) OR LENGTH(v_pol_lim_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_pol_lim_comment,
		v_total_comments_length > 7000, SUBSTR(v_pol_lim_comment, 1, length(v_pol_lim_comment) - ( FLOOR(v_extra_comments_length * pol_lmt_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS pol_lim_comment,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_claim_case_comment) OR IS_SPACES(v_claim_case_comment) OR LENGTH(v_claim_case_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_claim_case_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_claim_case_comment, 
	-- 1, 
	-- length(v_claim_case_comment) - ( FLOOR(v_extra_comments_length * claim_case_comment_ratio))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_claim_case_comment) OR IS_SPACES(v_claim_case_comment) OR LENGTH(v_claim_case_comment) =0,'N/A',LTRIM(RTRIM(v_claim_case_comment)))
	DECODE(TRUE,
		v_claim_case_comment IS NULL OR IS_SPACES(v_claim_case_comment) OR LENGTH(v_claim_case_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_claim_case_comment,
		v_total_comments_length > 7000, SUBSTR(v_claim_case_comment, 1, length(v_claim_case_comment) - ( FLOOR(v_extra_comments_length * claim_case_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS claim_case_comment,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_injury_dam_comment) OR IS_SPACES(v_injury_dam_comment) OR LENGTH(v_injury_dam_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_injury_dam_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_injury_dam_comment, 
	-- 1, 
	-- length(v_injury_dam_comment) - ( FLOOR(v_extra_comments_length * inj_dam_comment_ratio))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_injury_dam_comment) OR IS_SPACES(v_injury_dam_comment) OR LENGTH(v_injury_dam_comment) =0,'N/A',LTRIM(RTRIM(v_injury_dam_comment)))
	DECODE(TRUE,
		v_injury_dam_comment IS NULL OR IS_SPACES(v_injury_dam_comment) OR LENGTH(v_injury_dam_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_injury_dam_comment,
		v_total_comments_length > 7000, SUBSTR(v_injury_dam_comment, 1, length(v_injury_dam_comment) - ( FLOOR(v_extra_comments_length * inj_dam_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS injury_dam_comment,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_subrogation_contri_comment) OR IS_SPACES(v_subrogation_contri_comment) OR LENGTH(v_subrogation_contri_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_subrogation_contri_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_subrogation_contri_comment, 
	-- 1, 
	-- length(v_subrogation_contri_comment) - ( FLOOR(v_extra_comments_length * sub_cont_comment_ratio))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_subrogation_contri_comment) OR IS_SPACES(v_subrogation_contri_comment) OR LENGTH(v_subrogation_contri_comment) =0,'N/A',LTRIM(RTRIM(v_subrogation_contri_comment)))
	DECODE(TRUE,
		v_subrogation_contri_comment IS NULL OR IS_SPACES(v_subrogation_contri_comment) OR LENGTH(v_subrogation_contri_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_subrogation_contri_comment,
		v_total_comments_length > 7000, SUBSTR(v_subrogation_contri_comment, 1, length(v_subrogation_contri_comment) - ( FLOOR(v_extra_comments_length * sub_cont_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS subrogation_contri_comment,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_pros_comment) OR IS_SPACES(v_pros_comment) OR LENGTH(v_pros_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_pros_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_pros_comment, 
	-- 1, 
	-- length(v_pros_comment) - ( FLOOR(v_extra_comments_length * pros_comment_ratio))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_pros_comment) OR IS_SPACES(v_pros_comment) OR LENGTH(v_pros_comment) =0,'N/A',LTRIM--(RTRIM(v_pros_comment)))
	DECODE(TRUE,
		v_pros_comment IS NULL OR IS_SPACES(v_pros_comment) OR LENGTH(v_pros_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_pros_comment,
		v_total_comments_length > 7000, SUBSTR(v_pros_comment, 1, length(v_pros_comment) - ( FLOOR(v_extra_comments_length * pros_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS pros_comment,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_cons_comment) OR IS_SPACES(v_cons_comment) OR LENGTH(v_cons_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_cons_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_cons_comment, 
	-- 1, 
	-- length(v_cons_comment) - ( FLOOR(v_extra_comments_length * cons_comment_ratio))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_cons_comment) OR IS_SPACES(v_cons_comment) OR LENGTH(v_cons_comment) =0,'N/A',LTRIM--(RTRIM(v_cons_comment)))
	DECODE(TRUE,
		v_cons_comment IS NULL OR IS_SPACES(v_cons_comment) OR LENGTH(v_cons_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_cons_comment,
		v_total_comments_length > 7000, SUBSTR(v_cons_comment, 1, length(v_cons_comment) - ( FLOOR(v_extra_comments_length * cons_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS cons_comment,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_pripst_injury_comment) OR IS_SPACES(v_pripst_injury_comment) OR LENGTH(v_pripst_injury_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_pripst_injury_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_pripst_injury_comment, 
	-- 1, 
	-- length(v_pripst_injury_comment) - ( FLOOR(v_extra_comments_length * prior_post_inj_comment_ratio))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_pripst_injury_comment) OR IS_SPACES(v_pripst_injury_comment) OR LENGTH(v_pripst_injury_comment) =0,'N/A',LTRIM(RTRIM(v_pripst_injury_comment)))
	DECODE(TRUE,
		v_pripst_injury_comment IS NULL OR IS_SPACES(v_pripst_injury_comment) OR LENGTH(v_pripst_injury_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_pripst_injury_comment,
		v_total_comments_length > 7000, SUBSTR(v_pripst_injury_comment, 1, length(v_pripst_injury_comment) - ( FLOOR(v_extra_comments_length * prior_post_inj_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS pripst_injury_comment,
	-- *INF*: DECODE (TRUE,
	-- 
	-- ISNULL(v_liability_comment) OR IS_SPACES(v_liability_comment) OR LENGTH(v_liability_comment) =0, 'N/A', 
	-- 
	-- v_total_comments_length <= 7000,v_liability_comment,
	-- 
	-- v_total_comments_length > 7000, 
	-- SUBSTR(v_liability_comment, 
	-- 1, 
	-- length(v_liability_comment) - ( FLOOR(v_extra_comments_length * liab_comment_ratio))   - 35) || '...Rest of the comments removed...'
	-- )
	-- 
	-- // if this comment length is less than 7000 then it is loaded as it it.
	-- // if the length is more than 7000, then it is trimmed based on this comment's total length and the ratio of this length to the total length of all comment fields. A custom text is added to the end..
	-- 
	-- 
	-- --old expression removed on 9/8 by shiva
	-- --IIF(ISNULL(v_liability_comment) OR IS_SPACES(v_liability_comment) OR LENGTH(v_liability_comment) =--0,'N/A',LTRIM(RTRIM(v_liability_comment)))
	DECODE(TRUE,
		v_liability_comment IS NULL OR IS_SPACES(v_liability_comment) OR LENGTH(v_liability_comment) = 0, 'N/A',
		v_total_comments_length <= 7000, v_liability_comment,
		v_total_comments_length > 7000, SUBSTR(v_liability_comment, 1, length(v_liability_comment) - ( FLOOR(v_extra_comments_length * liab_comment_ratio) ) - 35) || '...Rest of the comments removed...') AS liability_comment,
	EXP_VALIDATE.settlement_type_cd,
	-- *INF*: IIF(ISNULL(settlement_type_cd), 'N/A', settlement_type_cd)
	IFF(settlement_type_cd IS NULL, 'N/A', settlement_type_cd) AS SettlementTypeCode
	FROM EXP_VALIDATE
	LEFT JOIN LKP_ADJUSTOR_TAB_STAGE
	ON LKP_ADJUSTOR_TAB_STAGE.caj_emp_client_id = EXP_VALIDATE.IN_prim_lit_handler
	LEFT JOIN LKP_CLM_CLT_RELATION_STAGE
	ON LKP_CLM_CLT_RELATION_STAGE.cre_claim_nbr = EXP_VALIDATE.tch_claim_nbr AND LKP_CLM_CLT_RELATION_STAGE.cre_client_id = EXP_VALIDATE.IN_prim_lit_handler
	LEFT JOIN LKP_Prim_Lit_handler_ak_id
	ON LKP_Prim_Lit_handler_ak_id.claim_party_key = EXP_VALIDATE.IN_prim_lit_handler
	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_policy_limit_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_policy_limit_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_policy_limit_id.comment_item_nbr = IN_policy_limit_id

	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_case_desc_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_case_desc_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_case_desc_id.comment_item_nbr = IN_case_desc_id

	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_inj_dam_desc_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_inj_dam_desc_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_inj_dam_desc_id.comment_item_nbr = IN_inj_dam_desc_id

	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_subro_cont_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_subro_cont_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_subro_cont_id.comment_item_nbr = IN_subro_cont_id

	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pros_cmt_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pros_cmt_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pros_cmt_id.comment_item_nbr = IN_pros_cmt_id

	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_cons_cmt_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_cons_cmt_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_cons_cmt_id.comment_item_nbr = IN_cons_cmt_id

	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pripst_inj_desc_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pripst_inj_desc_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_pripst_inj_desc_id.comment_item_nbr = IN_pripst_inj_desc_id

	LEFT JOIN LKP_CLM_COMMENTS_STAGE LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_liab_cmt_id
	ON LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_liab_cmt_id.folder_key = tch_claim_nbr
	AND LKP_CLM_COMMENTS_STAGE_tch_claim_nbr_IN_liab_cmt_id.comment_item_nbr = IN_liab_cmt_id

),
LKP_Claim_Case AS (
	SELECT
	claim_case_id,
	claim_case_ak_id,
	claim_case_name,
	claim_case_num,
	suit_county,
	suit_state,
	trial_date,
	first_notice_law_suit_ind,
	declaratory_action_ind,
	suit_status_code,
	suit_denial_date,
	prim_litigation_handler_ak_id,
	prim_litigation_handler_role_code,
	suit_open_date,
	suit_close_date,
	suit_how_claim_closed,
	reins_reported_ind,
	commercl_umb_reserve,
	suit_pay_amt,
	Demand_at_initial_litigation,
	SettlementTypeCode,
	IN_CLAIM_CASE_KEY,
	claim_case_key
	FROM (
		SELECT a.claim_case_id as claim_case_id, 
		a.claim_case_ak_id as claim_case_ak_id, 
		a.claim_case_name as claim_case_name, 
		a.claim_case_num as claim_case_num, 
		a.suit_county as suit_county, 
		a.suit_state as suit_state, 
		a.trial_date as trial_date, 
		a.first_notice_law_suit_ind as first_notice_law_suit_ind, 
		a.declaratory_action_ind as declaratory_action_ind, 
		a.suit_status_code as suit_status_code, 
		a.suit_denial_date as suit_denial_date, 
		a.prim_litigation_handler_ak_id as prim_litigation_handler_ak_id, 
		a.prim_litigation_handler_role_code as prim_litigation_handler_role_code, 
		a.suit_open_date as suit_open_date, 
		a.suit_close_date as suit_close_date, 
		a.suit_how_claim_closed as suit_how_claim_closed, 
		a.reins_reported_ind as reins_reported_ind, 
		a.commercl_umb_reserve as commercl_umb_reserve, 
		a.suit_pay_amt as suit_pay_amt, 
		a.Demand_at_initial_litigation as Demand_at_initial_litigation, 
		a.SettlementTypeCode as SettlementTypeCode,
		a.claim_case_key as claim_case_key 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case A
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_key ORDER BY claim_case_id DESC) = 1
),
EXP_DETECT_CHANGES AS (
	SELECT
	EXP_LKP_VALUES.CLAIM_CASE_KEY,
	EXP_LKP_VALUES.case_name,
	EXP_LKP_VALUES.case_number,
	EXP_LKP_VALUES.suit_venue,
	EXP_LKP_VALUES.suit_state,
	EXP_LKP_VALUES.trial_date,
	EXP_LKP_VALUES.first_not_law_suit,
	EXP_LKP_VALUES.declaratory_act,
	EXP_LKP_VALUES.suit_status,
	EXP_LKP_VALUES.denial_date,
	EXP_LKP_VALUES.Out_prim_lit_handler_ak_id AS Prim_lit_handler_ak_id,
	EXP_LKP_VALUES.prim_lit_handler_role_code,
	EXP_LKP_VALUES.litigation_date,
	EXP_LKP_VALUES.litigation_closed,
	EXP_LKP_VALUES.how_clm_closed,
	EXP_LKP_VALUES.reins_reported,
	EXP_LKP_VALUES.com_umb_res,
	EXP_LKP_VALUES.payment_amt,
	EXP_LKP_VALUES.demand_at_initial_litigation,
	LKP_Claim_Case.claim_case_id AS old_claim_case_id,
	LKP_Claim_Case.claim_case_ak_id AS old_claim_case_ak_id,
	LKP_Claim_Case.claim_case_name AS old_claim_case_name,
	LKP_Claim_Case.claim_case_num AS old_claim_case_num,
	LKP_Claim_Case.suit_county AS old_suit_county,
	LKP_Claim_Case.suit_state AS old_suit_state,
	LKP_Claim_Case.trial_date AS old_trial_date,
	LKP_Claim_Case.first_notice_law_suit_ind AS old_first_notice_law_suit_ind,
	LKP_Claim_Case.declaratory_action_ind AS old_declaratory_act,
	LKP_Claim_Case.suit_status_code AS old_suit_status_code,
	LKP_Claim_Case.suit_denial_date AS old_suit_denial_date,
	LKP_Claim_Case.prim_litigation_handler_ak_id AS old_prim_litigation_handler_ak_id,
	LKP_Claim_Case.prim_litigation_handler_role_code AS old_prim_litigation_handler_role_code,
	LKP_Claim_Case.suit_open_date AS old_suit_open_date,
	LKP_Claim_Case.suit_close_date AS old_suit_close_date,
	LKP_Claim_Case.suit_how_claim_closed AS old_suit_how_claim_closed,
	LKP_Claim_Case.reins_reported_ind AS old_reins_reported_ind,
	LKP_Claim_Case.commercl_umb_reserve AS old_commercl_umb_reserve,
	LKP_Claim_Case.suit_pay_amt AS old_suit_pay_amt,
	LKP_Claim_Case.Demand_at_initial_litigation AS old_Demand_at_initial_litigation,
	LKP_Claim_Case.SettlementTypeCode AS Old_SettlementTypeCode,
	-- *INF*: IIF(ISNULL(old_claim_case_id),'NEW',
	--      IIF(LTRIM(RTRIM(case_name)) <> LTRIM(RTRIM(old_claim_case_name)) OR 
	-- 	LTRIM(RTRIM(case_number)) <> LTRIM(RTRIM(old_claim_case_num)) OR 
	-- 	LTRIM(RTRIM(suit_venue)) <> LTRIM(RTRIM(old_suit_county)) OR 
	-- 	LTRIM(RTRIM(suit_state)) <> LTRIM(RTRIM(old_suit_state)) OR 
	-- 	trial_date <> old_trial_date OR 
	-- 	LTRIM(RTRIM(first_not_law_suit)) <> LTRIM(RTRIM(old_first_notice_law_suit_ind)) OR 
	-- 	LTRIM(RTRIM(declaratory_act)) <> LTRIM(RTRIM(old_declaratory_act)) OR 
	-- 	LTRIM(RTRIM(suit_status)) <> LTRIM(RTRIM(old_suit_status_code)) OR 
	-- 	denial_date <> old_suit_denial_date OR 
	-- 	Prim_lit_handler_ak_id <> old_prim_litigation_handler_ak_id OR 
	-- 	LTRIM(RTRIM(prim_lit_handler_role_code)) <> LTRIM(RTRIM(old_prim_litigation_handler_role_code)) OR 
	-- 	litigation_date <>old_suit_open_date OR 
	-- 	litigation_closed<> old_suit_close_date OR 
	-- 	LTRIM(RTRIM(how_clm_closed)) <> LTRIM(RTRIM(old_suit_how_claim_closed)) OR 
	-- 	LTRIM(RTRIM(reins_reported)) <> LTRIM(RTRIM(old_reins_reported_ind)) OR 
	-- 	com_umb_res<> old_commercl_umb_reserve OR 
	-- 	payment_amt <> old_suit_pay_amt OR
	--       demand_at_initial_litigation <> old_Demand_at_initial_litigation OR 
	--       SettlementTypeCode <> Old_SettlementTypeCode,
	-- 	'UPDATE','NOCHANGE'))
	-- 
	IFF(old_claim_case_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(case_name)) <> LTRIM(RTRIM(old_claim_case_name)) OR LTRIM(RTRIM(case_number)) <> LTRIM(RTRIM(old_claim_case_num)) OR LTRIM(RTRIM(suit_venue)) <> LTRIM(RTRIM(old_suit_county)) OR LTRIM(RTRIM(suit_state)) <> LTRIM(RTRIM(old_suit_state)) OR trial_date <> old_trial_date OR LTRIM(RTRIM(first_not_law_suit)) <> LTRIM(RTRIM(old_first_notice_law_suit_ind)) OR LTRIM(RTRIM(declaratory_act)) <> LTRIM(RTRIM(old_declaratory_act)) OR LTRIM(RTRIM(suit_status)) <> LTRIM(RTRIM(old_suit_status_code)) OR denial_date <> old_suit_denial_date OR Prim_lit_handler_ak_id <> old_prim_litigation_handler_ak_id OR LTRIM(RTRIM(prim_lit_handler_role_code)) <> LTRIM(RTRIM(old_prim_litigation_handler_role_code)) OR litigation_date <> old_suit_open_date OR litigation_closed <> old_suit_close_date OR LTRIM(RTRIM(how_clm_closed)) <> LTRIM(RTRIM(old_suit_how_claim_closed)) OR LTRIM(RTRIM(reins_reported)) <> LTRIM(RTRIM(old_reins_reported_ind)) OR com_umb_res <> old_commercl_umb_reserve OR payment_amt <> old_suit_pay_amt OR demand_at_initial_litigation <> old_Demand_at_initial_litigation OR SettlementTypeCode <> Old_SettlementTypeCode, 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_changed_flag='NEW',TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM:DD:YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM:DD:YYYY HH24:MI:SS') AS default_date,
	EXP_LKP_VALUES.SettlementTypeCode
	FROM EXP_LKP_VALUES
	LEFT JOIN LKP_Claim_Case
	ON LKP_Claim_Case.claim_case_key = EXP_LKP_VALUES.CLAIM_CASE_KEY
),
FIL_INSERT AS (
	SELECT
	old_claim_case_ak_id, 
	CLAIM_CASE_KEY, 
	case_name, 
	case_number, 
	suit_venue, 
	suit_state, 
	trial_date, 
	first_not_law_suit, 
	declaratory_act, 
	suit_status, 
	denial_date, 
	Prim_lit_handler_ak_id AS IN_prim_lit_handler_ak_id, 
	prim_lit_handler_role_code, 
	litigation_date, 
	litigation_closed, 
	how_clm_closed, 
	reins_reported, 
	com_umb_res, 
	payment_amt, 
	demand_at_initial_litigation, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	default_date, 
	SettlementTypeCode
	FROM EXP_DETECT_CHANGES
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_Claim_Case_AK_ID AS (
	CREATE SEQUENCE SEQ_Claim_Case_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	old_claim_case_ak_id,
	-- *INF*: IIF(changed_flag ='NEW',NEXTVAL,old_claim_case_ak_id)
	IFF(changed_flag = 'NEW', NEXTVAL, old_claim_case_ak_id) AS claim_case_ak_id,
	CLAIM_CASE_KEY,
	case_name,
	case_number,
	suit_venue,
	suit_state,
	trial_date,
	pol_lim_comment,
	claim_case_comment,
	injury_dam_comment,
	subrogation_contri_comment,
	first_not_law_suit,
	declaratory_act,
	suit_status,
	denial_date,
	IN_prim_lit_handler_ak_id,
	prim_lit_handler_role_code,
	litigation_date,
	litigation_closed,
	how_clm_closed,
	reins_reported,
	pros_comment,
	cons_comment,
	pripst_injury_comment,
	liability_comment,
	com_umb_res,
	payment_amt,
	demand_at_initial_litigation,
	changed_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	default_date,
	SEQ_Claim_Case_AK_ID.NEXTVAL,
	SettlementTypeCode
	FROM FIL_INSERT
),
claim_case AS (
	INSERT INTO claim_case
	(claim_case_ak_id, prim_litigation_handler_ak_id, claim_case_key, claim_case_name, claim_case_num, suit_county, suit_state, trial_date, first_notice_law_suit_ind, declaratory_action_ind, suit_status_code, suit_denial_date, prim_litigation_handler_role_code, suit_open_date, suit_close_date, suit_how_claim_closed, reins_reported_ind, commercl_umb_reserve, suit_pay_amt, arbitration_open_date, arbitration_close_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, demand_at_initial_litigation, SettlementTypeCode)
	SELECT 
	CLAIM_CASE_AK_ID, 
	IN_prim_lit_handler_ak_id AS PRIM_LITIGATION_HANDLER_AK_ID, 
	CLAIM_CASE_KEY AS CLAIM_CASE_KEY, 
	case_name AS CLAIM_CASE_NAME, 
	case_number AS CLAIM_CASE_NUM, 
	suit_venue AS SUIT_COUNTY, 
	SUIT_STATE, 
	TRIAL_DATE, 
	first_not_law_suit AS FIRST_NOTICE_LAW_SUIT_IND, 
	declaratory_act AS DECLARATORY_ACTION_IND, 
	suit_status AS SUIT_STATUS_CODE, 
	denial_date AS SUIT_DENIAL_DATE, 
	prim_lit_handler_role_code AS PRIM_LITIGATION_HANDLER_ROLE_CODE, 
	litigation_date AS SUIT_OPEN_DATE, 
	litigation_closed AS SUIT_CLOSE_DATE, 
	how_clm_closed AS SUIT_HOW_CLAIM_CLOSED, 
	reins_reported AS REINS_REPORTED_IND, 
	com_umb_res AS COMMERCL_UMB_RESERVE, 
	payment_amt AS SUIT_PAY_AMT, 
	default_date AS ARBITRATION_OPEN_DATE, 
	default_date AS ARBITRATION_CLOSE_DATE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	DEMAND_AT_INITIAL_LITIGATION, 
	SETTLEMENTTYPECODE
	FROM EXP_Determine_AK
),
SQ_claim_case AS (
	SELECT 
	a.claim_case_id, 
	a.claim_case_key,
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case a
	WHERE 
	a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND
	EXISTS(SELECT 1 
	                 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case b
	                 WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	                 AND a.claim_case_key = b.claim_case_key
	 	           GROUP BY b.claim_case_key
	                 HAVING COUNT(*) >1) 
	ORDER BY a.claim_case_key, a.eff_from_date DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_case_id,
	claim_case_key,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,claim_case_key=v_prev_row_claim_case_key,ADD_TO_DATE(v_prev_row_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(TRUE,
		claim_case_key = v_prev_row_claim_case_key, ADD_TO_DATE(v_prev_row_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_case_key AS v_prev_row_claim_case_key,
	eff_from_date AS v_prev_row_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_claim_case
),
FIL_Firstrow_INAKIDGROUP AS (
	SELECT
	claim_case_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <>eff_to_date
),
UPD_CLAIM_CASE AS (
	SELECT
	claim_case_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_Firstrow_INAKIDGROUP
),
claim_case_crrnt_snpsht_flag AS (
	MERGE INTO claim_case AS T
	USING UPD_CLAIM_CASE AS S
	ON T.claim_case_id = S.claim_case_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),